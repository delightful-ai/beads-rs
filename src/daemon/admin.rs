//! Admin / introspection handlers.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use crossbeam::channel::Sender;

use crate::api::{
    AdminCheckpointGroup, AdminClockAnomaly, AdminClockAnomalyKind, AdminDoctorOutput,
    AdminFingerprintMode, AdminFingerprintOutput, AdminFingerprintSample, AdminFlushOutput,
    AdminFlushSegment, AdminMaintenanceModeOutput, AdminMetricHistogram, AdminMetricLabel,
    AdminMetricSample, AdminMetricsOutput, AdminPolicyChange, AdminPolicyDiff,
    AdminRebuildIndexOutput, AdminRebuildIndexStats, AdminRebuildIndexTruncation,
    AdminReloadPoliciesOutput, AdminReplicaLiveness, AdminReplicationNamespace,
    AdminReloadReplicationOutput, AdminReplicationPeer, AdminRotateReplicaIdOutput,
    AdminScrubOutput, AdminStatusOutput, AdminWalNamespace, AdminWalSegment,
};
use crate::core::{
    NamespaceId, NamespacePolicies, NamespacePolicy, ReplicaId, WallClock, Watermarks,
};
use crate::daemon::clock::{ClockAnomaly, ClockAnomalyKind};
use crate::daemon::fingerprint::{FingerprintError, FingerprintMode, fingerprint_namespaces};
use crate::daemon::metrics::{MetricHistogram, MetricLabel, MetricSample, MetricsSnapshot};
use crate::daemon::scrubber::{ScrubOptions, scrub_store};
use crate::daemon::store_runtime::{StoreRuntimeError, load_replica_roster};
use crate::daemon::wal::{ReplayStats, rebuild_index};
use crate::git::checkpoint::layout::SHARD_COUNT;
use crate::paths;

use super::core::Daemon;
use super::ipc::ReadConsistency;
use super::{GitOp, OpError, QueryResult, Response, ResponsePayload};

impl Daemon {
    pub fn admin_status(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let proof = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(proof) => proof,
            Err(e) => return Response::err(e),
        };
        let read = match self.normalize_read_consistency(&proof, read) {
            Ok(read) => read,
            Err(e) => return Response::err(e),
        };
        if let Err(err) = self.check_read_gate(&proof, &read) {
            return Response::err(err);
        }
        let store = match self.store_runtime(&proof) {
            Ok(store) => store,
            Err(e) => return Response::err(e),
        };

        let namespaces = collect_namespaces(store);
        let wal = match build_wal_status(store, &namespaces) {
            Ok(wal) => wal,
            Err(err) => return Response::err(err),
        };
        let replication = build_replication_status(store, &namespaces);
        let replica_liveness = build_replica_liveness(store);
        let checkpoints =
            build_checkpoint_status(self.checkpoint_group_snapshots(store.meta.store_id()));

        let output = AdminStatusOutput {
            store_id: store.meta.store_id(),
            replica_id: store.meta.replica_id,
            namespaces: namespaces.into_iter().collect(),
            watermarks_applied: store.watermarks_applied.clone(),
            watermarks_durable: store.watermarks_durable.clone(),
            last_clock_anomaly: clock_anomaly_output(self.clock().last_anomaly()),
            wal,
            replication,
            replica_liveness,
            checkpoints,
        };

        Response::ok(ResponsePayload::query(QueryResult::AdminStatus(output)))
    }

    pub fn admin_metrics(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let proof = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(proof) => proof,
            Err(e) => return Response::err(e),
        };
        let read = match self.normalize_read_consistency(&proof, read) {
            Ok(read) => read,
            Err(e) => return Response::err(e),
        };
        if let Err(err) = self.check_read_gate(&proof, &read) {
            return Response::err(err);
        }

        let snapshot = crate::daemon::metrics::snapshot();
        let output = build_metrics_output(snapshot);
        Response::ok(ResponsePayload::query(QueryResult::AdminMetrics(output)))
    }

    pub fn admin_doctor(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        max_records_per_namespace: Option<u64>,
        verify_checkpoint_cache: bool,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let proof = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(proof) => proof,
            Err(e) => return Response::err(e),
        };
        let read = match self.normalize_read_consistency(&proof, read) {
            Ok(read) => read,
            Err(e) => return Response::err(e),
        };
        if let Err(err) = self.check_read_gate(&proof, &read) {
            return Response::err(err);
        }
        let store = match self.store_runtime(&proof) {
            Ok(store) => store,
            Err(e) => return Response::err(e),
        };

        let mut options = ScrubOptions::default_for_doctor();
        if let Some(value) = max_records_per_namespace {
            let value = usize::try_from(value).map_err(|_| OpError::InvalidRequest {
                field: Some("max_records_per_namespace".into()),
                reason: "max_records_per_namespace too large".into(),
            });
            match value {
                Ok(0) => {
                    return Response::err(OpError::InvalidRequest {
                        field: Some("max_records_per_namespace".into()),
                        reason: "max_records_per_namespace must be >= 1".into(),
                    });
                }
                Ok(value) => options.max_records_per_namespace = value,
                Err(err) => return Response::err(err),
            }
        }
        options.verify_checkpoint_cache = verify_checkpoint_cache;

        let checkpoint_groups = self
            .checkpoint_group_snapshots(store.meta.store_id())
            .into_iter()
            .map(|snapshot| snapshot.group)
            .collect::<Vec<_>>();

        let mut report = scrub_store(store, self.limits(), &checkpoint_groups, options);
        report.last_clock_anomaly = clock_anomaly_output(self.clock().last_anomaly());
        if report.summary.safe_to_accept_writes {
            crate::daemon::metrics::scrub_ok();
        } else {
            crate::daemon::metrics::scrub_err();
        }
        crate::daemon::metrics::scrub_records_checked(report.stats.records_checked);

        let output = AdminDoctorOutput { report };
        Response::ok(ResponsePayload::query(QueryResult::AdminDoctor(output)))
    }

    pub fn admin_scrub_now(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        max_records_per_namespace: Option<u64>,
        verify_checkpoint_cache: bool,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let proof = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(proof) => proof,
            Err(e) => return Response::err(e),
        };
        let read = match self.normalize_read_consistency(&proof, read) {
            Ok(read) => read,
            Err(e) => return Response::err(e),
        };
        if let Err(err) = self.check_read_gate(&proof, &read) {
            return Response::err(err);
        }
        let store = match self.store_runtime(&proof) {
            Ok(store) => store,
            Err(e) => return Response::err(e),
        };

        let mut options = ScrubOptions::default_for_scrub();
        if let Some(value) = max_records_per_namespace {
            let value = usize::try_from(value).map_err(|_| OpError::InvalidRequest {
                field: Some("max_records_per_namespace".into()),
                reason: "max_records_per_namespace too large".into(),
            });
            match value {
                Ok(0) => {
                    return Response::err(OpError::InvalidRequest {
                        field: Some("max_records_per_namespace".into()),
                        reason: "max_records_per_namespace must be >= 1".into(),
                    });
                }
                Ok(value) => options.max_records_per_namespace = value,
                Err(err) => return Response::err(err),
            }
        }
        options.verify_checkpoint_cache = verify_checkpoint_cache;

        let checkpoint_groups = self
            .checkpoint_group_snapshots(store.meta.store_id())
            .into_iter()
            .map(|snapshot| snapshot.group)
            .collect::<Vec<_>>();

        let mut report = scrub_store(store, self.limits(), &checkpoint_groups, options);
        report.last_clock_anomaly = clock_anomaly_output(self.clock().last_anomaly());
        if report.summary.safe_to_accept_writes {
            crate::daemon::metrics::scrub_ok();
        } else {
            crate::daemon::metrics::scrub_err();
        }
        crate::daemon::metrics::scrub_records_checked(report.stats.records_checked);

        let output = AdminScrubOutput { report };
        Response::ok(ResponsePayload::query(QueryResult::AdminScrub(output)))
    }

    pub fn admin_flush(
        &mut self,
        repo: &Path,
        namespace: Option<String>,
        checkpoint_now: bool,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let proof = match self.ensure_repo_loaded_strict(repo, git_tx) {
            Ok(proof) => proof,
            Err(err) => return Response::err(err),
        };
        let namespace = match self.normalize_namespace(&proof, namespace) {
            Ok(namespace) => namespace,
            Err(err) => return Response::err(err),
        };
        let flushed_at_ms = WallClock::now().0;
        let (segment, store_id) = match self.store_runtime_mut(&proof) {
            Ok(store) => {
                let store_id = store.meta.store_id();
                let segment = match store.event_wal.flush(&namespace) {
                    Ok(segment) => segment,
                    Err(err) => return Response::err(OpError::EventWal(Box::new(err))),
                };
                (segment, store_id)
            }
            Err(err) => return Response::err(err),
        };

        let checkpoint_groups = if checkpoint_now {
            self.force_checkpoint_for_namespace(store_id, &namespace)
        } else {
            Vec::new()
        };

        let segment = segment.map(|segment| AdminFlushSegment {
            segment_id: segment.segment_id,
            created_at_ms: segment.created_at_ms,
            path: segment.path.display().to_string(),
        });

        let output = AdminFlushOutput {
            namespace,
            flushed_at_ms,
            segment,
            checkpoint_now,
            checkpoint_groups,
        };
        Response::ok(ResponsePayload::query(QueryResult::AdminFlush(output)))
    }

    pub fn admin_fingerprint(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        mode: AdminFingerprintMode,
        sample: Option<AdminFingerprintSample>,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let proof = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(proof) => proof,
            Err(e) => return Response::err(e),
        };
        let read = match self.normalize_read_consistency(&proof, read) {
            Ok(read) => read,
            Err(e) => return Response::err(e),
        };
        if let Err(err) = self.check_read_gate(&proof, &read) {
            return Response::err(err);
        }
        let store = match self.store_runtime(&proof) {
            Ok(store) => store,
            Err(e) => return Response::err(e),
        };

        let fingerprint_mode = match mode {
            AdminFingerprintMode::Full => {
                if sample.is_some() {
                    return Response::err(OpError::InvalidRequest {
                        field: Some("sample".into()),
                        reason: "sample only valid with mode=sample".into(),
                    });
                }
                FingerprintMode::Full
            }
            AdminFingerprintMode::Sample => {
                let sample = match sample.as_ref() {
                    Some(sample) => sample,
                    None => {
                        return Response::err(OpError::InvalidRequest {
                            field: Some("sample".into()),
                            reason: "sample required for mode=sample".into(),
                        });
                    }
                };
                let shard_count = usize::from(sample.shard_count);
                if shard_count == 0 {
                    return Response::err(OpError::InvalidRequest {
                        field: Some("sample.shard_count".into()),
                        reason: "shard_count must be >= 1".into(),
                    });
                }
                if shard_count > SHARD_COUNT {
                    return Response::err(OpError::InvalidRequest {
                        field: Some("sample.shard_count".into()),
                        reason: format!("shard_count must be <= {SHARD_COUNT}"),
                    });
                }
                if sample.nonce.trim().is_empty() {
                    return Response::err(OpError::InvalidRequest {
                        field: Some("sample.nonce".into()),
                        reason: "nonce must be non-empty".into(),
                    });
                }
                FingerprintMode::Sample {
                    shard_count,
                    nonce: sample.nonce.clone(),
                }
            }
        };

        let namespaces = collect_namespaces(store).into_iter().collect::<Vec<_>>();
        let now_ms = crate::WallClock::now().0;
        let namespaces = match fingerprint_namespaces(store, &namespaces, fingerprint_mode, now_ms)
        {
            Ok(namespaces) => namespaces,
            Err(err) => {
                let reason = match &err {
                    FingerprintError::Snapshot(_) => err.to_string(),
                    FingerprintError::InvalidShardPath { .. }
                    | FingerprintError::InvalidShardIndex { .. } => err.to_string(),
                };
                return Response::err(OpError::InvalidRequest {
                    field: None,
                    reason,
                });
            }
        };

        let output = AdminFingerprintOutput {
            mode,
            sample,
            watermarks_applied: store.watermarks_applied.clone(),
            watermarks_durable: store.watermarks_durable.clone(),
            namespaces,
        };
        Response::ok(ResponsePayload::query(QueryResult::AdminFingerprint(
            output,
        )))
    }

    pub fn admin_reload_policies(&mut self, repo: &Path, git_tx: &Sender<GitOp>) -> Response {
        let proof = match self.ensure_repo_loaded_strict(repo, git_tx) {
            Ok(proof) => proof,
            Err(err) => return Response::err(err),
        };
        let store_id = proof.store_id();
        let store = match self.store_runtime_mut(&proof) {
            Ok(store) => store,
            Err(err) => return Response::err(err),
        };

        let path = crate::paths::namespaces_path(store_id);
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(err) => {
                return Response::err(OpError::ValidationFailed {
                    field: "namespaces".into(),
                    reason: format!("failed to read {}: {err}", path.display()),
                });
            }
        };
        let policies = match NamespacePolicies::from_toml_str(&raw) {
            Ok(policies) => policies,
            Err(err) => {
                return Response::err(OpError::ValidationFailed {
                    field: "namespaces".into(),
                    reason: format!("policy parse failed: {err}"),
                });
            }
        };

        let reload = diff_policy_reload(&store.policies, &policies.namespaces);
        store.policies = reload.updated;

        let output = AdminReloadPoliciesOutput {
            applied: reload.applied,
            requires_restart: reload.requires_restart,
        };
        Response::ok(ResponsePayload::query(QueryResult::AdminReloadPolicies(
            output,
        )))
    }

    pub fn admin_reload_replication(&mut self, repo: &Path, git_tx: &Sender<GitOp>) -> Response {
        let proof = match self.ensure_repo_loaded_strict(repo, git_tx) {
            Ok(proof) => proof,
            Err(err) => return Response::err(err),
        };
        let store_id = proof.store_id();

        if let Some(handles) = self.repl_handles.remove(&store_id) {
            handles.shutdown();
        }

        if let Err(err) = self.ensure_replication_runtime(store_id) {
            return Response::err(err);
        }

        let roster = match load_replica_roster(store_id) {
            Ok(roster) => roster,
            Err(err) => return Response::err(OpError::StoreRuntime(Box::new(err))),
        };
        let output = AdminReloadReplicationOutput {
            store_id,
            roster_present: roster.is_some(),
        };
        Response::ok(ResponsePayload::query(QueryResult::AdminReloadReplication(
            output,
        )))
    }

    pub fn admin_rotate_replica_id(&mut self, repo: &Path, git_tx: &Sender<GitOp>) -> Response {
        let proof = match self.ensure_repo_loaded_strict(repo, git_tx) {
            Ok(proof) => proof,
            Err(err) => return Response::err(err),
        };
        let store_id = proof.store_id();
        let store = match self.store_runtime_mut(&proof) {
            Ok(store) => store,
            Err(err) => return Response::err(err),
        };

        let (old_replica_id, new_replica_id) = match store.rotate_replica_id() {
            Ok(ids) => ids,
            Err(err) => return Response::err(OpError::StoreRuntime(Box::new(err))),
        };
        tracing::warn!(
            store_id = %store_id,
            old_replica_id = %old_replica_id,
            new_replica_id = %new_replica_id,
            "replica_id rotated"
        );

        let output = AdminRotateReplicaIdOutput {
            old_replica_id,
            new_replica_id,
        };
        Response::ok(ResponsePayload::query(QueryResult::AdminRotateReplicaId(
            output,
        )))
    }

    pub fn admin_maintenance_mode(
        &mut self,
        repo: &Path,
        enabled: bool,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let proof = match self.ensure_repo_loaded_strict(repo, git_tx) {
            Ok(proof) => proof,
            Err(err) => return Response::err(err),
        };
        let store = match self.store_runtime_mut(&proof) {
            Ok(store) => store,
            Err(err) => return Response::err(err),
        };

        store.maintenance_mode = enabled;
        let output = AdminMaintenanceModeOutput { enabled };
        Response::ok(ResponsePayload::query(QueryResult::AdminMaintenanceMode(
            output,
        )))
    }

    pub fn admin_rebuild_index(&mut self, repo: &Path, git_tx: &Sender<GitOp>) -> Response {
        let limits = self.limits().clone();
        let proof = match self.ensure_repo_loaded_strict(repo, git_tx) {
            Ok(proof) => proof,
            Err(err) => return Response::err(err),
        };
        let store = match self.store_runtime_mut(&proof) {
            Ok(store) => store,
            Err(err) => return Response::err(err),
        };
        if !store.maintenance_mode {
            return Response::err(OpError::MaintenanceMode {
                reason: Some("maintenance mode required".into()),
            });
        }

        let store_dir = crate::paths::store_dir(proof.store_id());
        let stats = match rebuild_index(&store_dir, &store.meta, store.wal_index.as_ref(), &limits)
        {
            Ok(stats) => stats,
            Err(err) => {
                return Response::err(OpError::StoreRuntime(Box::new(StoreRuntimeError::from(
                    err,
                ))));
            }
        };

        let output = AdminRebuildIndexOutput {
            stats: rebuild_stats(stats),
        };
        Response::ok(ResponsePayload::query(QueryResult::AdminRebuildIndex(
            output,
        )))
    }
}

fn collect_namespaces(store: &crate::daemon::store_runtime::StoreRuntime) -> BTreeSet<NamespaceId> {
    let mut namespaces = BTreeSet::new();
    namespaces.extend(store.policies.keys().cloned());
    namespaces.extend(store.watermarks_applied.namespaces().cloned());
    namespaces.extend(store.watermarks_durable.namespaces().cloned());
    namespaces
}

fn build_wal_status(
    store: &crate::daemon::store_runtime::StoreRuntime,
    namespaces: &BTreeSet<NamespaceId>,
) -> Result<Vec<AdminWalNamespace>, OpError> {
    let reader = store.wal_index.reader();
    let mut out = Vec::new();
    let store_dir = paths::store_dir(store.meta.store_id());
    for namespace in namespaces {
        let mut segments = reader
            .list_segments(namespace)
            .map_err(|err| OpError::StoreRuntime(Box::new(StoreRuntimeError::WalIndex(err))))?;
        segments.sort_by(|a, b| {
            a.created_at_ms
                .cmp(&b.created_at_ms)
                .then_with(|| a.segment_id.cmp(&b.segment_id))
        });
        let mut segment_infos = Vec::new();
        let mut total_bytes = 0u64;
        for segment in segments {
            let resolved_path = resolve_segment_path(&store_dir, &segment.segment_path);
            let bytes = segment_bytes(&resolved_path, segment.final_len);
            if let Some(value) = bytes {
                total_bytes = total_bytes.saturating_add(value);
            }
            segment_infos.push(AdminWalSegment {
                segment_id: segment.segment_id,
                created_at_ms: segment.created_at_ms,
                last_indexed_offset: segment.last_indexed_offset,
                sealed: segment.sealed,
                final_len: segment.final_len,
                bytes,
                path: resolved_path.to_string_lossy().to_string(),
            });
        }
        out.push(AdminWalNamespace {
            namespace: namespace.clone(),
            segment_count: segment_infos.len(),
            total_bytes,
            segments: segment_infos,
        });
    }
    Ok(out)
}

fn segment_bytes(path: &Path, final_len: Option<u64>) -> Option<u64> {
    final_len.or_else(|| fs::metadata(path).ok().map(|m| m.len()))
}

fn resolve_segment_path(store_dir: &Path, segment_path: &Path) -> PathBuf {
    if segment_path.is_absolute() {
        segment_path.to_path_buf()
    } else {
        store_dir.join(segment_path)
    }
}

fn build_replication_status(
    store: &crate::daemon::store_runtime::StoreRuntime,
    namespaces: &BTreeSet<NamespaceId>,
) -> Vec<AdminReplicationPeer> {
    let local_replica = store.meta.replica_id;
    let snapshots = store
        .peer_acks
        .lock()
        .expect("peer ack lock poisoned")
        .snapshot();

    snapshots
        .into_iter()
        .map(|snapshot| {
            let lag_by_namespace = namespaces
                .iter()
                .map(|namespace| {
                    let local_durable =
                        seq_for(&store.watermarks_durable, namespace, &local_replica);
                    let peer_durable = seq_for(&snapshot.durable, namespace, &local_replica);
                    let local_applied =
                        seq_for(&store.watermarks_applied, namespace, &local_replica);
                    let peer_applied = seq_for(&snapshot.applied, namespace, &local_replica);
                    AdminReplicationNamespace {
                        namespace: namespace.clone(),
                        local_durable_seq: local_durable,
                        peer_durable_seq: peer_durable,
                        durable_lag: local_durable.saturating_sub(peer_durable),
                        local_applied_seq: local_applied,
                        peer_applied_seq: peer_applied,
                        applied_lag: local_applied.saturating_sub(peer_applied),
                    }
                })
                .collect();

            AdminReplicationPeer {
                peer: snapshot.peer,
                last_ack_at_ms: snapshot.last_ack_at_ms,
                diverged: snapshot.diverged,
                lag_by_namespace,
                watermarks_durable: snapshot.durable,
                watermarks_applied: snapshot.applied,
            }
        })
        .collect()
}

fn build_replica_liveness(
    store: &crate::daemon::store_runtime::StoreRuntime,
) -> Vec<AdminReplicaLiveness> {
    let mut rows = match store.wal_index.reader().load_replica_liveness() {
        Ok(rows) => rows,
        Err(err) => {
            tracing::warn!(
                "replica liveness load failed for {}: {err}",
                store.meta.store_id()
            );
            return Vec::new();
        }
    };

    rows.sort_by(|a, b| a.replica_id.cmp(&b.replica_id));
    rows.into_iter()
        .map(|row| AdminReplicaLiveness {
            replica_id: row.replica_id,
            last_seen_ms: row.last_seen_ms,
            last_handshake_ms: row.last_handshake_ms,
            role: row.role,
            durability_eligible: row.durability_eligible,
        })
        .collect()
}

fn build_checkpoint_status(
    snapshots: Vec<crate::daemon::checkpoint_scheduler::CheckpointGroupSnapshot>,
) -> Vec<AdminCheckpointGroup> {
    snapshots
        .into_iter()
        .map(|snapshot| AdminCheckpointGroup {
            group: snapshot.group,
            namespaces: snapshot.namespaces,
            git_ref: snapshot.git_ref,
            dirty: snapshot.dirty,
            in_flight: snapshot.in_flight,
            last_checkpoint_wall_ms: snapshot.last_checkpoint_wall_ms,
        })
        .collect()
}

fn rebuild_stats(stats: ReplayStats) -> AdminRebuildIndexStats {
    AdminRebuildIndexStats {
        segments_scanned: stats.segments_scanned,
        records_indexed: stats.records_indexed,
        segments_truncated: stats.segments_truncated,
        tail_truncations: stats
            .tail_truncations
            .into_iter()
            .map(|truncation| AdminRebuildIndexTruncation {
                namespace: truncation.namespace,
                segment_id: truncation.segment_id,
                truncated_from_offset: truncation.truncated_from_offset,
            })
            .collect(),
    }
}

fn seq_for<K>(watermarks: &Watermarks<K>, namespace: &NamespaceId, origin: &ReplicaId) -> u64 {
    watermarks
        .get(namespace, origin)
        .map(|watermark| watermark.seq().get())
        .unwrap_or(0)
}

fn build_metrics_output(snapshot: MetricsSnapshot) -> AdminMetricsOutput {
    AdminMetricsOutput {
        counters: snapshot
            .counters
            .into_iter()
            .map(to_metric_sample)
            .collect(),
        gauges: snapshot.gauges.into_iter().map(to_metric_sample).collect(),
        histograms: snapshot
            .histograms
            .into_iter()
            .map(to_metric_histogram)
            .collect(),
    }
}

fn to_metric_sample(sample: MetricSample) -> AdminMetricSample {
    AdminMetricSample {
        name: sample.name.to_string(),
        value: sample.value,
        labels: to_metric_labels(sample.labels),
    }
}

fn to_metric_histogram(sample: MetricHistogram) -> AdminMetricHistogram {
    AdminMetricHistogram {
        name: sample.name.to_string(),
        count: sample.count,
        min: sample.min,
        max: sample.max,
        p50: sample.p50,
        p95: sample.p95,
        labels: to_metric_labels(sample.labels),
    }
}

fn to_metric_labels(labels: Vec<MetricLabel>) -> Vec<AdminMetricLabel> {
    labels
        .into_iter()
        .map(|label| AdminMetricLabel {
            key: label.key.to_string(),
            value: label.value,
        })
        .collect()
}

struct PolicyReloadSummary {
    applied: Vec<AdminPolicyDiff>,
    requires_restart: Vec<AdminPolicyDiff>,
    updated: BTreeMap<NamespaceId, NamespacePolicy>,
}

fn diff_policy_reload(
    current: &BTreeMap<NamespaceId, NamespacePolicy>,
    desired: &BTreeMap<NamespaceId, NamespacePolicy>,
) -> PolicyReloadSummary {
    let mut applied = Vec::new();
    let mut requires_restart = Vec::new();
    let mut updated = current.clone();

    let mut namespaces = BTreeSet::new();
    namespaces.extend(current.keys().cloned());
    namespaces.extend(desired.keys().cloned());

    for namespace in namespaces {
        match (current.get(&namespace), desired.get(&namespace)) {
            (Some(old), Some(new)) => {
                let mut updated_policy = old.clone();
                let (safe, restart) = diff_namespace_policy(old, new, &mut updated_policy);
                if !safe.is_empty() {
                    applied.push(AdminPolicyDiff {
                        namespace: namespace.clone(),
                        changes: safe,
                    });
                }
                if !restart.is_empty() {
                    requires_restart.push(AdminPolicyDiff {
                        namespace: namespace.clone(),
                        changes: restart,
                    });
                }
                updated.insert(namespace.clone(), updated_policy);
            }
            (None, Some(_)) => {
                requires_restart.push(AdminPolicyDiff {
                    namespace: namespace.clone(),
                    changes: vec![AdminPolicyChange {
                        field: "namespace".to_string(),
                        before: "absent".to_string(),
                        after: "added".to_string(),
                    }],
                });
            }
            (Some(_), None) => {
                requires_restart.push(AdminPolicyDiff {
                    namespace: namespace.clone(),
                    changes: vec![AdminPolicyChange {
                        field: "namespace".to_string(),
                        before: "present".to_string(),
                        after: "removed".to_string(),
                    }],
                });
            }
            (None, None) => {}
        }
    }

    PolicyReloadSummary {
        applied,
        requires_restart,
        updated,
    }
}

fn diff_namespace_policy(
    current: &NamespacePolicy,
    desired: &NamespacePolicy,
    updated: &mut NamespacePolicy,
) -> (Vec<AdminPolicyChange>, Vec<AdminPolicyChange>) {
    let mut safe = Vec::new();
    let mut restart = Vec::new();

    if current.visibility != desired.visibility {
        safe.push(policy_change(
            "visibility",
            format_visibility(current.visibility),
            format_visibility(desired.visibility),
        ));
        updated.visibility = desired.visibility;
    }

    if current.ready_eligible != desired.ready_eligible {
        safe.push(policy_change(
            "ready_eligible",
            format_bool(current.ready_eligible),
            format_bool(desired.ready_eligible),
        ));
        updated.ready_eligible = desired.ready_eligible;
    }

    if current.retention != desired.retention {
        safe.push(policy_change(
            "retention",
            format_retention(current.retention),
            format_retention(desired.retention),
        ));
        updated.retention = desired.retention;
    }

    if current.ttl_basis != desired.ttl_basis {
        safe.push(policy_change(
            "ttl_basis",
            format_ttl_basis(current.ttl_basis),
            format_ttl_basis(desired.ttl_basis),
        ));
        updated.ttl_basis = desired.ttl_basis;
    }

    if current.persist_to_git != desired.persist_to_git {
        restart.push(policy_change(
            "persist_to_git",
            format_bool(current.persist_to_git),
            format_bool(desired.persist_to_git),
        ));
    }

    if current.replicate_mode != desired.replicate_mode {
        restart.push(policy_change(
            "replicate_mode",
            format_replicate_mode(current.replicate_mode),
            format_replicate_mode(desired.replicate_mode),
        ));
    }

    if current.gc_authority != desired.gc_authority {
        restart.push(policy_change(
            "gc_authority",
            format_gc_authority(current.gc_authority),
            format_gc_authority(desired.gc_authority),
        ));
    }

    (safe, restart)
}

fn policy_change(field: &str, before: String, after: String) -> AdminPolicyChange {
    AdminPolicyChange {
        field: field.to_string(),
        before,
        after,
    }
}

fn format_bool(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

fn format_visibility(value: crate::core::NamespaceVisibility) -> String {
    match value {
        crate::core::NamespaceVisibility::Normal => "normal".to_string(),
        crate::core::NamespaceVisibility::Pinned => "pinned".to_string(),
    }
}

fn format_replicate_mode(value: crate::core::ReplicateMode) -> String {
    match value {
        crate::core::ReplicateMode::None => "none".to_string(),
        crate::core::ReplicateMode::Anchors => "anchors".to_string(),
        crate::core::ReplicateMode::Peers => "peers".to_string(),
        crate::core::ReplicateMode::P2p => "p2p".to_string(),
    }
}

fn format_retention(value: crate::core::RetentionPolicy) -> String {
    match value {
        crate::core::RetentionPolicy::Forever => "forever".to_string(),
        crate::core::RetentionPolicy::Ttl { ttl_ms } => format!("ttl:{ttl_ms}ms"),
        crate::core::RetentionPolicy::Size { max_bytes } => format!("size:{max_bytes}bytes"),
    }
}

fn format_ttl_basis(value: crate::core::TtlBasis) -> String {
    match value {
        crate::core::TtlBasis::LastMutationStamp => "last_mutation_stamp".to_string(),
        crate::core::TtlBasis::EventTime => "event_time".to_string(),
        crate::core::TtlBasis::ExplicitField => "explicit_field".to_string(),
    }
}

fn format_gc_authority(value: crate::core::GcAuthority) -> String {
    match value {
        crate::core::GcAuthority::CheckpointWriter => "checkpoint_writer".to_string(),
        crate::core::GcAuthority::ExplicitReplica { replica_id } => {
            format!("explicit_replica:{replica_id}")
        }
        crate::core::GcAuthority::None => "none".to_string(),
    }
}

fn clock_anomaly_output(anomaly: Option<ClockAnomaly>) -> Option<AdminClockAnomaly> {
    anomaly.map(|anomaly| AdminClockAnomaly {
        at_wall_ms: anomaly.wall_ms,
        kind: match anomaly.kind {
            ClockAnomalyKind::ForwardJumpClamped => AdminClockAnomalyKind::ForwardJumpClamped,
        },
        delta_ms: anomaly.delta_ms,
    })
}
