//! Admin / introspection handlers.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use crossbeam::channel::Sender;

use crate::api::{
    AdminCheckpointGroup, AdminMaintenanceModeOutput, AdminMetricHistogram, AdminMetricLabel,
    AdminMetricSample, AdminMetricsOutput, AdminRebuildIndexOutput, AdminRebuildIndexStats,
    AdminRebuildIndexTruncation, AdminReplicationNamespace, AdminReplicationPeer,
    AdminStatusOutput, AdminWalNamespace, AdminWalSegment,
};
use crate::core::{NamespaceId, ReplicaId, Watermarks};
use crate::daemon::metrics::{MetricHistogram, MetricLabel, MetricSample, MetricsSnapshot};
use crate::daemon::store_runtime::StoreRuntimeError;
use crate::daemon::wal::{ReplayStats, SegmentRow, WalIndex, rebuild_index};

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
        let read = match self.normalize_read_consistency(read) {
            Ok(read) => read,
            Err(e) => return Response::err(e),
        };
        let proof = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(proof) => proof,
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
        let checkpoints =
            build_checkpoint_status(self.checkpoint_group_snapshots(store.meta.store_id()));

        let output = AdminStatusOutput {
            store_id: store.meta.store_id(),
            replica_id: store.meta.replica_id,
            namespaces: namespaces.into_iter().collect(),
            watermarks_applied: store.watermarks_applied.clone(),
            watermarks_durable: store.watermarks_durable.clone(),
            wal,
            replication,
            checkpoints,
        };

        Response::ok(ResponsePayload::Query(QueryResult::AdminStatus(output)))
    }

    pub fn admin_metrics(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let read = match self.normalize_read_consistency(read) {
            Ok(read) => read,
            Err(e) => return Response::err(e),
        };
        let proof = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(proof) => proof,
            Err(e) => return Response::err(e),
        };
        if let Err(err) = self.check_read_gate(&proof, &read) {
            return Response::err(err);
        }

        let snapshot = crate::daemon::metrics::snapshot();
        let output = build_metrics_output(snapshot);
        Response::ok(ResponsePayload::Query(QueryResult::AdminMetrics(output)))
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
        Response::ok(ResponsePayload::Query(QueryResult::AdminMaintenanceMode(
            output,
        )))
    }

    pub fn admin_rebuild_index(&mut self, repo: &Path, git_tx: &Sender<GitOp>) -> Response {
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
        let stats = match rebuild_index(
            &store_dir,
            &store.meta,
            store.wal_index.as_ref(),
            self.limits(),
        ) {
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
        Response::ok(ResponsePayload::Query(QueryResult::AdminRebuildIndex(
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
            let bytes = segment_bytes(&segment);
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
                path: segment.segment_path.to_string_lossy().to_string(),
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

fn segment_bytes(segment: &SegmentRow) -> Option<u64> {
    segment
        .final_len
        .or_else(|| fs::metadata(&segment.segment_path).ok().map(|m| m.len()))
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
