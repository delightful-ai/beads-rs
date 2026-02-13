use super::policy_reload::diff_policy_reload;
use super::reporting::{
    build_checkpoint_status, build_metrics_output, build_replica_liveness,
    build_replication_status, build_wal_status, clock_anomaly_output, collect_namespaces,
    rebuild_stats,
};
use super::*;

impl Daemon {
    pub fn admin_status(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let limits = self.limits().clone();
        let last_clock_anomaly = clock_anomaly_output(self.clock().last_anomaly());
        let proof = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(proof) => proof,
            Err(e) => return Response::err_from(e),
        };
        let read = match proof.read_scope(read) {
            Ok(read) => read,
            Err(e) => return Response::err_from(e),
        };
        if let Err(err) = proof.check_read_gate(&read) {
            return Response::err_from(err);
        }
        let store = proof.runtime();
        let store_id = store.meta.store_id();
        let replica_id = store.meta.replica_id;

        let namespaces = collect_namespaces(store);
        let now_ms = WallClock::now().0;
        let wal_report = match build_wal_status(store, &namespaces, &limits, now_ms) {
            Ok(wal_report) => wal_report,
            Err(err) => return Response::err_from(err),
        };
        let replication = build_replication_status(store, &namespaces);
        let replica_liveness = build_replica_liveness(store);
        let watermarks_applied = store.watermarks_applied.clone();
        let watermarks_durable = store.watermarks_durable.clone();
        drop(proof);
        let checkpoints = build_checkpoint_status(self.checkpoint_group_snapshots(store_id));

        let output = AdminStatusOutput {
            store_id,
            replica_id,
            namespaces: namespaces.into_iter().collect(),
            watermarks_applied,
            watermarks_durable,
            last_clock_anomaly,
            wal: wal_report.namespaces,
            wal_warnings: wal_report.warnings,
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
        let limits = self.limits().clone();
        let proof = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(proof) => proof,
            Err(e) => return Response::err_from(e),
        };
        let read = match proof.read_scope(read) {
            Ok(read) => read,
            Err(e) => return Response::err_from(e),
        };
        if let Err(err) = proof.check_read_gate(&read) {
            return Response::err_from(err);
        }

        let store = proof.runtime();
        let namespaces = collect_namespaces(store);
        let now_ms = WallClock::now().0;
        if let Err(err) = build_wal_status(store, &namespaces, &limits, now_ms) {
            return Response::err_from(err);
        }
        drop(proof);
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
        // Ensure the repo is loaded before snapshotting checkpoint groups so
        // first-load doctor runs include default checkpoint group config.
        let store_id = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(proof) => proof.store_id(),
            Err(e) => return Response::err_from(e),
        };
        let checkpoint_groups = self
            .checkpoint_group_snapshots(store_id)
            .into_iter()
            .map(|snapshot| snapshot.group)
            .collect::<Vec<_>>();
        let limits = self.limits().clone();
        let last_clock_anomaly = clock_anomaly_output(self.clock().last_anomaly());
        let proof = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(proof) => proof,
            Err(e) => return Response::err_from(e),
        };
        let read = match proof.read_scope(read) {
            Ok(read) => read,
            Err(e) => return Response::err_from(e),
        };
        if let Err(err) = proof.check_read_gate(&read) {
            return Response::err_from(err);
        }
        let store = proof.runtime();

        let mut options = ScrubOptions::default_for_doctor();
        if let Some(value) = max_records_per_namespace {
            let value = usize::try_from(value).map_err(|_| OpError::InvalidRequest {
                field: Some("max_records_per_namespace".into()),
                reason: "max_records_per_namespace too large".into(),
            });
            match value {
                Ok(0) => {
                    return Response::err_from(OpError::InvalidRequest {
                        field: Some("max_records_per_namespace".into()),
                        reason: "max_records_per_namespace must be >= 1".into(),
                    });
                }
                Ok(value) => options.max_records_per_namespace = value,
                Err(err) => return Response::err_from(err),
            }
        }
        options.verify_checkpoint_cache = verify_checkpoint_cache;

        let mut report = scrub_store(store, &limits, &checkpoint_groups, options);
        drop(proof);
        report.last_clock_anomaly = last_clock_anomaly;
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
        // Ensure the repo is loaded before snapshotting checkpoint groups so
        // first-load scrub runs include default checkpoint group config.
        let store_id = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(proof) => proof.store_id(),
            Err(e) => return Response::err_from(e),
        };
        let checkpoint_groups = self
            .checkpoint_group_snapshots(store_id)
            .into_iter()
            .map(|snapshot| snapshot.group)
            .collect::<Vec<_>>();
        let limits = self.limits().clone();
        let last_clock_anomaly = clock_anomaly_output(self.clock().last_anomaly());
        let proof = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(proof) => proof,
            Err(e) => return Response::err_from(e),
        };
        let read = match proof.read_scope(read) {
            Ok(read) => read,
            Err(e) => return Response::err_from(e),
        };
        if let Err(err) = proof.check_read_gate(&read) {
            return Response::err_from(err);
        }
        let store = proof.runtime();

        let mut options = ScrubOptions::default_for_scrub();
        if let Some(value) = max_records_per_namespace {
            let value = usize::try_from(value).map_err(|_| OpError::InvalidRequest {
                field: Some("max_records_per_namespace".into()),
                reason: "max_records_per_namespace too large".into(),
            });
            match value {
                Ok(0) => {
                    return Response::err_from(OpError::InvalidRequest {
                        field: Some("max_records_per_namespace".into()),
                        reason: "max_records_per_namespace must be >= 1".into(),
                    });
                }
                Ok(value) => options.max_records_per_namespace = value,
                Err(err) => return Response::err_from(err),
            }
        }
        options.verify_checkpoint_cache = verify_checkpoint_cache;

        let mut report = scrub_store(store, &limits, &checkpoint_groups, options);
        drop(proof);
        report.last_clock_anomaly = last_clock_anomaly;
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
        namespace: Option<NamespaceId>,
        checkpoint_now: bool,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let mut proof = match self.ensure_repo_loaded_strict(repo, git_tx) {
            Ok(proof) => proof,
            Err(err) => return Response::err_from(err),
        };
        let namespace = match proof.normalize_namespace(namespace) {
            Ok(namespace) => namespace,
            Err(err) => return Response::err_from(err),
        };
        let flushed_at_ms = WallClock::now().0;
        let (segment, store_id) = {
            let store = proof.runtime_mut();
            let store_id = store.meta.store_id();
            let segment = match store.event_wal.flush(&namespace) {
                Ok(segment) => segment,
                Err(err) => return Response::err_from(OpError::EventWal(Box::new(err))),
            };
            (segment, store_id)
        };
        drop(proof);

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
            Err(e) => return Response::err_from(e),
        };
        let read = match proof.read_scope(read) {
            Ok(read) => read,
            Err(e) => return Response::err_from(e),
        };
        if let Err(err) = proof.check_read_gate(&read) {
            return Response::err_from(err);
        }
        let store = proof.runtime();

        let fingerprint_mode = match mode {
            AdminFingerprintMode::Full => {
                if sample.is_some() {
                    return Response::err_from(OpError::InvalidRequest {
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
                        return Response::err_from(OpError::InvalidRequest {
                            field: Some("sample".into()),
                            reason: "sample required for mode=sample".into(),
                        });
                    }
                };
                let shard_count = usize::from(sample.shard_count);
                if shard_count == 0 {
                    return Response::err_from(OpError::InvalidRequest {
                        field: Some("sample.shard_count".into()),
                        reason: "shard_count must be >= 1".into(),
                    });
                }
                if shard_count > SHARD_COUNT {
                    return Response::err_from(OpError::InvalidRequest {
                        field: Some("sample.shard_count".into()),
                        reason: format!("shard_count must be <= {SHARD_COUNT}"),
                    });
                }
                if sample.nonce.trim().is_empty() {
                    return Response::err_from(OpError::InvalidRequest {
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
                    FingerprintError::InvalidShardIndex { .. } => err.to_string(),
                };
                return Response::err_from(OpError::InvalidRequest {
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
        let mut proof = match self.ensure_repo_loaded_strict(repo, git_tx) {
            Ok(proof) => proof,
            Err(err) => return Response::err_from(err),
        };
        let store_id = proof.store_id();
        let store = proof.runtime_mut();

        let path = crate::paths::namespaces_path(store_id);
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(err) => {
                return Response::err_from(OpError::ValidationFailed {
                    field: "namespaces".into(),
                    reason: format!("failed to read {}: {err}", path.display()),
                });
            }
        };
        let policies = match NamespacePolicies::from_toml_str(&raw) {
            Ok(policies) => policies,
            Err(err) => {
                return Response::err_from(OpError::ValidationFailed {
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

    pub fn admin_reload_limits(&mut self, repo: &Path, git_tx: &Sender<GitOp>) -> Response {
        let proof = match self.ensure_repo_loaded_strict(repo, git_tx) {
            Ok(proof) => proof,
            Err(err) => return Response::err_from(err),
        };
        let store_id = proof.store_id();
        drop(proof);

        let config = match crate::config::load_for_repo(Some(repo)) {
            Ok(config) => config,
            Err(err) => {
                return Response::err_from(OpError::ValidationFailed {
                    field: "limits".into(),
                    reason: format!("failed to reload config: {err}"),
                });
            }
        };

        let new_limits = config.limits.clone();
        let requires_restart = match self.apply_limits(new_limits) {
            Ok(requires_restart) => requires_restart,
            Err(err) => return Response::err_from(err),
        };

        // Also reload checkpoint groups
        let checkpoint_groups_reloaded = match self.reload_checkpoint_groups(repo) {
            Ok(count) => Some(count),
            Err(err) => {
                tracing::warn!(error = ?err, "failed to reload checkpoint groups");
                None
            }
        };

        let output = AdminReloadLimitsOutput {
            store_id,
            requires_restart,
            checkpoint_groups_reloaded,
        };
        Response::ok(ResponsePayload::query(QueryResult::AdminReloadLimits(
            output,
        )))
    }

    pub fn admin_reload_replication(&mut self, repo: &Path, git_tx: &Sender<GitOp>) -> Response {
        let proof = match self.ensure_repo_loaded_strict(repo, git_tx) {
            Ok(proof) => proof,
            Err(err) => return Response::err_from(err),
        };
        let store_id = proof.store_id();
        drop(proof);

        // Reload replication config from disk
        if let Err(err) = self.reload_replication_config(repo) {
            return Response::err_from(err);
        }

        if let Err(err) = self.reload_replication_runtime(store_id) {
            return Response::err_from(err);
        }

        let roster = match load_replica_roster(store_id) {
            Ok(roster) => roster,
            Err(err) => return Response::err_from(OpError::StoreRuntime(Box::new(err))),
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
        let mut proof = match self.ensure_repo_loaded_strict(repo, git_tx) {
            Ok(proof) => proof,
            Err(err) => return Response::err_from(err),
        };
        let store_id = proof.store_id();
        let store = proof.runtime_mut();

        let (old_replica_id, new_replica_id) = match store.rotate_replica_id() {
            Ok(ids) => ids,
            Err(err) => return Response::err_from(OpError::StoreRuntime(Box::new(err))),
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
        let mut proof = match self.ensure_repo_loaded_strict(repo, git_tx) {
            Ok(proof) => proof,
            Err(err) => return Response::err_from(err),
        };
        let store = proof.runtime_mut();

        store.maintenance_mode = enabled;
        let output = AdminMaintenanceModeOutput { enabled };
        Response::ok(ResponsePayload::query(QueryResult::AdminMaintenanceMode(
            output,
        )))
    }

    pub fn admin_rebuild_index(&mut self, repo: &Path, git_tx: &Sender<GitOp>) -> Response {
        let limits = self.limits().clone();
        let mut proof = match self.ensure_repo_loaded_strict(repo, git_tx) {
            Ok(proof) => proof,
            Err(err) => return Response::err_from(err),
        };
        let store_id = proof.store_id();
        let store_dir = crate::paths::store_dir(store_id);
        let store = proof.runtime_mut();
        if !store.maintenance_mode {
            return Response::err_from(OpError::MaintenanceMode {
                reason: Some("maintenance mode required".into()),
            });
        }

        let stats = match rebuild_index(&store_dir, &store.meta, store.wal_index.as_ref(), &limits)
        {
            Ok(stats) => stats,
            Err(err) => {
                return Response::err_from(OpError::StoreRuntime(Box::new(
                    StoreRuntimeError::from(err),
                )));
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
