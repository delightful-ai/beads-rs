use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use crossbeam::channel::Sender;

use super::helpers::resolve_checkpoint_git_ref;
use super::{Daemon, StoreSessionToken};
use crate::core::{NamespaceId, NamespacePolicy, ReplicaId, StoreId, WallClock};
use crate::git::checkpoint::{CheckpointPublishError, CheckpointPublishOutcome};
use crate::runtime::checkpoint_scheduler::{
    CheckpointGroupConfig, CheckpointGroupKey, CheckpointGroupSnapshot,
};
use crate::runtime::git_worker::GitOp;
use crate::runtime::metrics;
use crate::runtime::ops::OpError;

impl Daemon {
    fn emit_checkpoint_queue_depth(&self) {
        metrics::set_checkpoint_queue_depth(self.checkpoint_scheduler.queue_depth());
    }

    pub(crate) fn mark_checkpoint_dirty(
        &mut self,
        store_id: StoreId,
        namespace: &NamespaceId,
        events: u64,
    ) {
        self.checkpoint_scheduler
            .mark_dirty_for_namespace(store_id, namespace, events);
        self.emit_checkpoint_queue_depth();
    }

    pub(super) fn register_default_checkpoint_groups(
        &mut self,
        store_id: StoreId,
    ) -> Result<(), OpError> {
        if !self.checkpoint_policy().allows_checkpoints() {
            return Ok(());
        }
        let (local_replica_id, policies) = {
            let store = self
                .store_sessions
                .get(&store_id)
                .expect("loaded store missing from state")
                .runtime();
            (store.meta.replica_id, store.policies.clone())
        };
        let configs = self.checkpoint_group_configs(store_id, local_replica_id, &policies)?;
        for config in configs {
            self.checkpoint_scheduler.register_group(config);
        }
        self.emit_checkpoint_queue_depth();
        Ok(())
    }

    fn checkpoint_group_configs(
        &self,
        store_id: StoreId,
        local_replica_id: ReplicaId,
        policies: &BTreeMap<NamespaceId, NamespacePolicy>,
    ) -> Result<Vec<CheckpointGroupConfig>, OpError> {
        if self.checkpoint_groups.is_empty() {
            let config = CheckpointGroupConfig::core_default(store_id, local_replica_id);
            Self::validate_checkpoint_group_namespaces(
                &config.group,
                &config.namespaces,
                policies,
            )?;
            return Ok(vec![config]);
        }

        let mut configs = Vec::new();
        for (group, spec) in &self.checkpoint_groups {
            let namespaces = if spec.namespaces.is_empty() {
                if group == "core" {
                    vec![NamespaceId::core()]
                } else {
                    tracing::warn!(
                        checkpoint_group = %group,
                        "checkpoint group has no namespaces; skipping"
                    );
                    continue;
                }
            } else {
                spec.namespaces.clone()
            };
            Self::validate_checkpoint_group_namespaces(group, &namespaces, policies)?;

            let mut config = CheckpointGroupConfig::core_default(store_id, local_replica_id);
            config.group = group.clone();
            config.namespaces = namespaces;
            config.git_ref = resolve_checkpoint_git_ref(store_id, group, spec.git_ref.as_deref());
            config.checkpoint_writers = if spec.checkpoint_writers.is_empty() {
                vec![local_replica_id]
            } else {
                spec.checkpoint_writers.clone()
            };
            config.primary_writer = spec.primary_writer.or(Some(local_replica_id));
            if let Some(primary_writer) = config.primary_writer
                && !config.checkpoint_writers.contains(&primary_writer)
            {
                config.checkpoint_writers.push(primary_writer);
            }
            if let Some(debounce_ms) = spec.debounce_ms {
                config.debounce = Duration::from_millis(debounce_ms);
            }
            if let Some(max_interval_ms) = spec.max_interval_ms {
                config.max_interval = Duration::from_millis(max_interval_ms);
            }
            if let Some(max_events) = spec.max_events {
                config.max_events = max_events;
            }
            config.durable_copy_via_git = spec.durable_copy_via_git;

            configs.push(config);
        }

        if configs.is_empty() {
            let config = CheckpointGroupConfig::core_default(store_id, local_replica_id);
            Self::validate_checkpoint_group_namespaces(
                &config.group,
                &config.namespaces,
                policies,
            )?;
            configs.push(config);
        }

        Ok(configs)
    }

    pub(super) fn validate_checkpoint_group_namespaces(
        group: &str,
        namespaces: &[NamespaceId],
        policies: &BTreeMap<NamespaceId, NamespacePolicy>,
    ) -> Result<(), OpError> {
        for namespace in namespaces {
            let Some(policy) = policies.get(namespace) else {
                return Err(OpError::NamespaceUnknown {
                    namespace: namespace.clone(),
                });
            };
            if !policy.persist_to_git {
                return Err(OpError::NamespacePolicyViolation {
                    namespace: namespace.clone(),
                    rule: "persist_to_git".to_string(),
                    reason: Some(format!(
                        "checkpoint group `{group}` publishes to git but namespace `{}` has persist_to_git=false",
                        namespace.as_str()
                    )),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn checkpoint_group_snapshots(
        &self,
        store_id: StoreId,
    ) -> Vec<CheckpointGroupSnapshot> {
        self.checkpoint_scheduler.snapshot_for_store(store_id)
    }

    pub(crate) fn force_checkpoint_for_namespace(
        &mut self,
        store_id: StoreId,
        namespace: &NamespaceId,
    ) -> Vec<String> {
        let groups = self
            .checkpoint_scheduler
            .force_checkpoint_for_namespace(store_id, namespace);
        self.emit_checkpoint_queue_depth();
        groups
    }

    pub(crate) fn force_checkpoint_group(&mut self, store_id: StoreId, group: &str) -> bool {
        let scheduled = self
            .checkpoint_scheduler
            .force_checkpoint_group(store_id, group);
        self.emit_checkpoint_queue_depth();
        scheduled
    }

    /// Reload checkpoint groups from config and re-register with scheduler.
    pub(crate) fn reload_checkpoint_groups(&mut self, repo_path: &Path) -> Result<usize, OpError> {
        let config = crate::config::load_for_repo(Some(repo_path)).map_err(|e| {
            OpError::ValidationFailed {
                field: "checkpoint_groups".into(),
                reason: format!("failed to reload config: {e}"),
            }
        })?;
        let runtime = crate::config::daemon_runtime_from_config(&config);

        let old_count = self.checkpoint_groups.len();
        self.checkpoint_groups = runtime.checkpoint_groups;
        let new_count = self.checkpoint_groups.len();

        tracing::info!(
            old_count = old_count,
            new_count = new_count,
            groups = ?self.checkpoint_groups.keys().collect::<Vec<_>>(),
            "reloaded checkpoint groups from config"
        );

        // Re-register checkpoint groups for all loaded stores
        let store_ids: Vec<StoreId> = self.store_sessions.keys().copied().collect();
        for store_id in store_ids {
            self.register_default_checkpoint_groups(store_id)?;
        }

        Ok(new_count)
    }

    pub(in crate::runtime) fn complete_checkpoint(
        &mut self,
        session: StoreSessionToken,
        checkpoint_group: &str,
        result: Result<CheckpointPublishOutcome, CheckpointPublishError>,
    ) {
        if !self.session_matches(session) {
            return;
        }
        let store_id = session.store_id();
        let key = CheckpointGroupKey {
            store_id,
            group: checkpoint_group.to_string(),
        };
        match &result {
            Ok(outcome) => {
                tracing::info!(
                    store_id = %store_id,
                    checkpoint_group = checkpoint_group,
                    checkpoint_id = %outcome.checkpoint_id,
                    "checkpoint publish succeeded"
                );
                if let Some(session) = self.store_session_by_id_mut(store_id) {
                    session
                        .runtime_mut()
                        .commit_checkpoint_dirty_shards(checkpoint_group);
                }
                self.checkpoint_scheduler.complete_success(
                    &key,
                    Instant::now(),
                    self.clock.wall_ms(),
                );
            }
            Err(err) => {
                tracing::warn!(
                    store_id = %store_id,
                    checkpoint_group = checkpoint_group,
                    error = ?err,
                    "checkpoint publish failed"
                );
                if let Some(session) = self.store_session_by_id_mut(store_id) {
                    session
                        .runtime_mut()
                        .rollback_checkpoint_dirty_shards(checkpoint_group);
                }
                self.checkpoint_scheduler
                    .complete_failure(&key, Instant::now());
            }
        }
        self.emit_checkpoint_queue_depth();
    }

    pub(in crate::runtime) fn fire_due_checkpoints(&mut self, git_tx: &Sender<GitOp>) {
        let now = Instant::now();
        let due = self.checkpoint_scheduler.drain_due(now);
        for key in due {
            self.start_checkpoint_job(&key, git_tx, now);
        }
    }

    fn start_checkpoint_job(
        &mut self,
        key: &CheckpointGroupKey,
        git_tx: &Sender<GitOp>,
        now: Instant,
    ) {
        let config = match self.checkpoint_scheduler.group_config(key).cloned() {
            Some(config) => config,
            None => return,
        };
        if !config.auto_push() {
            return;
        }

        let checkpoint_groups = self
            .checkpoint_scheduler
            .checkpoint_groups_for_store(key.store_id);

        let (session_token, snapshot, repo_path) = {
            let Some(session) = self.store_sessions.get_mut(&key.store_id) else {
                tracing::warn!(
                    store_id = %key.store_id,
                    checkpoint_group = %config.group,
                    "checkpoint store session missing"
                );
                self.checkpoint_scheduler.complete_failure(key, now);
                self.emit_checkpoint_queue_depth();
                return;
            };
            let Some(path) = session.lane().any_valid_path().cloned() else {
                tracing::warn!(
                    store_id = %key.store_id,
                    checkpoint_group = %config.group,
                    "checkpoint repo path missing"
                );
                self.checkpoint_scheduler.complete_failure(key, now);
                self.emit_checkpoint_queue_depth();
                return;
            };
            let store = session.runtime_mut();
            let created_at_ms = WallClock::now().0;
            let snapshot =
                match store.checkpoint_snapshot(&config.group, &config.namespaces, created_at_ms) {
                    Ok(snapshot) => snapshot,
                    Err(err) => {
                        tracing::warn!(
                            store_id = %key.store_id,
                            checkpoint_group = %config.group,
                            error = ?err,
                            "checkpoint snapshot failed"
                        );
                        self.checkpoint_scheduler.complete_failure(key, now);
                        self.emit_checkpoint_queue_depth();
                        return;
                    }
                };
            (session.token(), snapshot, path)
        };

        if git_tx
            .send(GitOp::Checkpoint {
                repo: repo_path,
                session: session_token,
                store_id: key.store_id,
                checkpoint_group: config.group.clone(),
                git_ref: config.git_ref.clone(),
                snapshot,
                checkpoint_groups,
            })
            .is_ok()
        {
            self.checkpoint_scheduler.start_in_flight(key, now);
            self.emit_checkpoint_queue_depth();
        } else {
            tracing::warn!(
                store_id = %key.store_id,
                checkpoint_group = %config.group,
                "checkpoint git worker not responding"
            );
            if let Some(session) = self.store_sessions.get_mut(&key.store_id) {
                session
                    .runtime_mut()
                    .rollback_checkpoint_dirty_shards(&config.group);
            }
            self.checkpoint_scheduler.complete_failure(key, now);
            self.emit_checkpoint_queue_depth();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policies_with_extra(
        namespace: NamespaceId,
        policy: NamespacePolicy,
    ) -> BTreeMap<NamespaceId, NamespacePolicy> {
        let mut policies = BTreeMap::new();
        policies.insert(NamespaceId::core(), NamespacePolicy::core_default());
        policies.insert(namespace, policy);
        policies
    }

    #[test]
    fn checkpoint_group_allows_multi_namespace_persisting_group() {
        let extmsg = NamespaceId::parse("extmsg").unwrap();
        let policies = policies_with_extra(extmsg.clone(), NamespacePolicy::core_default());

        Daemon::validate_checkpoint_group_namespaces(
            "durable",
            &[NamespaceId::core(), extmsg],
            &policies,
        )
        .expect("persisting namespaces are valid");
    }

    #[test]
    fn checkpoint_group_rejects_non_persisting_namespace() {
        let sessions = NamespaceId::parse("sessions").unwrap();
        let policies = policies_with_extra(sessions.clone(), NamespacePolicy::wf_default());

        let err = Daemon::validate_checkpoint_group_namespaces(
            "sessions",
            &[NamespaceId::core(), sessions.clone()],
            &policies,
        )
        .expect_err("non-persisting namespace must be rejected");

        assert!(matches!(
            err,
            OpError::NamespacePolicyViolation {
                namespace,
                rule,
                reason: Some(reason),
            } if namespace == sessions
                && rule == "persist_to_git"
                && reason.contains("checkpoint group `sessions`")
        ));
    }
}
