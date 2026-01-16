//! Coordinator logic: request dispatch, read gating, and scheduling.

use std::path::Path;
use std::time::{Duration, Instant};

use crossbeam::channel::Sender;

use super::core::{
    Daemon, HandleOutcome, LoadedStore, NormalizedReadConsistency, ParsedMutationMeta,
    ReadGateStatus, detect_clock_skew, max_write_stamp,
};
use super::git_worker::{GitOp, LoadResult};
use super::ipc::{MutationMeta, ReadConsistency, Request};
use super::ops::OpError;
use super::remote::RemoteUrl;
use crate::core::{
    ActorId, CanonicalState, ClientRequestId, DurabilityClass, NamespaceId, Stamp, WriteStamp,
    WallClock,
};
use crate::git::{SyncError, SyncOutcome};
use crate::git::collision::{detect_collisions, resolve_collisions};

impl Daemon {
    /// Get the next scheduled sync deadline for a remote, if any.
    pub(crate) fn next_sync_deadline_for(&self, remote: &RemoteUrl) -> Option<Instant> {
        self.scheduler.deadline_for(remote)
    }

    pub(crate) fn schedule_sync(&mut self, remote: RemoteUrl) {
        self.scheduler.schedule(remote);
    }

    /// Ensure repo is loaded and reasonably fresh from remote.
    ///
    /// For clean (non-dirty) repos, we periodically kick off a background refresh
    /// so read-only commands observe updates pushed by other machines or daemons.
    /// This is non-blocking - it returns cached state immediately and applies
    /// fresh state when the background load completes.
    ///
    /// Returns a `LoadedStore` proof that can be used for infallible state access.
    pub fn ensure_repo_fresh(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedStore, OpError> {
        let loaded = self.ensure_repo_loaded(repo, git_tx)?;

        let repo_state = self.git_lane_state(&loaded)?;
        let needs_refresh = !repo_state.dirty
            && !repo_state.sync_in_progress
            && !repo_state.refresh_in_progress
            && repo_state
                .last_refresh
                .map(|t| t.elapsed() >= super::core::REFRESH_TTL)
                .unwrap_or(true);

        if needs_refresh {
            // Get a valid path for the refresh operation
            let path = repo_state.any_valid_path().cloned();

            if let Some(refresh_path) = path {
                // Mark refresh in progress before sending to avoid races
                let repo_state = self.git_lane_state_mut(&loaded)?;
                repo_state.refresh_in_progress = true;

                // Kick off background refresh - don't wait for result
                let _ = git_tx.send(GitOp::Refresh {
                    repo: refresh_path,
                    remote: loaded.remote().clone(),
                });
            }
        }

        // Return immediately with cached state
        Ok(loaded)
    }

    /// Complete a background refresh operation.
    ///
    /// Called when git thread reports refresh result. Applies fresh state
    /// if refresh succeeded, otherwise just clears the in-progress flag.
    pub fn complete_refresh(
        &mut self,
        remote: &RemoteUrl,
        result: Result<LoadResult, SyncError>,
    ) {
        let store_id = match self.store_caches.remote_to_store_id.get(remote).copied() {
            Some(id) => id,
            None => return,
        };
        let (stores, git_lanes) = (&mut self.stores, &mut self.git_lanes);
        let store = match stores.get_mut(&store_id) {
            Some(store) => store,
            None => return,
        };
        let repo_state = match git_lanes.get_mut(&store_id) {
            Some(repo_state) => repo_state,
            None => return,
        };

        repo_state.refresh_in_progress = false;

        match result {
            Ok(fresh) => {
                // Advance clock to account for remote stamps
                if let Some(max_stamp) = fresh.last_seen_stamp.as_ref() {
                    self.clock.receive(max_stamp);
                }

                // Only apply refresh if repo is still clean (no mutations happened
                // during the refresh). If dirty, we'll sync soon anyway.
                if !repo_state.dirty {
                    store
                        .state
                        .set_namespace_state(NamespaceId::core(), fresh.state);
                    // Update root_slug if remote has one and we don't
                    if fresh.root_slug.is_some() {
                        repo_state.root_slug = fresh.root_slug;
                    }
                }
                repo_state.last_refresh = Some(Instant::now());

                repo_state.last_seen_stamp =
                    max_write_stamp(repo_state.last_seen_stamp.clone(), fresh.last_seen_stamp);

                let now_wall_ms = WallClock::now().0;
                repo_state.last_clock_skew = repo_state
                    .last_seen_stamp
                    .as_ref()
                    .and_then(|stamp| detect_clock_skew(now_wall_ms, stamp.wall_ms));
                repo_state.last_fetch_error = fresh.fetch_error.map(|message| {
                    super::git_lane::FetchErrorRecord {
                        message,
                        wall_ms: now_wall_ms,
                    }
                });
                repo_state.last_divergence = fresh.divergence.map(|divergence| {
                    super::git_lane::DivergenceRecord {
                        local_oid: divergence.local_oid.to_string(),
                        remote_oid: divergence.remote_oid.to_string(),
                        wall_ms: now_wall_ms,
                    }
                });
                repo_state.last_force_push = fresh.force_push.map(|force_push| {
                    super::git_lane::ForcePushRecord {
                        previous_remote_oid: force_push.previous_remote_oid.to_string(),
                        remote_oid: force_push.remote_oid.to_string(),
                        wall_ms: now_wall_ms,
                    }
                });

                // If local/WAL has changes that remote doesn't (crash recovery),
                // mark dirty so sync will push those changes.
                if fresh.needs_sync && !repo_state.dirty {
                    repo_state.mark_dirty();
                    self.scheduler.schedule(remote.clone());
                }
            }
            Err(e) => {
                // Refresh failed - log and continue with cached state.
                // Next TTL hit will retry.
                tracing::debug!("background refresh failed for {:?}: {:?}", remote, e);
            }
        }
    }

    /// Force reload state from git, invalidating any cached state.
    ///
    /// Use this after external changes to refs/heads/beads/store (e.g., migration).
    /// This is a blocking operation that fetches fresh state from git.
    pub fn force_reload(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedStore, OpError> {
        let resolved = self.store_caches.resolve_store(repo)?;

        // Remove cached state so ensure_repo_loaded will do a fresh load
        self.stores.remove(&resolved.store_id);
        self.git_lanes.remove(&resolved.store_id);

        // Now load fresh from git
        self.ensure_repo_loaded_strict(repo, git_tx)
    }

    /// Maybe start a background sync for a repo.
    ///
    /// Only starts if:
    /// - Repo is dirty
    /// - Not already syncing
    pub fn maybe_start_sync(&mut self, remote: &RemoteUrl, git_tx: &Sender<GitOp>) {
        let store_id = match self.store_caches.remote_to_store_id.get(remote).copied() {
            Some(id) => id,
            None => return,
        };
        let (stores, git_lanes) = (&mut self.stores, &mut self.git_lanes);
        let store = match stores.get_mut(&store_id) {
            Some(store) => store,
            None => return,
        };
        let repo_state = match git_lanes.get_mut(&store_id) {
            Some(repo_state) => repo_state,
            None => return,
        };

        if !repo_state.dirty || repo_state.sync_in_progress {
            return;
        }

        let path = match repo_state.any_valid_path() {
            Some(p) => p.clone(),
            None => return,
        };

        repo_state.start_sync();

        let _ = git_tx.send(GitOp::Sync {
            repo: path,
            remote: remote.clone(),
            state: store.state.get_or_default(&NamespaceId::core()),
            actor: self.actor.clone(),
        });
    }

    pub(crate) fn ensure_loaded_and_maybe_start_sync(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedStore, OpError> {
        let loaded = self.ensure_repo_loaded(repo, git_tx)?;
        self.maybe_start_sync(loaded.remote(), git_tx);
        Ok(loaded)
    }

    /// Complete a sync operation.
    ///
    /// Called when git thread reports sync result.
    pub fn complete_sync(&mut self, remote: &RemoteUrl, result: Result<SyncOutcome, SyncError>) {
        let mut backoff_ms = None;
        let mut sync_succeeded = false;
        let store_id = match self.store_caches.remote_to_store_id.get(remote).copied() {
            Some(id) => id,
            None => return,
        };

        match result {
            Ok(outcome) => {
                let synced_state = outcome.state;
                // Advance clock to account for remote stamps
                if let Some(max_stamp) = outcome.last_seen_stamp.as_ref() {
                    self.clock.receive(max_stamp);
                }
                let resolution_stamp = {
                    let write_stamp = self.clock_mut().tick();
                    Stamp {
                        at: write_stamp,
                        by: self.actor.clone(),
                    }
                };

                let (stores, git_lanes) = (&mut self.stores, &mut self.git_lanes);
                let store = match stores.get_mut(&store_id) {
                    Some(store) => store,
                    None => return,
                };
                let repo_state = match git_lanes.get_mut(&store_id) {
                    Some(repo_state) => repo_state,
                    None => return,
                };

                repo_state.last_seen_stamp =
                    max_write_stamp(repo_state.last_seen_stamp.clone(), outcome.last_seen_stamp);
                let now_wall_ms = WallClock::now().0;
                repo_state.last_clock_skew = repo_state
                    .last_seen_stamp
                    .as_ref()
                    .and_then(|stamp| detect_clock_skew(now_wall_ms, stamp.wall_ms));
                repo_state.last_divergence = outcome.divergence.map(|divergence| {
                    super::git_lane::DivergenceRecord {
                        local_oid: divergence.local_oid.to_string(),
                        remote_oid: divergence.remote_oid.to_string(),
                        wall_ms: now_wall_ms,
                    }
                });
                repo_state.last_force_push = outcome.force_push.map(|force_push| {
                    super::git_lane::ForcePushRecord {
                        previous_remote_oid: force_push.previous_remote_oid.to_string(),
                        remote_oid: force_push.remote_oid.to_string(),
                        wall_ms: now_wall_ms,
                    }
                });

                // If mutations happened during sync, merge them
                if repo_state.dirty {
                    let local_state = store.state.get_or_default(&NamespaceId::core());
                    let merged = match CanonicalState::join(&synced_state, &local_state) {
                        Ok(merged) => Ok(merged),
                        Err(mut errs) => {
                            let collisions = detect_collisions(&local_state, &synced_state);
                            if collisions.is_empty() {
                                Err(errs)
                            } else {
                                match resolve_collisions(
                                    &local_state,
                                    &synced_state,
                                    &collisions,
                                    resolution_stamp,
                                ) {
                                    Ok((local_resolved, remote_resolved)) => {
                                        CanonicalState::join(&remote_resolved, &local_resolved)
                                    }
                                    Err(err) => {
                                        errs.push(err);
                                        Err(errs)
                                    }
                                }
                            }
                        }
                    };

                    match merged {
                        Ok(merged) => {
                            let mut next_state = store.state.clone();
                            next_state.set_namespace_state(NamespaceId::core(), merged);
                            store.state = next_state;
                            repo_state.complete_sync(self.clock.wall_ms());
                            // Still dirty from mutations during sync - reschedule
                            repo_state.dirty = true;
                            self.scheduler.schedule(remote.clone());
                            sync_succeeded = true;
                        }
                        Err(errs) => {
                            tracing::error!(
                                "merge after sync failed for {:?}: {:?}; preserving local state",
                                remote,
                                errs
                            );
                            repo_state.fail_sync();
                            backoff_ms = Some(repo_state.backoff_ms());
                        }
                    }
                } else {
                    // No mutations during sync - just take synced state
                    let mut next_state = store.state.clone();
                    next_state.set_namespace_state(NamespaceId::core(), synced_state);
                    store.state = next_state;
                    repo_state.complete_sync(self.clock.wall_ms());

                    sync_succeeded = true;
                }
            }
            Err(e) => {
                tracing::error!("sync failed for {:?}: {:?}", remote, e);
                let repo_state = match self.git_lanes.get_mut(&store_id) {
                    Some(repo_state) => repo_state,
                    None => return,
                };
                repo_state.fail_sync();
                backoff_ms = Some(repo_state.backoff_ms());
            }
        }

        if let Some(backoff) = backoff_ms {
            let backoff = Duration::from_millis(backoff);
            self.scheduler.schedule_after(remote.clone(), backoff);
        }

        // Export Go-compatible JSONL after successful sync
        if sync_succeeded {
            self.export_go_compat(store_id, remote);
        }
    }

    pub fn next_sync_deadline(&mut self) -> Option<Instant> {
        self.scheduler.next_deadline()
    }

    pub fn next_checkpoint_deadline(&mut self) -> Option<Instant> {
        self.checkpoint_scheduler.next_deadline()
    }

    pub(crate) fn parse_mutation_meta(
        &self,
        proof: &LoadedStore,
        meta: MutationMeta,
    ) -> Result<ParsedMutationMeta, OpError> {
        let namespace = self.normalize_namespace(proof, meta.namespace)?;
        let durability = parse_durability_meta(meta.durability)?;
        let client_request_id = parse_optional_client_request_id(meta.client_request_id)?;
        let actor_id = parse_optional_actor_id(meta.actor_id, &self.actor)?;

        Ok(ParsedMutationMeta {
            namespace,
            durability,
            client_request_id,
            actor_id,
        })
    }

    pub(crate) fn normalize_read_consistency(
        &self,
        proof: &LoadedStore,
        read: ReadConsistency,
    ) -> Result<NormalizedReadConsistency, OpError> {
        let namespace = self.normalize_namespace(proof, read.namespace)?;
        Ok(NormalizedReadConsistency {
            namespace,
            require_min_seen: read.require_min_seen,
            wait_timeout_ms: read.wait_timeout_ms.unwrap_or(0),
        })
    }

    pub(crate) fn check_read_gate(
        &self,
        proof: &LoadedStore,
        read: &NormalizedReadConsistency,
    ) -> Result<(), OpError> {
        match self.read_gate_status(proof, read)? {
            ReadGateStatus::Satisfied => Ok(()),
            ReadGateStatus::Unsatisfied {
                required,
                current_applied,
            } => {
                if read.wait_timeout_ms() > 0 {
                    return Err(OpError::RequireMinSeenTimeout {
                        waited_ms: read.wait_timeout_ms(),
                        required: Box::new(required),
                        current_applied: Box::new(current_applied),
                    });
                }
                Err(OpError::RequireMinSeenUnsatisfied {
                    required: Box::new(required),
                    current_applied: Box::new(current_applied),
                })
            }
        }
    }

    pub(crate) fn read_gate_status(
        &self,
        proof: &LoadedStore,
        read: &NormalizedReadConsistency,
    ) -> Result<ReadGateStatus, OpError> {
        let Some(required) = read.require_min_seen() else {
            return Ok(ReadGateStatus::Satisfied);
        };
        let current_applied = self.store_runtime(proof)?.watermarks_applied.clone();
        if current_applied.satisfies_at_least(required) {
            return Ok(ReadGateStatus::Satisfied);
        }
        Ok(ReadGateStatus::Unsatisfied {
            required: required.clone(),
            current_applied,
        })
    }

    pub(crate) fn normalize_namespace(
        &self,
        proof: &LoadedStore,
        raw: Option<String>,
    ) -> Result<NamespaceId, OpError> {
        let namespace = match raw {
            None => NamespaceId::core(),
            Some(value) => {
                let trimmed = value.trim();
                NamespaceId::parse(trimmed.to_string()).map_err(|err| {
                    OpError::NamespaceInvalid {
                        namespace: trimmed.to_string(),
                        reason: err.to_string(),
                    }
                })?
            }
        };
        let store = self.store_runtime(proof)?;
        if store.policies.contains_key(&namespace) {
            Ok(namespace)
        } else {
            Err(OpError::NamespaceUnknown { namespace })
        }
    }

    /// Handle a request from IPC.
    ///
    /// Dispatches to appropriate handler based on request type.
    pub(crate) fn handle_request(&mut self, req: Request, git_tx: &Sender<GitOp>) -> HandleOutcome {
        match req {
            // Mutations - delegate to executor module
            Request::Create {
                repo,
                id,
                parent,
                title,
                bead_type,
                priority,
                description,
                design,
                acceptance_criteria,
                assignee,
                external_ref,
                estimated_minutes,
                labels,
                dependencies,
                meta,
            } => self.apply_create(
                &repo,
                meta,
                id,
                parent,
                title,
                bead_type,
                priority,
                description,
                design,
                acceptance_criteria,
                assignee,
                external_ref,
                estimated_minutes,
                labels,
                dependencies,
                git_tx,
            ),

            Request::Update {
                repo,
                id,
                patch,
                cas,
                meta,
            } => {
                self.apply_update(&repo, meta, id, patch, cas, git_tx)
            }

            Request::AddLabels {
                repo,
                id,
                labels,
                meta,
            } => {
                self.apply_add_labels(&repo, meta, id, labels, git_tx)
            }

            Request::RemoveLabels {
                repo,
                id,
                labels,
                meta,
            } => {
                self.apply_remove_labels(&repo, meta, id, labels, git_tx)
            }

            Request::SetParent {
                repo,
                id,
                parent,
                meta,
            } => {
                self.apply_set_parent(&repo, meta, id, parent, git_tx)
            }

            Request::Close {
                repo,
                id,
                reason,
                on_branch,
                meta,
            } => {
                self.apply_close(&repo, meta, id, reason, on_branch, git_tx)
            }

            Request::Reopen { repo, id, meta } => {
                self.apply_reopen(&repo, meta, id, git_tx)
            }

            Request::Delete {
                repo,
                id,
                reason,
                meta,
            } => {
                self.apply_delete(&repo, meta, id, reason, git_tx)
            }

            Request::AddDep {
                repo,
                from,
                to,
                kind,
                meta,
            } => {
                self.apply_add_dep(&repo, meta, from, to, kind, git_tx)
            }

            Request::RemoveDep {
                repo,
                from,
                to,
                kind,
                meta,
            } => {
                self.apply_remove_dep(&repo, meta, from, to, kind, git_tx)
            }

            Request::AddNote {
                repo,
                id,
                note,
                meta,
            } => {
                self.apply_add_note(&repo, meta, id, note, git_tx)
            }

            Request::SetNote {
                repo,
                id,
                note_id,
                note,
                meta,
            } => {
                self.apply_set_note(&repo, meta, id, note_id, note, git_tx)
            }

            Request::SetClaim { repo, id, claim, meta } => {
                self.apply_set_claim(&repo, meta, id, claim, git_tx)
            }

            Request::Ping { .. } => HandleOutcome::Response(super::ipc::Response::ok(
                super::ipc::ResponsePayload::pong(),
            )),

            Request::SyncWait { repo } => {
                HandleOutcome::SyncWait { repo }
            }

            Request::Status { repo, read } => self.query_status(&repo, read, git_tx),

            Request::Show { repo, id, read } => self.query_show(&repo, &id, read, git_tx),

            Request::ShowMultiple { repo, ids, read } => {
                self.query_show_multiple(&repo, &ids, read, git_tx)
            }

            Request::List { repo, filters, read } => {
                self.query_list(&repo, &filters, read, git_tx)
            }

            Request::Ready { repo, read } => self.query_ready(&repo, read, git_tx),

            Request::Blocked { repo, read } => self.query_blocked(&repo, read, git_tx),

            Request::Stale {
                repo,
                days,
                status,
                limit,
                read,
            } => {
                self.query_stale(&repo, days, status.as_deref(), limit, read, git_tx)
            }

            Request::Deps { repo, id, read } => self.query_deps(&repo, &id, read, git_tx),

            Request::DepsForBead { repo, id, read } => {
                self.query_deps_for_bead(&repo, &id, read, git_tx)
            }

            Request::DepsForTree { repo, id, read } => {
                self.query_deps_for_tree(&repo, &id, read, git_tx)
            }

            Request::Notes { repo, id, read } => self.query_notes(&repo, &id, read, git_tx),

            Request::Count {
                repo,
                filters,
                group_by,
                read,
            } => {
                self.query_count(&repo, &filters, group_by.as_deref(), read, git_tx)
            }

            Request::Deleted {
                repo,
                since_ms,
                id,
                read,
            } => {
                self.query_deleted(&repo, since_ms, id.as_ref(), read, git_tx)
            }

            Request::EpicStatus {
                repo,
                eligible_only,
                read,
            } => {
                self.query_epic_status(&repo, eligible_only, read, git_tx)
            }

            Request::Validate { repo, read } => self.query_validate(&repo, read, git_tx),

            Request::DepCycles { repo, read } => self.query_dep_cycles(&repo, read, git_tx),

            Request::DurabilityWait { repo, read, wait } => {
                HandleOutcome::DurabilityWait { repo, read, wait }
            }

            Request::AdminStatus { repo } => self.admin_status(&repo, git_tx),

            Request::AdminDoctor { repo } => self.admin_doctor(&repo, git_tx),

            Request::AdminMetrics { repo } => self.admin_metrics(&repo, git_tx),

            Request::AdminFingerprint { repo, limit } => {
                self.admin_fingerprint(&repo, limit, git_tx)
            }

            Request::AdminMaintenance { repo, enable, reason, ttl_ms } => {
                self.admin_maintenance(&repo, enable, reason, ttl_ms, git_tx)
            }

            Request::AdminRebuildIndex { repo } => {
                self.admin_rebuild_index(&repo, git_tx)
            }

            Request::AdminReloadPolicies { repo } => {
                self.admin_reload_policies(&repo, git_tx)
            }

            Request::AdminFlush { repo } => self.admin_flush(&repo, git_tx),

            Request::AdminScrub { repo } => self.admin_scrub(&repo, git_tx),

            Request::CheckpointStatus { repo } => self.checkpoint_status(&repo, git_tx),

            Request::CheckpointList { repo } => self.checkpoint_list(&repo, git_tx),

            Request::CheckpointExport { repo } => self.checkpoint_export(&repo, git_tx),

            Request::CheckpointImport { repo } => self.checkpoint_import(&repo, git_tx),

            Request::CheckpointImportPath { repo, path } => {
                self.checkpoint_import_path(&repo, path, git_tx)
            }

            Request::CheckpointImportLatest { repo } => {
                self.checkpoint_import_latest(&repo, git_tx)
            }

            Request::CheckpointClearCache { repo } => {
                self.checkpoint_clear_cache(&repo, git_tx)
            }

            Request::CheckpointPublish { repo } => self.checkpoint_publish(&repo, git_tx),

            Request::CheckpointPublishPath { repo, path, ref_name } => {
                self.checkpoint_publish_path(&repo, path, ref_name, git_tx)
            }

            Request::CheckpointRestore { repo, group } => {
                self.checkpoint_restore(&repo, group.as_deref(), git_tx)
            }

            Request::CheckpointSnapshot { repo, group, path } => {
                self.checkpoint_snapshot(&repo, group.as_deref(), path, git_tx)
            }

            Request::CheckpointGarbageCollect { repo } => {
                self.checkpoint_garbage_collect(&repo, git_tx)
            }

            Request::CheckpointDelete { repo, group, id } => {
                self.checkpoint_delete(&repo, group.as_deref(), id.as_deref(), git_tx)
            }

            Request::CheckpointDiff {
                repo,
                group,
                left,
                right,
            } => {
                self.checkpoint_diff(&repo, group.as_deref(), left.as_deref(), right.as_deref(), git_tx)
            }

            Request::AdminFsck { repo } => self.admin_fsck(&repo, git_tx),

            Request::AdminUnlock { store_id, force } => {
                self.admin_unlock(store_id, force)
            }

            Request::AdminSetClock { repo, stamp } => {
                self.admin_set_clock(&repo, stamp, git_tx)
            }

            Request::AdminSetStoreMeta { repo, store_meta } => {
                self.admin_set_store_meta(&repo, store_meta, git_tx)
            }

            Request::AdminReloadRoster { repo } => {
                self.admin_reload_roster(&repo, git_tx)
            }

            Request::Subscribe { repo, since, read } => {
                self.subscribe(&repo, since, read, git_tx)
            }

            Request::Unsubscribe { repo, token } => {
                self.unsubscribe(&repo, token, git_tx)
            }
        }
    }
}

fn parse_optional_client_request_id(
    raw: Option<String>,
) -> Result<Option<ClientRequestId>, OpError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(OpError::InvalidRequest {
            field: Some("client_request_id".into()),
            reason: "client_request_id cannot be empty".into(),
        });
    }
    ClientRequestId::parse_str(trimmed)
        .map(Some)
        .map_err(|err| OpError::InvalidRequest {
            field: Some("client_request_id".into()),
            reason: err.to_string(),
        })
}

fn parse_optional_actor_id(raw: Option<String>, fallback: &ActorId) -> Result<ActorId, OpError> {
    let Some(raw) = raw else {
        return Ok(fallback.clone());
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(OpError::InvalidRequest {
            field: Some("actor_id".into()),
            reason: "actor_id cannot be empty".into(),
        });
    }
    ActorId::new(trimmed.to_string()).map_err(|err| OpError::InvalidRequest {
        field: Some("actor_id".into()),
        reason: err.to_string(),
    })
}

fn parse_durability_meta(raw: Option<String>) -> Result<DurabilityClass, OpError> {
    DurabilityClass::parse_optional(raw.as_deref()).map_err(|err| OpError::InvalidRequest {
        field: Some("durability".into()),
        reason: err.to_string(),
    })
}
