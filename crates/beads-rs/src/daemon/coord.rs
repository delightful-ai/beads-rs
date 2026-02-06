//! Coordinator logic: request dispatch, read gating, and scheduling.

use std::path::Path;
use std::time::{Duration, Instant};

use crossbeam::channel::Sender;
use uuid::Uuid;

use super::core::{
    Daemon, HandleOutcome, LoadedStore, NormalizedReadConsistency, ParsedMutationMeta,
    ReadGateStatus, detect_clock_skew, max_write_stamp,
};
use super::git_worker::{GitOp, LoadResult};
use super::ipc::{
    ErrorPayload, MutationMeta, ReadConsistency, Request, Response, ResponseExt, ResponsePayload,
};
use super::ops::OpError;
use super::remote::RemoteUrl;
use crate::api::DaemonInfo as ApiDaemonInfo;
use crate::api::QueryResult;
use crate::core::{
    ActorId, CanonicalState, CliErrorCode, DurabilityClass, ErrorCode, NamespaceId,
    ProtocolErrorCode, TraceId, WallClock,
};
use crate::git::{SyncError, SyncOutcome};

const REFRESH_TTL: Duration = Duration::from_millis(1000);

impl Daemon {
    /// Get the next scheduled sync deadline for a remote, if any.
    pub(crate) fn next_sync_deadline_for(&self, remote: &RemoteUrl) -> Option<Instant> {
        self.scheduler().deadline_for(remote)
    }

    pub(crate) fn schedule_sync(&mut self, remote: RemoteUrl) {
        if !self.git_sync_policy().allows_sync() {
            return;
        }
        self.scheduler_mut().schedule(remote);
    }

    pub(crate) fn schedule_sync_after(&mut self, remote: RemoteUrl, delay: Duration) {
        if !self.git_sync_policy().allows_sync() {
            return;
        }
        self.scheduler_mut().schedule_after(remote, delay);
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
    ) -> Result<LoadedStore<'_>, OpError> {
        let mut loaded = self.ensure_repo_loaded(repo, git_tx)?;

        let repo_state = loaded.lane();
        let needs_refresh = !repo_state.dirty
            && !repo_state.sync_in_progress
            && !repo_state.refresh_in_progress
            && repo_state
                .last_refresh
                .map(|t| t.elapsed() >= REFRESH_TTL)
                .unwrap_or(true);

        if needs_refresh {
            // Get a valid path for the refresh operation
            let path = repo_state.any_valid_path().cloned();

            if let Some(refresh_path) = path {
                // Mark refresh in progress before sending to avoid races
                let repo_state = loaded.lane_mut();
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
    pub fn complete_refresh(&mut self, remote: &RemoteUrl, result: Result<LoadResult, SyncError>) {
        let store_id = match self.store_id_for_remote(remote) {
            Some(id) => id,
            None => return,
        };
        let mut schedule_sync = false;

        match result {
            Ok(fresh) => {
                // Advance clock to account for remote stamps
                if let Some(max_stamp) = fresh.last_seen_stamp.as_ref() {
                    self.clock_mut().receive(max_stamp);
                }

                let Some((store, repo_state)) = self.store_and_lane_by_id_mut(store_id) else {
                    return;
                };
                repo_state.refresh_in_progress = false;

                // Only apply refresh if repo is still clean (no mutations happened
                // during the refresh). If dirty, we'll sync soon anyway.
                if !repo_state.dirty {
                    store.state.set_core_state(fresh.state);
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
                repo_state.last_fetch_error =
                    fresh
                        .fetch_error
                        .map(|message| super::git_lane::FetchErrorRecord {
                            message,
                            wall_ms: now_wall_ms,
                        });
                repo_state.last_divergence =
                    fresh
                        .divergence
                        .map(|divergence| super::git_lane::DivergenceRecord {
                            local_oid: divergence.local_oid.to_string(),
                            remote_oid: divergence.remote_oid.to_string(),
                            wall_ms: now_wall_ms,
                        });
                repo_state.last_force_push =
                    fresh
                        .force_push
                        .map(|force_push| super::git_lane::ForcePushRecord {
                            previous_remote_oid: force_push.previous_remote_oid.to_string(),
                            remote_oid: force_push.remote_oid.to_string(),
                            wall_ms: now_wall_ms,
                        });

                // If local/WAL has changes that remote doesn't (crash recovery),
                // mark dirty so sync will push those changes.
                if fresh.needs_sync && !repo_state.dirty {
                    repo_state.mark_dirty();
                    schedule_sync = true;
                }
            }
            Err(e) => {
                if let Some((_, repo_state)) = self.store_and_lane_by_id_mut(store_id) {
                    repo_state.refresh_in_progress = false;
                }
                // Refresh failed - log and continue with cached state.
                // Next TTL hit will retry.
                tracing::debug!("background refresh failed for {:?}: {:?}", remote, e);
            }
        }

        if schedule_sync {
            self.schedule_sync(remote.clone());
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
    ) -> Result<LoadedStore<'_>, OpError> {
        let resolved = self.resolve_store(repo)?;

        // Remove cached state so ensure_repo_loaded will do a fresh load
        self.drop_store_state(resolved.store_id);

        // Now load fresh from git
        self.ensure_repo_loaded_strict(repo, git_tx)
    }

    /// Maybe start a background sync for a repo.
    ///
    /// Only starts if:
    /// - Repo is dirty
    /// - Not already syncing
    pub fn maybe_start_sync(&mut self, remote: &RemoteUrl, git_tx: &Sender<GitOp>) {
        let git_sync_policy = self.git_sync_policy();
        if !git_sync_policy.allows_sync() {
            return;
        }
        let store_id = match self.store_id_for_remote(remote) {
            Some(id) => id,
            None => return,
        };
        let actor = self.actor().clone();
        let remote = remote.clone();
        let Some(mut loaded) = self.try_loaded_store(store_id, remote) else {
            return;
        };
        loaded.maybe_start_sync(git_sync_policy, actor, git_tx);
    }

    pub(crate) fn ensure_loaded_and_maybe_start_sync(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedStore<'_>, OpError> {
        let actor = self.actor().clone();
        let git_sync_policy = self.git_sync_policy();
        let mut loaded = self.ensure_repo_loaded(repo, git_tx)?;
        loaded.maybe_start_sync(git_sync_policy, actor, git_tx);
        Ok(loaded)
    }

    /// Complete a sync operation.
    ///
    /// Called when git thread reports sync result.
    pub fn complete_sync(&mut self, remote: &RemoteUrl, result: Result<SyncOutcome, SyncError>) {
        let mut backoff_ms = None;
        let mut sync_succeeded = false;
        let store_id = match self.store_id_for_remote(remote) {
            Some(id) => id,
            None => return,
        };
        let mut reschedule_sync = false;

        match result {
            Ok(outcome) => {
                let synced_state = outcome.state;
                // Advance clock to account for remote stamps
                if let Some(max_stamp) = outcome.last_seen_stamp.as_ref() {
                    self.clock_mut().receive(max_stamp);
                }
                let wall_ms = self.clock().wall_ms();

                let Some((store, repo_state)) = self.store_and_lane_by_id_mut(store_id) else {
                    return;
                };

                repo_state.last_seen_stamp =
                    max_write_stamp(repo_state.last_seen_stamp.clone(), outcome.last_seen_stamp);
                let now_wall_ms = WallClock::now().0;
                repo_state.last_clock_skew = repo_state
                    .last_seen_stamp
                    .as_ref()
                    .and_then(|stamp| detect_clock_skew(now_wall_ms, stamp.wall_ms));
                repo_state.last_divergence =
                    outcome
                        .divergence
                        .map(|divergence| super::git_lane::DivergenceRecord {
                            local_oid: divergence.local_oid.to_string(),
                            remote_oid: divergence.remote_oid.to_string(),
                            wall_ms: now_wall_ms,
                        });
                repo_state.last_force_push =
                    outcome
                        .force_push
                        .map(|force_push| super::git_lane::ForcePushRecord {
                            previous_remote_oid: force_push.previous_remote_oid.to_string(),
                            remote_oid: force_push.remote_oid.to_string(),
                            wall_ms: now_wall_ms,
                        });

                // If mutations happened during sync, merge them
                if repo_state.dirty {
                    let local_state = store.state.core().clone();
                    let merged = CanonicalState::join(&synced_state, &local_state);

                    match merged {
                        Ok(merged) => {
                            let mut next_state = store.state.clone();
                            next_state.set_core_state(merged);
                            store.state = next_state;
                            repo_state.complete_sync(wall_ms);
                            // Still dirty from mutations during sync - reschedule
                            repo_state.dirty = true;
                            reschedule_sync = true;
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
                    next_state.set_core_state(synced_state);
                    store.state = next_state;
                    repo_state.complete_sync(wall_ms);

                    sync_succeeded = true;
                }
            }
            Err(e) => {
                tracing::error!("sync failed for {:?}: {:?}", remote, e);
                let Some((_, repo_state)) = self.store_and_lane_by_id_mut(store_id) else {
                    return;
                };
                repo_state.fail_sync();
                backoff_ms = Some(repo_state.backoff_ms());
            }
        }

        if reschedule_sync {
            self.schedule_sync(remote.clone());
        }

        if let Some(backoff) = backoff_ms {
            let backoff = Duration::from_millis(backoff);
            self.schedule_sync_after(remote.clone(), backoff);
        }

        // Export Go-compatible JSONL after successful sync
        if sync_succeeded {
            self.export_go_compat(store_id, remote);
        }
    }

    pub fn next_sync_deadline(&mut self) -> Option<Instant> {
        self.scheduler_mut().next_deadline()
    }

    pub fn next_checkpoint_deadline(&mut self) -> Option<Instant> {
        self.checkpoint_scheduler_mut().next_deadline()
    }

    /// Handle a request from IPC.
    ///
    /// Dispatches to appropriate handler based on request type.
    pub(crate) fn handle_request(&mut self, req: Request, git_tx: &Sender<GitOp>) -> HandleOutcome {
        match req {
            // Mutations - delegate to executor module
            Request::Create { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_create(&repo, meta, payload, git_tx)
            }

            Request::Update { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_update(&repo, meta, payload, git_tx)
            }

            Request::AddLabels { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_add_labels(&repo, meta, payload, git_tx)
            }

            Request::RemoveLabels { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_remove_labels(&repo, meta, payload, git_tx)
            }

            Request::SetParent { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_set_parent(&repo, meta, payload, git_tx)
            }

            Request::Close { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_close(&repo, meta, payload, git_tx)
            }

            Request::Reopen { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_reopen(&repo, meta, payload, git_tx)
            }

            Request::Delete { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_delete(&repo, meta, payload, git_tx)
            }

            Request::AddDep { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_add_dep(&repo, meta, payload, git_tx)
            }

            Request::RemoveDep { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_remove_dep(&repo, meta, payload, git_tx)
            }

            Request::AddNote { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_add_note(&repo, meta, payload, git_tx)
            }

            Request::Claim { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_claim(&repo, meta, payload, git_tx)
            }

            Request::Unclaim { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_unclaim(&repo, meta, payload, git_tx)
            }

            Request::ExtendClaim { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_extend_claim(&repo, meta, payload, git_tx)
            }

            // Queries - delegate to query_executor module
            Request::Show { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_show(&repo, &payload.id, read, git_tx).into()
            }

            Request::ShowMultiple { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_show_multiple(&repo, &payload.ids, read, git_tx)
                    .into()
            }

            Request::List { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_list(&repo, &payload.filters, read, git_tx)
                    .into()
            }

            Request::Ready { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_ready(&repo, payload.limit, read, git_tx).into()
            }

            Request::DepTree { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_dep_tree(&repo, &payload.id, read, git_tx).into()
            }

            Request::DepCycles { ctx, .. } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_dep_cycles(&repo, read, git_tx).into()
            }

            Request::Deps { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_deps(&repo, &payload.id, read, git_tx).into()
            }

            Request::Notes { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_notes(&repo, &payload.id, read, git_tx).into()
            }

            Request::Blocked { ctx, .. } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_blocked(&repo, read, git_tx).into()
            }

            Request::Stale { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_stale(
                    &repo,
                    payload.days,
                    payload.status.as_deref(),
                    payload.limit,
                    read,
                    git_tx,
                )
                .into()
            }

            Request::Count { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_count(
                    &repo,
                    &payload.filters,
                    payload.group_by.as_deref(),
                    read,
                    git_tx,
                )
                .into()
            }

            Request::Deleted { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_deleted(&repo, payload.since_ms, payload.id.as_ref(), read, git_tx)
                    .into()
            }

            Request::EpicStatus { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_epic_status(&repo, payload.eligible_only, read, git_tx)
                    .into()
            }

            Request::Status { ctx, .. } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_status(&repo, read, git_tx).into()
            }

            Request::AdminStatus { ctx, .. } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.admin_status(&repo, read, git_tx).into()
            }

            Request::AdminMetrics { ctx, .. } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.admin_metrics(&repo, read, git_tx).into()
            }

            Request::AdminDoctor { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.admin_doctor(
                    &repo,
                    read,
                    payload.max_records_per_namespace,
                    payload.verify_checkpoint_cache,
                    git_tx,
                )
                .into()
            }

            Request::AdminScrub { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.admin_scrub_now(
                    &repo,
                    read,
                    payload.max_records_per_namespace,
                    payload.verify_checkpoint_cache,
                    git_tx,
                )
                .into()
            }

            Request::AdminFlush { ctx, payload } => self
                .admin_flush(&ctx.path, payload.namespace, payload.checkpoint_now, git_tx)
                .into(),

            Request::AdminCheckpointWait { .. } => {
                unreachable!("AdminCheckpointWait is handled by the daemon state loop")
            }

            Request::AdminFingerprint { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.admin_fingerprint(&repo, read, payload.mode, payload.sample, git_tx)
                    .into()
            }

            Request::AdminReloadPolicies { ctx, .. } => {
                self.admin_reload_policies(&ctx.path, git_tx).into()
            }

            Request::AdminReloadLimits { ctx, .. } => {
                self.admin_reload_limits(&ctx.path, git_tx).into()
            }

            Request::AdminReloadReplication { ctx, .. } => {
                self.admin_reload_replication(&ctx.path, git_tx).into()
            }

            Request::AdminRotateReplicaId { ctx, .. } => {
                self.admin_rotate_replica_id(&ctx.path, git_tx).into()
            }

            Request::AdminMaintenanceMode { ctx, payload } => self
                .admin_maintenance_mode(&ctx.path, payload.enabled, git_tx)
                .into(),

            Request::AdminRebuildIndex { ctx, .. } => {
                self.admin_rebuild_index(&ctx.path, git_tx).into()
            }

            Request::Validate { ctx, .. } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_validate(&repo, read, git_tx).into()
            }

            Request::Subscribe { .. } => Response::err_from(error_payload(
                ProtocolErrorCode::InvalidRequest.into(),
                "subscribe must be handled by the streaming IPC path",
                false,
            ))
            .into(),

            // Control
            Request::Refresh { ctx, .. } => {
                // Force reload from git (invalidates cached state).
                // Used after external changes like migration.
                match self.force_reload(&ctx.path, git_tx) {
                    Ok(_) => Response::ok(ResponsePayload::refreshed()),
                    Err(e) => Response::err_from(e),
                }
                .into()
            }

            Request::Sync { ctx, .. } => {
                // Force immediate sync (used for graceful shutdown)
                match self.ensure_loaded_and_maybe_start_sync(&ctx.path, git_tx) {
                    Ok(_) => Response::ok(ResponsePayload::synced()),
                    Err(e) => Response::err_from(e),
                }
                .into()
            }

            Request::SyncWait { .. } => {
                unreachable!("SyncWait is handled by the daemon state loop")
            }

            Request::Init { ctx, .. } => {
                let repo = ctx.path;
                let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
                if git_tx
                    .send(GitOp::Init {
                        repo: repo.clone(),
                        respond: respond_tx,
                    })
                    .is_err()
                {
                    return Response::err_from(error_payload(
                        CliErrorCode::Internal.into(),
                        "git thread not responding",
                        false,
                    ))
                    .into();
                }

                match respond_rx.recv() {
                    Ok(Ok(())) => match self.ensure_repo_loaded(&repo, git_tx) {
                        Ok(_) => Response::ok(ResponsePayload::initialized()),
                        Err(e) => Response::err_from(e),
                    },
                    Ok(Err(e)) => Response::err_from(error_payload(
                        CliErrorCode::InitFailed.into(),
                        &e.to_string(),
                        false,
                    )),
                    Err(_) => Response::err_from(error_payload(
                        CliErrorCode::Internal.into(),
                        "git thread died",
                        false,
                    )),
                }
                .into()
            }

            Request::Ping => Response::ok(ResponsePayload::query(QueryResult::DaemonInfo(
                ApiDaemonInfo {
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    protocol_version: super::ipc::IPC_PROTOCOL_VERSION,
                    pid: std::process::id(),
                },
            )))
            .into(),

            Request::Shutdown => {
                self.begin_shutdown();
                Response::ok(ResponsePayload::shutting_down()).into()
            }
        }
    }
}

impl LoadedStore<'_> {
    pub(crate) fn parse_mutation_meta(
        &self,
        meta: MutationMeta,
        actor: &ActorId,
    ) -> Result<ParsedMutationMeta, OpError> {
        let namespace = self.normalize_namespace(meta.namespace)?;
        let durability = meta.durability.unwrap_or(DurabilityClass::LocalFsync);
        let client_request_id = meta.client_request_id;
        let trace_id = client_request_id
            .map(TraceId::from)
            .unwrap_or_else(|| TraceId::new(Uuid::new_v4()));
        let actor_id = meta.actor_id.unwrap_or_else(|| actor.clone());

        Ok(ParsedMutationMeta {
            namespace,
            durability,
            client_request_id,
            trace_id,
            actor_id,
        })
    }

    pub(crate) fn normalize_read_consistency(
        &self,
        read: ReadConsistency,
    ) -> Result<NormalizedReadConsistency, OpError> {
        let namespace = self.normalize_namespace(read.namespace)?;
        Ok(NormalizedReadConsistency::new(
            namespace,
            read.require_min_seen,
            read.wait_timeout_ms.unwrap_or(0),
        ))
    }

    pub(crate) fn check_read_gate(&self, read: &NormalizedReadConsistency) -> Result<(), OpError> {
        match self.read_gate_status(read)? {
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
        read: &NormalizedReadConsistency,
    ) -> Result<ReadGateStatus, OpError> {
        let Some(required) = read.require_min_seen() else {
            return Ok(ReadGateStatus::Satisfied);
        };
        let current_applied = self.runtime().watermarks_applied.clone();
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
        raw: Option<NamespaceId>,
    ) -> Result<NamespaceId, OpError> {
        let namespace = raw.unwrap_or_else(NamespaceId::core);
        if self.runtime().policies.contains_key(&namespace) {
            Ok(namespace)
        } else {
            Err(OpError::NamespaceUnknown { namespace })
        }
    }

    pub(crate) fn maybe_start_sync(
        &mut self,
        git_sync_policy: super::core::GitSyncPolicy,
        actor: ActorId,
        git_tx: &Sender<GitOp>,
    ) {
        if !git_sync_policy.allows_sync() {
            return;
        }
        let repo_state = self.lane_mut();
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
            remote: self.remote().clone(),
            store_id: self.store_id(),
            state: self.runtime().state.core().clone(),
            actor,
        });
    }
}

fn error_payload(code: ErrorCode, message: &str, retryable: bool) -> ErrorPayload {
    ErrorPayload::new(code, message, retryable)
}
