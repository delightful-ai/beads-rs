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
    ErrorPayload, IpcError, MutationMeta, ReadConsistency, Request, Response, ResponsePayload,
};
use super::ops::OpError;
use super::remote::RemoteUrl;
use crate::api::DaemonInfo as ApiDaemonInfo;
use crate::api::QueryResult;
use crate::core::{
    ActorId, BeadId, CanonicalState, CliErrorCode, ClientRequestId, CoreError, DurabilityClass,
    ErrorCode, NamespaceId, ProtocolErrorCode, TraceId, WallClock,
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
    ) -> Result<LoadedStore, OpError> {
        let loaded = self.ensure_repo_loaded(repo, git_tx)?;

        let repo_state = self.git_lane_state(&loaded)?;
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
    ) -> Result<LoadedStore, OpError> {
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
        if !self.git_sync_policy().allows_sync() {
            return;
        }
        let store_id = match self.store_id_for_remote(remote) {
            Some(id) => id,
            None => return,
        };
        let actor = self.actor().clone();
        let Some((store, repo_state)) = self.store_and_lane_by_id_mut(store_id) else {
            return;
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
            store_id,
            state: store.state.get_or_default(&NamespaceId::core()),
            actor,
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
                    let local_state = store.state.get_or_default(&NamespaceId::core());
                    let merged = CanonicalState::join(&synced_state, &local_state);

                    match merged {
                        Ok(merged) => {
                            let mut next_state = store.state.clone();
                            next_state.set_namespace_state(NamespaceId::core(), merged);
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
                    next_state.set_namespace_state(NamespaceId::core(), synced_state);
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

    pub(crate) fn parse_mutation_meta(
        &self,
        proof: &LoadedStore,
        meta: MutationMeta,
    ) -> Result<ParsedMutationMeta, OpError> {
        let namespace = self.normalize_namespace(proof, meta.namespace)?;
        let durability = parse_durability_meta(meta.durability)?;
        let client_request_id = parse_optional_client_request_id(meta.client_request_id)?;
        let trace_id = client_request_id
            .map(TraceId::from)
            .unwrap_or_else(|| TraceId::new(Uuid::new_v4()));
        let actor_id = parse_optional_actor_id(meta.actor_id, self.actor())?;

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
        proof: &LoadedStore,
        read: ReadConsistency,
    ) -> Result<NormalizedReadConsistency, OpError> {
        let namespace = self.normalize_namespace(proof, read.namespace)?;
        Ok(NormalizedReadConsistency::new(
            namespace,
            read.require_min_seen,
            read.wait_timeout_ms.unwrap_or(0),
        ))
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
            } => self.apply_update(&repo, meta, id, patch, cas, git_tx),

            Request::AddLabels {
                repo,
                id,
                labels,
                meta,
            } => self.apply_add_labels(&repo, meta, id, labels, git_tx),

            Request::RemoveLabels {
                repo,
                id,
                labels,
                meta,
            } => self.apply_remove_labels(&repo, meta, id, labels, git_tx),

            Request::SetParent {
                repo,
                id,
                parent,
                meta,
            } => self.apply_set_parent(&repo, meta, id, parent, git_tx),

            Request::Close {
                repo,
                id,
                reason,
                on_branch,
                meta,
            } => self.apply_close(&repo, meta, id, reason, on_branch, git_tx),

            Request::Reopen { repo, id, meta } => self.apply_reopen(&repo, meta, id, git_tx),

            Request::Delete {
                repo,
                id,
                reason,
                meta,
            } => self.apply_delete(&repo, meta, id, reason, git_tx),

            Request::AddDep {
                repo,
                from,
                to,
                kind,
                meta,
            } => self.apply_add_dep(&repo, meta, from, to, kind, git_tx),

            Request::RemoveDep {
                repo,
                from,
                to,
                kind,
                meta,
            } => self.apply_remove_dep(&repo, meta, from, to, kind, git_tx),

            Request::AddNote {
                repo,
                id,
                content,
                meta,
            } => self.apply_add_note(&repo, meta, id, content, git_tx),

            Request::Claim {
                repo,
                id,
                lease_secs,
                meta,
            } => self.apply_claim(&repo, meta, id, lease_secs, git_tx),

            Request::Unclaim { repo, id, meta } => self.apply_unclaim(&repo, meta, id, git_tx),

            Request::ExtendClaim {
                repo,
                id,
                lease_secs,
                meta,
            } => self.apply_extend_claim(&repo, meta, id, lease_secs, git_tx),

            // Queries - delegate to query_executor module
            Request::Show { repo, id, read } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.query_show(&repo, &id, read, git_tx).into()
            }

            Request::ShowMultiple { repo, ids, read } => {
                self.query_show_multiple(&repo, &ids, read, git_tx).into()
            }

            Request::List {
                repo,
                filters,
                read,
            } => self.query_list(&repo, &filters, read, git_tx).into(),

            Request::Ready { repo, limit, read } => {
                self.query_ready(&repo, limit, read, git_tx).into()
            }

            Request::DepTree { repo, id, read } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.query_dep_tree(&repo, &id, read, git_tx).into()
            }

            Request::DepCycles { repo, read } => self.query_dep_cycles(&repo, read, git_tx).into(),

            Request::Deps { repo, id, read } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.query_deps(&repo, &id, read, git_tx).into()
            }

            Request::Notes { repo, id, read } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.query_notes(&repo, &id, read, git_tx).into()
            }

            Request::Blocked { repo, read } => self.query_blocked(&repo, read, git_tx).into(),

            Request::Stale {
                repo,
                days,
                status,
                limit,
                read,
            } => self
                .query_stale(&repo, days, status.as_deref(), limit, read, git_tx)
                .into(),

            Request::Count {
                repo,
                filters,
                group_by,
                read,
            } => self
                .query_count(&repo, &filters, group_by.as_deref(), read, git_tx)
                .into(),

            Request::Deleted {
                repo,
                since_ms,
                id,
                read,
            } => {
                let id = match id {
                    Some(s) => Some(match BeadId::parse(&s) {
                        Ok(id) => id,
                        Err(e) => {
                            return Response::err(invalid_id_payload(e)).into();
                        }
                    }),
                    None => None,
                };
                self.query_deleted(&repo, since_ms, id.as_ref(), read, git_tx)
                    .into()
            }

            Request::EpicStatus {
                repo,
                eligible_only,
                read,
            } => self
                .query_epic_status(&repo, eligible_only, read, git_tx)
                .into(),

            Request::Status { repo, read } => self.query_status(&repo, read, git_tx).into(),

            Request::AdminStatus { repo, read } => self.admin_status(&repo, read, git_tx).into(),

            Request::AdminMetrics { repo, read } => self.admin_metrics(&repo, read, git_tx).into(),

            Request::AdminDoctor {
                repo,
                read,
                max_records_per_namespace,
                verify_checkpoint_cache,
            } => self
                .admin_doctor(
                    &repo,
                    read,
                    max_records_per_namespace,
                    verify_checkpoint_cache,
                    git_tx,
                )
                .into(),

            Request::AdminScrub {
                repo,
                read,
                max_records_per_namespace,
                verify_checkpoint_cache,
            } => self
                .admin_scrub_now(
                    &repo,
                    read,
                    max_records_per_namespace,
                    verify_checkpoint_cache,
                    git_tx,
                )
                .into(),

            Request::AdminFlush {
                repo,
                namespace,
                checkpoint_now,
            } => self
                .admin_flush(&repo, namespace, checkpoint_now, git_tx)
                .into(),

            Request::AdminCheckpointWait { .. } => {
                unreachable!("AdminCheckpointWait is handled by the daemon state loop")
            }

            Request::AdminFingerprint {
                repo,
                read,
                mode,
                sample,
            } => self
                .admin_fingerprint(&repo, read, mode, sample, git_tx)
                .into(),

            Request::AdminReloadPolicies { repo } => {
                self.admin_reload_policies(&repo, git_tx).into()
            }

            Request::AdminReloadReplication { repo } => {
                self.admin_reload_replication(&repo, git_tx).into()
            }

            Request::AdminRotateReplicaId { repo } => {
                self.admin_rotate_replica_id(&repo, git_tx).into()
            }

            Request::AdminMaintenanceMode { repo, enabled } => {
                self.admin_maintenance_mode(&repo, enabled, git_tx).into()
            }

            Request::AdminRebuildIndex { repo } => self.admin_rebuild_index(&repo, git_tx).into(),

            Request::Validate { repo, read } => self.query_validate(&repo, read, git_tx).into(),

            Request::Subscribe { .. } => Response::err(error_payload(
                ProtocolErrorCode::InvalidRequest.into(),
                "subscribe must be handled by the streaming IPC path",
                false,
            ))
            .into(),

            // Control
            Request::Refresh { repo } => {
                // Force reload from git (invalidates cached state).
                // Used after external changes like migration.
                match self.force_reload(&repo, git_tx) {
                    Ok(_) => Response::ok(ResponsePayload::refreshed()),
                    Err(e) => Response::err(e),
                }
                .into()
            }

            Request::Sync { repo } => {
                // Force immediate sync (used for graceful shutdown)
                match self.ensure_loaded_and_maybe_start_sync(&repo, git_tx) {
                    Ok(_) => Response::ok(ResponsePayload::synced()),
                    Err(e) => Response::err(e),
                }
                .into()
            }

            Request::SyncWait { .. } => {
                unreachable!("SyncWait is handled by the daemon state loop")
            }

            Request::Init { repo } => {
                let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
                if git_tx
                    .send(GitOp::Init {
                        repo: repo.clone(),
                        respond: respond_tx,
                    })
                    .is_err()
                {
                    return Response::err(error_payload(
                        CliErrorCode::Internal.into(),
                        "git thread not responding",
                        false,
                    ))
                    .into();
                }

                match respond_rx.recv() {
                    Ok(Ok(())) => match self.ensure_repo_loaded(&repo, git_tx) {
                        Ok(_) => Response::ok(ResponsePayload::initialized()),
                        Err(e) => Response::err(e),
                    },
                    Ok(Err(e)) => Response::err(error_payload(
                        CliErrorCode::InitFailed.into(),
                        &e.to_string(),
                        false,
                    )),
                    Err(_) => Response::err(error_payload(
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

fn error_payload(code: ErrorCode, message: &str, retryable: bool) -> ErrorPayload {
    ErrorPayload::new(code, message, retryable)
}

fn invalid_id_payload(err: CoreError) -> ErrorPayload {
    match err {
        CoreError::InvalidId(id) => IpcError::from(id).into(),
        other => ErrorPayload::new(
            ProtocolErrorCode::InternalError.into(),
            other.to_string(),
            false,
        ),
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
