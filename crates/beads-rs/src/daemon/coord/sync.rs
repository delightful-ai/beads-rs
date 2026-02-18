use super::*;

use beads_core::Crdt;

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
    pub(in crate::daemon) fn ensure_repo_fresh(
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
    pub(in crate::daemon) fn complete_refresh(
        &mut self,
        remote: &RemoteUrl,
        result: Result<LoadResult, SyncError>,
    ) {
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
                        .map(|message| beads_daemon::git_lane::FetchErrorRecord {
                            message,
                            wall_ms: now_wall_ms,
                        });
                repo_state.last_divergence =
                    fresh
                        .divergence
                        .map(|divergence| beads_daemon::git_lane::DivergenceRecord {
                            local_oid: divergence.local_oid.to_string(),
                            remote_oid: divergence.remote_oid.to_string(),
                            wall_ms: now_wall_ms,
                        });
                repo_state.last_force_push =
                    fresh
                        .force_push
                        .map(|force_push| beads_daemon::git_lane::ForcePushRecord {
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
    pub(in crate::daemon) fn force_reload(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedStore<'_>, OpError> {
        let resolved = self.resolve_store(repo)?;

        // Remove cached state so ensure_repo_loaded will do a fresh load
        self.drop_store_state(resolved.store_id());

        // Now load fresh from git
        self.ensure_repo_loaded_strict(repo, git_tx)
    }

    /// Maybe start a background sync for a repo.
    ///
    /// Only starts if:
    /// - Repo is dirty
    /// - Not already syncing
    pub(in crate::daemon) fn maybe_start_sync(
        &mut self,
        remote: &RemoteUrl,
        git_tx: &Sender<GitOp>,
    ) {
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
    pub(in crate::daemon) fn complete_sync(
        &mut self,
        remote: &RemoteUrl,
        result: Result<SyncOutcome, SyncError>,
    ) {
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
                        .map(|divergence| beads_daemon::git_lane::DivergenceRecord {
                            local_oid: divergence.local_oid.to_string(),
                            remote_oid: divergence.remote_oid.to_string(),
                            wall_ms: now_wall_ms,
                        });
                repo_state.last_force_push =
                    outcome
                        .force_push
                        .map(|force_push| beads_daemon::git_lane::ForcePushRecord {
                            previous_remote_oid: force_push.previous_remote_oid.to_string(),
                            remote_oid: force_push.remote_oid.to_string(),
                            wall_ms: now_wall_ms,
                        });

                // If mutations happened during sync, merge them
                if repo_state.dirty {
                    let local_state = store.state.core().clone();
                    let merged = synced_state.join(&local_state);

                    let mut next_state = store.state.clone();
                    next_state.set_core_state(merged);
                    store.state = next_state;
                    repo_state.complete_sync(wall_ms);
                    // Still dirty from mutations during sync - reschedule
                    repo_state.dirty = true;
                    reschedule_sync = true;
                    sync_succeeded = true;
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

    pub(in crate::daemon) fn next_sync_deadline(&mut self) -> Option<Instant> {
        self.scheduler_mut().next_deadline()
    }

    pub(in crate::daemon) fn next_checkpoint_deadline(&mut self) -> Option<Instant> {
        self.checkpoint_scheduler_mut().next_deadline()
    }
}
