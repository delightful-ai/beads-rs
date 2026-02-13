use super::*;

impl Daemon {
    /// Ensure repo is loaded using cached refs, without blocking on network fetch.
    ///
    /// If no local refs exist, this will attempt a one-time fetch with a bounded timeout.
    pub(in crate::daemon) fn ensure_repo_loaded(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedStore<'_>, OpError> {
        let resolved = self.store_caches.resolve_store(repo)?;
        let store_id = resolved.store_id();
        let remote = resolved.remote;
        self.store_caches
            .path_to_remote
            .insert(repo.to_owned(), remote.clone());

        if !self.stores.contains_key(&store_id) {
            let open = StoreRuntime::open(
                store_id,
                remote.clone(),
                WallClock::now().0,
                env!("CARGO_PKG_VERSION"),
                self.limits(),
                &self.namespace_defaults,
            )?;
            let runtime = open.runtime;
            self.seed_actor_clocks(&runtime)?;
            self.stores.insert(store_id, runtime);
            self.git_lanes.insert(store_id, GitLaneState::new());
            self.register_default_checkpoint_groups(store_id)?;

            let timeout = load_timeout();
            let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
            git_tx
                .send(GitOp::LoadLocal {
                    repo: repo.to_owned(),
                    respond: respond_tx,
                })
                .map_err(|_| OpError::Internal("git thread not responding"))?;

            match respond_rx.recv_timeout(timeout) {
                Ok(Ok(loaded)) => {
                    self.apply_loaded_repo_state(store_id, &remote, repo, loaded)?;
                }
                Ok(Err(SyncError::NoLocalRef(_))) => {
                    // No cached refs; attempt a bounded fetch to discover remote state.
                    let (fetch_tx, fetch_rx) = crossbeam::channel::bounded(1);
                    git_tx
                        .send(GitOp::Load {
                            repo: repo.to_owned(),
                            respond: fetch_tx,
                        })
                        .map_err(|_| OpError::Internal("git thread not responding"))?;

                    match fetch_rx.recv_timeout(timeout) {
                        Ok(Ok(loaded)) => {
                            self.apply_loaded_repo_state(store_id, &remote, repo, loaded)?;
                        }
                        Ok(Err(SyncError::NoLocalRef(_))) => {
                            return Err(OpError::RepoNotInitialized(repo.to_owned()));
                        }
                        Ok(Err(e)) => return Err(OpError::from(e)),
                        Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                            return Err(OpError::LoadTimeout {
                                repo: repo.to_owned(),
                                timeout_secs: timeout.as_secs(),
                                remote: remote.as_str().to_string(),
                            });
                        }
                        Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                            return Err(OpError::Internal("git thread died"));
                        }
                    }
                }
                Ok(Err(e)) => return Err(OpError::from(e)),
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                    return Err(OpError::Internal("git thread load-local timed out"));
                }
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                    return Err(OpError::Internal("git thread died"));
                }
            }
        } else if let Some(store) = self.stores.get_mut(&store_id) {
            if let Some(repo_state) = self.git_lanes.get_mut(&store_id) {
                repo_state.register_path(repo.to_owned());
            } else {
                let repo_state = GitLaneState::with_path(None, repo.to_owned());
                self.git_lanes.insert(store_id, repo_state);
            }
            if store.primary_remote != remote {
                store.primary_remote = remote.clone();
            }
            self.export_go_compat(store_id, &remote);
        }

        Ok(self.loaded_store(store_id, remote))
    }

    /// Ensure repo is loaded, fetching from git if needed.
    ///
    /// This is a blocking operation - sends Load to git thread and waits with a bounded
    /// timeout for the initial fetch. Returns a `LoadedStore` proof for state access.
    pub(in crate::daemon) fn ensure_repo_loaded_strict(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedStore<'_>, OpError> {
        let resolved = self.store_caches.resolve_store(repo)?;
        let store_id = resolved.store_id();
        let remote = resolved.remote;
        self.store_caches
            .path_to_remote
            .insert(repo.to_owned(), remote.clone());

        if !self.stores.contains_key(&store_id) {
            let open = StoreRuntime::open(
                store_id,
                remote.clone(),
                WallClock::now().0,
                env!("CARGO_PKG_VERSION"),
                self.limits(),
                &self.namespace_defaults,
            )?;
            let runtime = open.runtime;
            self.seed_actor_clocks(&runtime)?;
            self.stores.insert(store_id, runtime);
            self.git_lanes.insert(store_id, GitLaneState::new());
            self.register_default_checkpoint_groups(store_id)?;

            // Blocking load from git (fetches remote first in GitWorker).
            let timeout = load_timeout();
            let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
            git_tx
                .send(GitOp::Load {
                    repo: repo.to_owned(),
                    respond: respond_tx,
                })
                .map_err(|_| OpError::Internal("git thread not responding"))?;

            match respond_rx.recv_timeout(timeout) {
                Ok(Ok(loaded)) => {
                    self.apply_loaded_repo_state(store_id, &remote, repo, loaded)?;
                }
                Ok(Err(SyncError::NoLocalRef(_))) => {
                    return Err(OpError::RepoNotInitialized(repo.to_owned()));
                }
                Ok(Err(e)) => {
                    return Err(OpError::from(e));
                }
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                    return Err(OpError::LoadTimeout {
                        repo: repo.to_owned(),
                        timeout_secs: timeout.as_secs(),
                        remote: remote.as_str().to_string(),
                    });
                }
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                    return Err(OpError::Internal("git thread died"));
                }
            }
        } else if let Some(store) = self.stores.get_mut(&store_id) {
            if let Some(repo_state) = self.git_lanes.get_mut(&store_id) {
                repo_state.register_path(repo.to_owned());
            } else {
                let repo_state = GitLaneState::with_path(None, repo.to_owned());
                self.git_lanes.insert(store_id, repo_state);
            }
            if store.primary_remote != remote {
                store.primary_remote = remote.clone();
            }

            // Update symlinks for newly registered clone path
            self.export_go_compat(store_id, &remote);
        }

        Ok(self.loaded_store(store_id, remote))
    }

    pub(super) fn apply_loaded_repo_state(
        &mut self,
        store_id: StoreId,
        remote: &RemoteUrl,
        repo: &Path,
        loaded: LoadResult,
    ) -> Result<(), OpError> {
        let mut last_seen_stamp = loaded.last_seen_stamp;
        if let Some(max_stamp) = last_seen_stamp.as_ref() {
            self.clock.receive(max_stamp);
        }
        let mut needs_sync = loaded.needs_sync;
        let mut state = store_state_from_legacy(loaded.state);
        let root_slug = loaded.root_slug;
        let checkpoint_imports = self.load_checkpoint_imports(store_id, repo);
        for import in &checkpoint_imports {
            match merge_store_states(&state, &import.state) {
                Ok(merged) => state = merged,
                Err(CheckpointImportError::Merge(errors)) => {
                    tracing::warn!(
                        store_id = %store_id,
                        errors = ?errors,
                        "checkpoint merge failed"
                    );
                    return Err(OpError::Internal("checkpoint merge failed"));
                }
                Err(err) => {
                    tracing::warn!(store_id = %store_id, error = ?err, "checkpoint merge failed");
                    return Err(OpError::Internal("checkpoint merge failed"));
                }
            }
        }

        let replayed_event_wal = {
            let store = self
                .stores
                .get(&store_id)
                .expect("loaded store missing from state");
            replay_event_wal(
                store_id,
                store.wal_index.as_ref(),
                &mut state,
                self.limits(),
            )?
        };
        if replayed_event_wal {
            needs_sync = true;
        }

        if !checkpoint_imports.is_empty() {
            let store = self
                .stores
                .get_mut(&store_id)
                .expect("loaded store missing from state");
            apply_checkpoint_watermarks(store, &checkpoint_imports)?;
        }

        last_seen_stamp = max_write_stamp(last_seen_stamp, state.max_write_stamp());
        if let Some(max_stamp) = last_seen_stamp.as_ref() {
            self.clock.receive(max_stamp);
        }

        let now_wall_ms = WallClock::now().0;
        let clock_skew = last_seen_stamp
            .as_ref()
            .and_then(|stamp| detect_clock_skew(now_wall_ms, stamp.wall_ms));

        let mut repo_state = GitLaneState::with_path(root_slug, repo.to_owned());
        repo_state.last_seen_stamp = last_seen_stamp;
        repo_state.last_clock_skew = clock_skew;
        repo_state.last_fetch_error = loaded.fetch_error.map(|message| FetchErrorRecord {
            message,
            wall_ms: now_wall_ms,
        });
        repo_state.last_divergence = loaded.divergence.map(|divergence| DivergenceRecord {
            local_oid: divergence.local_oid.to_string(),
            remote_oid: divergence.remote_oid.to_string(),
            wall_ms: now_wall_ms,
        });
        repo_state.last_force_push = loaded.force_push.map(|force_push| ForcePushRecord {
            previous_remote_oid: force_push.previous_remote_oid.to_string(),
            remote_oid: force_push.remote_oid.to_string(),
            wall_ms: now_wall_ms,
        });

        // If local/WAL has changes that remote doesn't (crash recovery),
        // mark dirty so sync will push those changes.
        if needs_sync {
            repo_state.mark_dirty();
            self.scheduler.schedule(remote.clone());
        }

        let store = self
            .stores
            .get_mut(&store_id)
            .expect("loaded store missing from state");
        store.state = state;
        self.git_lanes.insert(store_id, repo_state);
        if store.primary_remote != *remote {
            store.primary_remote = remote.clone();
        }

        // Initial Go-compat export for newly loaded repo
        self.export_go_compat(store_id, remote);

        if let Err(err) = self.ensure_replication_runtime(store_id) {
            tracing::warn!("replication runtime init failed for {store_id}: {err}");
        }
        Ok(())
    }
}
