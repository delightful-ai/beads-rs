//! Daemon core - the central coordinator.
//!
//! Owns all per-repo state, the HLC clock, actor identity, and sync scheduler.
//! The serialization point for all mutations - runs on a single thread.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crossbeam::channel::Sender;
use git2::Repository;

use super::Clock;
use super::git_worker::{GitOp, LoadResult};
use super::ipc::{ErrorPayload, IpcError, Request, Response, ResponsePayload};
use super::ops::OpError;
use super::query::QueryResult;
use super::remote::{RemoteUrl, normalize_url};
use super::repo::{
    ClockSkewRecord, DivergenceRecord, FetchErrorRecord, ForcePushRecord, RepoState,
};
use super::scheduler::SyncScheduler;
use super::wal::{Wal, WalEntry};

use crate::compat::{ExportContext, ensure_symlinks, export_jsonl};

/// Proof that a repo is loaded. Only created by `Daemon::ensure_repo_loaded`,
/// `Daemon::ensure_repo_loaded_strict`, or `Daemon::ensure_repo_fresh`.
///
/// This type signals that a repo should exist in the daemon's state; accessors
/// still return Internal if the invariant is violated.
#[derive(Debug, Clone)]
pub struct LoadedRemote(RemoteUrl);

impl LoadedRemote {
    /// Get the underlying remote URL.
    pub fn remote(&self) -> &RemoteUrl {
        &self.0
    }
}
use crate::api::DaemonInfo as ApiDaemonInfo;
use crate::core::{
    ActorId, BeadId, CanonicalState, CoreError, ErrorCode, Stamp, WallClock, WriteStamp,
};
use crate::git::SyncError;
use crate::git::collision::{detect_collisions, resolve_collisions};
use crate::git::sync::SyncOutcome;

const REFRESH_TTL: Duration = Duration::from_millis(1000);
const LOAD_TIMEOUT_SECS: u64 = 30;

/// The daemon coordinator.
///
/// Owns all state and coordinates between IPC, state mutations, and git sync.
pub struct Daemon {
    /// Per-remote state, keyed by normalized remote URL.
    repos: BTreeMap<RemoteUrl, RepoState>,

    /// Cache of repo path → remote URL.
    path_to_remote: HashMap<PathBuf, RemoteUrl>,

    /// HLC clock for generating timestamps.
    clock: Clock,

    /// Actor identity (username@hostname).
    actor: ActorId,

    /// Sync scheduler for debouncing.
    scheduler: SyncScheduler,

    /// Write-ahead log for mutation durability.
    wal: Wal,

    /// Go-compatibility export context.
    export_ctx: Option<ExportContext>,
}

impl Daemon {
    /// Create a new daemon.
    pub fn new(actor: ActorId, wal: Wal) -> Self {
        // Initialize Go-compat export context (best effort - don't fail daemon startup)
        let export_ctx = match ExportContext::new() {
            Ok(ctx) => Some(ctx),
            Err(e) => {
                tracing::warn!("Failed to initialize Go-compat export: {}", e);
                None
            }
        };

        Daemon {
            repos: BTreeMap::new(),
            path_to_remote: HashMap::new(),
            clock: Clock::new(),
            actor,
            scheduler: SyncScheduler::new(),
            wal,
            export_ctx,
        }
    }

    /// Get a reference to the WAL.
    pub fn wal(&self) -> &Wal {
        &self.wal
    }

    /// Get the actor identity.
    pub fn actor(&self) -> &ActorId {
        &self.actor
    }

    /// Get the clock (for creating stamps).
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Get mutable clock.
    pub fn clock_mut(&mut self) -> &mut Clock {
        &mut self.clock
    }

    /// Get the next scheduled sync deadline for a remote, if any.
    pub(crate) fn next_sync_deadline_for(&self, remote: &RemoteUrl) -> Option<Instant> {
        self.scheduler.deadline_for(remote)
    }

    /// Get repo state. Returns Internal if invariant is violated.
    pub(crate) fn repo_state(&self, proof: &LoadedRemote) -> Result<&RepoState, OpError> {
        self.repos
            .get(proof.remote())
            .ok_or(OpError::Internal("loaded repo missing from state"))
    }

    /// Get mutable repo state. Returns Internal if invariant is violated.
    pub(crate) fn repo_state_mut(
        &mut self,
        proof: &LoadedRemote,
    ) -> Result<&mut RepoState, OpError> {
        self.repos
            .get_mut(proof.remote())
            .ok_or(OpError::Internal("loaded repo missing from state"))
    }

    /// Get repo state by raw remote URL (for internal sync waiters, etc.).
    /// Returns None if not loaded.
    pub(crate) fn repo_state_by_url(&self, remote: &RemoteUrl) -> Option<&RepoState> {
        self.repos.get(remote)
    }

    /// Resolve a repo path to a normalized remote URL.
    fn resolve_remote(&mut self, repo_path: &Path) -> Result<RemoteUrl, OpError> {
        // 1. Env override (highest priority).
        if let Ok(url) = std::env::var("BD_REMOTE_URL")
            && !url.trim().is_empty()
        {
            return Ok(RemoteUrl(normalize_url(&url)));
        }

        // 2. Cache.
        if let Some(remote) = self.path_to_remote.get(repo_path) {
            return Ok(remote.clone());
        }

        // 3. Git config.
        let repo =
            Repository::open(repo_path).map_err(|_| OpError::NotAGitRepo(repo_path.to_owned()))?;
        let remote = repo
            .find_remote("origin")
            .map_err(|_| OpError::NoRemote(repo_path.to_owned()))?;
        let url = remote
            .url()
            .ok_or_else(|| OpError::NoRemote(repo_path.to_owned()))?;

        let remote = RemoteUrl(normalize_url(url));
        self.path_to_remote
            .insert(repo_path.to_owned(), remote.clone());
        Ok(remote)
    }

    /// Ensure repo is loaded using cached refs, without blocking on network fetch.
    ///
    /// If no local refs exist, this will attempt a one-time fetch with a bounded timeout.
    pub fn ensure_repo_loaded(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedRemote, OpError> {
        let remote = self.resolve_remote(repo)?;
        self.path_to_remote.insert(repo.to_owned(), remote.clone());

        if !self.repos.contains_key(&remote) {
            let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
            git_tx
                .send(GitOp::LoadLocal {
                    repo: repo.to_owned(),
                    respond: respond_tx,
                })
                .map_err(|_| OpError::Internal("git thread not responding"))?;

            match respond_rx.recv() {
                Ok(Ok(loaded)) => {
                    self.apply_loaded_repo_state(&remote, repo, loaded)?;
                }
                Ok(Err(SyncError::NoLocalRef(_))) => {
                    // No cached refs; attempt a bounded fetch to discover remote state.
                    let timeout = load_timeout();
                    let (fetch_tx, fetch_rx) = crossbeam::channel::bounded(1);
                    git_tx
                        .send(GitOp::Load {
                            repo: repo.to_owned(),
                            respond: fetch_tx,
                        })
                        .map_err(|_| OpError::Internal("git thread not responding"))?;

                    match fetch_rx.recv_timeout(timeout) {
                        Ok(Ok(loaded)) => {
                            self.apply_loaded_repo_state(&remote, repo, loaded)?;
                        }
                        Ok(Err(SyncError::NoLocalRef(_))) => {
                            return Err(OpError::RepoNotInitialized(repo.to_owned()));
                        }
                        Ok(Err(e)) => return Err(OpError::Sync(e)),
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
                Ok(Err(e)) => return Err(OpError::Sync(e)),
                Err(_) => return Err(OpError::Internal("git thread died")),
            }
        } else if let Some(repo_state) = self.repos.get_mut(&remote) {
            repo_state.register_path(repo.to_owned());
            self.export_go_compat(&remote);
        }

        Ok(LoadedRemote(remote))
    }

    /// Ensure repo is loaded, fetching from git if needed.
    ///
    /// This is a blocking operation - sends Load to git thread and waits with a bounded
    /// timeout for the initial fetch. Returns a `LoadedRemote` proof for state access.
    pub fn ensure_repo_loaded_strict(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedRemote, OpError> {
        let remote = self.resolve_remote(repo)?;
        self.path_to_remote.insert(repo.to_owned(), remote.clone());

        if !self.repos.contains_key(&remote) {
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
                    self.apply_loaded_repo_state(&remote, repo, loaded)?;
                }
                Ok(Err(SyncError::NoLocalRef(_))) => {
                    return Err(OpError::RepoNotInitialized(repo.to_owned()));
                }
                Ok(Err(e)) => {
                    return Err(OpError::Sync(e));
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
        } else if let Some(repo_state) = self.repos.get_mut(&remote) {
            repo_state.register_path(repo.to_owned());

            // Update symlinks for newly registered clone path
            self.export_go_compat(&remote);
        }

        Ok(LoadedRemote(remote))
    }

    fn apply_loaded_repo_state(
        &mut self,
        remote: &RemoteUrl,
        repo: &Path,
        loaded: LoadResult,
    ) -> Result<(), OpError> {
        let mut last_seen_stamp = loaded.last_seen_stamp;
        if let Some(max_stamp) = last_seen_stamp.as_ref() {
            self.clock.receive(max_stamp);
        }
        let mut needs_sync = loaded.needs_sync;
        let mut state = loaded.state;
        let mut root_slug = loaded.root_slug;

        // WAL recovery: merge any state from WAL file
        match self.wal.read(remote) {
            Ok(Some(wal_entry)) => {
                tracing::info!(
                    "recovering WAL for {:?} (sequence {})",
                    remote,
                    wal_entry.sequence
                );

                // Advance clock for WAL timestamps
                if let Some(max_stamp) = wal_entry.state.max_write_stamp() {
                    self.clock.receive(&max_stamp);
                    last_seen_stamp = max_write_stamp(last_seen_stamp, Some(max_stamp));
                }

                // CRDT merge WAL state with git state
                match CanonicalState::join(&state, &wal_entry.state) {
                    Ok(merged) => {
                        state = merged;
                        // WAL slug takes precedence if set
                        if wal_entry.root_slug.is_some() {
                            root_slug = wal_entry.root_slug;
                        }
                        // WAL had data - need to sync it to remote
                        needs_sync = true;
                    }
                    Err(errs) => {
                        return Err(OpError::WalMerge { errors: errs });
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                return Err(OpError::Wal(e));
            }
        }

        last_seen_stamp = max_write_stamp(last_seen_stamp, state.max_write_stamp());

        let now_wall_ms = WallClock::now().0;
        let clock_skew = last_seen_stamp
            .as_ref()
            .and_then(|stamp| detect_clock_skew(now_wall_ms, stamp.wall_ms));

        let mut repo_state = RepoState::with_state_and_path(state, root_slug, repo.to_owned());
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

        self.repos.insert(remote.clone(), repo_state);

        // Initial Go-compat export for newly loaded repo
        self.export_go_compat(remote);
        Ok(())
    }

    /// Ensure repo is loaded and reasonably fresh from remote.
    ///
    /// For clean (non-dirty) repos, we periodically kick off a background refresh
    /// so read-only commands observe updates pushed by other machines or daemons.
    /// This is non-blocking - it returns cached state immediately and applies
    /// fresh state when the background load completes.
    ///
    /// Returns a `LoadedRemote` proof that can be used for infallible state access.
    pub fn ensure_repo_fresh(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedRemote, OpError> {
        let loaded = self.ensure_repo_loaded(repo, git_tx)?;

        let repo_state = self.repo_state(&loaded)?;
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
                let repo_state = self.repo_state_mut(&loaded)?;
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
        result: Result<super::git_worker::LoadResult, SyncError>,
    ) {
        let repo_state = match self.repos.get_mut(remote) {
            Some(s) => s,
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
                    repo_state.state = fresh.state;
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
                repo_state.last_fetch_error = fresh.fetch_error.map(|message| FetchErrorRecord {
                    message,
                    wall_ms: now_wall_ms,
                });
                repo_state.last_divergence = fresh.divergence.map(|divergence| DivergenceRecord {
                    local_oid: divergence.local_oid.to_string(),
                    remote_oid: divergence.remote_oid.to_string(),
                    wall_ms: now_wall_ms,
                });
                repo_state.last_force_push = fresh.force_push.map(|force_push| ForcePushRecord {
                    previous_remote_oid: force_push.previous_remote_oid.to_string(),
                    remote_oid: force_push.remote_oid.to_string(),
                    wall_ms: now_wall_ms,
                });

                // If refresh showed we have local changes that need syncing
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
    ) -> Result<LoadedRemote, OpError> {
        let remote = self.resolve_remote(repo)?;

        // Remove cached state so ensure_repo_loaded will do a fresh load
        self.repos.remove(&remote);

        // Now load fresh from git
        self.ensure_repo_loaded_strict(repo, git_tx)
    }

    /// Maybe start a background sync for a repo.
    ///
    /// Only starts if:
    /// - Repo is dirty
    /// - Not already syncing
    pub fn maybe_start_sync(&mut self, remote: &RemoteUrl, git_tx: &Sender<GitOp>) {
        let repo_state = match self.repos.get_mut(remote) {
            Some(s) => s,
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
            state: repo_state.state.clone(),
            actor: self.actor.clone(),
        });
    }

    pub(crate) fn ensure_loaded_and_maybe_start_sync(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedRemote, OpError> {
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

                let repo_state = match self.repos.get_mut(remote) {
                    Some(s) => s,
                    None => return,
                };

                repo_state.last_seen_stamp =
                    max_write_stamp(repo_state.last_seen_stamp.clone(), outcome.last_seen_stamp);
                let now_wall_ms = WallClock::now().0;
                repo_state.last_clock_skew = repo_state
                    .last_seen_stamp
                    .as_ref()
                    .and_then(|stamp| detect_clock_skew(now_wall_ms, stamp.wall_ms));
                repo_state.last_divergence =
                    outcome.divergence.map(|divergence| DivergenceRecord {
                        local_oid: divergence.local_oid.to_string(),
                        remote_oid: divergence.remote_oid.to_string(),
                        wall_ms: now_wall_ms,
                    });
                repo_state.last_force_push = outcome.force_push.map(|force_push| ForcePushRecord {
                    previous_remote_oid: force_push.previous_remote_oid.to_string(),
                    remote_oid: force_push.remote_oid.to_string(),
                    wall_ms: now_wall_ms,
                });

                // If mutations happened during sync, merge them
                if repo_state.dirty {
                    let local_state = repo_state.state.clone();
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
                            repo_state.complete_sync(merged, self.clock.wall_ms());
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
                    repo_state.complete_sync(synced_state, self.clock.wall_ms());

                    // State is now durable in remote - delete WAL
                    if let Err(e) = self.wal.delete(remote) {
                        tracing::warn!("failed to delete WAL after sync for {:?}: {}", remote, e);
                    }
                    sync_succeeded = true;
                }
            }
            Err(e) => {
                tracing::error!("sync failed for {:?}: {:?}", remote, e);
                let repo_state = match self.repos.get_mut(remote) {
                    Some(s) => s,
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
            self.export_go_compat(remote);
        }
    }

    /// Export state to Go-compatible JSONL format.
    ///
    /// Called after successful sync to keep .beads/issues.jsonl in sync.
    fn export_go_compat(&self, remote: &RemoteUrl) {
        let Some(ref ctx) = self.export_ctx else {
            return;
        };

        let Some(repo_state) = self.repos.get(remote) else {
            return;
        };

        // Export to canonical location
        let export_path = match export_jsonl(&repo_state.state, ctx, remote.as_str()) {
            Ok(path) => path,
            Err(e) => {
                tracing::warn!("Go-compat export failed for {:?}: {}", remote, e);
                return;
            }
        };

        // Create/update symlinks in each clone
        if let Err(e) = ensure_symlinks(&export_path, &repo_state.known_paths) {
            tracing::warn!("Go-compat symlink update failed for {:?}: {}", remote, e);
        }
    }

    /// Apply a mutation with WAL durability.
    ///
    /// The mutation runs against a cloned state. If the WAL write fails,
    /// the in-memory state is left unchanged and the error is returned.
    pub(crate) fn apply_wal_mutation<R>(
        &mut self,
        proof: &LoadedRemote,
        f: impl FnOnce(&mut CanonicalState, Stamp) -> Result<R, OpError>,
    ) -> Result<R, OpError> {
        let (mut next_state, root_slug, sequence, last_seen_stamp) = {
            let repo_state = self.repo_state(proof)?;
            (
                repo_state.state.clone(),
                repo_state.root_slug.clone(),
                repo_state.wal_sequence,
                repo_state.last_seen_stamp.clone(),
            )
        };

        if let Some(floor) = last_seen_stamp.as_ref() {
            self.clock.receive(floor);
        }
        let now_wall_ms = WallClock::now().0;
        let clock_skew = last_seen_stamp
            .as_ref()
            .and_then(|stamp| detect_clock_skew(now_wall_ms, stamp.wall_ms));

        let write_stamp = self.clock_mut().tick();
        let stamp = Stamp {
            at: write_stamp.clone(),
            by: self.actor.clone(),
        };

        let result = f(&mut next_state, stamp)?;

        let entry = WalEntry::new(
            next_state.clone(),
            root_slug,
            sequence,
            self.clock.wall_ms(),
        );
        self.wal.write(proof.remote(), &entry)?;

        let repo_state = self.repo_state_mut(proof)?;
        repo_state.state = next_state;
        repo_state.wal_sequence = sequence + 1;
        repo_state.last_seen_stamp = Some(write_stamp);
        repo_state.last_clock_skew = clock_skew;
        repo_state.mark_dirty();
        self.scheduler.schedule(proof.remote().clone());

        Ok(result)
    }

    pub fn next_sync_deadline(&mut self) -> Option<Instant> {
        self.scheduler.next_deadline()
    }

    pub fn fire_due_syncs(&mut self, git_tx: &Sender<GitOp>) {
        let due = self.scheduler.drain_due(Instant::now());
        for remote in due {
            self.maybe_start_sync(&remote, git_tx);
        }
    }

    /// Get iterator over all repos.
    pub fn repos(&self) -> impl Iterator<Item = (&RemoteUrl, &RepoState)> {
        self.repos.iter()
    }

    /// Get mutable iterator over all repos.
    pub fn repos_mut(&mut self) -> impl Iterator<Item = (&RemoteUrl, &mut RepoState)> {
        self.repos.iter_mut()
    }

    /// Handle a request from IPC.
    ///
    /// Dispatches to appropriate handler based on request type.
    pub fn handle_request(&mut self, req: Request, git_tx: &Sender<GitOp>) -> Response {
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
            } => self.apply_create(
                &repo,
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
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.apply_update(&repo, &id, patch, cas, git_tx)
            }

            Request::AddLabels { repo, id, labels } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.apply_add_labels(&repo, &id, labels, git_tx)
            }

            Request::RemoveLabels { repo, id, labels } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.apply_remove_labels(&repo, &id, labels, git_tx)
            }

            Request::SetParent { repo, id, parent } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                let parent = match parent {
                    Some(raw) => match BeadId::parse(&raw) {
                        Ok(parent) => Some(parent),
                        Err(e) => {
                            return Response::err(invalid_id_payload(e));
                        }
                    },
                    None => None,
                };
                self.apply_set_parent(&repo, &id, parent, git_tx)
            }

            Request::Close {
                repo,
                id,
                reason,
                on_branch,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.apply_close(&repo, &id, reason, on_branch, git_tx)
            }

            Request::Reopen { repo, id } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.apply_reopen(&repo, &id, git_tx)
            }

            Request::Delete { repo, id, reason } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.apply_delete(&repo, &id, reason, git_tx)
            }

            Request::AddDep {
                repo,
                from,
                to,
                kind,
            } => {
                let from = match BeadId::parse(&from) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                let to = match BeadId::parse(&to) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.apply_add_dep(&repo, &from, &to, kind, git_tx)
            }

            Request::RemoveDep {
                repo,
                from,
                to,
                kind,
            } => {
                let from = match BeadId::parse(&from) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                let to = match BeadId::parse(&to) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.apply_remove_dep(&repo, &from, &to, kind, git_tx)
            }

            Request::AddNote { repo, id, content } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.apply_add_note(&repo, &id, content, git_tx)
            }

            Request::Claim {
                repo,
                id,
                lease_secs,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.apply_claim(&repo, &id, lease_secs, git_tx)
            }

            Request::Unclaim { repo, id } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.apply_unclaim(&repo, &id, git_tx)
            }

            Request::ExtendClaim {
                repo,
                id,
                lease_secs,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.apply_extend_claim(&repo, &id, lease_secs, git_tx)
            }

            // Queries - delegate to query_executor module
            Request::Show { repo, id } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.query_show(&repo, &id, git_tx)
            }

            Request::List { repo, filters } => self.query_list(&repo, &filters, git_tx),

            Request::Ready { repo, limit } => self.query_ready(&repo, limit, git_tx),

            Request::DepTree { repo, id } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.query_dep_tree(&repo, &id, git_tx)
            }

            Request::Deps { repo, id } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.query_deps(&repo, &id, git_tx)
            }

            Request::Notes { repo, id } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)),
                };
                self.query_notes(&repo, &id, git_tx)
            }

            Request::Blocked { repo } => self.query_blocked(&repo, git_tx),

            Request::Stale {
                repo,
                days,
                status,
                limit,
            } => self.query_stale(&repo, days, status.as_deref(), limit, git_tx),

            Request::Count {
                repo,
                filters,
                group_by,
            } => self.query_count(&repo, &filters, group_by.as_deref(), git_tx),

            Request::Deleted { repo, since_ms, id } => {
                let id = match id {
                    Some(s) => Some(match BeadId::parse(&s) {
                        Ok(id) => id,
                        Err(e) => {
                            return Response::err(invalid_id_payload(e));
                        }
                    }),
                    None => None,
                };
                self.query_deleted(&repo, since_ms, id.as_ref(), git_tx)
            }

            Request::EpicStatus {
                repo,
                eligible_only,
            } => self.query_epic_status(&repo, eligible_only, git_tx),

            Request::Status { repo } => self.query_status(&repo, git_tx),

            Request::Validate { repo } => self.query_validate(&repo, git_tx),

            // Control
            Request::Refresh { repo } => {
                // Force reload from git (invalidates cached state).
                // Used after external changes like migration.
                match self.force_reload(&repo, git_tx) {
                    Ok(_) => Response::ok(ResponsePayload::refreshed()),
                    Err(e) => Response::err(e),
                }
            }

            Request::Sync { repo } => {
                // Force immediate sync (used for graceful shutdown)
                match self.ensure_loaded_and_maybe_start_sync(&repo, git_tx) {
                    Ok(_) => Response::ok(ResponsePayload::synced()),
                    Err(e) => Response::err(e),
                }
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
                        ErrorCode::Internal,
                        "git thread not responding",
                        false,
                    ));
                }

                match respond_rx.recv() {
                    Ok(Ok(())) => match self.ensure_repo_loaded(&repo, git_tx) {
                        Ok(_) => Response::ok(ResponsePayload::initialized()),
                        Err(e) => Response::err(e),
                    },
                    Ok(Err(e)) => {
                        Response::err(error_payload(ErrorCode::InitFailed, &e.to_string(), false))
                    }
                    Err(_) => Response::err(error_payload(
                        ErrorCode::Internal,
                        "git thread died",
                        false,
                    )),
                }
            }

            Request::Ping => Response::ok(ResponsePayload::Query(QueryResult::DaemonInfo(
                ApiDaemonInfo {
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    protocol_version: super::ipc::IPC_PROTOCOL_VERSION,
                    pid: std::process::id(),
                },
            ))),

            Request::Shutdown => Response::ok(ResponsePayload::shutting_down()),
        }
    }
}

fn error_payload(code: ErrorCode, message: &str, retryable: bool) -> ErrorPayload {
    ErrorPayload::new(code, message, retryable)
}

fn invalid_id_payload(err: CoreError) -> ErrorPayload {
    match err {
        CoreError::InvalidId(id) => IpcError::from(id).into(),
        other => ErrorPayload::new(ErrorCode::InternalError, other.to_string(), false),
    }
}

fn load_timeout() -> Duration {
    let override_secs = std::env::var("BD_LOAD_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|v| *v > 0);
    Duration::from_secs(override_secs.unwrap_or(LOAD_TIMEOUT_SECS))
}

const CLOCK_SKEW_WARN_MS: u64 = 5 * 60 * 1000;

fn detect_clock_skew(now_ms: u64, reference_ms: u64) -> Option<ClockSkewRecord> {
    let delta_ms = now_ms as i64 - reference_ms as i64;
    if delta_ms.unsigned_abs() >= CLOCK_SKEW_WARN_MS {
        Some(ClockSkewRecord {
            delta_ms,
            wall_ms: now_ms,
        })
    } else {
        None
    }
}

fn max_write_stamp(a: Option<WriteStamp>, b: Option<WriteStamp>) -> Option<WriteStamp> {
    match (a, b) {
        (Some(a), Some(b)) => Some(std::cmp::max(a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        Bead, BeadCore, BeadFields, BeadId, BeadType, CanonicalState, Claim, Labels, Lww, Priority,
        Stamp, WallClock, Workflow, WriteStamp,
    };
    use crate::daemon::git_worker::LoadResult;
    use crate::daemon::wal::Wal;
    use tempfile::TempDir;

    fn test_actor() -> ActorId {
        ActorId::new("test@host".to_string()).unwrap()
    }

    fn test_remote() -> RemoteUrl {
        RemoteUrl("example.com/test/repo".into())
    }

    fn test_wal() -> (TempDir, Wal) {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::new(tmp.path()).unwrap();
        (tmp, wal)
    }

    fn make_stamp(wall_ms: u64, actor: &str) -> Stamp {
        Stamp::new(
            WriteStamp::new(wall_ms, 0),
            ActorId::new(actor.to_string()).unwrap(),
        )
    }

    fn make_bead(id: &str, wall_ms: u64, actor: &str) -> Bead {
        let stamp = make_stamp(wall_ms, actor);
        let core = BeadCore::new(BeadId::parse(id).unwrap(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("test".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            labels: Lww::new(Labels::new(), stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::Open, stamp.clone()),
            claim: Lww::new(Claim::default(), stamp),
        };
        Bead::new(core, fields)
    }

    #[test]
    fn complete_refresh_clears_in_progress_flag() {
        let (_tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let remote = test_remote();

        // Manually insert a repo in refresh_in_progress state
        let mut repo_state = RepoState::new();
        repo_state.refresh_in_progress = true;
        daemon.repos.insert(remote.clone(), repo_state);

        // Complete refresh with success
        let result = Ok(LoadResult {
            state: CanonicalState::new(),
            root_slug: None,
            needs_sync: false,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(&remote, result);

        let repo_state = daemon.repos.get(&remote).unwrap();
        assert!(!repo_state.refresh_in_progress);
        assert!(repo_state.last_refresh.is_some());
    }

    #[test]
    fn complete_refresh_clears_flag_on_error() {
        let (_tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let remote = test_remote();

        let mut repo_state = RepoState::new();
        repo_state.refresh_in_progress = true;
        daemon.repos.insert(remote.clone(), repo_state);

        // Complete refresh with error
        let result = Err(SyncError::NoLocalRef("/test".to_string()));
        daemon.complete_refresh(&remote, result);

        let repo_state = daemon.repos.get(&remote).unwrap();
        assert!(!repo_state.refresh_in_progress);
        // last_refresh should NOT be updated on error
        assert!(repo_state.last_refresh.is_none());
    }

    #[test]
    fn complete_refresh_applies_state_when_clean() {
        let (_tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let remote = test_remote();

        let mut repo_state = RepoState::new();
        repo_state.refresh_in_progress = true;
        repo_state.dirty = false; // Clean state
        daemon.repos.insert(remote.clone(), repo_state);

        // Create fresh state with some content
        let fresh_state = CanonicalState::new();
        let result = Ok(LoadResult {
            state: fresh_state.clone(),
            root_slug: Some("fresh-slug".to_string()),
            needs_sync: false,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(&remote, result);

        let repo_state = daemon.repos.get(&remote).unwrap();
        assert_eq!(repo_state.root_slug, Some("fresh-slug".to_string()));
    }

    #[test]
    fn complete_refresh_skips_state_when_dirty() {
        let (_tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let remote = test_remote();

        let mut repo_state = RepoState::new();
        repo_state.refresh_in_progress = true;
        repo_state.dirty = true; // Dirty - mutations happened during refresh
        repo_state.root_slug = Some("original-slug".to_string());
        daemon.repos.insert(remote.clone(), repo_state);

        // Try to apply refresh
        let result = Ok(LoadResult {
            state: CanonicalState::new(),
            root_slug: Some("new-slug".to_string()),
            needs_sync: false,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(&remote, result);

        let repo_state = daemon.repos.get(&remote).unwrap();
        // Should keep original slug since dirty
        assert_eq!(repo_state.root_slug, Some("original-slug".to_string()));
        // But last_refresh should still be updated
        assert!(repo_state.last_refresh.is_some());
    }

    #[test]
    fn complete_refresh_schedules_sync_when_needs_sync() {
        let (_tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let remote = test_remote();

        let mut repo_state = RepoState::new();
        repo_state.refresh_in_progress = true;
        repo_state.dirty = false;
        daemon.repos.insert(remote.clone(), repo_state);

        // Refresh shows local has changes remote doesn't
        let result = Ok(LoadResult {
            state: CanonicalState::new(),
            root_slug: None,
            needs_sync: true,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(&remote, result);

        let repo_state = daemon.repos.get(&remote).unwrap();
        // Should mark dirty to trigger sync
        assert!(repo_state.dirty);
        // And scheduler should have it pending
        assert!(daemon.scheduler.is_pending(&remote));
    }

    #[test]
    fn complete_refresh_unknown_remote_is_noop() {
        let (_tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let unknown = RemoteUrl("unknown.com/repo".into());

        // Should not panic on unknown remote
        let result = Ok(LoadResult {
            state: CanonicalState::new(),
            root_slug: None,
            needs_sync: false,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(&unknown, result);
        // Just verify no panic and daemon is still valid
        assert!(daemon.repos.is_empty());
    }

    #[test]
    fn complete_sync_success_clean_deletes_wal() {
        use crate::daemon::wal::WalEntry;

        let (tmp, wal) = test_wal();
        let remote = test_remote();

        // Write a WAL entry
        let entry = WalEntry::new(CanonicalState::new(), None, 1, 0);
        wal.write(&remote, &entry).unwrap();
        assert!(wal.exists(&remote));

        // Create daemon with WAL, recreating from same dir
        let wal = Wal::new(tmp.path()).unwrap();
        let mut daemon = Daemon::new(test_actor(), wal);

        // Insert a clean repo state
        let mut repo_state = RepoState::new();
        repo_state.dirty = false;
        daemon.repos.insert(remote.clone(), repo_state);

        // Complete sync with success
        let outcome = SyncOutcome {
            state: CanonicalState::new(),
            divergence: None,
            force_push: None,
            last_seen_stamp: None,
        };
        daemon.complete_sync(&remote, Ok(outcome));

        // WAL should be deleted after successful sync on clean state
        assert!(!daemon.wal.exists(&remote));
    }

    #[test]
    fn complete_sync_success_dirty_keeps_wal() {
        use crate::daemon::wal::WalEntry;

        let (tmp, wal) = test_wal();
        let remote = test_remote();

        // Write a WAL entry
        let entry = WalEntry::new(CanonicalState::new(), None, 1, 0);
        wal.write(&remote, &entry).unwrap();
        assert!(wal.exists(&remote));

        // Create daemon with WAL
        let wal = Wal::new(tmp.path()).unwrap();
        let mut daemon = Daemon::new(test_actor(), wal);

        // Insert a DIRTY repo state (mutations happened during sync)
        let mut repo_state = RepoState::new();
        repo_state.dirty = true;
        daemon.repos.insert(remote.clone(), repo_state);

        // Complete sync with success
        let outcome = SyncOutcome {
            state: CanonicalState::new(),
            divergence: None,
            force_push: None,
            last_seen_stamp: None,
        };
        daemon.complete_sync(&remote, Ok(outcome));

        // WAL should NOT be deleted - dirty state needs another sync
        assert!(daemon.wal.exists(&remote));
    }

    #[test]
    fn complete_sync_failure_keeps_wal() {
        use crate::daemon::wal::WalEntry;

        let (tmp, wal) = test_wal();
        let remote = test_remote();

        // Write a WAL entry
        let entry = WalEntry::new(CanonicalState::new(), None, 1, 0);
        wal.write(&remote, &entry).unwrap();
        assert!(wal.exists(&remote));

        // Create daemon with WAL
        let wal = Wal::new(tmp.path()).unwrap();
        let mut daemon = Daemon::new(test_actor(), wal);

        // Insert repo state
        let repo_state = RepoState::new();
        daemon.repos.insert(remote.clone(), repo_state);

        // Complete sync with failure
        daemon.complete_sync(&remote, Err(SyncError::NonFastForward));

        // WAL should NOT be deleted on failure
        assert!(daemon.wal.exists(&remote));
    }

    #[test]
    fn wal_write_failure_aborts_mutation() {
        let (tmp, wal) = test_wal();
        let remote = test_remote();

        // Remove WAL directory to force write failure.
        let wal_dir = tmp.path().join("wal");
        std::fs::remove_dir_all(&wal_dir).unwrap();

        let mut daemon = Daemon::new(test_actor(), wal);
        daemon.repos.insert(remote.clone(), RepoState::new());

        let proof = LoadedRemote(remote.clone());
        let result = daemon.apply_wal_mutation(&proof, |state, stamp| {
            let bead = make_bead("bd-abc", stamp.at.wall_ms, stamp.by.as_str());
            state.insert_live(bead);
            Ok(())
        });

        assert!(matches!(result, Err(OpError::Wal(_))));
        let repo_state = daemon.repos.get(&remote).unwrap();
        assert_eq!(repo_state.state.live_count(), 0);
        assert_eq!(repo_state.wal_sequence, 0);
    }

    #[test]
    fn apply_wal_mutation_clamps_to_last_seen_stamp() {
        let (_tmp, wal) = test_wal();
        let remote = test_remote();
        let mut daemon = Daemon::new(test_actor(), wal);

        let future_stamp = WriteStamp::new(WallClock::now().0 + 60_000, 0);
        let mut repo_state = RepoState::new();
        repo_state.last_seen_stamp = Some(future_stamp.clone());
        daemon.repos.insert(remote.clone(), repo_state);

        let proof = LoadedRemote(remote.clone());
        let result = daemon.apply_wal_mutation(&proof, |state, stamp| {
            let bead = make_bead("bd-abc", stamp.at.wall_ms, stamp.by.as_str());
            state.insert_live(bead);
            Ok(stamp.at.clone())
        });

        let applied_stamp = result.unwrap();
        assert!(applied_stamp >= future_stamp);
    }

    #[test]
    fn complete_sync_resolves_collisions_for_dirty_state() {
        let (_tmp, wal) = test_wal();
        let remote = test_remote();
        let mut daemon = Daemon::new(test_actor(), wal);

        let winner = make_bead("bd-abc", 1000, "alice");
        let loser = make_bead("bd-abc", 2000, "bob");

        let mut synced_state = CanonicalState::new();
        synced_state.insert_live(winner.clone());

        let mut local_state = CanonicalState::new();
        local_state.insert_live(loser);

        let mut repo_state = RepoState::new();
        repo_state.state = local_state;
        repo_state.dirty = true;
        repo_state.sync_in_progress = true;
        daemon.repos.insert(remote.clone(), repo_state);

        let outcome = SyncOutcome {
            last_seen_stamp: synced_state.max_write_stamp(),
            state: synced_state,
            divergence: None,
            force_push: None,
        };
        daemon.complete_sync(&remote, Ok(outcome));

        let repo_state = daemon.repos.get(&remote).unwrap();
        assert_eq!(repo_state.state.live_count(), 2);

        let id = BeadId::parse("bd-abc").unwrap();
        let merged = repo_state.state.get_live(&id).unwrap();
        assert_eq!(merged.core.created(), winner.core.created());
    }

    #[test]
    fn detect_clock_skew_flags_large_delta() {
        let now = 1_700_000_000_000u64;
        let reference = now - (CLOCK_SKEW_WARN_MS + 1);
        let skew = detect_clock_skew(now, reference).unwrap();
        assert!(skew.delta_ms > 0);
        assert_eq!(skew.wall_ms, now);
    }

    #[test]
    fn detect_clock_skew_ignores_small_delta() {
        let now = 1_700_000_000_000u64;
        let reference = now - (CLOCK_SKEW_WARN_MS - 1);
        assert!(detect_clock_skew(now, reference).is_none());
    }
}
