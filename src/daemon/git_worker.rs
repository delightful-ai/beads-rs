//! Git worker for background sync operations.
//!
//! Owns git2::Repository handles (which are !Send !Sync) and runs on a dedicated thread.
//! Receives GitOp messages from state thread, sends results back.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crossbeam::channel::{Receiver, Sender};
use git2::Repository;

use super::remote::RemoteUrl;
use crate::core::{ActorId, CanonicalState, Stamp};
use crate::git::error::SyncError;
use crate::git::sync::{SyncProcess, init_beads_ref, sync_with_retry};

/// Result of a sync operation.
pub type SyncResult = Result<CanonicalState, SyncError>;

/// Result of a load operation (includes metadata).
#[derive(Clone)]
pub struct LoadResult {
    pub state: CanonicalState,
    pub root_slug: Option<String>,
    /// True if local state has changes that need to be pushed to remote.
    /// This happens when local durable commits exist that haven't been synced.
    pub needs_sync: bool,
}

/// Result of a background refresh operation.
pub enum GitResult {
    /// Sync completed.
    Sync(RemoteUrl, SyncResult),
    /// Background refresh completed.
    Refresh(RemoteUrl, Result<LoadResult, SyncError>),
}

/// Operations sent from state thread to git thread.
pub enum GitOp {
    /// Load state from git ref (blocking - first access to a repo).
    Load {
        repo: PathBuf,
        respond: Sender<Result<LoadResult, SyncError>>,
    },

    /// Background refresh (non-blocking - result sent via result channel).
    /// Used for periodic freshness checks without blocking client requests.
    Refresh { repo: PathBuf, remote: RemoteUrl },

    /// Background sync (non-blocking - result sent via result channel).
    Sync {
        repo: PathBuf,
        remote: RemoteUrl,
        state: CanonicalState,
        actor: ActorId,
    },

    /// Local-only commit for durability (blocking - called after each mutation).
    ///
    /// Writes state to refs/heads/beads/store without fetching or pushing.
    /// This ensures acknowledged mutations survive crashes.
    LocalCommit {
        repo: PathBuf,
        remote: RemoteUrl,
        state: CanonicalState,
        root_slug: Option<String>,
        respond: Sender<Result<(), SyncError>>,
    },

    /// Initialize beads ref (blocking - bd init command).
    Init {
        repo: PathBuf,
        respond: Sender<Result<(), SyncError>>,
    },

    /// Shutdown the git thread.
    Shutdown,
}

/// Git worker that owns Repository handles.
pub struct GitWorker {
    /// Cached repository handles.
    repos: HashMap<PathBuf, Repository>,

    /// Channel to send results back to state thread.
    result_tx: Sender<GitResult>,
}

impl GitWorker {
    /// Create a new GitWorker.
    pub fn new(result_tx: Sender<GitResult>) -> Self {
        GitWorker {
            repos: HashMap::new(),
            result_tx,
        }
    }

    /// Open or get cached repository.
    fn open(&mut self, path: &Path) -> Result<&Repository, SyncError> {
        if !self.repos.contains_key(path) {
            let repo =
                Repository::open(path).map_err(|e| SyncError::OpenRepo(path.to_owned(), e))?;
            self.repos.insert(path.to_owned(), repo);
        }
        Ok(self.repos.get(path).unwrap())
    }

    /// Load state from beads/store ref.
    ///
    /// This combines local durable state with remote state:
    /// 1. Read from local ref (includes acknowledged but not-yet-pushed commits)
    /// 2. Fetch and read from remote
    /// 3. CRDT-merge the two (both are authoritative in their own way)
    ///
    /// This handles both crash recovery (local has mutations remote doesn't) and
    /// concurrent edits (remote has mutations local doesn't).
    pub fn load(&mut self, path: &Path) -> Result<LoadResult, SyncError> {
        use crate::core::CanonicalState;
        use crate::git::sync::read_state_at_oid;

        let repo = self.open(path)?;

        // Step 1: Read local state if exists
        let (local_state, local_slug) =
            if let Ok(local_oid) = repo.refname_to_id("refs/heads/beads/store") {
                let loaded = read_state_at_oid(repo, local_oid)?;
                (loaded.state, loaded.root_slug)
            } else {
                (CanonicalState::new(), None)
            };

        // Step 2: Fetch remote and read its state
        let fetched = SyncProcess::new(path.to_owned()).fetch(repo)?;
        let remote_state = fetched.phase.remote_state;
        let remote_slug = fetched.phase.root_slug;

        // Step 3: CRDT merge - both local and remote are authoritative
        let merged = CanonicalState::join(&local_state, &remote_state)
            .map_err(|errs| SyncError::MergeConflict { errors: errs })?;

        // Prefer local slug if set, else remote
        let root_slug = local_slug.or(remote_slug);

        // Check if local has changes that remote doesn't.
        // Simple heuristic: if merged has more items than remote, local had new data.
        // This catches the common case of local-only commits. False negatives are
        // acceptable (sync will happen eventually), false positives just trigger
        // an extra sync that finds nothing to push.
        let needs_sync = merged.live_count() > remote_state.live_count()
            || merged.tombstone_count() > remote_state.tombstone_count()
            || merged.dep_count() > remote_state.dep_count();

        Ok(LoadResult {
            state: merged,
            root_slug,
            needs_sync,
        })
    }

    /// Sync local state to remote.
    pub fn sync(&mut self, path: &Path, state: &CanonicalState, actor: &ActorId) -> SyncResult {
        let repo = self.open(path)?;

        // Create resolution stamp for collision handling
        use crate::daemon::Clock;
        let mut clock = Clock::new();
        let write_stamp = clock.tick();
        let resolution_stamp = Stamp {
            at: write_stamp,
            by: actor.clone(),
        };

        sync_with_retry(repo, path, state, resolution_stamp, 5)
    }

    /// Initialize beads ref.
    pub fn init(&mut self, path: &Path) -> Result<(), SyncError> {
        let repo = self.open(path)?;
        init_beads_ref(repo, 5)
    }

    /// Commit state locally for durability (no fetch/push).
    ///
    /// This creates a local-only commit on refs/heads/beads/store,
    /// ensuring mutations survive daemon crashes. The full sync
    /// will later merge with remote and push.
    pub fn commit_local(
        &mut self,
        path: &Path,
        state: &CanonicalState,
        root_slug: Option<&str>,
    ) -> Result<(), SyncError> {
        use crate::git::sync::commit_local_only;
        let repo = self.open(path)?;
        commit_local_only(repo, state, root_slug)
    }

    /// Process a single GitOp.
    fn handle_op(&mut self, op: GitOp) -> bool {
        match op {
            GitOp::Load { repo, respond } => {
                let result = self.load(&repo);
                let _ = respond.send(result);
            }

            GitOp::Refresh { repo, remote } => {
                let result = self.load(&repo);
                let _ = self.result_tx.send(GitResult::Refresh(remote, result));
            }

            GitOp::Sync {
                repo,
                remote,
                state,
                actor,
            } => {
                let result = self.sync(&repo, &state, &actor);
                let _ = self.result_tx.send(GitResult::Sync(remote, result));
            }

            GitOp::LocalCommit {
                repo,
                remote: _,
                state,
                root_slug,
                respond,
            } => {
                let result = self.commit_local(&repo, &state, root_slug.as_deref());
                let _ = respond.send(result);
            }

            GitOp::Init { repo, respond } => {
                let result = self.init(&repo);
                let _ = respond.send(result);
            }

            GitOp::Shutdown => {
                return false; // Signal to exit loop
            }
        }
        true // Continue processing
    }
}

/// Run the git thread loop.
///
/// Processes GitOp messages until Shutdown is received.
pub fn run_git_loop(mut worker: GitWorker, git_rx: Receiver<GitOp>) {
    for op in git_rx {
        if !worker.handle_op(op) {
            break;
        }
    }
}
