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

/// Operations sent from state thread to git thread.
pub enum GitOp {
    /// Load state from git ref (blocking - first access to a repo).
    Load {
        repo: PathBuf,
        respond: Sender<Result<CanonicalState, SyncError>>,
    },

    /// Background sync (non-blocking - result sent via result channel).
    Sync {
        repo: PathBuf,
        remote: RemoteUrl,
        state: CanonicalState,
        actor: ActorId,
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

    /// Channel to send sync results back to state thread.
    result_tx: Sender<(RemoteUrl, SyncResult)>,
}

impl GitWorker {
    /// Create a new GitWorker.
    pub fn new(result_tx: Sender<(RemoteUrl, SyncResult)>) -> Self {
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
    pub fn load(&mut self, path: &Path) -> Result<CanonicalState, SyncError> {
        let repo = self.open(path)?;
        // Always fetch before reading, and fall back to local if remote missing.
        let fetched = SyncProcess::new(path.to_owned()).fetch(repo)?;
        Ok(fetched.phase.remote_state)
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

    /// Process a single GitOp.
    fn handle_op(&mut self, op: GitOp) -> bool {
        match op {
            GitOp::Load { repo, respond } => {
                let result = self.load(&repo);
                let _ = respond.send(result);
            }

            GitOp::Sync {
                repo,
                remote,
                state,
                actor,
            } => {
                let result = self.sync(&repo, &state, &actor);
                let _ = self.result_tx.send((remote, result));
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
