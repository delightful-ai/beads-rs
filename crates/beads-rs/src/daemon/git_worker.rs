//! Git worker for background sync operations.
//!
//! Owns git2::Repository handles (which are !Send !Sync) and runs on a dedicated thread.
//! Receives GitOp messages from state thread, sends results back.

use std::collections::{BTreeMap, HashMap, hash_map::Entry};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crossbeam::channel::{Receiver, Sender};
use git2::{ErrorCode, Oid, Repository};

use crate::core::{ActorId, BeadSlug, CanonicalState, StoreId, WriteStamp};
use crate::daemon::io_budget::TokenBucket;
use crate::daemon::metrics;
use crate::git::checkpoint::{
    CHECKPOINT_FORMAT_VERSION, CheckpointCache, CheckpointExport, CheckpointExportError,
    CheckpointExportInput, CheckpointPublishError, CheckpointPublishOutcome, CheckpointSnapshot,
    CheckpointStoreMeta, export_checkpoint, publish_checkpoint as publish_checkpoint_git,
};
use crate::git::error::SyncError;
use crate::git::sync::{DivergenceInfo, SyncOutcome, SyncProcess, init_beads_ref, sync_with_retry};
use beads_daemon::remote::RemoteUrl;

/// Result of a sync operation.
pub type SyncResult = Result<SyncOutcome, SyncError>;
pub type CheckpointResult = Result<CheckpointPublishOutcome, CheckpointPublishError>;

/// Result of a load operation (includes metadata).
#[derive(Clone)]
pub struct LoadResult {
    pub state: CanonicalState,
    pub root_slug: Option<BeadSlug>,
    /// True if local state has changes that need to be pushed to remote.
    /// This happens when local durable commits exist that haven't been synced.
    pub needs_sync: bool,
    /// Last observed write stamp (from state or meta).
    pub last_seen_stamp: Option<WriteStamp>,
    /// Best-effort fetch error (if any).
    pub fetch_error: Option<String>,
    /// Divergence detected between local and remote refs (if any).
    pub divergence: Option<DivergenceInfo>,
    /// Force-push detected on remote tracking ref (if any).
    pub force_push: Option<crate::git::sync::ForcePushInfo>,
}

/// Result of a background refresh operation.
pub enum GitResult {
    /// Sync completed.
    Sync(RemoteUrl, SyncResult),
    /// Background refresh completed.
    Refresh(RemoteUrl, Result<LoadResult, SyncError>),
    /// Checkpoint publish completed.
    Checkpoint(StoreId, String, CheckpointResult),
}

/// Operations sent from state thread to git thread.
pub enum GitOp {
    /// Load state from local refs only (no network).
    LoadLocal {
        repo: PathBuf,
        respond: Sender<Result<LoadResult, SyncError>>,
    },

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
        store_id: StoreId,
        state: CanonicalState,
        actor: ActorId,
    },

    /// Background checkpoint export + push (non-blocking).
    Checkpoint {
        repo: PathBuf,
        store_id: StoreId,
        checkpoint_group: String,
        git_ref: String,
        snapshot: CheckpointSnapshot,
        checkpoint_groups: BTreeMap<String, String>,
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

    limits: crate::core::Limits,
    background_io: HashMap<StoreId, TokenBucket>,
    checkpoint_exports: HashMap<(StoreId, String), CheckpointExport>,
}

impl GitWorker {
    /// Create a new GitWorker.
    pub fn new(result_tx: Sender<GitResult>, limits: crate::core::Limits) -> Self {
        GitWorker {
            repos: HashMap::new(),
            result_tx,
            limits,
            background_io: HashMap::new(),
            checkpoint_exports: HashMap::new(),
        }
    }

    /// Open or get cached repository.
    fn open(&mut self, path: &Path) -> Result<&Repository, SyncError> {
        match self.repos.entry(path.to_owned()) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let repo =
                    Repository::open(path).map_err(|e| SyncError::OpenRepo(path.to_owned(), e))?;
                Ok(entry.insert(repo))
            }
        }
    }

    fn background_budget(&mut self, store_id: StoreId) -> &mut TokenBucket {
        let rate = self.limits.max_background_io_bytes_per_sec as u64;
        self.background_io
            .entry(store_id)
            .or_insert_with(|| TokenBucket::new(rate))
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
        let (local_state, local_slug, local_meta_stamp) =
            if let Ok(local_oid) = repo.refname_to_id("refs/heads/beads/store") {
                let loaded = read_state_at_oid(repo, local_oid)?;
                (
                    loaded.state,
                    loaded.meta.root_slug().cloned(),
                    loaded.meta.last_write_stamp().cloned(),
                )
            } else {
                (CanonicalState::new(), None, None)
            };

        // Step 2: Fetch remote and read its state
        let fetched = SyncProcess::new(path.to_owned()).fetch_best_effort(repo)?;
        let remote_state = fetched.phase.remote_state;
        let remote_slug = fetched.phase.root_slug;
        let remote_meta_stamp = fetched.phase.parent_meta_stamp;
        let fetch_error = fetched.phase.fetch_error.clone();
        let divergence = fetched.phase.divergence.clone();
        let force_push = fetched.phase.force_push.clone();

        // Step 3: CRDT merge - both local and remote are authoritative
        build_load_result(LoadResultInputs {
            local_state,
            local_slug,
            local_meta_stamp,
            remote_state,
            remote_slug,
            remote_meta_stamp,
            fetch_error,
            divergence,
            force_push,
        })
    }

    /// Load state using only local refs (no network).
    pub fn load_local(&mut self, path: &Path) -> Result<LoadResult, SyncError> {
        use crate::core::CanonicalState;
        use crate::git::sync::read_state_at_oid;

        let repo = self.open(path)?;

        let local_oid_opt = refname_to_id_optional(repo, "refs/heads/beads/store")?;
        let remote_oid_opt = refname_to_id_optional(repo, "refs/remotes/origin/beads/store")?;

        if local_oid_opt.is_none() && remote_oid_opt.is_none() {
            return Err(SyncError::NoLocalRef(path.display().to_string()));
        }

        let (local_state, local_slug, local_meta_stamp) = match local_oid_opt {
            Some(oid) => {
                let loaded = read_state_at_oid(repo, oid)?;
                (
                    loaded.state,
                    loaded.meta.root_slug().cloned(),
                    loaded.meta.last_write_stamp().cloned(),
                )
            }
            None => (CanonicalState::new(), None, None),
        };

        let (remote_state, remote_slug, remote_meta_stamp) = match remote_oid_opt {
            Some(oid) => {
                let loaded = read_state_at_oid(repo, oid)?;
                (
                    loaded.state,
                    loaded.meta.root_slug().cloned(),
                    loaded.meta.last_write_stamp().cloned(),
                )
            }
            None => {
                if local_oid_opt.is_some() {
                    (
                        local_state.clone(),
                        local_slug.clone(),
                        local_meta_stamp.clone(),
                    )
                } else {
                    (CanonicalState::new(), None, None)
                }
            }
        };

        build_load_result(LoadResultInputs {
            local_state,
            local_slug,
            local_meta_stamp,
            remote_state,
            remote_slug,
            remote_meta_stamp,
            fetch_error: None,
            divergence: None,
            force_push: None,
        })
    }

    /// Sync local state to remote.
    pub fn sync(&mut self, path: &Path, state: &CanonicalState) -> SyncResult {
        let repo = self.open(path)?;

        sync_with_retry(repo, path, state, 5)
    }

    pub fn publish_checkpoint(
        &mut self,
        path: &Path,
        snapshot: &CheckpointSnapshot,
        git_ref: &str,
        checkpoint_groups: BTreeMap<String, String>,
    ) -> CheckpointResult {
        let key = (snapshot.store_id, snapshot.checkpoint_group.clone());
        let mut drop_in_memory_previous = false;
        let export = {
            let previous_from_map = self.checkpoint_exports.get(&key);
            let mut cache_previous = None;
            let previous = match previous_from_map {
                Some(previous) => Some(previous),
                None => {
                    let cache =
                        CheckpointCache::new(snapshot.store_id, snapshot.checkpoint_group.clone());
                    match cache.load_current_export() {
                        Ok(Some(export)) => {
                            cache_previous = Some(export);
                        }
                        Ok(None) => {}
                        Err(err) => {
                            tracing::warn!(error = ?err, "checkpoint cache load failed");
                        }
                    }
                    cache_previous.as_ref()
                }
            };

            match previous {
                Some(previous) => match export_checkpoint(CheckpointExportInput {
                    snapshot,
                    previous: Some(previous),
                }) {
                    Ok(export) => export,
                    Err(err) if is_previous_mismatch(&err) => {
                        if previous_from_map.is_some() {
                            drop_in_memory_previous = true;
                        }
                        export_checkpoint(CheckpointExportInput {
                            snapshot,
                            previous: None,
                        })?
                    }
                    Err(err) => return Err(err.into()),
                },
                None => export_checkpoint(CheckpointExportInput {
                    snapshot,
                    previous: None,
                })?,
            }
        };
        if drop_in_memory_previous {
            self.checkpoint_exports.remove(&key);
        }
        self.checkpoint_exports.insert(key, export.clone());

        let cache = CheckpointCache::new(snapshot.store_id, snapshot.checkpoint_group.clone());
        {
            let budget = self.background_budget(snapshot.store_id);
            let mut throttle = |bytes: u64| {
                let wait = budget.throttle(bytes);
                if !wait.is_zero() {
                    metrics::background_io_throttle(wait, bytes);
                }
            };
            if let Err(err) = cache.publish_with_throttle(&export, Some(&mut throttle)) {
                tracing::warn!(error = ?err, "checkpoint cache publish failed");
            }
        }

        let repo = self.open(path)?;
        let store_meta = CheckpointStoreMeta::new(
            snapshot.store_id,
            snapshot.store_epoch,
            CHECKPOINT_FORMAT_VERSION,
            checkpoint_groups,
        );

        publish_checkpoint_git(repo, &export, git_ref, &store_meta)
    }

    /// Initialize beads ref.
    pub fn init(&mut self, path: &Path) -> Result<(), SyncError> {
        let repo = self.open(path)?;
        init_beads_ref(repo, 5)
    }

    /// Process a single GitOp.
    fn handle_op(&mut self, op: GitOp) -> bool {
        match op {
            GitOp::LoadLocal { repo, respond } => {
                let result = self.load_local(&repo);
                let _ = respond.send(result);
            }

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
                store_id,
                state,
                actor,
            } => {
                let span = tracing::info_span!(
                    "sync",
                    store_id = %store_id,
                    remote = %remote,
                    repo = %repo.display(),
                    actor_id = %actor
                );
                let _guard = span.enter();
                let started = Instant::now();
                let result = self.sync(&repo, &state);
                let elapsed_ms = started.elapsed().as_millis();
                match &result {
                    Ok(_) => tracing::info!(elapsed_ms, "sync completed"),
                    Err(err) => tracing::warn!(elapsed_ms, error = ?err, "sync failed"),
                }
                let _ = self.result_tx.send(GitResult::Sync(remote, result));
            }

            GitOp::Checkpoint {
                repo,
                store_id,
                checkpoint_group,
                git_ref,
                snapshot,
                checkpoint_groups,
            } => {
                let span = tracing::info_span!(
                    "checkpoint",
                    store_id = %store_id,
                    checkpoint_group = %checkpoint_group,
                    repo = %repo.display()
                );
                let _guard = span.enter();
                let started = Instant::now();
                let result = self.publish_checkpoint(&repo, &snapshot, &git_ref, checkpoint_groups);
                let elapsed = started.elapsed();
                let elapsed_ms = elapsed.as_millis();
                match &result {
                    Ok(outcome) => tracing::info!(
                        elapsed_ms,
                        checkpoint_id = %outcome.checkpoint_id,
                        "checkpoint publish completed"
                    ),
                    Err(err) => {
                        tracing::warn!(elapsed_ms, error = ?err, "checkpoint publish failed")
                    }
                }
                match &result {
                    Ok(_) => metrics::checkpoint_export_ok(elapsed),
                    Err(_) => metrics::checkpoint_export_err(elapsed),
                }
                let _ =
                    self.result_tx
                        .send(GitResult::Checkpoint(store_id, checkpoint_group, result));
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

fn is_previous_mismatch(err: &CheckpointExportError) -> bool {
    matches!(
        err,
        CheckpointExportError::PreviousStoreId { .. }
            | CheckpointExportError::PreviousStoreEpoch { .. }
            | CheckpointExportError::PreviousGroup { .. }
            | CheckpointExportError::PreviousNamespaces { .. }
    )
}

struct LoadResultInputs {
    local_state: CanonicalState,
    local_slug: Option<BeadSlug>,
    local_meta_stamp: Option<WriteStamp>,
    remote_state: CanonicalState,
    remote_slug: Option<BeadSlug>,
    remote_meta_stamp: Option<WriteStamp>,
    fetch_error: Option<String>,
    divergence: Option<DivergenceInfo>,
    force_push: Option<crate::git::sync::ForcePushInfo>,
}

fn build_load_result(inputs: LoadResultInputs) -> Result<LoadResult, SyncError> {
    let LoadResultInputs {
        local_state,
        local_slug,
        local_meta_stamp,
        remote_state,
        remote_slug,
        remote_meta_stamp,
        fetch_error,
        divergence,
        force_push,
    } = inputs;
    let merged = CanonicalState::join(&local_state, &remote_state)
        .map_err(|errs| SyncError::MergeConflict { errors: errs })?;

    let root_slug = local_slug.or(remote_slug);

    // Check if local has changes that remote doesn't.
    // Simple heuristic: if merged has more items than remote, local had new data.
    // This catches the common case of local-only commits. False negatives are
    // acceptable (sync will happen eventually), false positives just trigger
    // an extra sync that finds nothing to push.
    let needs_sync = merged.live_count() > remote_state.live_count()
        || merged.tombstone_count() > remote_state.tombstone_count()
        || merged.dep_count() > remote_state.dep_count();

    let merged_stamp = merged.max_write_stamp();
    let meta_stamp = max_write_stamp(local_meta_stamp, remote_meta_stamp);
    let last_seen_stamp = max_write_stamp(merged_stamp, meta_stamp);

    Ok(LoadResult {
        state: merged,
        root_slug,
        needs_sync,
        last_seen_stamp,
        fetch_error,
        divergence,
        force_push,
    })
}

fn refname_to_id_optional(repo: &Repository, name: &str) -> Result<Option<Oid>, SyncError> {
    match repo.refname_to_id(name) {
        Ok(oid) => Ok(Some(oid)),
        Err(e) if e.code() == ErrorCode::NotFound => Ok(None),
        Err(e) => Err(SyncError::Git(e)),
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
