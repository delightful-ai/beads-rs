//! Internal git sync backend seams.
//!
//! These traits intentionally model small, capability-shaped operations so daemon
//! orchestration can depend on behavior rather than concrete git helper functions.

use std::path::Path;

use git2::{ErrorCode, Oid, Repository};

use crate::core::{BeadSlug, CanonicalState, WriteStamp};
use crate::git::checkpoint::{
    CheckpointExport, CheckpointPublishError, CheckpointPublishOutcome, CheckpointStoreMeta,
    publish_checkpoint as publish_checkpoint_git,
};
use crate::git::error::SyncError;
use crate::git::sync::{DivergenceInfo, ForcePushInfo, SyncOutcome, SyncProcess, sync_with_retry};

/// Result of best-effort remote fetch + decode for load/merge flow.
#[derive(Clone)]
pub(crate) struct RemoteFetch {
    pub(crate) remote_state: CanonicalState,
    pub(crate) root_slug: Option<BeadSlug>,
    pub(crate) parent_meta_stamp: Option<WriteStamp>,
    pub(crate) fetch_error: Option<String>,
    pub(crate) divergence: Option<DivergenceInfo>,
    pub(crate) force_push: Option<ForcePushInfo>,
}

/// Capability: fetch remote state for merge.
///
/// # Laws
/// - Must not mutate caller-provided in-memory CRDT state.
/// - May update repository fetch metadata/remote-tracking refs as part of
///   best-effort fetch.
/// - Must return diagnostics (`fetch_error`, `divergence`, `force_push`) whenever
///   best-effort fetch succeeds far enough to produce them.
pub(crate) trait FetchRemote {
    fn fetch_remote(&self, repo_path: &Path, repo: &Repository) -> Result<RemoteFetch, SyncError>;
}

/// Capability: push merged state to the remote store ref.
///
/// # Laws
/// - Must preserve linear-history semantics (no merge commits).
/// - Must not silently swallow non-fast-forward failures.
pub(crate) trait PushRemote {
    fn push_remote(
        &self,
        repo: &Repository,
        repo_path: &Path,
        state: &CanonicalState,
    ) -> Result<SyncOutcome, SyncError>;
}

/// Capability: read current checkpoint ref target.
///
/// # Laws
/// - Missing refs are represented as `Ok(None)`.
/// - Method is read-only and must not mutate repository state.
pub(crate) trait ReadCheckpointRef {
    fn read_checkpoint_ref(
        &self,
        repo: &Repository,
        git_ref: &str,
    ) -> Result<Option<Oid>, SyncError>;
}

/// Capability: publish checkpoint export to a git ref.
///
/// # Laws
/// - Published ref must match canonical checkpoint export content.
/// - Non-fast-forward / incompatible-remote errors are surfaced explicitly.
pub(crate) trait PublishCheckpointRef {
    fn publish_checkpoint_ref(
        &self,
        repo: &Repository,
        export: &CheckpointExport,
        git_ref: &str,
        store_meta: &CheckpointStoreMeta,
    ) -> Result<CheckpointPublishOutcome, CheckpointPublishError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DefaultGitBackend;

impl FetchRemote for DefaultGitBackend {
    fn fetch_remote(&self, repo_path: &Path, repo: &Repository) -> Result<RemoteFetch, SyncError> {
        let fetched = SyncProcess::new(repo_path.to_owned()).fetch_best_effort(repo)?;
        Ok(RemoteFetch {
            remote_state: fetched.phase.remote_state,
            root_slug: fetched.phase.root_slug,
            parent_meta_stamp: fetched.phase.parent_meta_stamp,
            fetch_error: fetched.phase.fetch_error,
            divergence: fetched.phase.divergence,
            force_push: fetched.phase.force_push,
        })
    }
}

impl PushRemote for DefaultGitBackend {
    fn push_remote(
        &self,
        repo: &Repository,
        repo_path: &Path,
        state: &CanonicalState,
    ) -> Result<SyncOutcome, SyncError> {
        sync_with_retry(repo, repo_path, state, 5)
    }
}

impl ReadCheckpointRef for DefaultGitBackend {
    fn read_checkpoint_ref(
        &self,
        repo: &Repository,
        git_ref: &str,
    ) -> Result<Option<Oid>, SyncError> {
        match repo.refname_to_id(git_ref) {
            Ok(oid) => Ok(Some(oid)),
            Err(err) if err.code() == ErrorCode::NotFound => Ok(None),
            Err(err) => Err(SyncError::Git(err)),
        }
    }
}

impl PublishCheckpointRef for DefaultGitBackend {
    fn publish_checkpoint_ref(
        &self,
        repo: &Repository,
        export: &CheckpointExport,
        git_ref: &str,
        store_meta: &CheckpointStoreMeta,
    ) -> Result<CheckpointPublishOutcome, CheckpointPublishError> {
        publish_checkpoint_git(repo, export, git_ref, store_meta)
    }
}
