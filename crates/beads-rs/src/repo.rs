//! Repository discovery and state loading.

use std::path::PathBuf;

use git2::Repository;

use crate::{Error, OpError};
use beads_bootstrap::repo as bootstrap_repo;
use beads_core::CanonicalState;
use beads_git::SyncError;
use beads_git::sync::{LoadedStore, read_state_at_oid};

/// Open the git repository containing the current directory.
pub fn discover() -> Result<(Repository, PathBuf), Error> {
    let start = PathBuf::from(".");
    let root =
        bootstrap_repo::discover_root(&start).map_err(|e| SyncError::OpenRepo(start.clone(), e))?;
    let repo = Repository::open(&root).map_err(|e| SyncError::OpenRepo(root.clone(), e))?;
    Ok((repo, root))
}

/// Load CanonicalState from the beads store ref.
///
/// Errors if ref doesn't exist (uninitialized repo).
pub fn load_state(repo: &Repository) -> Result<CanonicalState, Error> {
    let oid = repo
        .refname_to_id("refs/heads/beads/store")
        .map_err(|_| SyncError::NoLocalRef("refs/heads/beads/store".into()))?;
    Ok(read_state_at_oid(repo, oid)
        .map_err(map_strict_store_load_error)?
        .state)
}

/// Load state and metadata from the beads store ref.
///
/// Errors if ref doesn't exist (uninitialized repo).
pub fn load_store(repo: &Repository) -> Result<LoadedStore, Error> {
    let oid = repo
        .refname_to_id("refs/heads/beads/store")
        .map_err(|_| SyncError::NoLocalRef("refs/heads/beads/store".into()))?;
    read_state_at_oid(repo, oid).map_err(map_strict_store_load_error)
}

fn map_strict_store_load_error(err: SyncError) -> Error {
    if let Some(hint) = err.legacy_deps_runtime_hint() {
        return Error::Op(OpError::ValidationFailed {
            field: "store".into(),
            reason: format!("{}; {hint}", err),
        });
    }
    err.into()
}
