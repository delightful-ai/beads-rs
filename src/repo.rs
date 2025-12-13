//! Repository discovery and state loading.

use std::path::PathBuf;

use git2::Repository;

use crate::core::CanonicalState;
use crate::git::sync::read_state_at_oid;
use crate::git::SyncError;
use crate::Error;

/// Open the git repository containing the current directory.
pub fn discover() -> Result<(Repository, PathBuf), Error> {
    let repo = Repository::discover(".")
        .map_err(|e| SyncError::OpenRepo(PathBuf::from("."), e))?;
    let path = repo
        .workdir()
        .ok_or_else(|| SyncError::OpenRepo(PathBuf::from("."), git2::Error::from_str("bare repository not supported")))? 
        .to_owned();
    Ok((repo, path))
}

/// Load CanonicalState from the beads store ref.
///
/// Errors if ref doesn't exist (uninitialized repo).
pub fn load_state(repo: &Repository) -> Result<CanonicalState, Error> {
    let oid = repo
        .refname_to_id("refs/heads/beads/store")
        .map_err(|_| SyncError::NoLocalRef("refs/heads/beads/store".into()))?;
    Ok(read_state_at_oid(repo, oid)?)
}
