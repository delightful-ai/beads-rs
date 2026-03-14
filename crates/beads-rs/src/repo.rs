//! Repository opening helpers for the assembly crate.

use std::path::PathBuf;

use git2::Repository;

use crate::Error;
use beads_bootstrap::repo as bootstrap_repo;
use beads_git::SyncError;

/// Open the git repository containing the current directory.
pub fn discover() -> Result<(Repository, PathBuf), Error> {
    let start = PathBuf::from(".");
    let (repo, root) =
        bootstrap_repo::discover(&start).map_err(|e| SyncError::OpenRepo(start.clone(), e))?;
    Ok((repo, root))
}
