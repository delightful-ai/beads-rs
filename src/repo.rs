//! Repository discovery and state loading.

use std::path::{Path, PathBuf};

use git2::Repository;

use crate::Error;
use crate::core::CanonicalState;
use crate::git::SyncError;
use crate::git::sync::{LoadedStore, read_state_at_oid};

/// Open the git repository containing the current directory.
pub fn discover() -> Result<(Repository, PathBuf), Error> {
    let start = PathBuf::from(".");
    if should_fast_discover()
        && let Some(root) = fast_repo_root(&start)
        && let Ok(repo) = Repository::open(&root)
    {
        return Ok((repo, root));
    }
    let repo = Repository::discover(&start).map_err(|e| SyncError::OpenRepo(start.clone(), e))?;
    let path = repo
        .workdir()
        .ok_or_else(|| {
            SyncError::OpenRepo(
                start.clone(),
                git2::Error::from_str("bare repository not supported"),
            )
        })?
        .to_owned();
    Ok((repo, path))
}

/// Discover the repository root for a given starting path.
pub fn discover_root(start: impl AsRef<Path>) -> Result<PathBuf, Error> {
    let start = start.as_ref();
    let root =
        discover_root_inner(start).map_err(|e| SyncError::OpenRepo(start.to_path_buf(), e))?;
    Ok(root)
}

/// Discover the repository root for a given starting path, if any.
pub fn discover_root_optional(start: impl AsRef<Path>) -> Option<PathBuf> {
    discover_root_inner(start.as_ref()).ok()
}

fn discover_root_inner(start: &Path) -> Result<PathBuf, git2::Error> {
    if should_fast_discover()
        && let Some(root) = fast_repo_root(start)
    {
        return Ok(root);
    }
    let repo = Repository::discover(start)?;
    repo.workdir()
        .map(|path| path.to_owned())
        .ok_or_else(|| git2::Error::from_str("bare repository not supported"))
}

fn should_fast_discover() -> bool {
    if let Some(dir) = std::env::var_os("GIT_DIR")
        && !dir.is_empty()
    {
        return false;
    }
    if let Some(dir) = std::env::var_os("GIT_WORK_TREE")
        && !dir.is_empty()
    {
        return false;
    }
    true
}

fn fast_repo_root(start: &Path) -> Option<PathBuf> {
    // Convert to absolute path before traversing so parent() works correctly
    // on relative paths like "." (where ".".parent() returns Some("") not the actual parent)
    let start = if start.is_absolute() {
        start.to_path_buf()
    } else {
        std::env::current_dir().ok()?.join(start)
    };

    let mut current = Some(start.as_path());
    while let Some(dir) = current {
        if std::fs::symlink_metadata(dir.join(".git")).is_ok() {
            return Some(dir.to_path_buf());
        }
        current = dir.parent();
    }
    None
}

/// Load CanonicalState from the beads store ref.
///
/// Errors if ref doesn't exist (uninitialized repo).
pub fn load_state(repo: &Repository) -> Result<CanonicalState, Error> {
    let oid = repo
        .refname_to_id("refs/heads/beads/store")
        .map_err(|_| SyncError::NoLocalRef("refs/heads/beads/store".into()))?;
    Ok(read_state_at_oid(repo, oid)?.state)
}

/// Load state and metadata from the beads store ref.
///
/// Errors if ref doesn't exist (uninitialized repo).
pub fn load_store(repo: &Repository) -> Result<LoadedStore, Error> {
    let oid = repo
        .refname_to_id("refs/heads/beads/store")
        .map_err(|_| SyncError::NoLocalRef("refs/heads/beads/store".into()))?;
    Ok(read_state_at_oid(repo, oid)?)
}
