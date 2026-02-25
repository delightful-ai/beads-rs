//! Repository discovery helpers used by daemon runtime/config loading.

use std::path::{Path, PathBuf};

use git2::Repository;

pub fn discover_root(start: impl AsRef<Path>) -> Result<PathBuf, git2::Error> {
    discover_root_inner(start.as_ref())
}

pub fn discover_root_optional(start: impl AsRef<Path>) -> Option<PathBuf> {
    discover_root_inner(start.as_ref()).ok()
}

fn discover_root_inner(start: &Path) -> Result<PathBuf, git2::Error> {
    if should_fast_discover()
        && let Some(root) = fast_repo_root(start)
        && Repository::open(&root).is_ok()
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
