use std::path::{Path, PathBuf};

use git2::Repository;

pub fn discover(start: impl AsRef<Path>) -> Result<(Repository, PathBuf), git2::Error> {
    discover_inner(start.as_ref())
}

pub fn discover_root(start: impl AsRef<Path>) -> Result<PathBuf, git2::Error> {
    discover_inner(start.as_ref()).map(|(_, root)| root)
}

pub fn discover_root_optional(start: impl AsRef<Path>) -> Option<PathBuf> {
    discover_inner(start.as_ref()).ok().map(|(_, root)| root)
}

fn discover_inner(start: &Path) -> Result<(Repository, PathBuf), git2::Error> {
    if should_fast_discover()
        && let Some(root) = fast_repo_root(start)
        && let Ok(repo) = Repository::open(&root)
    {
        return Ok((repo, root));
    }
    let repo = if git_env_override_enabled() {
        Repository::open_from_env()?
    } else {
        Repository::discover(start)?
    };
    let root = repo
        .workdir()
        .map(|path| path.to_owned())
        .ok_or_else(|| git2::Error::from_str("bare repository not supported"))?;
    Ok((repo, root))
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

fn git_env_override_enabled() -> bool {
    env_var_present("GIT_DIR") || env_var_present("GIT_WORK_TREE")
}

fn env_var_present(key: &str) -> bool {
    std::env::var_os(key).is_some_and(|value| !value.is_empty())
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
