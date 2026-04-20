use std::path::{Path, PathBuf};
use std::process::Command;

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
        && let Some((repo, root)) = fast_repo_discovery(start)
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

fn fast_repo_discovery(start: &Path) -> Option<(Repository, PathBuf)> {
    let start = absolute_path(start)?;
    let mut current = Some(start.as_path());
    while let Some(dir) = current {
        if std::fs::symlink_metadata(dir.join(".git")).is_ok()
            && let Ok(repo) = Repository::open(dir)
        {
            return Some((repo, dir.to_path_buf()));
        }
        if std::fs::symlink_metadata(dir.join(".jj")).is_ok()
            && let Some(repo) = open_jj_backing_repo(dir)
        {
            return Some((repo, dir.to_path_buf()));
        }
        current = dir.parent();
    }
    None
}

fn absolute_path(path: &Path) -> Option<PathBuf> {
    if path.is_absolute() {
        Some(path.to_path_buf())
    } else {
        std::env::current_dir().ok().map(|cwd| cwd.join(path))
    }
}

fn open_jj_backing_repo(workspace_root: &Path) -> Option<Repository> {
    let git_dir = jj_git_dir(workspace_root)?;
    Repository::open(git_dir).ok()
}

fn jj_git_dir(workspace_root: &Path) -> Option<PathBuf> {
    let output = Command::new("jj")
        .current_dir(workspace_root)
        .args(["git", "root"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let raw = String::from_utf8(output.stdout).ok()?;
    let path = raw.trim();
    if path.is_empty() {
        return None;
    }

    let git_dir = PathBuf::from(path);
    if git_dir.is_absolute() {
        Some(git_dir)
    } else {
        Some(workspace_root.join(git_dir))
    }
}
