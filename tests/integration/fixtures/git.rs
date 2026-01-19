use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use git2::Repository;
use tempfile::Builder;

static TEMPLATE_REPO_PATH: OnceLock<Result<PathBuf, String>> = OnceLock::new();

pub fn init_bare_repo(path: &Path) -> Result<(), String> {
    Repository::init_bare(path)
        .map_err(|err| format!("git init --bare failed for {path:?}: {err}"))?;
    Ok(())
}

pub fn init_repo(path: &Path) -> Result<Repository, String> {
    let template = template_repo_path()?;
    copy_dir_all(template, path)?;
    Repository::open(path).map_err(|err| format!("open repo failed for {path:?}: {err}"))
}

pub fn init_repo_with_origin(repo_dir: &Path, remote_dir: &Path) -> Result<(), String> {
    let repo = init_repo(repo_dir)?;
    add_origin_remote(&repo, remote_dir)?;
    Ok(())
}

pub fn repo_has_branch(repo_dir: &Path, branch: &str) -> Result<bool, String> {
    let repo = Repository::open(repo_dir)
        .map_err(|err| format!("open repo failed for {repo_dir:?}: {err}"))?;
    let refname = format!("refs/heads/{branch}");
    Ok(repo.find_reference(&refname).is_ok())
}

fn configure_test_repo(repo: &Repository) -> Result<(), String> {
    let mut cfg = repo
        .config()
        .map_err(|err| format!("open repo config failed: {err}"))?;
    cfg.set_str("user.name", "Test")
        .map_err(|err| format!("set user.name failed: {err}"))?;
    cfg.set_str("user.email", "test@test.com")
        .map_err(|err| format!("set user.email failed: {err}"))?;
    Ok(())
}

fn add_origin_remote(repo: &Repository, remote_dir: &Path) -> Result<(), String> {
    let remote = remote_dir
        .to_str()
        .ok_or_else(|| format!("remote dir path is not utf8: {remote_dir:?}"))?;
    repo.remote("origin", remote)
        .map_err(|err| format!("git remote add origin failed: {err}"))?;
    Ok(())
}

fn template_repo_path() -> Result<&'static Path, String> {
    match TEMPLATE_REPO_PATH.get_or_init(|| {
        let dir = Builder::new()
            .prefix("beads-git-template")
            .tempdir()
            .map_err(|err| format!("create template dir failed: {err}"))?;
        let path = dir.keep();
        let repo = Repository::init(&path)
            .map_err(|err| format!("git init failed for {path:?}: {err}"))?;
        configure_test_repo(&repo)?;
        Ok(path)
    }) {
        Ok(path) => Ok(path.as_path()),
        Err(err) => Err(err.clone()),
    }
}

fn copy_dir_all(src: &Path, dst: &Path) -> Result<(), String> {
    fs::create_dir_all(dst).map_err(|err| format!("create dir failed for {dst:?}: {err}"))?;
    for entry in fs::read_dir(src).map_err(|err| format!("read dir failed for {src:?}: {err}"))? {
        let entry = entry.map_err(|err| format!("read dir entry failed: {err}"))?;
        let file_type = entry
            .file_type()
            .map_err(|err| format!("stat file type failed: {err}"))?;
        let source_path = entry.path();
        let dest_path = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&source_path, &dest_path)?;
        } else if file_type.is_file() {
            fs::copy(&source_path, &dest_path)
                .map_err(|err| format!("copy failed for {source_path:?}: {err}"))?;
        } else if file_type.is_symlink() {
            let target = fs::read_link(&source_path)
                .map_err(|err| format!("read link failed for {source_path:?}: {err}"))?;
            #[cfg(unix)]
            std::os::unix::fs::symlink(&target, &dest_path)
                .map_err(|err| format!("symlink failed for {dest_path:?}: {err}"))?;
            #[cfg(not(unix))]
            return Err(format!(
                "symlink copy unsupported for {source_path:?} on this platform"
            ));
        }
    }
    Ok(())
}
