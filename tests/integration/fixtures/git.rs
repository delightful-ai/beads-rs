use std::path::Path;

use git2::Repository;

pub fn init_bare_repo(path: &Path) -> Result<(), String> {
    Repository::init_bare(path)
        .map_err(|err| format!("git init --bare failed for {path:?}: {err}"))?;
    Ok(())
}

pub fn init_repo(path: &Path) -> Result<Repository, String> {
    let repo =
        Repository::init(path).map_err(|err| format!("git init failed for {path:?}: {err}"))?;
    configure_test_repo(&repo)?;
    Ok(repo)
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
