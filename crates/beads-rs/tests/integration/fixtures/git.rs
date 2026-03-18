use std::fs;
use std::path::Path;
use std::sync::OnceLock;

use git2::Repository;
use tempfile::Builder;

use super::timing;

static TEMPLATE_REPO: OnceLock<Result<TemplateRepo, String>> = OnceLock::new();

struct TemplateRepo {
    dir: tempfile::TempDir,
}

impl TemplateRepo {
    fn path(&self) -> &Path {
        self.dir.path()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BranchPresence {
    Present,
    Absent,
}

impl BranchPresence {
    pub fn is_present(self) -> bool {
        matches!(self, BranchPresence::Present)
    }
}

pub fn init_bare_repo(path: &Path) -> Result<(), String> {
    let _phase = timing::scoped_phase_with_context("fixture.git.init_bare_repo", path.display());
    Repository::init_bare(path)
        .map_err(|err| format!("git init --bare failed for {path:?}: {err}"))?;
    Ok(())
}

pub fn init_repo(path: &Path) -> Result<Repository, String> {
    let _phase = timing::scoped_phase_with_context("fixture.git.init_repo", path.display());
    let template = template_repo()?;
    copy_dir_all(template.path(), path)?;
    Repository::open(path).map_err(|err| format!("open repo failed for {path:?}: {err}"))
}

pub fn init_repo_with_origin(repo_dir: &Path, remote_dir: &Path) -> Result<(), String> {
    let _phase =
        timing::scoped_phase_with_context("fixture.git.init_repo_with_origin", repo_dir.display());
    let repo = init_repo(repo_dir)?;
    add_origin_remote(&repo, remote_dir)?;
    Ok(())
}

pub fn repo_has_branch(repo_dir: &Path, branch: &str) -> Result<BranchPresence, String> {
    let repo = Repository::open(repo_dir)
        .map_err(|err| format!("open repo failed for {repo_dir:?}: {err}"))?;
    let refname = format!("refs/heads/{branch}");
    let presence = if repo.find_reference(&refname).is_ok() {
        BranchPresence::Present
    } else {
        BranchPresence::Absent
    };
    Ok(presence)
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

fn template_repo() -> Result<&'static TemplateRepo, String> {
    match TEMPLATE_REPO.get_or_init(|| {
        let dir = Builder::new()
            .prefix("beads-git-template")
            .tempdir()
            .map_err(|err| format!("create template dir failed: {err}"))?;
        let repo = Repository::init(dir.path())
            .map_err(|err| format!("git init failed for {:?}: {err}", dir.path()))?;
        configure_test_repo(&repo)?;
        Ok(TemplateRepo { dir })
    }) {
        Ok(template) => Ok(template),
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

#[cfg(test)]
mod tests {
    use super::{init_repo, template_repo};

    #[test]
    fn template_repo_is_cached() {
        let left = template_repo().expect("template repo").path().to_path_buf();
        let right = template_repo().expect("template repo").path().to_path_buf();

        assert_eq!(left, right);
    }

    #[test]
    fn init_repo_copies_configured_identity() {
        let dir = tempfile::tempdir().expect("temp dir");
        let repo = init_repo(dir.path()).expect("init repo");
        let cfg = repo.config().expect("repo config");

        assert_eq!(cfg.get_string("user.name").expect("user.name"), "Test");
        assert_eq!(
            cfg.get_string("user.email").expect("user.email"),
            "test@test.com"
        );
    }
}
