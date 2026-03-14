use std::env;
use std::fs;
use std::sync::{LazyLock, Mutex};

static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[test]
fn discovers_repo_root_from_nested_worktree_path() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    let _env_guard = EnvGuard::capture(&["GIT_DIR", "GIT_WORK_TREE"]);

    let temp = tempfile::tempdir().expect("tempdir");
    let repo_root = temp.path().join("repo");
    let nested = repo_root.join("a/b/c");

    git2::Repository::init(&repo_root).expect("init repo");
    fs::create_dir_all(&nested).expect("create nested dir");

    let discovered =
        beads_bootstrap::repo::discover_root(&nested).expect("discover repo root from nested dir");

    assert_eq!(discovered, repo_root);
}

#[test]
fn discovers_repo_from_git_dir_and_work_tree_env() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    let _env_guard = EnvGuard::capture(&["GIT_DIR", "GIT_WORK_TREE"]);

    let temp = tempfile::tempdir().expect("tempdir");
    let repo_root = temp.path().join("repo");
    let worktree = temp.path().join("worktree");
    let probe = temp.path().join("outside");

    git2::Repository::init(&repo_root).expect("init repo");
    fs::create_dir_all(&worktree).expect("create worktree");
    fs::create_dir_all(&probe).expect("create probe path");

    unsafe {
        env::set_var("GIT_DIR", repo_root.join(".git"));
        env::set_var("GIT_WORK_TREE", &worktree);
    }

    let discovered_root =
        beads_bootstrap::repo::discover_root(&probe).expect("discover worktree root from git env");
    let (repo, root) =
        beads_bootstrap::repo::discover(&probe).expect("discover repo handle from git env");

    let expected_root = fs::canonicalize(&worktree).expect("canonicalize worktree");

    assert_eq!(
        fs::canonicalize(&discovered_root).expect("canonicalize discovered root"),
        expected_root
    );
    assert_eq!(
        fs::canonicalize(&root).expect("canonicalize repo root"),
        expected_root
    );
    assert_eq!(
        repo.workdir()
            .map(fs::canonicalize)
            .transpose()
            .expect("canonicalize repo workdir"),
        Some(expected_root)
    );
}

struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    fn capture(keys: &[&'static str]) -> Self {
        let saved = keys
            .iter()
            .map(|key| (*key, env::var(key).ok()))
            .collect::<Vec<_>>();
        for key in keys {
            unsafe {
                env::remove_var(key);
            }
        }
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, value) in &self.saved {
            match value {
                Some(value) => unsafe {
                    env::set_var(key, value);
                },
                None => unsafe {
                    env::remove_var(key);
                },
            }
        }
    }
}
