use std::env;
use std::fs;
use std::path::Path;
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

    set_env_var("GIT_DIR", repo_root.join(".git"));
    set_env_var("GIT_WORK_TREE", &worktree);

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

#[test]
fn discovers_repo_from_nested_jj_workspace_path() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    let _env_guard = EnvGuard::capture(&["GIT_DIR", "GIT_WORK_TREE", "PATH", "FAKE_JJ_GIT_ROOT"]);

    let temp = tempfile::tempdir().expect("tempdir");
    let repo_root = temp.path().join("repo");
    let workspace_root = temp.path().join("workspace");
    let nested = workspace_root.join("a/b/c");
    let bin_dir = temp.path().join("bin");

    git2::Repository::init(&repo_root).expect("init repo");
    fs::create_dir_all(workspace_root.join(".jj")).expect("create workspace marker");
    fs::create_dir_all(&nested).expect("create nested workspace dir");
    fs::create_dir_all(&bin_dir).expect("create bin dir");

    write_fake_jj_binary(&bin_dir.join("jj"));

    let existing_path = env::var_os("PATH").unwrap_or_default();
    let mut combined_path = std::ffi::OsString::new();
    combined_path.push(&bin_dir);
    if !existing_path.is_empty() {
        combined_path.push(std::ffi::OsStr::new(":"));
        combined_path.push(existing_path);
    }

    set_env_var("PATH", combined_path);
    set_env_var("FAKE_JJ_GIT_ROOT", repo_root.join(".git"));

    let discovered = beads_bootstrap::repo::discover_root(&nested)
        .expect("discover repo root from jj workspace");
    let (repo, root) =
        beads_bootstrap::repo::discover(&nested).expect("discover repo handle from jj workspace");

    assert_eq!(discovered, workspace_root);
    assert_eq!(root, workspace_root);
    assert_eq!(
        fs::canonicalize(repo.path()).expect("canonicalize git dir"),
        fs::canonicalize(repo_root.join(".git")).expect("canonicalize backing git dir")
    );
    assert_eq!(
        repo.workdir()
            .map(fs::canonicalize)
            .transpose()
            .expect("canonicalize repo workdir"),
        Some(fs::canonicalize(&repo_root).expect("canonicalize repo root"))
    );
}

fn write_fake_jj_binary(path: &Path) {
    fs::write(
        path,
        concat!(
            "#!/bin/sh\n",
            "if [ \"$1\" = \"git\" ] && [ \"$2\" = \"root\" ]; then\n",
            "  printf '%s\\n' \"$FAKE_JJ_GIT_ROOT\"\n",
            "  exit 0\n",
            "fi\n",
            "echo \"unexpected jj invocation: $*\" >&2\n",
            "exit 1\n",
        ),
    )
    .expect("write fake jj script");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut perms = fs::metadata(path).expect("script metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("mark fake jj executable");
    }
}

#[allow(unsafe_code)]
fn set_env_var<K: AsRef<std::ffi::OsStr>, V: AsRef<std::ffi::OsStr>>(key: K, value: V) {
    unsafe {
        env::set_var(key, value);
    }
}

#[allow(unsafe_code)]
fn remove_env_var<K: AsRef<std::ffi::OsStr>>(key: K) {
    unsafe {
        env::remove_var(key);
    }
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
            remove_env_var(key);
        }
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, value) in &self.saved {
            match value {
                Some(value) => set_env_var(key, value),
                None => remove_env_var(key),
            }
        }
    }
}
