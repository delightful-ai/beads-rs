#![allow(dead_code)]

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::Mutex;

use assert_cmd::Command;
use tempfile::TempDir;

use super::daemon_runtime::shutdown_daemon;

static ENV_LOCK: Mutex<()> = Mutex::new(());

pub struct RealtimeFixture {
    _lock: std::sync::MutexGuard<'static, ()>,
    runtime_dir: TempDir,
    repo_dir: TempDir,
    #[allow(dead_code)]
    remote_dir: TempDir,
    data_dir: PathBuf,
    prev_env: EnvBackup,
}

impl RealtimeFixture {
    pub fn new() -> Self {
        let lock = ENV_LOCK.lock().expect("env lock poisoned");
        let runtime_dir = TempDir::new().expect("create runtime dir");
        let repo_dir = TempDir::new().expect("create repo dir");
        let remote_dir = TempDir::new().expect("create remote dir");

        init_git_repo(repo_dir.path(), remote_dir.path());

        let data_dir = runtime_dir.path().join("data");
        fs::create_dir_all(&data_dir).expect("create test data dir");

        let prev_env = EnvBackup::capture();
        unsafe {
            std::env::set_var("XDG_RUNTIME_DIR", runtime_dir.path());
            std::env::set_var("BD_WAL_DIR", runtime_dir.path());
            std::env::set_var("BD_DATA_DIR", &data_dir);
            std::env::set_var("BD_NO_AUTO_UPGRADE", "1");
        }

        Self {
            _lock: lock,
            runtime_dir,
            repo_dir,
            remote_dir,
            data_dir,
            prev_env,
        }
    }

    pub fn repo_path(&self) -> &Path {
        self.repo_dir.path()
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn bd(&self) -> Command {
        let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("bd");
        cmd.current_dir(self.repo_dir.path());
        cmd.env("XDG_RUNTIME_DIR", self.runtime_dir.path());
        cmd.env("BD_WAL_DIR", self.runtime_dir.path());
        cmd.env("BD_DATA_DIR", &self.data_dir);
        cmd.env("BD_NO_AUTO_UPGRADE", "1");
        cmd
    }

    pub fn start_daemon(&self) {
        self.bd().arg("init").assert().success();
    }
}

impl Drop for RealtimeFixture {
    fn drop(&mut self) {
        shutdown_daemon(self.runtime_dir.path());
        self.prev_env.restore();
    }
}

struct EnvBackup {
    xdg_runtime_dir: Option<OsString>,
    wal_dir: Option<OsString>,
    data_dir: Option<OsString>,
    no_auto_upgrade: Option<OsString>,
}

impl EnvBackup {
    fn capture() -> Self {
        Self {
            xdg_runtime_dir: std::env::var_os("XDG_RUNTIME_DIR"),
            wal_dir: std::env::var_os("BD_WAL_DIR"),
            data_dir: std::env::var_os("BD_DATA_DIR"),
            no_auto_upgrade: std::env::var_os("BD_NO_AUTO_UPGRADE"),
        }
    }

    fn restore(&mut self) {
        restore_var("XDG_RUNTIME_DIR", self.xdg_runtime_dir.take());
        restore_var("BD_WAL_DIR", self.wal_dir.take());
        restore_var("BD_DATA_DIR", self.data_dir.take());
        restore_var("BD_NO_AUTO_UPGRADE", self.no_auto_upgrade.take());
    }
}

fn restore_var(name: &str, value: Option<OsString>) {
    match value {
        Some(val) => unsafe { std::env::set_var(name, val) },
        None => unsafe { std::env::remove_var(name) },
    }
}

fn init_git_repo(repo_dir: &Path, remote_dir: &Path) {
    StdCommand::new("git")
        .args(["init", "--bare"])
        .current_dir(remote_dir)
        .output()
        .expect("git init --bare");

    StdCommand::new("git")
        .args(["init"])
        .current_dir(repo_dir)
        .output()
        .expect("git init");

    StdCommand::new("git")
        .args(["config", "user.email", "test@test.com"])
        .current_dir(repo_dir)
        .output()
        .expect("git config email");

    StdCommand::new("git")
        .args(["config", "user.name", "Test"])
        .current_dir(repo_dir)
        .output()
        .expect("git config name");

    StdCommand::new("git")
        .args(["remote", "add", "origin", remote_dir.to_str().unwrap()])
        .current_dir(repo_dir)
        .output()
        .expect("git remote add origin");
}
