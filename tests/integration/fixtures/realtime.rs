#![allow(dead_code)]

use assert_cmd::Command;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use tempfile::TempDir;

use super::daemon_runtime::shutdown_daemon;
use beads_rs::daemon::ipc::IpcClient;

pub struct RealtimeFixture {
    runtime_dir: TempDir,
    repo_dir: TempDir,
    #[allow(dead_code)]
    remote_dir: TempDir,
    data_dir: PathBuf,
}

impl RealtimeFixture {
    pub fn new() -> Self {
        let runtime_dir = TempDir::new().expect("create runtime dir");
        let repo_dir = TempDir::new().expect("create repo dir");
        let remote_dir = TempDir::new().expect("create remote dir");

        init_git_repo(repo_dir.path(), remote_dir.path());

        let data_dir = runtime_dir.path().join("data");
        fs::create_dir_all(&data_dir).expect("create test data dir");

        Self {
            runtime_dir,
            repo_dir,
            remote_dir,
            data_dir,
        }
    }

    pub fn repo_path(&self) -> &Path {
        self.repo_dir.path()
    }

    pub fn runtime_dir(&self) -> &Path {
        self.runtime_dir.path()
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

    pub fn ipc_client(&self) -> IpcClient {
        IpcClient::for_runtime_dir(self.runtime_dir())
    }

    pub fn start_daemon(&self) {
        self.bd().arg("init").assert().success();
    }
}

impl Drop for RealtimeFixture {
    fn drop(&mut self) {
        shutdown_daemon(self.runtime_dir.path());
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
