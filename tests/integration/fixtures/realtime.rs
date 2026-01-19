#![allow(dead_code)]

use assert_cmd::Command;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

use super::daemon_runtime::shutdown_daemon;
use beads_rs::daemon::ipc::{IpcClient, Request, Response, ResponsePayload};
use beads_rs::api::QueryResult;

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

        init_git_repo(repo_dir.path(), remote_dir.path())
            .unwrap_or_else(|err| panic!("git fixture init failed: {err}"));

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
        cmd.env("BD_TESTING", "1");
        cmd
    }

    pub fn ipc_client(&self) -> IpcClient {
        IpcClient::for_runtime_dir(self.runtime_dir())
    }

    pub fn start_daemon(&self) {
        let client = self.ipc_client().with_autostart(false);
        if !ping_daemon(&client) {
            let mut cmd = StdCommand::new(assert_cmd::cargo::cargo_bin!("bd"));
            cmd.current_dir(self.repo_dir.path());
            cmd.env("XDG_RUNTIME_DIR", self.runtime_dir.path());
            cmd.env("BD_WAL_DIR", self.runtime_dir.path());
            cmd.env("BD_DATA_DIR", &self.data_dir);
            cmd.env("BD_NO_AUTO_UPGRADE", "1");
            cmd.env("BD_TESTING", "1");
            cmd.args(["daemon", "run"]);
            cmd.stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            cmd.spawn().expect("spawn daemon");

            let ok = poll_until(Duration::from_secs(5), || ping_daemon(&client));
            assert!(ok, "daemon failed to start");
        }

        let request = Request::Init {
            repo: self.repo_dir.path().to_path_buf(),
        };
        let response = client
            .send_request_no_autostart(&request)
            .expect("init response");
        match response {
            Response::Ok {
                ok: ResponsePayload::Initialized(_),
            } => {}
            other => panic!("unexpected init response: {other:?}"),
        }
    }
}

impl Drop for RealtimeFixture {
    fn drop(&mut self) {
        shutdown_daemon(self.runtime_dir.path());
    }
}

fn init_git_repo(repo_dir: &Path, remote_dir: &Path) -> Result<(), String> {
    run_git(&["init", "--bare"], remote_dir)?;
    run_git(&["init"], repo_dir)?;
    run_git(&["config", "user.email", "test@test.com"], repo_dir)?;
    run_git(&["config", "user.name", "Test"], repo_dir)?;

    let remote = remote_dir
        .to_str()
        .ok_or_else(|| format!("remote dir path is not utf8: {remote_dir:?}"))?;
    run_git(&["remote", "add", "origin", remote], repo_dir)?;
    Ok(())
}

fn run_git(args: &[&str], cwd: &Path) -> Result<(), String> {
    let output = StdCommand::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .map_err(|err| format!("git {:?} failed to start in {:?}: {err}", args, cwd))?;
    if output.status.success() {
        return Ok(());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(format!(
        "git {:?} failed in {:?} (status {}): stdout: {stdout} stderr: {stderr}",
        args, cwd, output.status
    ))
}

fn ping_daemon(client: &IpcClient) -> bool {
    matches!(
        client.send_request_no_autostart(&Request::Ping),
        Ok(Response::Ok {
            ok: ResponsePayload::Query(QueryResult::DaemonInfo(_)),
        })
    )
}

fn poll_until<F>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(10);
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        std::thread::sleep(backoff);
        backoff = std::cmp::min(backoff.saturating_mul(2), Duration::from_millis(100));
    }
    condition()
}
