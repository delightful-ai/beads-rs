//! Phase 7 tests: admin status and metrics IPC ops.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;

use assert_cmd::Command;
use tempfile::TempDir;

struct AdminFixture {
    runtime_dir: TempDir,
    repo_dir: TempDir,
    remote_dir: TempDir,
}

impl AdminFixture {
    fn new() -> Self {
        let runtime_dir = TempDir::new().expect("create runtime dir");
        let repo_dir = TempDir::new().expect("create repo dir");
        let remote_dir = TempDir::new().expect("create remote dir");

        init_git_repo(repo_dir.path(), remote_dir.path());

        Self {
            runtime_dir,
            repo_dir,
            remote_dir,
        }
    }

    fn data_dir(&self) -> PathBuf {
        let dir = self.runtime_dir.path().join("data");
        fs::create_dir_all(&dir).expect("create test data dir");
        dir
    }

    fn bd(&self) -> Command {
        let data_dir = self.data_dir();
        let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("bd");
        cmd.current_dir(self.repo_dir.path());
        cmd.env("XDG_RUNTIME_DIR", self.runtime_dir.path());
        cmd.env("BD_WAL_DIR", self.runtime_dir.path());
        cmd.env("BD_DATA_DIR", &data_dir);
        cmd.env("BD_NO_AUTO_UPGRADE", "1");
        cmd
    }

    fn start_daemon(&self) {
        self.bd().arg("init").assert().success();
    }

    fn create_issue(&self, title: &str) {
        self.bd().args(["create", title]).assert().success();
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

#[test]
fn admin_status_includes_expected_fields() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin status test");

    let output = fixture
        .bd()
        .args(["admin", "status", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let payload: serde_json::Value = serde_json::from_slice(&output).expect("parse json");
    assert_eq!(payload["result"], "admin_status");
    let data = &payload["data"];
    assert!(data["store_id"].is_string());
    assert!(data["replica_id"].is_string());
    assert!(data["namespaces"].is_array());
    assert!(data["watermarks_applied"].is_object());
    assert!(data["watermarks_durable"].is_object());
    assert!(data["wal"].is_array());
    assert!(data["checkpoints"].is_array());
}

#[test]
fn admin_metrics_includes_counters() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin metrics test");

    let output = fixture
        .bd()
        .args(["admin", "metrics", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let payload: serde_json::Value = serde_json::from_slice(&output).expect("parse json");
    assert_eq!(payload["result"], "admin_metrics");
    let counters = payload["data"]["counters"]
        .as_array()
        .expect("counters array");
    let has_wal_append = counters
        .iter()
        .any(|counter| counter["name"].as_str() == Some("wal_append_ok"));
    assert!(
        has_wal_append || !counters.is_empty(),
        "expected wal_append_ok or any counters"
    );
}
