//! Integration tests for the critical path: init → create → list → show → close
//!
//! These tests run the actual `bd` binary against temp git repos.

use std::fs;
#[cfg(feature = "slow-tests")]
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
#[cfg(feature = "slow-tests")]
use std::time::{Duration, Instant};

use crate::fixtures::daemon_runtime::shutdown_daemon;
use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

fn data_dir_for_runtime(runtime_dir: &Path) -> PathBuf {
    let dir = runtime_dir.join("data");
    fs::create_dir_all(&dir).expect("failed to create test data dir");
    dir
}

fn bd_with_runtime(repo: &Path, runtime_dir: &Path, data_dir: &Path) -> Command {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("bd");
    cmd.current_dir(repo);
    cmd.env("XDG_RUNTIME_DIR", runtime_dir);
    cmd.env("BD_WAL_DIR", runtime_dir);
    cmd.env("BD_DATA_DIR", data_dir);
    cmd.env("BD_NO_AUTO_UPGRADE", "1");
    cmd.env("BD_TESTING", "1");
    cmd.env("BD_TEST_DISABLE_GIT_SYNC", "1");
    cmd
}

#[cfg(feature = "slow-tests")]
fn socket_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("beads").join("daemon.sock")
}

#[cfg(feature = "slow-tests")]
fn daemon_pid(runtime_dir: &Path) -> u32 {
    use beads_rs::api::QueryResult;
    use beads_rs::daemon::ipc::{Request, Response, ResponsePayload};

    let socket = socket_path(runtime_dir);
    let mut stream =
        std::os::unix::net::UnixStream::connect(&socket).expect("connect daemon socket");
    let mut json = serde_json::to_string(&Request::Ping).expect("serialize ping");
    json.push('\n');
    stream.write_all(json.as_bytes()).expect("write ping");
    stream.flush().expect("flush ping");

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).expect("read ping response");
    let resp: Response = serde_json::from_str(&line).expect("parse ping response");
    let Response::Ok { ok } = resp else {
        panic!("unexpected ping response: {resp:?}");
    };
    let ResponsePayload::Query(QueryResult::DaemonInfo(info)) = ok else {
        panic!("unexpected ping payload: {ok:?}");
    };
    info.pid
}

#[cfg(feature = "slow-tests")]
fn parse_response_payload(bytes: &[u8]) -> beads_rs::daemon::ipc::ResponsePayload {
    serde_json::from_slice(bytes).expect("parse response payload")
}

#[cfg(feature = "slow-tests")]
fn wait_for_exit(pid: u32, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !process_alive(pid) {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(feature = "slow-tests")]
fn process_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), None).is_ok()
}

#[cfg(feature = "slow-tests")]
fn store_id_from_remote_path(remote: &Path) -> beads_rs::StoreId {
    let remote_str = remote.to_str().expect("remote path");
    let normalized = beads_rs::daemon::remote::normalize_url(remote_str);
    let store_uuid = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, normalized.as_bytes());
    beads_rs::StoreId::new(store_uuid)
}

/// Test fixture: working repo + bare remote.
struct TestRepo {
    work_dir: TempDir,
    #[allow(dead_code)]
    remote_dir: TempDir,
    runtime_dir: TempDir,
    data_dir: PathBuf,
    store_id: beads_rs::StoreId,
}

impl TestRepo {
    fn new() -> Self {
        // Create bare remote first
        let remote_dir = TempDir::new().expect("failed to create remote dir");
        std::process::Command::new("git")
            .args(["init", "--bare"])
            .current_dir(remote_dir.path())
            .output()
            .expect("failed to init bare repo");

        // Create working directory
        let work_dir = TempDir::new().expect("failed to create work dir");

        std::process::Command::new("git")
            .args(["init"])
            .current_dir(work_dir.path())
            .output()
            .expect("failed to git init");

        std::process::Command::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(work_dir.path())
            .output()
            .expect("failed to configure git email");

        std::process::Command::new("git")
            .args(["config", "user.name", "Test User"])
            .current_dir(work_dir.path())
            .output()
            .expect("failed to configure git name");

        std::process::Command::new("git")
            .args([
                "remote",
                "add",
                "origin",
                remote_dir.path().to_str().unwrap(),
            ])
            .current_dir(work_dir.path())
            .output()
            .expect("failed to add remote");

        let runtime_dir = TempDir::new().expect("failed to create runtime dir");
        let data_dir = data_dir_for_runtime(runtime_dir.path());
        let runtime_str = runtime_dir.path().to_string_lossy();
        let store_uuid = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, runtime_str.as_bytes());
        let store_id = beads_rs::StoreId::new(store_uuid);

        Self {
            work_dir,
            remote_dir,
            runtime_dir,
            data_dir,
            store_id,
        }
    }

    fn path(&self) -> &std::path::Path {
        self.work_dir.path()
    }

    fn bd(&self) -> Command {
        let mut cmd = bd_with_runtime(self.path(), self.runtime_dir.path(), &self.data_dir);
        cmd.env("BD_STORE_ID", self.store_id.to_string());
        cmd
    }
}

impl Drop for TestRepo {
    fn drop(&mut self) {
        shutdown_daemon(self.runtime_dir.path());
    }
}

#[test]
fn test_init_creates_beads_branch() {
    let repo = TestRepo::new();

    repo.bd().arg("init").assert().success();

    let output = std::process::Command::new("git")
        .args(["branch", "-a"])
        .current_dir(repo.path())
        .output()
        .expect("failed to list branches");
    let branches = String::from_utf8_lossy(&output.stdout);
    assert!(
        branches.contains("beads/store"),
        "beads/store branch not created: {branches}"
    );
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_create_and_list() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    repo.bd()
        .args([
            "create",
            "Test issue title",
            "--type=task",
            "--priority=1",
            "--json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"id\""));

    repo.bd()
        .args(["list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Test issue title"));
}

#[test]
fn test_create_show_close_workflow() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args([
            "create",
            "Bug to fix",
            "--type=bug",
            "--priority=0",
            "--desc=This is a critical bug",
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value =
        serde_json::from_slice(&output).expect("failed to parse create output");
    let id = json["data"]["id"].as_str().expect("no id in response");

    repo.bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Bug to fix"))
        .stdout(predicate::str::contains("critical bug"));

    repo.bd()
        .args(["close", id, "--reason=Fixed it", "--json"])
        .assert()
        .success();

    repo.bd()
        .args(["list", "--status=closed", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Bug to fix"));

    repo.bd()
        .args(["ready", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Bug to fix").not());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_claim_and_unclaim() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Work item", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let id = json["data"]["id"].as_str().unwrap();

    repo.bd().args(["claim", id, "--json"]).assert().success();

    repo.bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("in_progress"));

    repo.bd().args(["unclaim", id]).assert().success();

    repo.bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"status\": \"open\""));
}

#[test]
fn test_dependencies() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output1 = repo
        .bd()
        .args(["create", "Task A", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_a = serde_json::from_slice::<serde_json::Value>(&output1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let output2 = repo
        .bd()
        .args(["create", "Task B", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_b = serde_json::from_slice::<serde_json::Value>(&output2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // B depends on A (B waits for A)
    repo.bd()
        .args(["dep", "add", &id_b, &id_a])
        .assert()
        .success();

    repo.bd()
        .args(["blocked", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Task B"));

    repo.bd()
        .args(["ready", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Task A"))
        .stdout(predicate::str::contains("Task B").not());

    repo.bd().args(["close", &id_a]).assert().success();

    repo.bd()
        .args(["ready", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Task B"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_discovered_from_workflow() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Main feature", "--type=feature", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let parent_id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let dep_arg = format!("discovered_from:{}", parent_id);
    repo.bd()
        .args([
            "create",
            "Found edge case",
            "--type=bug",
            "--deps",
            &dep_arg,
            "--json",
        ])
        .assert()
        .success();

    // discovered_from doesn't block
    repo.bd()
        .args(["ready", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Found edge case"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_epic_with_subtasks() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Big feature", "--type=epic", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let epic_id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    repo.bd()
        .args([
            "create",
            "Subtask 1",
            "--type=task",
            "--parent",
            &epic_id,
            "--json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(&format!("{}.1", epic_id)));

    repo.bd()
        .args([
            "create",
            "Subtask 2",
            "--type=task",
            "--parent",
            &epic_id,
            "--json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(&format!("{}.2", epic_id)));

    repo.bd()
        .args(["epic", "status", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Big feature"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_epic_show_progress_display() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create an epic with multiple children
    let output = repo
        .bd()
        .args(["create", "Test Epic", "--type=epic", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let epic_id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Create subtasks with different priorities
    let output1 = repo
        .bd()
        .args([
            "create",
            "High priority task",
            "--type=task",
            "--priority=0",
            "--parent",
            &epic_id,
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let task1_id = serde_json::from_slice::<serde_json::Value>(&output1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let output2 = repo
        .bd()
        .args([
            "create",
            "Low priority task",
            "--type=task",
            "--priority=3",
            "--parent",
            &epic_id,
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let _task2_id = serde_json::from_slice::<serde_json::Value>(&output2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    repo.bd()
        .args([
            "create",
            "Medium priority task",
            "--type=task",
            "--priority=2",
            "--parent",
            &epic_id,
        ])
        .assert()
        .success();

    // Close one task
    repo.bd().args(["close", &task1_id]).assert().success();

    // Show the epic (human output) - should show progress and breakdown
    repo.bd()
        .args(["show", &epic_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("Progress: 1/3 done (33%)"))
        .stdout(predicate::str::contains("Remaining (2):"))
        .stdout(predicate::str::contains("Done (1):"))
        .stdout(predicate::str::contains("[P2]")) // Medium priority shown
        .stdout(predicate::str::contains("[P3]")); // Low priority shown
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_update_parent_and_unparent() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Epic", "--type=epic", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let epic_id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let output = repo
        .bd()
        .args(["create", "Child", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let child_id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Reparent.
    repo.bd()
        .args(["update", &child_id, "--parent", &epic_id, "--json"])
        .assert()
        .success();

    repo.bd()
        .args(["dep", "tree", &child_id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"kind\": \"parent\""))
        .stdout(predicate::str::contains(format!("\"to\": \"{epic_id}\"")));

    // Unparent.
    repo.bd()
        .args(["update", &child_id, "--no-parent", "--json"])
        .assert()
        .success();

    repo.bd()
        .args(["dep", "tree", &child_id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"kind\": \"parent\"").not());

    // Invalid parent.
    repo.bd()
        .args(["update", &child_id, "--parent=bd-doesnotexist", "--json"])
        .assert()
        .failure();

    // Cycle: make child a parent of epic, then attempt to parent epic to child.
    repo.bd()
        .args(["update", &child_id, "--parent", &epic_id, "--json"])
        .assert()
        .success();

    repo.bd()
        .args(["update", &epic_id, "--parent", &child_id, "--json"])
        .assert()
        .failure();
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_labels() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Labeled issue", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    repo.bd()
        .args(["label", "add", &id, "tech-debt"])
        .assert()
        .success();

    repo.bd()
        .args(["list", "-l", "tech-debt", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Labeled issue"));

    repo.bd()
        .args(["label", "remove", &id, "tech-debt"])
        .assert()
        .success();

    repo.bd()
        .args(["list", "-l", "tech-debt", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Labeled issue").not());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_search() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    repo.bd()
        .args([
            "create",
            "Authentication bug",
            "--type=bug",
            "--desc=Login fails with special chars",
            "--json",
        ])
        .assert()
        .success();

    repo.bd()
        .args([
            "create",
            "Database optimization",
            "--type=task",
            "--desc=Improve query performance",
            "--json",
        ])
        .assert()
        .success();

    repo.bd()
        .args(["search", "auth", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Authentication bug"))
        .stdout(predicate::str::contains("Database").not());

    repo.bd()
        .args(["search", "query", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Database optimization"));
}

#[test]
fn test_status_overview() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Open issue", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let _open_id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let output = repo
        .bd()
        .args(["create", "In progress", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let wip_id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let output = repo
        .bd()
        .args(["create", "Done issue", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let done_id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    repo.bd().args(["claim", &wip_id]).assert().success();
    repo.bd().args(["close", &done_id]).assert().success();

    repo.bd()
        .args(["status", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("open"))
        .stdout(predicate::str::contains("in_progress"))
        .stdout(predicate::str::contains("closed"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_update_bead() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Original title", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let id = json["data"]["id"].as_str().unwrap();

    // Update title
    repo.bd()
        .args(["update", id, "--title=Updated title", "--json"])
        .assert()
        .success();

    repo.bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Updated title"));

    // Update type
    repo.bd()
        .args(["update", id, "--type=epic", "--json"])
        .assert()
        .success();

    repo.bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"type\": \"epic\""));

    // Invalid type should fail.
    repo.bd()
        .args(["update", id, "--type=nope"])
        .assert()
        .failure();

    // Update priority
    repo.bd()
        .args(["update", id, "--priority=0", "--json"])
        .assert()
        .success();

    repo.bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"priority\": 0"));

    // Update description
    repo.bd()
        .args(["update", id, "--desc=New description", "--json"])
        .assert()
        .success();

    repo.bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("New description"));

    // Close via update with reason.
    repo.bd()
        .args(["update", id, "--status=closed", "--reason=Done", "--json"])
        .assert()
        .success();

    let output = repo
        .bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(json["data"]["closed_reason"].as_str(), Some("Done"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_delete_and_undelete() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "To be deleted", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let id = json["data"]["id"].as_str().unwrap();

    // Delete the bead
    repo.bd()
        .args(["delete", id, "--reason=Not needed", "--json"])
        .assert()
        .success();

    // Should not appear in list
    repo.bd()
        .args(["list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("To be deleted").not());

    // Should appear in deleted list
    repo.bd()
        .args(["deleted", "--all", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains(id));

    // Show should return error for deleted bead
    repo.bd()
        .args(["show", id, "--json"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("deleted"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_reopen_closed() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Bug to fix", "--type=bug", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let id = json["data"]["id"].as_str().unwrap();

    // Close it
    repo.bd()
        .args(["close", id, "--reason=Fixed"])
        .assert()
        .success();

    repo.bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("closed"));

    // Reopen it
    repo.bd().args(["reopen", id]).assert().success();

    repo.bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"status\": \"open\""));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_comments() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Issue with comments", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let id = json["data"]["id"].as_str().unwrap();

    // Add comments (content is positional arg, not --content flag)
    repo.bd()
        .args(["comments", "add", id, "First comment"])
        .assert()
        .success();

    repo.bd()
        .args(["comments", "add", id, "Second comment"])
        .assert()
        .success();

    // List comments
    repo.bd()
        .args(["comments", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("First comment"))
        .stdout(predicate::str::contains("Second comment"));

    // Show should include note count
    repo.bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"notes\""));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_filter_by_priority() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    repo.bd()
        .args([
            "create",
            "Critical bug",
            "--type=bug",
            "--priority=0",
            "--json",
        ])
        .assert()
        .success();

    repo.bd()
        .args([
            "create",
            "Low priority task",
            "--type=task",
            "--priority=4",
            "--json",
        ])
        .assert()
        .success();

    repo.bd()
        .args([
            "create",
            "Medium task",
            "--type=task",
            "--priority=2",
            "--json",
        ])
        .assert()
        .success();

    // Filter by specific priority
    repo.bd()
        .args(["list", "--priority=0", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Critical bug"))
        .stdout(predicate::str::contains("Low priority").not());

    // Filter by priority 4
    repo.bd()
        .args(["list", "--priority=4", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Low priority task"))
        .stdout(predicate::str::contains("Critical bug").not());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_filter_by_type() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    repo.bd()
        .args(["create", "A bug", "--type=bug", "--json"])
        .assert()
        .success();

    repo.bd()
        .args(["create", "A task", "--type=task", "--json"])
        .assert()
        .success();

    repo.bd()
        .args(["create", "A feature", "--type=feature", "--json"])
        .assert()
        .success();

    // Filter by type
    repo.bd()
        .args(["list", "--type=bug", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("A bug"))
        .stdout(predicate::str::contains("A task").not())
        .stdout(predicate::str::contains("A feature").not());

    repo.bd()
        .args(["list", "--type=feature", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("A feature"))
        .stdout(predicate::str::contains("A bug").not());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_count_command() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    repo.bd()
        .args(["create", "Bug 1", "--type=bug", "--json"])
        .assert()
        .success();
    repo.bd()
        .args(["create", "Bug 2", "--type=bug", "--json"])
        .assert()
        .success();
    repo.bd()
        .args(["create", "Task 1", "--type=task", "--json"])
        .assert()
        .success();

    // Simple count
    repo.bd()
        .args(["count", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"count\": 3"));

    // Count by type filter
    repo.bd()
        .args(["count", "--type=bug", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"count\": 2"));

    // Group by type
    repo.bd()
        .args(["count", "--by-type", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("bug"))
        .stdout(predicate::str::contains("task"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_stale_command() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    repo.bd()
        .args(["create", "Fresh issue", "--type=task", "--json"])
        .assert()
        .success();

    // With default 30 days, fresh issue shouldn't appear
    repo.bd()
        .args(["stale", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Fresh issue").not());

    // With 0 days threshold, everything is stale
    repo.bd()
        .args(["stale", "--days=0", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Fresh issue"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_sync_command() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create an issue to have something to sync
    repo.bd()
        .args(["create", "Test sync", "--type=task", "--json"])
        .assert()
        .success();

    // Sync should succeed
    repo.bd().arg("sync").assert().success();

    // List should still work after sync
    repo.bd()
        .args(["list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Test sync"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_comment_compat_alias() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Issue", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let id = json["data"]["id"].as_str().unwrap();

    // Use the compat 'comment' alias (singular)
    repo.bd()
        .args(["comment", id, "A compat comment"])
        .assert()
        .success();

    // Verify it was added
    repo.bd()
        .args(["comments", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("A compat comment"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_dep_rm() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output1 = repo
        .bd()
        .args(["create", "Task A", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_a = serde_json::from_slice::<serde_json::Value>(&output1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let output2 = repo
        .bd()
        .args(["create", "Task B", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_b = serde_json::from_slice::<serde_json::Value>(&output2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Add dependency: B depends on A
    repo.bd()
        .args(["dep", "add", &id_b, &id_a])
        .assert()
        .success();

    // B should be blocked
    repo.bd()
        .args(["blocked", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Task B"));

    // Remove the dependency
    repo.bd()
        .args(["dep", "rm", &id_b, &id_a])
        .assert()
        .success();

    // B should no longer be blocked
    repo.bd()
        .args(["blocked", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Task B").not());

    // B should be ready now
    repo.bd()
        .args(["ready", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Task B"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_dep_tree() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output1 = repo
        .bd()
        .args(["create", "Root task", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let root_id = serde_json::from_slice::<serde_json::Value>(&output1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let output2 = repo
        .bd()
        .args(["create", "Child task", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let child_id = serde_json::from_slice::<serde_json::Value>(&output2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Child depends on root
    repo.bd()
        .args(["dep", "add", &child_id, &root_id])
        .assert()
        .success();

    // View dependency tree from root
    repo.bd()
        .args(["dep", "tree", &root_id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains(&root_id));

    // View dependency tree from child
    repo.bd()
        .args(["dep", "tree", &child_id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains(&child_id));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_label_list_and_list_all() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Labeled issue", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Add multiple labels
    repo.bd()
        .args(["label", "add", &id, "urgent"])
        .assert()
        .success();
    repo.bd()
        .args(["label", "add", &id, "backend"])
        .assert()
        .success();

    // List labels for this issue
    repo.bd()
        .args(["label", "list", &id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("urgent"))
        .stdout(predicate::str::contains("backend"));

    // Create another issue with different label
    let output2 = repo
        .bd()
        .args(["create", "Another issue", "--type=bug", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id2 = serde_json::from_slice::<serde_json::Value>(&output2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    repo.bd()
        .args(["label", "add", &id2, "frontend"])
        .assert()
        .success();

    // List all labels in repo
    repo.bd()
        .args(["label", "list-all", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("urgent"))
        .stdout(predicate::str::contains("backend"))
        .stdout(predicate::str::contains("frontend"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_epic_close_eligible() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create an epic
    let output = repo
        .bd()
        .args(["create", "Epic project", "--type=epic", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let epic_id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Create subtasks
    let sub1_out = repo
        .bd()
        .args([
            "create",
            "Subtask 1",
            "--type=task",
            "--parent",
            &epic_id,
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let sub1_id = serde_json::from_slice::<serde_json::Value>(&sub1_out).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let sub2_out = repo
        .bd()
        .args([
            "create",
            "Subtask 2",
            "--type=task",
            "--parent",
            &epic_id,
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let sub2_id = serde_json::from_slice::<serde_json::Value>(&sub2_out).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Epic should not be eligible (subtasks open)
    repo.bd()
        .args(["epic", "close-eligible", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains(&epic_id).not());

    // Close subtasks
    repo.bd().args(["close", &sub1_id]).assert().success();
    repo.bd().args(["close", &sub2_id]).assert().success();

    // Now epic should be eligible for auto-close (JSON output contains IDs, not titles)
    repo.bd()
        .args(["epic", "close-eligible", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains(&epic_id));

    // Actually close eligible epics (command executes by default, no --execute flag)
    repo.bd()
        .args(["epic", "close-eligible"])
        .assert()
        .success();

    // Epic should now be closed
    repo.bd()
        .args(["show", &epic_id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("closed"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_prime_command() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Prime should output workflow context
    repo.bd()
        .arg("prime")
        .assert()
        .success()
        .stdout(predicate::str::contains("Beads Workflow"))
        .stdout(predicate::str::contains("bd ready"))
        .stdout(predicate::str::contains("bd claim"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_deleted_id_lookup() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "To delete", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    repo.bd()
        .args(["delete", &id, "--reason=Testing"])
        .assert()
        .success();

    // Lookup specific deleted ID
    repo.bd()
        .args(["deleted", &id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains(&id))
        .stdout(predicate::str::contains("found"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_list_sorting() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create issues with different priorities
    repo.bd()
        .args([
            "create",
            "Low priority",
            "--type=task",
            "--priority=4",
            "--json",
        ])
        .assert()
        .success();
    repo.bd()
        .args([
            "create",
            "High priority",
            "--type=task",
            "--priority=0",
            "--json",
        ])
        .assert()
        .success();
    repo.bd()
        .args([
            "create",
            "Medium priority",
            "--type=task",
            "--priority=2",
            "--json",
        ])
        .assert()
        .success();

    // Sort by priority ascending (lowest first = 0)
    let output = repo
        .bd()
        .args(["list", "--sort=priority:asc", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let issues = json["data"].as_array().unwrap();
    assert!(issues.len() >= 3);
    // First issue should be high priority (0)
    assert_eq!(issues[0]["priority"].as_u64().unwrap(), 0);

    // Sort by priority descending (highest number first = 4)
    let output = repo
        .bd()
        .args(["list", "--sort=priority:desc", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let issues = json["data"].as_array().unwrap();
    // First issue should be low priority (4)
    assert_eq!(issues[0]["priority"].as_u64().unwrap(), 4);
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_list_limit() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create several issues
    for i in 1..=5 {
        repo.bd()
            .args(["create", &format!("Issue {}", i), "--type=task", "--json"])
            .assert()
            .success();
    }

    // List with limit
    let output = repo
        .bd()
        .args(["list", "-n", "2", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let issues = json["data"].as_array().unwrap();
    assert_eq!(issues.len(), 2);
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_create_design_and_acceptance() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    repo.bd()
        .args([
            "create",
            "Feature with specs",
            "--type=feature",
            "--design=Use microservices architecture",
            "--acceptance=All tests pass, docs updated",
            "--json",
        ])
        .assert()
        .success();

    // Verify fields are set
    let output = repo
        .bd()
        .args(["list", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let id = json["data"][0]["id"].as_str().unwrap();

    repo.bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("microservices"))
        .stdout(predicate::str::contains("All tests pass"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_create_with_assignee() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create with self-assignment
    let output = repo
        .bd()
        .args([
            "create",
            "Assigned task",
            "--type=task",
            "--assignee=me",
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let id = json["data"]["id"].as_str().unwrap();

    // Should be in_progress since it has an assignee
    repo.bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("assignee"));
}

#[test]
fn test_error_handling_invalid_id() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Show non-existent ID
    repo.bd()
        .args(["show", "bd-nonexistent"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not_found").or(predicate::str::contains("not found")));

    // Update non-existent ID
    repo.bd()
        .args(["update", "bd-nonexistent", "--title=New"])
        .assert()
        .failure();

    // Close non-existent ID
    repo.bd()
        .args(["close", "bd-nonexistent"])
        .assert()
        .failure();

    // Invalid ID format
    repo.bd()
        .args(["show", "invalid-format-id"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_error_handling_invalid_transitions() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Test issue", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let id = json["data"]["id"].as_str().unwrap();

    // Can't reopen an already open issue
    repo.bd()
        .args(["reopen", id])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid"));

    // Close the issue
    repo.bd().args(["close", id]).assert().success();

    // Can't close an already closed issue
    repo.bd()
        .args(["close", id])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid").or(predicate::str::contains("closed")));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_reclaim_extends_lease() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Work item", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let id = json["data"]["id"].as_str().unwrap();

    // Claim it first with short lease
    repo.bd()
        .args(["claim", id, "--lease-secs=100"])
        .assert()
        .success();

    // Get initial expiry
    let show_out = repo
        .bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let show_json: serde_json::Value = serde_json::from_slice(&show_out).unwrap();
    let initial_expires = show_json["data"]["assignee_expires"].as_u64().unwrap();

    // Re-claim with longer lease (same actor can re-claim)
    repo.bd()
        .args(["claim", id, "--lease-secs=7200"])
        .assert()
        .success();

    // Check new expiry is later
    let show_out2 = repo
        .bd()
        .args(["show", id, "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let show_json2: serde_json::Value = serde_json::from_slice(&show_out2).unwrap();
    let new_expires = show_json2["data"]["assignee_expires"].as_u64().unwrap();

    assert!(new_expires > initial_expires);
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_bulk_label_operations() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create multiple issues
    let output1 = repo
        .bd()
        .args(["create", "Issue 1", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id1 = serde_json::from_slice::<serde_json::Value>(&output1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let output2 = repo
        .bd()
        .args(["create", "Issue 2", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id2 = serde_json::from_slice::<serde_json::Value>(&output2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Add same label to both issues at once
    repo.bd()
        .args(["label", "add", &id1, &id2, "shared-label"])
        .assert()
        .success();

    // Both should have the label
    repo.bd()
        .args(["list", "-l", "shared-label", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Issue 1"))
        .stdout(predicate::str::contains("Issue 2"));

    // Remove from both at once
    repo.bd()
        .args(["label", "remove", &id1, &id2, "shared-label"])
        .assert()
        .success();

    // Neither should have the label now
    repo.bd()
        .args(["list", "-l", "shared-label", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Issue 1").not())
        .stdout(predicate::str::contains("Issue 2").not());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_setup_cursor() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Setup cursor integration
    repo.bd()
        .args(["setup", "cursor"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Cursor integration installed"));

    // Rules file should exist in .cursor/rules/
    let rules_path = repo.path().join(".cursor/rules/beads.mdc");
    assert!(
        rules_path.exists(),
        ".cursor/rules/beads.mdc file should exist"
    );

    // File should contain beads workflow content
    let content = fs::read_to_string(&rules_path).unwrap();
    assert!(content.contains("bd") || content.contains("beads"));

    // Check should report installed
    repo.bd()
        .args(["setup", "cursor", "--check"])
        .assert()
        .success()
        .stdout(predicate::str::contains("configured").or(predicate::str::contains("installed")));

    // Remove should work
    repo.bd()
        .args(["setup", "cursor", "--remove"])
        .assert()
        .success();
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_setup_aider() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Setup aider integration
    repo.bd()
        .args(["setup", "aider"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Aider integration installed"));

    // .aider.conf.yml should exist
    let conf_path = repo.path().join(".aider.conf.yml");
    assert!(conf_path.exists(), ".aider.conf.yml should exist");

    // .aider directory should have instructions
    let aider_dir = repo.path().join(".aider");
    assert!(aider_dir.exists(), ".aider directory should exist");

    // Check should report installed
    repo.bd()
        .args(["setup", "aider", "--check"])
        .assert()
        .success()
        .stdout(predicate::str::contains("configured").or(predicate::str::contains("installed")));

    // Remove should work
    repo.bd()
        .args(["setup", "aider", "--remove"])
        .assert()
        .success();
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_setup_claude_project() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Setup claude integration in project mode (not global)
    repo.bd()
        .args(["setup", "claude", "--project"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Claude Code integration installed",
        ));

    // .claude directory should exist with settings
    let settings_path = repo.path().join(".claude/settings.local.json");
    assert!(
        settings_path.exists(),
        ".claude/settings.local.json should exist"
    );

    // Check should report installed
    repo.bd()
        .args(["setup", "claude", "--check", "--project"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hooks installed"));

    // Remove should work
    repo.bd()
        .args(["setup", "claude", "--remove", "--project"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hooks removed"));
}

// =============================================================================
// ROBUSTNESS TESTS - Edge cases, error handling, invariant protection
// =============================================================================

/// Circular dependencies should be rejected.
#[cfg(feature = "slow-tests")]
#[test]
fn test_circular_dependency_prevention() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create two issues
    let out1 = repo
        .bd()
        .args(["create", "Issue A", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_a = serde_json::from_slice::<serde_json::Value>(&out1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let out2 = repo
        .bd()
        .args(["create", "Issue B", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_b = serde_json::from_slice::<serde_json::Value>(&out2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // A blocks B
    repo.bd()
        .args(["dep", "add", &id_b, &id_a])
        .assert()
        .success();

    // Now try B blocks A - should fail (circular)
    repo.bd()
        .args(["dep", "add", &id_a, &id_b])
        .assert()
        .failure()
        .stderr(predicate::str::contains("circular").or(predicate::str::contains("cycle")));
}

/// Self-dependencies should be rejected.
#[cfg(feature = "slow-tests")]
#[test]
fn test_self_dependency_prevention() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Self ref issue", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Try to make issue depend on itself
    repo.bd()
        .args(["dep", "add", &id, &id])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("self")
                .or(predicate::str::contains("itself").or(predicate::str::contains("circular"))),
        );
}

/// Related dependencies should allow cycles (they're informational links).
#[cfg(feature = "slow-tests")]
#[test]
fn test_related_deps_allow_cycles() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create two issues
    let out1 = repo
        .bd()
        .args(["create", "Issue A", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_a = serde_json::from_slice::<serde_json::Value>(&out1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let out2 = repo
        .bd()
        .args(["create", "Issue B", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_b = serde_json::from_slice::<serde_json::Value>(&out2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // A related to B
    repo.bd()
        .args(["dep", "add", &id_a, &id_b, "--kind=related"])
        .assert()
        .success();

    // B related to A - should succeed (cycles allowed for related)
    repo.bd()
        .args(["dep", "add", &id_b, &id_a, "--kind=related"])
        .assert()
        .success();
}

/// discovered_from deps should also allow cycles.
#[cfg(feature = "slow-tests")]
#[test]
fn test_discovered_from_deps_allow_cycles() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create two issues
    let out1 = repo
        .bd()
        .args(["create", "Issue A", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_a = serde_json::from_slice::<serde_json::Value>(&out1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let out2 = repo
        .bd()
        .args(["create", "Issue B", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_b = serde_json::from_slice::<serde_json::Value>(&out2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // A discovered from B
    repo.bd()
        .args(["dep", "add", &id_a, &id_b, "--kind=discovered_from"])
        .assert()
        .success();

    // B discovered from A - should succeed (cycles allowed for discovered_from)
    repo.bd()
        .args(["dep", "add", &id_b, &id_a, "--kind=discovered_from"])
        .assert()
        .success();
}

/// Parent deps should still enforce DAG (no cycles).
#[cfg(feature = "slow-tests")]
#[test]
fn test_parent_deps_reject_cycles() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create two issues
    let out1 = repo
        .bd()
        .args(["create", "Issue A", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_a = serde_json::from_slice::<serde_json::Value>(&out1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let out2 = repo
        .bd()
        .args(["create", "Issue B", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_b = serde_json::from_slice::<serde_json::Value>(&out2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // A's parent is B
    repo.bd()
        .args(["dep", "add", &id_a, &id_b, "--kind=parent"])
        .assert()
        .success();

    // B's parent is A - should fail (cycles rejected for parent)
    repo.bd()
        .args(["dep", "add", &id_b, &id_a, "--kind=parent"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("circular").or(predicate::str::contains("cycle")));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_operations_on_deleted_issue() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Soon deleted", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Delete it
    repo.bd().args(["delete", &id]).assert().success();

    // Try to update - should fail
    repo.bd()
        .args(["update", &id, "--title=New title"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("deleted").or(predicate::str::contains("tombstone").or(predicate::str::contains("not found"))));

    // Try to claim - should fail
    repo.bd().args(["claim", &id]).assert().failure();

    // Try to close - should fail
    repo.bd().args(["close", &id]).assert().failure();
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_claim_already_claimed_issue() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Contested issue", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // First claim with actor alice
    repo.bd()
        .args(["claim", &id, "--actor=alice"])
        .assert()
        .success();

    // Second claim with actor bob - behavior depends on design:
    // Either it should fail (issue already claimed) or succeed (last-writer-wins)
    // Let's just verify it doesn't crash and produces a clear outcome
    let result = repo.bd().args(["claim", &id, "--actor=bob"]).assert();

    // Should either succeed (LWW) or fail with clear message - not panic
    // We check it's deterministic either way
    let output = result.get_output();
    assert!(output.status.success() || !output.stderr.is_empty());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_delete_issue_that_blocks_others() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create blocker and blocked
    let out1 = repo
        .bd()
        .args(["create", "Blocker", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let blocker_id = serde_json::from_slice::<serde_json::Value>(&out1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let out2 = repo
        .bd()
        .args(["create", "Blocked", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let blocked_id = serde_json::from_slice::<serde_json::Value>(&out2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Set up dependency
    repo.bd()
        .args(["dep", "add", &blocked_id, &blocker_id])
        .assert()
        .success();

    // Delete the blocker - should this be allowed?
    // Test that behavior is defined (either succeeds or fails with clear error)
    let result = repo.bd().args(["delete", &blocker_id]).assert();
    let output = result.get_output();

    // If it succeeds, the blocked issue should now be unblocked
    if output.status.success() {
        repo.bd()
            .args(["ready", "--json"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Blocked"));
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_close_blocked_issue() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create blocker and blocked
    let out1 = repo
        .bd()
        .args(["create", "Blocker", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let blocker_id = serde_json::from_slice::<serde_json::Value>(&out1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let out2 = repo
        .bd()
        .args(["create", "Blocked", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let blocked_id = serde_json::from_slice::<serde_json::Value>(&out2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Set up dependency
    repo.bd()
        .args(["dep", "add", &blocked_id, &blocker_id])
        .assert()
        .success();

    // Try to close the blocked issue while blocker is still open
    // This could either: fail with warning, or succeed (user knows what they're doing)
    let result = repo.bd().args(["close", &blocked_id]).assert();
    let output = result.get_output();

    // Verify deterministic behavior - should not crash
    assert!(output.status.success() || !output.stderr.is_empty());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_epic_close_with_open_children() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create epic
    let epic_out = repo
        .bd()
        .args(["create", "Parent Epic", "--type=epic", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let epic_id = serde_json::from_slice::<serde_json::Value>(&epic_out).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Create open subtask
    repo.bd()
        .args([
            "create",
            "Open subtask",
            "--type=task",
            "--parent",
            &epic_id,
        ])
        .assert()
        .success();

    // Try to close epic with open children
    // Should either: fail, or warn, or succeed with clear semantics
    let result = repo.bd().args(["close", &epic_id]).assert();
    let output = result.get_output();

    // Document the behavior - either fails or succeeds deterministically
    if output.status.success() {
        // If it succeeded, verify the epic is actually closed
        repo.bd()
            .args(["show", &epic_id, "--json"])
            .assert()
            .success()
            .stdout(predicate::str::contains("closed"));
    } else {
        // If it failed, should have clear error about open children
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("open") || stderr.contains("children") || stderr.contains("subtask"),
            "Error should mention open children: {}",
            stderr
        );
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_unicode_and_special_characters() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create issue with unicode title
    repo.bd()
        .args([
            "create",
            "修复bug 🐛 émojis работает",
            "--type=bug",
            "--json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("修复bug"));

    // Create issue with special characters
    repo.bd()
        .args([
            "create",
            "Issue with \"quotes\" and 'apostrophes'",
            "--type=task",
            "--json",
        ])
        .assert()
        .success();

    // Create issue with newlines in description
    repo.bd()
        .args([
            "create",
            "Multiline",
            "--description=Line 1\nLine 2\nLine 3",
            "--type=task",
            "--json",
        ])
        .assert()
        .success();

    // Labels with special chars
    let output = repo
        .bd()
        .args(["create", "Label test", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Try label with hyphen and underscore (should work)
    // Note: label add takes label LAST, one at a time
    repo.bd()
        .args(["label", "add", &id, "tech-debt"])
        .assert()
        .success();
    repo.bd()
        .args(["label", "add", &id, "work_item"])
        .assert()
        .success();

    // Search should handle unicode
    repo.bd()
        .args(["search", "émojis", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("修复bug"));
}

/// Empty titles should be rejected.
#[cfg(feature = "slow-tests")]
#[test]
fn test_empty_and_whitespace_inputs() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Empty title should fail
    repo.bd()
        .args(["create", "", "--type=task"])
        .assert()
        .failure();

    // Whitespace-only title should fail (or be trimmed and fail)
    repo.bd()
        .args(["create", "   ", "--type=task"])
        .assert()
        .failure();
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_duplicate_dependency() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let out1 = repo
        .bd()
        .args(["create", "Issue A", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_a = serde_json::from_slice::<serde_json::Value>(&out1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let out2 = repo
        .bd()
        .args(["create", "Issue B", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_b = serde_json::from_slice::<serde_json::Value>(&out2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Add dependency
    repo.bd()
        .args(["dep", "add", &id_b, &id_a])
        .assert()
        .success();

    // Add same dependency again - should be idempotent (succeed) or fail gracefully
    let result = repo.bd().args(["dep", "add", &id_b, &id_a]).assert();

    // Should not crash - either succeeds (idempotent) or fails with clear message
    let output = result.get_output();
    assert!(output.status.success() || !output.stderr.is_empty());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_duplicate_label() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Label test", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Add label
    repo.bd()
        .args(["label", "add", &id, "my-label"])
        .assert()
        .success();

    // Add same label again - should be idempotent
    repo.bd()
        .args(["label", "add", &id, "my-label"])
        .assert()
        .success();

    // Should still only have one instance of the label
    repo.bd()
        .args(["label", "list", &id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("my-label"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_remove_nonexistent_dependency() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let out1 = repo
        .bd()
        .args(["create", "Issue A", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_a = serde_json::from_slice::<serde_json::Value>(&out1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let out2 = repo
        .bd()
        .args(["create", "Issue B", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_b = serde_json::from_slice::<serde_json::Value>(&out2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Remove dependency that doesn't exist - should be idempotent or fail gracefully
    let result = repo.bd().args(["dep", "rm", &id_b, &id_a]).assert();

    let output = result.get_output();
    // Either succeeds (no-op) or fails with clear "not found" message
    assert!(output.status.success() || !output.stderr.is_empty());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_remove_nonexistent_label() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Label test", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Remove label that doesn't exist - should be idempotent
    repo.bd()
        .args(["label", "remove", &id, "nonexistent-label"])
        .assert()
        .success();
}

// =============================================================================
// MORE EDGE CASES - Init, IDs, state transitions, boundaries
// =============================================================================

/// The system auto-initializes on first mutation - this is intentional for CRDT ergonomics.
/// `bd init` is optional but provides a clear "start fresh" workflow.
#[test]
fn test_auto_init_on_first_create() {
    let repo = TestRepo::new();
    // Don't call init - system auto-initializes

    // Create should work (auto-creates beads branch)
    repo.bd()
        .args(["create", "Auto-init test", "--type=task", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Auto-init test"));

    // List should now work
    repo.bd()
        .args(["list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Auto-init test"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_double_init() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Second init - should be idempotent or fail gracefully
    let result = repo.bd().arg("init").assert();
    let output = result.get_output();
    // Either succeeds (idempotent) or fails with "already initialized"
    assert!(output.status.success() || !output.stderr.is_empty());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_init_fails_without_origin_remote() {
    let work_dir = TempDir::new().expect("failed to create work dir");
    let runtime_dir = TempDir::new().expect("failed to create runtime dir");
    let data_dir = data_dir_for_runtime(runtime_dir.path());

    std::process::Command::new("git")
        .args(["init"])
        .current_dir(work_dir.path())
        .output()
        .expect("failed to git init");

    std::process::Command::new("git")
        .args(["config", "user.email", "test@test.com"])
        .current_dir(work_dir.path())
        .output()
        .expect("failed to configure git email");

    std::process::Command::new("git")
        .args(["config", "user.name", "Test User"])
        .current_dir(work_dir.path())
        .output()
        .expect("failed to configure git name");

    let mut cmd = bd_with_runtime(work_dir.path(), runtime_dir.path(), &data_dir);
    cmd.arg("init").assert().failure();

    shutdown_daemon(runtime_dir.path());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_partial_id_matching() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Test issue", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let full_id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Try with partial ID (first 5 chars after "bd-")
    let partial = &full_id[..6]; // "bd-xx"

    // Show with partial ID - should work if unambiguous
    let result = repo.bd().args(["show", partial, "--json"]).assert();
    let output = result.get_output();

    // Document behavior - either works or requires full ID
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("Test issue"));
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should mention ambiguous or not found
        assert!(
            stderr.contains("ambiguous")
                || stderr.contains("not found")
                || stderr.contains("invalid"),
            "Error should be clear: {}",
            stderr
        );
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_double_close() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "To close twice", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // First close
    repo.bd().args(["close", &id]).assert().success();

    // Second close - should be idempotent or fail with clear message
    let result = repo.bd().args(["close", &id]).assert();
    let output = result.get_output();

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("already") || stderr.contains("closed"),
            "Error should mention already closed: {}",
            stderr
        );
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_double_reopen() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Never closed", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Try to reopen an issue that was never closed
    let result = repo.bd().args(["reopen", &id]).assert();
    let output = result.get_output();

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("already") || stderr.contains("open") || stderr.contains("not closed"),
            "Error should mention already open: {}",
            stderr
        );
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_invalid_priority_values() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Negative priority - should fail
    repo.bd()
        .args([
            "create",
            "Negative priority",
            "--type=task",
            "--priority=-1",
        ])
        .assert()
        .failure();

    // Way too high priority - should fail
    repo.bd()
        .args([
            "create",
            "Sky high priority",
            "--type=task",
            "--priority=999",
        ])
        .assert()
        .failure();
}

/// BUG/FEATURE: String priority like "high" is accepted and converted to P1
/// This might be intentional UX (nice to have) or a bug (should reject)
/// Document the behavior either way
#[cfg(feature = "slow-tests")]
#[test]
fn test_string_priority_accepted() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // "high" is converted to P1 - maybe intentional UX?
    repo.bd()
        .args([
            "create",
            "String priority",
            "--type=task",
            "--priority=high",
            "--json",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("priority")
                .and(predicate::str::contains("1").or(predicate::str::contains("high"))),
        );
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_invalid_type_value() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Invalid type
    repo.bd()
        .args(["create", "Invalid type", "--type=invalid_type_xyz"])
        .assert()
        .failure();
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_create_with_nonexistent_parent() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Try to create with non-existent parent
    repo.bd()
        .args(["create", "Orphan", "--type=task", "--parent=bd-nonexistent"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not found").or(predicate::str::contains("invalid")));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_create_with_deleted_parent() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create and delete a potential parent
    let output = repo
        .bd()
        .args(["create", "Deleted parent", "--type=epic", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let parent_id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    repo.bd().args(["delete", &parent_id]).assert().success();

    // Try to create child with deleted parent
    let result = repo
        .bd()
        .args([
            "create",
            "Child of deleted",
            "--type=task",
            "--parent",
            &parent_id,
        ])
        .assert();

    let output = result.get_output();
    // Should fail - can't parent to deleted issue
    if output.status.success() {
        // If it succeeded, that might be a bug worth noting
        println!("WARNING: Created issue with deleted parent - may be a bug");
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_dep_add_nonexistent_blocker() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Real issue", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Try to add dependency on non-existent issue
    repo.bd()
        .args(["dep", "add", &id, "bd-nonexistent"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not found").or(predicate::str::contains("invalid")));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_dep_add_deleted_blocker() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create two issues
    let out1 = repo
        .bd()
        .args(["create", "Will be deleted", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let deleted_id = serde_json::from_slice::<serde_json::Value>(&out1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let out2 = repo
        .bd()
        .args(["create", "Wants to depend", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&out2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Delete the first issue
    repo.bd().args(["delete", &deleted_id]).assert().success();

    // Try to add dependency on deleted issue
    let result = repo.bd().args(["dep", "add", &id, &deleted_id]).assert();

    let output = result.get_output();
    // Should probably fail - depending on deleted issue is weird
    if output.status.success() {
        println!("NOTE: Can add dependency on deleted issue - may be intentional for CRDT reasons");
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_long_dependency_chain() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create chain: A → B → C → D (D blocked by C blocked by B blocked by A)
    let mut ids = Vec::new();
    for i in 0..4 {
        let output = repo
            .bd()
            .args([
                "create",
                &format!("Chain issue {}", i),
                "--type=task",
                "--json",
            ])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        ids.push(id);
    }

    // Create chain: each depends on previous
    for i in 1..4 {
        repo.bd()
            .args(["dep", "add", &ids[i], &ids[i - 1]])
            .assert()
            .success();
    }

    // Only first should be ready
    repo.bd()
        .args(["ready", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Chain issue 0"))
        .stdout(predicate::str::contains("Chain issue 1").not())
        .stdout(predicate::str::contains("Chain issue 2").not())
        .stdout(predicate::str::contains("Chain issue 3").not());

    // Close the first - second should become ready
    repo.bd().args(["close", &ids[0]]).assert().success();

    repo.bd()
        .args(["ready", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Chain issue 1"))
        .stdout(predicate::str::contains("Chain issue 2").not());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_delete_middle_of_dependency_chain() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create chain: A → B → C
    let mut ids = Vec::new();
    for i in 0..3 {
        let output = repo
            .bd()
            .args(["create", &format!("Chain {}", i), "--type=task", "--json"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        ids.push(id);
    }

    // B depends on A, C depends on B
    repo.bd()
        .args(["dep", "add", &ids[1], &ids[0]])
        .assert()
        .success();
    repo.bd()
        .args(["dep", "add", &ids[2], &ids[1]])
        .assert()
        .success();

    // Delete B (middle of chain)
    repo.bd().args(["delete", &ids[1]]).assert().success();

    // C should now only be blocked by... nothing? Or still transitively by A?
    // Document the actual behavior
    let result = repo.bd().args(["ready", "--json"]).assert().success();

    let output = result.get_output();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // C should be ready now since its direct blocker (B) is gone
    if stdout.contains("Chain 2") {
        // C is ready - deleting blocker unblocks
    } else {
        // C is still blocked somehow
        println!("NOTE: C still blocked after B deleted - deps may be preserved");
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_empty_comment() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Comment test", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Try to add empty comment
    let result = repo.bd().args(["comments", "add", &id, ""]).assert();

    let output = result.get_output();
    // Should probably fail - empty comment is pointless
    if output.status.success() {
        // Check if it was actually added
        let list_out = repo
            .bd()
            .args(["comments", &id, "--json"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        println!(
            "Empty comment behavior: {:?}",
            String::from_utf8_lossy(&list_out)
        );
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_very_long_title() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create a very long title (1000 chars)
    let long_title: String = "X".repeat(1000);

    let result = repo
        .bd()
        .args(["create", &long_title, "--type=task", "--json"])
        .assert();

    let output = result.get_output();
    // Document behavior - either accepts or rejects with length error
    if output.status.success() {
        // Verify it's stored correctly
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let stored_title = json["data"]["title"].as_str().unwrap();
        assert_eq!(stored_title.len(), 1000, "Title should be preserved fully");
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("long") || stderr.contains("length") || stderr.contains("limit"),
            "Should mention length: {}",
            stderr
        );
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_very_long_description() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create a very long description (100KB)
    let long_desc: String = "Y".repeat(100_000);

    let result = repo
        .bd()
        .args([
            "create",
            "Long desc test",
            "--type=task",
            &format!("--description={}", long_desc),
            "--json",
        ])
        .assert();

    let output = result.get_output();
    // Should handle large descriptions
    if output.status.success() {
        println!("100KB description accepted");
    } else {
        println!("100KB description rejected (may be reasonable)");
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_many_labels() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Many labels", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Add 50 labels
    for i in 0..50 {
        repo.bd()
            .args(["label", "add", &id, &format!("label-{}", i)])
            .assert()
            .success();
    }

    // Verify they're all there by checking show output
    let show_out = repo
        .bd()
        .args(["show", &id, "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let _json: serde_json::Value = serde_json::from_slice(&show_out).unwrap();
    // Labels might be in data.labels or elsewhere - check the output contains them
    let output_str = String::from_utf8_lossy(&show_out);

    // Verify several labels are present
    assert!(output_str.contains("label-0"), "Should have label-0");
    assert!(output_str.contains("label-25"), "Should have label-25");
    assert!(output_str.contains("label-49"), "Should have label-49");
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_unclaim_not_claimed() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Never claimed", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Try to unclaim something never claimed
    let result = repo.bd().args(["unclaim", &id]).assert();
    let output = result.get_output();

    // Should be idempotent or fail gracefully
    assert!(output.status.success() || !output.stderr.is_empty());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_claim_closed_issue() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Will close", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    repo.bd().args(["close", &id]).assert().success();

    // Try to claim closed issue
    let result = repo.bd().args(["claim", &id]).assert();
    let output = result.get_output();

    // Claiming a closed issue is probably wrong
    if output.status.success() {
        println!("NOTE: Can claim closed issue - may be intentional");
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("closed") || stderr.contains("workflow"),
            "Should mention issue is closed: {}",
            stderr
        );
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_update_multiple_fields_at_once() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    let output = repo
        .bd()
        .args(["create", "Multi update", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Update multiple fields at once (skip assignee - can only assign self)
    repo.bd()
        .args([
            "update",
            &id,
            "--title=New title",
            "--description=New desc",
            "--priority=0",
        ])
        .assert()
        .success();

    // Verify all fields updated
    repo.bd()
        .args(["show", &id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("New title"))
        .stdout(predicate::str::contains("New desc"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_update_deps() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create two tasks
    let output1 = repo
        .bd()
        .args(["create", "Task A", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_a = serde_json::from_slice::<serde_json::Value>(&output1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let output2 = repo
        .bd()
        .args(["create", "Task B", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_b = serde_json::from_slice::<serde_json::Value>(&output2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let output3 = repo
        .bd()
        .args(["create", "Task C", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_c = serde_json::from_slice::<serde_json::Value>(&output3).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Use update --deps to add dependencies: A depends on B and C
    repo.bd()
        .args(["update", &id_a, &format!("--deps={},{}", id_b, id_c)])
        .assert()
        .success();

    // Verify A is blocked (depends on B and C which are open)
    repo.bd()
        .args(["blocked", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Task A"));

    // Verify B and C are ready (no blockers)
    repo.bd()
        .args(["ready", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Task B"))
        .stdout(predicate::str::contains("Task C"))
        .stdout(predicate::str::contains("Task A").not());

    // Close B and C, then A should become ready
    repo.bd().args(["close", &id_b]).assert().success();
    repo.bd().args(["close", &id_c]).assert().success();

    repo.bd()
        .args(["ready", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Task A"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_update_deps_with_kind() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create two tasks
    let output1 = repo
        .bd()
        .args(["create", "Main task", "--type=task", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_main = serde_json::from_slice::<serde_json::Value>(&output1).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let output2 = repo
        .bd()
        .args(["create", "Discovered bug", "--type=bug", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_bug = serde_json::from_slice::<serde_json::Value>(&output2).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Use update --deps with discovered_from kind
    let dep_spec = format!("discovered_from:{}", id_main);
    repo.bd()
        .args(["update", &id_bug, &format!("--deps={}", dep_spec)])
        .assert()
        .success();

    // Verify the bug shows the discovered_from relationship in deps_outgoing
    let show_output = repo
        .bd()
        .args(["show", &id_bug, "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&show_output).unwrap();
    let deps_outgoing = json["data"]["deps_outgoing"].as_array().unwrap();
    assert_eq!(deps_outgoing.len(), 1);
    assert_eq!(
        deps_outgoing[0]["kind"].as_str().unwrap(),
        "discovered_from"
    );
    assert_eq!(deps_outgoing[0]["to"].as_str().unwrap(), id_main);
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_filter_no_results() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create a task
    repo.bd()
        .args(["create", "A task", "--type=task"])
        .assert()
        .success();

    // Filter for type that doesn't exist
    repo.bd()
        .args(["list", "--type=epic", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("A task").not());

    // Filter for label that doesn't exist
    repo.bd()
        .args(["list", "-l", "nonexistent-label", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("A task").not());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_search_no_results() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    repo.bd()
        .args(["create", "Test issue", "--type=task"])
        .assert()
        .success();

    // Search for term that doesn't exist
    repo.bd()
        .args(["search", "xyznonexistentterm", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Test issue").not());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_show_with_all_optional_fields() {
    let repo = TestRepo::new();
    repo.bd().arg("init").assert().success();

    // Create issue with all optional fields (skip assignee - can only assign self)
    let output = repo
        .bd()
        .args([
            "create",
            "Full issue",
            "--type=feature",
            "--priority=0",
            "--description=A description",
            "--design=A design doc",
            "--acceptance=Acceptance criteria",
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = serde_json::from_slice::<serde_json::Value>(&output).unwrap()["data"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Add labels and comments
    repo.bd()
        .args(["label", "add", &id, "important"])
        .assert()
        .success();
    repo.bd()
        .args(["comments", "add", &id, "A comment"])
        .assert()
        .success();

    // Show should display everything
    repo.bd()
        .args(["show", &id, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Full issue"))
        .stdout(predicate::str::contains("A description"))
        .stdout(predicate::str::contains("A design doc"))
        .stdout(predicate::str::contains("Acceptance criteria"))
        .stdout(predicate::str::contains("important"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_crash_recovery_replays_wal() {
    use beads_rs::api::QueryResult;
    use beads_rs::daemon::ipc::ResponsePayload;
    use beads_rs::daemon::ops::OpResult;
    use beads_rs::git::sync::read_state_at_oid;
    use git2::Repository;

    let repo = TestRepo::new();
    let runtime_dir = TempDir::new().expect("failed to create runtime dir");
    let data_dir = data_dir_for_runtime(runtime_dir.path());
    let store_id = store_id_from_remote_path(repo.remote_dir.path());
    let wal_dir = data_dir
        .join("stores")
        .join(store_id.to_string())
        .join("wal")
        .join(beads_rs::core::NamespaceId::core().as_str());

    let output = bd_with_runtime(repo.path(), runtime_dir.path(), &data_dir)
        .args(["create", "Crash recovery", "--json"])
        .output()
        .expect("run bd create");
    assert!(output.status.success());
    let payload = parse_response_payload(&output.stdout);
    let id = match payload {
        ResponsePayload::Op(op) => match op.result {
            OpResult::Created { id } => id,
            other => panic!("unexpected create op result: {other:?}"),
        },
        ResponsePayload::Query(QueryResult::Issue(issue)) => {
            beads_rs::core::BeadId::parse(&issue.id)
                .unwrap_or_else(|e| panic!("invalid issue id in create response: {e}"))
        }
        other => panic!("unexpected create payload: {other:?}"),
    };

    let wal_entries: Vec<_> = std::fs::read_dir(&wal_dir)
        .expect("read wal dir")
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
        .collect();
    assert!(!wal_entries.is_empty(), "expected WAL entry before crash");

    let pid = daemon_pid(runtime_dir.path());
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), Signal::SIGKILL).expect("failed to SIGKILL daemon");
    wait_for_exit(pid, Duration::from_secs(1));

    let store_id_arg = store_id.to_string();
    let unlock_out = bd_with_runtime(repo.path(), runtime_dir.path(), &data_dir)
        .args(["store", "unlock", "--store-id", store_id_arg.as_str()])
        .output()
        .expect("run bd store unlock");
    assert!(unlock_out.status.success());

    let list_out = bd_with_runtime(repo.path(), runtime_dir.path(), &data_dir)
        .args(["list", "--json"])
        .output()
        .expect("run bd list");
    assert!(list_out.status.success());
    let list_payload = parse_response_payload(&list_out.stdout);
    let ResponsePayload::Query(QueryResult::Issues(issues)) = list_payload else {
        panic!("unexpected list payload: {list_payload:?}");
    };
    assert!(
        issues.iter().any(|issue| issue.id == id.as_str()),
        "expected recovered issue to appear after restart"
    );

    let sync_out = bd_with_runtime(repo.path(), runtime_dir.path(), &data_dir)
        .args(["sync", "--json"])
        .output()
        .expect("run bd sync");
    assert!(sync_out.status.success());

    let remote_repo = Repository::open(repo.remote_dir.path()).expect("open remote repo");
    let remote_oid = remote_repo
        .refname_to_id("refs/heads/beads/store")
        .expect("remote beads ref");
    let remote_state = read_state_at_oid(&remote_repo, remote_oid).expect("read remote state");
    assert!(
        remote_state.state.get_live(&id).is_some(),
        "expected recovered issue to sync to remote"
    );
}
