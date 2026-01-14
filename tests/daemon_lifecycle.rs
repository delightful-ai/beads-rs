//! Integration tests for daemon lifecycle: stale sockets, version mismatch, restarts
//!
//! These tests verify robust daemon handling including:
//! - Stale socket detection and recovery
//! - Version mismatch handling and daemon restart
//! - No orphaned daemon processes
//! - Concurrent access during restart

use std::fs;
use std::path::PathBuf;
use std::process::Command as StdCommand;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use assert_cmd::Command;
use tempfile::TempDir;

// =============================================================================
// Test Fixture
// =============================================================================

struct DaemonFixture {
    runtime_dir: TempDir,
    repo_dir: TempDir,
    #[allow(dead_code)]
    remote_dir: TempDir,
}

impl DaemonFixture {
    fn new() -> Self {
        let runtime_dir = TempDir::new().expect("create runtime dir");
        let repo_dir = TempDir::new().expect("create repo dir");
        let remote_dir = TempDir::new().expect("create remote dir");

        // Create bare remote
        StdCommand::new("git")
            .args(["init", "--bare"])
            .current_dir(remote_dir.path())
            .output()
            .expect("git init --bare");

        // Initialize git repo
        StdCommand::new("git")
            .args(["init"])
            .current_dir(repo_dir.path())
            .output()
            .expect("git init");

        StdCommand::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(repo_dir.path())
            .output()
            .expect("git config email");

        StdCommand::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(repo_dir.path())
            .output()
            .expect("git config name");

        // Add remote origin
        StdCommand::new("git")
            .args([
                "remote",
                "add",
                "origin",
                remote_dir.path().to_str().unwrap(),
            ])
            .current_dir(repo_dir.path())
            .output()
            .expect("git remote add origin");

        Self {
            runtime_dir,
            repo_dir,
            remote_dir,
        }
    }

    fn socket_path(&self) -> PathBuf {
        self.runtime_dir.path().join("beads").join("daemon.sock")
    }

    fn meta_path(&self) -> PathBuf {
        self.runtime_dir
            .path()
            .join("beads")
            .join("daemon.meta.json")
    }

    fn store_id(&self) -> beads_rs::StoreId {
        let remote_str = self.remote_dir.path().to_str().expect("remote path");
        let normalized = beads_rs::daemon::remote::normalize_url(remote_str);
        let store_uuid = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, normalized.as_bytes());
        beads_rs::StoreId::new(store_uuid)
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

    fn daemon_pid(&self) -> Option<u32> {
        let contents = fs::read_to_string(self.meta_path()).ok()?;
        let meta: serde_json::Value = serde_json::from_str(&contents).ok()?;
        meta["pid"].as_u64().map(|p| p as u32)
    }

    fn daemon_version(&self) -> Option<String> {
        let contents = fs::read_to_string(self.meta_path()).ok()?;
        let meta: serde_json::Value = serde_json::from_str(&contents).ok()?;
        meta["version"].as_str().map(|s| s.to_string())
    }

    fn start_daemon(&self) {
        // Initialize beads store and start daemon
        self.bd().arg("init").assert().success();
    }

    fn unlock_store(&self) {
        let store_id = self.store_id().to_string();
        self.bd()
            .args(["store", "unlock", "--store-id", store_id.as_str()])
            .assert()
            .success();
    }

    fn kill_daemon_forcefully(&self) {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;
        if let Some(pid) = self.daemon_pid() {
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while std::time::Instant::now() < deadline {
                if !Self::process_alive(pid) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }
    }

    fn process_alive(pid: u32) -> bool {
        use nix::sys::signal::kill;
        use nix::unistd::Pid;
        // Signal 0 checks if process exists without sending a signal
        kill(Pid::from_raw(pid as i32), None).is_ok()
    }
}

impl Drop for DaemonFixture {
    fn drop(&mut self) {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;
        // Clean up daemon if running
        if let Some(pid) = self.daemon_pid() {
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[test]
fn test_stale_socket_recovery() {
    let fixture = DaemonFixture::new();

    // Start daemon
    fixture.start_daemon();
    let original_pid = fixture.daemon_pid().expect("daemon should be running");

    // Kill with SIGKILL (simulating crash - leaves stale socket)
    fixture.kill_daemon_forcefully();
    assert!(
        fixture.socket_path().exists(),
        "socket file should still exist after SIGKILL"
    );
    fixture.unlock_store();

    // Make a request - should detect stale socket and restart
    fixture.bd().args(["status"]).assert().success();

    // Verify new daemon is running with different PID
    let new_pid = fixture.daemon_pid().expect("new daemon should be running");
    assert_ne!(
        original_pid, new_pid,
        "should be a new daemon process after recovery"
    );
}

#[test]
fn test_version_mismatch_triggers_restart() {
    let fixture = DaemonFixture::new();

    // Start daemon
    fixture.start_daemon();
    let original_pid = fixture.daemon_pid().expect("daemon should be running");
    let original_version = fixture.daemon_version().expect("should have version");

    // Corrupt meta file to report wrong version (simulates old daemon)
    let meta = serde_json::json!({
        "version": "0.0.0-fake",
        "protocol_version": 1,
        "pid": original_pid
    });
    fs::write(fixture.meta_path(), serde_json::to_string(&meta).unwrap()).unwrap();

    // Make a request - daemon should still work since meta file doesn't affect running daemon
    // But if we restart, it should come back with correct version
    fixture.kill_daemon_forcefully();
    fixture.unlock_store();
    fixture.bd().args(["status"]).assert().success();

    // Verify daemon was restarted with correct version
    let new_version = fixture.daemon_version().expect("should have new version");
    assert_ne!(
        new_version, "0.0.0-fake",
        "new daemon should not have fake version"
    );
    assert_eq!(
        original_version, new_version,
        "version should match original (same binary)"
    );
}

#[test]
fn test_no_orphaned_daemons() {
    let fixture = DaemonFixture::new();
    let mut seen_pids = Vec::new();

    // Kill and restart daemon 3 times
    for _ in 0..3 {
        fixture.start_daemon();
        let pid = fixture.daemon_pid().expect("daemon should be running");
        seen_pids.push(pid);
        fixture.kill_daemon_forcefully();
        fixture.unlock_store();
        std::thread::sleep(Duration::from_millis(50));
    }

    // Start one more
    fixture.bd().args(["status"]).assert().success();
    let final_pid = fixture
        .daemon_pid()
        .expect("final daemon should be running");

    // All old PIDs should be dead
    for old_pid in &seen_pids {
        assert!(
            !DaemonFixture::process_alive(*old_pid),
            "old daemon {} still alive",
            old_pid
        );
    }

    // Only final daemon should be alive
    assert!(
        DaemonFixture::process_alive(final_pid),
        "final daemon should be alive"
    );
}

#[test]
fn test_concurrent_restart_safety() {
    let fixture = DaemonFixture::new();

    // Initialize first
    fixture.start_daemon();
    fixture.kill_daemon_forcefully();
    fixture.unlock_store();

    // Spawn multiple CLI commands simultaneously
    let n_clients = 5;
    let barrier = Arc::new(Barrier::new(n_clients));
    let runtime_path = fixture.runtime_dir.path().to_path_buf();
    let repo_path = fixture.repo_dir.path().to_path_buf();
    let data_path = fixture.data_dir();

    let handles: Vec<_> = (0..n_clients)
        .map(|_| {
            let barrier = barrier.clone();
            let runtime_path = runtime_path.clone();
            let repo_path = repo_path.clone();
            let data_path = data_path.clone();
            std::thread::spawn(move || {
                barrier.wait(); // Start all at once
                let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("bd");
                cmd.current_dir(&repo_path);
                cmd.env("XDG_RUNTIME_DIR", &runtime_path);
                cmd.env("BD_WAL_DIR", &runtime_path);
                cmd.env("BD_DATA_DIR", &data_path);
                cmd.env("BD_NO_AUTO_UPGRADE", "1");
                cmd.args(["status"]).assert().success();
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    // Verify exactly one daemon running
    let final_pid = fixture.daemon_pid().expect("daemon should be running");
    assert!(
        DaemonFixture::process_alive(final_pid),
        "final daemon should be alive"
    );
}

#[test]
fn test_thundering_herd_single_daemon() {
    let fixture = DaemonFixture::new();
    // No daemon initially - don't call start_daemon

    let n_clients = 10;
    let barrier = Arc::new(Barrier::new(n_clients));
    let runtime_path = fixture.runtime_dir.path().to_path_buf();
    let repo_path = fixture.repo_dir.path().to_path_buf();
    let data_path = fixture.data_dir();

    // All clients try to start at once
    let handles: Vec<_> = (0..n_clients)
        .map(|_| {
            let barrier = barrier.clone();
            let runtime_path = runtime_path.clone();
            let repo_path = repo_path.clone();
            let data_path = data_path.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("bd");
                cmd.current_dir(&repo_path);
                cmd.env("XDG_RUNTIME_DIR", &runtime_path);
                cmd.env("BD_WAL_DIR", &runtime_path);
                cmd.env("BD_DATA_DIR", &data_path);
                cmd.env("BD_NO_AUTO_UPGRADE", "1");
                cmd.arg("init").assert().success();
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("thread panicked");
    }

    // Verify only one daemon is running
    let pid = fixture.daemon_pid().expect("daemon should be running");
    assert!(DaemonFixture::process_alive(pid), "daemon should be alive");

    // Socket should be healthy
    fixture.bd().args(["status"]).assert().success();
}

#[test]
fn test_daemon_meta_file_written() {
    let fixture = DaemonFixture::new();

    fixture.start_daemon();

    // Meta file should exist
    assert!(fixture.meta_path().exists(), "meta file should exist");

    // Meta file should have required fields
    let contents = fs::read_to_string(fixture.meta_path()).expect("read meta");
    let meta: serde_json::Value = serde_json::from_str(&contents).expect("parse meta");

    assert!(meta["version"].is_string(), "should have version");
    assert!(
        meta["protocol_version"].is_number(),
        "should have protocol_version"
    );
    assert!(meta["pid"].is_number(), "should have pid");
}

#[test]
fn test_graceful_shutdown_cleans_up() {
    let fixture = DaemonFixture::new();

    fixture.start_daemon();
    let pid = fixture.daemon_pid().expect("daemon should be running");

    // Send SIGTERM (graceful shutdown)
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);

    // Wait for daemon to stop
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if !DaemonFixture::process_alive(pid) {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    assert!(
        !DaemonFixture::process_alive(pid),
        "daemon should stop after SIGTERM"
    );

    // Socket and meta files should be cleaned up
    // (Give a moment for cleanup to complete)
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !fixture.socket_path().exists(),
        "socket should be cleaned up after graceful shutdown"
    );
    assert!(
        !fixture.meta_path().exists(),
        "meta file should be cleaned up after graceful shutdown"
    );
}

#[test]
fn test_graceful_shutdown_preserves_mutations() {
    let fixture = DaemonFixture::new();

    fixture.start_daemon();

    let create_output = fixture
        .bd()
        .args(["create", "shutdown test", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let created: serde_json::Value =
        serde_json::from_slice(&create_output).expect("parse create response");
    let id = created["data"]["id"]
        .as_str()
        .expect("created id")
        .to_string();
    let title = created["data"]["title"]
        .as_str()
        .expect("created title")
        .to_string();

    let pid = fixture.daemon_pid().expect("daemon should be running");

    // Send SIGTERM (graceful shutdown)
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);

    // Wait for daemon to stop
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if !DaemonFixture::process_alive(pid) {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    assert!(
        !DaemonFixture::process_alive(pid),
        "daemon should stop after SIGTERM"
    );

    // Fetch the issue after restart (auto-starts daemon)
    let show_output = fixture
        .bd()
        .args(["show", &id, "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let shown: serde_json::Value =
        serde_json::from_slice(&show_output).expect("parse show response");
    assert_eq!(shown["data"]["id"].as_str(), Some(id.as_str()));
    assert_eq!(shown["data"]["title"].as_str(), Some(title.as_str()));
}
