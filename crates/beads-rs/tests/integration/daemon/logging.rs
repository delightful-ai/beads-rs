#![cfg(feature = "slow-tests")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as StdCommand};
use std::time::{Duration, Instant};

use tempfile::TempDir;

use crate::fixtures::daemon_runtime::shutdown_daemon;

fn tmp_root() -> PathBuf {
    let root = std::env::current_dir().expect("cwd").join("tmp");
    fs::create_dir_all(&root).expect("create tmp root");
    root
}

fn wait_for<F>(timeout: Duration, mut condition: F) -> bool
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

fn log_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with("beads.log"))
                .unwrap_or(false)
            {
                files.push(path);
            }
        }
    }
    files
}

struct LogDaemon {
    runtime_dir: TempDir,
    data_dir: PathBuf,
    log_dir: PathBuf,
}

impl LogDaemon {
    fn new() -> Self {
        let root = tmp_root();
        let runtime_dir = TempDir::new_in(&root).expect("runtime dir");
        let data_dir = runtime_dir.path().join("data");
        fs::create_dir_all(&data_dir).expect("data dir");
        let log_dir = data_dir.join("logs");
        fs::create_dir_all(&log_dir).expect("log dir");
        Self {
            runtime_dir,
            data_dir,
            log_dir,
        }
    }

    fn spawn(&self, testing: bool) -> Child {
        let mut cmd = StdCommand::new(assert_cmd::cargo::cargo_bin!("bd"));
        cmd.args(["daemon", "run"]);
        cmd.current_dir(std::env::current_dir().expect("cwd"));
        cmd.env("XDG_RUNTIME_DIR", self.runtime_dir.path());
        cmd.env("BD_DATA_DIR", &self.data_dir);
        cmd.env("BD_LOG_DIR", &self.log_dir);
        cmd.env("BD_NO_AUTO_UPGRADE", "1");
        if testing {
            cmd.env("BD_TESTING", "1");
            cmd.env("BD_TEST_FAST", "1");
            cmd.env("BD_TEST_DISABLE_GIT_SYNC", "1");
            cmd.env("BD_TEST_DISABLE_CHECKPOINTS", "1");
            cmd.env("BD_WAL_SYNC_MODE", "none");
        } else {
            cmd.env_remove("BD_TESTING");
            cmd.env_remove("RUST_TEST_THREADS");
            cmd.env_remove("BD_TEST_FAST");
            cmd.env_remove("BD_TEST_DISABLE_GIT_SYNC");
            cmd.env_remove("BD_TEST_DISABLE_CHECKPOINTS");
            cmd.env_remove("BD_WAL_SYNC_MODE");
        }
        cmd.spawn().expect("spawn daemon")
    }

    fn shutdown(&self) {
        shutdown_daemon(self.runtime_dir.path());
    }
}

#[test]
fn daemon_run_enables_file_logging_by_default() {
    let daemon = LogDaemon::new();
    let mut child = daemon.spawn(false);

    let ok = wait_for(Duration::from_secs(3), || {
        !log_files(&daemon.log_dir).is_empty()
    });
    daemon.shutdown();
    let _ = child.wait();
    assert!(ok, "expected log file under {}", daemon.log_dir.display());
}

#[test]
fn daemon_run_skips_file_logging_in_tests() {
    let daemon = LogDaemon::new();
    let mut child = daemon.spawn(true);

    let ok = wait_for(Duration::from_secs(2), || {
        log_files(&daemon.log_dir).is_empty()
    });
    daemon.shutdown();
    let _ = child.wait();
    assert!(ok, "unexpected log file under {}", daemon.log_dir.display());
}
