#![cfg(feature = "slow-tests")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as StdCommand};
use std::time::Duration;

use tempfile::TempDir;

use crate::fixtures::bd_runtime::{
    BdCommandProfile, config_dir_for_runtime, configure_std_bd_command, data_dir_for_runtime,
};
use crate::fixtures::daemon_runtime::shutdown_daemon;
use crate::fixtures::temp;
use crate::fixtures::wait;

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
        let runtime_dir = temp::fixture_tempdir("daemon-logging");
        let data_dir = data_dir_for_runtime(runtime_dir.path());
        let config_dir = config_dir_for_runtime(runtime_dir.path());
        let log_dir = data_dir.join("logs");
        fs::create_dir_all(&log_dir).expect("log dir");
        assert!(config_dir.exists(), "config dir should exist");
        Self {
            runtime_dir,
            data_dir,
            log_dir,
        }
    }

    fn spawn(&self, testing: bool) -> Child {
        let mut cmd = StdCommand::new(assert_cmd::cargo::cargo_bin!("bd"));
        let cwd = std::env::current_dir().expect("cwd");
        let profile = if testing {
            BdCommandProfile::fast_daemon()
        } else {
            BdCommandProfile::daemon()
        };
        configure_std_bd_command(
            &mut cmd,
            &cwd,
            self.runtime_dir.path(),
            &self.data_dir,
            profile,
        );
        cmd.args(["daemon", "run"]);
        cmd.env(
            "BD_CONFIG_DIR",
            config_dir_for_runtime(self.runtime_dir.path()),
        );
        cmd.env("BD_LOG_DIR", &self.log_dir);
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

    let ok = wait::poll_until_with_phase(
        "fixture.logging.wait_for_log_file",
        "default",
        Duration::from_secs(3),
        || !log_files(&daemon.log_dir).is_empty(),
    );
    daemon.shutdown();
    let _ = child.wait();
    assert!(ok, "expected log file under {}", daemon.log_dir.display());
}

#[test]
fn daemon_run_skips_file_logging_in_tests() {
    let daemon = LogDaemon::new();
    let mut child = daemon.spawn(true);

    let ok = wait::poll_until_with_phase(
        "fixture.logging.wait_for_log_suppression",
        "testing",
        Duration::from_secs(2),
        || log_files(&daemon.log_dir).is_empty(),
    );
    daemon.shutdown();
    let _ = child.wait();
    assert!(ok, "unexpected log file under {}", daemon.log_dir.display());
}
