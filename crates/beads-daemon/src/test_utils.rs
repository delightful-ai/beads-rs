//! Shared deterministic test helpers owned by the daemon boundary.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Runtime socket path used by daemon IPC tests.
#[must_use]
pub fn daemon_socket_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("beads").join("daemon.sock")
}

/// Runtime metadata path used by daemon lifecycle tests.
#[must_use]
pub fn daemon_meta_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("beads").join("daemon.meta.json")
}

/// Creates and returns `./tmp`, matching legacy test-harness behavior.
#[must_use]
pub fn ensure_relative_tmp_root() -> PathBuf {
    let root = PathBuf::from("./tmp");
    fs::create_dir_all(&root).expect("create ./tmp");
    root
}

/// Polls until `condition` becomes true or timeout elapses.
///
/// Backoff grows exponentially and is capped at `max_backoff`.
pub fn poll_until_with_backoff<F>(
    timeout: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
    mut condition: F,
) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    let mut backoff = if initial_backoff.is_zero() {
        Duration::from_millis(1)
    } else {
        initial_backoff
    };
    let max_backoff = std::cmp::max(max_backoff, backoff);
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        std::thread::sleep(backoff);
        backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
    }
    condition()
}

/// Polls with default backoff used by most daemon integration fixtures.
pub fn poll_until<F>(timeout: Duration, condition: F) -> bool
where
    F: FnMut() -> bool,
{
    poll_until_with_backoff(
        timeout,
        Duration::from_millis(10),
        Duration::from_millis(100),
        condition,
    )
}
