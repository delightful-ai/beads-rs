//! Test-only hooks for integration crash tests.

use crate::runtime::wal::WalIndexError;

#[cfg(any(feature = "slow-tests", feature = "test-harness"))]
pub(crate) fn maybe_pause(stage: &str) {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    let Ok(target) = std::env::var("BD_TEST_WAL_HANG_STAGE") else {
        return;
    };
    if target != stage {
        return;
    }

    let Ok(dir) = std::env::var("BD_TEST_WAL_HANG_DIR") else {
        return;
    };

    let marker = PathBuf::from(dir).join(format!("beads-wal-hang-{stage}"));
    let _ = fs::write(marker, b"");

    let timeout_ms = std::env::var("BD_TEST_WAL_HANG_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30_000);
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(not(any(feature = "slow-tests", feature = "test-harness")))]
pub(crate) fn maybe_pause(_stage: &str) {}

#[cfg(any(test, feature = "test-harness"))]
#[derive(Clone, Debug)]
struct AtomicCommitFailpoint {
    stage: String,
    owner: std::thread::ThreadId,
}

#[cfg(any(test, feature = "test-harness"))]
fn atomic_commit_failpoint_slot() -> &'static std::sync::Mutex<Option<AtomicCommitFailpoint>> {
    static SLOT: std::sync::OnceLock<std::sync::Mutex<Option<AtomicCommitFailpoint>>> =
        std::sync::OnceLock::new();
    SLOT.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(any(test, feature = "test-harness"))]
pub(crate) fn maybe_fail_atomic_commit(stage: &str) -> Result<(), WalIndexError> {
    let slot = atomic_commit_failpoint_slot();
    let active = slot.lock().expect("atomic commit failpoint lock");
    if let Some(failpoint) = active.as_ref()
        && failpoint.stage == stage
        && failpoint.owner == std::thread::current().id()
    {
        return Err(WalIndexError::Sql {
            message: format!("injected atomic commit failure at {stage}"),
        });
    }
    Ok(())
}

#[cfg(not(any(test, feature = "test-harness")))]
pub(crate) fn maybe_fail_atomic_commit(_stage: &str) -> Result<(), WalIndexError> {
    Ok(())
}

#[cfg(test)]
pub(crate) struct AtomicCommitFailpointGuard {
    previous: Option<AtomicCommitFailpoint>,
}

#[cfg(test)]
impl Drop for AtomicCommitFailpointGuard {
    fn drop(&mut self) {
        let slot = atomic_commit_failpoint_slot();
        *slot.lock().expect("atomic commit failpoint lock") = self.previous.take();
    }
}

#[cfg(test)]
pub(crate) fn set_atomic_commit_fail_stage_for_tests(stage: &str) -> AtomicCommitFailpointGuard {
    let slot = atomic_commit_failpoint_slot();
    let mut active = slot.lock().expect("atomic commit failpoint lock");
    let previous = active.replace(AtomicCommitFailpoint {
        stage: stage.to_string(),
        owner: std::thread::current().id(),
    });
    AtomicCommitFailpointGuard { previous }
}
