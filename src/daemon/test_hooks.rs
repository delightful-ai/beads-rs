//! Test-only hooks for integration crash tests.

#[cfg(feature = "slow-tests")]
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

#[cfg(not(feature = "slow-tests"))]
pub(crate) fn maybe_pause(_stage: &str) {}
