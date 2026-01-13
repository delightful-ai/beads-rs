//! HLC (Hybrid Logical Clock) for causal ordering.
//!
//! The clock generates monotonically increasing WriteStamps that form
//! a total order across all actors.

use std::time::{SystemTime, UNIX_EPOCH};

pub trait TimeSource: Send + Sync {
    fn now_ms(&self) -> u64;
}

pub struct SystemTimeSource;

impl TimeSource for SystemTimeSource {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

use crate::core::{Limits, WriteStamp};

/// Hybrid Logical Clock.
///
/// Combines wall clock time with a logical counter to ensure monotonicity
/// even when wall clock jumps backward or multiple events happen in the
/// same millisecond.
pub struct Clock {
    /// Last known wall time in milliseconds.
    wall_ms: u64,
    /// Logical counter for tie-breaking within same wall time.
    counter: u32,
    /// Max forward drift allowed within a session.
    max_forward_drift_ms: u64,
    time_source: Box<dyn TimeSource>,
}

impl Clock {
    /// Create a new clock initialized to current wall time.
    pub fn new() -> Self {
        Self::new_with_max_forward_drift(Limits::default().hlc_max_forward_drift_ms)
    }

    pub fn new_with_max_forward_drift(max_forward_drift_ms: u64) -> Self {
        Self::with_time_source_and_max_forward_drift(
            Box::new(SystemTimeSource),
            max_forward_drift_ms,
        )
    }

    pub fn with_time_source(time_source: Box<dyn TimeSource>) -> Self {
        Self::with_time_source_and_max_forward_drift(
            time_source,
            Limits::default().hlc_max_forward_drift_ms,
        )
    }

    pub fn with_time_source_and_max_forward_drift(
        time_source: Box<dyn TimeSource>,
        max_forward_drift_ms: u64,
    ) -> Self {
        let now = time_source.now_ms();
        Self {
            wall_ms: now,
            counter: 0,
            max_forward_drift_ms,
            time_source,
        }
    }

    pub fn state(&self) -> WriteStamp {
        WriteStamp::new(self.wall_ms, self.counter)
    }

    /// Generate a new WriteStamp, advancing the clock.
    ///
    /// Guarantees:
    /// - Returned stamp is strictly greater than any previous stamp from this clock
    /// - Monotonic even if wall clock goes backward
    pub fn tick(&mut self) -> WriteStamp {
        let now = self.time_source.now_ms();

        if now > self.wall_ms {
            let prev_wall_ms = self.wall_ms;
            let max_forward = prev_wall_ms.saturating_add(self.max_forward_drift_ms);
            if now > max_forward {
                // Clamp large forward jumps within a session.
                self.wall_ms = max_forward;
                self.counter += 1;
                tracing::warn!(
                    now_ms = now,
                    last_physical_ms = prev_wall_ms,
                    clamped_physical_ms = self.wall_ms,
                    max_forward_drift_ms = self.max_forward_drift_ms,
                    "clock forward jump clamped"
                );
            } else {
                // Wall clock advanced - use new time, reset counter.
                self.wall_ms = now;
                self.counter = 0;
            }
        } else {
            // Same millisecond or clock went backward - increment counter
            self.counter += 1;
        }

        WriteStamp::new(self.wall_ms, self.counter)
    }

    /// Update clock based on a remote stamp.
    ///
    /// Ensures next tick() will produce a stamp > remote.
    /// Call this when receiving state from sync.
    pub fn receive(&mut self, remote: &WriteStamp) {
        let now = self.time_source.now_ms();

        if remote.wall_ms > self.wall_ms {
            // Remote is ahead - adopt its time
            self.wall_ms = remote.wall_ms;
            self.counter = remote.counter;
        } else if remote.wall_ms == self.wall_ms && remote.counter > self.counter {
            // Same time but remote has higher counter
            self.counter = remote.counter;
        }
        // else: our clock is already ahead, nothing to do

        // Also advance to current wall time if it's ahead
        if now > self.wall_ms {
            self.wall_ms = now;
            self.counter = 0;
        }
    }

    /// Get the current wall time tracked by this clock.
    pub fn wall_ms(&self) -> u64 {
        self.wall_ms
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn tick_is_monotonic() {
        let mut clock = Clock::new();
        let s1 = clock.tick();
        let s2 = clock.tick();
        let s3 = clock.tick();

        assert!(s2 > s1);
        assert!(s3 > s2);
    }

    #[test]
    fn receive_advances_clock() {
        let mut clock = Clock::new();
        let local = clock.tick();

        // Simulate remote with future timestamp
        let remote = WriteStamp::new(local.wall_ms + 10000, 5);
        clock.receive(&remote);

        let after = clock.tick();
        assert!(after > remote);
    }

    #[test]
    fn receive_with_older_stamp_is_noop() {
        let mut clock = Clock::new();
        let s1 = clock.tick();
        let s2 = clock.tick();

        // Remote is older than our current state
        let old_remote = WriteStamp::new(s1.wall_ms, s1.counter);
        clock.receive(&old_remote);

        let s3 = clock.tick();
        assert!(s3 > s2);
    }

    struct TestTimeSource {
        now: Arc<AtomicU64>,
    }

    impl TimeSource for TestTimeSource {
        fn now_ms(&self) -> u64 {
            self.now.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn backward_jump_keeps_monotonicity() {
        let now = Arc::new(AtomicU64::new(1_000));
        let source = Box::new(TestTimeSource { now: now.clone() });
        let mut clock = Clock::with_time_source(source);

        let s1 = clock.tick();
        now.store(900, Ordering::SeqCst);
        let s2 = clock.tick();

        assert!(s2 > s1);
        assert_eq!(s2.wall_ms, s1.wall_ms);
    }

    #[test]
    fn forward_jump_advances_wall_time() {
        let now = Arc::new(AtomicU64::new(1_000));
        let source = Box::new(TestTimeSource { now: now.clone() });
        let mut clock = Clock::with_time_source(source);

        let s1 = clock.tick();
        now.store(2_000, Ordering::SeqCst);
        let s2 = clock.tick();

        assert!(s2 > s1);
        assert_eq!(s2.wall_ms, 2_000);
    }

    #[test]
    fn forward_jump_clamps_within_session() {
        let now = Arc::new(AtomicU64::new(1_000));
        let source = Box::new(TestTimeSource { now: now.clone() });
        let mut clock = Clock::with_time_source_and_max_forward_drift(source, 100);

        let s1 = clock.tick();
        now.store(1_500, Ordering::SeqCst);
        let s2 = clock.tick();

        assert!(s2 > s1);
        assert_eq!(s2.wall_ms, 1_100);
        assert_eq!(s2.counter, s1.counter + 1);
    }

    #[test]
    fn restart_recovers_without_regression() {
        let now = Arc::new(AtomicU64::new(1_000));
        let source = Box::new(TestTimeSource { now: now.clone() });
        let mut clock = Clock::with_time_source(source);
        let persisted = clock.tick();

        let now = Arc::new(AtomicU64::new(900));
        let source = Box::new(TestTimeSource { now: now.clone() });
        let mut restarted = Clock::with_time_source(source);
        restarted.receive(&persisted);
        let after = restarted.tick();

        assert!(after > persisted);
    }
}
