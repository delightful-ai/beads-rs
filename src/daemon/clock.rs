//! HLC (Hybrid Logical Clock) for causal ordering.
//!
//! The clock generates monotonically increasing WriteStamps that form
//! a total order across all actors.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::core::WriteStamp;

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
}

impl Clock {
    /// Create a new clock initialized to current wall time.
    pub fn new() -> Self {
        Self {
            wall_ms: Self::now_ms(),
            counter: 0,
        }
    }

    /// Generate a new WriteStamp, advancing the clock.
    ///
    /// Guarantees:
    /// - Returned stamp is strictly greater than any previous stamp from this clock
    /// - Monotonic even if wall clock goes backward
    pub fn tick(&mut self) -> WriteStamp {
        let now = Self::now_ms();

        if now > self.wall_ms {
            // Wall clock advanced - use new time, reset counter
            self.wall_ms = now;
            self.counter = 0;
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
        let now = Self::now_ms();

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

    /// Current wall time in milliseconds since Unix epoch.
    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
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
}
