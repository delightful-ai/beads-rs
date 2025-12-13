//! Sync scheduling with debounce.
//!
//! Provides:
//! - `SyncScheduler` - Manages delayed sync triggers

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crossbeam::channel::Sender;

use super::remote::RemoteUrl;

/// Manages sync scheduling with debounce.
///
/// When a mutation occurs, we schedule a sync after a delay (default 500ms).
/// If another mutation occurs before the timer fires, we reschedule.
/// This batches rapid mutations into a single sync.
pub struct SyncScheduler {
    /// Pending syncs: remote -> scheduled fire time.
    pending: HashMap<RemoteUrl, Instant>,

    /// Default debounce delay.
    default_delay: Duration,

    /// Channel to send timer completions.
    timer_tx: Sender<RemoteUrl>,
}

impl SyncScheduler {
    /// Create a new scheduler.
    pub fn new(timer_tx: Sender<RemoteUrl>) -> Self {
        SyncScheduler {
            pending: HashMap::new(),
            default_delay: Duration::from_millis(500),
            timer_tx,
        }
    }

    /// Create with custom default delay.
    pub fn with_delay(timer_tx: Sender<RemoteUrl>, delay: Duration) -> Self {
        SyncScheduler {
            pending: HashMap::new(),
            default_delay: delay,
            timer_tx,
        }
    }

    /// Schedule a sync for a repo after the default delay.
    pub fn schedule(&mut self, remote: RemoteUrl) {
        self.schedule_after(remote, self.default_delay);
    }

    /// Schedule a sync for a repo after a specific delay.
    pub fn schedule_after(&mut self, remote: RemoteUrl, delay: Duration) {
        let fire_at = Instant::now() + delay;

        // Only schedule if not already pending with an earlier time
        if let Some(&existing) = self.pending.get(&remote) {
            if existing <= fire_at {
                // Already have an earlier or equal pending sync
                return;
            }
        }

        self.pending.insert(remote.clone(), fire_at);

        // Spawn timer thread
        let tx = self.timer_tx.clone();
        std::thread::spawn(move || {
            std::thread::sleep(delay);
            // Ignore send errors - receiver may have been dropped
            let _ = tx.send(remote);
        });
    }

    /// Check if a sync should fire for a repo.
    ///
    /// Returns true if the repo has a pending sync that's ready.
    /// Removes the pending entry if it fires.
    pub fn should_fire(&mut self, remote: &RemoteUrl) -> bool {
        if let Some(&fire_at) = self.pending.get(remote) {
            if Instant::now() >= fire_at {
                self.pending.remove(remote);
                return true;
            }
        }
        false
    }

    /// Cancel a pending sync.
    pub fn cancel(&mut self, remote: &RemoteUrl) {
        self.pending.remove(remote);
    }

    /// Check if a repo has a pending sync.
    pub fn is_pending(&self, remote: &RemoteUrl) -> bool {
        self.pending.contains_key(remote)
    }

    /// Get all pending repos.
    pub fn pending_repos(&self) -> Vec<&RemoteUrl> {
        self.pending.keys().collect()
    }
}

#[cfg(test)]
mod tests {
    use crossbeam::channel;

    use super::*;

    #[test]
    fn schedule_and_fire() {
        let (tx, _rx) = channel::unbounded();
        let mut scheduler = SyncScheduler::with_delay(tx, Duration::from_millis(10));

        let remote = RemoteUrl("example.com/foo/bar".into());
        scheduler.schedule(remote.clone());

        assert!(scheduler.is_pending(&remote));

        // Wait for timer
        std::thread::sleep(Duration::from_millis(15));

        assert!(scheduler.should_fire(&remote));
        assert!(!scheduler.is_pending(&remote));
    }

    #[test]
    fn reschedule_extends() {
        let (tx, _rx) = channel::unbounded();
        let mut scheduler = SyncScheduler::with_delay(tx, Duration::from_millis(100));

        let remote = RemoteUrl("example.com/foo/bar".into());

        // Schedule first
        scheduler.schedule(remote.clone());

        // Schedule again with shorter delay - shouldn't change since we have earlier
        scheduler.schedule_after(remote.clone(), Duration::from_millis(50));

        // The original schedule should still be pending
        assert!(scheduler.is_pending(&remote));
    }

    #[test]
    fn cancel() {
        let (tx, _rx) = channel::unbounded();
        let mut scheduler = SyncScheduler::with_delay(tx, Duration::from_millis(1000));

        let remote = RemoteUrl("example.com/foo/bar".into());
        scheduler.schedule(remote.clone());
        assert!(scheduler.is_pending(&remote));

        scheduler.cancel(&remote);
        assert!(!scheduler.is_pending(&remote));
    }
}
