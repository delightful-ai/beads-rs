//! Sync scheduling with debounce.
//!
//! Provides:
//! - `SyncScheduler` - Manages delayed sync triggers

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::remote::RemoteUrl;

const DEFAULT_DEBOUNCE_MS: u64 = 500;
const TEST_FAST_DEBOUNCE_MS: u64 = 50;

fn default_delay() -> Duration {
    if env_flag_truthy("BD_TEST_FAST") {
        Duration::from_millis(TEST_FAST_DEBOUNCE_MS)
    } else {
        Duration::from_millis(DEFAULT_DEBOUNCE_MS)
    }
}

fn env_flag_truthy(name: &str) -> bool {
    let Ok(raw) = std::env::var(name) else {
        return false;
    };
    !matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "no" | "n" | "off"
    )
}

/// Manages sync scheduling with debounce.
///
/// When a mutation occurs, we schedule a sync after a delay (default 500ms).
/// If another mutation occurs before the timer fires, we reschedule.
/// This batches rapid mutations into a single sync.
pub struct SyncScheduler {
    /// Pending syncs: remote -> scheduled fire time.
    pending: HashMap<RemoteUrl, Instant>,

    /// Heap of (fire_at, remote) for fast "next deadline" lookup.
    ///
    /// We may push multiple entries for the same remote; stale entries are
    /// discarded by checking against `pending`.
    heap: BinaryHeap<Reverse<(Instant, RemoteUrl)>>,

    /// Default debounce delay.
    default_delay: Duration,
}

impl Default for SyncScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncScheduler {
    /// Create a new scheduler.
    pub fn new() -> Self {
        SyncScheduler {
            pending: HashMap::new(),
            heap: BinaryHeap::new(),
            default_delay: default_delay(),
        }
    }

    /// Create with custom default delay.
    pub fn with_delay(delay: Duration) -> Self {
        SyncScheduler {
            pending: HashMap::new(),
            heap: BinaryHeap::new(),
            default_delay: delay,
        }
    }

    /// Schedule a sync for a repo after the default delay.
    pub fn schedule(&mut self, remote: RemoteUrl) {
        self.schedule_after(remote, self.default_delay);
    }

    /// Schedule a sync for a repo after a specific delay.
    pub fn schedule_after(&mut self, remote: RemoteUrl, delay: Duration) {
        self.schedule_after_at(remote, delay, Instant::now());
    }

    fn schedule_after_at(&mut self, remote: RemoteUrl, delay: Duration, now: Instant) {
        let candidate = now + delay;
        let fire_at = self
            .pending
            .get(&remote)
            .copied()
            .map(|existing| existing.max(candidate))
            .unwrap_or(candidate);

        if self.pending.get(&remote).copied() == Some(fire_at) {
            return;
        }

        self.pending.insert(remote.clone(), fire_at);
        self.heap.push(Reverse((fire_at, remote)));
    }

    /// Get the next scheduled deadline across all repos, if any.
    pub fn next_deadline(&mut self) -> Option<Instant> {
        self.pop_stale();
        self.heap.peek().map(|Reverse((t, _))| *t)
    }

    /// Drain all repos whose deadline is due at `now`.
    pub fn drain_due(&mut self, now: Instant) -> Vec<RemoteUrl> {
        let mut due = Vec::new();
        loop {
            self.pop_stale();
            let Some(Reverse((fire_at, remote))) = self.heap.peek().cloned() else {
                break;
            };
            if fire_at > now {
                break;
            }
            let _ = self.heap.pop();
            if self.pending.get(&remote).copied() == Some(fire_at) {
                self.pending.remove(&remote);
                due.push(remote);
            }
        }
        due
    }

    /// Cancel a pending sync.
    pub fn cancel(&mut self, remote: &RemoteUrl) {
        self.pending.remove(remote);
    }

    /// Check if a repo has a pending sync.
    pub fn is_pending(&self, remote: &RemoteUrl) -> bool {
        self.pending.contains_key(remote)
    }

    /// Get the scheduled deadline for a repo, if pending.
    pub fn deadline_for(&self, remote: &RemoteUrl) -> Option<Instant> {
        self.pending.get(remote).copied()
    }

    /// Get all pending repos.
    pub fn pending_repos(&self) -> Vec<&RemoteUrl> {
        self.pending.keys().collect()
    }

    fn pop_stale(&mut self) {
        while let Some(Reverse((fire_at, remote))) = self.heap.peek() {
            match self.pending.get(remote).copied() {
                Some(current) if current == *fire_at => break,
                _ => {
                    let _ = self.heap.pop();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_and_drain_due() {
        let mut scheduler = SyncScheduler::with_delay(Duration::from_millis(10));

        let remote = RemoteUrl("example.com/foo/bar".into());
        let base = Instant::now();
        scheduler.schedule_after_at(remote.clone(), Duration::from_millis(10), base);

        assert!(scheduler.is_pending(&remote));
        assert_eq!(
            scheduler.next_deadline(),
            Some(base + Duration::from_millis(10))
        );

        let due = scheduler.drain_due(base + Duration::from_millis(9));
        assert!(due.is_empty());

        let due = scheduler.drain_due(base + Duration::from_millis(10));
        assert_eq!(due, vec![remote.clone()]);
        assert!(!scheduler.is_pending(&remote));
    }

    #[test]
    fn reschedule_debounces_later() {
        let mut scheduler = SyncScheduler::with_delay(Duration::from_millis(10));

        let remote = RemoteUrl("example.com/foo/bar".into());
        let base = Instant::now();

        scheduler.schedule_after_at(remote.clone(), Duration::from_millis(10), base);
        scheduler.schedule_after_at(
            remote.clone(),
            Duration::from_millis(10),
            base + Duration::from_millis(5),
        );

        // Debounce pushes the deadline out to (last_schedule + delay).
        assert_eq!(
            scheduler.next_deadline(),
            Some(base + Duration::from_millis(15))
        );

        assert!(scheduler.is_pending(&remote));
    }

    #[test]
    fn backoff_never_shortens_deadline() {
        let mut scheduler = SyncScheduler::with_delay(Duration::from_millis(10));

        let remote = RemoteUrl("example.com/foo/bar".into());
        let base = Instant::now();

        scheduler.schedule_after_at(remote.clone(), Duration::from_millis(10), base);
        // Longer backoff should extend the deadline, not be ignored.
        scheduler.schedule_after_at(
            remote.clone(),
            Duration::from_millis(50),
            base + Duration::from_millis(1),
        );

        assert_eq!(
            scheduler.next_deadline(),
            Some(base + Duration::from_millis(51))
        );
    }

    #[test]
    fn stress_reschedules_do_not_accumulate_due_fires() {
        let mut scheduler = SyncScheduler::with_delay(Duration::from_millis(10));

        let remote = RemoteUrl("example.com/foo/bar".into());
        let base = Instant::now();

        for i in 0..1000u64 {
            scheduler.schedule_after_at(
                remote.clone(),
                Duration::from_millis(10),
                base + Duration::from_millis(i),
            );
        }

        let due = scheduler.drain_due(base + Duration::from_millis(1010));
        assert_eq!(due, vec![remote.clone()]);
        assert!(scheduler.next_deadline().is_none());
        assert!(!scheduler.is_pending(&remote));
    }

    #[test]
    fn cancel() {
        let mut scheduler = SyncScheduler::with_delay(Duration::from_millis(1000));

        let remote = RemoteUrl("example.com/foo/bar".into());
        scheduler.schedule(remote.clone());
        assert!(scheduler.is_pending(&remote));

        scheduler.cancel(&remote);
        assert!(!scheduler.is_pending(&remote));
    }
}
