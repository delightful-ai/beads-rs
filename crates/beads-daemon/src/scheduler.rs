//! Sync scheduling with debounce.
//!
//! Provides:
//! - `SyncScheduler` - Manages delayed sync triggers

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::env_flags::env_flag_truthy;

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

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SyncGateToken {
    remote: RemoteUrl,
    epoch: u64,
}

impl SyncGateToken {
    fn new(remote: RemoteUrl, epoch: u64) -> Self {
        Self { remote, epoch }
    }

    pub(crate) fn remote(&self) -> &RemoteUrl {
        &self.remote
    }

    fn epoch(&self) -> u64 {
        self.epoch
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScheduledSyncKind {
    Debounce,
    Backoff,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ScheduledSync {
    fire_at: Instant,
    kind: ScheduledSyncKind,
}

/// Manages sync scheduling with debounce.
///
/// When a mutation occurs, we schedule a sync after a delay (default 500ms).
/// If another mutation occurs before the timer fires, we reschedule.
/// This batches rapid mutations into a single sync.
pub struct SyncScheduler {
    /// Pending syncs: remote -> scheduled fire time.
    pending: HashMap<RemoteUrl, ScheduledSync>,
    /// Per-remote epoch used to invalidate stale issued gates.
    epochs: HashMap<RemoteUrl, u64>,

    /// Heap of (fire_at, remote) for fast "next deadline" lookup.
    ///
    /// We may push multiple entries for the same remote; stale entries are
    /// discarded by checking against `pending`.
    heap: BinaryHeap<Reverse<(Instant, RemoteUrl)>>,

    /// Default debounce delay.
    default_delay: Duration,
    /// Monotonic gate epoch allocator.
    next_epoch: u64,
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
            epochs: HashMap::new(),
            heap: BinaryHeap::new(),
            default_delay: default_delay(),
            next_epoch: 1,
        }
    }

    /// Create with custom default delay.
    pub fn with_delay(delay: Duration) -> Self {
        SyncScheduler {
            pending: HashMap::new(),
            epochs: HashMap::new(),
            heap: BinaryHeap::new(),
            default_delay: delay,
            next_epoch: 1,
        }
    }

    /// Schedule a sync for a repo after the default delay.
    pub fn schedule(&mut self, remote: RemoteUrl) {
        self.schedule_after_kind(remote, self.default_delay, ScheduledSyncKind::Debounce);
    }

    /// Schedule a sync for a repo after a specific delay.
    pub fn schedule_after(&mut self, remote: RemoteUrl, delay: Duration) {
        self.schedule_after_kind(remote, delay, ScheduledSyncKind::Backoff);
    }

    fn schedule_after_kind(&mut self, remote: RemoteUrl, delay: Duration, kind: ScheduledSyncKind) {
        self.schedule_after_at(remote, delay, Instant::now(), kind);
    }

    fn schedule_after_at(
        &mut self,
        remote: RemoteUrl,
        delay: Duration,
        now: Instant,
        kind: ScheduledSyncKind,
    ) {
        let candidate = ScheduledSync {
            fire_at: now + delay,
            kind,
        };
        let scheduled = match self.pending.get(&remote).copied() {
            Some(existing) => ScheduledSync {
                fire_at: existing.fire_at.max(candidate.fire_at),
                kind: if matches!(existing.kind, ScheduledSyncKind::Backoff)
                    || matches!(candidate.kind, ScheduledSyncKind::Backoff)
                {
                    ScheduledSyncKind::Backoff
                } else {
                    ScheduledSyncKind::Debounce
                },
            },
            None => candidate,
        };

        if self.pending.get(&remote).copied() == Some(scheduled) {
            return;
        }

        self.pending.insert(remote.clone(), scheduled);
        self.bump_epoch(&remote);
        self.heap.push(Reverse((scheduled.fire_at, remote)));
    }

    /// Get the next scheduled deadline across all repos, if any.
    pub fn next_deadline(&mut self) -> Option<Instant> {
        self.pop_stale();
        self.heap.peek().map(|Reverse((t, _))| *t)
    }

    /// Drain all repos whose deadline is due at `now`.
    pub(crate) fn drain_due(&mut self, now: Instant) -> Vec<SyncGateToken> {
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
            if self.pending.get(&remote).map(|scheduled| scheduled.fire_at) == Some(fire_at) {
                self.pending.remove(&remote);
                due.push(self.issue_gate(remote));
            }
        }
        due
    }

    pub(crate) fn issue_immediate_gate(
        &mut self,
        remote: &RemoteUrl,
        now: Instant,
    ) -> Option<SyncGateToken> {
        match self.pending.get(remote).copied() {
            Some(scheduled)
                if scheduled.fire_at <= now
                    || matches!(scheduled.kind, ScheduledSyncKind::Debounce) =>
            {
                self.pending.remove(remote);
                Some(self.issue_gate(remote.clone()))
            }
            Some(_) => None,
            None => Some(self.issue_gate(remote.clone())),
        }
    }

    pub(crate) fn gate_is_current(&self, gate: &SyncGateToken) -> bool {
        self.current_epoch(gate.remote()) == gate.epoch()
    }

    /// Cancel a pending sync.
    pub fn cancel(&mut self, remote: &RemoteUrl) {
        if self.pending.remove(remote).is_some() {
            self.bump_epoch(remote);
        }
    }

    /// Check if a repo has a pending sync.
    pub fn is_pending(&self, remote: &RemoteUrl) -> bool {
        self.pending.contains_key(remote)
    }

    /// Get the scheduled deadline for a repo, if pending.
    pub fn deadline_for(&self, remote: &RemoteUrl) -> Option<Instant> {
        self.pending.get(remote).map(|scheduled| scheduled.fire_at)
    }

    /// Get all pending repos.
    pub fn pending_repos(&self) -> Vec<&RemoteUrl> {
        self.pending.keys().collect()
    }

    fn pop_stale(&mut self) {
        while let Some(Reverse((fire_at, remote))) = self.heap.peek() {
            match self.pending.get(remote).map(|scheduled| scheduled.fire_at) {
                Some(current) if current == *fire_at => break,
                _ => {
                    let _ = self.heap.pop();
                }
            }
        }
    }

    fn issue_gate(&self, remote: RemoteUrl) -> SyncGateToken {
        let epoch = self.current_epoch(&remote);
        SyncGateToken::new(remote, epoch)
    }

    fn current_epoch(&self, remote: &RemoteUrl) -> u64 {
        self.epochs.get(remote).copied().unwrap_or(0)
    }

    fn bump_epoch(&mut self, remote: &RemoteUrl) {
        let epoch = self.next_epoch;
        self.next_epoch += 1;
        self.epochs.insert(remote.clone(), epoch);
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
        scheduler.schedule_after_at(
            remote.clone(),
            Duration::from_millis(10),
            base,
            ScheduledSyncKind::Backoff,
        );

        assert!(scheduler.is_pending(&remote));
        assert_eq!(
            scheduler.next_deadline(),
            Some(base + Duration::from_millis(10))
        );

        let due = scheduler.drain_due(base + Duration::from_millis(9));
        assert!(due.is_empty());

        let due = scheduler.drain_due(base + Duration::from_millis(10));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].remote(), &remote);
        assert!(!scheduler.is_pending(&remote));
    }

    #[test]
    fn reschedule_debounces_later() {
        let mut scheduler = SyncScheduler::with_delay(Duration::from_millis(10));

        let remote = RemoteUrl("example.com/foo/bar".into());
        let base = Instant::now();

        scheduler.schedule_after_at(
            remote.clone(),
            Duration::from_millis(10),
            base,
            ScheduledSyncKind::Debounce,
        );
        scheduler.schedule_after_at(
            remote.clone(),
            Duration::from_millis(10),
            base + Duration::from_millis(5),
            ScheduledSyncKind::Debounce,
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

        scheduler.schedule_after_at(
            remote.clone(),
            Duration::from_millis(10),
            base,
            ScheduledSyncKind::Debounce,
        );
        // Longer backoff should extend the deadline, not be ignored.
        scheduler.schedule_after_at(
            remote.clone(),
            Duration::from_millis(50),
            base + Duration::from_millis(1),
            ScheduledSyncKind::Backoff,
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
                ScheduledSyncKind::Debounce,
            );
        }

        let due = scheduler.drain_due(base + Duration::from_millis(1010));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].remote(), &remote);
        assert!(scheduler.next_deadline().is_none());
        assert!(!scheduler.is_pending(&remote));
    }

    #[test]
    fn immediate_gate_consumes_debounce_but_not_backoff() {
        let mut scheduler = SyncScheduler::with_delay(Duration::from_millis(10));
        let remote = RemoteUrl("example.com/foo/bar".into());
        let base = Instant::now();

        scheduler.schedule_after_at(
            remote.clone(),
            Duration::from_millis(10),
            base,
            ScheduledSyncKind::Debounce,
        );
        let gate = scheduler
            .issue_immediate_gate(&remote, base + Duration::from_millis(1))
            .expect("debounce gate");
        assert_eq!(gate.remote(), &remote);
        assert!(!scheduler.is_pending(&remote));

        scheduler.schedule_after_at(
            remote.clone(),
            Duration::from_millis(10),
            base,
            ScheduledSyncKind::Backoff,
        );
        assert!(
            scheduler
                .issue_immediate_gate(&remote, base + Duration::from_millis(1))
                .is_none(),
            "backoff windows must not be bypassed"
        );
        assert!(scheduler.is_pending(&remote));
    }

    #[test]
    fn future_backoff_still_blocks_when_later_debounce_was_already_pending() {
        let mut scheduler = SyncScheduler::with_delay(Duration::from_millis(10));
        let remote = RemoteUrl("example.com/foo/bar".into());
        let base = Instant::now();

        scheduler.schedule_after_at(
            remote.clone(),
            Duration::from_millis(50),
            base,
            ScheduledSyncKind::Debounce,
        );
        scheduler.schedule_after_at(
            remote.clone(),
            Duration::from_millis(10),
            base + Duration::from_millis(1),
            ScheduledSyncKind::Backoff,
        );

        assert!(
            scheduler
                .issue_immediate_gate(&remote, base + Duration::from_millis(2))
                .is_none(),
            "future backoff must dominate even when an older debounce kept a later deadline"
        );
    }

    #[test]
    fn scheduling_change_invalidates_issued_gate() {
        let mut scheduler = SyncScheduler::with_delay(Duration::from_millis(10));
        let remote = RemoteUrl("example.com/foo/bar".into());
        let gate = scheduler
            .issue_immediate_gate(&remote, Instant::now())
            .expect("initial gate");
        assert!(scheduler.gate_is_current(&gate));

        scheduler.schedule_after(remote.clone(), Duration::from_secs(30));

        assert!(
            !scheduler.gate_is_current(&gate),
            "new scheduling state must invalidate previously issued gates"
        );
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
