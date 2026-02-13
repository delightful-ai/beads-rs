use std::time::Instant;

use crossbeam::channel::{Receiver, Sender};

use super::super::git_worker::{GitOp, GitResult};
use super::daemon_api::StateLoopDaemon;
use super::waiters::{DurabilityWaiter, ReadGateWaiter};

pub(super) struct StateLoopEngine;

impl StateLoopEngine {
    fn earliest_deadline(deadlines: [Option<Instant>; 7]) -> Option<Instant> {
        deadlines.into_iter().flatten().min()
    }

    pub(super) fn next_deadline<D: StateLoopDaemon>(
        daemon: &mut D,
        read_gate_waiters: &[ReadGateWaiter],
        durability_waiters: &[DurabilityWaiter],
    ) -> Option<Instant> {
        let next_sync = daemon.next_sync_deadline();
        let next_checkpoint = daemon.next_checkpoint_deadline();
        let next_wal_checkpoint = daemon.next_wal_checkpoint_deadline();
        let next_lock_heartbeat = daemon.next_lock_heartbeat_deadline();
        let next_export = daemon.next_export_deadline();
        let next_read_gate = read_gate_waiters.iter().map(|waiter| waiter.deadline).min();
        let next_durability = durability_waiters
            .iter()
            .map(|waiter| waiter.deadline)
            .min();

        Self::earliest_deadline([
            next_sync,
            next_checkpoint,
            next_wal_checkpoint,
            next_lock_heartbeat,
            next_export,
            next_read_gate,
            next_durability,
        ])
    }

    pub(super) fn next_tick<D: StateLoopDaemon>(
        daemon: &mut D,
        read_gate_waiters: &[ReadGateWaiter],
        durability_waiters: &[DurabilityWaiter],
    ) -> Receiver<Instant> {
        match Self::next_deadline(daemon, read_gate_waiters, durability_waiters) {
            Some(deadline) => {
                let wait = deadline.saturating_duration_since(Instant::now());
                crossbeam::channel::after(wait)
            }
            None => crossbeam::channel::never(),
        }
    }

    pub(super) fn fire_due<D: StateLoopDaemon>(
        daemon: &mut D,
        git_tx: &Sender<GitOp>,
        include_wal_checkpoint: bool,
    ) {
        daemon.fire_due_syncs(git_tx);
        daemon.fire_due_checkpoints(git_tx);
        if include_wal_checkpoint {
            daemon.fire_due_wal_checkpoints();
        }
        daemon.fire_due_lock_heartbeats();
        daemon.fire_due_exports();
    }

    pub(super) fn apply_git_result<D: StateLoopDaemon>(daemon: &mut D, result: GitResult) {
        daemon.apply_git_result(result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeDaemon {
        next_sync: Option<Instant>,
        next_checkpoint: Option<Instant>,
        next_wal_checkpoint: Option<Instant>,
        next_lock_heartbeat: Option<Instant>,
        next_export: Option<Instant>,
        fire_calls: Vec<&'static str>,
        git_results_applied: usize,
    }

    impl StateLoopDaemon for FakeDaemon {
        fn next_sync_deadline(&mut self) -> Option<Instant> {
            self.next_sync
        }

        fn next_checkpoint_deadline(&mut self) -> Option<Instant> {
            self.next_checkpoint
        }

        fn next_wal_checkpoint_deadline(&mut self) -> Option<Instant> {
            self.next_wal_checkpoint
        }

        fn next_lock_heartbeat_deadline(&mut self) -> Option<Instant> {
            self.next_lock_heartbeat
        }

        fn next_export_deadline(&mut self) -> Option<Instant> {
            self.next_export
        }

        fn fire_due_syncs(&mut self, _git_tx: &Sender<GitOp>) {
            self.fire_calls.push("sync");
        }

        fn fire_due_checkpoints(&mut self, _git_tx: &Sender<GitOp>) {
            self.fire_calls.push("checkpoint");
        }

        fn fire_due_wal_checkpoints(&mut self) {
            self.fire_calls.push("wal_checkpoint");
        }

        fn fire_due_lock_heartbeats(&mut self) {
            self.fire_calls.push("lock_heartbeat");
        }

        fn fire_due_exports(&mut self) {
            self.fire_calls.push("export");
        }

        fn apply_git_result(&mut self, _result: GitResult) {
            self.git_results_applied += 1;
        }
    }

    #[test]
    fn earliest_deadline_picks_minimum() {
        let now = Instant::now();
        let deadlines = [
            Some(now + std::time::Duration::from_millis(30)),
            None,
            Some(now + std::time::Duration::from_millis(5)),
            Some(now + std::time::Duration::from_millis(10)),
            None,
            None,
            None,
        ];

        assert_eq!(
            StateLoopEngine::earliest_deadline(deadlines),
            Some(now + std::time::Duration::from_millis(5))
        );
    }

    #[test]
    fn next_deadline_reads_daemon_deadlines() {
        let now = Instant::now();
        let mut daemon = FakeDaemon {
            next_sync: Some(now + std::time::Duration::from_millis(50)),
            next_checkpoint: Some(now + std::time::Duration::from_millis(20)),
            next_wal_checkpoint: None,
            next_lock_heartbeat: Some(now + std::time::Duration::from_millis(40)),
            next_export: None,
            fire_calls: Vec::new(),
            git_results_applied: 0,
        };

        let deadline = StateLoopEngine::next_deadline(&mut daemon, &[], &[]);
        assert_eq!(deadline, Some(now + std::time::Duration::from_millis(20)));
    }

    #[test]
    fn fire_due_respects_wal_checkpoint_flag() {
        let mut daemon = FakeDaemon {
            next_sync: None,
            next_checkpoint: None,
            next_wal_checkpoint: None,
            next_lock_heartbeat: None,
            next_export: None,
            fire_calls: Vec::new(),
            git_results_applied: 0,
        };
        let (git_tx, _git_rx) = crossbeam::channel::unbounded();

        StateLoopEngine::fire_due(&mut daemon, &git_tx, false);
        assert_eq!(
            daemon.fire_calls,
            vec!["sync", "checkpoint", "lock_heartbeat", "export"]
        );

        daemon.fire_calls.clear();
        StateLoopEngine::fire_due(&mut daemon, &git_tx, true);
        assert_eq!(
            daemon.fire_calls,
            vec![
                "sync",
                "checkpoint",
                "wal_checkpoint",
                "lock_heartbeat",
                "export"
            ]
        );
    }
}
