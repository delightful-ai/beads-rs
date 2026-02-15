use std::time::Instant;

use crossbeam::channel::Sender;

use super::super::core::Daemon;
use super::super::git_worker::{GitOp, GitResult};

pub(in crate::daemon::server) trait StateLoopDaemon {
    fn next_sync_deadline(&mut self) -> Option<Instant>;
    fn next_checkpoint_deadline(&mut self) -> Option<Instant>;
    fn next_wal_checkpoint_deadline(&mut self) -> Option<Instant>;
    fn next_lock_heartbeat_deadline(&mut self) -> Option<Instant>;
    fn next_export_deadline(&mut self) -> Option<Instant>;

    fn fire_due_syncs(&mut self, git_tx: &Sender<GitOp>);
    fn fire_due_checkpoints(&mut self, git_tx: &Sender<GitOp>);
    fn fire_due_wal_checkpoints(&mut self);
    fn fire_due_lock_heartbeats(&mut self);
    fn fire_due_exports(&mut self);

    fn apply_git_result(&mut self, result: GitResult);
}

impl StateLoopDaemon for Daemon {
    fn next_sync_deadline(&mut self) -> Option<Instant> {
        Daemon::next_sync_deadline(self)
    }

    fn next_checkpoint_deadline(&mut self) -> Option<Instant> {
        Daemon::next_checkpoint_deadline(self)
    }

    fn next_wal_checkpoint_deadline(&mut self) -> Option<Instant> {
        Daemon::next_wal_checkpoint_deadline(self)
    }

    fn next_lock_heartbeat_deadline(&mut self) -> Option<Instant> {
        Daemon::next_lock_heartbeat_deadline(self)
    }

    fn next_export_deadline(&mut self) -> Option<Instant> {
        Daemon::next_export_deadline(self)
    }

    fn fire_due_syncs(&mut self, git_tx: &Sender<GitOp>) {
        Daemon::fire_due_syncs(self, git_tx)
    }

    fn fire_due_checkpoints(&mut self, git_tx: &Sender<GitOp>) {
        Daemon::fire_due_checkpoints(self, git_tx)
    }

    fn fire_due_wal_checkpoints(&mut self) {
        Daemon::fire_due_wal_checkpoints(self)
    }

    fn fire_due_lock_heartbeats(&mut self) {
        Daemon::fire_due_lock_heartbeats(self)
    }

    fn fire_due_exports(&mut self) {
        Daemon::fire_due_exports(self)
    }

    fn apply_git_result(&mut self, result: GitResult) {
        match result {
            GitResult::Sync(remote, sync_result) => {
                self.complete_sync(&remote, sync_result);
            }
            GitResult::Refresh(remote, refresh_result) => {
                self.complete_refresh(&remote, refresh_result);
            }
            GitResult::Checkpoint(store_id, group, result) => {
                self.complete_checkpoint(store_id, &group, result);
            }
        }
    }
}
