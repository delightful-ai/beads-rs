//! Sync observer seam for git sync side effects.

pub trait SyncObserver: Send + Sync + 'static {
    fn on_fetch_start(&self) {}
    fn on_fetch_end(&self, _elapsed_ms: u64) {}
    fn on_push_start(&self) {}
    fn on_push_end(&self, _elapsed_ms: u64) {}
    fn on_merge_start(&self) {}
    fn on_merge_end(&self, _beads_merged: usize) {}
    fn on_backup_ref_scan(&self, _count: usize) {}
    fn on_backup_ref_pruned(&self, _count: usize) {}
    fn on_backup_ref_lock_contention(&self, _operation: &'static str) {}
    fn on_backup_ref_lock_cleanup(&self, _operation: &'static str, _outcome: &'static str) {}
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopSyncObserver;

impl SyncObserver for NoopSyncObserver {}
