use std::time::{Duration, Instant};

use beads_daemon::remote::RemoteUrl;

use super::super::export_worker::ExportJob;
use super::super::metrics;
use super::Daemon;
use crate::core::{CanonicalState, NamespaceId, StoreId, WallClock};

const STORE_LOCK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const EXPORT_DEBOUNCE: Duration = Duration::from_millis(250);

pub(super) struct ExportPending {
    remote: RemoteUrl,
    deadline: Instant,
}

impl Daemon {
    /// Schedule export of core namespace state to Go-compatible JSONL format.
    ///
    /// Export runs asynchronously and is debounced to avoid per-mutation work.
    pub(crate) fn export_go_compat(&mut self, store_id: StoreId, remote: &RemoteUrl) {
        if self.export_worker.is_none() {
            return;
        }
        if !self.stores.contains_key(&store_id) {
            return;
        }

        let deadline = Instant::now() + EXPORT_DEBOUNCE;
        self.export_pending.insert(
            store_id,
            ExportPending {
                remote: remote.clone(),
                deadline,
            },
        );
    }

    pub(in crate::daemon) fn next_export_deadline(&mut self) -> Option<Instant> {
        self.export_pending
            .values()
            .map(|pending| pending.deadline)
            .min()
    }

    pub(in crate::daemon) fn fire_due_exports(&mut self) {
        if self.export_pending.is_empty() {
            return;
        }

        let now = Instant::now();
        let due: Vec<StoreId> = self
            .export_pending
            .iter()
            .filter_map(|(store_id, pending)| (pending.deadline <= now).then_some(*store_id))
            .collect();

        let mut jobs = Vec::new();
        for store_id in due {
            let Some(pending) = self.export_pending.remove(&store_id) else {
                continue;
            };
            let Some(store) = self.stores.get(&store_id) else {
                continue;
            };
            let empty_state = CanonicalState::new();
            let core_state = store
                .state
                .get(&NamespaceId::core())
                .unwrap_or(&empty_state)
                .clone();
            let known_paths = self
                .git_lanes
                .get(&store_id)
                .map(|lane| lane.known_paths.clone())
                .unwrap_or_default();

            jobs.push(ExportJob {
                remote: pending.remote,
                core_state,
                known_paths,
            });
        }

        if let Some(worker) = self.export_worker.as_ref() {
            for job in jobs {
                if worker.enqueue(job).is_err() {
                    tracing::warn!("Go-compat export queue failed");
                    break;
                }
            }
        }
    }

    pub(in crate::daemon) fn next_wal_checkpoint_deadline(&mut self) -> Option<Instant> {
        self.next_wal_checkpoint_deadline_at(Instant::now())
    }

    pub(in crate::daemon::core) fn next_wal_checkpoint_deadline_at(
        &self,
        now: Instant,
    ) -> Option<Instant> {
        let interval = self.wal_checkpoint_interval()?;
        self.stores
            .values()
            .filter_map(|store| store.wal_checkpoint_deadline(now, interval))
            .min()
    }

    pub(in crate::daemon) fn next_lock_heartbeat_deadline(&mut self) -> Option<Instant> {
        self.next_lock_heartbeat_deadline_at(Instant::now())
    }

    pub(in crate::daemon::core) fn next_lock_heartbeat_deadline_at(
        &self,
        now: Instant,
    ) -> Option<Instant> {
        self.stores
            .values()
            .filter_map(|store| store.lock_heartbeat_deadline(now, STORE_LOCK_HEARTBEAT_INTERVAL))
            .min()
    }

    pub(in crate::daemon) fn fire_due_wal_checkpoints(&mut self) {
        self.fire_due_wal_checkpoints_at(Instant::now());
    }

    pub(in crate::daemon::core) fn fire_due_wal_checkpoints_at(&mut self, now: Instant) {
        let Some(interval) = self.wal_checkpoint_interval() else {
            return;
        };
        for (store_id, store) in self.stores.iter_mut() {
            if !store.wal_checkpoint_due(now, interval) {
                continue;
            }
            let start = Instant::now();
            match store.wal_index.checkpoint_truncate() {
                Ok(()) => {
                    metrics::wal_index_checkpoint_ok(start.elapsed());
                    store.mark_wal_checkpoint(now);
                }
                Err(err) => {
                    metrics::wal_index_checkpoint_err(start.elapsed());
                    tracing::warn!(
                        store_id = %store_id,
                        error = ?err,
                        "wal sqlite checkpoint failed"
                    );
                    store.mark_wal_checkpoint(now);
                }
            }
        }
    }

    pub(in crate::daemon) fn fire_due_lock_heartbeats(&mut self) {
        self.fire_due_lock_heartbeats_at(
            Instant::now(),
            STORE_LOCK_HEARTBEAT_INTERVAL,
            WallClock::now().0,
        );
    }

    pub(in crate::daemon::core) fn fire_due_lock_heartbeats_at(
        &mut self,
        now: Instant,
        interval: Duration,
        now_ms: u64,
    ) {
        if interval == Duration::ZERO {
            return;
        }
        for (store_id, store) in self.stores.iter_mut() {
            if !store.lock_heartbeat_due(now, interval) {
                continue;
            }
            match store.update_lock_heartbeat(now_ms) {
                Ok(()) => {
                    store.mark_lock_heartbeat(now);
                }
                Err(err) => {
                    tracing::warn!(
                        store_id = %store_id,
                        error = ?err,
                        "store lock heartbeat update failed"
                    );
                    store.mark_lock_heartbeat(now);
                }
            }
        }
    }

    fn wal_checkpoint_interval(&self) -> Option<Duration> {
        let interval_ms = self.limits.wal_sqlite_checkpoint_interval_ms;
        if interval_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(interval_ms))
        }
    }
}
