//! Store runtime state and on-disk identity handling.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(test)]
use std::sync::{LazyLock, MutexGuard};

use rand::RngCore;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use uuid::Uuid;

use crate::admission::AdmissionController;
use crate::broadcast::{BroadcasterLimits, EventBroadcaster};
use crate::core::error::details as error_details;
use crate::core::error::details::WalTailTruncatedDetails;
use crate::core::{
    ActorId, Applied, ApplyOutcome, CanonicalState, CliErrorCode, ContentHash, Durable, ErrorCode,
    ErrorPayload, HeadStatus, IntoErrorPayload, Limits, NamespaceId, NamespacePolicies,
    NamespacePolicy, ProtocolErrorCode, ReplicaId, ReplicaRoster, ReplicaRosterError, SegmentId,
    StoreEpoch, StoreId, StoreIdentity, StoreMeta, StoreMetaVersions, StoreState, Transience,
    WatermarkError, Watermarks, WriteStamp,
};
use crate::git::checkpoint::{
    CheckpointFileKind, CheckpointShardPath, CheckpointSnapshot, CheckpointSnapshotError,
    CheckpointSnapshotInput, build_snapshot, policy_hash, roster_hash, shard_for_bead,
    shard_for_dep, shard_for_tombstone,
};
use crate::layout::DaemonLayout;
use crate::remote::RemoteUrl;
use crate::runtime::repl::PeerAckTable;
use crate::runtime::store::lock::{StoreLock, StoreLockError};
use crate::runtime::wal::{
    EventWal, HlcRow, IndexDurabilityMode, ReplayStats, SqliteWalIndex, WalIndex, WalIndexError,
    WalReplayError, catch_up_index, rebuild_index,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct StoreConfig {
    index_durability_mode: IndexDurabilityMode,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            index_durability_mode: IndexDurabilityMode::Cache,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WalTailTruncatedRecord {
    pub namespace: NamespaceId,
    pub segment_id: Option<SegmentId>,
    pub truncated_from_offset: u64,
    pub wall_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ReplicationRuntimeVersion(u64);

impl ReplicationRuntimeVersion {
    const INITIAL: Self = Self(1);

    fn next(self) -> Self {
        Self(
            self.0
                .checked_add(1)
                .expect("replication runtime version overflow"),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReplicaIdRotation {
    pub old_replica_id: ReplicaId,
    pub new_replica_id: ReplicaId,
    pub runtime_version: ReplicationRuntimeVersion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PolicyReloadOutcome {
    pub reload_replication_runtime: bool,
}

pub struct StoreRuntime {
    layout: DaemonLayout,
    pub primary_remote: RemoteUrl,
    pub meta: StoreMeta,
    replication_runtime_version: ReplicationRuntimeVersion,
    pub policies: BTreeMap<NamespaceId, NamespacePolicy>,
    pub state: StoreState,
    pub last_wal_tail_truncated: Option<WalTailTruncatedRecord>,
    pub watermarks_applied: Watermarks<Applied>,
    pub watermarks_durable: Watermarks<Durable>,
    checkpoint_dirty_shards: BTreeMap<NamespaceId, BTreeSet<CheckpointShardPath>>,
    checkpoint_dirty_inflight:
        BTreeMap<String, BTreeMap<NamespaceId, BTreeSet<CheckpointShardPath>>>,
    pub broadcaster: EventBroadcaster,
    pub admission: AdmissionController,
    pub maintenance_mode: bool,
    pub peer_acks: Arc<Mutex<PeerAckTable>>,
    pub event_wal: EventWal,
    pub wal_index: Arc<dyn WalIndex>,
    pub last_wal_checkpoint: Option<Instant>,
    pub last_lock_heartbeat: Option<Instant>,
    lock: StoreLock,
}

pub struct StoreRuntimeOpen {
    pub runtime: StoreRuntime,
    pub replay_stats: ReplayStats,
}

enum StoreMetaOpenState {
    Ready(StoreMeta),
    Pending(PendingStoreMetaTransition),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PendingStoreMetaTransitionRecord {
    meta: StoreMeta,
}

struct PendingStoreMetaTransition {
    meta_path: PathBuf,
    pending_path: PathBuf,
    meta: StoreMeta,
    marker_persisted: bool,
}

struct NormalizedPendingStoreMetaTransition {
    record: PendingStoreMetaTransitionRecord,
    marker_persisted: bool,
}

impl StoreMetaOpenState {
    fn meta(&self) -> &StoreMeta {
        match self {
            StoreMetaOpenState::Ready(meta) => meta,
            StoreMetaOpenState::Pending(pending) => &pending.meta,
        }
    }

    fn commit(self) -> Result<StoreMeta, StoreRuntimeError> {
        match self {
            StoreMetaOpenState::Ready(meta) => Ok(meta),
            StoreMetaOpenState::Pending(pending) => pending.commit(),
        }
    }

    fn ensure_pending_persisted(&mut self) -> Result<(), StoreRuntimeError> {
        if let StoreMetaOpenState::Pending(pending) = self {
            pending.ensure_persisted()?;
        }
        Ok(())
    }

    fn pending(&self) -> Option<&PendingStoreMetaTransition> {
        match self {
            StoreMetaOpenState::Ready(_) => None,
            StoreMetaOpenState::Pending(pending) => Some(pending),
        }
    }
}

impl PendingStoreMetaTransition {
    fn new(meta_path: PathBuf, pending_path: PathBuf, meta: StoreMeta) -> Self {
        Self {
            meta_path,
            pending_path,
            meta,
            marker_persisted: false,
        }
    }

    fn persisted(meta_path: PathBuf, pending_path: PathBuf, meta: StoreMeta) -> Self {
        Self {
            meta_path,
            pending_path,
            meta,
            marker_persisted: true,
        }
    }

    fn ensure_persisted(&mut self) -> Result<(), StoreRuntimeError> {
        if !self.marker_persisted {
            write_pending_store_meta_transition(
                &self.pending_path,
                &PendingStoreMetaTransitionRecord {
                    meta: self.meta.clone(),
                },
            )?;
            self.marker_persisted = true;
        }
        Ok(())
    }

    fn commit(self) -> Result<StoreMeta, StoreRuntimeError> {
        write_store_meta_for_open(&self.meta_path, &self.meta)?;
        #[cfg(test)]
        maybe_panic_store_meta_transition("store_meta_transition_after_meta_commit");
        if let Err(err) = remove_pending_store_meta_transition(&self.pending_path) {
            tracing::warn!(
                pending_path = %self.pending_path.display(),
                error = ?err,
                "store meta transition marker cleanup failed after committed meta write"
            );
        }
        Ok(self.meta)
    }
}

fn derive_store_meta_open_state(
    existing: Option<&StoreMeta>,
    meta_path: &Path,
    pending_path: &Path,
    store_id: StoreId,
    now_ms: u64,
    expected_versions: StoreMetaVersions,
) -> Result<StoreMetaOpenState, StoreRuntimeError> {
    match existing {
        Some(meta) => {
            if meta.store_id() != store_id {
                return Err(StoreRuntimeError::MetaMismatch {
                    expected: store_id,
                    got: meta.store_id(),
                });
            }
            let got_versions = meta.versions();
            if got_versions == expected_versions {
                Ok(StoreMetaOpenState::Ready(meta.clone()))
            } else if can_upgrade_index_schema_only(got_versions, expected_versions) {
                let mut upgraded = meta.clone();
                upgraded.index_schema_version = expected_versions.index_schema_version;
                Ok(StoreMetaOpenState::Pending(
                    PendingStoreMetaTransition::new(
                        meta_path.to_path_buf(),
                        pending_path.to_path_buf(),
                        upgraded,
                    ),
                ))
            } else {
                Err(StoreRuntimeError::UnsupportedStoreMetaVersion {
                    expected: expected_versions,
                    got: got_versions,
                })
            }
        }
        None => {
            let identity = StoreIdentity::new(store_id, StoreEpoch::ZERO);
            Ok(StoreMetaOpenState::Pending(
                PendingStoreMetaTransition::new(
                    meta_path.to_path_buf(),
                    pending_path.to_path_buf(),
                    StoreMeta::new(identity, new_replica_id(), expected_versions, now_ms),
                ),
            ))
        }
    }
}

fn normalize_pending_store_meta_transition(
    pending: &PendingStoreMetaTransitionRecord,
    store_id: StoreId,
    expected_versions: StoreMetaVersions,
) -> Result<NormalizedPendingStoreMetaTransition, StoreRuntimeError> {
    if pending.meta.store_id() != store_id {
        return Err(StoreRuntimeError::MetaMismatch {
            expected: store_id,
            got: pending.meta.store_id(),
        });
    }

    let got_versions = pending.meta.versions();
    if got_versions == expected_versions {
        Ok(NormalizedPendingStoreMetaTransition {
            record: pending.clone(),
            marker_persisted: true,
        })
    } else if can_upgrade_index_schema_only(got_versions, expected_versions) {
        let mut upgraded = pending.meta.clone();
        upgraded.index_schema_version = expected_versions.index_schema_version;
        Ok(NormalizedPendingStoreMetaTransition {
            record: PendingStoreMetaTransitionRecord { meta: upgraded },
            marker_persisted: false,
        })
    } else {
        Err(StoreRuntimeError::UnsupportedStoreMetaVersion {
            expected: expected_versions,
            got: got_versions,
        })
    }
}

fn recover_pending_store_meta_transition(
    layout: &DaemonLayout,
    store_id: StoreId,
    committed: Option<&StoreMeta>,
    meta_path: PathBuf,
    pending_path: PathBuf,
    expected_versions: StoreMetaVersions,
    pending: PendingStoreMetaTransitionRecord,
) -> Result<StoreMetaOpenState, StoreRuntimeError> {
    if pending.meta.store_id() != store_id {
        return Err(StoreRuntimeError::MetaMismatch {
            expected: store_id,
            got: pending.meta.store_id(),
        });
    }
    if pending.meta.versions() != expected_versions {
        return Err(StoreRuntimeError::UnsupportedStoreMetaVersion {
            expected: expected_versions,
            got: pending.meta.versions(),
        });
    }
    if let Some(meta) = committed {
        if meta.store_id() != store_id {
            return Err(StoreRuntimeError::MetaMismatch {
                expected: store_id,
                got: meta.store_id(),
            });
        }
        if meta == &pending.meta {
            if let Err(err) = remove_pending_store_meta_transition(&pending_path) {
                tracing::warn!(
                    pending_path = %pending_path.display(),
                    error = ?err,
                    "stale store meta transition marker cleanup failed during recovery"
                );
            }
            return Ok(StoreMetaOpenState::Ready(meta.clone()));
        }
    }
    remove_wal_index_files_with_layout(layout, store_id)?;
    Ok(StoreMetaOpenState::Pending(
        PendingStoreMetaTransition::persisted(meta_path, pending_path, pending.meta),
    ))
}

fn rollback_store_meta_transition_after_failure(
    layout: &DaemonLayout,
    store_id: StoreId,
    pending_path: &Path,
) -> Result<(), StoreRuntimeError> {
    remove_wal_index_files_with_layout(layout, store_id)?;
    remove_pending_store_meta_transition(pending_path)
}

pub(crate) fn checkpoint_dirty_paths_for_outcome(
    namespace: &NamespaceId,
    state: &CanonicalState,
    outcome: &ApplyOutcome,
) -> BTreeSet<CheckpointShardPath> {
    let mut dirty = BTreeSet::new();
    if outcome.changed_beads.is_empty()
        && outcome.changed_deps.is_empty()
        && outcome.changed_notes.is_empty()
    {
        return dirty;
    }
    for bead_id in &outcome.changed_beads {
        if state.bead_view(bead_id).is_some() {
            let shard = shard_for_bead(bead_id);
            dirty.insert(CheckpointShardPath::new(
                namespace.clone(),
                CheckpointFileKind::State,
                shard,
            ));
        }
        if state.get_tombstone(bead_id).is_some() || state.has_collision_tombstone(bead_id) {
            let tombstone_shard = shard_for_tombstone(bead_id);
            dirty.insert(CheckpointShardPath::new(
                namespace.clone(),
                CheckpointFileKind::Tombstones,
                tombstone_shard,
            ));
        }
    }
    for dep_key in &outcome.changed_deps {
        let shard = shard_for_dep(dep_key.from(), dep_key.to(), dep_key.kind());
        dirty.insert(CheckpointShardPath::new(
            namespace.clone(),
            CheckpointFileKind::Deps,
            shard,
        ));
    }
    for note_key in &outcome.changed_notes {
        if state.bead_view(&note_key.bead_id).is_some() {
            let shard = shard_for_bead(&note_key.bead_id);
            dirty.insert(CheckpointShardPath::new(
                namespace.clone(),
                CheckpointFileKind::State,
                shard,
            ));
        }
        if state.get_tombstone(&note_key.bead_id).is_some()
            || state.has_collision_tombstone(&note_key.bead_id)
        {
            let tombstone_shard = shard_for_tombstone(&note_key.bead_id);
            dirty.insert(CheckpointShardPath::new(
                namespace.clone(),
                CheckpointFileKind::Tombstones,
                tombstone_shard,
            ));
        }
    }
    dirty
}

impl StoreRuntime {
    pub fn open(
        layout: &DaemonLayout,
        store_id: StoreId,
        primary_remote: RemoteUrl,
        now_ms: u64,
        daemon_version: &str,
        limits: &Limits,
        namespace_defaults: &BTreeMap<NamespaceId, NamespacePolicy>,
    ) -> Result<StoreRuntimeOpen, StoreRuntimeError> {
        let meta_path = layout.store_meta_path(&store_id);
        let pending_path = layout.store_meta_pending_path(&store_id);
        let existing = read_store_meta_optional(&meta_path)?;
        let pending = read_pending_store_meta_transition_optional(&pending_path)?;

        let expected_versions = StoreMetaVersions::current();
        let normalized_pending = pending
            .as_ref()
            .map(|pending| {
                normalize_pending_store_meta_transition(pending, store_id, expected_versions)
            })
            .transpose()?;
        let mut meta_state = match normalized_pending.as_ref() {
            Some(pending) => {
                let transition = if pending.marker_persisted {
                    PendingStoreMetaTransition::persisted(
                        meta_path.clone(),
                        pending_path.clone(),
                        pending.record.meta.clone(),
                    )
                } else {
                    PendingStoreMetaTransition::new(
                        meta_path.clone(),
                        pending_path.clone(),
                        pending.record.meta.clone(),
                    )
                };
                StoreMetaOpenState::Pending(transition)
            }
            None => derive_store_meta_open_state(
                existing.as_ref(),
                &meta_path,
                &pending_path,
                store_id,
                now_ms,
                expected_versions,
            )?,
        };

        let lock = StoreLock::acquire(
            layout,
            store_id,
            meta_state.meta().replica_id,
            now_ms,
            daemon_version,
        )?;

        if let Some(pending) = normalized_pending.map(|pending| pending.record) {
            meta_state = recover_pending_store_meta_transition(
                layout,
                store_id,
                existing.as_ref(),
                meta_path.clone(),
                pending_path.clone(),
                expected_versions,
                pending,
            )?;
        }
        if existing.is_none() && !matches!(meta_state, StoreMetaOpenState::Ready(_)) {
            remove_wal_index_files_with_layout(layout, store_id)?;
        }
        meta_state.ensure_pending_persisted()?;
        let pending_cleanup_path = meta_state
            .pending()
            .map(|pending| pending.pending_path.clone());
        let open_result =
            (|| -> Result<(SqliteWalIndex, ReplayStats, StoreMeta), StoreRuntimeError> {
                let store_dir = layout.store_dir(&store_id);
                let store_config = load_store_config_with_layout(layout, store_id, true)?;
                let (mut wal_index, needs_rebuild) = open_wal_index_with_layout(
                    layout,
                    store_id,
                    &store_dir,
                    meta_state.meta(),
                    store_config.index_durability_mode,
                )?;
                let replay_stats = if needs_rebuild {
                    rebuild_index(&store_dir, meta_state.meta(), &wal_index, limits)?
                } else {
                    match catch_up_index(&store_dir, meta_state.meta(), &wal_index, limits) {
                        Ok(stats) => stats,
                        Err(WalReplayError::IndexOffsetInvalid { .. }) => {
                            remove_wal_index_files_with_layout(layout, store_id)?;
                            wal_index = SqliteWalIndex::open(
                                &store_dir,
                                meta_state.meta(),
                                store_config.index_durability_mode,
                            )?;
                            rebuild_index(&store_dir, meta_state.meta(), &wal_index, limits)?
                        }
                        Err(err) => return Err(StoreRuntimeError::WalReplay(Box::new(err))),
                    }
                };
                #[cfg(test)]
                maybe_panic_store_meta_transition("store_meta_transition_before_meta_commit");
                let meta = meta_state.commit()?;
                Ok((wal_index, replay_stats, meta))
            })();
        let (wal_index, replay_stats, meta) = match open_result {
            Ok(result) => result,
            Err(err) => {
                if let Some(pending_path) = pending_cleanup_path.as_deref()
                    && let Err(rollback_err) =
                        rollback_store_meta_transition_after_failure(layout, store_id, pending_path)
                {
                    tracing::warn!(
                        store_id = %store_id,
                        rollback_error = ?rollback_err,
                        original_error = ?err,
                        "store meta transition cleanup failed after open error"
                    );
                }
                return Err(err);
            }
        };
        let store_dir = layout.store_dir(&store_id);

        let wal_index: Arc<dyn WalIndex> = Arc::new(wal_index);
        let (watermarks_applied, watermarks_durable) = load_watermarks(wal_index.as_ref())?;
        let broadcaster = EventBroadcaster::new(BroadcasterLimits::from_limits(limits));
        let admission = AdmissionController::new(limits);
        let peer_acks = Arc::new(Mutex::new(PeerAckTable::new()));
        let mut last_wal_tail_truncated = None;
        for truncation in &replay_stats.tail_truncations {
            let payload = ErrorPayload::new(
                ProtocolErrorCode::WalTailTruncated.into(),
                "wal tail truncated",
                true,
            )
            .with_details(WalTailTruncatedDetails {
                namespace: truncation.namespace.clone(),
                segment_id: Some(truncation.segment_id),
                truncated_from_offset: truncation.truncated_from_offset,
            });
            tracing::warn!(payload = ?payload, "wal tail truncated");
            last_wal_tail_truncated = Some(WalTailTruncatedRecord {
                namespace: truncation.namespace.clone(),
                segment_id: Some(truncation.segment_id),
                truncated_from_offset: truncation.truncated_from_offset,
                wall_ms: now_ms,
            });
        }

        let event_wal = EventWal::new(store_dir.clone(), meta.clone(), limits);
        let now = Instant::now();
        let runtime = Self {
            layout: layout.clone(),
            primary_remote,
            meta,
            replication_runtime_version: ReplicationRuntimeVersion::INITIAL,
            policies: load_namespace_policies_with_layout(layout, store_id, namespace_defaults)?,
            state: StoreState::new(),
            last_wal_tail_truncated,
            watermarks_applied,
            watermarks_durable,
            checkpoint_dirty_shards: BTreeMap::new(),
            checkpoint_dirty_inflight: BTreeMap::new(),
            broadcaster,
            admission,
            maintenance_mode: false,
            peer_acks,
            event_wal,
            wal_index,
            last_wal_checkpoint: None,
            last_lock_heartbeat: Some(now),
            lock,
        };

        Ok(StoreRuntimeOpen {
            runtime,
            replay_stats,
        })
    }

    pub fn applied_head_sha(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
    ) -> Option<[u8; 32]> {
        head_status_to_sha(
            self.watermarks_applied
                .get(namespace, origin)
                .copied()
                .map(|watermark| watermark.head()),
        )
    }

    pub fn reload_limits(&mut self, limits: &Limits) {
        self.admission = AdmissionController::new(limits);
        if let Err(err) = self
            .broadcaster
            .update_limits(BroadcasterLimits::from_limits(limits))
        {
            tracing::warn!("failed to update broadcaster limits: {err}");
        }
        self.event_wal.update_limits(limits);
    }

    pub fn durable_head_sha(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
    ) -> Option<[u8; 32]> {
        head_status_to_sha(
            self.watermarks_durable
                .get(namespace, origin)
                .copied()
                .map(|watermark| watermark.head()),
        )
    }

    pub(crate) fn wal_checkpoint_deadline(
        &self,
        now: Instant,
        interval: Duration,
    ) -> Option<Instant> {
        if interval == Duration::ZERO {
            return None;
        }
        Some(match self.last_wal_checkpoint {
            Some(last) => last + interval,
            None => now,
        })
    }

    pub(crate) fn wal_checkpoint_due(&self, now: Instant, interval: Duration) -> bool {
        self.wal_checkpoint_deadline(now, interval)
            .is_some_and(|deadline| deadline <= now)
    }

    pub(crate) fn mark_wal_checkpoint(&mut self, now: Instant) {
        self.last_wal_checkpoint = Some(now);
    }

    pub(crate) fn lock_heartbeat_deadline(
        &self,
        now: Instant,
        interval: Duration,
    ) -> Option<Instant> {
        if interval == Duration::ZERO {
            return None;
        }
        Some(match self.last_lock_heartbeat {
            Some(last) => last + interval,
            None => now,
        })
    }

    pub(crate) fn lock_heartbeat_due(&self, now: Instant, interval: Duration) -> bool {
        self.lock_heartbeat_deadline(now, interval)
            .is_some_and(|deadline| deadline <= now)
    }

    pub(crate) fn mark_lock_heartbeat(&mut self, now: Instant) {
        self.last_lock_heartbeat = Some(now);
    }

    pub(crate) fn update_lock_heartbeat(&mut self, now_ms: u64) -> Result<(), StoreLockError> {
        self.lock.update_heartbeat(now_ms)
    }

    pub(crate) fn layout(&self) -> &DaemonLayout {
        &self.layout
    }

    pub fn hlc_state_for_actor(
        &self,
        actor: &ActorId,
    ) -> Result<Option<WriteStamp>, StoreRuntimeError> {
        let rows = self.wal_index.reader().load_hlc()?;
        Ok(rows
            .into_iter()
            .find(|row| row.actor_id == *actor)
            .map(|row| WriteStamp::new(row.last_physical_ms, row.last_logical)))
    }

    pub fn hlc_rows(&self) -> Result<Vec<HlcRow>, StoreRuntimeError> {
        Ok(self.wal_index.reader().load_hlc()?)
    }

    fn checkpoint_roster_hash(&self) -> Result<Option<ContentHash>, CheckpointSnapshotError> {
        let roster = match load_replica_roster(&self.layout, self.meta.store_id()) {
            Ok(Some(roster)) => roster,
            Ok(None) => return Ok(None),
            Err(err) => {
                tracing::warn!(
                    store_id = %self.meta.store_id(),
                    error = ?err,
                    "replica roster load failed for checkpoint"
                );
                return Ok(None);
            }
        };

        Ok(Some(roster_hash(&roster)?))
    }

    pub fn checkpoint_snapshot_readonly(
        &self,
        checkpoint_group: &str,
        namespaces: &[NamespaceId],
        created_at_ms: u64,
    ) -> Result<CheckpointSnapshot, CheckpointSnapshotError> {
        let policy_hash = policy_hash(&self.policies)?;
        let roster_hash = self.checkpoint_roster_hash()?;
        build_snapshot(CheckpointSnapshotInput {
            checkpoint_group: checkpoint_group.to_string(),
            namespaces: namespaces.to_vec().into(),
            store_id: self.meta.store_id(),
            store_epoch: self.meta.store_epoch(),
            created_at_ms,
            created_by_replica_id: self.meta.replica_id,
            policy_hash,
            roster_hash,
            dirty_shards: None,
            state: &self.state,
            watermarks_durable: &self.watermarks_durable,
        })
    }

    pub fn checkpoint_snapshot(
        &mut self,
        checkpoint_group: &str,
        namespaces: &[NamespaceId],
        created_at_ms: u64,
    ) -> Result<CheckpointSnapshot, CheckpointSnapshotError> {
        let policy_hash = policy_hash(&self.policies)?;
        let roster_hash = self.checkpoint_roster_hash()?;
        let dirty_shards = self.begin_checkpoint_dirty_shards(checkpoint_group, namespaces);
        let snapshot = build_snapshot(CheckpointSnapshotInput {
            checkpoint_group: checkpoint_group.to_string(),
            namespaces: namespaces.to_vec().into(),
            store_id: self.meta.store_id(),
            store_epoch: self.meta.store_epoch(),
            created_at_ms,
            created_by_replica_id: self.meta.replica_id,
            policy_hash,
            roster_hash,
            dirty_shards: Some(dirty_shards),
            state: &self.state,
            watermarks_durable: &self.watermarks_durable,
        });
        if snapshot.is_err() {
            self.rollback_checkpoint_dirty_shards(checkpoint_group);
        }
        snapshot
    }

    pub(crate) fn record_checkpoint_dirty_shards(
        &mut self,
        namespace: &NamespaceId,
        outcome: &ApplyOutcome,
    ) {
        let Some(state) = self.state.get(namespace) else {
            return;
        };
        let paths = checkpoint_dirty_paths_for_outcome(namespace, state, outcome);
        self.record_checkpoint_dirty_paths(namespace, paths);
    }

    pub(crate) fn record_checkpoint_dirty_paths(
        &mut self,
        namespace: &NamespaceId,
        paths: BTreeSet<CheckpointShardPath>,
    ) {
        if paths.is_empty() {
            return;
        }
        self.checkpoint_dirty_shards
            .entry(namespace.clone())
            .or_default()
            .extend(paths);
    }

    pub(crate) fn commit_checkpoint_dirty_shards(&mut self, checkpoint_group: &str) {
        self.checkpoint_dirty_inflight.remove(checkpoint_group);
    }

    pub(crate) fn replication_runtime_version(&self) -> ReplicationRuntimeVersion {
        self.replication_runtime_version
    }

    pub(crate) fn reload_policies(
        &mut self,
        policies: BTreeMap<NamespaceId, NamespacePolicy>,
        reload_replication_runtime: bool,
    ) -> PolicyReloadOutcome {
        self.policies = policies;
        if reload_replication_runtime {
            self.replication_runtime_version = self.replication_runtime_version.next();
        }
        PolicyReloadOutcome {
            reload_replication_runtime,
        }
    }

    pub(crate) fn restore_policies(
        &mut self,
        policies: BTreeMap<NamespaceId, NamespacePolicy>,
        runtime_version: ReplicationRuntimeVersion,
    ) {
        self.policies = policies;
        self.replication_runtime_version = runtime_version;
    }

    pub(crate) fn rollback_checkpoint_dirty_shards(&mut self, checkpoint_group: &str) {
        let Some(in_flight) = self.checkpoint_dirty_inflight.remove(checkpoint_group) else {
            return;
        };
        for (namespace, shards) in in_flight {
            self.checkpoint_dirty_shards
                .entry(namespace)
                .or_default()
                .extend(shards);
        }
    }

    fn begin_checkpoint_dirty_shards(
        &mut self,
        checkpoint_group: &str,
        namespaces: &[NamespaceId],
    ) -> BTreeSet<CheckpointShardPath> {
        let mut in_flight: BTreeMap<NamespaceId, BTreeSet<CheckpointShardPath>> = BTreeMap::new();
        for namespace in namespaces {
            let shards = self
                .checkpoint_dirty_shards
                .remove(namespace)
                .unwrap_or_default();
            in_flight.insert(namespace.clone(), shards);
        }
        if let Some(existing) = self.checkpoint_dirty_inflight.remove(checkpoint_group) {
            tracing::warn!(
                checkpoint_group = checkpoint_group,
                "checkpoint dirty shards already in flight; merging"
            );
            for (namespace, shards) in existing {
                in_flight.entry(namespace).or_default().extend(shards);
            }
        }

        let dirty_shards = in_flight
            .values()
            .flat_map(|shards| shards.iter().cloned())
            .collect();
        self.checkpoint_dirty_inflight
            .insert(checkpoint_group.to_string(), in_flight);
        dirty_shards
    }

    pub(crate) fn rotate_replica_id(&mut self) -> Result<ReplicaIdRotation, StoreRuntimeError> {
        let old_replica_id = self.meta.replica_id;
        let new_replica_id = new_replica_id();
        self.meta.replica_id = new_replica_id;
        let path = self.layout.store_meta_path(&self.meta.store_id());
        write_store_meta(&path, &self.meta)?;
        self.replication_runtime_version = self.replication_runtime_version.next();
        Ok(ReplicaIdRotation {
            old_replica_id,
            new_replica_id,
            runtime_version: self.replication_runtime_version,
        })
    }
}

#[derive(Debug, Error)]
pub enum StoreRuntimeError {
    #[error(transparent)]
    Lock(#[from] StoreLockError),
    #[error("store meta path is a symlink: {path:?}")]
    MetaSymlink { path: Box<std::path::PathBuf> },
    #[error("store meta read failed at {path:?}: {source}")]
    MetaRead {
        path: Box<std::path::PathBuf>,
        #[source]
        source: io::Error,
    },
    #[error("store meta parse failed at {path:?}: {source}")]
    MetaParse {
        path: Box<std::path::PathBuf>,
        #[source]
        source: serde_json::Error,
    },
    #[error("store meta store_id mismatch: expected {expected}, got {got}")]
    MetaMismatch { expected: StoreId, got: StoreId },
    #[error("store meta versions unsupported: expected {expected:?}, got {got:?}")]
    UnsupportedStoreMetaVersion {
        expected: StoreMetaVersions,
        got: StoreMetaVersions,
    },
    #[error("store meta write failed at {path:?}: {source}")]
    MetaWrite {
        path: Box<std::path::PathBuf>,
        #[source]
        source: io::Error,
    },
    #[error("namespace policies read failed at {path:?}: {source}")]
    NamespacePoliciesRead {
        path: Box<std::path::PathBuf>,
        #[source]
        source: io::Error,
    },
    #[error("namespace policies path is a symlink: {path:?}")]
    NamespacePoliciesSymlink { path: Box<std::path::PathBuf> },
    #[error("namespace policies parse failed at {path:?}: {source}")]
    NamespacePoliciesParse {
        path: Box<std::path::PathBuf>,
        #[source]
        source: crate::core::NamespacePoliciesError,
    },
    #[error("replica roster path is a symlink: {path:?}")]
    ReplicaRosterSymlink { path: Box<std::path::PathBuf> },
    #[error("replica roster read failed at {path:?}: {source}")]
    ReplicaRosterRead {
        path: Box<std::path::PathBuf>,
        #[source]
        source: io::Error,
    },
    #[error("replica roster parse failed at {path:?}: {source}")]
    ReplicaRosterParse {
        path: Box<std::path::PathBuf>,
        #[source]
        source: ReplicaRosterError,
    },
    #[error("store config path is a symlink: {path:?}")]
    StoreConfigSymlink { path: Box<std::path::PathBuf> },
    #[error("store config read failed at {path:?}: {source}")]
    StoreConfigRead {
        path: Box<std::path::PathBuf>,
        #[source]
        source: io::Error,
    },
    #[error("store config parse failed at {path:?}: {source}")]
    StoreConfigParse {
        path: Box<std::path::PathBuf>,
        #[source]
        source: toml::de::Error,
    },
    #[error("store config serialize failed at {path:?}: {source}")]
    StoreConfigSerialize {
        path: Box<std::path::PathBuf>,
        #[source]
        source: toml::ser::Error,
    },
    #[error("store config write failed at {path:?}: {source}")]
    StoreConfigWrite {
        path: Box<std::path::PathBuf>,
        #[source]
        source: io::Error,
    },
    #[error(transparent)]
    WalIndex(#[from] WalIndexError),
    #[error(transparent)]
    WalReplay(#[from] Box<WalReplayError>),
    #[error("invalid {kind} watermark for {namespace} {origin}: {source}")]
    WatermarkInvalid {
        kind: &'static str,
        namespace: NamespaceId,
        origin: ReplicaId,
        #[source]
        source: Box<WatermarkError>,
    },
}

impl From<WalReplayError> for StoreRuntimeError {
    fn from(err: WalReplayError) -> Self {
        StoreRuntimeError::WalReplay(Box::new(err))
    }
}

impl StoreRuntimeError {
    pub fn code(&self) -> ErrorCode {
        match self {
            StoreRuntimeError::Lock(lock_err) => lock_err.code(),
            StoreRuntimeError::MetaSymlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
            StoreRuntimeError::MetaRead { source, .. }
            | StoreRuntimeError::MetaWrite { source, .. } => {
                if source.kind() == io::ErrorKind::PermissionDenied {
                    ProtocolErrorCode::PermissionDenied.into()
                } else {
                    ProtocolErrorCode::InternalError.into()
                }
            }
            StoreRuntimeError::MetaParse { .. } => ProtocolErrorCode::Corruption.into(),
            StoreRuntimeError::MetaMismatch { .. } => ProtocolErrorCode::WrongStore.into(),
            StoreRuntimeError::UnsupportedStoreMetaVersion { .. } => {
                ProtocolErrorCode::VersionIncompatible.into()
            }
            StoreRuntimeError::NamespacePoliciesSymlink { .. }
            | StoreRuntimeError::ReplicaRosterSymlink { .. } => {
                ProtocolErrorCode::PathSymlinkRejected.into()
            }
            StoreRuntimeError::NamespacePoliciesRead { source, .. } => {
                if source.kind() == io::ErrorKind::PermissionDenied {
                    ProtocolErrorCode::PermissionDenied.into()
                } else {
                    CliErrorCode::ValidationFailed.into()
                }
            }
            StoreRuntimeError::NamespacePoliciesParse { .. } => {
                CliErrorCode::ValidationFailed.into()
            }
            StoreRuntimeError::ReplicaRosterRead { source, .. } => {
                if source.kind() == io::ErrorKind::PermissionDenied {
                    ProtocolErrorCode::PermissionDenied.into()
                } else {
                    CliErrorCode::ValidationFailed.into()
                }
            }
            StoreRuntimeError::ReplicaRosterParse { .. } => CliErrorCode::ValidationFailed.into(),
            StoreRuntimeError::StoreConfigSymlink { .. } => {
                ProtocolErrorCode::PathSymlinkRejected.into()
            }
            StoreRuntimeError::StoreConfigRead { source, .. } => {
                if source.kind() == io::ErrorKind::PermissionDenied {
                    ProtocolErrorCode::PermissionDenied.into()
                } else {
                    CliErrorCode::ValidationFailed.into()
                }
            }
            StoreRuntimeError::StoreConfigParse { .. } => CliErrorCode::ValidationFailed.into(),
            StoreRuntimeError::StoreConfigSerialize { .. } => {
                ProtocolErrorCode::InternalError.into()
            }
            StoreRuntimeError::StoreConfigWrite { source, .. } => {
                if source.kind() == io::ErrorKind::PermissionDenied {
                    ProtocolErrorCode::PermissionDenied.into()
                } else {
                    ProtocolErrorCode::InternalError.into()
                }
            }
            StoreRuntimeError::WalIndex(err) => err.code(),
            StoreRuntimeError::WalReplay(err) => err.code(),
            StoreRuntimeError::WatermarkInvalid { .. } => ProtocolErrorCode::IndexCorrupt.into(),
        }
    }

    pub fn transience(&self) -> Transience {
        match self {
            StoreRuntimeError::Lock(lock_err) => lock_err.transience(),
            StoreRuntimeError::MetaSymlink { .. } => Transience::Permanent,
            StoreRuntimeError::MetaParse { .. }
            | StoreRuntimeError::MetaMismatch { .. }
            | StoreRuntimeError::UnsupportedStoreMetaVersion { .. } => Transience::Permanent,
            StoreRuntimeError::MetaRead { source, .. }
            | StoreRuntimeError::MetaWrite { source, .. } => {
                if source.kind() == io::ErrorKind::PermissionDenied {
                    Transience::Permanent
                } else {
                    Transience::Retryable
                }
            }
            StoreRuntimeError::NamespacePoliciesSymlink { .. }
            | StoreRuntimeError::ReplicaRosterSymlink { .. } => Transience::Permanent,
            StoreRuntimeError::NamespacePoliciesRead { source, .. } => {
                if source.kind() == io::ErrorKind::PermissionDenied {
                    Transience::Permanent
                } else {
                    Transience::Retryable
                }
            }
            StoreRuntimeError::NamespacePoliciesParse { .. } => Transience::Permanent,
            StoreRuntimeError::ReplicaRosterRead { source, .. } => {
                if source.kind() == io::ErrorKind::PermissionDenied {
                    Transience::Permanent
                } else {
                    Transience::Retryable
                }
            }
            StoreRuntimeError::ReplicaRosterParse { .. } => Transience::Permanent,
            StoreRuntimeError::StoreConfigSymlink { .. }
            | StoreRuntimeError::StoreConfigParse { .. }
            | StoreRuntimeError::StoreConfigSerialize { .. } => Transience::Permanent,
            StoreRuntimeError::StoreConfigRead { source, .. }
            | StoreRuntimeError::StoreConfigWrite { source, .. } => {
                if source.kind() == io::ErrorKind::PermissionDenied {
                    Transience::Permanent
                } else {
                    Transience::Retryable
                }
            }
            StoreRuntimeError::WalIndex(err) => err.transience(),
            StoreRuntimeError::WalReplay(err) => err.transience(),
            StoreRuntimeError::WatermarkInvalid { .. } => Transience::Permanent,
        }
    }
}

impl IntoErrorPayload for StoreRuntimeError {
    fn into_error_payload(self) -> ErrorPayload {
        let message = self.to_string();
        let retryable = self.transience().is_retryable();
        match self {
            StoreRuntimeError::Lock(err) => err.into_error_payload(),
            StoreRuntimeError::MetaSymlink { path } => ErrorPayload::new(
                ProtocolErrorCode::PathSymlinkRejected.into(),
                message,
                retryable,
            )
            .with_details(error_details::PathSymlinkRejectedDetails {
                path: path.display().to_string(),
            }),
            StoreRuntimeError::MetaRead { path, source } => match source.kind() {
                io::ErrorKind::PermissionDenied => ErrorPayload::new(
                    ProtocolErrorCode::PermissionDenied.into(),
                    message,
                    retryable,
                )
                .with_details(error_details::PermissionDeniedDetails {
                    path: path.display().to_string(),
                    operation: error_details::PermissionOperation::Read,
                }),
                _ => ErrorPayload::new(ProtocolErrorCode::InternalError.into(), message, retryable),
            },
            StoreRuntimeError::MetaParse { source, .. } => {
                ErrorPayload::new(ProtocolErrorCode::Corruption.into(), message, retryable)
                    .with_details(error_details::CorruptionDetails {
                        reason: source.to_string(),
                    })
            }
            StoreRuntimeError::MetaMismatch { expected, got } => {
                ErrorPayload::new(ProtocolErrorCode::WrongStore.into(), message, retryable)
                    .with_details(error_details::WrongStoreDetails {
                        expected_store_id: expected,
                        got_store_id: got,
                    })
            }
            StoreRuntimeError::UnsupportedStoreMetaVersion { expected, got } => ErrorPayload::new(
                ProtocolErrorCode::VersionIncompatible.into(),
                message,
                retryable,
            )
            .with_details(error_details::StoreMetaVersionMismatchDetails { expected, got }),
            StoreRuntimeError::MetaWrite { path, source } => match source.kind() {
                io::ErrorKind::PermissionDenied => ErrorPayload::new(
                    ProtocolErrorCode::PermissionDenied.into(),
                    message,
                    retryable,
                )
                .with_details(error_details::PermissionDeniedDetails {
                    path: path.display().to_string(),
                    operation: error_details::PermissionOperation::Write,
                }),
                _ => ErrorPayload::new(ProtocolErrorCode::InternalError.into(), message, retryable),
            },
            StoreRuntimeError::NamespacePoliciesSymlink { path }
            | StoreRuntimeError::ReplicaRosterSymlink { path } => ErrorPayload::new(
                ProtocolErrorCode::PathSymlinkRejected.into(),
                message,
                retryable,
            )
            .with_details(error_details::PathSymlinkRejectedDetails {
                path: path.display().to_string(),
            }),
            StoreRuntimeError::NamespacePoliciesRead { path, source } => match source.kind() {
                io::ErrorKind::PermissionDenied => ErrorPayload::new(
                    ProtocolErrorCode::PermissionDenied.into(),
                    message,
                    retryable,
                )
                .with_details(error_details::PermissionDeniedDetails {
                    path: path.display().to_string(),
                    operation: error_details::PermissionOperation::Read,
                }),
                _ => ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                    .with_details(error_details::ValidationFailedDetails {
                        field: "namespaces".to_string(),
                        reason: format!("failed to read {}: {source}", path.display()),
                    }),
            },
            StoreRuntimeError::NamespacePoliciesParse { source, .. } => {
                ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                    .with_details(error_details::ValidationFailedDetails {
                        field: "namespaces".to_string(),
                        reason: source.to_string(),
                    })
            }
            StoreRuntimeError::ReplicaRosterRead { path, source } => match source.kind() {
                io::ErrorKind::PermissionDenied => ErrorPayload::new(
                    ProtocolErrorCode::PermissionDenied.into(),
                    message,
                    retryable,
                )
                .with_details(error_details::PermissionDeniedDetails {
                    path: path.display().to_string(),
                    operation: error_details::PermissionOperation::Read,
                }),
                _ => ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                    .with_details(error_details::ValidationFailedDetails {
                        field: "replicas".to_string(),
                        reason: format!("failed to read {}: {source}", path.display()),
                    }),
            },
            StoreRuntimeError::ReplicaRosterParse { source, .. } => {
                ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                    .with_details(error_details::ValidationFailedDetails {
                        field: "replicas".to_string(),
                        reason: source.to_string(),
                    })
            }
            StoreRuntimeError::StoreConfigSymlink { path } => ErrorPayload::new(
                ProtocolErrorCode::PathSymlinkRejected.into(),
                message,
                retryable,
            )
            .with_details(error_details::PathSymlinkRejectedDetails {
                path: path.display().to_string(),
            }),
            StoreRuntimeError::StoreConfigRead { path, source } => match source.kind() {
                io::ErrorKind::PermissionDenied => ErrorPayload::new(
                    ProtocolErrorCode::PermissionDenied.into(),
                    message,
                    retryable,
                )
                .with_details(error_details::PermissionDeniedDetails {
                    path: path.display().to_string(),
                    operation: error_details::PermissionOperation::Read,
                }),
                _ => ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                    .with_details(error_details::ValidationFailedDetails {
                        field: "store_config".to_string(),
                        reason: format!("failed to read {}: {source}", path.display()),
                    }),
            },
            StoreRuntimeError::StoreConfigParse { source, .. } => {
                ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                    .with_details(error_details::ValidationFailedDetails {
                        field: "store_config".to_string(),
                        reason: source.to_string(),
                    })
            }
            StoreRuntimeError::StoreConfigSerialize { .. } => {
                ErrorPayload::new(ProtocolErrorCode::InternalError.into(), message, retryable)
            }
            StoreRuntimeError::StoreConfigWrite { path, source } => match source.kind() {
                io::ErrorKind::PermissionDenied => ErrorPayload::new(
                    ProtocolErrorCode::PermissionDenied.into(),
                    message,
                    retryable,
                )
                .with_details(error_details::PermissionDeniedDetails {
                    path: path.display().to_string(),
                    operation: error_details::PermissionOperation::Write,
                }),
                _ => ErrorPayload::new(ProtocolErrorCode::InternalError.into(), message, retryable),
            },
            StoreRuntimeError::WatermarkInvalid {
                kind,
                namespace,
                origin,
                source,
            } => ErrorPayload::new(ProtocolErrorCode::IndexCorrupt.into(), message, retryable)
                .with_details(error_details::IndexCorruptDetails {
                    reason: format!("{kind} watermark for {namespace} {origin}: {source}"),
                }),
            StoreRuntimeError::WalIndex(err) => err.into_payload_with_context(message, retryable),
            StoreRuntimeError::WalReplay(err) => (*err).into_error_payload(),
        }
    }
}

fn open_wal_index_with_layout(
    layout: &DaemonLayout,
    store_id: StoreId,
    store_dir: &Path,
    meta: &StoreMeta,
    mode: IndexDurabilityMode,
) -> Result<(SqliteWalIndex, bool), StoreRuntimeError> {
    let db_path = layout.wal_index_path(&store_id);
    let mut needs_rebuild = !db_path.exists();

    match SqliteWalIndex::open(store_dir, meta, mode) {
        Ok(index) => Ok((index, needs_rebuild)),
        Err(WalIndexError::SchemaVersionMismatch { .. }) => {
            needs_rebuild = true;
            remove_wal_index_files_with_layout(layout, store_id)?;
            let index = SqliteWalIndex::open(store_dir, meta, mode)?;
            Ok((index, needs_rebuild))
        }
        Err(err) => Err(StoreRuntimeError::WalIndex(err)),
    }
}

fn can_upgrade_index_schema_only(got: StoreMetaVersions, expected: StoreMetaVersions) -> bool {
    got.store_format_version == expected.store_format_version
        && got.wal_format_version == expected.wal_format_version
        && got.checkpoint_format_version == expected.checkpoint_format_version
        && got.replication_protocol_version == expected.replication_protocol_version
        && got.index_schema_version < expected.index_schema_version
}

fn load_store_config_with_layout(
    layout: &DaemonLayout,
    store_id: StoreId,
    write_default: bool,
) -> Result<StoreConfig, StoreRuntimeError> {
    let path = layout.store_config_path(&store_id);
    match read_secure_store_file(&path) {
        Ok(Some(raw)) => {
            toml::from_str(&raw).map_err(|source| StoreRuntimeError::StoreConfigParse {
                path: Box::new(path),
                source,
            })
        }
        Ok(None) => {
            let config = StoreConfig::default();
            if write_default {
                write_store_config(&path, &config)?;
            }
            Ok(config)
        }
        Err(StoreConfigFileError::Symlink { path }) => Err(StoreRuntimeError::StoreConfigSymlink {
            path: Box::new(path),
        }),
        Err(StoreConfigFileError::Read { path, source }) => {
            Err(StoreRuntimeError::StoreConfigRead {
                path: Box::new(path),
                source,
            })
        }
    }
}

fn write_store_config(path: &Path, config: &StoreConfig) -> Result<(), StoreRuntimeError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| StoreRuntimeError::StoreConfigWrite {
            path: Box::new(path.to_path_buf()),
            source,
        })?;
    }
    let contents = toml::to_string_pretty(config).map_err(|source| {
        StoreRuntimeError::StoreConfigSerialize {
            path: Box::new(path.to_path_buf()),
            source,
        }
    })?;
    fs::write(path, contents).map_err(|source| StoreRuntimeError::StoreConfigWrite {
        path: Box::new(path.to_path_buf()),
        source,
    })?;
    ensure_secure_file_permissions(path).map_err(|source| StoreRuntimeError::StoreConfigWrite {
        path: Box::new(path.to_path_buf()),
        source,
    })?;
    Ok(())
}

fn remove_wal_index_files_with_layout(
    layout: &DaemonLayout,
    store_id: StoreId,
) -> Result<(), StoreRuntimeError> {
    let index_dir = layout.store_dir(&store_id).join("index");
    match fs::symlink_metadata(&index_dir) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(StoreRuntimeError::WalIndex(WalIndexError::Symlink {
                path: index_dir,
            }));
        }
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(StoreRuntimeError::WalIndex(WalIndexError::Io {
                path: Some(index_dir),
                reason: err.to_string(),
            }));
        }
    }

    let db_path = layout.wal_index_path(&store_id);
    for suffix in ["", "-wal", "-shm"] {
        let path = if suffix.is_empty() {
            db_path.clone()
        } else {
            PathBuf::from(format!("{}{}", db_path.display(), suffix))
        };
        match fs::symlink_metadata(&path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(StoreRuntimeError::WalIndex(WalIndexError::Symlink {
                    path: path.clone(),
                }));
            }
            Ok(_) => {
                fs::remove_file(&path).map_err(|source| {
                    StoreRuntimeError::WalIndex(WalIndexError::Io {
                        path: Some(path.clone()),
                        reason: source.to_string(),
                    })
                })?;
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(StoreRuntimeError::WalIndex(WalIndexError::Io {
                    path: Some(path.clone()),
                    reason: err.to_string(),
                }));
            }
        }
    }
    Ok(())
}

fn load_namespace_policies_with_layout(
    layout: &DaemonLayout,
    store_id: StoreId,
    defaults: &BTreeMap<NamespaceId, NamespacePolicy>,
) -> Result<BTreeMap<NamespaceId, NamespacePolicy>, StoreRuntimeError> {
    let path = layout.namespaces_path(&store_id);
    let raw = match read_secure_store_file(&path) {
        Ok(Some(raw)) => raw,
        Ok(None) => return Ok(defaults.clone()),
        Err(StoreConfigFileError::Symlink { path }) => {
            return Err(StoreRuntimeError::NamespacePoliciesSymlink {
                path: Box::new(path),
            });
        }
        Err(StoreConfigFileError::Read { path, source }) => {
            return Err(StoreRuntimeError::NamespacePoliciesRead {
                path: Box::new(path),
                source,
            });
        }
    };

    let policies = NamespacePolicies::from_toml_str(&raw).map_err(|source| {
        StoreRuntimeError::NamespacePoliciesParse {
            path: Box::new(path),
            source,
        }
    })?;

    Ok(policies.namespaces)
}

#[cfg(test)]
pub(crate) fn load_namespace_policies(
    store_id: StoreId,
    defaults: &BTreeMap<NamespaceId, NamespacePolicy>,
) -> Result<BTreeMap<NamespaceId, NamespacePolicy>, StoreRuntimeError> {
    let path = crate::daemon_layout_from_paths().namespaces_path(&store_id);
    let raw = match read_secure_store_file(&path) {
        Ok(Some(raw)) => raw,
        Ok(None) => return Ok(defaults.clone()),
        Err(StoreConfigFileError::Symlink { path }) => {
            return Err(StoreRuntimeError::NamespacePoliciesSymlink {
                path: Box::new(path),
            });
        }
        Err(StoreConfigFileError::Read { path, source }) => {
            return Err(StoreRuntimeError::NamespacePoliciesRead {
                path: Box::new(path),
                source,
            });
        }
    };

    let policies = NamespacePolicies::from_toml_str(&raw).map_err(|source| {
        StoreRuntimeError::NamespacePoliciesParse {
            path: Box::new(path),
            source,
        }
    })?;

    Ok(policies.namespaces)
}

pub(crate) fn load_replica_roster(
    layout: &DaemonLayout,
    store_id: StoreId,
) -> Result<Option<ReplicaRoster>, StoreRuntimeError> {
    let path = layout.replicas_path(&store_id);
    let raw = match read_secure_store_file(&path) {
        Ok(Some(raw)) => raw,
        Ok(None) => return Ok(None),
        Err(StoreConfigFileError::Symlink { path }) => {
            return Err(StoreRuntimeError::ReplicaRosterSymlink {
                path: Box::new(path),
            });
        }
        Err(StoreConfigFileError::Read { path, source }) => {
            return Err(StoreRuntimeError::ReplicaRosterRead {
                path: Box::new(path),
                source,
            });
        }
    };

    ReplicaRoster::from_toml_str(&raw)
        .map(Some)
        .map_err(|source| StoreRuntimeError::ReplicaRosterParse {
            path: Box::new(path),
            source,
        })
}

fn load_watermarks(
    index: &dyn WalIndex,
) -> Result<(Watermarks<Applied>, Watermarks<Durable>), StoreRuntimeError> {
    let rows = index.reader().load_watermarks()?;
    let mut applied = Watermarks::<Applied>::new();
    let mut durable = Watermarks::<Durable>::new();

    for row in rows {
        let namespace = row.namespace.clone();
        let origin = row.origin;
        let applied_row = row.applied();
        let durable_row = row.durable();

        applied
            .observe_at_least(&namespace, &origin, applied_row.seq(), applied_row.head())
            .map_err(|source| StoreRuntimeError::WatermarkInvalid {
                kind: "applied",
                namespace: namespace.clone(),
                origin,
                source: Box::new(source),
            })?;

        durable
            .observe_at_least(&namespace, &origin, durable_row.seq(), durable_row.head())
            .map_err(|source| StoreRuntimeError::WatermarkInvalid {
                kind: "durable",
                namespace: namespace.clone(),
                origin,
                source: Box::new(source),
            })?;
    }

    Ok((applied, durable))
}

fn head_status_to_sha(head: Option<HeadStatus>) -> Option<[u8; 32]> {
    match head {
        Some(HeadStatus::Known(sha)) => Some(sha),
        _ => None,
    }
}

fn read_store_json_optional<T: DeserializeOwned>(
    path: &Path,
) -> Result<Option<T>, StoreRuntimeError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(StoreRuntimeError::MetaSymlink {
                path: Box::new(path.to_path_buf()),
            });
        }
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(StoreRuntimeError::MetaRead {
                path: Box::new(path.to_path_buf()),
                source: err,
            });
        }
    }

    let bytes = fs::read(path).map_err(|source| StoreRuntimeError::MetaRead {
        path: Box::new(path.to_path_buf()),
        source,
    })?;
    let value = serde_json::from_slice(&bytes).map_err(|source| StoreRuntimeError::MetaParse {
        path: Box::new(path.to_path_buf()),
        source,
    })?;
    ensure_file_permissions(path)?;
    Ok(Some(value))
}

fn read_store_meta_optional(path: &Path) -> Result<Option<StoreMeta>, StoreRuntimeError> {
    read_store_json_optional(path)
}

fn read_pending_store_meta_transition_optional(
    path: &Path,
) -> Result<Option<PendingStoreMetaTransitionRecord>, StoreRuntimeError> {
    read_store_json_optional(path)
}

fn write_store_json(path: &Path, value: &impl Serialize) -> Result<(), StoreRuntimeError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| StoreRuntimeError::MetaWrite {
            path: Box::new(path.to_path_buf()),
            source,
        })?;
    }

    let bytes = serde_json::to_vec(value).map_err(|source| StoreRuntimeError::MetaParse {
        path: Box::new(path.to_path_buf()),
        source,
    })?;

    let dir = path.parent().ok_or_else(|| StoreRuntimeError::MetaWrite {
        path: Box::new(path.to_path_buf()),
        source: io::Error::other("store meta path missing parent"),
    })?;
    let mut temp =
        tempfile::NamedTempFile::new_in(dir).map_err(|source| StoreRuntimeError::MetaWrite {
            path: Box::new(path.to_path_buf()),
            source,
        })?;
    temp.write_all(&bytes)
        .map_err(|source| StoreRuntimeError::MetaWrite {
            path: Box::new(path.to_path_buf()),
            source,
        })?;
    temp.as_file()
        .sync_all()
        .map_err(|source| StoreRuntimeError::MetaWrite {
            path: Box::new(path.to_path_buf()),
            source,
        })?;
    temp.persist(path)
        .map_err(|err| StoreRuntimeError::MetaWrite {
            path: Box::new(path.to_path_buf()),
            source: err.error,
        })?;
    ensure_file_permissions(path)?;
    sync_parent_dir(path);
    Ok(())
}

fn write_store_meta(path: &Path, meta: &StoreMeta) -> Result<(), StoreRuntimeError> {
    write_store_json(path, meta)
}

fn write_pending_store_meta_transition(
    path: &Path,
    transition: &PendingStoreMetaTransitionRecord,
) -> Result<(), StoreRuntimeError> {
    write_store_json(path, transition)
}

fn remove_pending_store_meta_transition(path: &Path) -> Result<(), StoreRuntimeError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(StoreRuntimeError::MetaSymlink {
                path: Box::new(path.to_path_buf()),
            });
        }
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(StoreRuntimeError::MetaRead {
                path: Box::new(path.to_path_buf()),
                source: err,
            });
        }
    }
    fs::remove_file(path).map_err(|source| StoreRuntimeError::MetaWrite {
        path: Box::new(path.to_path_buf()),
        source,
    })?;
    sync_parent_dir(path);
    Ok(())
}

fn sync_parent_dir(path: &Path) {
    #[cfg(unix)]
    {
        let Some(dir) = path.parent() else {
            return;
        };
        if let Ok(dir) = fs::File::open(dir) {
            let _ = dir.sync_all();
        }
    }
}

#[cfg(test)]
type MetaWriteHookFn = Box<dyn FnOnce(&Path, &StoreMeta) -> Result<(), StoreRuntimeError> + Send>;

#[cfg(test)]
struct MetaWriteHook {
    path: PathBuf,
    hook: MetaWriteHookFn,
}

#[cfg(test)]
static META_WRITE_HOOK: LazyLock<Mutex<Option<MetaWriteHook>>> = LazyLock::new(|| Mutex::new(None));

#[cfg(test)]
static META_WRITE_HOOK_INSTALL_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[cfg(test)]
struct MetaWriteHookGuard {
    _install_guard: MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for MetaWriteHookGuard {
    fn drop(&mut self) {
        *lock_meta_write_hook() = None;
    }
}

#[cfg(test)]
fn scoped_meta_write_hook(
    path: PathBuf,
    hook: impl FnOnce(&Path, &StoreMeta) -> Result<(), StoreRuntimeError> + Send + 'static,
) -> MetaWriteHookGuard {
    let install_guard = lock_meta_write_hook_install();
    let mut slot = lock_meta_write_hook();
    assert!(slot.is_none(), "meta write hook already installed");
    *slot = Some(MetaWriteHook {
        path,
        hook: Box::new(hook),
    });
    drop(slot);
    MetaWriteHookGuard {
        _install_guard: install_guard,
    }
}

#[cfg(test)]
fn lock_meta_write_hook() -> MutexGuard<'static, Option<MetaWriteHook>> {
    META_WRITE_HOOK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

#[cfg(test)]
fn lock_meta_write_hook_install() -> MutexGuard<'static, ()> {
    META_WRITE_HOOK_INSTALL_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

fn write_store_meta_for_open(path: &Path, meta: &StoreMeta) -> Result<(), StoreRuntimeError> {
    #[cfg(test)]
    {
        let maybe_hook = {
            let mut slot = lock_meta_write_hook();
            match slot.as_ref() {
                Some(installed) if installed.path == path => slot.take(),
                _ => None,
            }
        };
        if let Some(hook) = maybe_hook {
            return (hook.hook)(path, meta);
        }
    }
    write_store_meta(path, meta)
}

#[cfg(test)]
#[derive(Clone, Debug)]
struct StoreMetaTransitionCrashFailpoint {
    stage: String,
    owner: std::thread::ThreadId,
}

#[cfg(test)]
fn store_meta_transition_crash_slot()
-> &'static std::sync::Mutex<Option<StoreMetaTransitionCrashFailpoint>> {
    static SLOT: std::sync::OnceLock<std::sync::Mutex<Option<StoreMetaTransitionCrashFailpoint>>> =
        std::sync::OnceLock::new();
    SLOT.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn store_meta_transition_crash_install_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

#[cfg(test)]
fn maybe_panic_store_meta_transition(stage: &str) {
    let slot = store_meta_transition_crash_slot();
    let active = slot.lock().unwrap_or_else(|poison| poison.into_inner());
    if let Some(failpoint) = active.as_ref()
        && failpoint.stage == stage
        && failpoint.owner == std::thread::current().id()
    {
        panic!("injected store meta transition crash at {stage}");
    }
}

#[cfg(test)]
struct StoreMetaTransitionCrashGuard {
    _install_guard: MutexGuard<'static, ()>,
    previous: Option<StoreMetaTransitionCrashFailpoint>,
}

#[cfg(test)]
impl Drop for StoreMetaTransitionCrashGuard {
    fn drop(&mut self) {
        let slot = store_meta_transition_crash_slot();
        *slot.lock().unwrap_or_else(|poison| poison.into_inner()) = self.previous.take();
    }
}

#[cfg(test)]
fn set_store_meta_transition_crash_stage_for_tests(stage: &str) -> StoreMetaTransitionCrashGuard {
    let install_guard = store_meta_transition_crash_install_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let slot = store_meta_transition_crash_slot();
    let mut active = slot.lock().unwrap_or_else(|poison| poison.into_inner());
    let previous = active.replace(StoreMetaTransitionCrashFailpoint {
        stage: stage.to_string(),
        owner: std::thread::current().id(),
    });
    drop(active);
    StoreMetaTransitionCrashGuard {
        _install_guard: install_guard,
        previous,
    }
}

#[derive(Debug, Error)]
pub(crate) enum StoreConfigFileError {
    #[error("config path is a symlink: {path:?}")]
    Symlink { path: PathBuf },
    #[error("config read failed at {path:?}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

pub(crate) fn read_secure_store_file(path: &Path) -> Result<Option<String>, StoreConfigFileError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(StoreConfigFileError::Symlink {
                path: path.to_path_buf(),
            });
        }
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(StoreConfigFileError::Read {
                path: path.to_path_buf(),
                source: err,
            });
        }
    }

    let raw = fs::read_to_string(path).map_err(|source| StoreConfigFileError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    ensure_secure_file_permissions(path).map_err(|source| StoreConfigFileError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(Some(raw))
}

fn ensure_secure_file_permissions(path: &Path) -> Result<(), io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn ensure_file_permissions(path: &Path) -> Result<(), StoreRuntimeError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
            StoreRuntimeError::MetaWrite {
                path: Box::new(path.to_path_buf()),
                source,
            }
        })?;
    }
    Ok(())
}

fn new_replica_id() -> ReplicaId {
    let mut rng = rand::rng();
    let mut bytes = [0u8; 16];
    rng.fill_bytes(&mut bytes);
    ReplicaId::new(Uuid::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    use crate::core::bead::{BeadCore, BeadFields};
    use crate::core::composite::{Claim, Workflow};
    use crate::core::crdt::Lww;
    use crate::core::domain::{BeadType, DepKind, Priority};
    use crate::core::identity::BeadId;
    use crate::core::time::{Stamp, WriteStamp};
    use crate::core::{
        ActorId, CanonicalState, DepKey, Dot, ReplicaId, Seq0, StoreState, Watermark, WatermarkPair,
    };
    use crate::paths;
    use crate::remote::RemoteUrl;
    use crate::runtime::store::lock::{StoreLockMeta, read_lock_meta};
    use crate::runtime::wal::{IndexDurabilityMode, SqliteWalIndex, WalIndex};
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};
    use uuid::Uuid;

    fn write_meta_for(store_id: StoreId, replica_id: ReplicaId, now_ms: u64) -> StoreMeta {
        let identity = StoreIdentity::new(store_id, StoreEpoch::ZERO);
        let versions = StoreMetaVersions::current();
        let meta = StoreMeta::new(identity, replica_id, versions, now_ms);
        write_store_meta(&paths::store_meta_path(store_id), &meta).expect("write meta");
        meta
    }

    fn write_meta_with_versions_for(
        store_id: StoreId,
        replica_id: ReplicaId,
        now_ms: u64,
        versions: StoreMetaVersions,
    ) -> StoreMeta {
        let identity = StoreIdentity::new(store_id, StoreEpoch::ZERO);
        let meta = StoreMeta::new(identity, replica_id, versions, now_ms);
        write_store_meta(&paths::store_meta_path(store_id), &meta).expect("write meta");
        meta
    }

    fn write_legacy_index_with_sentinel(store_id: StoreId, meta: &StoreMeta) {
        let db_path = paths::store_dir(store_id).join("index").join("wal.sqlite");
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create index dir");
        }
        let conn = rusqlite::Connection::open(&db_path).expect("open legacy sqlite");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta (
               key TEXT PRIMARY KEY,
               value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS legacy_sentinel (
               id INTEGER PRIMARY KEY
             );",
        )
        .expect("create legacy schema");
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?1, ?2)",
            rusqlite::params!["store_id", meta.store_id().to_string()],
        )
        .expect("insert store_id");
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?1, ?2)",
            rusqlite::params!["store_epoch", meta.store_epoch().get().to_string()],
        )
        .expect("insert store_epoch");
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?1, ?2)",
            rusqlite::params![
                "index_schema_version",
                meta.index_schema_version.to_string()
            ],
        )
        .expect("insert index_schema_version");
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?1, ?2)",
            rusqlite::params!["wal_format_version", meta.wal_format_version.to_string()],
        )
        .expect("insert wal_format_version");
        conn.execute("INSERT INTO legacy_sentinel (id) VALUES (1)", [])
            .expect("insert sentinel");
    }

    fn write_stale_lock_meta(
        store_id: StoreId,
        replica_id: ReplicaId,
        started_at_ms: u64,
        daemon_version: &str,
    ) {
        let lock_path = paths::store_lock_path(store_id);
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).expect("create lock dir");
        }
        let lock_meta = StoreLockMeta {
            store_id,
            replica_id,
            pid: i32::MAX as u32,
            started_at_ms,
            daemon_version: daemon_version.to_string(),
            lease_epoch: 1,
            lease_token: Some(Uuid::from_bytes([201u8; 16])),
            last_heartbeat_ms: Some(started_at_ms),
        };
        let data = serde_json::to_vec(&lock_meta).expect("serialize stale lock meta");
        fs::write(&lock_path, data).expect("write stale lock meta");
        #[cfg(unix)]
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600))
            .expect("secure stale lock permissions");
    }

    #[test]
    fn phase3_head_sha_loads_from_index() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([10u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([11u8; 16]));
        let now_ms = 1_700_000_000_000;
        let meta = write_meta_for(store_id, replica_id, now_ms);
        let index = SqliteWalIndex::open(
            &paths::store_dir(store_id),
            &meta,
            IndexDurabilityMode::Cache,
        )
        .expect("open wal index");

        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([12u8; 16]));
        let head = [7u8; 32];
        let mut txn = index.writer().begin_txn().expect("begin txn");
        let applied =
            Watermark::<Applied>::new(Seq0::new(2), HeadStatus::Known(head)).expect("watermark");
        let durable =
            Watermark::<Durable>::new(Seq0::new(2), HeadStatus::Known(head)).expect("watermark");
        let watermarks = WatermarkPair::new(applied, durable).expect("watermark pair");
        txn.update_watermark(&namespace, &origin, watermarks)
            .expect("update watermark");
        txn.commit().expect("commit watermark");

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let runtime = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            now_ms + 1,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("open runtime")
        .runtime;

        let applied = runtime
            .watermarks_applied
            .get(&namespace, &origin)
            .copied()
            .expect("applied watermark");
        assert_eq!(applied.seq().get(), 2);
        assert!(matches!(applied.head(), HeadStatus::Known(sha) if sha == head));

        let durable = runtime
            .watermarks_durable
            .get(&namespace, &origin)
            .copied()
            .expect("durable watermark");
        assert_eq!(durable.seq().get(), 2);
        assert!(matches!(durable.head(), HeadStatus::Known(sha) if sha == head));
        assert_eq!(runtime.durable_head_sha(&namespace, &origin), Some(head));
        assert_eq!(runtime.applied_head_sha(&namespace, &origin), Some(head));
    }

    #[test]
    fn runtime_upgrades_stale_index_meta_and_rebuilds_legacy_index() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([20u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([21u8; 16]));
        let now_ms = 1_700_000_000_000;
        let mut stale_versions = StoreMetaVersions::current();
        stale_versions.index_schema_version = stale_versions.index_schema_version.saturating_sub(1);
        let stale_meta = write_meta_with_versions_for(store_id, replica_id, now_ms, stale_versions);
        write_legacy_index_with_sentinel(store_id, &stale_meta);

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let _runtime = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            now_ms + 1,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("open runtime with stale index schema")
        .runtime;

        let persisted_meta = read_store_meta_optional(&paths::store_meta_path(store_id))
            .expect("read store meta")
            .expect("store meta exists");
        assert_eq!(
            persisted_meta.index_schema_version,
            StoreMetaVersions::INDEX_SCHEMA_VERSION
        );

        let db_path = paths::store_dir(store_id).join("index").join("wal.sqlite");
        let conn = rusqlite::Connection::open(&db_path).expect("open rebuilt wal sqlite");
        let sentinel_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                rusqlite::params!["legacy_sentinel"],
                |row| row.get(0),
            )
            .expect("query sqlite_master");
        assert_eq!(sentinel_count, 0, "legacy index should be replaced");

        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([22u8; 16]));
        let insert_err = conn
            .execute(
                "INSERT INTO watermarks (namespace, origin_replica_id, applied_seq, durable_seq, applied_head_sha, durable_head_sha) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    namespace.as_str(),
                    origin.as_uuid().as_bytes().to_vec(),
                    1i64,
                    0i64,
                    Option::<Vec<u8>>::None,
                    Option::<Vec<u8>>::None,
                ],
            )
            .expect_err("invalid watermark insert should fail");
        assert!(
            insert_err.to_string().contains("CHECK constraint failed"),
            "expected CHECK constraint failure, got {insert_err}"
        );
    }

    #[test]
    fn stale_index_meta_upgrade_failure_rolls_back_index() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([23u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([24u8; 16]));
        let now_ms = 1_700_000_000_000;
        let mut stale_versions = StoreMetaVersions::current();
        stale_versions.index_schema_version = stale_versions.index_schema_version.saturating_sub(1);
        let stale_meta = write_meta_with_versions_for(store_id, replica_id, now_ms, stale_versions);
        write_legacy_index_with_sentinel(store_id, &stale_meta);

        let meta_path = paths::store_meta_path(store_id);
        let _hook = scoped_meta_write_hook(meta_path.clone(), move |path, _meta| {
            assert_eq!(path, meta_path);
            Err(StoreRuntimeError::MetaWrite {
                path: Box::new(path.to_path_buf()),
                source: io::Error::other("injected stale meta upgrade failure"),
            })
        });

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let err = match StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            now_ms + 1,
            "test",
            &Limits::default(),
            &namespace_defaults,
        ) {
            Ok(_) => panic!("stale meta upgrade should fail when meta persistence fails"),
            Err(err) => err,
        };

        assert!(matches!(err, StoreRuntimeError::MetaWrite { .. }));

        let persisted_meta = read_store_meta_optional(&paths::store_meta_path(store_id))
            .expect("read store meta after failed upgrade")
            .expect("store meta exists");
        assert_eq!(
            persisted_meta.index_schema_version, stale_versions.index_schema_version,
            "stale meta must remain unchanged when upgrade persistence fails"
        );
        assert!(
            !paths::wal_index_path(store_id).exists(),
            "wal index must roll back when stale meta upgrade persistence fails"
        );
        assert!(
            read_pending_store_meta_transition_optional(&paths::store_meta_pending_path(store_id))
                .expect("read pending meta after failed upgrade")
                .is_none(),
            "pending upgrade marker must be cleaned up on synchronous upgrade failure"
        );
    }

    #[test]
    fn stale_pending_marker_from_older_index_schema_upgrades_on_open() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([45u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([46u8; 16]));
        let now_ms = 1_700_000_000_000;
        let mut stale_versions = StoreMetaVersions::current();
        stale_versions.index_schema_version = stale_versions.index_schema_version.saturating_sub(1);
        let stale_meta = write_meta_with_versions_for(store_id, replica_id, now_ms, stale_versions);
        write_pending_store_meta_transition(
            &paths::store_meta_pending_path(store_id),
            &PendingStoreMetaTransitionRecord {
                meta: stale_meta.clone(),
            },
        )
        .expect("write stale pending marker");
        write_legacy_index_with_sentinel(store_id, &stale_meta);

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let open = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            now_ms + 1,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("open runtime with stale pending marker")
        .runtime;

        assert_eq!(
            open.meta.index_schema_version,
            StoreMetaVersions::INDEX_SCHEMA_VERSION
        );
        assert!(
            read_pending_store_meta_transition_optional(&paths::store_meta_pending_path(store_id))
                .expect("read pending meta after recovery")
                .is_none(),
            "recovery must clear stale pending markers after upgrading committed meta"
        );
    }

    #[test]
    fn bootstrap_crash_before_meta_commit_recovers_on_next_open() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([38u8; 16]));
        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let failpoint = set_store_meta_transition_crash_stage_for_tests(
            "store_meta_transition_before_meta_commit",
        );
        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = StoreRuntime::open(
                &crate::daemon_layout_from_paths(),
                store_id,
                RemoteUrl::new("example.com/test/repo"),
                1_700_000_000_000,
                "test",
                &Limits::default(),
                &namespace_defaults,
            );
        }));
        drop(failpoint);

        assert!(crashed.is_err(), "crash failpoint should unwind open");
        assert!(
            read_store_meta_optional(&paths::store_meta_path(store_id))
                .expect("read committed meta after injected crash")
                .is_none(),
            "meta must remain absent before commit"
        );
        assert!(
            read_pending_store_meta_transition_optional(&paths::store_meta_pending_path(store_id))
                .expect("read pending meta after injected crash")
                .is_some(),
            "pending marker must survive the injected crash"
        );
        assert!(
            paths::wal_index_path(store_id).exists(),
            "bootstrap crash before meta commit should leave durable index state to recover"
        );

        let open = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            1_700_000_000_001,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("reopen runtime after injected crash");

        assert!(
            read_pending_store_meta_transition_optional(&paths::store_meta_pending_path(store_id))
                .expect("read pending meta after recovery")
                .is_none(),
            "recovery must clear the pending marker"
        );
        let persisted_meta = read_store_meta_optional(&paths::store_meta_path(store_id))
            .expect("read committed meta after recovery")
            .expect("meta exists after recovery");
        assert_eq!(persisted_meta, open.runtime.meta);
    }

    #[test]
    fn bootstrap_crash_before_meta_commit_reclaims_stale_lock_on_recovery() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([44u8; 16]));
        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let failpoint = set_store_meta_transition_crash_stage_for_tests(
            "store_meta_transition_before_meta_commit",
        );
        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = StoreRuntime::open(
                &crate::daemon_layout_from_paths(),
                store_id,
                RemoteUrl::new("example.com/test/repo"),
                1_700_000_000_000,
                "test",
                &Limits::default(),
                &namespace_defaults,
            );
        }));
        drop(failpoint);

        assert!(crashed.is_err(), "crash failpoint should unwind open");
        let pending =
            read_pending_store_meta_transition_optional(&paths::store_meta_pending_path(store_id))
                .expect("read pending meta after injected crash")
                .expect("pending marker survives injected crash");
        assert!(
            read_lock_meta(store_id)
                .expect("read lock after injected crash")
                .is_none(),
            "catch_unwind drops the in-process lock; seed a stale on-disk lock to model real process death"
        );
        write_stale_lock_meta(
            store_id,
            pending.meta.replica_id,
            1_700_000_000_000,
            "crashed-daemon",
        );

        let open = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            1_700_000_000_001,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("reopen runtime after injected crash with stale lock");

        let lock_meta = read_lock_meta(store_id)
            .expect("read lock after recovery")
            .expect("lock exists after recovery");
        assert_eq!(lock_meta.replica_id, open.runtime.meta.replica_id);
        assert_eq!(lock_meta.pid, std::process::id());
        assert!(
            read_pending_store_meta_transition_optional(&paths::store_meta_pending_path(store_id))
                .expect("read pending meta after recovery")
                .is_none(),
            "recovery must clear the pending marker after reclaiming a stale lock"
        );
    }

    #[test]
    fn bootstrap_crash_after_meta_commit_recovers_on_next_open() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([39u8; 16]));
        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let failpoint = set_store_meta_transition_crash_stage_for_tests(
            "store_meta_transition_after_meta_commit",
        );
        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = StoreRuntime::open(
                &crate::daemon_layout_from_paths(),
                store_id,
                RemoteUrl::new("example.com/test/repo"),
                1_700_000_000_000,
                "test",
                &Limits::default(),
                &namespace_defaults,
            );
        }));
        drop(failpoint);

        assert!(crashed.is_err(), "crash failpoint should unwind open");
        let persisted_before_recovery = read_store_meta_optional(&paths::store_meta_path(store_id))
            .expect("read committed meta after injected crash")
            .expect("meta must already be durable after commit");
        assert!(
            read_pending_store_meta_transition_optional(&paths::store_meta_pending_path(store_id))
                .expect("read pending meta after injected crash")
                .is_some(),
            "pending marker must survive the injected crash"
        );

        let open = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            1_700_000_000_001,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("reopen runtime after injected crash");

        assert!(
            read_pending_store_meta_transition_optional(&paths::store_meta_pending_path(store_id))
                .expect("read pending meta after recovery")
                .is_none(),
            "recovery must clear the pending marker after committed meta survives"
        );
        assert_eq!(persisted_before_recovery, open.runtime.meta);
    }

    #[test]
    fn stale_index_meta_upgrade_crash_before_meta_commit_recovers_on_next_open() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([40u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([41u8; 16]));
        let now_ms = 1_700_000_000_000;
        let mut stale_versions = StoreMetaVersions::current();
        stale_versions.index_schema_version = stale_versions.index_schema_version.saturating_sub(1);
        let stale_meta = write_meta_with_versions_for(store_id, replica_id, now_ms, stale_versions);
        write_legacy_index_with_sentinel(store_id, &stale_meta);

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let failpoint = set_store_meta_transition_crash_stage_for_tests(
            "store_meta_transition_before_meta_commit",
        );
        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = StoreRuntime::open(
                &crate::daemon_layout_from_paths(),
                store_id,
                RemoteUrl::new("example.com/test/repo"),
                now_ms + 1,
                "test",
                &Limits::default(),
                &namespace_defaults,
            );
        }));
        drop(failpoint);

        assert!(crashed.is_err(), "crash failpoint should unwind open");
        let persisted_meta = read_store_meta_optional(&paths::store_meta_path(store_id))
            .expect("read stale meta after injected crash")
            .expect("stale meta exists");
        assert_eq!(
            persisted_meta.index_schema_version, stale_versions.index_schema_version,
            "stale committed meta must remain visible before recovery"
        );
        assert!(
            read_pending_store_meta_transition_optional(&paths::store_meta_pending_path(store_id))
                .expect("read pending meta after injected crash")
                .is_some(),
            "pending upgrade marker must survive the injected crash"
        );

        let _open = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            now_ms + 2,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("reopen runtime after injected crash");

        let recovered_meta = read_store_meta_optional(&paths::store_meta_path(store_id))
            .expect("read upgraded meta after recovery")
            .expect("upgraded meta exists");
        assert_eq!(
            recovered_meta.index_schema_version,
            StoreMetaVersions::INDEX_SCHEMA_VERSION
        );
        assert!(
            read_pending_store_meta_transition_optional(&paths::store_meta_pending_path(store_id))
                .expect("read pending meta after recovery")
                .is_none(),
            "recovery must clear the pending upgrade marker"
        );
    }

    #[test]
    fn store_runtime_rejects_version_mismatch() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([30u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([31u8; 16]));
        let now_ms = 1_700_000_000_000;

        let identity = StoreIdentity::new(store_id, StoreEpoch::ZERO);
        let mut versions = StoreMetaVersions::current();
        versions.wal_format_version = versions.wal_format_version.saturating_add(1);
        let meta = StoreMeta::new(identity, replica_id, versions, now_ms);
        write_store_meta(&paths::store_meta_path(store_id), &meta).expect("write meta");

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let err = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            now_ms + 1,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .err()
        .expect("expected version mismatch");

        let expected = StoreMetaVersions::current();
        assert!(matches!(
            err,
            StoreRuntimeError::UnsupportedStoreMetaVersion { expected: got_expected, got }
                if got_expected == expected && got == versions
        ));
    }

    #[test]
    fn store_runtime_rejects_newer_index_schema_version() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([33u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([34u8; 16]));
        let now_ms = 1_700_000_000_000;

        let mut versions = StoreMetaVersions::current();
        versions.index_schema_version = versions.index_schema_version.saturating_add(1);
        write_meta_with_versions_for(store_id, replica_id, now_ms, versions);

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let err = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            now_ms + 1,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .err()
        .expect("expected version mismatch");

        let expected = StoreMetaVersions::current();
        assert!(matches!(
            err,
            StoreRuntimeError::UnsupportedStoreMetaVersion { expected: got_expected, got }
                if got_expected == expected && got == versions
        ));
    }

    #[test]
    fn store_runtime_persists_orset_counter() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([32u8; 16]));
        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let runtime = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            1_700_000_000_000,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("open runtime")
        .runtime;

        let mut txn = runtime
            .wal_index
            .writer()
            .begin_txn()
            .expect("begin wal index txn");
        let first = txn.next_orset_counter().expect("increment");
        let second = txn.next_orset_counter().expect("increment");
        txn.commit().expect("commit wal index txn");
        assert_eq!(first, 1);
        assert_eq!(second, 2);

        let meta_path = paths::store_meta_path(store_id);
        let modified_before = std::fs::metadata(&meta_path)
            .expect("meta metadata before readback")
            .modified()
            .expect("meta modified time before readback");
        let meta = read_store_meta_optional(&meta_path)
            .expect("read meta")
            .expect("meta exists");
        let modified_after = std::fs::metadata(&meta_path)
            .expect("meta metadata after readback")
            .modified()
            .expect("meta modified time after readback");
        assert_eq!(meta.orset_counter, 0);
        assert_eq!(modified_after, modified_before);
        assert_eq!(
            runtime
                .wal_index
                .reader()
                .load_orset_counter()
                .expect("load orset counter"),
            2
        );
    }

    #[test]
    fn rotate_replica_id_bumps_replication_runtime_version() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([35u8; 16]));
        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let mut runtime = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            1_700_000_000_000,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("open runtime")
        .runtime;

        let version_v1 = runtime.replication_runtime_version();
        let old_replica_id = runtime.meta.replica_id;
        let rotation = runtime.rotate_replica_id().expect("rotate replica id");

        assert_eq!(rotation.old_replica_id, old_replica_id);
        assert_ne!(rotation.old_replica_id, rotation.new_replica_id);
        assert!(rotation.runtime_version > version_v1);
        assert_eq!(
            rotation.runtime_version,
            runtime.replication_runtime_version()
        );

        let meta_path = paths::store_meta_path(store_id);
        let meta = read_store_meta_optional(&meta_path)
            .expect("read meta")
            .expect("meta exists");
        assert_eq!(meta.replica_id, rotation.new_replica_id);
    }

    #[test]
    fn store_config_defaults_to_cache_and_persists() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([30u8; 16]));
        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let runtime = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            1_700_000_000_000,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("open runtime")
        .runtime;

        assert_eq!(
            runtime.wal_index.durability_mode(),
            IndexDurabilityMode::Cache
        );

        let raw =
            std::fs::read_to_string(paths::store_config_path(store_id)).expect("read store config");
        let config: StoreConfig = toml::from_str(&raw).expect("parse store config");
        assert_eq!(config.index_durability_mode, IndexDurabilityMode::Cache);
    }

    #[test]
    fn store_config_durable_mode_applies() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([31u8; 16]));
        let config_path = paths::store_config_path(store_id);
        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent).expect("create store dir");
        }
        std::fs::write(&config_path, "index_durability_mode = \"durable\"")
            .expect("write store config");

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let runtime = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            1_700_000_000_000,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("open runtime")
        .runtime;

        assert_eq!(
            runtime.wal_index.durability_mode(),
            IndexDurabilityMode::Durable
        );
    }

    #[cfg(unix)]
    #[test]
    fn bootstrap_failure_does_not_persist_meta_without_index() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([36u8; 16]));
        let store_dir = paths::store_dir(store_id);
        fs::create_dir_all(&store_dir).expect("create store dir");
        let index_dir = store_dir.join("index");
        let target = temp.path().join("index-target");
        fs::create_dir_all(&target).expect("create index target");
        fs::write(target.join("wal.sqlite"), b"sentinel").expect("seed target wal sqlite");
        symlink(&target, &index_dir).expect("symlink index dir");

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let err = match StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            1_700_000_000_000,
            "test",
            &Limits::default(),
            &namespace_defaults,
        ) {
            Ok(_) => panic!("bootstrap should fail when wal index dir is a symlink"),
            Err(err) => err,
        };

        assert!(matches!(
            err,
            StoreRuntimeError::WalIndex(WalIndexError::Symlink { .. })
        ));
        assert!(
            read_store_meta_optional(&paths::store_meta_path(store_id))
                .expect("read meta after failed bootstrap")
                .is_none(),
            "meta must not persist when bootstrap fails before index init"
        );
        assert!(
            read_pending_store_meta_transition_optional(&paths::store_meta_pending_path(store_id))
                .expect("read pending meta after failed bootstrap")
                .is_none(),
            "pending bootstrap marker must be cleaned up on synchronous bootstrap failure"
        );
        assert!(
            target.join("wal.sqlite").exists(),
            "bootstrap cleanup must not follow symlinked index targets"
        );
    }

    #[cfg(unix)]
    #[test]
    fn bootstrap_meta_write_failure_rolls_back_new_index() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([37u8; 16]));
        let meta_path = paths::store_meta_path(store_id);
        let _hook = scoped_meta_write_hook(meta_path.clone(), move |path, _meta| {
            assert_eq!(path, meta_path);
            Err(StoreRuntimeError::MetaWrite {
                path: Box::new(path.to_path_buf()),
                source: io::Error::other("injected meta write failure"),
            })
        });

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let err = match StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            1_700_000_000_000,
            "test",
            &Limits::default(),
            &namespace_defaults,
        ) {
            Ok(_) => panic!("bootstrap should fail when meta write cannot complete"),
            Err(err) => err,
        };

        assert!(matches!(err, StoreRuntimeError::MetaWrite { .. }));
        assert!(
            read_store_meta_optional(&paths::store_meta_path(store_id))
                .expect("read meta after failed meta write")
                .is_none(),
            "meta must not persist when bootstrap meta write fails"
        );
        assert!(
            !paths::wal_index_path(store_id).exists(),
            "wal index must roll back when bootstrap meta write fails"
        );
        assert!(
            read_pending_store_meta_transition_optional(&paths::store_meta_pending_path(store_id))
                .expect("read pending meta after failed meta write")
                .is_none(),
            "pending bootstrap marker must be cleaned up on synchronous meta write failure"
        );
    }

    #[cfg(unix)]
    #[test]
    fn bootstrap_store_config_failure_cleans_pending_marker() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([42u8; 16]));
        let config_path = paths::store_config_path(store_id);
        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent).expect("create store dir");
        }
        let target = temp.path().join("store-config-target");
        fs::write(&target, "index_durability_mode = \"cache\"").expect("write target config");
        symlink(&target, &config_path).expect("symlink store config");

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let err = match StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            1_700_000_000_000,
            "test",
            &Limits::default(),
            &namespace_defaults,
        ) {
            Ok(_) => panic!("bootstrap should fail when store config path is a symlink"),
            Err(err) => err,
        };

        assert!(matches!(err, StoreRuntimeError::StoreConfigSymlink { .. }));
        assert!(
            read_store_meta_optional(&paths::store_meta_path(store_id))
                .expect("read meta after failed bootstrap")
                .is_none(),
            "meta must remain absent when store config load fails"
        );
        assert!(
            read_pending_store_meta_transition_optional(&paths::store_meta_pending_path(store_id))
                .expect("read pending meta after failed bootstrap")
                .is_none(),
            "pending bootstrap marker must be cleaned up when store config load fails"
        );
    }

    #[test]
    fn fresh_bootstrap_lock_replica_matches_runtime_meta() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([43u8; 16]));
        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let open = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            1_700_000_000_000,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("open runtime");

        let lock_meta = read_lock_meta(store_id)
            .expect("read store lock")
            .expect("store lock exists");
        assert_eq!(lock_meta.replica_id, open.runtime.meta.replica_id);
    }

    #[test]
    fn checkpoint_dirty_shards_roundtrip() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([50u8; 16]));
        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let mut runtime = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            1_700_000_000_000,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("open runtime")
        .runtime;

        let namespace = NamespaceId::core();
        let stamp = Stamp::new(
            WriteStamp::new(1_700_000_000_000, 1),
            ActorId::new("author").expect("actor id"),
        );
        let bead_id = BeadId::parse("bd-dirty1").expect("bead id");
        let dep_to = BeadId::parse("bd-dirty2").expect("bead id");
        let core = BeadCore::new(bead_id.clone(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("title".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        let bead = crate::core::Bead::new(core, fields);
        let dep_key = DepKey::new(bead_id.clone(), dep_to, DepKind::Blocks).expect("dep key");
        let mut core_state = CanonicalState::new();
        core_state.insert(bead).expect("insert bead");
        let dep_dot = Dot {
            replica: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            counter: 1,
        };
        let dep_key_checked = core_state
            .check_dep_add_key(dep_key.clone())
            .expect("dep key");
        core_state.apply_dep_add(dep_key_checked, dep_dot, stamp.clone());
        let mut store_state = StoreState::new();
        store_state.set_core_state(core_state);
        runtime.state = store_state;

        let mut outcome = ApplyOutcome::default();
        outcome.changed_beads.insert(bead_id.clone());
        outcome.changed_deps.insert(dep_key.clone());
        runtime.record_checkpoint_dirty_shards(&namespace, &outcome);

        let snapshot = runtime
            .checkpoint_snapshot("core", std::slice::from_ref(&namespace), 1_700_000_000_000)
            .expect("snapshot");

        let state_path = CheckpointShardPath::new(
            namespace.clone(),
            CheckpointFileKind::State,
            shard_for_bead(&bead_id),
        );
        let dep_path = CheckpointShardPath::new(
            namespace.clone(),
            CheckpointFileKind::Deps,
            shard_for_dep(dep_key.from(), dep_key.to(), dep_key.kind()),
        );
        let mut expected_paths = BTreeSet::new();
        expected_paths.insert(state_path);
        expected_paths.insert(dep_path);
        assert_eq!(snapshot.dirty_shards, expected_paths);
        assert!(!runtime.checkpoint_dirty_shards.contains_key(&namespace));

        runtime.rollback_checkpoint_dirty_shards("core");
        let restored = runtime
            .checkpoint_dirty_shards
            .get(&namespace)
            .cloned()
            .unwrap_or_default();
        assert_eq!(restored, expected_paths);

        let _ = runtime
            .checkpoint_snapshot("core", std::slice::from_ref(&namespace), 1_700_000_000_001)
            .expect("snapshot");
        runtime.commit_checkpoint_dirty_shards("core");
        assert!(!runtime.checkpoint_dirty_shards.contains_key(&namespace));
    }

    #[test]
    fn checkpoint_snapshot_includes_roster_hash() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([60u8; 16]));
        let roster_toml = r#"
[[replicas]]
replica_id = "00000000-0000-0000-0000-000000000001"
name = "alpha"
role = "anchor"
durability_eligible = true
"#;
        let roster_path = paths::replicas_path(store_id);
        if let Some(parent) = roster_path.parent() {
            std::fs::create_dir_all(parent).expect("create store dir");
        }
        std::fs::write(&roster_path, roster_toml).expect("write roster");

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let runtime = StoreRuntime::open(
            &crate::daemon_layout_from_paths(),
            store_id,
            RemoteUrl::new("example.com/test/repo"),
            1_700_000_000_000,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("open runtime")
        .runtime;

        let snapshot = runtime
            .checkpoint_snapshot_readonly("core", &[NamespaceId::core()], 1_700_000_000_000)
            .expect("snapshot");
        let roster = ReplicaRoster::from_toml_str(roster_toml).expect("parse roster");
        let expected = roster_hash(&roster).expect("hash roster");
        assert_eq!(snapshot.roster_hash, Some(expected));
    }

    #[cfg(unix)]
    #[test]
    fn namespace_policies_enforce_permissions() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([42u8; 16]));
        let path = paths::namespaces_path(store_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create store dir");
        }

        let mut namespaces = BTreeMap::new();
        namespaces.insert(NamespaceId::core(), NamespacePolicy::core_default());
        let policies = NamespacePolicies { namespaces };
        let toml = toml::to_string(&policies).expect("toml encode");
        fs::write(&path, toml).expect("write namespaces.toml");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
            .expect("chmod namespaces.toml");

        let defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        load_namespace_policies(store_id, &defaults).expect("load policies");

        let mode = fs::metadata(&path).expect("metadata").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn namespace_policies_reject_symlink() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([43u8; 16]));
        let path = paths::namespaces_path(store_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create store dir");
        }

        let target = temp.path().join("namespaces-target.toml");
        fs::write(&target, b"").expect("write target");
        symlink(&target, &path).expect("symlink namespaces.toml");

        let defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let err = load_namespace_policies(store_id, &defaults).unwrap_err();
        assert!(matches!(
            err,
            StoreRuntimeError::NamespacePoliciesSymlink { .. }
        ));
    }

    #[test]
    fn namespace_policies_fall_back_to_defaults_when_missing() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([44u8; 16]));
        let defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let loaded = load_namespace_policies(store_id, &defaults).expect("load policies");

        assert_eq!(loaded, defaults);
        assert!(loaded.contains_key(&NamespaceId::core()));
        assert!(loaded.contains_key(&NamespaceId::parse("sys").unwrap()));
        assert!(loaded.contains_key(&NamespaceId::parse("wf").unwrap()));
        assert!(loaded.contains_key(&NamespaceId::parse("tmp").unwrap()));
    }
}
