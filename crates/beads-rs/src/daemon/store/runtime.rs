//! Store runtime state and on-disk identity handling.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::RngCore;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::core::error::details::WalTailTruncatedDetails;
use crate::core::{
    ActorId, Applied, ApplyOutcome, ContentHash, Durable, ErrorPayload, HeadStatus, Limits,
    NamespaceId, NamespacePolicies, NamespacePolicy, ProtocolErrorCode, ReplicaId, ReplicaRoster,
    ReplicaRosterError, SegmentId, StoreEpoch, StoreId, StoreIdentity, StoreMeta,
    StoreMetaVersions, StoreState, WatermarkError, Watermarks, WriteStamp,
};
use crate::daemon::admission::AdmissionController;
use crate::daemon::broadcast::{BroadcasterLimits, EventBroadcaster};
use crate::daemon::remote::RemoteUrl;
use crate::daemon::repl::PeerAckTable;
use crate::daemon::store_lock::{StoreLock, StoreLockError};
use crate::daemon::wal::{
    EventWal, HlcRow, IndexDurabilityMode, ReplayStats, SqliteWalIndex, WalIndex, WalIndexError,
    WalReplayError, catch_up_index, rebuild_index,
};
use crate::git::checkpoint::{
    CheckpointFileKind, CheckpointShardPath, CheckpointSnapshot, CheckpointSnapshotError,
    CheckpointSnapshotInput, build_snapshot, policy_hash, roster_hash, shard_for_bead,
    shard_for_dep, shard_for_tombstone,
};
use crate::paths;

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
pub(crate) struct WalTailTruncatedRecord {
    pub(crate) namespace: NamespaceId,
    pub(crate) segment_id: Option<SegmentId>,
    pub(crate) truncated_from_offset: u64,
    pub(crate) wall_ms: u64,
}

pub struct StoreRuntime {
    pub(crate) primary_remote: RemoteUrl,
    pub(crate) meta: StoreMeta,
    #[allow(dead_code)]
    pub(crate) policies: BTreeMap<NamespaceId, NamespacePolicy>,
    pub(crate) state: StoreState,
    pub(crate) last_wal_tail_truncated: Option<WalTailTruncatedRecord>,
    pub(crate) watermarks_applied: Watermarks<Applied>,
    pub(crate) watermarks_durable: Watermarks<Durable>,
    checkpoint_dirty_shards: BTreeMap<NamespaceId, BTreeSet<CheckpointShardPath>>,
    checkpoint_dirty_inflight:
        BTreeMap<String, BTreeMap<NamespaceId, BTreeSet<CheckpointShardPath>>>,
    pub(crate) broadcaster: EventBroadcaster,
    pub(crate) admission: AdmissionController,
    pub(crate) maintenance_mode: bool,
    #[allow(dead_code)]
    pub(crate) peer_acks: Arc<Mutex<PeerAckTable>>,
    pub(crate) event_wal: EventWal,
    #[allow(dead_code)]
    pub(crate) wal_index: Arc<dyn WalIndex>,
    pub(crate) last_wal_checkpoint: Option<Instant>,
    pub(crate) last_lock_heartbeat: Option<Instant>,
    #[allow(dead_code)]
    lock: StoreLock,
}

pub struct StoreRuntimeOpen {
    pub runtime: StoreRuntime,
    pub replay_stats: ReplayStats,
}

impl StoreRuntime {
    pub fn open(
        store_id: StoreId,
        primary_remote: RemoteUrl,
        now_ms: u64,
        daemon_version: &str,
        limits: &Limits,
        namespace_defaults: &BTreeMap<NamespaceId, NamespacePolicy>,
    ) -> Result<StoreRuntimeOpen, StoreRuntimeError> {
        let meta_path = paths::store_meta_path(store_id);
        let existing = read_store_meta_optional(&meta_path)?;

        let expected_versions = StoreMetaVersions::current();
        let meta = match existing.as_ref() {
            Some(meta) => {
                if meta.store_id() != store_id {
                    return Err(StoreRuntimeError::MetaMismatch {
                        expected: store_id,
                        got: meta.store_id(),
                    });
                }
                let got_versions = meta.versions();
                if got_versions != expected_versions {
                    return Err(StoreRuntimeError::UnsupportedStoreMetaVersion {
                        expected: expected_versions,
                        got: got_versions,
                    });
                }
                meta.clone()
            }
            None => {
                let identity = StoreIdentity::new(store_id, StoreEpoch::ZERO);
                StoreMeta::new(identity, new_replica_id(), expected_versions, now_ms)
            }
        };

        let lock = StoreLock::acquire(store_id, meta.replica_id, now_ms, daemon_version)?;

        if existing.is_none() {
            write_store_meta(&meta_path, &meta)?;
        }

        let store_dir = paths::store_dir(store_id);
        let store_config = load_store_config(store_id, true)?;
        let (mut wal_index, needs_rebuild) = open_wal_index(
            store_id,
            &store_dir,
            &meta,
            store_config.index_durability_mode,
        )?;
        let replay_stats = if needs_rebuild {
            rebuild_index(&store_dir, &meta, &wal_index, limits)?
        } else {
            match catch_up_index(&store_dir, &meta, &wal_index, limits) {
                Ok(stats) => stats,
                Err(WalReplayError::IndexOffsetInvalid { .. }) => {
                    remove_wal_index_files(store_id)?;
                    wal_index = SqliteWalIndex::open(
                        &store_dir,
                        &meta,
                        store_config.index_durability_mode,
                    )?;
                    rebuild_index(&store_dir, &meta, &wal_index, limits)?
                }
                Err(err) => return Err(StoreRuntimeError::WalReplay(Box::new(err))),
            }
        };

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
            primary_remote,
            meta,
            policies: load_namespace_policies(store_id, namespace_defaults)?,
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
        let roster = match load_replica_roster(self.meta.store_id()) {
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
            namespaces: namespaces.to_vec(),
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
            namespaces: namespaces.to_vec(),
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
        if outcome.changed_beads.is_empty()
            && outcome.changed_deps.is_empty()
            && outcome.changed_notes.is_empty()
        {
            return;
        }
        let dirty = self
            .checkpoint_dirty_shards
            .entry(namespace.clone())
            .or_default();
        let Some(state) = self.state.get(namespace) else {
            return;
        };
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
    }

    pub(crate) fn commit_checkpoint_dirty_shards(&mut self, checkpoint_group: &str) {
        self.checkpoint_dirty_inflight.remove(checkpoint_group);
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

    pub fn rotate_replica_id(&mut self) -> Result<(ReplicaId, ReplicaId), StoreRuntimeError> {
        let old = self.meta.replica_id;
        let new = new_replica_id();
        self.meta.replica_id = new;
        let path = paths::store_meta_path(self.meta.store_id());
        write_store_meta(&path, &self.meta)?;
        Ok((old, new))
    }

    pub fn next_orset_counter(&mut self) -> Result<u64, StoreRuntimeError> {
        let next = self
            .meta
            .orset_counter
            .checked_add(1)
            .expect("orset counter overflow");
        self.meta.orset_counter = next;
        let path = paths::store_meta_path(self.meta.store_id());
        write_store_meta(&path, &self.meta)?;
        Ok(next)
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

fn open_wal_index(
    store_id: StoreId,
    store_dir: &Path,
    meta: &StoreMeta,
    mode: IndexDurabilityMode,
) -> Result<(SqliteWalIndex, bool), StoreRuntimeError> {
    let db_path = paths::wal_index_path(store_id);
    let mut needs_rebuild = !db_path.exists();

    match SqliteWalIndex::open(store_dir, meta, mode) {
        Ok(index) => Ok((index, needs_rebuild)),
        Err(WalIndexError::SchemaVersionMismatch { .. }) => {
            needs_rebuild = true;
            remove_wal_index_files(store_id)?;
            let index = SqliteWalIndex::open(store_dir, meta, mode)?;
            Ok((index, needs_rebuild))
        }
        Err(err) => Err(StoreRuntimeError::WalIndex(err)),
    }
}

fn load_store_config(
    store_id: StoreId,
    write_default: bool,
) -> Result<StoreConfig, StoreRuntimeError> {
    let path = paths::store_config_path(store_id);
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

pub(crate) fn store_index_durability_mode(
    store_id: StoreId,
) -> Result<IndexDurabilityMode, StoreRuntimeError> {
    Ok(load_store_config(store_id, false)?.index_durability_mode)
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

fn remove_wal_index_files(store_id: StoreId) -> Result<(), StoreRuntimeError> {
    let db_path = paths::wal_index_path(store_id);
    for suffix in ["", "-wal", "-shm"] {
        let path = if suffix.is_empty() {
            db_path.clone()
        } else {
            PathBuf::from(format!("{}{}", db_path.display(), suffix))
        };
        if path.exists() {
            fs::remove_file(&path).map_err(|source| {
                StoreRuntimeError::WalIndex(WalIndexError::Io {
                    path: path.clone(),
                    source,
                })
            })?;
        }
    }
    Ok(())
}

pub(crate) fn load_namespace_policies(
    store_id: StoreId,
    defaults: &BTreeMap<NamespaceId, NamespacePolicy>,
) -> Result<BTreeMap<NamespaceId, NamespacePolicy>, StoreRuntimeError> {
    let path = paths::namespaces_path(store_id);
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
    store_id: StoreId,
) -> Result<Option<ReplicaRoster>, StoreRuntimeError> {
    let path = paths::replicas_path(store_id);
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
        let namespace = row.namespace;
        let origin = row.origin;

        applied
            .observe_at_least(&namespace, &origin, row.applied.seq(), row.applied.head())
            .map_err(|source| StoreRuntimeError::WatermarkInvalid {
                kind: "applied",
                namespace: namespace.clone(),
                origin,
                source: Box::new(source),
            })?;

        durable
            .observe_at_least(&namespace, &origin, row.durable.seq(), row.durable.head())
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

fn read_store_meta_optional(path: &Path) -> Result<Option<StoreMeta>, StoreRuntimeError> {
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
    let meta = serde_json::from_slice(&bytes).map_err(|source| StoreRuntimeError::MetaParse {
        path: Box::new(path.to_path_buf()),
        source,
    })?;
    ensure_file_permissions(path)?;
    Ok(Some(meta))
}

fn write_store_meta(path: &Path, meta: &StoreMeta) -> Result<(), StoreRuntimeError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| StoreRuntimeError::MetaWrite {
            path: Box::new(path.to_path_buf()),
            source,
        })?;
    }

    let bytes = serde_json::to_vec(meta).map_err(|source| StoreRuntimeError::MetaParse {
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
    #[cfg(unix)]
    {
        if let Ok(dir) = fs::File::open(dir) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
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
        ActorId, CanonicalState, DepKey, Dot, ReplicaId, Seq0, StoreState, Watermark,
    };
    use crate::daemon::remote::RemoteUrl;
    use crate::daemon::wal::{
        IndexDurabilityMode, SqliteWalIndex, WalIndex, WalIndexError, WalReplayError,
    };
    use crate::paths;
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
        txn.update_watermark(&namespace, &origin, applied, durable)
            .expect("update watermark");
        txn.commit().expect("commit watermark");

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let runtime = StoreRuntime::open(
            store_id,
            RemoteUrl("example.com/test/repo".to_string()),
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
    fn phase3_head_sha_rejects_missing_head() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([20u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([21u8; 16]));
        let now_ms = 1_700_000_000_000;
        let meta = write_meta_for(store_id, replica_id, now_ms);
        let _index = SqliteWalIndex::open(
            &paths::store_dir(store_id),
            &meta,
            IndexDurabilityMode::Cache,
        )
        .expect("open wal index");

        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([22u8; 16]));
        let db_path = paths::store_dir(store_id).join("index").join("wal.sqlite");
        let conn = rusqlite::Connection::open(&db_path).expect("open wal sqlite");
        conn.execute(
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
        .expect("insert invalid watermark");

        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let result = StoreRuntime::open(
            store_id,
            RemoteUrl("example.com/test/repo".to_string()),
            now_ms + 1,
            "test",
            &Limits::default(),
            &namespace_defaults,
        );

        assert!(matches!(
            result,
            Err(StoreRuntimeError::WalReplay(err))
                if matches!(
                    &*err,
                    WalReplayError::Index(WalIndexError::WatermarkRowDecode(_))
                )
        ));
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
            store_id,
            RemoteUrl("example.com/test/repo".to_string()),
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
        let mut runtime = StoreRuntime::open(
            store_id,
            RemoteUrl("example.com/test/repo".to_string()),
            1_700_000_000_000,
            "test",
            &Limits::default(),
            &namespace_defaults,
        )
        .expect("open runtime")
        .runtime;

        let first = runtime.next_orset_counter().expect("increment");
        let second = runtime.next_orset_counter().expect("increment");
        assert_eq!(first, 1);
        assert_eq!(second, 2);

        let meta_path = paths::store_meta_path(store_id);
        let meta = read_store_meta_optional(&meta_path)
            .expect("read meta")
            .expect("meta exists");
        assert_eq!(meta.orset_counter, 2);
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
            store_id,
            RemoteUrl("example.com/test/repo".to_string()),
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
            store_id,
            RemoteUrl("example.com/test/repo".to_string()),
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

    #[test]
    fn checkpoint_dirty_shards_roundtrip() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([50u8; 16]));
        let namespace_defaults = crate::config::Config::default()
            .namespace_defaults
            .namespaces;
        let mut runtime = StoreRuntime::open(
            store_id,
            RemoteUrl("example.com/test/repo".to_string()),
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
            store_id,
            RemoteUrl("example.com/test/repo".to_string()),
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
