//! Store runtime state and on-disk identity handling.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rand::RngCore;
use thiserror::Error;
use uuid::Uuid;

use crate::core::{
    ActorId, Applied, Durable, HeadStatus, Limits, NamespaceId, NamespacePolicy, ReplicaId, Seq0,
    StoreEpoch, StoreId, StoreIdentity, StoreMeta, StoreMetaVersions, WatermarkError, Watermarks,
    WriteStamp,
};
use crate::daemon::admission::AdmissionController;
use crate::daemon::broadcast::{BroadcasterLimits, EventBroadcaster};
use crate::daemon::remote::RemoteUrl;
use crate::daemon::repl::PeerAckTable;
use crate::daemon::repo::RepoState;
use crate::daemon::store_lock::{StoreLock, StoreLockError};
use crate::daemon::wal::{
    IndexDurabilityMode, SqliteWalIndex, Wal, WalIndex, WalIndexError, WalReplayError,
    catch_up_index, rebuild_index,
};
use crate::paths;

const STORE_FORMAT_VERSION: u32 = 1;
const WAL_FORMAT_VERSION: u32 = 2;
const CHECKPOINT_FORMAT_VERSION: u32 = 1;
const REPLICATION_PROTOCOL_VERSION: u32 = 1;
const INDEX_SCHEMA_VERSION: u32 = 1;

pub struct StoreRuntime {
    pub(crate) primary_remote: RemoteUrl,
    pub(crate) meta: StoreMeta,
    #[allow(dead_code)]
    pub(crate) policies: BTreeMap<NamespaceId, NamespacePolicy>,
    pub(crate) repo_state: RepoState,
    pub(crate) watermarks_applied: Watermarks<Applied>,
    pub(crate) watermarks_durable: Watermarks<Durable>,
    pub(crate) broadcaster: EventBroadcaster,
    pub(crate) admission: AdmissionController,
    #[allow(dead_code)]
    pub(crate) peer_acks: Arc<Mutex<PeerAckTable>>,
    #[allow(dead_code)]
    pub(crate) wal: Arc<Wal>,
    #[allow(dead_code)]
    pub(crate) wal_index: Arc<SqliteWalIndex>,
    #[allow(dead_code)]
    lock: StoreLock,
}

impl StoreRuntime {
    pub fn open(
        store_id: StoreId,
        primary_remote: RemoteUrl,
        wal: Arc<Wal>,
        now_ms: u64,
        daemon_version: &str,
        limits: &Limits,
    ) -> Result<Self, StoreRuntimeError> {
        let meta_path = paths::store_meta_path(store_id);
        let existing = read_store_meta_optional(&meta_path)?;

        let meta = match existing.as_ref() {
            Some(meta) => {
                if meta.store_id() != store_id {
                    return Err(StoreRuntimeError::MetaMismatch {
                        expected: store_id,
                        got: meta.store_id(),
                    });
                }
                meta.clone()
            }
            None => {
                let identity = StoreIdentity::new(store_id, StoreEpoch::ZERO);
                let versions = StoreMetaVersions::new(
                    STORE_FORMAT_VERSION,
                    WAL_FORMAT_VERSION,
                    CHECKPOINT_FORMAT_VERSION,
                    REPLICATION_PROTOCOL_VERSION,
                    INDEX_SCHEMA_VERSION,
                );
                StoreMeta::new(identity, new_replica_id(), versions, now_ms)
            }
        };

        let lock = StoreLock::acquire(store_id, meta.replica_id, now_ms, daemon_version)?;

        if existing.is_none() {
            write_store_meta(&meta_path, &meta)?;
        }

        let store_dir = paths::store_dir(store_id);
        let (mut wal_index, needs_rebuild) = open_wal_index(store_id, &store_dir, &meta)?;
        if needs_rebuild {
            rebuild_index(&store_dir, &meta, &wal_index, limits)?;
        } else if let Err(err) = catch_up_index(&store_dir, &meta, &wal_index, limits) {
            match err {
                WalReplayError::IndexOffsetInvalid { .. } => {
                    remove_wal_index_files(store_id)?;
                    wal_index =
                        SqliteWalIndex::open(&store_dir, &meta, IndexDurabilityMode::Cache)?;
                    rebuild_index(&store_dir, &meta, &wal_index, limits)?;
                }
                err => return Err(StoreRuntimeError::WalReplay(Box::new(err))),
            }
        }

        let (watermarks_applied, watermarks_durable) = load_watermarks(&wal_index)?;
        let broadcaster = EventBroadcaster::new(BroadcasterLimits::from_limits(limits));
        let admission = AdmissionController::new(limits);
        let peer_acks = Arc::new(Mutex::new(PeerAckTable::new()));

        Ok(Self {
            primary_remote,
            meta,
            policies: default_policies(),
            repo_state: RepoState::new(),
            watermarks_applied,
            watermarks_durable,
            broadcaster,
            admission,
            peer_acks,
            wal,
            wal_index: Arc::new(wal_index),
            lock,
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
    #[error("store meta write failed at {path:?}: {source}")]
    MetaWrite {
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
        source: WatermarkError,
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
) -> Result<(SqliteWalIndex, bool), StoreRuntimeError> {
    let db_path = paths::wal_index_path(store_id);
    let mut needs_rebuild = !db_path.exists();

    match SqliteWalIndex::open(store_dir, meta, IndexDurabilityMode::Cache) {
        Ok(index) => Ok((index, needs_rebuild)),
        Err(WalIndexError::SchemaVersionMismatch { .. }) => {
            needs_rebuild = true;
            remove_wal_index_files(store_id)?;
            let index = SqliteWalIndex::open(store_dir, meta, IndexDurabilityMode::Cache)?;
            Ok((index, needs_rebuild))
        }
        Err(err) => Err(StoreRuntimeError::WalIndex(err)),
    }
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

fn default_policies() -> BTreeMap<NamespaceId, NamespacePolicy> {
    let mut policies = BTreeMap::new();
    policies.insert(NamespaceId::core(), NamespacePolicy::core_default());
    policies
}

fn load_watermarks(
    index: &SqliteWalIndex,
) -> Result<(Watermarks<Applied>, Watermarks<Durable>), StoreRuntimeError> {
    let rows = index.reader().load_watermarks()?;
    let mut applied = Watermarks::<Applied>::new();
    let mut durable = Watermarks::<Durable>::new();

    for row in rows {
        let namespace = row.namespace;
        let origin = row.origin;

        let applied_head =
            head_status_from_row(row.applied_seq, row.applied_head_sha).map_err(|source| {
                StoreRuntimeError::WatermarkInvalid {
                    kind: "applied",
                    namespace: namespace.clone(),
                    origin,
                    source,
                }
            })?;
        applied
            .observe_at_least(
                &namespace,
                &origin,
                Seq0::new(row.applied_seq),
                applied_head,
            )
            .map_err(|source| StoreRuntimeError::WatermarkInvalid {
                kind: "applied",
                namespace: namespace.clone(),
                origin,
                source,
            })?;

        let durable_head =
            head_status_from_row(row.durable_seq, row.durable_head_sha).map_err(|source| {
                StoreRuntimeError::WatermarkInvalid {
                    kind: "durable",
                    namespace: namespace.clone(),
                    origin,
                    source,
                }
            })?;
        durable
            .observe_at_least(
                &namespace,
                &origin,
                Seq0::new(row.durable_seq),
                durable_head,
            )
            .map_err(|source| StoreRuntimeError::WatermarkInvalid {
                kind: "durable",
                namespace: namespace.clone(),
                origin,
                source,
            })?;
    }

    Ok((applied, durable))
}

fn head_status_from_row(seq: u64, head: Option<[u8; 32]>) -> Result<HeadStatus, WatermarkError> {
    let seq0 = Seq0::new(seq);
    if seq == 0 {
        return match head {
            None => Ok(HeadStatus::Genesis),
            Some(_) => Err(WatermarkError::UnexpectedHead { seq: seq0 }),
        };
    }

    match head {
        Some(sha) => Ok(HeadStatus::Known(sha)),
        None => Err(WatermarkError::MissingHead { seq: seq0 }),
    }
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
    fs::write(path, bytes).map_err(|source| StoreRuntimeError::MetaWrite {
        path: Box::new(path.to_path_buf()),
        source,
    })?;
    ensure_file_permissions(path)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    use crate::daemon::remote::RemoteUrl;
    use crate::daemon::wal::{IndexDurabilityMode, SqliteWalIndex, Wal, WalIndex};
    use crate::paths;
    use std::sync::Arc;

    struct DataDirGuard;

    impl Drop for DataDirGuard {
        fn drop(&mut self) {
            paths::set_data_dir_for_tests(None);
        }
    }

    fn write_meta_for(store_id: StoreId, replica_id: ReplicaId, now_ms: u64) -> StoreMeta {
        let identity = StoreIdentity::new(store_id, StoreEpoch::ZERO);
        let versions = StoreMetaVersions::new(
            STORE_FORMAT_VERSION,
            WAL_FORMAT_VERSION,
            CHECKPOINT_FORMAT_VERSION,
            REPLICATION_PROTOCOL_VERSION,
            INDEX_SCHEMA_VERSION,
        );
        let meta = StoreMeta::new(identity, replica_id, versions, now_ms);
        write_store_meta(&paths::store_meta_path(store_id), &meta).expect("write meta");
        meta
    }

    #[test]
    fn phase3_head_sha_loads_from_index() {
        let _lock = paths::lock_data_dir_for_tests();
        let temp = TempDir::new().expect("temp dir");
        paths::set_data_dir_for_tests(Some(temp.path().to_path_buf()));
        let _guard = DataDirGuard;

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
        txn.update_watermark(&namespace, &origin, 2, 2, Some(head), Some(head))
            .expect("update watermark");
        txn.commit().expect("commit watermark");

        let wal = Wal::new(temp.path()).expect("wal");
        let runtime = StoreRuntime::open(
            store_id,
            RemoteUrl("example.com/test/repo".to_string()),
            Arc::new(wal),
            now_ms + 1,
            "test",
            &Limits::default(),
        )
        .expect("open runtime");

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
        let _lock = paths::lock_data_dir_for_tests();
        let temp = TempDir::new().expect("temp dir");
        paths::set_data_dir_for_tests(Some(temp.path().to_path_buf()));
        let _guard = DataDirGuard;

        let store_id = StoreId::new(Uuid::from_bytes([20u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([21u8; 16]));
        let now_ms = 1_700_000_000_000;
        let meta = write_meta_for(store_id, replica_id, now_ms);
        let index = SqliteWalIndex::open(
            &paths::store_dir(store_id),
            &meta,
            IndexDurabilityMode::Cache,
        )
        .expect("open wal index");

        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([22u8; 16]));
        let mut txn = index.writer().begin_txn().expect("begin txn");
        txn.update_watermark(&namespace, &origin, 1, 0, None, None)
            .expect("update watermark");
        txn.commit().expect("commit watermark");

        let wal = Wal::new(temp.path()).expect("wal");
        let result = StoreRuntime::open(
            store_id,
            RemoteUrl("example.com/test/repo".to_string()),
            Arc::new(wal),
            now_ms + 1,
            "test",
            &Limits::default(),
        );

        assert!(matches!(
            result,
            Err(StoreRuntimeError::WatermarkInvalid { .. })
        ));
    }
}

fn new_replica_id() -> ReplicaId {
    let mut rng = rand::rng();
    let mut bytes = [0u8; 16];
    rng.fill_bytes(&mut bytes);
    ReplicaId::new(Uuid::from_bytes(bytes))
}
