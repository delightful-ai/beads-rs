//! Store runtime state and on-disk identity handling.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;

use rand::RngCore;
use thiserror::Error;
use uuid::Uuid;

use crate::core::{
    Applied, Durable, NamespaceId, NamespacePolicy, ReplicaId, StoreEpoch, StoreId, StoreIdentity,
    StoreMeta, StoreMetaVersions, Watermarks,
};
use crate::daemon::remote::RemoteUrl;
use crate::daemon::repo::RepoState;
use crate::daemon::store_lock::{StoreLock, StoreLockError};
use crate::daemon::wal::Wal;
use crate::paths;

const STORE_FORMAT_VERSION: u32 = 1;
const WAL_FORMAT_VERSION: u32 = 1;
const CHECKPOINT_FORMAT_VERSION: u32 = 1;
const REPLICATION_PROTOCOL_VERSION: u32 = 1;
const INDEX_SCHEMA_VERSION: u32 = 1;

pub struct StoreRuntime {
    pub(crate) primary_remote: RemoteUrl,
    pub(crate) meta: StoreMeta,
    #[allow(dead_code)]
    pub(crate) policies: BTreeMap<NamespaceId, NamespacePolicy>,
    pub(crate) repo_state: RepoState,
    #[allow(dead_code)]
    pub(crate) watermarks_applied: Watermarks<Applied>,
    #[allow(dead_code)]
    pub(crate) watermarks_durable: Watermarks<Durable>,
    #[allow(dead_code)]
    pub(crate) wal: Arc<Wal>,
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

        Ok(Self {
            primary_remote,
            meta,
            policies: default_policies(),
            repo_state: RepoState::new(),
            watermarks_applied: Watermarks::default(),
            watermarks_durable: Watermarks::default(),
            wal,
            lock,
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
    #[error("store meta write failed at {path:?}: {source}")]
    MetaWrite {
        path: Box<std::path::PathBuf>,
        #[source]
        source: io::Error,
    },
}

fn default_policies() -> BTreeMap<NamespaceId, NamespacePolicy> {
    let mut policies = BTreeMap::new();
    policies.insert(NamespaceId::core(), NamespacePolicy::core_default());
    policies
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
            })
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

fn new_replica_id() -> ReplicaId {
    let mut rng = rand::rng();
    let mut bytes = [0u8; 16];
    rng.fill_bytes(&mut bytes);
    ReplicaId::new(Uuid::from_bytes(bytes))
}
