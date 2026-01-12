//! Store lock handling and metadata persistence.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::core::{ReplicaId, StoreId};
use crate::paths;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreLockMeta {
    pub store_id: StoreId,
    pub replica_id: ReplicaId,
    pub pid: u32,
    pub started_at_ms: u64,
    pub daemon_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_ms: Option<u64>,
}

impl StoreLockMeta {
    pub fn new(
        store_id: StoreId,
        replica_id: ReplicaId,
        started_at_ms: u64,
        daemon_version: impl Into<String>,
    ) -> Self {
        Self {
            store_id,
            replica_id,
            pid: std::process::id(),
            started_at_ms,
            daemon_version: daemon_version.into(),
            last_heartbeat_ms: Some(started_at_ms),
        }
    }
}

#[derive(Debug)]
pub struct StoreLock {
    path: PathBuf,
    meta: StoreLockMeta,
    released: bool,
}

impl StoreLock {
    pub fn acquire(
        store_id: StoreId,
        replica_id: ReplicaId,
        started_at_ms: u64,
        daemon_version: impl Into<String>,
    ) -> Result<Self, StoreLockError> {
        ensure_dir(&paths::stores_dir())?;
        let store_dir = paths::store_dir(store_id);
        ensure_dir(&store_dir)?;

        let path = paths::store_lock_path(store_id);
        reject_symlink(&path)?;

        let meta = StoreLockMeta::new(store_id, replica_id, started_at_ms, daemon_version);

        let mut file = match open_new_lock_file(&path) {
            Ok(file) => file,
            Err(StoreLockError::Io(err)) if err.kind() == io::ErrorKind::AlreadyExists => {
                let (meta, meta_error) = match read_metadata(&path) {
                    Ok(meta) => (Some(meta), None),
                    Err(err) => (None, Some(err.to_string())),
                };
                return Err(StoreLockError::Held {
                    store_id,
                    path: Box::new(path),
                    meta: meta.map(Box::new),
                    meta_error,
                });
            }
            Err(err) => return Err(err),
        };

        write_metadata(&mut file, &path, &meta)?;
        set_file_permissions(&path, 0o600)?;

        Ok(Self {
            path,
            meta,
            released: false,
        })
    }

    pub fn meta(&self) -> &StoreLockMeta {
        &self.meta
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn update_heartbeat(&mut self, now_ms: u64) -> Result<(), StoreLockError> {
        self.meta.last_heartbeat_ms = Some(now_ms);
        let mut file = open_existing_lock_file(&self.path)?;
        write_metadata(&mut file, &self.path, &self.meta)?;
        set_file_permissions(&self.path, 0o600)?;
        Ok(())
    }

    pub fn release(mut self) -> Result<(), StoreLockError> {
        if !self.released {
            fs::remove_file(&self.path)?;
            self.released = true;
        }
        Ok(())
    }
}

impl Drop for StoreLock {
    fn drop(&mut self) {
        if !self.released {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub fn read_lock_meta(store_id: StoreId) -> Result<Option<StoreLockMeta>, StoreLockError> {
    let path = paths::store_lock_path(store_id);
    match fs::symlink_metadata(&path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(StoreLockError::Symlink { path }),
        Ok(_) => Ok(Some(read_metadata(&path)?)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(StoreLockError::Io(err)),
    }
}

#[derive(Debug, Error)]
pub enum StoreLockError {
    #[error("store lock already held for {store_id} at {path:?}")]
    Held {
        store_id: StoreId,
        path: Box<PathBuf>,
        meta: Option<Box<StoreLockMeta>>,
        meta_error: Option<String>,
    },
    #[error("store lock path is a symlink: {path:?}")]
    Symlink { path: PathBuf },
    #[error("lock metadata corrupted at {path:?}: {source}")]
    MetadataCorrupt {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

fn ensure_dir(path: &Path) -> Result<(), StoreLockError> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(StoreLockError::Symlink {
                    path: path.to_path_buf(),
                });
            }
            if !meta.is_dir() {
                return Err(StoreLockError::Io(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("expected directory at {:?}", path),
                )));
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path)?;
        }
        Err(err) => return Err(StoreLockError::Io(err)),
    }
    set_dir_permissions(path, 0o700)?;
    Ok(())
}

fn reject_symlink(path: &Path) -> Result<(), StoreLockError> {
    if let Ok(meta) = fs::symlink_metadata(path) && meta.file_type().is_symlink() {
        return Err(StoreLockError::Symlink {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn read_metadata(path: &Path) -> Result<StoreLockMeta, StoreLockError> {
    reject_symlink(path)?;
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes).map_err(|source| StoreLockError::MetadataCorrupt {
        path: path.to_path_buf(),
        source,
    })
}

fn write_metadata(
    file: &mut fs::File,
    path: &Path,
    meta: &StoreLockMeta,
) -> Result<(), StoreLockError> {
    serde_json::to_writer(&mut *file, meta).map_err(|source| StoreLockError::MetadataCorrupt {
        path: path.to_path_buf(),
        source,
    })?;
    file.sync_all()?;
    Ok(())
}

fn open_new_lock_file(path: &Path) -> Result<fs::File, StoreLockError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true).mode(0o600);
        Ok(options.open(path)?)
    }
    #[cfg(not(unix))]
    {
        Ok(fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?)
    }
}

fn open_existing_lock_file(path: &Path) -> Result<fs::File, StoreLockError> {
    reject_symlink(path)?;
    Ok(fs::OpenOptions::new().write(true).truncate(true).open(path)?)
}

fn set_dir_permissions(path: &Path, mode: u32) -> Result<(), StoreLockError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perm = fs::Permissions::from_mode(mode);
        fs::set_permissions(path, perm)?;
    }
    Ok(())
}

fn set_file_permissions(path: &Path, mode: u32) -> Result<(), StoreLockError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perm = fs::Permissions::from_mode(mode);
        fs::set_permissions(path, perm)?;
    }
    Ok(())
}
