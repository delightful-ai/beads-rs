#![allow(dead_code)]

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use beads_bootstrap::paths as moved;
use beads_core::StoreId;
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnlockOutcome {
    Removed,
    Missing,
}

#[derive(Debug, Error)]
pub enum UnlockStoreError {
    #[error("store lock path is a symlink: {path:?}")]
    Symlink { path: PathBuf },
    #[error("io error while unlocking {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

pub fn unlock_store(data_dir: &Path, store_id: StoreId) -> Result<UnlockOutcome, UnlockStoreError> {
    let path = moved::store_lock_path(data_dir, store_id);
    let removed = match fs::symlink_metadata(&path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(UnlockStoreError::Symlink { path });
        }
        Ok(_) => {
            fs::remove_file(&path).map_err(|source| UnlockStoreError::Io {
                path: path.clone(),
                source,
            })?;
            true
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => false,
        Err(source) => return Err(UnlockStoreError::Io { path, source }),
    };
    Ok(if removed {
        UnlockOutcome::Removed
    } else {
        UnlockOutcome::Missing
    })
}
