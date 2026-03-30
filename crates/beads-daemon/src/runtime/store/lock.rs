//! Store lock handling and metadata persistence.

use std::fs;
use std::io::{self, Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::core::error::details as error_details;
use crate::core::{
    ErrorCode, ErrorPayload, IntoErrorPayload, ProtocolErrorCode, ReplicaId, StoreId, Transience,
};
use crate::layout::DaemonLayout;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreLockMeta {
    pub store_id: StoreId,
    pub replica_id: ReplicaId,
    pub pid: u32,
    pub started_at_ms: u64,
    pub daemon_version: String,
    #[serde(default)]
    pub lease_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_token: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_ms: Option<u64>,
}

pub(crate) const STORE_LOCK_LEASE_TIMEOUT_MS: u64 = 30_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreLockOperation {
    Read,
    Write,
    Fsync,
}

impl StoreLockMeta {
    pub fn new(
        store_id: StoreId,
        replica_id: ReplicaId,
        started_at_ms: u64,
        lease_epoch: u64,
        daemon_version: impl Into<String>,
    ) -> Self {
        Self {
            store_id,
            replica_id,
            pid: std::process::id(),
            started_at_ms,
            daemon_version: daemon_version.into(),
            lease_epoch,
            lease_token: Some(Uuid::new_v4()),
            last_heartbeat_ms: Some(started_at_ms),
        }
    }

    fn heartbeat_reference_ms(&self) -> u64 {
        self.last_heartbeat_ms.unwrap_or(self.started_at_ms)
    }

    pub(crate) fn lease_is_fresh(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.heartbeat_reference_ms()) <= STORE_LOCK_LEASE_TIMEOUT_MS
    }

    fn owner_matches(&self, other: &Self) -> bool {
        if self.lease_token.is_none() && other.lease_token.is_none() {
            return self.lease_epoch == other.lease_epoch
                && self.pid == other.pid
                && self.replica_id == other.replica_id
                && self.started_at_ms == other.started_at_ms;
        }
        self.lease_epoch == other.lease_epoch
            && self.lease_token.is_some()
            && self.lease_token == other.lease_token
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
        layout: &DaemonLayout,
        store_id: StoreId,
        replica_id: ReplicaId,
        started_at_ms: u64,
        daemon_version: impl Into<String>,
    ) -> Result<Self, StoreLockError> {
        Self::acquire_with_pid_check(
            layout,
            store_id,
            replica_id,
            started_at_ms,
            daemon_version,
            pid_state_for_lock,
        )
    }

    fn acquire_with_pid_check<F>(
        layout: &DaemonLayout,
        store_id: StoreId,
        replica_id: ReplicaId,
        started_at_ms: u64,
        daemon_version: impl Into<String>,
        check_pid: F,
    ) -> Result<Self, StoreLockError>
    where
        F: FnOnce(u32) -> LockPidState,
    {
        ensure_dir(&layout.stores_dir)?;
        let store_dir = layout.store_dir(&store_id);
        ensure_dir(&store_dir)?;

        let path = layout.store_lock_path(&store_id);
        reject_symlink(&path)?;
        with_store_lock_path_guard(&path, || {
            let mut next_lease_epoch = 1;
            let mut file = match open_new_lock_file(&path) {
                Ok(file) => file,
                Err(StoreLockError::Io { source, .. })
                    if source.kind() == io::ErrorKind::AlreadyExists =>
                {
                    loop {
                        let mut current = match open_current_lock_file(&path)? {
                            Some(current) => current,
                            None => match open_new_lock_file(&path) {
                                Ok(file) => break file,
                                Err(StoreLockError::Io { source, .. })
                                    if source.kind() == io::ErrorKind::AlreadyExists =>
                                {
                                    continue;
                                }
                                Err(err) => return Err(err),
                            },
                        };
                        let (held_meta, held_meta_error) = match current.read_metadata(&path) {
                            Ok(meta) => (Some(meta), None),
                            Err(err) => (None, Some(err.to_string())),
                        };
                        let Some(held_meta) = held_meta else {
                            return Err(StoreLockError::Held {
                                store_id,
                                path: Box::new(path.clone()),
                                meta: None,
                                meta_error: held_meta_error,
                            });
                        };
                        if !current.matches_current_path(&path)? {
                            drop(current);
                            match open_new_lock_file(&path) {
                                Ok(file) => break file,
                                Err(StoreLockError::Io { source, .. })
                                    if source.kind() == io::ErrorKind::AlreadyExists =>
                                {
                                    return Err(held_error_from_snapshot(
                                        store_id,
                                        &path,
                                        current_path_snapshot(&path),
                                    ));
                                }
                                Err(err) => return Err(err),
                            }
                        }

                        let pid_state = check_pid(held_meta.pid);
                        let reclaimable = matches!(pid_state, LockPidState::Missing)
                            || !held_meta.lease_is_fresh(started_at_ms);
                        if reclaimable {
                            #[cfg(test)]
                            maybe_run_lock_mutation_hook(
                                &path,
                                LockMutationHookStage::BeforeReclaimRemove,
                            );
                            if !current.matches_current_path(&path)? {
                                drop(current);
                                match open_new_lock_file(&path) {
                                    Ok(file) => break file,
                                    Err(StoreLockError::Io { source, .. })
                                        if source.kind() == io::ErrorKind::AlreadyExists =>
                                    {
                                        return Err(held_error_from_snapshot(
                                            store_id,
                                            &path,
                                            current_path_snapshot(&path),
                                        ));
                                    }
                                    Err(err) => return Err(err),
                                }
                            }
                            next_lease_epoch = held_meta.lease_epoch.saturating_add(1).max(1);
                            remove_lock_path_if_present(&path)?;
                            tracing::warn!(
                                store_id = %store_id,
                                lock_path = %path.display(),
                                stale_pid = held_meta.pid,
                                previous_lease_epoch = held_meta.lease_epoch,
                                previous_last_heartbeat_ms = held_meta.last_heartbeat_ms,
                                "reclaimed stale store lock"
                            );
                            drop(current);
                            match open_new_lock_file(&path) {
                                Ok(file) => break file,
                                Err(StoreLockError::Io { source, .. })
                                    if source.kind() == io::ErrorKind::AlreadyExists =>
                                {
                                    return Err(held_error_from_snapshot(
                                        store_id,
                                        &path,
                                        current_path_snapshot(&path),
                                    ));
                                }
                                Err(err) => return Err(err),
                            }
                        } else {
                            return Err(StoreLockError::Held {
                                store_id,
                                path: Box::new(path.clone()),
                                meta: Some(Box::new(held_meta)),
                                meta_error: held_meta_error,
                            });
                        }
                    }
                }
                Err(err) => return Err(err),
            };
            let meta = StoreLockMeta::new(
                store_id,
                replica_id,
                started_at_ms,
                next_lease_epoch,
                daemon_version,
            );

            write_metadata(&mut file, &path, &meta)?;
            set_file_permissions(&path, 0o600)?;

            Ok(Self {
                path: path.clone(),
                meta,
                released: false,
            })
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
        with_store_lock_path_guard(&self.path, || {
            let mut file = open_owned_current_lock_file(&self.path, &self.meta)?;
            #[cfg(test)]
            maybe_run_lock_mutation_hook(&self.path, LockMutationHookStage::BeforeHeartbeatWrite);
            if !file_matches_current_path(&file, &self.path)? {
                return Err(held_error_from_snapshot(
                    self.meta.store_id,
                    &self.path,
                    current_path_snapshot(&self.path),
                ));
            }
            rewrite_metadata(&mut file, &self.path, &self.meta)?;
            set_file_permissions(&self.path, 0o600)?;
            Ok(())
        })
    }

    pub fn release(mut self) -> Result<(), StoreLockError> {
        if !self.released {
            with_store_lock_path_guard(&self.path, || {
                remove_if_owner_matches(&self.path, &self.meta)
            })?;
            self.released = true;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LockPidState {
    Missing,
    Alive,
    Unknown,
}

fn pid_state_for_lock(pid: u32) -> LockPidState {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    match kill(Pid::from_raw(pid as i32), None) {
        Ok(()) | Err(Errno::EPERM) => LockPidState::Alive,
        Err(Errno::ESRCH) => LockPidState::Missing,
        Err(err) => {
            tracing::debug!(%err, pid, "store lock pid check returned unexpected error");
            LockPidState::Unknown
        }
    }
}

impl Drop for StoreLock {
    fn drop(&mut self) {
        if !self.released {
            let _ = with_store_lock_path_guard(&self.path, || {
                remove_if_owner_matches(&self.path, &self.meta)
            });
            self.released = true;
        }
    }
}

pub fn read_lock_meta_with_layout(
    layout: &DaemonLayout,
    store_id: StoreId,
) -> Result<Option<StoreLockMeta>, StoreLockError> {
    let path = layout.store_lock_path(&store_id);
    match fs::symlink_metadata(&path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(StoreLockError::Symlink { path }),
        Ok(_) => Ok(Some(read_metadata(&path)?)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(StoreLockError::Io {
            path,
            operation: StoreLockOperation::Read,
            source: err,
        }),
    }
}

pub fn read_lock_meta(store_id: StoreId) -> Result<Option<StoreLockMeta>, StoreLockError> {
    let layout = crate::daemon_layout_from_paths();
    read_lock_meta_with_layout(&layout, store_id)
}

pub fn remove_lock_file_with_layout(
    layout: &DaemonLayout,
    store_id: StoreId,
) -> Result<bool, StoreLockError> {
    let path = layout.store_lock_path(&store_id);
    with_store_lock_path_guard(&path, || match fs::symlink_metadata(&path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            Err(StoreLockError::Symlink { path: path.clone() })
        }
        Ok(_) => {
            fs::remove_file(&path).map_err(|source| StoreLockError::Io {
                path: path.clone(),
                operation: StoreLockOperation::Write,
                source,
            })?;
            Ok(true)
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(StoreLockError::Io {
            path: path.clone(),
            operation: StoreLockOperation::Read,
            source: err,
        }),
    })
}

pub fn remove_lock_file_if_meta_matches_with_layout(
    layout: &DaemonLayout,
    owner: &StoreLockMeta,
) -> Result<bool, StoreLockError> {
    let path = layout.store_lock_path(&owner.store_id);
    with_store_lock_path_guard(&path, || match fs::symlink_metadata(&path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            Err(StoreLockError::Symlink { path: path.clone() })
        }
        Ok(_) => {
            remove_if_owner_matches(&path, owner)?;
            Ok(true)
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(StoreLockError::Io {
            path: path.clone(),
            operation: StoreLockOperation::Read,
            source: err,
        }),
    })
}

pub fn remove_lock_file(store_id: StoreId) -> Result<bool, StoreLockError> {
    let layout = crate::daemon_layout_from_paths();
    remove_lock_file_with_layout(&layout, store_id)
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
    #[error("io error while {operation:?} {path:?}: {source}")]
    Io {
        path: PathBuf,
        operation: StoreLockOperation,
        #[source]
        source: io::Error,
    },
}

impl StoreLockError {
    pub fn code(&self) -> ErrorCode {
        match self {
            StoreLockError::Held { .. } => ProtocolErrorCode::LockHeld.into(),
            StoreLockError::Symlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
            StoreLockError::MetadataCorrupt { .. } => ProtocolErrorCode::Corruption.into(),
            StoreLockError::Io { source, .. } => {
                if source.kind() == io::ErrorKind::PermissionDenied {
                    ProtocolErrorCode::PermissionDenied.into()
                } else {
                    ProtocolErrorCode::InternalError.into()
                }
            }
        }
    }

    pub fn transience(&self) -> Transience {
        match self {
            StoreLockError::Held { .. }
            | StoreLockError::Symlink { .. }
            | StoreLockError::MetadataCorrupt { .. } => Transience::Permanent,
            StoreLockError::Io { source, .. } => {
                if source.kind() == io::ErrorKind::PermissionDenied {
                    Transience::Permanent
                } else {
                    Transience::Retryable
                }
            }
        }
    }
}

impl IntoErrorPayload for StoreLockError {
    fn into_error_payload(self) -> ErrorPayload {
        let message = self.to_string();
        let retryable = self.transience().is_retryable();
        match self {
            StoreLockError::Held { store_id, meta, .. } => {
                let (holder_pid, holder_replica_id, started_at_ms, daemon_version) = meta
                    .as_deref()
                    .map(|meta| {
                        (
                            Some(meta.pid),
                            Some(meta.replica_id),
                            Some(meta.started_at_ms),
                            Some(meta.daemon_version.clone()),
                        )
                    })
                    .unwrap_or((None, None, None, None));
                ErrorPayload::new(ProtocolErrorCode::LockHeld.into(), message, retryable)
                    .with_details(error_details::LockHeldDetails {
                        store_id,
                        holder_pid,
                        holder_replica_id,
                        started_at_ms,
                        daemon_version,
                    })
            }
            StoreLockError::Symlink { path } => ErrorPayload::new(
                ProtocolErrorCode::PathSymlinkRejected.into(),
                message,
                retryable,
            )
            .with_details(error_details::PathSymlinkRejectedDetails {
                path: path.display().to_string(),
            }),
            StoreLockError::MetadataCorrupt { source, .. } => {
                ErrorPayload::new(ProtocolErrorCode::Corruption.into(), message, retryable)
                    .with_details(error_details::CorruptionDetails {
                        reason: source.to_string(),
                    })
            }
            StoreLockError::Io {
                path,
                operation,
                source,
            } => match source.kind() {
                io::ErrorKind::PermissionDenied => ErrorPayload::new(
                    ProtocolErrorCode::PermissionDenied.into(),
                    message,
                    retryable,
                )
                .with_details(error_details::PermissionDeniedDetails {
                    path: path.display().to_string(),
                    operation: lock_permission_operation(operation),
                }),
                _ => ErrorPayload::new(ProtocolErrorCode::InternalError.into(), message, retryable),
            },
        }
    }
}

fn lock_permission_operation(operation: StoreLockOperation) -> error_details::PermissionOperation {
    match operation {
        StoreLockOperation::Read => error_details::PermissionOperation::Read,
        StoreLockOperation::Write => error_details::PermissionOperation::Write,
        StoreLockOperation::Fsync => error_details::PermissionOperation::Fsync,
    }
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
                return Err(StoreLockError::Io {
                    path: path.to_path_buf(),
                    operation: StoreLockOperation::Write,
                    source: io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!("expected directory at {:?}", path),
                    ),
                });
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|source| StoreLockError::Io {
                path: path.to_path_buf(),
                operation: StoreLockOperation::Write,
                source,
            })?;
        }
        Err(err) => {
            return Err(StoreLockError::Io {
                path: path.to_path_buf(),
                operation: StoreLockOperation::Read,
                source: err,
            });
        }
    }
    set_dir_permissions(path, 0o700)?;
    Ok(())
}

fn reject_symlink(path: &Path) -> Result<(), StoreLockError> {
    if let Ok(meta) = fs::symlink_metadata(path)
        && meta.file_type().is_symlink()
    {
        return Err(StoreLockError::Symlink {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn read_metadata(path: &Path) -> Result<StoreLockMeta, StoreLockError> {
    reject_symlink(path)?;
    let bytes = fs::read(path).map_err(|source| StoreLockError::Io {
        path: path.to_path_buf(),
        operation: StoreLockOperation::Read,
        source,
    })?;
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
    file.sync_all().map_err(|source| StoreLockError::Io {
        path: path.to_path_buf(),
        operation: StoreLockOperation::Fsync,
        source,
    })?;
    Ok(())
}

fn rewrite_metadata(
    file: &mut fs::File,
    path: &Path,
    meta: &StoreLockMeta,
) -> Result<(), StoreLockError> {
    file.set_len(0).map_err(|source| StoreLockError::Io {
        path: path.to_path_buf(),
        operation: StoreLockOperation::Write,
        source,
    })?;
    file.rewind().map_err(|source| StoreLockError::Io {
        path: path.to_path_buf(),
        operation: StoreLockOperation::Write,
        source,
    })?;
    write_metadata(file, path, meta)
}

fn open_new_lock_file(path: &Path) -> Result<fs::File, StoreLockError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true).mode(0o600);
        options.open(path).map_err(|source| StoreLockError::Io {
            path: path.to_path_buf(),
            operation: StoreLockOperation::Write,
            source,
        })
    }
    #[cfg(not(unix))]
    {
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|source| StoreLockError::Io {
                path: path.to_path_buf(),
                operation: StoreLockOperation::Write,
                source,
            })
    }
}

fn open_existing_lock_file(path: &Path) -> Result<fs::File, StoreLockError> {
    reject_symlink(path)?;
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|source| StoreLockError::Io {
            path: path.to_path_buf(),
            operation: StoreLockOperation::Write,
            source,
        })
}

struct CurrentLockFile {
    file: nix::fcntl::Flock<fs::File>,
}

impl CurrentLockFile {
    fn read_metadata(&mut self, path: &Path) -> Result<StoreLockMeta, StoreLockError> {
        self.file.rewind().map_err(|source| StoreLockError::Io {
            path: path.to_path_buf(),
            operation: StoreLockOperation::Read,
            source,
        })?;
        let mut bytes = Vec::new();
        self.file
            .read_to_end(&mut bytes)
            .map_err(|source| StoreLockError::Io {
                path: path.to_path_buf(),
                operation: StoreLockOperation::Read,
                source,
            })?;
        serde_json::from_slice(&bytes).map_err(|source| StoreLockError::MetadataCorrupt {
            path: path.to_path_buf(),
            source,
        })
    }

    fn matches_current_path(&self, path: &Path) -> Result<bool, StoreLockError> {
        file_matches_current_path(&self.file, path)
    }
}

fn open_current_lock_file(path: &Path) -> Result<Option<CurrentLockFile>, StoreLockError> {
    let file = match open_existing_lock_file(path) {
        Ok(file) => file,
        Err(StoreLockError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(err) => return Err(err),
    };
    let file = lock_file_exclusive(file, path)?;
    if !file_matches_current_path(&file, path)? {
        return Ok(None);
    }
    Ok(Some(CurrentLockFile { file }))
}

fn open_owned_current_lock_file(
    path: &Path,
    owner: &StoreLockMeta,
) -> Result<nix::fcntl::Flock<fs::File>, StoreLockError> {
    let mut current = match open_current_lock_file(path)? {
        Some(current) => current,
        None => {
            return Err(held_error_from_snapshot(
                owner.store_id,
                path,
                current_path_snapshot(path),
            ));
        }
    };
    let on_disk = current.read_metadata(path)?;
    if !on_disk.owner_matches(owner) {
        return Err(StoreLockError::Held {
            store_id: owner.store_id,
            path: Box::new(path.to_path_buf()),
            meta: Some(Box::new(on_disk)),
            meta_error: None,
        });
    }
    Ok(current.file)
}

fn lock_file_exclusive(
    file: fs::File,
    path: &Path,
) -> Result<nix::fcntl::Flock<fs::File>, StoreLockError> {
    nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusive).map_err(|(_file, errno)| {
        StoreLockError::Io {
            path: path.to_path_buf(),
            operation: StoreLockOperation::Write,
            source: io::Error::from_raw_os_error(errno as i32),
        }
    })
}

fn file_matches_current_path(file: &fs::File, path: &Path) -> Result<bool, StoreLockError> {
    let current = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(StoreLockError::Io {
                path: path.to_path_buf(),
                operation: StoreLockOperation::Read,
                source,
            });
        }
    };
    if current.file_type().is_symlink() {
        return Err(StoreLockError::Symlink {
            path: path.to_path_buf(),
        });
    }
    let held = file.metadata().map_err(|source| StoreLockError::Io {
        path: path.to_path_buf(),
        operation: StoreLockOperation::Read,
        source,
    })?;
    Ok(same_file_identity(&held, &current))
}

#[cfg(unix)]
fn same_file_identity(held: &fs::Metadata, current: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    held.dev() == current.dev() && held.ino() == current.ino()
}

#[cfg(not(unix))]
fn same_file_identity(_held: &fs::Metadata, _current: &fs::Metadata) -> bool {
    true
}

fn remove_lock_path_if_present(path: &Path) -> Result<(), StoreLockError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(StoreLockError::Io {
            path: path.to_path_buf(),
            operation: StoreLockOperation::Write,
            source,
        }),
    }
}

const STORE_LOCK_PATH_GUARD_STRIPES: usize = 64;

fn store_lock_path_guards() -> &'static [Mutex<()>] {
    static GUARDS: OnceLock<Box<[Mutex<()>]>> = OnceLock::new();
    GUARDS.get_or_init(|| {
        (0..STORE_LOCK_PATH_GUARD_STRIPES)
            .map(|_| Mutex::new(()))
            .collect::<Vec<_>>()
            .into_boxed_slice()
    })
}

fn lock_store_lock_path(path: &Path) -> MutexGuard<'static, ()> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    let stripe = (hasher.finish() as usize) % STORE_LOCK_PATH_GUARD_STRIPES;
    store_lock_path_guards()[stripe]
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

fn with_store_lock_path_guard<T>(
    path: &Path,
    f: impl FnOnce() -> Result<T, StoreLockError>,
) -> Result<T, StoreLockError> {
    let guard = lock_store_lock_path(path);
    let result = f();
    drop(guard);
    result
}

enum CurrentPathSnapshot {
    Missing,
    Present {
        meta: Option<Box<StoreLockMeta>>,
        meta_error: Option<String>,
    },
}

fn current_path_snapshot(path: &Path) -> CurrentPathSnapshot {
    match read_metadata(path) {
        Ok(meta) => CurrentPathSnapshot::Present {
            meta: Some(Box::new(meta)),
            meta_error: None,
        },
        Err(StoreLockError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
            CurrentPathSnapshot::Missing
        }
        Err(err) => CurrentPathSnapshot::Present {
            meta: None,
            meta_error: Some(err.to_string()),
        },
    }
}

fn held_error_from_snapshot(
    store_id: StoreId,
    path: &Path,
    snapshot: CurrentPathSnapshot,
) -> StoreLockError {
    let (meta, meta_error) = match snapshot {
        CurrentPathSnapshot::Missing => (None, None),
        CurrentPathSnapshot::Present { meta, meta_error } => (meta, meta_error),
    };
    StoreLockError::Held {
        store_id,
        path: Box::new(path.to_path_buf()),
        meta,
        meta_error,
    }
}

fn remove_if_owner_matches(path: &Path, owner: &StoreLockMeta) -> Result<(), StoreLockError> {
    let mut current_file = match open_current_lock_file(path)? {
        Some(current) => current,
        None => {
            return match current_path_snapshot(path) {
                CurrentPathSnapshot::Missing => Ok(()),
                snapshot @ CurrentPathSnapshot::Present { .. } => {
                    Err(held_error_from_snapshot(owner.store_id, path, snapshot))
                }
            };
        }
    };
    let current = current_file.read_metadata(path)?;
    if !current.owner_matches(owner) {
        return Err(StoreLockError::Held {
            store_id: owner.store_id,
            path: Box::new(path.to_path_buf()),
            meta: Some(Box::new(current)),
            meta_error: None,
        });
    }
    #[cfg(test)]
    maybe_run_lock_mutation_hook(path, LockMutationHookStage::BeforeOwnerMatchedRemove);
    if !current_file.matches_current_path(path)? {
        return match current_path_snapshot(path) {
            CurrentPathSnapshot::Missing => Ok(()),
            snapshot @ CurrentPathSnapshot::Present { .. } => {
                Err(held_error_from_snapshot(owner.store_id, path, snapshot))
            }
        };
    }
    remove_lock_path_if_present(path)
}

fn set_dir_permissions(path: &Path, mode: u32) -> Result<(), StoreLockError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perm = fs::Permissions::from_mode(mode);
        fs::set_permissions(path, perm).map_err(|source| StoreLockError::Io {
            path: path.to_path_buf(),
            operation: StoreLockOperation::Write,
            source,
        })?;
    }
    Ok(())
}

fn set_file_permissions(path: &Path, mode: u32) -> Result<(), StoreLockError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perm = fs::Permissions::from_mode(mode);
        fs::set_permissions(path, perm).map_err(|source| StoreLockError::Io {
            path: path.to_path_buf(),
            operation: StoreLockOperation::Write,
            source,
        })?;
    }
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LockMutationHookStage {
    BeforeReclaimRemove,
    BeforeHeartbeatWrite,
    BeforeOwnerMatchedRemove,
}

#[cfg(test)]
type LockMutationHookFn = Box<dyn FnOnce() + Send>;

#[cfg(test)]
struct LockMutationHook {
    path: PathBuf,
    stage: LockMutationHookStage,
    hook: LockMutationHookFn,
}

#[cfg(test)]
static LOCK_MUTATION_HOOK: std::sync::LazyLock<std::sync::Mutex<Option<LockMutationHook>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(None));

#[cfg(test)]
static LOCK_MUTATION_HOOK_INSTALL_LOCK: std::sync::LazyLock<std::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

#[cfg(test)]
struct LockMutationHookGuard {
    _install_guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for LockMutationHookGuard {
    fn drop(&mut self) {
        *lock_mutation_hook() = None;
    }
}

#[cfg(test)]
fn scoped_lock_mutation_hook(
    path: PathBuf,
    stage: LockMutationHookStage,
    hook: impl FnOnce() + Send + 'static,
) -> LockMutationHookGuard {
    let install_guard = lock_mutation_hook_install();
    let mut slot = lock_mutation_hook();
    assert!(slot.is_none(), "lock mutation hook already installed");
    *slot = Some(LockMutationHook {
        path,
        stage,
        hook: Box::new(hook),
    });
    drop(slot);
    LockMutationHookGuard {
        _install_guard: install_guard,
    }
}

#[cfg(test)]
fn lock_mutation_hook() -> std::sync::MutexGuard<'static, Option<LockMutationHook>> {
    LOCK_MUTATION_HOOK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

#[cfg(test)]
fn lock_mutation_hook_install() -> std::sync::MutexGuard<'static, ()> {
    LOCK_MUTATION_HOOK_INSTALL_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

#[cfg(test)]
fn maybe_run_lock_mutation_hook(path: &Path, stage: LockMutationHookStage) {
    let maybe_hook = {
        let mut slot = lock_mutation_hook();
        match slot.as_ref() {
            Some(installed) if installed.path == path && installed.stage == stage => slot.take(),
            _ => None,
        }
    };
    if let Some(hook) = maybe_hook {
        (hook.hook)();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;
    use crate::paths;
    use tempfile::TempDir;
    use uuid::Uuid;

    fn with_test_data_dir<F>(f: F)
    where
        F: FnOnce(&TempDir),
    {
        let temp = TempDir::new().unwrap();
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));
        f(&temp);
    }

    fn write_lock_meta(path: &Path, meta: &StoreLockMeta) {
        std::fs::create_dir_all(path.parent().expect("lock parent")).unwrap();
        let data = serde_json::to_vec(meta).unwrap();
        std::fs::write(path, data).unwrap();
    }

    fn write_legacy_lock_meta(
        path: &Path,
        store_id: StoreId,
        replica_id: ReplicaId,
        pid: u32,
        started_at_ms: u64,
        daemon_version: &str,
        last_heartbeat_ms: Option<u64>,
    ) {
        std::fs::create_dir_all(path.parent().expect("lock parent")).unwrap();
        let mut value = serde_json::json!({
            "store_id": store_id,
            "replica_id": replica_id,
            "pid": pid,
            "started_at_ms": started_at_ms,
            "daemon_version": daemon_version,
        });
        if let Some(last_heartbeat_ms) = last_heartbeat_ms {
            value["last_heartbeat_ms"] = serde_json::json!(last_heartbeat_ms);
        }
        std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    }

    #[test]
    fn acquire_reclaims_stale_lock_when_pid_missing() {
        with_test_data_dir(|_| {
            let store_id = StoreId::new(Uuid::from_bytes([11u8; 16]));
            let stale_meta = StoreLockMeta {
                store_id,
                replica_id: ReplicaId::new(Uuid::from_bytes([12u8; 16])),
                pid: 4242,
                started_at_ms: 1,
                daemon_version: "old".to_string(),
                lease_epoch: 3,
                lease_token: Some(Uuid::from_bytes([14u8; 16])),
                last_heartbeat_ms: Some(2),
            };
            let lock_path = paths::store_lock_path(store_id);
            write_lock_meta(&lock_path, &stale_meta);
            let layout = crate::daemon_layout_from_paths();

            let lock = StoreLock::acquire_with_pid_check(
                &layout,
                store_id,
                ReplicaId::new(Uuid::from_bytes([13u8; 16])),
                10,
                "new",
                |_| LockPidState::Missing,
            )
            .expect("acquire should reclaim stale lock");

            let on_disk = read_lock_meta(store_id)
                .expect("read lock")
                .expect("meta exists");
            assert_eq!(on_disk.store_id, store_id);
            assert_eq!(on_disk.pid, std::process::id());
            assert_ne!(on_disk.pid, stale_meta.pid);
            assert_eq!(on_disk.lease_epoch, stale_meta.lease_epoch + 1);
            lock.release().unwrap();
        });
    }

    #[test]
    fn acquire_keeps_live_lock_when_pid_alive() {
        with_test_data_dir(|_| {
            let store_id = StoreId::new(Uuid::from_bytes([21u8; 16]));
            let stale_meta = StoreLockMeta {
                store_id,
                replica_id: ReplicaId::new(Uuid::from_bytes([22u8; 16])),
                pid: 5151,
                started_at_ms: 1,
                daemon_version: "live".to_string(),
                lease_epoch: 7,
                lease_token: Some(Uuid::from_bytes([24u8; 16])),
                last_heartbeat_ms: Some(2),
            };
            let lock_path = paths::store_lock_path(store_id);
            write_lock_meta(&lock_path, &stale_meta);
            let layout = crate::daemon_layout_from_paths();

            let err = StoreLock::acquire_with_pid_check(
                &layout,
                store_id,
                ReplicaId::new(Uuid::from_bytes([23u8; 16])),
                10,
                "new",
                |_| LockPidState::Alive,
            )
            .expect_err("live lock must be preserved");
            match err {
                StoreLockError::Held { meta, .. } => {
                    assert_eq!(meta.expect("holder meta").pid, stale_meta.pid);
                }
                other => panic!("unexpected error: {other}"),
            }
        });
    }

    #[test]
    fn acquire_reclaims_expired_lock_even_when_pid_looks_alive() {
        with_test_data_dir(|_| {
            let store_id = StoreId::new(Uuid::from_bytes([31u8; 16]));
            let stale_meta = StoreLockMeta {
                store_id,
                replica_id: ReplicaId::new(Uuid::from_bytes([32u8; 16])),
                pid: 6262,
                started_at_ms: 1,
                daemon_version: "old".to_string(),
                lease_epoch: 9,
                lease_token: Some(Uuid::from_bytes([33u8; 16])),
                last_heartbeat_ms: Some(1),
            };
            let lock_path = paths::store_lock_path(store_id);
            write_lock_meta(&lock_path, &stale_meta);
            let layout = crate::daemon_layout_from_paths();

            let lock = StoreLock::acquire_with_pid_check(
                &layout,
                store_id,
                ReplicaId::new(Uuid::from_bytes([34u8; 16])),
                STORE_LOCK_LEASE_TIMEOUT_MS + 10,
                "new",
                |_| LockPidState::Alive,
            )
            .expect("expired lease should be reclaimable");

            let on_disk = read_lock_meta(store_id)
                .expect("read lock")
                .expect("meta exists");
            assert_eq!(on_disk.lease_epoch, stale_meta.lease_epoch + 1);
            lock.release().unwrap();
        });
    }

    #[test]
    fn acquire_reclaim_rechecks_path_before_remove() {
        with_test_data_dir(|_| {
            let layout = crate::daemon_layout_from_paths();
            let store_id = StoreId::new(Uuid::from_bytes([35u8; 16]));
            let stale_meta = StoreLockMeta {
                store_id,
                replica_id: ReplicaId::new(Uuid::from_bytes([36u8; 16])),
                pid: 7373,
                started_at_ms: 1,
                daemon_version: "old".to_string(),
                lease_epoch: 5,
                lease_token: Some(Uuid::from_bytes([37u8; 16])),
                last_heartbeat_ms: Some(1),
            };
            let lock_path = paths::store_lock_path(store_id);
            write_lock_meta(&lock_path, &stale_meta);

            let replacement_meta = StoreLockMeta {
                store_id,
                replica_id: ReplicaId::new(Uuid::from_bytes([38u8; 16])),
                pid: 8383,
                started_at_ms: 200,
                daemon_version: "new".to_string(),
                lease_epoch: stale_meta.lease_epoch + 1,
                lease_token: Some(Uuid::from_bytes([39u8; 16])),
                last_heartbeat_ms: Some(200),
            };
            let replacement_path = lock_path.clone();
            let replacement_meta_for_hook = replacement_meta.clone();
            let _hook = scoped_lock_mutation_hook(
                lock_path,
                LockMutationHookStage::BeforeReclaimRemove,
                move || {
                    std::fs::remove_file(&replacement_path).expect("remove stale lock path");
                    write_lock_meta(&replacement_path, &replacement_meta_for_hook);
                },
            );

            let err = StoreLock::acquire_with_pid_check(
                &layout,
                store_id,
                ReplicaId::new(Uuid::from_bytes([40u8; 16])),
                STORE_LOCK_LEASE_TIMEOUT_MS + 10,
                "newer",
                |_| LockPidState::Alive,
            )
            .expect_err("reclaim must reject path swaps before unlink");
            assert!(matches!(err, StoreLockError::Held { .. }));

            let on_disk = read_lock_meta(store_id)
                .expect("read lock")
                .expect("replacement lock should remain on disk");
            assert_eq!(on_disk.lease_epoch, replacement_meta.lease_epoch);
            assert_eq!(on_disk.lease_token, replacement_meta.lease_token);
        });
    }

    #[test]
    fn stale_owner_cannot_overwrite_or_remove_reclaimed_lock() {
        with_test_data_dir(|_| {
            let layout = crate::daemon_layout_from_paths();
            let store_id = StoreId::new(Uuid::from_bytes([41u8; 16]));
            let replica_id = ReplicaId::new(Uuid::from_bytes([42u8; 16]));
            let mut old_lock = StoreLock::acquire(&layout, store_id, replica_id, 100, "old")
                .expect("acquire old lock");
            let old_meta = old_lock.meta().clone();

            let replacement_meta = StoreLockMeta {
                store_id,
                replica_id: ReplicaId::new(Uuid::from_bytes([43u8; 16])),
                pid: 7777,
                started_at_ms: 200,
                daemon_version: "new".to_string(),
                lease_epoch: old_meta.lease_epoch + 1,
                lease_token: Some(Uuid::from_bytes([44u8; 16])),
                last_heartbeat_ms: Some(200),
            };
            write_lock_meta(old_lock.path(), &replacement_meta);

            let heartbeat_err = old_lock
                .update_heartbeat(201)
                .expect_err("stale owner heartbeat must not clobber replacement");
            assert!(matches!(heartbeat_err, StoreLockError::Held { .. }));
            let release_err = old_lock
                .release()
                .expect_err("stale owner release must not remove replacement");
            assert!(matches!(release_err, StoreLockError::Held { .. }));

            let on_disk = read_lock_meta(store_id)
                .expect("read lock")
                .expect("meta exists");
            assert_eq!(on_disk.lease_epoch, replacement_meta.lease_epoch);
            assert_eq!(on_disk.lease_token, replacement_meta.lease_token);
        });
    }

    #[test]
    fn stale_release_rechecks_path_after_owner_match_before_remove() {
        with_test_data_dir(|_| {
            let layout = crate::daemon_layout_from_paths();
            let store_id = StoreId::new(Uuid::from_bytes([45u8; 16]));
            let replica_id = ReplicaId::new(Uuid::from_bytes([46u8; 16]));
            let old_lock = StoreLock::acquire(&layout, store_id, replica_id, 100, "old")
                .expect("acquire old lock");
            let old_path = old_lock.path().to_path_buf();

            let replacement_meta = StoreLockMeta {
                store_id,
                replica_id: ReplicaId::new(Uuid::from_bytes([47u8; 16])),
                pid: 8888,
                started_at_ms: 200,
                daemon_version: "new".to_string(),
                lease_epoch: old_lock.meta().lease_epoch + 1,
                lease_token: Some(Uuid::from_bytes([48u8; 16])),
                last_heartbeat_ms: Some(200),
            };
            let replacement_path = old_path.clone();
            let replacement_meta_for_hook = replacement_meta.clone();
            let _hook = scoped_lock_mutation_hook(
                old_path,
                LockMutationHookStage::BeforeOwnerMatchedRemove,
                move || {
                    std::fs::remove_file(&replacement_path).expect("remove old lock path");
                    write_lock_meta(&replacement_path, &replacement_meta_for_hook);
                },
            );

            let release_err = old_lock
                .release()
                .expect_err("stale release must reject path swaps after owner match");
            assert!(matches!(release_err, StoreLockError::Held { .. }));

            let on_disk = read_lock_meta(store_id)
                .expect("read lock")
                .expect("replacement lock should remain on disk");
            assert_eq!(on_disk.lease_epoch, replacement_meta.lease_epoch);
            assert_eq!(on_disk.lease_token, replacement_meta.lease_token);
        });
    }

    #[test]
    fn stale_heartbeat_rechecks_path_after_owner_match_before_write() {
        with_test_data_dir(|_| {
            let layout = crate::daemon_layout_from_paths();
            let store_id = StoreId::new(Uuid::from_bytes([49u8; 16]));
            let replica_id = ReplicaId::new(Uuid::from_bytes([50u8; 16]));
            let mut old_lock = StoreLock::acquire(&layout, store_id, replica_id, 100, "old")
                .expect("acquire old lock");
            let old_path = old_lock.path().to_path_buf();

            let replacement_meta = StoreLockMeta {
                store_id,
                replica_id: ReplicaId::new(Uuid::from_bytes([51u8; 16])),
                pid: 9999,
                started_at_ms: 200,
                daemon_version: "new".to_string(),
                lease_epoch: old_lock.meta().lease_epoch + 1,
                lease_token: Some(Uuid::from_bytes([52u8; 16])),
                last_heartbeat_ms: Some(200),
            };
            let replacement_path = old_path.clone();
            let replacement_meta_for_hook = replacement_meta.clone();
            let _hook = scoped_lock_mutation_hook(
                old_path,
                LockMutationHookStage::BeforeHeartbeatWrite,
                move || {
                    std::fs::remove_file(&replacement_path).expect("remove old lock path");
                    write_lock_meta(&replacement_path, &replacement_meta_for_hook);
                },
            );

            let heartbeat_err = old_lock
                .update_heartbeat(201)
                .expect_err("stale heartbeat must reject path swaps after owner match");
            assert!(matches!(heartbeat_err, StoreLockError::Held { .. }));

            let on_disk = read_lock_meta(store_id)
                .expect("read lock")
                .expect("replacement lock should remain on disk");
            assert_eq!(on_disk.lease_epoch, replacement_meta.lease_epoch);
            assert_eq!(on_disk.lease_token, replacement_meta.lease_token);
        });
    }

    #[test]
    fn stale_release_cannot_delete_reclaimer_lock_between_owner_check_and_remove() {
        with_test_data_dir(|_| {
            let layout = crate::daemon_layout_from_paths();
            let store_id = StoreId::new(Uuid::from_bytes([61u8; 16]));
            let replica_id = ReplicaId::new(Uuid::from_bytes([62u8; 16]));
            let old_lock = StoreLock::acquire(&layout, store_id, replica_id, 100, "old")
                .expect("acquire old lock");
            let old_path = old_lock.path().to_path_buf();
            let replacement_layout = layout.clone();

            let (pause_tx, pause_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();
            let _hook = scoped_lock_mutation_hook(
                old_path,
                LockMutationHookStage::BeforeOwnerMatchedRemove,
                move || {
                    pause_tx.send(()).expect("signal paused");
                    resume_rx.recv().expect("resume release");
                },
            );

            let release_handle = std::thread::spawn(move || old_lock.release());
            pause_rx.recv().expect("wait for release pause");

            let (replacement_done_tx, replacement_done_rx) = mpsc::channel();
            let replacement_handle = std::thread::spawn(move || {
                let result = StoreLock::acquire_with_pid_check(
                    &replacement_layout,
                    store_id,
                    ReplicaId::new(Uuid::from_bytes([63u8; 16])),
                    STORE_LOCK_LEASE_TIMEOUT_MS + 200,
                    "new",
                    |_| LockPidState::Alive,
                );
                replacement_done_tx
                    .send(())
                    .expect("signal replacement completion");
                result
            });

            assert!(
                replacement_done_rx
                    .recv_timeout(Duration::from_millis(100))
                    .is_err(),
                "replacement should block until stale release finishes"
            );

            resume_tx.send(()).expect("resume release");
            release_handle
                .join()
                .expect("join release")
                .expect("release should complete");
            let replacement = replacement_handle
                .join()
                .expect("join replacement acquire")
                .expect("replacement should acquire after stale release finishes");

            let on_disk = read_lock_meta(store_id)
                .expect("read lock")
                .expect("replacement lock should remain on disk");
            assert_eq!(on_disk.lease_token, replacement.meta().lease_token);
            assert_eq!(on_disk.lease_epoch, replacement.meta().lease_epoch);
            replacement.release().expect("release replacement");
        });
    }

    #[test]
    fn stale_heartbeat_cannot_clobber_reclaimer_lock_between_owner_check_and_write() {
        with_test_data_dir(|_| {
            let layout = crate::daemon_layout_from_paths();
            let store_id = StoreId::new(Uuid::from_bytes([71u8; 16]));
            let replica_id = ReplicaId::new(Uuid::from_bytes([72u8; 16]));
            let old_lock = StoreLock::acquire(&layout, store_id, replica_id, 100, "old")
                .expect("acquire old lock");
            let old_meta = old_lock.meta().clone();
            let old_path = old_lock.path().to_path_buf();
            let heartbeat_ms = STORE_LOCK_LEASE_TIMEOUT_MS + 200;
            let replacement_layout = layout.clone();

            let (pause_tx, pause_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();
            let _hook = scoped_lock_mutation_hook(
                old_path,
                LockMutationHookStage::BeforeHeartbeatWrite,
                move || {
                    pause_tx.send(()).expect("signal paused");
                    resume_rx.recv().expect("resume heartbeat");
                },
            );

            let heartbeat_handle = std::thread::spawn(move || {
                let mut lock = old_lock;
                let result = lock.update_heartbeat(heartbeat_ms);
                (lock, result)
            });
            pause_rx.recv().expect("wait for heartbeat pause");

            let (replacement_done_tx, replacement_done_rx) = mpsc::channel();
            let replacement_handle = std::thread::spawn(move || {
                let result = StoreLock::acquire_with_pid_check(
                    &replacement_layout,
                    store_id,
                    ReplicaId::new(Uuid::from_bytes([73u8; 16])),
                    heartbeat_ms + 1,
                    "new",
                    |_| LockPidState::Alive,
                );
                replacement_done_tx
                    .send(())
                    .expect("signal replacement completion");
                result
            });

            assert!(
                replacement_done_rx
                    .recv_timeout(Duration::from_millis(100))
                    .is_err(),
                "reclaimer should block while stale heartbeat holds the current lock"
            );

            resume_tx.send(()).expect("resume heartbeat");
            let (old_lock, heartbeat_result) = heartbeat_handle.join().expect("join heartbeat");
            heartbeat_result.expect("stale heartbeat unexpectedly failed");
            let replacement = replacement_handle.join().expect("join replacement acquire");
            assert!(
                matches!(replacement, Err(StoreLockError::Held { .. })),
                "fresh heartbeat should keep the reclaimer out"
            );

            let on_disk = read_lock_meta(store_id)
                .expect("read lock")
                .expect("original lock should remain on disk");
            assert_eq!(on_disk.lease_token, old_meta.lease_token);
            assert_eq!(on_disk.lease_epoch, old_meta.lease_epoch);
            assert_eq!(on_disk.last_heartbeat_ms, Some(heartbeat_ms));
            old_lock.release().expect("release old lock");
        });
    }

    #[test]
    fn acquire_reclaims_legacy_lock_file_and_upgrades_lease_fields() {
        with_test_data_dir(|_| {
            let store_id = StoreId::new(Uuid::from_bytes([51u8; 16]));
            let replica_id = ReplicaId::new(Uuid::from_bytes([52u8; 16]));
            let lock_path = paths::store_lock_path(store_id);
            write_legacy_lock_meta(&lock_path, store_id, replica_id, 9292, 1, "legacy", Some(1));
            let layout = crate::daemon_layout_from_paths();

            let lock = StoreLock::acquire_with_pid_check(
                &layout,
                store_id,
                ReplicaId::new(Uuid::from_bytes([53u8; 16])),
                STORE_LOCK_LEASE_TIMEOUT_MS + 10,
                "new",
                |_| LockPidState::Alive,
            )
            .expect("legacy lock should be reclaimable after expiry");

            let on_disk = read_lock_meta(store_id)
                .expect("read lock")
                .expect("meta exists");
            assert_eq!(on_disk.lease_epoch, 1);
            assert!(on_disk.lease_token.is_some());
            lock.release().unwrap();
        });
    }
}
