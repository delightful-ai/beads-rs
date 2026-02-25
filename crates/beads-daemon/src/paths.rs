//! XDG directory helpers for daemon runtime/config locations.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::config::PathsConfig;
use crate::core::{NamespaceId, StoreId};

static PATHS_CONFIG: OnceLock<Mutex<PathsConfig>> = OnceLock::new();

/// Initialize path overrides from config.
pub fn init_from_config(config: &PathsConfig) {
    let lock = PATHS_CONFIG.get_or_init(|| Mutex::new(config.clone()));
    let mut guard = lock.lock().expect("paths config lock poisoned");
    *guard = config.clone();

    beads_surface::ipc::set_runtime_dir_override(config.runtime_dir.clone());
}

/// Get the config-based data_dir override, if set.
pub fn config_data_dir_override() -> Option<PathBuf> {
    PATHS_CONFIG
        .get()
        .and_then(|lock| lock.lock().ok().and_then(|config| config.data_dir.clone()))
}

/// Get the config-based runtime_dir override, if set.
pub fn config_runtime_dir_override() -> Option<PathBuf> {
    PATHS_CONFIG.get().and_then(|lock| {
        lock.lock()
            .ok()
            .and_then(|config| config.runtime_dir.clone())
    })
}

/// Base directory for persistent data (WAL, exports, caches).
pub(crate) fn data_dir() -> PathBuf {
    if let Some(dir) = thread_local_data_dir_override() {
        return dir;
    }

    #[cfg(test)]
    if let Some(dir) = test_data_dir_override() {
        return dir;
    }

    let env_data_dir = std::env::var("BD_DATA_DIR").ok();
    let config_override = config_data_dir_override();
    resolve_data_dir(env_data_dir.as_deref(), config_override.as_deref())
}

/// Base directory for log files.
pub(crate) fn log_dir() -> PathBuf {
    let env_log_dir = std::env::var("BD_LOG_DIR").ok();
    resolve_log_dir(env_log_dir.as_deref(), &data_dir())
}

#[doc(hidden)]
pub struct DataDirOverride {
    prev: Option<PathBuf>,
}

impl DataDirOverride {
    pub fn new(path: Option<PathBuf>) -> Self {
        let prev = TEST_DATA_DIR_OVERRIDE.with(|cell| cell.replace(path));
        Self { prev }
    }
}

impl Drop for DataDirOverride {
    fn drop(&mut self) {
        let prev = self.prev.take();
        TEST_DATA_DIR_OVERRIDE.with(|cell| {
            cell.replace(prev);
        });
    }
}

#[doc(hidden)]
pub fn override_data_dir_for_tests(path: Option<PathBuf>) -> DataDirOverride {
    DataDirOverride::new(path)
}

fn thread_local_data_dir_override() -> Option<PathBuf> {
    TEST_DATA_DIR_OVERRIDE.with(|cell| cell.borrow().clone())
}

thread_local! {
    static TEST_DATA_DIR_OVERRIDE: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn set_data_dir_for_tests(path: Option<PathBuf>) {
    let lock = TEST_DATA_DIR.get_or_init(|| Mutex::new(None));
    *lock.lock().expect("test data dir lock poisoned") = path;
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn lock_data_dir_for_tests() -> std::sync::MutexGuard<'static, ()> {
    let lock = TEST_DATA_DIR_LOCK.get_or_init(|| Mutex::new(()));
    lock.lock().expect("test data dir lock poisoned")
}

#[cfg(test)]
fn test_data_dir_override() -> Option<PathBuf> {
    let lock = TEST_DATA_DIR.get_or_init(|| Mutex::new(None));
    lock.lock().expect("test data dir lock poisoned").clone()
}

#[cfg(test)]
static TEST_DATA_DIR: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();

#[cfg(test)]
#[allow(dead_code)]
static TEST_DATA_DIR_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub fn stores_dir() -> PathBuf {
    stores_dir_inner(&data_dir())
}

pub fn store_dir(store_id: StoreId) -> PathBuf {
    store_dir_inner(&data_dir(), store_id)
}

pub fn store_meta_path(store_id: StoreId) -> PathBuf {
    store_meta_path_inner(&data_dir(), store_id)
}

pub fn store_lock_path(store_id: StoreId) -> PathBuf {
    store_lock_path_inner(&data_dir(), store_id)
}

pub fn namespaces_path(store_id: StoreId) -> PathBuf {
    namespaces_path_inner(&data_dir(), store_id)
}

pub fn replicas_path(store_id: StoreId) -> PathBuf {
    replicas_path_inner(&data_dir(), store_id)
}

pub fn store_config_path(store_id: StoreId) -> PathBuf {
    store_config_path_inner(&data_dir(), store_id)
}

pub fn wal_dir(store_id: StoreId) -> PathBuf {
    wal_dir_inner(&data_dir(), store_id)
}

pub fn wal_namespace_dir(store_id: StoreId, namespace: &NamespaceId) -> PathBuf {
    wal_namespace_dir_inner(&data_dir(), store_id, namespace)
}

pub fn wal_index_path(store_id: StoreId) -> PathBuf {
    wal_index_path_inner(&data_dir(), store_id)
}

pub fn checkpoint_cache_dir(store_id: StoreId) -> PathBuf {
    checkpoint_cache_dir_inner(&data_dir(), store_id)
}

pub(crate) fn config_dir() -> PathBuf {
    let env_config_dir = std::env::var("BD_CONFIG_DIR").ok();
    resolve_config_dir(env_config_dir.as_deref())
}

fn resolve_data_dir(env_data_dir: Option<&str>, config_data_dir: Option<&Path>) -> PathBuf {
    if let Some(dir) = env_data_dir.map(str::trim).filter(|dir| !dir.is_empty()) {
        return PathBuf::from(dir);
    }

    if let Some(dir) = config_data_dir {
        return dir.to_path_buf();
    }

    std::env::var("XDG_DATA_HOME")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join(".local")
                .join("share")
        })
        .join("beads-rs")
}

fn resolve_log_dir(env_log_dir: Option<&str>, data_dir: &Path) -> PathBuf {
    if let Some(dir) = env_log_dir.map(str::trim).filter(|dir| !dir.is_empty()) {
        return PathBuf::from(dir);
    }

    data_dir.join("logs")
}

fn resolve_config_dir(env_config_dir: Option<&str>) -> PathBuf {
    if let Some(dir) = env_config_dir.map(str::trim).filter(|dir| !dir.is_empty()) {
        return PathBuf::from(dir);
    }

    std::env::var("XDG_CONFIG_HOME")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join(".config")
        })
        .join("beads-rs")
}

fn stores_dir_inner(data_dir: &Path) -> PathBuf {
    data_dir.join("stores")
}

fn store_dir_inner(data_dir: &Path, store_id: StoreId) -> PathBuf {
    stores_dir_inner(data_dir).join(store_id.to_string())
}

fn store_meta_path_inner(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir_inner(data_dir, store_id).join("meta.json")
}

fn store_lock_path_inner(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir_inner(data_dir, store_id).join("store.lock")
}

fn namespaces_path_inner(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir_inner(data_dir, store_id).join("namespaces.toml")
}

fn replicas_path_inner(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir_inner(data_dir, store_id).join("replicas.toml")
}

fn store_config_path_inner(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir_inner(data_dir, store_id).join("store_config.toml")
}

fn wal_dir_inner(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir_inner(data_dir, store_id).join("wal")
}

fn wal_namespace_dir_inner(data_dir: &Path, store_id: StoreId, namespace: &NamespaceId) -> PathBuf {
    wal_dir_inner(data_dir, store_id).join(namespace.as_str())
}

fn wal_index_path_inner(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir_inner(data_dir, store_id)
        .join("index")
        .join("wal.sqlite")
}

fn checkpoint_cache_dir_inner(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir_inner(data_dir, store_id).join("checkpoint_cache")
}
