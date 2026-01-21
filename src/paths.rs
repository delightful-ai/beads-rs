//! XDG directory helpers for config/data locations.

use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use crate::config::PathsConfig;
use crate::core::{NamespaceId, StoreId};

// =============================================================================
// Config-based path overrides (from beads.toml)
// =============================================================================

static PATHS_CONFIG: OnceLock<Mutex<PathsConfig>> = OnceLock::new();

/// Initialize path overrides from config.
///
/// This should be called early in CLI/daemon startup after config is loaded.
/// If called multiple times, the latest config wins.
pub fn init_from_config(config: &PathsConfig) {
    let lock = PATHS_CONFIG.get_or_init(|| Mutex::new(config.clone()));
    let mut guard = lock.lock().expect("paths config lock poisoned");
    *guard = config.clone();
}

/// Get the config-based data_dir override, if set.
pub fn config_data_dir_override() -> Option<PathBuf> {
    PATHS_CONFIG
        .get()
        .and_then(|lock| lock.lock().ok().and_then(|c| c.data_dir.clone()))
}

/// Get the config-based runtime_dir override, if set.
pub fn config_runtime_dir_override() -> Option<PathBuf> {
    PATHS_CONFIG
        .get()
        .and_then(|lock| lock.lock().ok().and_then(|c| c.runtime_dir.clone()))
}

/// Base directory for persistent data (WAL, exports, caches).
///
/// Priority order:
/// 1. Thread-local test override
/// 2. `BD_DATA_DIR` env var
/// 3. Config-based override (from beads.toml)
/// 4. `$XDG_DATA_HOME/beads-rs` or `~/.local/share/beads-rs`
pub(crate) fn data_dir() -> PathBuf {
    if let Some(dir) = thread_local_data_dir_override() {
        return dir;
    }

    #[cfg(test)]
    if let Some(dir) = test_data_dir_override() {
        return dir;
    }

    if let Ok(dir) = std::env::var("BD_DATA_DIR")
        && !dir.trim().is_empty()
    {
        return PathBuf::from(dir);
    }

    if let Some(dir) = config_data_dir_override() {
        return dir;
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

/// Base directory for log files.
///
/// Uses `BD_LOG_DIR` if set, otherwise `<data_dir>/logs`.
pub(crate) fn log_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("BD_LOG_DIR")
        && !dir.trim().is_empty()
    {
        return PathBuf::from(dir);
    }

    data_dir().join("logs")
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

/// Base directory for store data.
pub fn stores_dir() -> PathBuf {
    data_dir().join("stores")
}

/// Store root directory for a specific store.
pub fn store_dir(store_id: StoreId) -> PathBuf {
    stores_dir().join(store_id.to_string())
}

/// Store metadata path (meta.json).
pub fn store_meta_path(store_id: StoreId) -> PathBuf {
    store_dir(store_id).join("meta.json")
}

/// Store lock file path.
pub fn store_lock_path(store_id: StoreId) -> PathBuf {
    store_dir(store_id).join("store.lock")
}

/// Namespace policy path for a store (namespaces.toml).
pub fn namespaces_path(store_id: StoreId) -> PathBuf {
    store_dir(store_id).join("namespaces.toml")
}

/// Replica roster path for a store (replicas.toml).
pub fn replicas_path(store_id: StoreId) -> PathBuf {
    store_dir(store_id).join("replicas.toml")
}

/// Store configuration path (store_config.toml).
pub fn store_config_path(store_id: StoreId) -> PathBuf {
    store_dir(store_id).join("store_config.toml")
}

/// Root WAL directory for a store.
pub fn wal_dir(store_id: StoreId) -> PathBuf {
    store_dir(store_id).join("wal")
}

/// Namespace WAL directory for a store.
pub fn wal_namespace_dir(store_id: StoreId, namespace: &NamespaceId) -> PathBuf {
    wal_dir(store_id).join(namespace.as_str())
}

/// WAL index path for a store.
pub fn wal_index_path(store_id: StoreId) -> PathBuf {
    store_dir(store_id).join("index").join("wal.sqlite")
}

/// Checkpoint cache root directory for a store.
pub fn checkpoint_cache_dir(store_id: StoreId) -> PathBuf {
    store_dir(store_id).join("checkpoint_cache")
}

/// Base directory for configuration files.
///
/// Uses `BD_CONFIG_DIR` if set, otherwise `$XDG_CONFIG_HOME/beads-rs` or
/// `~/.config/beads-rs`.
#[allow(dead_code)]
pub(crate) fn config_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("BD_CONFIG_DIR")
        && !dir.trim().is_empty()
    {
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

#[cfg(test)]
mod tests {
    use super::*;

    static PATHS_CONFIG_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn lock_paths_config() -> std::sync::MutexGuard<'static, ()> {
        PATHS_CONFIG_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("paths config lock poisoned")
    }

    #[test]
    fn init_from_config_updates_overrides() {
        let _guard = lock_paths_config();

        let initial = PathsConfig {
            data_dir: Some(PathBuf::from("/tmp/beads-paths-one")),
            runtime_dir: Some(PathBuf::from("/tmp/beads-runtime-one")),
        };
        init_from_config(&initial);
        assert_eq!(
            config_data_dir_override(),
            Some(PathBuf::from("/tmp/beads-paths-one"))
        );
        assert_eq!(
            config_runtime_dir_override(),
            Some(PathBuf::from("/tmp/beads-runtime-one"))
        );

        let update = PathsConfig {
            data_dir: Some(PathBuf::from("/tmp/beads-paths-two")),
            runtime_dir: None,
        };
        init_from_config(&update);
        assert_eq!(
            config_data_dir_override(),
            Some(PathBuf::from("/tmp/beads-paths-two"))
        );
        assert_eq!(config_runtime_dir_override(), None);
    }
}
