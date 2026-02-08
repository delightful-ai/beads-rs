//! Shared path resolution helpers for CLI-facing support modules.

use std::path::{Path, PathBuf};

use beads_core::{NamespaceId, StoreId};

/// Resolve persistent data root.
///
/// Priority order:
/// 1. `BD_DATA_DIR` when non-empty
/// 2. config-provided override
/// 3. `$XDG_DATA_HOME/beads-rs` or `~/.local/share/beads-rs`
pub fn resolve_data_dir(env_data_dir: Option<&str>, config_data_dir: Option<&Path>) -> PathBuf {
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

/// Resolve log root.
///
/// Uses `BD_LOG_DIR` when non-empty, otherwise `<data_dir>/logs`.
pub fn resolve_log_dir(env_log_dir: Option<&str>, data_dir: &Path) -> PathBuf {
    if let Some(dir) = env_log_dir.map(str::trim).filter(|dir| !dir.is_empty()) {
        return PathBuf::from(dir);
    }

    data_dir.join("logs")
}

/// Resolve config root.
///
/// Uses `BD_CONFIG_DIR` when non-empty, otherwise `$XDG_CONFIG_HOME/beads-rs`
/// or `~/.config/beads-rs`.
pub fn resolve_config_dir(env_config_dir: Option<&str>) -> PathBuf {
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

/// Base directory for all stores beneath `data_dir`.
pub fn stores_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("stores")
}

/// Root directory for a specific store.
pub fn store_dir(data_dir: &Path, store_id: StoreId) -> PathBuf {
    stores_dir(data_dir).join(store_id.to_string())
}

/// Store metadata path (`meta.json`).
pub fn store_meta_path(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir(data_dir, store_id).join("meta.json")
}

/// Store lock file path (`store.lock`).
pub fn store_lock_path(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir(data_dir, store_id).join("store.lock")
}

/// Namespace policy path (`namespaces.toml`).
pub fn namespaces_path(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir(data_dir, store_id).join("namespaces.toml")
}

/// Replica roster path (`replicas.toml`).
pub fn replicas_path(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir(data_dir, store_id).join("replicas.toml")
}

/// Store configuration path (`store_config.toml`).
pub fn store_config_path(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir(data_dir, store_id).join("store_config.toml")
}

/// Root WAL directory for a store.
pub fn wal_dir(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir(data_dir, store_id).join("wal")
}

/// Namespace WAL directory for a store.
pub fn wal_namespace_dir(data_dir: &Path, store_id: StoreId, namespace: &NamespaceId) -> PathBuf {
    wal_dir(data_dir, store_id).join(namespace.as_str())
}

/// WAL index path for a store.
pub fn wal_index_path(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir(data_dir, store_id)
        .join("index")
        .join("wal.sqlite")
}

/// Checkpoint cache root directory for a store.
pub fn checkpoint_cache_dir(data_dir: &Path, store_id: StoreId) -> PathBuf {
    store_dir(data_dir, store_id).join("checkpoint_cache")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_data_dir_prefers_env_override() {
        let resolved = resolve_data_dir(
            Some("/tmp/beads-data-from-env"),
            Some(Path::new("/tmp/beads-data-from-config")),
        );
        assert_eq!(resolved, PathBuf::from("/tmp/beads-data-from-env"));
    }

    #[test]
    fn resolve_log_dir_prefers_env_override() {
        let resolved = resolve_log_dir(
            Some("/tmp/beads-log-from-env"),
            Path::new("/tmp/beads-data-default"),
        );
        assert_eq!(resolved, PathBuf::from("/tmp/beads-log-from-env"));
    }

    #[test]
    fn store_paths_are_derived_from_data_dir() {
        let data_dir = Path::new("/tmp/beads-data");
        let store_id = StoreId::new(uuid::Uuid::from_bytes([3u8; 16]));
        let store_root = store_dir(data_dir, store_id);
        assert_eq!(
            store_root,
            Path::new("/tmp/beads-data/stores").join("03030303-0303-0303-0303-030303030303")
        );
        assert_eq!(
            wal_index_path(data_dir, store_id),
            store_root.join("index").join("wal.sqlite")
        );
        assert_eq!(
            checkpoint_cache_dir(data_dir, store_id),
            store_root.join("checkpoint_cache")
        );
    }
}
