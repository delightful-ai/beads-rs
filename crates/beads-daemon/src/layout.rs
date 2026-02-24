use std::path::PathBuf;

use beads_core::{NamespaceId, StoreId};

/// Runtime filesystem layout for daemon-owned state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonLayout {
    pub data_dir: PathBuf,
    pub socket_path: PathBuf,
    pub stores_dir: PathBuf,
    pub log_dir: PathBuf,
}

impl DaemonLayout {
    #[must_use]
    pub fn new(data_dir: PathBuf, socket_path: PathBuf, log_dir: PathBuf) -> Self {
        let stores_dir = data_dir.join("stores");
        Self {
            data_dir,
            socket_path,
            stores_dir,
            log_dir,
        }
    }

    #[must_use]
    pub fn store_dir(&self, store_id: &StoreId) -> PathBuf {
        self.stores_dir.join(store_id.to_string())
    }

    #[must_use]
    pub fn wal_dir(&self, store_id: &StoreId) -> PathBuf {
        self.store_dir(store_id).join("wal")
    }

    #[must_use]
    pub fn wal_namespace_dir(&self, store_id: &StoreId, namespace: &NamespaceId) -> PathBuf {
        self.wal_dir(store_id).join(namespace.as_str())
    }

    #[must_use]
    pub fn wal_index_path(&self, store_id: &StoreId) -> PathBuf {
        self.store_dir(store_id).join("index").join("wal.sqlite")
    }

    #[must_use]
    pub fn store_meta_path(&self, store_id: &StoreId) -> PathBuf {
        self.store_dir(store_id).join("meta.json")
    }

    #[must_use]
    pub fn store_lock_path(&self, store_id: &StoreId) -> PathBuf {
        self.store_dir(store_id).join("store.lock")
    }

    #[must_use]
    pub fn namespaces_path(&self, store_id: &StoreId) -> PathBuf {
        self.store_dir(store_id).join("namespaces.toml")
    }

    #[must_use]
    pub fn replicas_path(&self, store_id: &StoreId) -> PathBuf {
        self.store_dir(store_id).join("replicas.toml")
    }

    #[must_use]
    pub fn store_config_path(&self, store_id: &StoreId) -> PathBuf {
        self.store_dir(store_id).join("store_config.toml")
    }

    #[must_use]
    pub fn checkpoint_cache_dir(&self, store_id: &StoreId) -> PathBuf {
        self.store_dir(store_id).join("checkpoint_cache")
    }

    #[must_use]
    pub fn log_dir(&self) -> PathBuf {
        self.log_dir.clone()
    }
}
