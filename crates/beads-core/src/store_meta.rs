//! Store metadata for on-disk identity and format versions.

use serde::{Deserialize, Serialize};

use super::{ReplicaId, StoreEpoch, StoreId, StoreIdentity};

/// Legacy snapshot format version (pre-store metadata).
pub const LEGACY_SNAPSHOT_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreMetaVersions {
    pub store_format_version: u32,
    pub wal_format_version: u32,
    pub checkpoint_format_version: u32,
    pub replication_protocol_version: u32,
    pub index_schema_version: u32,
}

impl StoreMetaVersions {
    pub const STORE_FORMAT_VERSION: u32 = 1;
    pub const WAL_FORMAT_VERSION: u32 = 2;
    pub const CHECKPOINT_FORMAT_VERSION: u32 = 1;
    pub const REPLICATION_PROTOCOL_VERSION: u32 = 1;
    pub const INDEX_SCHEMA_VERSION: u32 = 1;

    pub const CURRENT: StoreMetaVersions = StoreMetaVersions {
        store_format_version: Self::STORE_FORMAT_VERSION,
        wal_format_version: Self::WAL_FORMAT_VERSION,
        checkpoint_format_version: Self::CHECKPOINT_FORMAT_VERSION,
        replication_protocol_version: Self::REPLICATION_PROTOCOL_VERSION,
        index_schema_version: Self::INDEX_SCHEMA_VERSION,
    };

    pub const fn current() -> Self {
        Self::CURRENT
    }

    pub fn new(
        store_format_version: u32,
        wal_format_version: u32,
        checkpoint_format_version: u32,
        replication_protocol_version: u32,
        index_schema_version: u32,
    ) -> Self {
        Self {
            store_format_version,
            wal_format_version,
            checkpoint_format_version,
            replication_protocol_version,
            index_schema_version,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreMeta {
    #[serde(flatten)]
    pub identity: StoreIdentity,
    pub replica_id: ReplicaId,
    pub store_format_version: u32,
    pub wal_format_version: u32,
    pub checkpoint_format_version: u32,
    pub replication_protocol_version: u32,
    pub index_schema_version: u32,
    pub created_at_ms: u64,
    #[serde(default)]
    pub orset_counter: u64,
}

impl StoreMeta {
    pub fn new(
        identity: StoreIdentity,
        replica_id: ReplicaId,
        versions: StoreMetaVersions,
        created_at_ms: u64,
    ) -> Self {
        Self {
            identity,
            replica_id,
            store_format_version: versions.store_format_version,
            wal_format_version: versions.wal_format_version,
            checkpoint_format_version: versions.checkpoint_format_version,
            replication_protocol_version: versions.replication_protocol_version,
            index_schema_version: versions.index_schema_version,
            created_at_ms,
            orset_counter: 0,
        }
    }

    pub fn store_id(&self) -> StoreId {
        self.identity.store_id
    }

    pub fn store_epoch(&self) -> StoreEpoch {
        self.identity.store_epoch
    }

    pub fn versions(&self) -> StoreMetaVersions {
        StoreMetaVersions::new(
            self.store_format_version,
            self.wal_format_version,
            self.checkpoint_format_version,
            self.replication_protocol_version,
            self.index_schema_version,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn store_meta_serde_roundtrip() {
        let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
        let identity = StoreIdentity::new(store_id, StoreEpoch::new(0));
        let versions = StoreMetaVersions::new(1, 2, 3, 4, 5);
        let mut meta = StoreMeta::new(
            identity,
            ReplicaId::new(Uuid::from_bytes([8u8; 16])),
            versions,
            1_726_000_000_000,
        );
        meta.orset_counter = 42;

        let json = serde_json::to_string(&meta).unwrap();
        let parsed: StoreMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, parsed);
    }

    #[test]
    fn store_meta_versions_current_is_consistent() {
        let current = StoreMetaVersions::current();
        assert_eq!(
            current.store_format_version,
            StoreMetaVersions::STORE_FORMAT_VERSION
        );
        assert_eq!(
            current.wal_format_version,
            StoreMetaVersions::WAL_FORMAT_VERSION
        );
        assert_eq!(
            current.checkpoint_format_version,
            StoreMetaVersions::CHECKPOINT_FORMAT_VERSION
        );
        assert_eq!(
            current.replication_protocol_version,
            StoreMetaVersions::REPLICATION_PROTOCOL_VERSION
        );
        assert_eq!(
            current.index_schema_version,
            StoreMetaVersions::INDEX_SCHEMA_VERSION
        );
    }
}
