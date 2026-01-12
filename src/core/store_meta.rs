//! Store metadata for on-disk identity and format versions.

use serde::{Deserialize, Serialize};

use super::{ReplicaId, StoreEpoch, StoreId, StoreIdentity};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreMetaVersions {
    pub store_format_version: u32,
    pub wal_format_version: u32,
    pub checkpoint_format_version: u32,
    pub replication_protocol_version: u32,
    pub index_schema_version: u32,
}

impl StoreMetaVersions {
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
        }
    }

    pub fn store_id(&self) -> StoreId {
        self.identity.store_id
    }

    pub fn store_epoch(&self) -> StoreEpoch {
        self.identity.store_epoch
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
        let meta = StoreMeta::new(
            identity,
            ReplicaId::new(Uuid::from_bytes([8u8; 16])),
            versions,
            1_726_000_000_000,
        );

        let json = serde_json::to_string(&meta).unwrap();
        let parsed: StoreMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, parsed);
    }
}
