//! Checkpoint meta definitions.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::CHECKPOINT_FORMAT_VERSION;
use super::json_canon::{CanonJsonError, to_canon_json_bytes};
use crate::core::{ContentHash, NamespaceId, ReplicaId, StoreEpoch, StoreId, sha256_bytes};

pub type IncludedWatermarks = BTreeMap<NamespaceId, BTreeMap<ReplicaId, u64>>;
pub type IncludedHeads = BTreeMap<NamespaceId, BTreeMap<ReplicaId, ContentHash>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointFormatVersion {
    V1,
}

impl CheckpointFormatVersion {
    pub fn as_u32(self) -> u32 {
        match self {
            CheckpointFormatVersion::V1 => CHECKPOINT_FORMAT_VERSION,
        }
    }

    pub fn parse(raw: u32) -> Option<Self> {
        if raw == CHECKPOINT_FORMAT_VERSION {
            Some(CheckpointFormatVersion::V1)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SupportedCheckpointMeta {
    version: CheckpointFormatVersion,
    meta: CheckpointMeta,
}

impl SupportedCheckpointMeta {
    pub(crate) fn new(meta: CheckpointMeta, version: CheckpointFormatVersion) -> Self {
        Self { version, meta }
    }

    pub fn meta(&self) -> &CheckpointMeta {
        &self.meta
    }

    pub fn version(&self) -> CheckpointFormatVersion {
        self.version
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointMeta {
    pub checkpoint_format_version: u32,
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
    pub checkpoint_group: String,
    pub namespaces: Vec<NamespaceId>,
    pub created_at_ms: u64,
    pub created_by_replica_id: ReplicaId,
    pub policy_hash: ContentHash,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub roster_hash: Option<ContentHash>,
    pub included: IncludedWatermarks,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub included_heads: Option<IncludedHeads>,
    pub content_hash: ContentHash,
    pub manifest_hash: ContentHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointMetaPreimage {
    pub checkpoint_format_version: u32,
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
    pub checkpoint_group: String,
    pub namespaces: Vec<NamespaceId>,
    pub created_at_ms: u64,
    pub created_by_replica_id: ReplicaId,
    pub policy_hash: ContentHash,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub roster_hash: Option<ContentHash>,
    pub included: IncludedWatermarks,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub included_heads: Option<IncludedHeads>,
    pub manifest_hash: ContentHash,
}

impl CheckpointMeta {
    pub fn canon_bytes(&self) -> Result<Vec<u8>, CanonJsonError> {
        to_canon_json_bytes(&self.normalized())
    }

    pub fn compute_content_hash(&self) -> Result<ContentHash, CanonJsonError> {
        let preimage = self.preimage();
        let bytes = to_canon_json_bytes(&preimage)?;
        Ok(ContentHash::from_bytes(sha256_bytes(&bytes).0))
    }

    pub fn preimage(&self) -> CheckpointMetaPreimage {
        let mut namespaces = self.namespaces.clone();
        namespaces.sort();
        namespaces.dedup();
        CheckpointMetaPreimage {
            checkpoint_format_version: self.checkpoint_format_version,
            store_id: self.store_id,
            store_epoch: self.store_epoch,
            checkpoint_group: self.checkpoint_group.clone(),
            namespaces,
            created_at_ms: self.created_at_ms,
            created_by_replica_id: self.created_by_replica_id,
            policy_hash: self.policy_hash,
            roster_hash: self.roster_hash,
            included: self.included.clone(),
            included_heads: self.included_heads.clone(),
            manifest_hash: self.manifest_hash,
        }
    }

    fn normalized(&self) -> Self {
        let mut namespaces = self.namespaces.clone();
        namespaces.sort();
        namespaces.dedup();
        let mut cloned = self.clone();
        cloned.namespaces = namespaces;
        cloned
    }

    pub(crate) fn namespaces_normalized(&self) -> Vec<NamespaceId> {
        let mut namespaces = self.namespaces.clone();
        namespaces.sort();
        namespaces.dedup();
        namespaces
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn content_hash_matches_preimage() {
        let store_id = StoreId::new(Uuid::from_u128(10));
        let replica_id = ReplicaId::new(Uuid::from_u128(20));
        let mut included = IncludedWatermarks::new();
        included
            .entry(NamespaceId::core())
            .or_default()
            .insert(replica_id, 7);

        let meta = CheckpointMeta {
            checkpoint_format_version: 1,
            store_id,
            store_epoch: StoreEpoch::new(0),
            checkpoint_group: "core".to_string(),
            namespaces: vec![NamespaceId::core()],
            created_at_ms: 1_700_000_000_000,
            created_by_replica_id: replica_id,
            policy_hash: ContentHash::from_bytes([3u8; 32]),
            roster_hash: None,
            included,
            included_heads: None,
            content_hash: ContentHash::from_bytes([0u8; 32]),
            manifest_hash: ContentHash::from_bytes([4u8; 32]),
        };

        let preimage = meta.preimage();
        let preimage_bytes = to_canon_json_bytes(&preimage).expect("preimage json");
        assert!(!String::from_utf8_lossy(&preimage_bytes).contains("content_hash"));

        let expected = ContentHash::from_bytes(sha256_bytes(&preimage_bytes).0);
        let computed = meta.compute_content_hash().expect("content hash");
        assert_eq!(computed, expected);
    }
}
