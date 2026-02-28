//! Checkpoint snapshot barrier types.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use super::layout::CheckpointShardPath;
use super::meta::{IncludedHeads, IncludedWatermarks};
use crate::core::{ContentHash, NamespaceSet, ReplicaId, StoreEpoch, StoreId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointShardPayload {
    pub path: CheckpointShardPath,
    pub bytes: Bytes,
}

/// Immutable snapshot payload captured by the coordinator and handed to workers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointSnapshot {
    pub checkpoint_group: String,
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
    pub namespaces: NamespaceSet,
    pub created_at_ms: u64,
    pub created_by_replica_id: ReplicaId,
    pub policy_hash: ContentHash,
    pub roster_hash: Option<ContentHash>,
    pub included: IncludedWatermarks,
    pub included_heads: Option<IncludedHeads>,
    pub shards: BTreeMap<CheckpointShardPath, CheckpointShardPayload>,
    pub dirty_shards: BTreeSet<CheckpointShardPath>,
}
