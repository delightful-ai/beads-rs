//! Checkpoint snapshot builder (coordinator-side barrier).

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::Serialize;
use thiserror::Error;

use super::json_canon::{CanonJsonError, to_canon_json_bytes};
use super::layout::{
    CheckpointFileKind, shard_for_bead, shard_for_dep, shard_for_tombstone, shard_path,
};
use super::meta::{IncludedHeads, IncludedWatermarks};
use super::types::{CheckpointShardPayload, CheckpointSnapshot};
use crate::core::dep::DepKey;
use crate::core::tombstone::TombstoneKey;
use crate::core::{
    CanonicalState, ContentHash, DepEdge, Durable, HeadStatus, NamespaceId, NamespacePolicy,
    ReplicaId, StoreEpoch, StoreId, Tombstone, Watermarks, WireBeadFull, WireDepV1, WireStamp,
    WireTombstoneV1, sha256_bytes,
};

#[derive(Debug, Error)]
pub enum CheckpointSnapshotError {
    #[error("checkpoint snapshot unsupported namespace {namespace}")]
    NamespaceUnsupported { namespace: NamespaceId },
    #[error(transparent)]
    CanonJson(#[from] CanonJsonError),
}

pub fn policy_hash(
    policies: &BTreeMap<NamespaceId, NamespacePolicy>,
) -> Result<ContentHash, CanonJsonError> {
    let bytes = to_canon_json_bytes(policies)?;
    Ok(ContentHash::from_bytes(sha256_bytes(&bytes).0))
}

pub struct CheckpointSnapshotInput<'a> {
    pub checkpoint_group: String,
    pub namespaces: Vec<NamespaceId>,
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
    pub created_at_ms: u64,
    pub created_by_replica_id: ReplicaId,
    pub policy_hash: ContentHash,
    pub roster_hash: Option<ContentHash>,
    pub state: &'a CanonicalState,
    pub watermarks_durable: &'a Watermarks<Durable>,
}

pub fn build_snapshot(
    input: CheckpointSnapshotInput<'_>,
) -> Result<CheckpointSnapshot, CheckpointSnapshotError> {
    let CheckpointSnapshotInput {
        checkpoint_group,
        mut namespaces,
        store_id,
        store_epoch,
        created_at_ms,
        created_by_replica_id,
        policy_hash,
        roster_hash,
        state,
        watermarks_durable,
    } = input;
    namespaces.sort();
    namespaces.dedup();

    let mut shards: BTreeMap<String, CheckpointShardPayload> = BTreeMap::new();
    for namespace in &namespaces {
        if *namespace != NamespaceId::core() {
            return Err(CheckpointSnapshotError::NamespaceUnsupported {
                namespace: namespace.clone(),
            });
        }
        let ns_shards = build_namespace_shards(namespace, state)?;
        shards.extend(ns_shards);
    }

    let included = included_watermarks(watermarks_durable, &namespaces);
    let included_heads = included_heads(watermarks_durable, &namespaces);
    let dirty_shards = shards.keys().cloned().collect();

    Ok(CheckpointSnapshot {
        checkpoint_group,
        store_id,
        store_epoch,
        namespaces,
        created_at_ms,
        created_by_replica_id,
        policy_hash,
        roster_hash,
        included,
        included_heads,
        shards,
        dirty_shards,
    })
}

fn included_watermarks(
    watermarks: &Watermarks<Durable>,
    namespaces: &[NamespaceId],
) -> IncludedWatermarks {
    let mut included = IncludedWatermarks::new();
    for namespace in namespaces {
        let mut origins = BTreeMap::new();
        for (origin, watermark) in watermarks.origins(namespace) {
            origins.insert(*origin, watermark.seq().get());
        }
        included.insert(namespace.clone(), origins);
    }
    included
}

fn included_heads(
    watermarks: &Watermarks<Durable>,
    namespaces: &[NamespaceId],
) -> Option<IncludedHeads> {
    let mut heads = IncludedHeads::new();
    for namespace in namespaces {
        let mut origins = BTreeMap::new();
        for (origin, watermark) in watermarks.origins(namespace) {
            if let HeadStatus::Known(head) = watermark.head() {
                origins.insert(*origin, ContentHash::from_bytes(head));
            }
        }
        if !origins.is_empty() {
            heads.insert(namespace.clone(), origins);
        }
    }
    if heads.is_empty() { None } else { Some(heads) }
}

fn build_namespace_shards(
    namespace: &NamespaceId,
    state: &CanonicalState,
) -> Result<BTreeMap<String, CheckpointShardPayload>, CheckpointSnapshotError> {
    let mut payloads: BTreeMap<String, Vec<u8>> = BTreeMap::new();

    for (id, bead) in state.iter_live() {
        let shard = shard_for_bead(id);
        let path = shard_path(namespace, CheckpointFileKind::State, &shard);
        let wire = WireBeadFull::from(bead);
        push_jsonl_line(&mut payloads, path, &wire)?;
    }

    for (key, tombstone) in state.iter_tombstones() {
        let TombstoneKey { id, .. } = key;
        let shard = shard_for_tombstone(id);
        let path = shard_path(namespace, CheckpointFileKind::Tombstones, &shard);
        let wire = wire_tombstone(tombstone);
        push_jsonl_line(&mut payloads, path, &wire)?;
    }

    for (key, edge) in state.iter_deps() {
        let shard = shard_for_dep(key.from(), key.to(), key.kind());
        let path = shard_path(namespace, CheckpointFileKind::Deps, &shard);
        let wire = wire_dep(key, edge);
        push_jsonl_line(&mut payloads, path, &wire)?;
    }

    let shards = payloads
        .into_iter()
        .map(|(path, bytes)| {
            (
                path.clone(),
                CheckpointShardPayload {
                    path,
                    bytes: Bytes::from(bytes),
                },
            )
        })
        .collect();

    Ok(shards)
}

fn push_jsonl_line<T: Serialize>(
    payloads: &mut BTreeMap<String, Vec<u8>>,
    path: String,
    value: &T,
) -> Result<(), CheckpointSnapshotError> {
    let mut line = to_canon_json_bytes(value)?;
    line.push(b'\n');
    let entry = payloads.entry(path).or_default();
    entry.extend_from_slice(&line);
    Ok(())
}

fn wire_tombstone(tombstone: &Tombstone) -> WireTombstoneV1 {
    let deleted = &tombstone.deleted;
    let (lineage_created_at, lineage_created_by) = tombstone
        .lineage
        .as_ref()
        .map(|stamp| (Some(WireStamp::from(&stamp.at)), Some(stamp.by.clone())))
        .unwrap_or((None, None));

    WireTombstoneV1 {
        id: tombstone.id.clone(),
        deleted_at: WireStamp::from(&deleted.at),
        deleted_by: deleted.by.clone(),
        reason: tombstone.reason.clone(),
        lineage_created_at,
        lineage_created_by,
    }
}

fn wire_dep(key: &DepKey, edge: &DepEdge) -> WireDepV1 {
    let deleted = edge.deleted_stamp();
    WireDepV1 {
        from: key.from().clone(),
        to: key.to().clone(),
        kind: key.kind(),
        created_at: WireStamp::from(&edge.created.at),
        created_by: edge.created.by.clone(),
        deleted_at: deleted.map(|stamp| WireStamp::from(&stamp.at)),
        deleted_by: deleted.map(|stamp| stamp.by.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    use crate::core::bead::{BeadCore, BeadFields};
    use crate::core::collections::Labels;
    use crate::core::composite::{Claim, Workflow};
    use crate::core::crdt::Lww;
    use crate::core::domain::{BeadType, DepKind, Priority};
    use crate::core::identity::BeadId;
    use crate::core::time::{Stamp, WriteStamp};
    use crate::core::{ActorId, Seq0};

    fn make_stamp(wall_ms: u64, counter: u32, actor: &str) -> Stamp {
        Stamp::new(
            WriteStamp::new(wall_ms, counter),
            ActorId::new(actor).expect("actor id"),
        )
    }

    fn make_bead(id: &BeadId, stamp: &Stamp) -> crate::core::Bead {
        let core = BeadCore::new(id.clone(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("title".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            labels: Lww::new(Labels::new(), stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        crate::core::Bead::new(core, fields)
    }

    #[test]
    fn snapshot_builds_shards_and_includes_watermarks() {
        let namespace = NamespaceId::core();
        let stamp = make_stamp(1, 0, "author");
        let bead_id = BeadId::parse("beads-rs-abc1").unwrap();
        let dep_to = BeadId::parse("beads-rs-abc2").unwrap();

        let bead = make_bead(&bead_id, &stamp);
        let tombstone = Tombstone::new(bead_id.clone(), stamp.clone(), Some("bye".into()));
        let dep_key = DepKey::new(bead_id.clone(), dep_to.clone(), DepKind::Blocks).unwrap();
        let dep_edge = DepEdge::new(stamp.clone());

        let mut state = CanonicalState::new();
        state.insert(bead.clone()).unwrap();
        state.insert_tombstone(tombstone.clone());
        state.insert_dep(dep_key.clone(), dep_edge.clone());

        let origin = ReplicaId::new(Uuid::from_bytes([3u8; 16]));
        let mut watermarks = Watermarks::<Durable>::new();
        watermarks
            .observe_at_least(
                &namespace,
                &origin,
                Seq0::new(2),
                HeadStatus::Known([2u8; 32]),
            )
            .unwrap();

        let snapshot = build_snapshot(CheckpointSnapshotInput {
            checkpoint_group: "core".to_string(),
            namespaces: vec![namespace.clone()],
            store_id: StoreId::new(Uuid::from_bytes([4u8; 16])),
            store_epoch: StoreEpoch::new(0),
            created_at_ms: 1_700_000_000_000,
            created_by_replica_id: origin,
            policy_hash: ContentHash::from_bytes([9u8; 32]),
            roster_hash: None,
            state: &state,
            watermarks_durable: &watermarks,
        })
        .unwrap();

        assert_eq!(
            snapshot
                .included
                .get(&namespace)
                .and_then(|origins| origins.get(&origin)),
            Some(&2)
        );

        let state_path = shard_path(
            &namespace,
            CheckpointFileKind::State,
            &shard_for_bead(&bead_id),
        );
        let state_payload = snapshot.shards.get(&state_path).expect("state shard");
        let mut expected = to_canon_json_bytes(&WireBeadFull::from(&bead)).unwrap();
        expected.push(b'\n');
        assert_eq!(state_payload.bytes.as_ref(), expected);

        let tomb_path = shard_path(
            &namespace,
            CheckpointFileKind::Tombstones,
            &shard_for_tombstone(&bead_id),
        );
        let tomb_payload = snapshot.shards.get(&tomb_path).expect("tombstone shard");
        let mut expected = to_canon_json_bytes(&wire_tombstone(&tombstone)).unwrap();
        expected.push(b'\n');
        assert_eq!(tomb_payload.bytes.as_ref(), expected);

        let dep_path = shard_path(
            &namespace,
            CheckpointFileKind::Deps,
            &shard_for_dep(dep_key.from(), dep_key.to(), dep_key.kind()),
        );
        let dep_payload = snapshot.shards.get(&dep_path).expect("dep shard");
        let mut expected = to_canon_json_bytes(&wire_dep(&dep_key, &dep_edge)).unwrap();
        expected.push(b'\n');
        assert_eq!(dep_payload.bytes.as_ref(), expected);

        assert!(snapshot.dirty_shards.contains(&state_path));
        assert!(snapshot.dirty_shards.contains(&tomb_path));
        assert!(snapshot.dirty_shards.contains(&dep_path));
    }
}
