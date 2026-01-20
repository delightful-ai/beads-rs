//! Checkpoint snapshot builder (coordinator-side barrier).

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use serde::Serialize;
use thiserror::Error;

use super::CHECKPOINT_FORMAT_VERSION;
use super::json_canon::{CanonJsonError, to_canon_json_bytes};
use super::layout::{
    CheckpointFileKind, CheckpointShardPath, shard_for_bead, shard_for_dep, shard_for_tombstone,
    shard_name, shard_path,
};
use super::manifest::{CheckpointManifest, ManifestFile};
use super::meta::{CheckpointMeta, IncludedHeads, IncludedWatermarks};
use super::types::{CheckpointShardPayload, CheckpointSnapshot};
use crate::core::dep::DepKey;
use crate::core::tombstone::TombstoneKey;
use crate::core::{
    ContentHash, Durable, HeadStatus, NamespaceId, NamespacePolicy, ReplicaId, ReplicaRoster,
    StoreEpoch, StoreId, StoreState, Tombstone, Watermarks, WireBeadFull, WireDepEntryV1,
    WireDepStoreV1, WireStamp, WireTombstoneV1, sha256_bytes,
};

#[derive(Debug, Error)]
pub enum CheckpointSnapshotError {
    #[error("checkpoint snapshot unsupported namespace {namespace}")]
    NamespaceUnsupported { namespace: NamespaceId },
    #[error(transparent)]
    CanonJson(#[from] CanonJsonError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointExport {
    pub manifest: CheckpointManifest,
    pub meta: CheckpointMeta,
    pub files: BTreeMap<String, CheckpointShardPayload>,
}

#[derive(Debug, Error)]
pub enum CheckpointExportError {
    #[error(
        "checkpoint export previous store id mismatch (previous {previous}, snapshot {snapshot})"
    )]
    PreviousStoreId {
        previous: StoreId,
        snapshot: StoreId,
    },
    #[error(
        "checkpoint export previous store epoch mismatch (previous {previous}, snapshot {snapshot})"
    )]
    PreviousStoreEpoch {
        previous: StoreEpoch,
        snapshot: StoreEpoch,
    },
    #[error("checkpoint export previous group mismatch (previous {previous}, snapshot {snapshot})")]
    PreviousGroup { previous: String, snapshot: String },
    #[error(
        "checkpoint export previous namespaces mismatch (previous {previous:?}, snapshot {snapshot:?})"
    )]
    PreviousNamespaces {
        previous: Vec<NamespaceId>,
        snapshot: Vec<NamespaceId>,
    },
    #[error(transparent)]
    CanonJson(#[from] CanonJsonError),
}

pub fn policy_hash(
    policies: &BTreeMap<NamespaceId, NamespacePolicy>,
) -> Result<ContentHash, CanonJsonError> {
    let bytes = to_canon_json_bytes(policies)?;
    Ok(ContentHash::from_bytes(sha256_bytes(&bytes).0))
}

pub fn roster_hash(roster: &ReplicaRoster) -> Result<ContentHash, CanonJsonError> {
    let mut roster = roster.clone();
    for entry in &mut roster.replicas {
        if let Some(namespaces) = &mut entry.allowed_namespaces {
            namespaces.sort();
            namespaces.dedup();
        }
    }
    roster.replicas.sort_by_key(|entry| entry.replica_id);
    let bytes = to_canon_json_bytes(&roster)?;
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
    pub dirty_shards: Option<BTreeSet<CheckpointShardPath>>,
    pub state: &'a StoreState,
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
        dirty_shards,
        state,
        watermarks_durable,
    } = input;
    namespaces.sort();
    namespaces.dedup();

    let mut shards: BTreeMap<String, CheckpointShardPayload> = BTreeMap::new();
    for namespace in &namespaces {
        let ns_shards = build_namespace_shards(namespace, state)?;
        shards.extend(ns_shards);
    }

    let included = included_watermarks(watermarks_durable, &namespaces);
    let included_heads = included_heads(watermarks_durable, &namespaces);
    let dirty_shards = match dirty_shards {
        Some(paths) => paths.into_iter().map(|path| path.to_path()).collect(),
        None => shards.keys().cloned().collect(),
    };

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

pub struct CheckpointExportInput<'a> {
    pub snapshot: &'a CheckpointSnapshot,
    pub previous: Option<&'a CheckpointExport>,
}

pub fn export_checkpoint(
    input: CheckpointExportInput<'_>,
) -> Result<CheckpointExport, CheckpointExportError> {
    let CheckpointExportInput { snapshot, previous } = input;
    if let Some(previous) = previous {
        validate_previous(snapshot, previous)?;
    }

    let files = assemble_export_files(snapshot, previous);
    let manifest = build_manifest(snapshot, &files);
    let manifest_hash = manifest.manifest_hash()?;
    let mut meta = CheckpointMeta {
        checkpoint_format_version: CHECKPOINT_FORMAT_VERSION,
        store_id: snapshot.store_id,
        store_epoch: snapshot.store_epoch,
        checkpoint_group: snapshot.checkpoint_group.clone(),
        namespaces: snapshot.namespaces.clone(),
        created_at_ms: snapshot.created_at_ms,
        created_by_replica_id: snapshot.created_by_replica_id,
        policy_hash: snapshot.policy_hash,
        roster_hash: snapshot.roster_hash,
        included: snapshot.included.clone(),
        included_heads: snapshot.included_heads.clone(),
        content_hash: ContentHash::from_bytes([0u8; 32]),
        manifest_hash,
    };
    let content_hash = meta.compute_content_hash()?;
    meta.content_hash = content_hash;

    Ok(CheckpointExport {
        manifest,
        meta,
        files,
    })
}

fn validate_previous(
    snapshot: &CheckpointSnapshot,
    previous: &CheckpointExport,
) -> Result<(), CheckpointExportError> {
    let prev_manifest = &previous.manifest;
    if prev_manifest.store_id != snapshot.store_id {
        return Err(CheckpointExportError::PreviousStoreId {
            previous: prev_manifest.store_id,
            snapshot: snapshot.store_id,
        });
    }
    if prev_manifest.store_epoch != snapshot.store_epoch {
        return Err(CheckpointExportError::PreviousStoreEpoch {
            previous: prev_manifest.store_epoch,
            snapshot: snapshot.store_epoch,
        });
    }
    if prev_manifest.checkpoint_group != snapshot.checkpoint_group {
        return Err(CheckpointExportError::PreviousGroup {
            previous: prev_manifest.checkpoint_group.clone(),
            snapshot: snapshot.checkpoint_group.clone(),
        });
    }
    let prev_namespaces = normalize_namespaces(&prev_manifest.namespaces);
    let snap_namespaces = normalize_namespaces(&snapshot.namespaces);
    if prev_namespaces != snap_namespaces {
        return Err(CheckpointExportError::PreviousNamespaces {
            previous: prev_namespaces,
            snapshot: snap_namespaces,
        });
    }
    Ok(())
}

fn normalize_namespaces(namespaces: &[NamespaceId]) -> Vec<NamespaceId> {
    let mut out = namespaces.to_vec();
    out.sort();
    out.dedup();
    out
}

fn assemble_export_files(
    snapshot: &CheckpointSnapshot,
    previous: Option<&CheckpointExport>,
) -> BTreeMap<String, CheckpointShardPayload> {
    let mut files = BTreeMap::new();

    if let Some(previous) = previous {
        for (path, payload) in &previous.files {
            if snapshot.dirty_shards.contains(path) {
                continue;
            }
            files.insert(path.clone(), payload.clone());
        }
    }

    for (path, payload) in &snapshot.shards {
        if snapshot.dirty_shards.contains(path) || !files.contains_key(path) {
            files.insert(path.clone(), payload.clone());
        }
    }

    if previous.is_some() {
        for path in snapshot.dirty_shards.iter() {
            if !snapshot.shards.contains_key(path) {
                files.remove(path);
            }
        }
    }

    files
}

fn build_manifest(
    snapshot: &CheckpointSnapshot,
    files: &BTreeMap<String, CheckpointShardPayload>,
) -> CheckpointManifest {
    let mut manifest_files = BTreeMap::new();
    for (path, payload) in files {
        let hash = ContentHash::from_bytes(sha256_bytes(payload.bytes.as_ref()).0);
        let entry = ManifestFile {
            sha256: hash,
            bytes: payload.bytes.len() as u64,
        };
        manifest_files.insert(path.clone(), entry);
    }
    CheckpointManifest {
        checkpoint_group: snapshot.checkpoint_group.clone(),
        store_id: snapshot.store_id,
        store_epoch: snapshot.store_epoch,
        namespaces: snapshot.namespaces.clone(),
        files: manifest_files,
    }
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
    state: &StoreState,
) -> Result<BTreeMap<String, CheckpointShardPayload>, CheckpointSnapshotError> {
    let Some(state) = state.get(namespace) else {
        return Ok(BTreeMap::new());
    };
    let mut payloads: BTreeMap<String, Vec<u8>> = BTreeMap::new();

    for (id, _) in state.iter_live() {
        let shard = shard_for_bead(id);
        let path = shard_path(namespace, CheckpointFileKind::State, &shard);
        let Some(view) = state.bead_view(id) else {
            continue;
        };
        let label_state = state.label_store().state(id);
        let wire = WireBeadFull::from_view(&view, label_state);
        push_jsonl_line(&mut payloads, path, &wire)?;
    }

    for (key, tombstone) in state.iter_tombstones() {
        let TombstoneKey { id, .. } = key;
        let shard = shard_for_tombstone(&id);
        let path = shard_path(namespace, CheckpointFileKind::Tombstones, &shard);
        let wire = wire_tombstone(tombstone);
        push_jsonl_line(&mut payloads, path, &wire)?;
    }

    let dep_store = state.dep_store();
    let mut dep_entries_by_shard: BTreeMap<String, Vec<WireDepEntryV1>> = BTreeMap::new();
    for key in dep_store.values() {
        let mut dots: Vec<Dot> = dep_store
            .dots_for(key)
            .map(|dots| dots.iter().copied().collect())
            .unwrap_or_default();
        dots.sort();
        let entry = WireDepEntryV1 {
            key: key.clone(),
            dots,
        };
        let shard = shard_for_dep(key.from(), key.to(), key.kind());
        dep_entries_by_shard.entry(shard).or_default().push(entry);
    }

    if dep_entries_by_shard.is_empty()
        && (!dep_store.cc().max.is_empty() || dep_store.stamp().is_some())
    {
        dep_entries_by_shard.insert(shard_name(0), Vec::new());
    }

    for (shard, mut entries) in dep_entries_by_shard {
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        let wire = WireDepStoreV1 {
            cc: dep_store.cc().clone(),
            entries,
            stamp: dep_store
                .stamp()
                .map(|stamp| (WireStamp::from(&stamp.at), stamp.by.clone())),
        };
        let path = shard_path(namespace, CheckpointFileKind::Deps, &shard);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use uuid::Uuid;

    use crate::core::bead::{BeadCore, BeadFields};
    use crate::core::composite::{Claim, Workflow};
    use crate::core::crdt::Lww;
    use crate::core::domain::{BeadType, DepKind, Priority};
    use crate::core::identity::BeadId;
    use crate::core::time::{Stamp, WriteStamp};
    use crate::core::{ActorId, CanonicalState, Dot, ReplicaEntry, ReplicaRole, Seq0, Sha256};

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
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        crate::core::Bead::new(core, fields)
    }

    fn find_two_ids_same_shard() -> (BeadId, BeadId, String) {
        let mut seen: BTreeMap<String, BeadId> = BTreeMap::new();
        for i in 0..10_000 {
            let id = BeadId::parse(&format!("beads-rs-{:04}", i)).expect("bead id");
            let shard = shard_for_bead(&id);
            if let Some(prev) = seen.insert(shard.clone(), id.clone()) {
                return (prev, id, shard);
            }
        }
        panic!("failed to find shard collision");
    }

    fn find_two_ids_different_shards() -> (BeadId, BeadId) {
        let mut first: Option<(BeadId, String)> = None;
        for i in 0..10_000 {
            let id = BeadId::parse(&format!("beads-rs-{:04}", i)).expect("bead id");
            let shard = shard_for_bead(&id);
            match &first {
                None => first = Some((id, shard)),
                Some((first_id, first_shard)) if &shard != first_shard => {
                    return (first_id.clone(), id);
                }
                _ => {}
            }
        }
        panic!("failed to find different shard ids");
    }

    #[test]
    fn roster_hash_is_stable_for_reordered_entries() {
        let entry_a = ReplicaEntry {
            replica_id: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            name: "alpha".to_string(),
            role: ReplicaRole::Anchor,
            durability_eligible: true,
            allowed_namespaces: Some(vec![NamespaceId::core()]),
            expire_after_ms: None,
        };
        let entry_b = ReplicaEntry {
            replica_id: ReplicaId::new(Uuid::from_bytes([2u8; 16])),
            name: "beta".to_string(),
            role: ReplicaRole::Peer,
            durability_eligible: false,
            allowed_namespaces: Some(vec![
                NamespaceId::core(),
                NamespaceId::parse("tmp").expect("tmp namespace"),
            ]),
            expire_after_ms: Some(5_000),
        };

        let roster_a = ReplicaRoster {
            replicas: vec![entry_a.clone(), entry_b.clone()],
        };
        let roster_b = ReplicaRoster {
            replicas: vec![entry_b, entry_a],
        };

        let hash_a = roster_hash(&roster_a).expect("hash a");
        let hash_b = roster_hash(&roster_b).expect("hash b");
        assert_eq!(hash_a, hash_b);
    }

    fn build_test_snapshot(
        state: &StoreState,
        namespace: &NamespaceId,
        origin: ReplicaId,
        seq: u64,
    ) -> CheckpointSnapshot {
        let mut watermarks = Watermarks::<Durable>::new();
        watermarks
            .observe_at_least(
                namespace,
                &origin,
                Seq0::new(seq),
                HeadStatus::Known([seq as u8; 32]),
            )
            .unwrap();
        build_snapshot(CheckpointSnapshotInput {
            checkpoint_group: "core".to_string(),
            namespaces: vec![namespace.clone()],
            store_id: StoreId::new(Uuid::from_bytes([4u8; 16])),
            store_epoch: StoreEpoch::new(0),
            created_at_ms: 1_700_000_000_000,
            created_by_replica_id: origin,
            policy_hash: ContentHash::from_bytes([9u8; 32]),
            roster_hash: None,
            dirty_shards: None,
            state,
            watermarks_durable: &watermarks,
        })
        .expect("snapshot")
    }

    #[test]
    fn snapshot_builds_shards_and_includes_watermarks() {
        let namespace = NamespaceId::core();
        let stamp = make_stamp(1, 0, "author");
        let bead_id = BeadId::parse("beads-rs-abc1").unwrap();
        let dep_to = BeadId::parse("beads-rs-abc2").unwrap();

        let bead = make_bead(&bead_id, &stamp);
        let collision_lineage = make_stamp(0, 0, "loser");
        let tombstone = Tombstone::new_collision(
            bead_id.clone(),
            stamp.clone(),
            collision_lineage,
            Some("bye".into()),
        );
        let dep_key = DepKey::new(bead_id.clone(), dep_to.clone(), DepKind::Blocks).unwrap();
        let dep_dot = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([5u8; 16])),
            counter: 1,
        };

        let mut core_state = CanonicalState::new();
        core_state.insert(bead.clone()).unwrap();
        core_state.insert_tombstone(tombstone.clone());
        core_state.apply_dep_add(dep_key.clone(), dep_dot, Sha256([0; 32]), stamp.clone());
        let mut state = StoreState::new();
        state.set_namespace_state(namespace.clone(), core_state.clone());

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
            dirty_shards: None,
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
        let view = core_state.bead_view(&bead_id).expect("bead view");
        let label_state = core_state.label_store().state(&bead_id);
        let mut expected =
            to_canon_json_bytes(&WireBeadFull::from_view(&view, label_state)).unwrap();
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
        let mut expected = to_canon_json_bytes(&WireDepStoreV1 {
            cc: core_state.dep_store().cc().clone(),
            entries: vec![WireDepEntryV1 {
                key: dep_key.clone(),
                dots: vec![dep_dot],
            }],
            stamp: core_state
                .dep_store()
                .stamp()
                .map(|stamp| (WireStamp::from(&stamp.at), stamp.by.clone())),
        })
        .unwrap();
        expected.push(b'\n');
        assert_eq!(dep_payload.bytes.as_ref(), expected);

        assert!(snapshot.dirty_shards.contains(&state_path));
        assert!(snapshot.dirty_shards.contains(&tomb_path));
        assert!(snapshot.dirty_shards.contains(&dep_path));
    }

    #[test]
    fn snapshot_respects_dirty_shards_input() {
        let namespace = NamespaceId::core();
        let stamp = make_stamp(1, 0, "author");
        let bead_id = BeadId::parse("beads-rs-abc1").unwrap();
        let bead = make_bead(&bead_id, &stamp);

        let mut core_state = CanonicalState::new();
        core_state.insert(bead.clone()).unwrap();
        let mut state = StoreState::new();
        state.set_namespace_state(namespace.clone(), core_state);

        let origin = ReplicaId::new(Uuid::from_bytes([8u8; 16]));
        let mut watermarks = Watermarks::<Durable>::new();
        watermarks
            .observe_at_least(
                &namespace,
                &origin,
                Seq0::new(4),
                HeadStatus::Known([4u8; 32]),
            )
            .unwrap();
        let mut expected_paths = BTreeSet::new();
        expected_paths.insert(CheckpointShardPath::new(
            namespace.clone(),
            CheckpointFileKind::State,
            shard_for_bead(&bead_id),
        ));
        let custom = build_snapshot(CheckpointSnapshotInput {
            checkpoint_group: "core".to_string(),
            namespaces: vec![namespace.clone()],
            store_id: StoreId::new(Uuid::from_bytes([4u8; 16])),
            store_epoch: StoreEpoch::new(0),
            created_at_ms: 1_700_000_000_000,
            created_by_replica_id: origin,
            policy_hash: ContentHash::from_bytes([9u8; 32]),
            roster_hash: None,
            dirty_shards: Some(expected_paths.clone()),
            state: &state,
            watermarks_durable: &watermarks,
        })
        .expect("snapshot");

        let expected_dirty: BTreeSet<String> =
            expected_paths.iter().map(|path| path.to_path()).collect();
        assert_eq!(custom.dirty_shards, expected_dirty);
    }

    #[test]
    fn export_builds_manifest_and_meta() {
        let namespace = NamespaceId::core();
        let stamp = make_stamp(1, 0, "author");
        let bead_id = BeadId::parse("beads-rs-abc1").unwrap();

        let bead = make_bead(&bead_id, &stamp);
        let mut core_state = CanonicalState::new();
        core_state.insert(bead.clone()).unwrap();
        let mut state = StoreState::new();
        state.set_namespace_state(namespace.clone(), core_state);

        let origin = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
        let snapshot = build_test_snapshot(&state, &namespace, origin, 5);

        let export = export_checkpoint(CheckpointExportInput {
            snapshot: &snapshot,
            previous: None,
        })
        .expect("export");

        let state_path = shard_path(
            &namespace,
            CheckpointFileKind::State,
            &shard_for_bead(&bead_id),
        );
        let file = export.files.get(&state_path).expect("state file");
        let manifest_entry = export
            .manifest
            .files
            .get(&state_path)
            .expect("manifest entry");
        let expected_hash = ContentHash::from_bytes(sha256_bytes(file.bytes.as_ref()).0);
        assert_eq!(manifest_entry.sha256, expected_hash);
        assert_eq!(manifest_entry.bytes, file.bytes.len() as u64);

        assert_eq!(
            export.meta.checkpoint_format_version,
            CHECKPOINT_FORMAT_VERSION
        );
        assert_eq!(
            export
                .meta
                .included
                .get(&namespace)
                .and_then(|origins| origins.get(&origin)),
            Some(&5)
        );
        let computed = export.meta.compute_content_hash().expect("content hash");
        assert_eq!(export.meta.content_hash, computed);
        assert_eq!(
            export.meta.manifest_hash,
            export.manifest.manifest_hash().expect("manifest hash"),
        );
    }

    #[test]
    fn export_reuses_clean_shards() {
        let namespace = NamespaceId::core();
        let stamp = make_stamp(1, 0, "author");
        let (id_dirty, id_clean) = find_two_ids_different_shards();

        let bead_dirty = make_bead(&id_dirty, &stamp);
        let bead_clean = make_bead(&id_clean, &stamp);
        let mut core_state = CanonicalState::new();
        core_state.insert(bead_dirty.clone()).unwrap();
        core_state.insert(bead_clean.clone()).unwrap();
        let mut state = StoreState::new();
        state.set_namespace_state(namespace.clone(), core_state);

        let origin = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let snapshot = build_test_snapshot(&state, &namespace, origin, 1);
        let export = export_checkpoint(CheckpointExportInput {
            snapshot: &snapshot,
            previous: None,
        })
        .expect("export");

        let mut next_snapshot = snapshot.clone();
        let dirty_path = shard_path(
            &namespace,
            CheckpointFileKind::State,
            &shard_for_bead(&id_dirty),
        );
        let clean_path = shard_path(
            &namespace,
            CheckpointFileKind::State,
            &shard_for_bead(&id_clean),
        );

        next_snapshot.dirty_shards = [dirty_path.clone()].into_iter().collect();
        let dirty_payload = next_snapshot
            .shards
            .get_mut(&dirty_path)
            .expect("dirty shard");
        dirty_payload.bytes = Bytes::from_static(b"{\"dirty\":true}\n");
        let clean_payload = next_snapshot
            .shards
            .get_mut(&clean_path)
            .expect("clean shard");
        clean_payload.bytes = Bytes::from_static(b"{\"corrupt\":true}\n");

        let export_next = export_checkpoint(CheckpointExportInput {
            snapshot: &next_snapshot,
            previous: Some(&export),
        })
        .expect("export next");

        assert_eq!(
            export_next.files.get(&clean_path).unwrap().bytes,
            export.files.get(&clean_path).unwrap().bytes
        );
        assert_eq!(
            export_next.files.get(&dirty_path).unwrap().bytes,
            Bytes::from_static(b"{\"dirty\":true}\n"),
        );
    }

    #[test]
    fn export_writes_sorted_shards() {
        let namespace = NamespaceId::core();
        let stamp = make_stamp(1, 0, "author");
        let (id_a, id_b, shard) = find_two_ids_same_shard();

        let bead_a = make_bead(&id_a, &stamp);
        let bead_b = make_bead(&id_b, &stamp);

        let mut core_state = CanonicalState::new();
        core_state.insert(bead_b.clone()).unwrap();
        core_state.insert(bead_a.clone()).unwrap();
        let mut state = StoreState::new();
        state.set_namespace_state(namespace.clone(), core_state);

        let origin = ReplicaId::new(Uuid::from_bytes([5u8; 16]));
        let snapshot = build_test_snapshot(&state, &namespace, origin, 2);
        let export = export_checkpoint(CheckpointExportInput {
            snapshot: &snapshot,
            previous: None,
        })
        .expect("export");

        let path = shard_path(&namespace, CheckpointFileKind::State, &shard);
        let payload = export.files.get(&path).expect("state shard");

        let mut ids = vec![id_a.clone(), id_b.clone()];
        ids.sort();
        let mut expected = Vec::new();
        let view_state = state.get(&namespace).expect("state");
        for id in ids {
            let view = view_state.bead_view(&id).expect("bead view");
            let label_state = view_state.label_store().state(&id);
            let mut line =
                to_canon_json_bytes(&WireBeadFull::from_view(&view, label_state)).unwrap();
            line.push(b'\n');
            expected.extend_from_slice(&line);
        }

        assert_eq!(payload.bytes.as_ref(), expected);
    }
}
