#![allow(dead_code)]

use std::collections::BTreeMap;

use beads_core::{
    ActorId, Bead, BeadCore, BeadFields, BeadId, BeadType, CanonicalState, Claim, DepKey, Durable,
    HeadStatus, IssueStatus, Lww, NamespaceId, Priority, ReplicaId, Seq0, Stamp, StoreState,
    Tombstone, Watermarks, WriteStamp,
};
use beads_core::{ContentHash, Dot, StoreEpoch, StoreId};
use beads_git::checkpoint::{
    CheckpointExport, CheckpointExportInput, CheckpointManifest, CheckpointMeta,
    CheckpointShardPath, CheckpointShardPayload, CheckpointSnapshot, CheckpointSnapshotInput,
    export_checkpoint,
};
use uuid::Uuid;

pub struct CheckpointFixture {
    pub snapshot: CheckpointSnapshot,
    pub export: CheckpointExport,
}

impl CheckpointFixture {
    fn from_snapshot(snapshot: CheckpointSnapshot) -> Self {
        let export = export_checkpoint(CheckpointExportInput {
            snapshot: &snapshot,
            previous: None,
        })
        .expect("export checkpoint");
        Self { snapshot, export }
    }
}

pub fn fixture_small_state() -> CheckpointFixture {
    let namespace = NamespaceId::core();
    let stamp = make_stamp(1_700_000_000_000, 1, "fixture");
    let bead_id = BeadId::parse("bd-small").expect("bead id");
    let bead = make_bead(&bead_id, &stamp, "small");

    let state = build_state(vec![bead], Vec::new(), Vec::new());
    let mut states = BTreeMap::new();
    states.insert(namespace.clone(), state);

    let mut watermarks = Watermarks::<Durable>::new();
    let origin = replica_id(1);
    observe_watermark(&mut watermarks, &namespace, &origin, 3, [3u8; 32]);

    CheckpointFixture::from_snapshot(build_fixture_snapshot(
        "core",
        vec![namespace],
        &states,
        &watermarks,
    ))
}

pub fn fixture_multi_namespace() -> CheckpointFixture {
    let core = NamespaceId::core();
    let sys = NamespaceId::parse("sys").expect("sys namespace");

    let stamp = make_stamp(1_700_000_100_000, 1, "fixture");
    let core_id = BeadId::parse("bd-core").expect("bead id");
    let sys_id = BeadId::parse("bd-sys").expect("bead id");

    let core_state = build_state(
        vec![make_bead(&core_id, &stamp, "core")],
        Vec::new(),
        Vec::new(),
    );
    let sys_state = build_state(
        vec![make_bead(&sys_id, &stamp, "sys")],
        Vec::new(),
        Vec::new(),
    );

    let mut states = BTreeMap::new();
    states.insert(core.clone(), core_state);
    states.insert(sys.clone(), sys_state);

    let mut watermarks = Watermarks::<Durable>::new();
    observe_watermark(&mut watermarks, &core, &replica_id(3), 5, [5u8; 32]);
    observe_watermark(&mut watermarks, &sys, &replica_id(4), 2, [2u8; 32]);

    CheckpointFixture::from_snapshot(build_fixture_snapshot(
        "core",
        vec![core, sys],
        &states,
        &watermarks,
    ))
}

pub fn assert_manifest_files(
    manifest: &CheckpointManifest,
    files: &BTreeMap<CheckpointShardPath, CheckpointShardPayload>,
) {
    let mut errors = Vec::new();
    for (path, entry) in &manifest.files {
        let payload = match files.get(path) {
            Some(payload) => payload,
            None => {
                errors.push(format!("manifest references missing file {path}"));
                continue;
            }
        };
        let hash = ContentHash::from_bytes(beads_core::sha256_bytes(payload.bytes.as_ref()).0);
        if hash != entry.sha256 {
            errors.push(format!(
                "file hash mismatch for {path}: expected {}, got {}",
                entry.sha256, hash
            ));
        }
        let bytes = payload.bytes.len() as u64;
        if bytes != entry.bytes {
            errors.push(format!(
                "file size mismatch for {path}: expected {}, got {}",
                entry.bytes, bytes
            ));
        }
    }
    for path in files.keys() {
        if !manifest.files.contains_key(path) {
            errors.push(format!("file {path} missing from manifest"));
        }
    }
    if !errors.is_empty() {
        panic!("manifest validation failed:\n{}", errors.join("\n"));
    }
}

pub fn assert_meta_hashes(meta: &CheckpointMeta, manifest: &CheckpointManifest) {
    let computed_manifest = manifest.manifest_hash().expect("manifest hash");
    assert_eq!(
        meta.manifest_hash, computed_manifest,
        "manifest hash mismatch"
    );
    let computed_content = meta.compute_content_hash().expect("content hash");
    assert_eq!(meta.content_hash, computed_content, "content hash mismatch");
}

fn store_id(seed: u8) -> StoreId {
    StoreId::new(Uuid::from_bytes([seed; 16]))
}

fn replica_id(seed: u8) -> ReplicaId {
    ReplicaId::new(Uuid::from_bytes([seed; 16]))
}

fn make_stamp(wall_ms: u64, counter: u32, actor: &str) -> Stamp {
    Stamp::new(
        WriteStamp::new(wall_ms, counter),
        ActorId::new(actor).expect("actor id"),
    )
}

fn make_bead(id: &BeadId, stamp: &Stamp, title: &str) -> Bead {
    let core = BeadCore::new(id.clone(), stamp.clone(), None);
    let fields = BeadFields {
        title: Lww::new(title.to_string(), stamp.clone()),
        description: Lww::new(String::new(), stamp.clone()),
        design: Lww::new(None, stamp.clone()),
        acceptance_criteria: Lww::new(None, stamp.clone()),
        priority: Lww::new(Priority::default(), stamp.clone()),
        bead_type: Lww::new(BeadType::Task, stamp.clone()),
        external_ref: Lww::new(None, stamp.clone()),
        source_repo: Lww::new(None, stamp.clone()),
        estimated_minutes: Lww::new(None, stamp.clone()),
        status: Lww::new(IssueStatus::Todo, stamp.clone()),
        closed_on_branch: Lww::new(None, stamp.clone()),
        claim: Lww::new(Claim::default(), stamp.clone()),
    };
    Bead::new(core, fields)
}

fn build_state(
    beads: Vec<Bead>,
    tombstones: Vec<Tombstone>,
    deps: Vec<(DepKey, Dot, Stamp)>,
) -> CanonicalState {
    let mut state = CanonicalState::new();
    for bead in beads {
        state.insert(bead).expect("insert bead");
    }
    for tombstone in tombstones {
        state.insert_tombstone(tombstone);
    }
    for (key, dot, stamp) in deps {
        let key = state.check_dep_add_key(key).expect("dep key valid");
        state.apply_dep_add(key, dot, stamp);
    }
    state
}

fn build_fixture_snapshot(
    checkpoint_group: &str,
    namespaces: Vec<NamespaceId>,
    states: &BTreeMap<NamespaceId, CanonicalState>,
    watermarks: &Watermarks<Durable>,
) -> CheckpointSnapshot {
    let mut store_state = StoreState::new();
    for (namespace, state) in states {
        store_state.set_namespace_state(namespace.clone(), state.clone());
    }

    beads_git::checkpoint::build_snapshot(CheckpointSnapshotInput {
        checkpoint_group: checkpoint_group.to_string(),
        namespaces: namespaces.into(),
        store_id: store_id(1),
        store_epoch: StoreEpoch::new(0),
        created_at_ms: 1_700_000_000_000,
        created_by_replica_id: replica_id(9),
        policy_hash: ContentHash::from_bytes([9u8; 32]),
        roster_hash: None,
        dirty_shards: None,
        state: &store_state,
        watermarks_durable: watermarks,
    })
    .expect("snapshot")
}

fn observe_watermark(
    watermarks: &mut Watermarks<Durable>,
    namespace: &NamespaceId,
    origin: &ReplicaId,
    seq: u64,
    head: [u8; 32],
) {
    watermarks
        .observe_at_least(namespace, origin, Seq0::new(seq), HeadStatus::Known(head))
        .expect("watermark");
}
