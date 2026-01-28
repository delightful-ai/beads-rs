#![allow(dead_code)]

use std::collections::BTreeMap;

use beads_rs::core::{ContentHash, Dot};
use beads_rs::git::checkpoint::{
    CheckpointExport, CheckpointExportInput, CheckpointFileKind, CheckpointManifest,
    CheckpointMeta, CheckpointShardPath, CheckpointShardPayload, CheckpointSnapshot,
    CheckpointSnapshotInput, SHARD_COUNT, export_checkpoint, shard_name,
};
use beads_rs::{
    ActorId, Bead, BeadCore, BeadFields, BeadId, BeadType, CanonicalState, Claim, DepKey, Durable,
    HeadStatus, Lww, NamespaceId, Priority, ReplicaId, Seq0, Stamp, StoreState, Tombstone,
    Watermarks, Workflow, WriteStamp, sha256_bytes,
};

use super::identity;

pub struct CheckpointFixture {
    pub snapshot: CheckpointSnapshot,
    pub export: CheckpointExport,
    pub manifest_json: Vec<u8>,
    pub meta_json: Vec<u8>,
}

impl CheckpointFixture {
    fn from_snapshot(snapshot: CheckpointSnapshot) -> Self {
        let export = export_checkpoint(CheckpointExportInput {
            snapshot: &snapshot,
            previous: None,
        })
        .expect("export checkpoint");
        let manifest_json = export.manifest.canon_bytes().expect("manifest canon bytes");
        let meta_json = export.meta.canon_bytes().expect("meta canon bytes");
        Self {
            snapshot,
            export,
            manifest_json,
            meta_json,
        }
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
    let origin = identity::replica_id(1);
    observe_watermark(&mut watermarks, &namespace, &origin, 3, [3u8; 32]);

    CheckpointFixture::from_snapshot(build_fixture_snapshot(
        "core",
        vec![namespace],
        &states,
        &watermarks,
    ))
}

pub fn fixture_tombstones() -> CheckpointFixture {
    let namespace = NamespaceId::core();
    let stamp = make_stamp(1_700_000_000_000, 2, "fixture");
    let live_id = BeadId::parse("bd-live").expect("bead id");
    let dead_id = BeadId::parse("bd-dead").expect("bead id");

    let bead = make_bead(&live_id, &stamp, "live");
    let tombstone = Tombstone::new(dead_id.clone(), stamp.clone(), Some("gone".into()));

    let state = build_state(vec![bead], vec![tombstone], Vec::new());
    let mut states = BTreeMap::new();
    states.insert(namespace.clone(), state);

    let mut watermarks = Watermarks::<Durable>::new();
    let origin = identity::replica_id(2);
    observe_watermark(&mut watermarks, &namespace, &origin, 7, [7u8; 32]);

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
    observe_watermark(
        &mut watermarks,
        &core,
        &identity::replica_id(3),
        5,
        [5u8; 32],
    );
    observe_watermark(
        &mut watermarks,
        &sys,
        &identity::replica_id(4),
        2,
        [2u8; 32],
    );

    CheckpointFixture::from_snapshot(build_fixture_snapshot(
        "core",
        vec![core, sys],
        &states,
        &watermarks,
    ))
}

pub fn state_shard_paths(namespace: &NamespaceId) -> Vec<CheckpointShardPath> {
    shard_paths(namespace, CheckpointFileKind::State)
}

pub fn tombstone_shard_paths(namespace: &NamespaceId) -> Vec<CheckpointShardPath> {
    shard_paths(namespace, CheckpointFileKind::Tombstones)
}

pub fn dep_shard_paths(namespace: &NamespaceId) -> Vec<CheckpointShardPath> {
    shard_paths(namespace, CheckpointFileKind::Deps)
}

pub fn shard_paths(namespace: &NamespaceId, kind: CheckpointFileKind) -> Vec<CheckpointShardPath> {
    let mut paths = Vec::with_capacity(SHARD_COUNT);
    for i in 0..SHARD_COUNT {
        paths.push(CheckpointShardPath::new(
            namespace.clone(),
            kind,
            shard_name(i as u8),
        ));
    }
    paths
}

pub fn build_manifest_from_files(
    checkpoint_group: &str,
    store_id: beads_rs::StoreId,
    store_epoch: beads_rs::StoreEpoch,
    namespaces: Vec<NamespaceId>,
    files: &BTreeMap<CheckpointShardPath, CheckpointShardPayload>,
) -> CheckpointManifest {
    let mut manifest_files = BTreeMap::new();
    for (path, payload) in files {
        let hash = ContentHash::from_bytes(sha256_bytes(payload.bytes.as_ref()).0);
        manifest_files.insert(
            path.clone(),
            beads_rs::git::checkpoint::ManifestFile {
                sha256: hash,
                bytes: payload.bytes.len() as u64,
            },
        );
    }
    CheckpointManifest {
        checkpoint_group: checkpoint_group.to_string(),
        store_id,
        store_epoch,
        namespaces,
        files: manifest_files,
    }
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
        let hash = ContentHash::from_bytes(sha256_bytes(payload.bytes.as_ref()).0);
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
        workflow: Lww::new(Workflow::default(), stamp.clone()),
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
        if namespace.is_core() {
            store_state.set_core_state(state.clone());
        } else {
            let non_core = namespace
                .clone()
                .try_non_core()
                .expect("non-core namespace");
            store_state.set_namespace_state(non_core, state.clone());
        }
    }

    beads_rs::git::checkpoint::build_snapshot(CheckpointSnapshotInput {
        checkpoint_group: checkpoint_group.to_string(),
        namespaces,
        store_id: identity::store_id(1),
        store_epoch: beads_rs::StoreEpoch::new(0),
        created_at_ms: 1_700_000_000_000,
        created_by_replica_id: identity::replica_id(9),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixtures_checkpoint_small_manifest_and_meta_hashes() {
        let fixture = fixture_small_state();
        assert_manifest_files(&fixture.export.manifest, &fixture.export.files);
        assert_meta_hashes(&fixture.export.meta, &fixture.export.manifest);
    }

    #[test]
    fn fixtures_checkpoint_tombstone_has_tombstone_shards() {
        let fixture = fixture_tombstones();
        let namespace = NamespaceId::core();
        let tomb_paths = tombstone_shard_paths(&namespace);
        assert!(
            fixture
                .export
                .files
                .keys()
                .any(|path| tomb_paths.contains(path))
        );
    }

    #[test]
    fn fixtures_checkpoint_multi_namespace_contains_two_namespaces() {
        let fixture = fixture_multi_namespace();
        assert_eq!(fixture.export.manifest.namespaces.len(), 2);
    }
}
