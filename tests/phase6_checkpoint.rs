//! Phase 6 tests: checkpoint export/import determinism.

mod fixtures;

use std::collections::BTreeMap;
use std::path::Path;

use tempfile::TempDir;
use uuid::Uuid;

use beads_rs::core::ContentHash;
use beads_rs::git::checkpoint::{
    CheckpointExport, CheckpointExportInput, CheckpointImportError, CheckpointSnapshotInput,
    IncludedHeads, IncludedWatermarks, export_checkpoint, import_checkpoint,
};
use beads_rs::{
    ActorId, Bead, BeadCore, BeadFields, BeadId, BeadType, CanonicalState, Claim, DepEdge, DepKey,
    DepKind, Durable, HeadStatus, Labels, Lww, NamespaceId, Priority, ReplicaId, Seq0, Stamp,
    StoreEpoch, StoreId, StoreState, Tombstone, Watermarks, Workflow, WriteStamp,
};

use fixtures::checkpoint::{
    assert_manifest_files, assert_meta_hashes, fixture_multi_namespace, fixture_small_state,
};

#[test]
fn phase6_checkpoint_export_is_deterministic() {
    let fixture = fixture_small_state();
    let export = &fixture.export;
    let export_again = export_checkpoint(CheckpointExportInput {
        snapshot: &fixture.snapshot,
        previous: None,
    })
    .expect("export again");

    let bytes = export.manifest.canon_bytes().expect("manifest bytes");
    let bytes_again = export_again
        .manifest
        .canon_bytes()
        .expect("manifest bytes again");
    assert_eq!(bytes, bytes_again, "manifest bytes drifted");
}

#[test]
fn phase6_checkpoint_manifest_hashes_match_files() {
    let fixture = fixture_small_state();
    assert_manifest_files(&fixture.export.manifest, &fixture.export.files);
    assert_meta_hashes(&fixture.export.meta, &fixture.export.manifest);
}

#[test]
fn phase6_checkpoint_import_rejects_corrupt_files() {
    let fixture = fixture_small_state();
    let temp = TempDir::new().expect("temp checkpoint dir");
    write_checkpoint_tree(temp.path(), &fixture.export).expect("write checkpoint");

    let (path, _) = fixture.export.files.iter().next().expect("export file");
    let target = temp.path().join(path);
    corrupt_jsonl_preserving_syntax(&target).expect("corrupt file");

    let err = import_checkpoint(temp.path(), &beads_rs::Limits::default()).unwrap_err();
    match err {
        CheckpointImportError::FileHashMismatch { .. } => {}
        other => panic!("expected FileHashMismatch, got {other:?}"),
    }
}

#[test]
fn phase6_checkpoint_round_trip_preserves_state_and_manifest() {
    let core = NamespaceId::core();
    let (store_state, _watermarks, expected_export) = build_core_store_state();

    let temp = TempDir::new().expect("temp checkpoint dir");
    write_checkpoint_tree(temp.path(), &expected_export).expect("write checkpoint");

    let imported = import_checkpoint(temp.path(), &beads_rs::Limits::default()).expect("import");
    assert_store_state_stats(&store_state, &imported.state);

    let imported_state = imported.state.get(&core).expect("core state present");
    let imported_watermarks =
        watermarks_from_included(&imported.included, imported.included_heads.as_ref());

    let snapshot = build_snapshot_from_state(
        "core",
        vec![core.clone()],
        expected_export.meta.store_id,
        expected_export.meta.store_epoch,
        expected_export.meta.created_at_ms,
        expected_export.meta.created_by_replica_id,
        expected_export.meta.policy_hash,
        expected_export.meta.roster_hash,
        imported_state,
        &imported_watermarks,
    );
    let export_again = export_checkpoint(CheckpointExportInput {
        snapshot: &snapshot,
        previous: None,
    })
    .expect("export again");

    let bytes = expected_export
        .manifest
        .canon_bytes()
        .expect("manifest bytes");
    let bytes_again = export_again
        .manifest
        .canon_bytes()
        .expect("manifest bytes again");
    assert_eq!(bytes, bytes_again, "manifest bytes drifted");
}

#[test]
fn phase6_checkpoint_multi_namespace_includes_all_namespaces() {
    let fixture = fixture_multi_namespace();
    let mut namespaces = fixture.export.manifest.namespaces.clone();
    namespaces.sort();
    namespaces.dedup();
    assert_eq!(namespaces.len(), 2);

    let files = fixture.export.files.keys().collect::<Vec<_>>();
    assert!(files.iter().any(|path| path.contains("namespaces/core/")));
    assert!(files.iter().any(|path| path.contains("namespaces/sys/")));
}

#[test]
fn phase6_checkpoint_included_watermarks_match() {
    let core = NamespaceId::core();
    let (store_state, watermarks, export) = build_core_store_state();

    let expected_included = included_from_watermarks(&watermarks, &[core.clone()]);
    assert_eq!(export.meta.included, expected_included);
    assert!(export.meta.included_heads.is_some());

    let temp = TempDir::new().expect("temp checkpoint dir");
    write_checkpoint_tree(temp.path(), &export).expect("write checkpoint");
    let imported = import_checkpoint(temp.path(), &beads_rs::Limits::default()).expect("import");
    assert_store_state_stats(&store_state, &imported.state);
}

fn build_core_store_state() -> (StoreState, Watermarks<Durable>, CheckpointExport) {
    let core = NamespaceId::core();
    let stamp = make_stamp(1_700_000_000_000, 1, "author");
    let bead_id = BeadId::parse("bd-core").expect("bead id");
    let other_id = BeadId::parse("bd-other").expect("bead id");

    let mut state = CanonicalState::new();
    state
        .insert(make_bead(&bead_id, &stamp, "core"))
        .expect("insert");
    state
        .insert(make_bead(&other_id, &stamp, "other"))
        .expect("insert");
    let tombstone = Tombstone::new(other_id.clone(), stamp.clone(), Some("removed".into()));
    state.insert_tombstone(tombstone);

    let dep_key = DepKey::new(bead_id.clone(), other_id.clone(), DepKind::Blocks).expect("dep key");
    let dep_edge = DepEdge::new(stamp.clone());
    state.insert_dep(dep_key, dep_edge);

    let mut store_state = StoreState::new();
    *store_state.ensure_namespace(core.clone()) = state.clone();

    let mut watermarks = Watermarks::<Durable>::new();
    let origin = ReplicaId::new(Uuid::from_bytes([3u8; 16]));
    watermarks
        .observe_at_least(&core, &origin, Seq0::new(2), HeadStatus::Known([2u8; 32]))
        .expect("watermark");

    let snapshot = build_snapshot_from_state(
        "core",
        vec![core],
        StoreId::new(Uuid::from_bytes([4u8; 16])),
        StoreEpoch::new(0),
        1_700_000_000_000,
        origin,
        ContentHash::from_bytes([9u8; 32]),
        None,
        &state,
        &watermarks,
    );
    let export = export_checkpoint(CheckpointExportInput {
        snapshot: &snapshot,
        previous: None,
    })
    .expect("export");

    (store_state, watermarks, export)
}

fn build_snapshot_from_state(
    checkpoint_group: &str,
    namespaces: Vec<NamespaceId>,
    store_id: StoreId,
    store_epoch: StoreEpoch,
    created_at_ms: u64,
    created_by_replica_id: ReplicaId,
    policy_hash: ContentHash,
    roster_hash: Option<ContentHash>,
    state: &CanonicalState,
    watermarks: &Watermarks<Durable>,
) -> beads_rs::git::checkpoint::CheckpointSnapshot {
    beads_rs::git::checkpoint::build_snapshot(CheckpointSnapshotInput {
        checkpoint_group: checkpoint_group.to_string(),
        namespaces,
        store_id,
        store_epoch,
        created_at_ms,
        created_by_replica_id,
        policy_hash,
        roster_hash,
        state,
        watermarks_durable: watermarks,
    })
    .expect("snapshot")
}

fn included_from_watermarks(
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

fn watermarks_from_included(
    included: &IncludedWatermarks,
    heads: Option<&IncludedHeads>,
) -> Watermarks<Durable> {
    let mut watermarks = Watermarks::<Durable>::new();
    for (namespace, origins) in included {
        for (origin, seq) in origins {
            let head = heads
                .and_then(|heads| heads.get(namespace))
                .and_then(|origin_heads| origin_heads.get(origin))
                .map(|hash| HeadStatus::Known(*hash.as_bytes()))
                .unwrap_or_else(|| {
                    if *seq == 0 {
                        HeadStatus::Genesis
                    } else {
                        HeadStatus::Unknown
                    }
                });
            watermarks
                .observe_at_least(namespace, origin, Seq0::new(*seq), head)
                .expect("watermark");
        }
    }
    watermarks
}

fn assert_store_state_stats(expected: &StoreState, actual: &StoreState) {
    let expected_stats = store_state_stats(expected);
    let actual_stats = store_state_stats(actual);
    assert_eq!(expected_stats, actual_stats, "store state mismatch");
}

fn store_state_stats(state: &StoreState) -> BTreeMap<NamespaceId, (usize, usize, usize)> {
    let mut stats = BTreeMap::new();
    for (namespace, state) in state.namespaces() {
        stats.insert(
            namespace.clone(),
            (
                state.live_count(),
                state.tombstone_count(),
                state.dep_count(),
            ),
        );
    }
    stats
}

fn write_checkpoint_tree(dir: &Path, export: &CheckpointExport) -> std::io::Result<()> {
    write_bytes(
        &dir.join("meta.json"),
        &export.meta.canon_bytes().expect("meta bytes"),
    )?;
    write_bytes(
        &dir.join("manifest.json"),
        &export.manifest.canon_bytes().expect("manifest bytes"),
    )?;

    for (path, payload) in &export.files {
        let file_path = dir.join(path);
        write_bytes(&file_path, payload.bytes.as_ref())?;
    }
    Ok(())
}

fn write_bytes(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, bytes)
}

fn corrupt_jsonl_preserving_syntax(path: &Path) -> std::io::Result<()> {
    let mut bytes = std::fs::read(path)?;
    if bytes.is_empty() {
        bytes.push(b'0');
        return std::fs::write(path, bytes);
    }

    if replace_bytes(&mut bytes, b"bd-small", b"bd-smoll") {
        return std::fs::write(path, bytes);
    }

    let mut in_string = false;
    let mut escaped = false;
    for byte in bytes.iter_mut() {
        if escaped {
            escaped = false;
            continue;
        }
        if *byte == b'\\' {
            escaped = true;
            continue;
        }
        if *byte == b'"' {
            in_string = !in_string;
            continue;
        }
        if in_string && byte.is_ascii_lowercase() {
            *byte = if *byte == b'a' { b'b' } else { b'a' };
            break;
        }
    }
    std::fs::write(path, bytes)
}

fn replace_bytes(buf: &mut [u8], needle: &[u8], replacement: &[u8]) -> bool {
    if needle.len() != replacement.len() {
        return false;
    }
    if let Some(pos) = buf
        .windows(needle.len())
        .position(|window| window == needle)
    {
        buf[pos..pos + needle.len()].copy_from_slice(replacement);
        return true;
    }
    false
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
        labels: Lww::new(Labels::new(), stamp.clone()),
        external_ref: Lww::new(None, stamp.clone()),
        source_repo: Lww::new(None, stamp.clone()),
        estimated_minutes: Lww::new(None, stamp.clone()),
        workflow: Lww::new(Workflow::default(), stamp.clone()),
        claim: Lww::new(Claim::default(), stamp.clone()),
    };
    Bead::new(core, fields)
}
