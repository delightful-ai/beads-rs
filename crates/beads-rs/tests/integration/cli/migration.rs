//! Integration tests for migration from beads-go.
//!
//! Tests importing issues.jsonl from Go beads export format.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::time::Duration;

use crate::fixtures::bd_runtime::{BdRuntimeRepo, wait_for_store_id};
use crate::fixtures::daemon_runtime::shutdown_daemon;
use crate::fixtures::legacy_store::{
    backup_ref_oid, backup_ref_targets, create_backup_ref, create_detached_store_commit,
    fetch_remote_store_ref, fixture, push_store_ref, read_store_blob, read_store_meta_json,
    read_store_state, rewrite_store_meta, rewrite_store_ref_commit, set_ref_target,
    store_first_parent_oid, store_ref_oid, wait_for_fetched_remote_store_ref,
    write_nonempty_strict_store_commit, write_store_commit, write_strict_store_commit,
};
use beads_core::{BeadId, CanonicalState, Claim, DepKey, DepKind, StoreMetaVersions, WriteStamp};
use beads_git::sync::migrate_store_ref_to_v2_with_before_push_for_testing;
use beads_git::wire::{StoreChecksums, serialize_meta};
use git2::{ObjectType, Repository};
use predicates::prelude::*;

type TestRepo = BdRuntimeRepo;

/// Sample Go beads JSONL export with various issue types.
fn sample_go_export() -> &'static str {
    r#"{"id":"bd-abc1","title":"Open task","description":"A task that is open","status":"open","priority":1,"issue_type":"task","created_at":"2025-01-01T10:00:00Z","updated_at":"2025-01-01T10:00:00Z"}
{"id":"bd-abc2","title":"Bug to fix","description":"Something is broken","status":"in_progress","priority":0,"issue_type":"bug","assignee":"alice","created_at":"2025-01-02T10:00:00Z","updated_at":"2025-01-02T12:00:00Z"}
{"id":"bd-abc3","title":"Completed feature","description":"All done","status":"closed","priority":2,"issue_type":"feature","created_at":"2025-01-03T10:00:00Z","updated_at":"2025-01-03T15:00:00Z","closed_at":"2025-01-03T15:00:00Z","close_reason":"Shipped it"}
"#
}

fn note_ids_for(state: &CanonicalState, id: &BeadId) -> BTreeSet<String> {
    state
        .notes_for(id)
        .iter()
        .map(|note| note.id.as_str().to_owned())
        .collect()
}

fn expected_note_ids(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| (*id).to_owned()).collect()
}

fn assert_note_payload(
    state: &CanonicalState,
    bead: &BeadId,
    note_id: &str,
    content: &str,
    author: &str,
    wall_ms: u64,
    counter: u32,
) {
    let note = state
        .notes_for(bead)
        .into_iter()
        .find(|note| note.id.as_str() == note_id)
        .unwrap_or_else(|| panic!("missing note {note_id} on {}", bead.as_str()));
    assert_eq!(
        note.content, content,
        "note {note_id} content should be preserved"
    );
    assert_eq!(
        note.author.as_str(),
        author,
        "note {note_id} author should be preserved"
    );
    assert_eq!(
        note.at.wall_ms, wall_ms,
        "note {note_id} wall_ms should be preserved"
    );
    assert_eq!(
        note.at.counter, counter,
        "note {note_id} counter should be preserved"
    );
}

fn assert_meta_checksums_match_store(repo_path: &Path) {
    let state_bytes = read_store_blob(repo_path, "state.jsonl").expect("state.jsonl");
    let tombs_bytes = read_store_blob(repo_path, "tombstones.jsonl").expect("tombstones.jsonl");
    let deps_bytes = read_store_blob(repo_path, "deps.jsonl").expect("deps.jsonl");
    let notes_bytes = read_store_blob(repo_path, "notes.jsonl").expect("notes.jsonl");
    let checksums =
        StoreChecksums::from_bytes(&state_bytes, &tombs_bytes, &deps_bytes, Some(&notes_bytes));
    let meta_json = read_store_meta_json(repo_path);
    let state_sha = checksums.state.to_string();
    let tombs_sha = checksums.tombstones.to_string();
    let deps_sha = checksums.deps.to_string();
    let notes_sha = checksums.notes.expect("notes checksum").to_string();
    assert_eq!(
        meta_json
            .get("state_sha256")
            .and_then(|value| value.as_str()),
        Some(state_sha.as_str()),
        "meta.json should carry the recomputed state checksum"
    );
    assert_eq!(
        meta_json
            .get("tombstones_sha256")
            .and_then(|value| value.as_str()),
        Some(tombs_sha.as_str()),
        "meta.json should carry the recomputed tombstones checksum"
    );
    assert_eq!(
        meta_json
            .get("deps_sha256")
            .and_then(|value| value.as_str()),
        Some(deps_sha.as_str()),
        "meta.json should carry the recomputed deps checksum"
    );
    assert_eq!(
        meta_json
            .get("notes_sha256")
            .and_then(|value| value.as_str()),
        Some(notes_sha.as_str()),
        "meta.json should carry the recomputed notes checksum"
    );
}

fn read_tree_blob(repo: &Repository, name: &str) -> Option<Vec<u8>> {
    let repo_path = repo.workdir().unwrap_or(repo.path());
    read_store_blob(repo_path, name)
}

fn run_bd_json(repo: &TestRepo, args: &[&str]) -> serde_json::Value {
    let output = repo
        .bd()
        .args(args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    serde_json::from_slice(&output).expect("json")
}

fn assert_detect_outcome(
    json: &serde_json::Value,
    meta_format_version: Option<u64>,
    deps_format: &str,
    notes_present: bool,
    checksums_present: bool,
    effective_format_version: u64,
    needs_migration: bool,
    expected_reasons: &[&str],
) {
    assert_eq!(
        json.get("meta_format_version")
            .and_then(|value| value.as_u64()),
        meta_format_version,
        "unexpected meta_format_version: {json}"
    );
    assert_eq!(
        json.get("deps_format").and_then(|value| value.as_str()),
        Some(deps_format),
        "unexpected deps_format: {json}"
    );
    assert_eq!(
        json.get("notes_present").and_then(|value| value.as_bool()),
        Some(notes_present),
        "unexpected notes_present: {json}"
    );
    assert_eq!(
        json.get("checksums_present")
            .and_then(|value| value.as_bool()),
        Some(checksums_present),
        "unexpected checksums_present: {json}"
    );
    assert_eq!(
        json.get("effective_format_version")
            .and_then(|value| value.as_u64()),
        Some(effective_format_version),
        "unexpected effective_format_version: {json}"
    );
    assert_eq!(
        json.get("needs_migration")
            .and_then(|value| value.as_bool()),
        Some(needs_migration),
        "unexpected needs_migration: {json}"
    );

    let mut actual_reasons: Vec<String> = json
        .get("reasons")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_str().map(ToOwned::to_owned))
        .collect();
    actual_reasons.sort();

    let mut expected_reasons: Vec<String> = expected_reasons
        .iter()
        .map(|reason| (*reason).to_owned())
        .collect();
    expected_reasons.sort();
    assert_eq!(
        actual_reasons, expected_reasons,
        "unexpected migration reasons: {json}"
    );
}

fn bead_id(raw: &str) -> BeadId {
    BeadId::parse(raw).expect("valid bead id")
}

fn dep_key(from: &str, to: &str, kind: DepKind) -> DepKey {
    DepKey::new(bead_id(from), bead_id(to), kind).expect("valid dep key")
}

fn assert_rich_workflow_state(
    state: &CanonicalState,
    expect_related_dep: bool,
    expect_peer_bead: bool,
    expected_claimed_notes: &[&str],
) {
    let claimed_id = bead_id("bd-rich-claimed");
    let claimed = state
        .bead_view(&claimed_id)
        .expect("claimed bead should exist after migration");
    assert!(
        matches!(claimed.bead.fields.claim.value, Claim::Claimed { .. }),
        "claimed bead should remain claimed"
    );
    assert_eq!(
        note_ids_for(state, &claimed_id),
        expected_note_ids(expected_claimed_notes),
        "claimed bead should preserve the exact embedded note ids through migration"
    );
    assert_note_payload(
        state,
        &claimed_id,
        "note-rich-claimed-a",
        "First embedded note",
        "darin@dusk",
        1_765_400_000_100,
        0,
    );
    if expected_claimed_notes.contains(&"note-rich-claimed-b") {
        assert_note_payload(
            state,
            &claimed_id,
            "note-rich-claimed-b",
            "Second embedded note",
            "alice",
            1_765_400_000_200,
            0,
        );
    }
    if expected_claimed_notes.contains(&"note-rich-claimed-c") {
        assert_note_payload(
            state,
            &claimed_id,
            "note-rich-claimed-c",
            "Peer-side note for divergence coverage",
            "alice",
            1_765_400_000_250,
            0,
        );
    }
    assert!(
        state
            .dep_store()
            .contains(&dep_key("bd-rich-claimed", "bd-rich-open", DepKind::Blocks)),
        "primary blocks dependency should survive migration"
    );
    assert_eq!(
        state
            .dep_store()
            .dots_for(&dep_key("bd-rich-claimed", "bd-rich-open", DepKind::Blocks))
            .map(|dots| dots.len()),
        Some(1),
        "shared local/remote legacy edge should converge to one deterministic dot"
    );

    let closed = state
        .bead_view(&bead_id("bd-rich-closed"))
        .expect("closed bead should exist after migration");
    assert!(
        closed.bead.fields.status.value.is_terminal(),
        "closed bead should remain closed"
    );
    assert_eq!(
        note_ids_for(state, &bead_id("bd-rich-closed")),
        expected_note_ids(&["note-rich-closed-a"]),
        "closed bead should preserve its note identity"
    );
    assert_note_payload(
        state,
        &bead_id("bd-rich-closed"),
        "note-rich-closed-a",
        "Closed with legacy redundant fields omitted",
        "darin@dusk",
        1_765_400_001_300,
        0,
    );

    let open = state
        .bead_view(&bead_id("bd-rich-open"))
        .expect("open bead should exist after migration");
    assert!(
        open.labels.contains("migration"),
        "legacy labels should survive migration"
    );
    assert!(
        open.labels.contains("frontend"),
        "legacy labels array should survive migration"
    );
    assert_eq!(
        note_ids_for(state, &bead_id("bd-rich-open")),
        expected_note_ids(&["note-rich-open-a"]),
        "open bead should preserve its note identity"
    );
    assert_note_payload(
        state,
        &bead_id("bd-rich-open"),
        "note-rich-open-a",
        "Open note from legacy state",
        "codex@book",
        1_765_400_002_100,
        0,
    );
    if expect_related_dep {
        assert!(
            state.dep_store().contains(&dep_key(
                "bd-rich-open",
                "bd-rich-closed",
                DepKind::Related
            )),
            "alias-shaped related dependency should survive migration"
        );
        assert_eq!(
            state
                .dep_store()
                .dots_for(&dep_key("bd-rich-open", "bd-rich-closed", DepKind::Related))
                .map(|dots| dots.len()),
            Some(1),
            "alias-shaped related dependency should migrate to a single deterministic dot"
        );
    }

    if expect_peer_bead {
        assert!(
            state.bead_view(&bead_id("bd-rich-review")).is_some(),
            "peer-only bead should be present after divergence merge"
        );
        assert!(
            claimed.labels.contains("peer"),
            "peer-side label should survive divergence merge"
        );
        assert!(
            open.labels.contains("api"),
            "peer-side legacy label should survive divergence merge"
        );
        assert!(
            state
                .dep_store()
                .contains(&dep_key("bd-rich-review", "bd-rich-open", DepKind::Blocks)),
            "peer-only dependency should survive divergence merge"
        );
        assert_eq!(
            state
                .dep_store()
                .dots_for(&dep_key("bd-rich-review", "bd-rich-open", DepKind::Blocks))
                .map(|dots| dots.len()),
            Some(1),
            "peer-only dependency should migrate to a single deterministic dot"
        );
    }
}

fn assert_tombstone_deleted_dep_state(state: &CanonicalState) {
    let live_id = bead_id("bd-tomb-live");
    assert!(
        state.bead_view(&live_id).is_some(),
        "live bead should survive migration"
    );
    assert_eq!(
        note_ids_for(state, &live_id),
        expected_note_ids(&["note-tomb-live-a"]),
        "tombstone fixture should preserve the embedded note id through notes backfill"
    );
    assert_note_payload(
        state,
        &live_id,
        "note-tomb-live-a",
        "Fixture note for deleted dep coverage",
        "darin@dusk",
        1_765_400_011_100,
        0,
    );
    assert!(
        state
            .iter_tombstones()
            .any(|(_, tomb)| tomb.id == bead_id("bd-tomb-gone")),
        "tombstone should survive migration"
    );
    assert!(
        state
            .dep_store()
            .contains(&dep_key("bd-tomb-live", "bd-tomb-target", DepKind::Blocks)),
        "active legacy dep should survive migration"
    );
    assert!(
        !state
            .dep_store()
            .contains(&dep_key("bd-tomb-live", "bd-tomb-gone", DepKind::Related)),
        "deleted legacy dep should remain absent after migration"
    );
}

/// Sample Go beads JSONL export using a repo-name slug (beads-go default).
#[cfg(feature = "slow-tests")]
fn sample_go_export_repo_slug() -> &'static str {
    r#"{"id":"beads-rs-abc1","title":"Open task","description":"A task that is open","status":"open","priority":1,"issue_type":"task","created_at":"2025-01-01T10:00:00Z","updated_at":"2025-01-01T10:00:00Z"}
{"id":"beads-rs-abc2","title":"Bug to fix","description":"Something is broken","status":"in_progress","priority":0,"issue_type":"bug","assignee":"alice","created_at":"2025-01-02T10:00:00Z","updated_at":"2025-01-02T12:00:00Z"}
"#
}

/// Sample with dependencies.
#[cfg(feature = "slow-tests")]
fn sample_with_deps() -> &'static str {
    r#"{"id":"bd-dep1","title":"Foundation","description":"Build the base","status":"open","priority":1,"issue_type":"task","created_at":"2025-01-01T10:00:00Z","updated_at":"2025-01-01T10:00:00Z"}
{"id":"bd-dep2","title":"Feature on top","description":"Needs foundation","status":"open","priority":1,"issue_type":"feature","dependencies":[{"issue_id":"bd-dep2","depends_on_id":"bd-dep1","type":"blocks","created_at":"2025-01-01T10:00:00Z"}],"created_at":"2025-01-01T11:00:00Z","updated_at":"2025-01-01T11:00:00Z"}
"#
}

/// Sample with labels.
#[cfg(feature = "slow-tests")]
fn sample_with_labels() -> &'static str {
    r#"{"id":"bd-lbl1","title":"Labeled issue","description":"Has some labels","status":"open","priority":2,"issue_type":"task","labels":["tech-debt","backend"],"created_at":"2025-01-01T10:00:00Z","updated_at":"2025-01-01T10:00:00Z"}
"#
}

/// Sample with comments/notes.
#[cfg(feature = "slow-tests")]
fn sample_with_comments() -> &'static str {
    r#"{"id":"bd-cmt1","title":"Issue with discussion","description":"Has comments","status":"open","priority":2,"issue_type":"task","comments":[{"id":1,"issue_id":"bd-cmt1","author":"bob","text":"First comment","created_at":"2025-01-01T11:00:00Z"},{"id":2,"issue_id":"bd-cmt1","author":"alice","text":"Reply here","created_at":"2025-01-01T12:00:00Z"}],"created_at":"2025-01-01T10:00:00Z","updated_at":"2025-01-01T12:00:00Z"}
"#
}

/// Sample epic with hierarchical IDs.
#[cfg(feature = "slow-tests")]
fn sample_epic() -> &'static str {
    r#"{"id":"bd-epic","title":"Big project","description":"An epic","status":"open","priority":1,"issue_type":"epic","created_at":"2025-01-01T10:00:00Z","updated_at":"2025-01-01T10:00:00Z"}
{"id":"bd-epic.1","title":"Subtask one","description":"First part","status":"open","priority":2,"issue_type":"task","created_at":"2025-01-01T11:00:00Z","updated_at":"2025-01-01T11:00:00Z"}
{"id":"bd-epic.2","title":"Subtask two","description":"Second part","status":"closed","priority":2,"issue_type":"task","created_at":"2025-01-01T12:00:00Z","updated_at":"2025-01-01T13:00:00Z","closed_at":"2025-01-01T13:00:00Z"}
"#
}

/// Sample tombstone (deleted issue).
#[cfg(feature = "slow-tests")]
fn sample_tombstone() -> &'static str {
    r#"{"id":"bd-tomb","title":"(deleted)","description":"","status":"tombstone","priority":4,"issue_type":"task","deleted_at":"2025-01-05T10:00:00Z","deleted_by":"admin","delete_reason":"Duplicate","created_at":"2025-01-01T10:00:00Z","updated_at":"2025-01-05T10:00:00Z"}
"#
}

#[test]
fn test_migrate_dry_run() {
    let repo = TestRepo::new();

    // Write sample export
    let export_path = repo.path().join("issues.jsonl");
    fs::write(&export_path, sample_go_export()).expect("failed to write export");

    // Dry run should report what would be imported
    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--dry-run",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"live_beads\": 3"))
        .stdout(predicate::str::contains("\"dry_run\": true"));
}

#[test]
fn test_migrate_detect_returns_structural_payload() {
    let repo = TestRepo::new();

    // Canonical strict deps payload (single object) with checksums + notes.
    write_store_commit(
        repo.path(),
        br#"{"cc":{"max":{},"dots":[]},"entries":[]}
"#,
        true,
        true,
    );

    let json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(&json, Some(2), "orset_v1", true, true, 2, false, &[]);
}

#[test]
fn test_migrate_to_2_dry_run_is_implemented() {
    let repo = TestRepo::new();
    write_store_commit(
        repo.path(),
        br#"{"cc":{"max":{},"dots":[]},"entries":[]}
"#,
        true,
        true,
    );

    let output = repo
        .bd()
        .args(["migrate", "to", "2", "--dry-run", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&output).expect("json");
    assert_eq!(
        json.get("dry_run").and_then(|value| value.as_bool()),
        Some(true)
    );
    assert_eq!(
        json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_push"),
        "dry-run migration should report push skipped: {json}"
    );
}

#[test]
fn test_migrate_to_2_noop_when_already_canonical() {
    let repo = TestRepo::new();
    write_strict_store_commit(repo.path());
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        None,
        "test setup should start without a remote store ref"
    );

    let output = repo
        .bd()
        .args(["migrate", "to", "2", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&output).expect("json");
    assert_eq!(
        json.get("commit_oid"),
        Some(&serde_json::Value::Null),
        "already-canonical migration should be a no-op: {json}"
    );
    assert_eq!(
        json.get("converted_deps").and_then(|value| value.as_bool()),
        Some(false),
        "expected converted_deps=false for no-op migration: {json}"
    );
    assert_eq!(
        json.get("added_notes_file")
            .and_then(|value| value.as_bool()),
        Some(false),
        "expected added_notes_file=false for no-op migration: {json}"
    );
    assert_eq!(
        json.get("wrote_checksums")
            .and_then(|value| value.as_bool()),
        Some(false),
        "expected wrote_checksums=false for no-op migration: {json}"
    );
    assert_eq!(
        json.get("push").and_then(|value| value.as_str()),
        Some("pushed"),
        "canonical local store with origin should publish the missing remote store ref: {json}"
    );
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        "no-op publish should create the remote store ref without rewriting local history"
    );
}

#[test]
fn test_migrate_to_2_rewrites_legacy_deps_and_store_invariants() {
    let repo = TestRepo::new();
    let local_before = fixture("v0_1_26_minimal").install(repo.path());
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        None,
        "test setup should start without a remote store ref"
    );

    let output = repo
        .bd()
        .args(["migrate", "to", "2", "--no-push", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&output).expect("json");
    assert_eq!(
        json.get("converted_deps").and_then(|v| v.as_bool()),
        Some(true),
        "expected legacy deps conversion, got: {json}"
    );
    assert_eq!(
        json.get("added_notes_file").and_then(|v| v.as_bool()),
        Some(true),
        "expected notes backfill, got: {json}"
    );
    assert_eq!(
        json.get("wrote_checksums").and_then(|v| v.as_bool()),
        Some(true),
        "expected checksums write, got: {json}"
    );
    let commit_oid = json
        .get("commit_oid")
        .and_then(|v| v.as_str())
        .expect("commit oid in migrate output")
        .to_string();

    let git = Repository::open(repo.path()).expect("open repo");
    let head_oid = git
        .refname_to_id("refs/heads/beads/store")
        .expect("store ref");
    assert_eq!(head_oid.to_string(), commit_oid);
    assert_eq!(
        json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_push"),
        "--no-push migration must report skipped push disposition: {json}"
    );
    assert_eq!(
        backup_ref_oid(repo.path(), local_before),
        Some(local_before),
        "case-2 local rewrite must create a backup ref for the previous local store head"
    );
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        None,
        "--no-push local rewrite must not create a remote store ref when origin has none"
    );

    let commit = git.find_commit(head_oid).expect("find commit");
    let tree = commit.tree().expect("tree");
    assert!(tree.get_name("notes.jsonl").is_some());
    assert!(tree.get_name("meta.json").is_some());

    let deps_entry = tree.get_name("deps.jsonl").expect("deps entry");
    let deps_blob = git
        .find_object(deps_entry.id(), Some(ObjectType::Blob))
        .expect("deps object")
        .peel_to_blob()
        .expect("deps blob");
    beads_git::wire::parse_deps_wire(deps_blob.content()).expect("strict deps parse");

    beads_git::read_state_at_oid(&git, head_oid).expect("strict store load");
}

#[test]
fn test_migrate_to_2_rebuilds_older_local_daemon_store_on_next_load() {
    let repo = TestRepo::new();
    fixture("rich_workflow").install(repo.path());

    let migrate = repo
        .bd()
        .args(["migrate", "to", "2", "--no-push", "--json"])
        .output()
        .expect("run migrate to 2");
    assert!(
        migrate.status.success(),
        "migration should succeed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );
    let migrate_json: serde_json::Value =
        serde_json::from_slice(&migrate.stdout).expect("parse migrate json");
    let warnings = migrate_json
        .get("warnings")
        .and_then(serde_json::Value::as_array)
        .expect("migration warnings array");
    assert!(
        warnings.iter().any(|warning| warning
            .as_str()
            .is_some_and(|warning| warning.contains("local daemon-store caches"))),
        "migration should surface the local daemon-store rebuild note: {migrate_json}"
    );

    let migrated_id = read_store_state(repo.path())
        .iter_live()
        .next()
        .map(|(id, _)| id.clone())
        .expect("migrated store should contain a live bead");

    let first_show = repo
        .bd()
        .args(["show", migrated_id.as_str(), "--json"])
        .output()
        .expect("run bd show after migration");
    assert!(
        first_show.status.success(),
        "bd show should autostart daemon after migration: {}",
        String::from_utf8_lossy(&first_show.stderr)
    );

    let store_id = wait_for_store_id(repo.data_dir(), Duration::from_secs(2))
        .expect("daemon should materialize local store");
    shutdown_daemon(repo.runtime_dir(), repo.data_dir());

    let store_dir = repo.data_dir().join("stores").join(store_id.to_string());
    let meta_path = store_dir.join("meta.json");
    let mut meta_json: serde_json::Value =
        serde_json::from_slice(&fs::read(&meta_path).expect("read local meta"))
            .expect("parse local meta");
    meta_json["store_format_version"] =
        serde_json::Value::from(StoreMetaVersions::STORE_FORMAT_VERSION.saturating_sub(1));
    fs::write(
        &meta_path,
        serde_json::to_vec_pretty(&meta_json).expect("encode downgraded local meta"),
    )
    .expect("write downgraded local meta");

    let wal_sentinel = store_dir
        .join("wal")
        .join(beads_core::NamespaceId::core().as_str())
        .join("legacy-segment.wal");
    fs::create_dir_all(wal_sentinel.parent().expect("wal sentinel parent"))
        .expect("create wal sentinel dir");
    fs::write(&wal_sentinel, b"legacy wal").expect("write wal sentinel");

    let cache_sentinel = store_dir.join("checkpoint_cache").join("CURRENT");
    fs::create_dir_all(cache_sentinel.parent().expect("cache sentinel parent"))
        .expect("create cache sentinel dir");
    fs::write(&cache_sentinel, b"legacy checkpoint").expect("write cache sentinel");

    let second_show = repo
        .bd()
        .args(["show", migrated_id.as_str(), "--json"])
        .output()
        .expect("run bd show after local-store downgrade");
    assert!(
        second_show.status.success(),
        "bd show should rebuild older local store cache on restart: {}",
        String::from_utf8_lossy(&second_show.stderr)
    );

    let rebuilt_meta: serde_json::Value =
        serde_json::from_slice(&fs::read(&meta_path).expect("read rebuilt local meta"))
            .expect("parse rebuilt local meta");
    assert_eq!(
        rebuilt_meta["store_format_version"].as_u64(),
        Some(StoreMetaVersions::STORE_FORMAT_VERSION as u64),
        "local store should reopen on the current format version"
    );
    assert!(
        !wal_sentinel.exists(),
        "older local wal segments should be removed during rebuild"
    );
    assert!(
        !cache_sentinel.exists(),
        "older local checkpoint cache should be removed during rebuild"
    );
}

#[test]
fn test_migrate_to_2_reports_skipped_no_remote_without_origin() {
    let repo = TestRepo::new_local_only();
    write_store_commit(
        repo.path(),
        br#"{"from":"bd-abc1","to":"bd-abc2","kind":"blocks"}
"#,
        false,
        false,
    );

    let output = repo
        .bd()
        .args(["migrate", "to", "2", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&output).expect("json");
    assert!(
        json.get("commit_oid")
            .and_then(|value| value.as_str())
            .is_some(),
        "migration should write a local commit: {json}"
    );
    assert_eq!(
        json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_remote"),
        "migration without origin must report skipped_no_remote: {json}"
    );
}

#[test]
fn test_migrate_to_2_noop_without_origin_reports_skipped_no_remote() {
    let repo = TestRepo::new_local_only();
    write_strict_store_commit(repo.path());
    let before_oid = store_ref_oid(repo.path(), "refs/heads/beads/store");

    let json = run_bd_json(&repo, &["migrate", "to", "2", "--json"]);
    assert_eq!(
        json.get("commit_oid"),
        Some(&serde_json::Value::Null),
        "canonical store without origin should remain a no-op: {json}"
    );
    assert_eq!(
        json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_remote"),
        "no-origin no-op must not look like a converged pushed repo: {json}"
    );
    assert_eq!(
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        before_oid,
        "no-origin no-op should leave the local store ref unchanged"
    );
}

#[test]
fn test_migrate_to_2_nonempty_canonical_without_last_write_stamp_is_noop() {
    let repo = TestRepo::new_local_only();
    write_nonempty_strict_store_commit(repo.path());
    let before_oid = store_ref_oid(repo.path(), "refs/heads/beads/store");
    let before_meta = read_store_meta_json(repo.path());
    assert!(
        before_meta.get("last_write_stamp").is_none(),
        "test setup should start without a last_write_stamp"
    );

    let detect_json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(&detect_json, Some(2), "orset_v1", true, true, 2, false, &[]);

    let json = run_bd_json(&repo, &["migrate", "to", "2", "--json"]);
    assert_eq!(
        json.get("commit_oid"),
        Some(&serde_json::Value::Null),
        "non-empty canonical store without last_write_stamp should remain a no-op: {json}"
    );
    assert_eq!(
        json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_remote"),
        "no-origin canonical no-op should still report skipped_no_remote: {json}"
    );
    assert_eq!(
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        before_oid,
        "no-op migration should leave the local store ref unchanged"
    );
    let after_meta = read_store_meta_json(repo.path());
    assert!(
        after_meta.get("last_write_stamp").is_none(),
        "migration should preserve an absent last_write_stamp"
    );
}

#[test]
fn test_migrate_equal_local_and_remote_legacy_rewrites_once_and_creates_backup_ref() {
    let repo = TestRepo::new();
    let base_oid = fixture("rich_workflow").install(repo.path());
    push_store_ref(repo.path());
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        Some(base_oid),
        "test setup should start from equal local and remote store refs"
    );
    let remote_before = store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store");

    let json = run_bd_json(&repo, &["migrate", "to", "2", "--no-push", "--json"]);
    assert!(
        json.get("commit_oid")
            .and_then(|value| value.as_str())
            .is_some(),
        "equal local/remote legacy migration should write a local rewrite commit: {json}"
    );
    assert_eq!(
        json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_push"),
        "equal local/remote migration should honor --no-push: {json}"
    );
    assert_eq!(
        store_first_parent_oid(repo.path(), "refs/heads/beads/store"),
        Some(base_oid),
        "equal local/remote migration must parent the rewrite on the remote head"
    );
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        remote_before,
        "--no-push equal-history migration must not mutate origin"
    );
    assert_eq!(
        backup_ref_oid(repo.path(), base_oid),
        Some(base_oid),
        "equal-history rewrite must create a backup ref for the previous local store head"
    );

    let state = read_store_state(repo.path());
    assert_rich_workflow_state(
        &state,
        true,
        false,
        &["note-rich-claimed-a", "note-rich-claimed-b"],
    );
    assert_meta_checksums_match_store(repo.path());
}

#[test]
fn test_migrate_local_rewrite_prunes_backup_refs_to_cap_and_keeps_current_target() {
    let repo = TestRepo::new_local_only();
    let base_oid = fixture("rich_workflow").install(repo.path());
    let mut seeded_backups = Vec::new();
    for idx in 0..64 {
        let oid = create_detached_store_commit(
            repo.path(),
            1_700_000_000 + idx as i64,
            &format!("backup-seed-{idx}"),
        );
        create_backup_ref(repo.path(), oid);
        seeded_backups.push(oid);
    }
    assert_eq!(
        backup_ref_targets(repo.path()).len(),
        64,
        "test setup should start at the backup ref cap"
    );
    let oldest_seed = seeded_backups[0];

    let json = run_bd_json(&repo, &["migrate", "to", "2", "--no-push", "--json"]);
    assert!(
        json.get("commit_oid")
            .and_then(|value| value.as_str())
            .is_some(),
        "legacy rewrite should still commit under backup pressure: {json}"
    );
    assert_eq!(
        json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_push"),
        "backup-pruning coverage should stay in the local rewrite path: {json}"
    );

    let backup_targets = backup_ref_targets(repo.path());
    assert_eq!(
        backup_targets.len(),
        64,
        "migration rewrite should prune backup refs back to the configured cap"
    );
    assert!(
        backup_targets.contains(&base_oid),
        "migration rewrite must preserve the protected backup ref for the previous local store head"
    );
    assert!(
        !backup_targets.contains(&oldest_seed),
        "migration rewrite should prune the oldest backup when backup pressure exceeds the cap"
    );
    assert_meta_checksums_match_store(repo.path());
}

#[test]
fn test_migrate_local_rewrite_prunes_when_the_protected_backup_ref_already_exists() {
    let repo = TestRepo::new_local_only();
    let base_oid = fixture("rich_workflow").install(repo.path());
    create_backup_ref(repo.path(), base_oid);
    let mut seeded_backups = Vec::new();
    for idx in 0..64 {
        let oid = create_detached_store_commit(
            repo.path(),
            1_700_001_000 + idx as i64,
            &format!("existing-protected-backup-seed-{idx}"),
        );
        create_backup_ref(repo.path(), oid);
        seeded_backups.push(oid);
    }
    assert_eq!(
        backup_ref_targets(repo.path()).len(),
        65,
        "test setup should start over the backup ref cap with the protected ref already present"
    );
    let oldest_seed = seeded_backups[0];

    let json = run_bd_json(&repo, &["migrate", "to", "2", "--no-push", "--json"]);
    assert!(
        json.get("commit_oid")
            .and_then(|value| value.as_str())
            .is_some(),
        "legacy rewrite should still commit when the protected backup ref already exists: {json}"
    );

    let backup_targets = backup_ref_targets(repo.path());
    assert_eq!(
        backup_targets.len(),
        64,
        "migration rewrite should prune backup refs back to the configured cap even when the protected ref already existed"
    );
    assert!(
        backup_targets.contains(&base_oid),
        "migration rewrite must preserve the preexisting protected backup ref for the previous local store head"
    );
    assert!(
        !backup_targets.contains(&oldest_seed),
        "migration rewrite should still prune the oldest backup when the protected ref already existed"
    );
    assert_meta_checksums_match_store(repo.path());
}

#[test]
fn test_migrate_detect_flags_legacy_deps_even_with_current_meta() {
    let repo = TestRepo::new();
    // Legacy line-per-edge deps content plus current-format meta/checksums.
    write_store_commit(
        repo.path(),
        br#"{"from":"bd-abc1","to":"bd-abc2","kind":"blocks"}
"#,
        true,
        true,
    );

    let json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &json,
        Some(2),
        "legacy_edges",
        true,
        true,
        0,
        true,
        &["deps.jsonl is legacy line-per-edge (missing cc)"],
    );
}

#[test]
fn test_migrate_detect_flags_missing_notes_and_checksums() {
    let repo = TestRepo::new_local_only();
    write_store_commit(
        repo.path(),
        br#"{"cc":{"max":{},"dots":[]},"entries":[]}
"#,
        false,
        false,
    );

    let json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &json,
        None,
        "orset_v1",
        false,
        false,
        0,
        true,
        &["meta checksums missing", "notes.jsonl missing"],
    );
}

#[test]
fn test_migrate_detect_flags_missing_notes_checksum_in_v1_meta() {
    let repo = TestRepo::new_local_only();
    write_strict_store_commit(repo.path());

    let state_bytes = read_tree_blob(
        &Repository::open(repo.path()).expect("open repo"),
        "state.jsonl",
    )
    .expect("state.jsonl");
    let tombs_bytes = read_tree_blob(
        &Repository::open(repo.path()).expect("open repo"),
        "tombstones.jsonl",
    )
    .expect("tombstones.jsonl");
    let deps_bytes = read_tree_blob(
        &Repository::open(repo.path()).expect("open repo"),
        "deps.jsonl",
    )
    .expect("deps.jsonl");
    let notes_bytes = read_tree_blob(
        &Repository::open(repo.path()).expect("open repo"),
        "notes.jsonl",
    )
    .expect("notes.jsonl");
    let checksums =
        StoreChecksums::from_bytes(&state_bytes, &tombs_bytes, &deps_bytes, Some(&notes_bytes));
    let meta_bytes = serde_json::to_vec(&serde_json::json!({
        "format_version": 1,
        "root_slug": "bd",
        "state_sha256": checksums.state.to_string(),
        "tombstones_sha256": checksums.tombstones.to_string(),
        "deps_sha256": checksums.deps.to_string()
    }))
    .expect("meta json");
    rewrite_store_meta(repo.path(), Some(&meta_bytes));

    let json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &json,
        Some(1),
        "orset_v1",
        true,
        false,
        0,
        true,
        &["meta checksums missing"],
    );
}

#[test]
fn test_migrate_detect_uses_live_remote_when_local_and_tracking_are_missing() {
    let repo = TestRepo::new();
    write_store_commit(
        repo.remote_dir.path(),
        br#"{"from":"bd-abc1","to":"bd-abc2","kind":"blocks"}
"#,
        false,
        true,
    );

    let json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &json,
        Some(2),
        "legacy_edges",
        false,
        true,
        0,
        true,
        &[
            "deps.jsonl is legacy line-per-edge (missing cc)",
            "notes.jsonl missing",
        ],
    );
}

#[test]
fn test_migrate_detect_uses_live_remote_without_mutating_stale_tracking_ref() {
    let repo = TestRepo::new();
    write_strict_store_commit(repo.remote_dir.path());
    let stale_tracking_oid = fetch_remote_store_ref(repo.path());
    write_store_commit(
        repo.remote_dir.path(),
        br#"{"from":"bd-abc1","to":"bd-abc2","kind":"blocks"}
"#,
        false,
        true,
    );

    let json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &json,
        Some(2),
        "legacy_edges",
        false,
        true,
        0,
        true,
        &[
            "deps.jsonl is legacy line-per-edge (missing cc)",
            "notes.jsonl missing",
        ],
    );
    assert_eq!(
        store_ref_oid(repo.path(), "refs/remotes/origin/beads/store"),
        Some(stale_tracking_oid),
        "detect should classify the live remote without mutating tracking refs"
    );
}

#[test]
fn test_migrate_dry_run_uses_live_remote_without_mutating_stale_tracking_ref() {
    let repo = TestRepo::new();
    write_strict_store_commit(repo.remote_dir.path());
    let stale_tracking_oid = fetch_remote_store_ref(repo.path());
    write_store_commit(
        repo.remote_dir.path(),
        br#"{"from":"bd-abc1","to":"bd-abc2","kind":"blocks"}
"#,
        false,
        true,
    );

    let json = run_bd_json(&repo, &["migrate", "to", "2", "--dry-run", "--json"]);
    assert_eq!(
        json.get("from_effective_version")
            .and_then(|value| value.as_u64()),
        Some(0),
        "dry-run should classify the fetched remote legacy state: {json}"
    );
    assert_eq!(
        json.get("deps_format_before")
            .and_then(|value| value.as_str()),
        Some("legacy_edges"),
        "dry-run should classify the fetched remote legacy deps: {json}"
    );
    assert_eq!(
        json.get("commit_oid"),
        Some(&serde_json::Value::Null),
        "dry-run should not create a commit: {json}"
    );
    assert_eq!(
        store_ref_oid(repo.path(), "refs/remotes/origin/beads/store"),
        Some(stale_tracking_oid),
        "dry-run should classify live remote state without mutating tracking refs"
    );
}

#[test]
fn test_migrate_detect_uses_local_store_when_origin_preview_is_broken() {
    let repo = TestRepo::new();
    fixture("rich_workflow").install(repo.path());
    fs::remove_dir_all(repo.remote_dir.path()).expect("remove remote dir");

    let json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &json,
        Some(1),
        "legacy_edges",
        false,
        false,
        0,
        true,
        &[
            "deps.jsonl is legacy line-per-edge (missing cc)",
            "meta checksums missing",
            "notes.jsonl missing",
        ],
    );
}

#[test]
fn test_migrate_detect_uses_local_store_without_trusting_stale_tracking_when_origin_is_broken() {
    let repo = TestRepo::new();
    write_strict_store_commit(repo.path());
    write_store_commit(
        repo.remote_dir.path(),
        br#"{"from":"bd-abc1","to":"bd-abc2","kind":"blocks"}
"#,
        false,
        true,
    );
    let stale_tracking_oid = fetch_remote_store_ref(repo.path());
    fs::remove_dir_all(repo.remote_dir.path()).expect("remove remote dir");

    let json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(&json, Some(2), "orset_v1", true, true, 2, false, &[]);
    assert_eq!(
        store_ref_oid(repo.path(), "refs/remotes/origin/beads/store"),
        Some(stale_tracking_oid),
        "detect should leave stale tracking refs untouched while falling back to local-only classification"
    );
}

#[test]
fn test_migrate_detect_fails_when_local_missing_and_origin_preview_is_broken() {
    let repo = TestRepo::new();
    write_store_commit(
        repo.remote_dir.path(),
        br#"{"from":"bd-abc1","to":"bd-abc2","kind":"blocks"}
"#,
        false,
        true,
    );
    fs::remove_dir_all(repo.remote_dir.path()).expect("remove remote dir");

    repo.bd()
        .args(["migrate", "detect", "--json"])
        .assert()
        .failure();
}

#[test]
fn test_migrate_dry_run_fails_without_trusting_stale_tracking_when_origin_is_broken() {
    let repo = TestRepo::new();
    write_strict_store_commit(repo.path());
    write_store_commit(
        repo.remote_dir.path(),
        br#"{"from":"bd-abc1","to":"bd-abc2","kind":"blocks"}
"#,
        false,
        true,
    );
    let stale_tracking_oid = fetch_remote_store_ref(repo.path());
    let before_oid = store_ref_oid(repo.path(), "refs/heads/beads/store");
    fs::remove_dir_all(repo.remote_dir.path()).expect("remove remote dir");

    repo.bd()
        .args(["migrate", "to", "2", "--dry-run", "--json"])
        .assert()
        .failure();
    assert_eq!(
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        before_oid,
        "dry-run should leave the local store ref unchanged on preview failure"
    );
    assert_eq!(
        store_ref_oid(repo.path(), "refs/remotes/origin/beads/store"),
        Some(stale_tracking_oid),
        "dry-run should not mutate tracking refs while surfacing the live preview error"
    );
}

#[test]
fn test_migrate_no_push_succeeds_without_fetch_when_origin_is_broken() {
    let repo = TestRepo::new();
    let local_before = fixture("rich_workflow").install(repo.path());
    fs::remove_dir_all(repo.remote_dir.path()).expect("remove remote dir");

    let json = run_bd_json(&repo, &["migrate", "to", "2", "--no-push", "--json"]);
    assert_eq!(
        json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_push"),
        "offline --no-push migration should stay local-only: {json}"
    );
    assert!(
        json.get("commit_oid")
            .and_then(|value| value.as_str())
            .is_some(),
        "offline --no-push migration should still rewrite the local store: {json}"
    );
    assert_eq!(
        store_first_parent_oid(repo.path(), "refs/heads/beads/store"),
        Some(local_before),
        "offline --no-push migration should parent the rewrite on the local store head"
    );
    assert_eq!(
        backup_ref_oid(repo.path(), local_before),
        Some(local_before),
        "offline --no-push migration must preserve a backup ref for the previous local store head"
    );

    let state = read_store_state(repo.path());
    assert_rich_workflow_state(
        &state,
        true,
        false,
        &["note-rich-claimed-a", "note-rich-claimed-b"],
    );
}

#[test]
fn test_migrate_fixture_rich_workflow_rewrites_and_preserves_state() {
    let repo = TestRepo::new_local_only();
    fixture("rich_workflow").install(repo.path());
    let before_dry_run = store_ref_oid(repo.path(), "refs/heads/beads/store");

    let detect_json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &detect_json,
        Some(1),
        "legacy_edges",
        false,
        false,
        0,
        true,
        &[
            "deps.jsonl is legacy line-per-edge (missing cc)",
            "meta checksums missing",
            "notes.jsonl missing",
        ],
    );

    let dry_run_json = run_bd_json(&repo, &["migrate", "to", "2", "--dry-run", "--json"]);
    assert_eq!(
        dry_run_json.get("commit_oid"),
        Some(&serde_json::Value::Null),
        "dry-run should not create a commit: {dry_run_json}"
    );
    assert_eq!(
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        before_dry_run,
        "dry-run must not mutate the store ref"
    );

    let migrate_json = run_bd_json(&repo, &["migrate", "to", "2", "--no-push", "--json"]);
    assert_eq!(
        migrate_json
            .get("converted_deps")
            .and_then(|value| value.as_bool()),
        Some(true),
        "fixture migration should rewrite legacy deps: {migrate_json}"
    );
    assert_eq!(
        migrate_json
            .get("added_notes_file")
            .and_then(|value| value.as_bool()),
        Some(true),
        "fixture migration should backfill notes.jsonl: {migrate_json}"
    );
    assert_eq!(
        migrate_json
            .get("wrote_checksums")
            .and_then(|value| value.as_bool()),
        Some(true),
        "fixture migration should backfill checksums: {migrate_json}"
    );
    assert_eq!(
        migrate_json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_push"),
        "fixture migration should honor --no-push: {migrate_json}"
    );

    let post_detect_json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &post_detect_json,
        Some(2),
        "orset_v1",
        true,
        true,
        2,
        false,
        &[],
    );

    let state = read_store_state(repo.path());
    assert_rich_workflow_state(
        &state,
        true,
        false,
        &["note-rich-claimed-a", "note-rich-claimed-b"],
    );
}

#[test]
fn test_migrate_fixture_rich_workflow_peer_runs_full_round_trip() {
    let repo = TestRepo::new_local_only();
    fixture("rich_workflow_peer").install(repo.path());
    let before_dry_run = store_ref_oid(repo.path(), "refs/heads/beads/store");

    let detect_json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &detect_json,
        Some(1),
        "legacy_edges",
        false,
        false,
        0,
        true,
        &[
            "deps.jsonl is legacy line-per-edge (missing cc)",
            "meta checksums missing",
            "notes.jsonl missing",
        ],
    );

    let dry_run_json = run_bd_json(&repo, &["migrate", "to", "2", "--dry-run", "--json"]);
    assert_eq!(
        dry_run_json.get("commit_oid"),
        Some(&serde_json::Value::Null),
        "dry-run should not create a commit: {dry_run_json}"
    );
    assert_eq!(
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        before_dry_run,
        "dry-run must not mutate the store ref"
    );

    let migrate_json = run_bd_json(&repo, &["migrate", "to", "2", "--no-push", "--json"]);
    assert_eq!(
        migrate_json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_push"),
        "fixture migration should honor --no-push: {migrate_json}"
    );

    let post_detect_json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &post_detect_json,
        Some(2),
        "orset_v1",
        true,
        true,
        2,
        false,
        &[],
    );

    let state = read_store_state(repo.path());
    assert_rich_workflow_state(
        &state,
        false,
        true,
        &["note-rich-claimed-a", "note-rich-claimed-c"],
    );
}

#[test]
fn test_migrate_fixture_tombstone_deleted_dep_preserves_semantics() {
    let repo = TestRepo::new_local_only();
    fixture("tombstone_deleted_dep").install(repo.path());
    let before_dry_run = store_ref_oid(repo.path(), "refs/heads/beads/store");

    let detect_json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &detect_json,
        None,
        "legacy_edges",
        false,
        false,
        0,
        true,
        &[
            "deps.jsonl is legacy line-per-edge (missing cc)",
            "meta checksums missing",
            "notes.jsonl missing",
        ],
    );

    let dry_run_json = run_bd_json(&repo, &["migrate", "to", "2", "--dry-run", "--json"]);
    assert_eq!(
        dry_run_json.get("commit_oid"),
        Some(&serde_json::Value::Null),
        "dry-run should not create a commit: {dry_run_json}"
    );
    assert_eq!(
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        before_dry_run,
        "dry-run must not mutate the store ref"
    );

    let migrate_json = run_bd_json(&repo, &["migrate", "to", "2", "--no-push", "--json"]);
    assert_eq!(
        migrate_json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_push"),
        "fixture migration should honor --no-push: {migrate_json}"
    );

    let post_detect_json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &post_detect_json,
        Some(2),
        "orset_v1",
        true,
        true,
        2,
        false,
        &[],
    );

    let state = read_store_state(repo.path());
    assert_tombstone_deleted_dep_state(&state);
}

#[test]
fn test_migrate_fixture_remote_only_detect_dry_run_and_local_rewrite() {
    let repo = TestRepo::new();
    let remote_oid = fixture("rich_workflow").install(repo.remote_dir.path());
    let remote_before = store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store");
    assert_eq!(
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        None,
        "remote-only setup should not create a local store ref before migration"
    );

    let detect_json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &detect_json,
        Some(1),
        "legacy_edges",
        false,
        false,
        0,
        true,
        &[
            "deps.jsonl is legacy line-per-edge (missing cc)",
            "meta checksums missing",
            "notes.jsonl missing",
        ],
    );

    let dry_run_json = run_bd_json(&repo, &["migrate", "to", "2", "--dry-run", "--json"]);
    assert_eq!(
        dry_run_json.get("commit_oid"),
        Some(&serde_json::Value::Null),
        "dry-run should not create a commit: {dry_run_json}"
    );
    assert_eq!(
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        None,
        "remote-only dry-run must not materialize a local store ref"
    );

    let migrate_json = run_bd_json(&repo, &["migrate", "to", "2", "--no-push", "--json"]);
    assert_eq!(
        migrate_json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_push"),
        "remote-only migration should honor --no-push: {migrate_json}"
    );
    assert_eq!(
        store_first_parent_oid(repo.path(), "refs/heads/beads/store"),
        Some(remote_oid),
        "remote-only migration must parent the local rewrite commit on the fetched remote ref"
    );
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        remote_before,
        "--no-push remote-only migration must not mutate origin"
    );

    let state = read_store_state(repo.path());
    assert_rich_workflow_state(
        &state,
        true,
        false,
        &["note-rich-claimed-a", "note-rich-claimed-b"],
    );
}

#[test]
fn test_migrate_fixture_remote_only_pushes_rewritten_store_ref() {
    let repo = TestRepo::new();
    let remote_before_oid = fixture("rich_workflow").install(repo.remote_dir.path());

    let migrate_json = run_bd_json(&repo, &["migrate", "to", "2", "--json"]);
    assert_eq!(
        migrate_json.get("push").and_then(|value| value.as_str()),
        Some("pushed"),
        "remote-only migration with origin should publish the rewritten store ref: {migrate_json}"
    );

    let local_oid =
        store_ref_oid(repo.path(), "refs/heads/beads/store").expect("local store ref after push");
    let remote_oid = fetch_remote_store_ref(repo.path());
    assert_eq!(
        local_oid, remote_oid,
        "successful pushed migration should leave local and remote store refs aligned"
    );
    assert_eq!(
        store_first_parent_oid(repo.path(), "refs/heads/beads/store"),
        Some(remote_before_oid),
        "remote-only pushed migration must parent the rewritten commit on the remote head"
    );

    let post_detect_json = run_bd_json(&repo, &["migrate", "detect", "--json"]);
    assert_detect_outcome(
        &post_detect_json,
        Some(2),
        "orset_v1",
        true,
        true,
        2,
        false,
        &[],
    );

    let state = read_store_state(repo.path());
    assert_rich_workflow_state(
        &state,
        true,
        false,
        &["note-rich-claimed-a", "note-rich-claimed-b"],
    );
}

#[test]
fn test_migrate_remote_parent_preserves_existing_meta_root_slug_and_last_write_stamp() {
    let repo = TestRepo::new();
    fixture("rich_workflow").install(repo.remote_dir.path());

    let remote_git = Repository::open(repo.remote_dir.path()).expect("open remote repo");
    let state_bytes = read_tree_blob(&remote_git, "state.jsonl").expect("remote state.jsonl");
    let tombs_bytes =
        read_tree_blob(&remote_git, "tombstones.jsonl").expect("remote tombstones.jsonl");
    let deps_bytes = read_tree_blob(&remote_git, "deps.jsonl").expect("remote deps.jsonl");
    let notes_bytes = read_tree_blob(&remote_git, "notes.jsonl").unwrap_or_default();
    let checksums =
        StoreChecksums::from_bytes(&state_bytes, &tombs_bytes, &deps_bytes, Some(&notes_bytes));
    let preserved_stamp = WriteStamp::new(1_765_401_111_000, 9);
    let meta_bytes = serialize_meta(Some("remote-meta"), Some(&preserved_stamp), &checksums)
        .expect("serialize remote meta");
    rewrite_store_meta(repo.remote_dir.path(), Some(&meta_bytes));

    run_bd_json(&repo, &["migrate", "to", "2", "--no-push", "--json"]);

    let meta_json = read_store_meta_json(repo.path());
    assert_eq!(
        meta_json.get("root_slug").and_then(|value| value.as_str()),
        Some("remote-meta"),
        "remote-parented migration should preserve an existing remote root_slug"
    );
    assert_eq!(
        meta_json
            .get("last_write_stamp")
            .and_then(|value| value.get(0))
            .and_then(|value| value.as_u64()),
        Some(preserved_stamp.wall_ms),
        "remote-parented migration should preserve remote last_write_stamp wall_ms"
    );
    assert_eq!(
        meta_json
            .get("last_write_stamp")
            .and_then(|value| value.get(1))
            .and_then(|value| value.as_u64()),
        Some(u64::from(preserved_stamp.counter)),
        "remote-parented migration should preserve remote last_write_stamp counter"
    );
    assert_meta_checksums_match_store(repo.path());
}

#[test]
fn test_migrate_hybrid_remote_canonical_local_legacy_no_push_parents_on_remote_and_preserves_remote_meta()
 {
    let repo = TestRepo::new();
    write_strict_store_commit(repo.remote_dir.path());
    let remote_git = Repository::open(repo.remote_dir.path()).expect("open remote repo");
    let remote_state_bytes = read_tree_blob(&remote_git, "state.jsonl").expect("remote state");
    let remote_tombs_bytes =
        read_tree_blob(&remote_git, "tombstones.jsonl").expect("remote tombstones");
    let remote_deps_bytes = read_tree_blob(&remote_git, "deps.jsonl").expect("remote deps");
    let remote_notes_bytes = read_tree_blob(&remote_git, "notes.jsonl").expect("remote notes");
    let remote_checksums = StoreChecksums::from_bytes(
        &remote_state_bytes,
        &remote_tombs_bytes,
        &remote_deps_bytes,
        Some(&remote_notes_bytes),
    );
    let remote_stamp = WriteStamp::new(1_765_401_777_000, 9);
    let remote_meta_bytes =
        serialize_meta(Some("remote-meta"), Some(&remote_stamp), &remote_checksums)
            .expect("serialize remote meta");
    rewrite_store_meta(repo.remote_dir.path(), Some(&remote_meta_bytes));
    let remote_before = store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store");
    let remote_tracking_oid = fetch_remote_store_ref(repo.path());
    Repository::open(repo.path())
        .expect("open local repo")
        .reference(
            "refs/heads/beads/store",
            remote_tracking_oid,
            true,
            "seed local store ref from remote head",
        )
        .expect("seed local store ref");

    write_store_commit(
        repo.path(),
        br#"{"from":"bd-abc1","to":"bd-abc2","kind":"blocks"}
"#,
        false,
        true,
    );
    let local_git = Repository::open(repo.path()).expect("open local repo");
    let local_state_bytes = read_tree_blob(&local_git, "state.jsonl").expect("local state");
    let local_tombs_bytes =
        read_tree_blob(&local_git, "tombstones.jsonl").expect("local tombstones");
    let local_deps_bytes = read_tree_blob(&local_git, "deps.jsonl").expect("local deps");
    let local_notes_bytes = Vec::new();
    let local_checksums = StoreChecksums::from_bytes(
        &local_state_bytes,
        &local_tombs_bytes,
        &local_deps_bytes,
        Some(&local_notes_bytes),
    );
    let local_stamp = WriteStamp::new(1_765_401_666_000, 4);
    let local_meta_bytes = serialize_meta(Some("local-meta"), Some(&local_stamp), &local_checksums)
        .expect("serialize local meta");
    rewrite_store_meta(repo.path(), Some(&local_meta_bytes));

    let json = run_bd_json(&repo, &["migrate", "to", "2", "--no-push", "--json"]);
    assert_eq!(
        json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_push"),
        "hybrid no-push migration should report skipped_no_push: {json}"
    );
    assert!(
        json.get("commit_oid")
            .and_then(|value| value.as_str())
            .is_some(),
        "hybrid no-push migration should still create a local rewrite commit: {json}"
    );
    assert_eq!(
        store_first_parent_oid(repo.path(), "refs/heads/beads/store"),
        remote_before,
        "hybrid migration should parent the rewrite on the canonical remote head"
    );
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        remote_before,
        "hybrid --no-push migration must not mutate origin"
    );

    let meta_json = read_store_meta_json(repo.path());
    assert_eq!(
        meta_json.get("root_slug").and_then(|value| value.as_str()),
        Some("remote-meta"),
        "hybrid migration should prefer the remote root_slug"
    );
    assert_eq!(
        meta_json
            .get("last_write_stamp")
            .and_then(|value| value.get(0))
            .and_then(|value| value.as_u64()),
        Some(remote_stamp.wall_ms),
        "hybrid migration should preserve the max remote/local last_write_stamp wall_ms"
    );
    assert_eq!(
        meta_json
            .get("last_write_stamp")
            .and_then(|value| value.get(1))
            .and_then(|value| value.as_u64()),
        Some(u64::from(remote_stamp.counter)),
        "hybrid migration should preserve the max remote/local last_write_stamp counter"
    );
    assert_meta_checksums_match_store(repo.path());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_migrate_fixture_related_divergence_merges_realistic_fixtures() {
    let repo = TestRepo::new();
    let base_oid = fixture("rich_workflow").install(repo.path());
    push_store_ref(repo.path());

    let remote_oid =
        fixture("rich_workflow_peer").install_with_parent(repo.remote_dir.path(), Some(base_oid));
    let remote_before = store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store");
    let remote_tracking_oid = fetch_remote_store_ref(repo.path());
    assert_eq!(
        remote_tracking_oid,
        remote_before.expect("remote store ref"),
        "test setup must seed the remote-tracking ref before exercising no-push divergence"
    );

    repo.bd()
        .args(["migrate", "to", "2", "--no-push", "--json"])
        .assert()
        .success();
    assert_eq!(
        store_first_parent_oid(repo.path(), "refs/heads/beads/store"),
        Some(remote_oid),
        "related-divergence migration must parent the local rewrite on the fetched remote head"
    );
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        remote_before,
        "--no-push related-divergence migration must not mutate origin"
    );
    assert_eq!(
        backup_ref_oid(repo.path(), base_oid),
        Some(base_oid),
        "related-divergence rewrite must create a backup ref for the previous local store head"
    );

    let state = read_store_state(repo.path());
    assert_rich_workflow_state(
        &state,
        true,
        true,
        &[
            "note-rich-claimed-a",
            "note-rich-claimed-b",
            "note-rich-claimed-c",
        ],
    );
    let meta_json = read_store_meta_json(repo.path());
    assert_eq!(
        meta_json.get("root_slug").and_then(|value| value.as_str()),
        Some("bd"),
        "related-divergence migration should preserve the existing fixture root_slug"
    );
    assert!(
        meta_json.get("last_write_stamp").is_none(),
        "related-divergence migration should preserve absent last_write_stamp when neither side had one"
    );
    assert_meta_checksums_match_store(repo.path());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_migrate_fixture_unrelated_divergence_requires_force() {
    let repo = TestRepo::new();
    let local_before = fixture("rich_workflow_peer").install(repo.path());
    let remote_oid = fixture("tombstone_deleted_dep").install(repo.remote_dir.path());
    let remote_before = store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store");
    let remote_tracking_oid = fetch_remote_store_ref(repo.path());
    assert_eq!(
        remote_tracking_oid,
        remote_before.expect("remote store ref"),
        "test setup must seed the remote-tracking ref before exercising no-push divergence"
    );

    repo.bd()
        .args(["migrate", "to", "2", "--dry-run", "--json"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "no common ancestor between local and remote",
        ));

    repo.bd()
        .args(["migrate", "to", "2", "--force", "--dry-run", "--json"])
        .assert()
        .success();
    assert_eq!(
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        Some(local_before),
        "dry-run with --force must remain non-mutating"
    );

    repo.bd()
        .args(["migrate", "to", "2", "--no-push", "--json"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "no common ancestor between local and remote",
        ));

    repo.bd()
        .args(["migrate", "to", "2", "--force", "--no-push", "--json"])
        .assert()
        .success();
    assert_eq!(
        store_first_parent_oid(repo.path(), "refs/heads/beads/store"),
        Some(remote_oid),
        "forced unrelated-divergence migration must parent the rewrite on the fetched remote head"
    );
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        remote_before,
        "--no-push forced unrelated-divergence migration must not mutate origin"
    );
    assert_eq!(
        backup_ref_oid(repo.path(), local_before),
        Some(local_before),
        "forced unrelated-divergence rewrite must create a backup ref for the previous local store head"
    );

    let state = read_store_state(repo.path());
    assert_rich_workflow_state(
        &state,
        false,
        true,
        &["note-rich-claimed-a", "note-rich-claimed-c"],
    );
    assert_tombstone_deleted_dep_state(&state);
    let meta_json = read_store_meta_json(repo.path());
    assert_eq!(
        meta_json.get("root_slug").and_then(|value| value.as_str()),
        Some("bd"),
        "forced unrelated-divergence migration should preserve the local fixture root_slug"
    );
    assert!(
        meta_json.get("last_write_stamp").is_none(),
        "forced unrelated-divergence migration should preserve absent last_write_stamp when neither side had one"
    );
    assert_meta_checksums_match_store(repo.path());
}

#[test]
fn test_migrate_retryable_push_race_recomputes_and_preserves_backup_ref() {
    let repo = TestRepo::new();
    let base_oid = fixture("rich_workflow").install(repo.path());
    push_store_ref(repo.path());

    let remote_repo = Repository::open(repo.remote_dir.path()).expect("open remote repo");
    beads_git::sync::migrate_store_ref_to_v2(
        &remote_repo,
        repo.remote_dir.path(),
        false,
        false,
        true,
        0,
    )
    .expect("winner migration")
    .commit_oid
    .expect("winner commit oid");
    let winner_oid = rewrite_store_ref_commit(
        repo.remote_dir.path(),
        "refs/heads/beads/store",
        1_900_000_001,
        "simulate distinct remote winner",
    );
    set_ref_target(
        repo.remote_dir.path(),
        "refs/heads/beads/store",
        base_oid,
        "reset remote to base",
    );
    wait_for_fetched_remote_store_ref(repo.path(), base_oid, Duration::from_secs(1));

    let local_repo = Repository::open(repo.path()).expect("open local repo");
    let outcome = migrate_store_ref_to_v2_with_before_push_for_testing(
        &local_repo,
        repo.path(),
        false,
        false,
        false,
        1,
        |retries, _commit_oid| {
            if retries == 0 {
                set_ref_target(
                    repo.remote_dir.path(),
                    "refs/heads/beads/store",
                    winner_oid,
                    "simulate remote winner before first push",
                );
            }
            Ok(())
        },
    )
    .expect("migration should converge after retryable push race");

    assert_eq!(
        outcome.commit_oid, None,
        "retryable push race should converge to the remote winner without minting a second rewrite"
    );
    assert_eq!(
        outcome.push,
        beads_git::sync::MigratePushDisposition::SkippedNoPush,
        "retryable push race should finish as a converged no-op after refetch/recompute"
    );
    assert_eq!(
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        Some(winner_oid),
        "local store ref should realign to the fetched remote winner"
    );
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        Some(winner_oid),
        "remote store ref should stay on the winner after the retry"
    );
    assert_eq!(
        backup_ref_oid(repo.path(), base_oid),
        Some(base_oid),
        "lost push races must preserve a backup ref for the original local store head"
    );
    assert_meta_checksums_match_store(repo.path());
}

#[test]
fn test_migrate_retryable_push_race_exhausts_retry_budget() {
    let repo = TestRepo::new();
    let base_oid = fixture("rich_workflow").install(repo.path());
    push_store_ref(repo.path());

    let remote_repo = Repository::open(repo.remote_dir.path()).expect("open remote repo");
    beads_git::sync::migrate_store_ref_to_v2(
        &remote_repo,
        repo.remote_dir.path(),
        false,
        false,
        true,
        0,
    )
    .expect("winner migration")
    .commit_oid
    .expect("winner commit oid");
    let winner_oid = rewrite_store_ref_commit(
        repo.remote_dir.path(),
        "refs/heads/beads/store",
        1_900_000_002,
        "simulate distinct remote winner",
    );
    set_ref_target(
        repo.remote_dir.path(),
        "refs/heads/beads/store",
        base_oid,
        "reset remote to base",
    );
    wait_for_fetched_remote_store_ref(repo.path(), base_oid, Duration::from_secs(1));

    let local_repo = Repository::open(repo.path()).expect("open local repo");
    let err = migrate_store_ref_to_v2_with_before_push_for_testing(
        &local_repo,
        repo.path(),
        false,
        false,
        false,
        0,
        |retries, _commit_oid| {
            if retries == 0 {
                set_ref_target(
                    repo.remote_dir.path(),
                    "refs/heads/beads/store",
                    winner_oid,
                    "simulate remote winner before first push",
                );
            }
            Ok(())
        },
    )
    .expect_err("migration should stop once the retry budget is exhausted");

    let err_text = err.to_string();
    match &err {
        beads_git::SyncError::TooManyRetries(retries) => assert_eq!(*retries, 1),
        other => panic!("expected TooManyRetries, got {other:?}"),
    }
    assert_eq!(
        err_text, "too many sync retries (1)",
        "retry exhaustion should surface the terminal sync error text"
    );
    assert_eq!(
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        Some(base_oid),
        "retry exhaustion must restore the original local store ref"
    );
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        Some(winner_oid),
        "retry exhaustion must leave the remote winner intact"
    );
    assert_eq!(
        backup_ref_oid(repo.path(), base_oid),
        Some(base_oid),
        "retry exhaustion must still preserve a backup ref for the original local store head"
    );
}

#[test]
fn test_migrate_cli_retryable_push_race_recomputes_and_preserves_backup_ref() {
    let repo = TestRepo::new();
    let base_oid = fixture("rich_workflow").install(repo.path());
    push_store_ref(repo.path());

    let remote_repo = Repository::open(repo.remote_dir.path()).expect("open remote repo");
    beads_git::sync::migrate_store_ref_to_v2(
        &remote_repo,
        repo.remote_dir.path(),
        false,
        false,
        true,
        0,
    )
    .expect("winner migration")
    .commit_oid
    .expect("winner commit oid");
    let winner_oid = rewrite_store_ref_commit(
        repo.remote_dir.path(),
        "refs/heads/beads/store",
        1_900_000_003,
        "simulate distinct remote winner",
    );
    set_ref_target(
        repo.remote_dir.path(),
        "refs/heads/beads/store",
        base_oid,
        "reset remote to base",
    );
    wait_for_fetched_remote_store_ref(repo.path(), base_oid, Duration::from_secs(1));

    let output = repo
        .bd()
        .env(
            "BD_TEST_MIGRATE_BEFORE_PUSH_WINNER",
            format!("once:{winner_oid}"),
        )
        .args(["migrate", "to", "2", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&output).expect("json");

    assert_eq!(
        json.get("commit_oid"),
        Some(&serde_json::Value::Null),
        "CLI migrate should converge to the remote winner without minting a second rewrite: {json}"
    );
    assert_eq!(
        json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_push"),
        "CLI migrate should report converged retry as skipped_no_push after refetch/recompute: {json}"
    );
    assert_eq!(
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        Some(winner_oid),
        "CLI migrate should realign the local store ref to the fetched remote winner"
    );
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        Some(winner_oid),
        "CLI migrate should leave the remote store ref on the winner"
    );
    assert_eq!(
        backup_ref_oid(repo.path(), base_oid),
        Some(base_oid),
        "CLI migrate should preserve a backup ref for the original local store head after a lost push race"
    );
}

#[test]
fn test_migrate_cli_retryable_push_race_exhausts_retry_budget() {
    let repo = TestRepo::new();
    let base_oid = fixture("rich_workflow").install(repo.path());
    push_store_ref(repo.path());

    let remote_repo = Repository::open(repo.remote_dir.path()).expect("open remote repo");
    beads_git::sync::migrate_store_ref_to_v2(
        &remote_repo,
        repo.remote_dir.path(),
        false,
        false,
        true,
        0,
    )
    .expect("winner migration")
    .commit_oid
    .expect("winner commit oid");
    let winner_oid = rewrite_store_ref_commit(
        repo.remote_dir.path(),
        "refs/heads/beads/store",
        1_900_000_004,
        "simulate distinct remote winner",
    );
    set_ref_target(
        repo.remote_dir.path(),
        "refs/heads/beads/store",
        base_oid,
        "reset remote to base",
    );
    wait_for_fetched_remote_store_ref(repo.path(), base_oid, Duration::from_secs(1));

    repo.bd()
        .env(
            "BD_TEST_MIGRATE_BEFORE_PUSH_WINNER",
            format!("once:{winner_oid}"),
        )
        .env("BD_TEST_MIGRATE_MAX_RETRIES", "0")
        .args(["migrate", "to", "2", "--json"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("too many sync retries (1)"));
    assert_eq!(
        store_ref_oid(repo.path(), "refs/heads/beads/store"),
        Some(base_oid),
        "CLI migrate should restore the original local store ref after retry exhaustion"
    );
    assert_eq!(
        store_ref_oid(repo.remote_dir.path(), "refs/heads/beads/store"),
        Some(winner_oid),
        "CLI migrate should leave the remote winner intact after retry exhaustion"
    );
    assert_eq!(
        backup_ref_oid(repo.path(), base_oid),
        Some(base_oid),
        "CLI migrate should preserve a backup ref for the original local store head after retry exhaustion"
    );
}

#[test]
fn test_migrate_infers_missing_root_slug_from_existing_ids() {
    let repo_a = TestRepo::new_local_only();
    fixture("rich_workflow").install(repo_a.path());
    rewrite_store_meta(repo_a.path(), Some(br#"{"format_version":1}"#));

    let repo_b = TestRepo::new_local_only();
    fixture("rich_workflow").install(repo_b.path());
    rewrite_store_meta(repo_b.path(), Some(br#"{"format_version":1}"#));

    for repo in [&repo_a, &repo_b] {
        run_bd_json(repo, &["migrate", "to", "2", "--no-push", "--json"]);

        let meta_bytes = read_tree_blob(
            &Repository::open(repo.path()).expect("open repo"),
            "meta.json",
        )
        .expect("meta.json after migration");
        let json: serde_json::Value = serde_json::from_slice(&meta_bytes).expect("meta json");
        let id = json
            .get("root_slug")
            .and_then(|value| value.as_str())
            .expect("migrated root_slug");
        assert!(
            id == "bd-rich",
            "migrated root slug should be inferred from existing ids, got {id}"
        );
    }
}

#[test]
fn test_migrate_preserves_existing_meta_root_slug_and_last_write_stamp() {
    let repo = TestRepo::new_local_only();
    fixture("rich_workflow").install(repo.path());

    let git = Repository::open(repo.path()).expect("open repo");
    let state_bytes = read_tree_blob(&git, "state.jsonl").expect("state.jsonl");
    let tombs_bytes = read_tree_blob(&git, "tombstones.jsonl").expect("tombstones.jsonl");
    let deps_bytes = read_tree_blob(&git, "deps.jsonl").expect("deps.jsonl");
    let notes_bytes = read_tree_blob(&git, "notes.jsonl").unwrap_or_default();
    let checksums =
        StoreChecksums::from_bytes(&state_bytes, &tombs_bytes, &deps_bytes, Some(&notes_bytes));
    let preserved_stamp = WriteStamp::new(1_765_400_555_000, 7);
    let meta_bytes = serialize_meta(Some("kept-meta"), Some(&preserved_stamp), &checksums)
        .expect("serialize meta");
    rewrite_store_meta(repo.path(), Some(&meta_bytes));

    run_bd_json(&repo, &["migrate", "to", "2", "--no-push", "--json"]);

    let meta_json = read_store_meta_json(repo.path());
    assert_eq!(
        meta_json.get("root_slug").and_then(|value| value.as_str()),
        Some("kept-meta"),
        "migration should preserve an existing root_slug from meta.json"
    );
    assert_eq!(
        meta_json
            .get("last_write_stamp")
            .and_then(|value| value.get(0))
            .and_then(|value| value.as_u64()),
        Some(preserved_stamp.wall_ms),
        "migration should preserve an existing last_write_stamp wall_ms"
    );
    assert_eq!(
        meta_json
            .get("last_write_stamp")
            .and_then(|value| value.get(1))
            .and_then(|value| value.as_u64()),
        Some(u64::from(preserved_stamp.counter)),
        "migration should preserve an existing last_write_stamp counter"
    );
    assert_meta_checksums_match_store(repo.path());
}

#[test]
fn test_runtime_strict_load_legacy_deps_shows_migration_hint() {
    let repo = TestRepo::new();
    write_store_commit(
        repo.path(),
        br#"{"from":"bd-abc1","to":"bd-abc2","kind":"blocks"}
"#,
        false,
        true,
    );

    repo.bd()
        .args(["list", "--json"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("bd migrate to 2"))
        .stderr(predicate::str::contains("legacy deps store detected"));
}

#[test]
fn test_runtime_strict_load_unrelated_parse_error_does_not_show_migration_hint() {
    let repo = TestRepo::new();
    write_store_commit(repo.path(), b"[]\n", false, true);

    repo.bd()
        .args(["list", "--json"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("deps.jsonl"))
        .stderr(predicate::str::contains("bd migrate to 2").not());
}

#[test]
fn test_migrate_to_2_missing_store_ref_errors_with_actionable_message() {
    let repo = TestRepo::new_local_only();

    repo.bd()
        .args(["migrate", "to", "2", "--json"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "refs/heads/beads/store is missing",
        ))
        .stderr(predicate::str::contains("migrate from-go"));
}

#[test]
fn test_migrate_to_rejects_unsupported_target_version() {
    let repo = TestRepo::new();

    repo.bd()
        .args(["migrate", "to", "999", "--json"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "unsupported migration target 999 (latest supported is 2)",
        ));
}

#[test]
fn test_migrate_to_without_force_fails_on_warningful_legacy_parse() {
    let repo = TestRepo::new_local_only();
    write_store_commit(
        repo.path(),
        br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
{"from":"","to":"bd-c","kind":"blocks"}
"#,
        false,
        false,
    );

    repo.bd()
        .args(["migrate", "to", "2", "--no-push", "--json"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("legacy deps parse produced"))
        .stderr(predicate::str::contains("--force"));
}

#[test]
fn test_migrate_to_force_no_push_allows_warningful_legacy_parse() {
    let repo = TestRepo::new_local_only();
    write_store_commit(
        repo.path(),
        br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
not-json
[]
{"from":"bd-a","to":"bd-c","kind":"not-a-kind"}
{"from":"bd-a","to":"bd-a","kind":"blocks"}
"#,
        false,
        false,
    );

    let output = repo
        .bd()
        .args(["migrate", "to", "2", "--force", "--no-push", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&output).expect("json");
    assert!(
        json.get("commit_oid")
            .and_then(|value| value.as_str())
            .is_some(),
        "forced migration should commit despite warnings: {json}"
    );
    let warnings = json
        .get("warnings")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        !warnings.is_empty(),
        "expected warning details when forcing through malformed legacy lines: {json}"
    );
    let legacy_warning_count = warnings
        .iter()
        .filter(|warning| {
            warning
                .as_str()
                .is_some_and(|text| text.starts_with("LEGACY_DEPS_"))
        })
        .count();
    let rebuild_warning_count = warnings
        .iter()
        .filter(|warning| {
            warning.as_str().is_some_and(|warning| {
                warning.contains("local daemon-store caches")
                    && warning.contains("reset and rebuild from repo truth on next load")
            })
        })
        .count();
    assert_eq!(
        legacy_warning_count, 4,
        "expected one malformed-legacy warning per bad input class: {json}"
    );
    assert_eq!(
        warnings.len(),
        legacy_warning_count + rebuild_warning_count,
        "unexpected non-legacy migration warning surfaced: {json}"
    );
    assert!(
        warnings
            .iter()
            .all(|warning| warning.as_str().is_some_and(|text| {
                text.starts_with("LEGACY_DEPS_")
                    || (text.contains("local daemon-store caches")
                        && text.contains("reset and rebuild from repo truth on next load"))
            })),
        "warning details should use stable LEGACY_DEPS_* prefixes: {json}"
    );
    let has_warning = |prefix: &str| {
        warnings.iter().any(|warning| {
            warning
                .as_str()
                .is_some_and(|text| text.starts_with(prefix))
        })
    };
    assert!(
        has_warning("LEGACY_DEPS_JSON(line=2):"),
        "forced migration should surface JSON parse warnings with line context: {json}"
    );
    assert!(
        has_warning("LEGACY_DEPS_SHAPE(line=3):"),
        "forced migration should surface shape warnings with line context: {json}"
    );
    assert!(
        has_warning("LEGACY_DEPS_KIND(line=4):"),
        "forced migration should surface kind warnings with line context: {json}"
    );
    assert!(
        has_warning("LEGACY_DEPS_KEY(line=5):"),
        "forced migration should surface key warnings with line context: {json}"
    );
    assert_eq!(
        json.get("push").and_then(|value| value.as_str()),
        Some("skipped_no_push"),
        "--no-push must report skipped_no_push: {json}"
    );
}

#[test]
fn test_migrate_to_2_ignores_blank_lines_and_collapses_duplicate_legacy_edges() {
    let repo = TestRepo::new_local_only();
    write_store_commit(
        repo.path(),
        br#"
{"from":"bd-a","to":"bd-b","kind":"blocks"}

{"from":"bd-a","to":"bd-b","kind":"blocks"}
"#,
        false,
        false,
    );

    let json = run_bd_json(&repo, &["migrate", "to", "2", "--no-push", "--json"]);
    assert!(
        json.get("commit_oid")
            .and_then(|value| value.as_str())
            .is_some(),
        "duplicate-edge migration should still commit a canonical rewrite: {json}"
    );
    let warnings = json
        .get("warnings")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    let rebuild_warning_count = warnings
        .iter()
        .filter(|warning| {
            warning.as_str().is_some_and(|warning| {
                warning.contains("local daemon-store caches")
                    && warning.contains("reset and rebuild from repo truth on next load")
            })
        })
        .count();
    assert!(
        warnings.len() == rebuild_warning_count,
        "blank lines and duplicate logical edge rows should not emit dependency migration warnings: {json}"
    );

    let state = read_store_state(repo.path());
    let key = dep_key("bd-a", "bd-b", DepKind::Blocks);
    assert!(
        state.dep_store().contains(&key),
        "duplicate legacy edge rows should still migrate the dependency"
    );
    assert_eq!(
        state.dep_store().dots_for(&key).map(|dots| dots.len()),
        Some(1),
        "duplicate logical edge rows should collapse to a single migrated dot"
    );
}

#[test]
fn test_migrate_from_go_reports_pushed_false_without_origin() {
    let repo = TestRepo::new_local_only();
    let export_path = repo.path().join("issues.jsonl");
    fs::write(&export_path, sample_go_export()).expect("failed to write export");

    let output = repo
        .bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&output).expect("json");
    assert_eq!(
        json.get("pushed").and_then(|value| value.as_bool()),
        Some(false),
        "from-go migration without origin must report pushed=false: {json}"
    );
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_migrate_basic_import() {
    let repo = TestRepo::new();

    // Write sample export
    let export_path = repo.path().join("issues.jsonl");
    fs::write(&export_path, sample_go_export()).expect("failed to write export");

    // Import (no-push since we have no remote)
    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--no-push",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"live_beads\": 3"));

    // Now list should show the imported issues
    repo.bd()
        .args(["list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Open task"))
        .stdout(predicate::str::contains("Bug to fix"))
        .stdout(predicate::str::contains("Completed feature"));

    // Show should work on imported issue
    repo.bd()
        .args(["show", "bd-abc2", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Something is broken"))
        .stdout(predicate::str::contains("In Progress"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_migrate_with_dependencies() {
    let repo = TestRepo::new();

    let export_path = repo.path().join("issues.jsonl");
    fs::write(&export_path, sample_with_deps()).expect("failed to write export");

    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--no-push",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"deps\": 1"));

    // bd-dep2 should be blocked by bd-dep1
    repo.bd()
        .args(["blocked", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Feature on top"));

    // Only bd-dep1 should be ready
    repo.bd()
        .args(["ready", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Foundation"))
        .stdout(predicate::str::contains("Feature on top").not());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_migrate_with_labels() {
    let repo = TestRepo::new();

    let export_path = repo.path().join("issues.jsonl");
    fs::write(&export_path, sample_with_labels()).expect("failed to write export");

    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--no-push",
        ])
        .assert()
        .success();

    // Should be able to filter by label
    repo.bd()
        .args(["list", "-l", "tech-debt", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Labeled issue"));

    repo.bd()
        .args(["list", "-l", "backend", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Labeled issue"));

    // Non-existent label should return empty
    repo.bd()
        .args(["list", "-l", "nonexistent", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Labeled issue").not());
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_migrate_with_comments() {
    let repo = TestRepo::new();

    let export_path = repo.path().join("issues.jsonl");
    fs::write(&export_path, sample_with_comments()).expect("failed to write export");

    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--no-push",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"notes\": 2"));

    // Comments should be visible
    repo.bd()
        .args(["comments", "bd-cmt1", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("First comment"))
        .stdout(predicate::str::contains("Reply here"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_migrate_epic_hierarchy() {
    let repo = TestRepo::new();

    let export_path = repo.path().join("issues.jsonl");
    fs::write(&export_path, sample_epic()).expect("failed to write export");

    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--no-push",
        ])
        .assert()
        .success();

    // Should list all including subtasks
    repo.bd()
        .args(["list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Big project"))
        .stdout(predicate::str::contains("Subtask one"))
        .stdout(predicate::str::contains("Subtask two"));

    // Epic status should show the epic
    repo.bd()
        .args(["epic", "status", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Big project"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_migrate_tombstone() {
    let repo = TestRepo::new();

    let export_path = repo.path().join("issues.jsonl");
    fs::write(&export_path, sample_tombstone()).expect("failed to write export");

    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--no-push",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tombstones\": 1"));

    // Tombstone should NOT appear in regular list
    repo.bd()
        .args(["list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("bd-tomb").not());

    // But deleted command should show it (use --all to bypass since filter)
    repo.bd()
        .args(["deleted", "--all", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("bd-tomb"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_migrate_refuses_without_force_if_exists() {
    let repo = TestRepo::new();

    let export_path = repo.path().join("issues.jsonl");
    fs::write(&export_path, sample_go_export()).expect("failed to write export");

    // First import succeeds
    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--no-push",
        ])
        .assert()
        .success();

    // Second import without --force should fail
    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--no-push",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("already exists"));

    // With --force should succeed (merge)
    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--no-push",
            "--force",
        ])
        .assert()
        .success();
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_migrate_then_create_new() {
    let repo = TestRepo::new();

    let export_path = repo.path().join("issues.jsonl");
    fs::write(&export_path, sample_go_export()).expect("failed to write export");

    // Import existing issues
    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--no-push",
        ])
        .assert()
        .success();

    // Create a new issue on top
    repo.bd()
        .args([
            "create",
            "Brand new issue",
            "--type=task",
            "--priority=1",
            "--json",
        ])
        .assert()
        .success();

    // Should have both old and new
    repo.bd()
        .args(["list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Open task")) // from import
        .stdout(predicate::str::contains("Brand new issue")); // newly created
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_migrate_preserves_repo_slug_and_new_ids_use_it() {
    let repo = TestRepo::new();

    let export_path = repo.path().join("issues.jsonl");
    fs::write(&export_path, sample_go_export_repo_slug()).expect("failed to write export");

    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--no-push",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"root_slug\": \"beads-rs\""));

    repo.bd()
        .args(["show", "beads-rs-abc2", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Something is broken"))
        .stdout(predicate::str::contains("In Progress"));

    let output = repo
        .bd()
        .args([
            "create",
            "Brand new issue",
            "--type=task",
            "--priority=1",
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let id = json["data"]["id"].as_str().unwrap().to_string();
    assert!(
        id.starts_with("beads-rs-"),
        "expected new id to use repo slug, got {id}"
    );
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_migrate_can_rewrite_root_slug() {
    let repo = TestRepo::new();

    let export_path = repo.path().join("issues.jsonl");
    fs::write(&export_path, sample_go_export_repo_slug()).expect("failed to write export");

    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--root-slug",
            "custom",
            "--no-push",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"root_slug\": \"custom\""));

    repo.bd()
        .args(["show", "custom-abc2", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Something is broken"));
}

#[cfg(feature = "slow-tests")]
#[test]
fn test_migrate_preserves_all_fields() {
    let repo = TestRepo::new();

    // Issue with all optional fields populated
    let full_issue = r#"{"id":"bd-full","title":"Full issue","description":"Desc here","design":"Design notes","acceptance_criteria":"AC here","notes":"Some notes","status":"open","priority":1,"issue_type":"feature","assignee":"dev1","estimated_minutes":120,"labels":["important"],"external_ref":"gh-123","created_at":"2025-01-01T10:00:00Z","updated_at":"2025-01-01T10:00:00Z"}
"#;

    let export_path = repo.path().join("issues.jsonl");
    fs::write(&export_path, full_issue).expect("failed to write export");

    repo.bd()
        .args([
            "migrate",
            "from-go",
            "--input",
            export_path.to_str().unwrap(),
            "--no-push",
        ])
        .assert()
        .success();

    // Show should display all fields
    repo.bd()
        .args(["show", "bd-full", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Desc here"))
        .stdout(predicate::str::contains("Design notes"))
        .stdout(predicate::str::contains("AC here"))
        .stdout(predicate::str::contains("dev1"))
        .stdout(predicate::str::contains("120"))
        .stdout(predicate::str::contains("important"));
}
