//! Integration tests for migration from beads-go.
//!
//! Tests importing issues.jsonl from Go beads export format.

use std::fs;
use std::path::Path;

use crate::fixtures::daemon_runtime::shutdown_daemon;
use crate::fixtures::git::{init_bare_repo, init_repo, init_repo_with_origin};
use assert_cmd::Command;
use beads_git::wire::{StoreChecksums, serialize_meta};
use git2::{ObjectType, Repository, Signature};
use predicates::prelude::*;
use tempfile::TempDir;

fn data_dir_for_runtime(runtime_dir: &std::path::Path) -> std::path::PathBuf {
    let dir = runtime_dir.join("data");
    fs::create_dir_all(&dir).expect("failed to create test data dir");
    dir
}

/// Test fixture: working repo + bare remote.
struct TestRepo {
    work_dir: TempDir,
    #[allow(dead_code)]
    remote_dir: TempDir,
    runtime_dir: TempDir,
    data_dir: std::path::PathBuf,
}

impl TestRepo {
    fn new() -> Self {
        let remote_dir = TempDir::new().expect("failed to create remote dir");
        init_bare_repo(remote_dir.path()).expect("failed to init bare repo");

        let work_dir = TempDir::new().expect("failed to create work dir");

        init_repo_with_origin(work_dir.path(), remote_dir.path())
            .expect("failed to init repo with origin");

        let runtime_dir = TempDir::new().expect("failed to create runtime dir");
        let data_dir = data_dir_for_runtime(runtime_dir.path());

        Self {
            work_dir,
            remote_dir,
            runtime_dir,
            data_dir,
        }
    }

    fn new_local_only() -> Self {
        let remote_dir = TempDir::new().expect("failed to create remote dir");
        let work_dir = TempDir::new().expect("failed to create work dir");
        init_repo(work_dir.path()).expect("failed to init local-only repo");

        let runtime_dir = TempDir::new().expect("failed to create runtime dir");
        let data_dir = data_dir_for_runtime(runtime_dir.path());

        Self {
            work_dir,
            remote_dir,
            runtime_dir,
            data_dir,
        }
    }

    fn path(&self) -> &std::path::Path {
        self.work_dir.path()
    }

    fn bd(&self) -> Command {
        let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("bd");
        cmd.current_dir(self.path());
        cmd.env("XDG_RUNTIME_DIR", self.runtime_dir.path());
        cmd.env("BD_DATA_DIR", &self.data_dir);
        cmd.env("BD_NO_AUTO_UPGRADE", "1");
        cmd.env("BD_TESTING", "1");
        cmd.env("BD_TEST_DISABLE_GIT_SYNC", "1");
        cmd
    }
}

impl Drop for TestRepo {
    fn drop(&mut self) {
        shutdown_daemon(self.runtime_dir.path());
    }
}

/// Sample Go beads JSONL export with various issue types.
fn sample_go_export() -> &'static str {
    r#"{"id":"bd-abc1","title":"Open task","description":"A task that is open","status":"open","priority":1,"issue_type":"task","created_at":"2025-01-01T10:00:00Z","updated_at":"2025-01-01T10:00:00Z"}
{"id":"bd-abc2","title":"Bug to fix","description":"Something is broken","status":"in_progress","priority":0,"issue_type":"bug","assignee":"alice","created_at":"2025-01-02T10:00:00Z","updated_at":"2025-01-02T12:00:00Z"}
{"id":"bd-abc3","title":"Completed feature","description":"All done","status":"closed","priority":2,"issue_type":"feature","created_at":"2025-01-03T10:00:00Z","updated_at":"2025-01-03T15:00:00Z","closed_at":"2025-01-03T15:00:00Z","close_reason":"Shipped it"}
"#
}

fn write_store_commit(
    repo_path: &Path,
    deps_bytes: &[u8],
    include_notes: bool,
    include_meta: bool,
) {
    let repo = Repository::open(repo_path).expect("open repo");
    let state_bytes = b"".to_vec();
    let tombs_bytes = b"".to_vec();
    let notes_bytes = if include_notes {
        b"".to_vec()
    } else {
        Vec::new()
    };

    let state_oid = repo.blob(&state_bytes).expect("state blob");
    let tombs_oid = repo.blob(&tombs_bytes).expect("tombs blob");
    let deps_oid = repo.blob(deps_bytes).expect("deps blob");
    let notes_oid = if include_notes {
        Some(repo.blob(&notes_bytes).expect("notes blob"))
    } else {
        None
    };
    let meta_oid = if include_meta {
        let checksums =
            StoreChecksums::from_bytes(&state_bytes, &tombs_bytes, deps_bytes, Some(&notes_bytes));
        let meta = serialize_meta(Some("bd"), None, &checksums).expect("meta bytes");
        Some(repo.blob(&meta).expect("meta blob"))
    } else {
        None
    };

    let mut builder = repo.treebuilder(None).expect("treebuilder");
    builder
        .insert("state.jsonl", state_oid, 0o100644)
        .expect("state insert");
    builder
        .insert("tombstones.jsonl", tombs_oid, 0o100644)
        .expect("tombs insert");
    builder
        .insert("deps.jsonl", deps_oid, 0o100644)
        .expect("deps insert");
    if let Some(notes_oid) = notes_oid {
        builder
            .insert("notes.jsonl", notes_oid, 0o100644)
            .expect("notes insert");
    }
    if let Some(meta_oid) = meta_oid {
        builder
            .insert("meta.json", meta_oid, 0o100644)
            .expect("meta insert");
    }

    let tree_oid = builder.write().expect("tree write");
    let tree = repo.find_tree(tree_oid).expect("find tree");
    let sig = Signature::now("test", "test@example.com").expect("signature");
    let parent = repo
        .refname_to_id("refs/heads/beads/store")
        .ok()
        .and_then(|oid| repo.find_commit(oid).ok());
    let parent_refs: Vec<_> = parent.iter().collect();
    let commit_oid = repo
        .commit(None, &sig, &sig, "test store commit", &tree, &parent_refs)
        .expect("commit");
    repo.reference(
        "refs/heads/beads/store",
        commit_oid,
        true,
        "test update store ref",
    )
    .expect("update store ref");
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

    let output = repo
        .bd()
        .args(["migrate", "detect", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&output).expect("json");

    assert!(
        json.get("deps_format").is_some(),
        "expected deps_format in detect output, got: {json}"
    );
    assert!(
        json.get("effective_format_version").is_some(),
        "expected effective_format_version in detect output, got: {json}"
    );
    assert!(
        json.get("reasons").is_some(),
        "expected reasons in detect output, got: {json}"
    );
}

#[test]
fn test_migrate_to_1_dry_run_is_implemented() {
    let repo = TestRepo::new();
    write_store_commit(
        repo.path(),
        br#"{"cc":{"max":{},"dots":[]},"entries":[]}
"#,
        true,
        true,
    );

    repo.bd()
        .args(["migrate", "to", "1", "--dry-run", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"dry_run\": true"));
}

#[test]
fn test_migrate_to_1_noop_when_already_canonical() {
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
        .args(["migrate", "to", "1", "--json"])
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
        json.get("pushed").and_then(|value| value.as_bool()),
        Some(false),
        "no-op migration must not report push activity: {json}"
    );
}

#[test]
fn test_migrate_to_1_rewrites_legacy_deps_and_store_invariants() {
    let repo = TestRepo::new();
    write_store_commit(
        repo.path(),
        br#"{"from":"bd-abc1","to":"bd-abc2","kind":"blocks"}
"#,
        false,
        false,
    );

    let output = repo
        .bd()
        .args(["migrate", "to", "1", "--no-push", "--json"])
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
fn test_migrate_to_1_reports_pushed_false_without_origin() {
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
        .args(["migrate", "to", "1", "--json"])
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
        json.get("pushed").and_then(|value| value.as_bool()),
        Some(false),
        "migration without origin must report pushed=false: {json}"
    );
}

#[test]
fn test_migrate_detect_flags_legacy_deps_even_with_meta_v1() {
    let repo = TestRepo::new();
    // Legacy line-per-edge deps content plus v1 meta/checksums.
    write_store_commit(
        repo.path(),
        br#"{"from":"bd-abc1","to":"bd-abc2","kind":"blocks"}
"#,
        true,
        true,
    );

    let output = repo
        .bd()
        .args(["migrate", "detect", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&output).expect("json");
    assert_eq!(
        json.get("deps_format").and_then(|v| v.as_str()),
        Some("legacy_edges"),
        "expected legacy deps classification, got: {json}"
    );
    assert_eq!(
        json.get("needs_migration").and_then(|v| v.as_bool()),
        Some(true),
        "expected needs_migration=true, got: {json}"
    );
}

#[test]
fn test_migrate_to_1_missing_store_ref_errors_with_actionable_message() {
    let repo = TestRepo::new_local_only();

    repo.bd()
        .args(["migrate", "to", "1", "--json"])
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
            "unsupported migration target 999 (latest supported is 1)",
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
        .args(["migrate", "to", "1", "--no-push", "--json"])
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
{"from":"","to":"bd-c","kind":"blocks"}
"#,
        false,
        false,
    );

    let output = repo
        .bd()
        .args(["migrate", "to", "1", "--force", "--no-push", "--json"])
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
    assert_eq!(
        json.get("pushed").and_then(|value| value.as_bool()),
        Some(false),
        "--no-push must report pushed=false: {json}"
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
        .stdout(predicate::str::contains("in_progress"));
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
        .stdout(predicate::str::contains("in_progress"));

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
