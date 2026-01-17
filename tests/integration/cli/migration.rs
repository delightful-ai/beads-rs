//! Integration tests for migration from beads-go.
//!
//! Tests importing issues.jsonl from Go beads export format.

use std::fs;

use crate::fixtures::daemon_runtime::shutdown_daemon;
use assert_cmd::Command;
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
        std::process::Command::new("git")
            .args(["init", "--bare"])
            .current_dir(remote_dir.path())
            .output()
            .expect("failed to init bare repo");

        let work_dir = TempDir::new().expect("failed to create work dir");

        std::process::Command::new("git")
            .args(["init"])
            .current_dir(work_dir.path())
            .output()
            .expect("failed to git init");

        std::process::Command::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(work_dir.path())
            .output()
            .expect("failed to configure git email");

        std::process::Command::new("git")
            .args(["config", "user.name", "Test User"])
            .current_dir(work_dir.path())
            .output()
            .expect("failed to configure git name");

        std::process::Command::new("git")
            .args([
                "remote",
                "add",
                "origin",
                remote_dir.path().to_str().unwrap(),
            ])
            .current_dir(work_dir.path())
            .output()
            .expect("failed to add remote");

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
