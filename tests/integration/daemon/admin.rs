//! Admin IPC ops: status and metrics.


use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;

use assert_cmd::Command;
use tempfile::TempDir;

use beads_rs::api::{
    AdminClockAnomaly, AdminClockAnomalyKind, AdminHealthReport, AdminHealthRisk, AdminHealthStats,
    AdminHealthSummary, AdminStatusOutput,
};
use beads_rs::{
    Applied, Durable, NamespaceId, NamespacePolicies, NamespacePolicy, ReplicaId, ReplicateMode,
    StoreId, Watermarks,
};
use crate::fixtures::daemon_runtime::shutdown_daemon;
use uuid::Uuid;

struct AdminFixture {
    runtime_dir: TempDir,
    repo_dir: TempDir,
    #[allow(dead_code)]
    remote_dir: TempDir,
}

impl AdminFixture {
    fn new() -> Self {
        let runtime_dir = TempDir::new().expect("create runtime dir");
        let repo_dir = TempDir::new().expect("create repo dir");
        let remote_dir = TempDir::new().expect("create remote dir");

        init_git_repo(repo_dir.path(), remote_dir.path());

        Self {
            runtime_dir,
            repo_dir,
            remote_dir,
        }
    }

    fn data_dir(&self) -> PathBuf {
        let dir = self.runtime_dir.path().join("data");
        fs::create_dir_all(&dir).expect("create test data dir");
        dir
    }

    fn bd(&self) -> Command {
        let data_dir = self.data_dir();
        let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("bd");
        cmd.current_dir(self.repo_dir.path());
        cmd.env("XDG_RUNTIME_DIR", self.runtime_dir.path());
        cmd.env("BD_WAL_DIR", self.runtime_dir.path());
        cmd.env("BD_DATA_DIR", &data_dir);
        cmd.env("BD_NO_AUTO_UPGRADE", "1");
        cmd
    }

    fn start_daemon(&self) {
        self.bd().arg("init").assert().success();
    }

    fn create_issue(&self, title: &str) {
        self.bd().args(["create", title]).assert().success();
    }
}

impl Drop for AdminFixture {
    fn drop(&mut self) {
        shutdown_daemon(self.runtime_dir.path());
    }
}

fn init_git_repo(repo_dir: &Path, remote_dir: &Path) {
    StdCommand::new("git")
        .args(["init", "--bare"])
        .current_dir(remote_dir)
        .output()
        .expect("git init --bare");

    StdCommand::new("git")
        .args(["init"])
        .current_dir(repo_dir)
        .output()
        .expect("git init");

    StdCommand::new("git")
        .args(["config", "user.email", "test@test.com"])
        .current_dir(repo_dir)
        .output()
        .expect("git config email");

    StdCommand::new("git")
        .args(["config", "user.name", "Test"])
        .current_dir(repo_dir)
        .output()
        .expect("git config name");

    StdCommand::new("git")
        .args(["remote", "add", "origin", remote_dir.to_str().unwrap()])
        .current_dir(repo_dir)
        .output()
        .expect("git remote add origin");
}

#[test]
fn admin_status_includes_expected_fields() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin status test");

    let output = fixture
        .bd()
        .args(["admin", "status", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let payload: serde_json::Value = serde_json::from_slice(&output).expect("parse json");
    assert_eq!(payload["result"], "admin_status");
    let data = &payload["data"];
    assert!(data["store_id"].is_string());
    assert!(data["replica_id"].is_string());
    assert!(data["namespaces"].is_array());
    assert!(data["watermarks_applied"].is_object());
    assert!(data["watermarks_durable"].is_object());
    assert!(data["wal"].is_array());
    assert!(data["checkpoints"].is_array());
}

#[test]
fn admin_metrics_includes_counters() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin metrics test");

    let output = fixture
        .bd()
        .args(["admin", "metrics", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let payload: serde_json::Value = serde_json::from_slice(&output).expect("parse json");
    assert_eq!(payload["result"], "admin_metrics");
    let counters = payload["data"]["counters"]
        .as_array()
        .expect("counters array");
    let has_wal_append = counters
        .iter()
        .any(|counter| counter["name"].as_str() == Some("wal_append_ok"));
    assert!(
        has_wal_append || !counters.is_empty(),
        "expected wal_append_ok or any counters"
    );
}

#[test]
fn admin_doctor_includes_checks() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin doctor test");

    let output = fixture
        .bd()
        .args(["admin", "doctor", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let payload: serde_json::Value = serde_json::from_slice(&output).expect("parse json");
    assert_eq!(payload["result"], "admin_doctor");
    let report = &payload["data"]["report"];
    assert!(report["checked_at_ms"].is_number());
    assert!(report["checks"].is_array());
    assert!(report["summary"].is_object());
}

#[test]
fn admin_scrub_reports_segment_header_failure() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin scrub test");

    let status_output = fixture
        .bd()
        .args(["admin", "status", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let status_payload: serde_json::Value =
        serde_json::from_slice(&status_output).expect("parse status json");
    let store_id = status_payload["data"]["store_id"]
        .as_str()
        .expect("store_id");

    let wal_dir = fixture
        .data_dir()
        .join("stores")
        .join(store_id)
        .join("wal")
        .join("core");
    fs::create_dir_all(&wal_dir).expect("create wal dir");
    let bad_path = wal_dir.join("segment-invalid.wal");
    fs::write(&bad_path, b"bad wal").expect("write invalid wal segment");

    let output = fixture
        .bd()
        .args(["admin", "scrub", "--json", "--max-records", "1"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let payload: serde_json::Value = serde_json::from_slice(&output).expect("parse json");
    assert_eq!(payload["result"], "admin_scrub");
    let checks = payload["data"]["report"]["checks"]
        .as_array()
        .expect("checks array");
    let wal_frames = checks
        .iter()
        .find(|check| check["id"].as_str() == Some("wal_frames"))
        .expect("wal_frames check");
    assert_eq!(wal_frames["status"], "fail");
}

#[test]
fn admin_reload_policies_reports_safe_and_restart_changes() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin reload policies");

    let status_output = fixture
        .bd()
        .args(["admin", "status", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let status_payload: serde_json::Value =
        serde_json::from_slice(&status_output).expect("parse status json");
    let store_id = status_payload["data"]["store_id"]
        .as_str()
        .expect("store_id");

    let mut core_policy = NamespacePolicy::core_default();
    core_policy.ready_eligible = false;
    core_policy.replicate_mode = ReplicateMode::Anchors;
    let mut namespaces = BTreeMap::new();
    namespaces.insert(NamespaceId::core(), core_policy);
    let policies = NamespacePolicies { namespaces };
    let toml = toml::to_string(&policies).expect("serialize policies toml");
    let policy_path = fixture
        .data_dir()
        .join("stores")
        .join(store_id)
        .join("namespaces.toml");
    fs::write(&policy_path, toml).expect("write namespaces.toml");

    let output = fixture
        .bd()
        .args(["admin", "reload-policies", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let payload: serde_json::Value = serde_json::from_slice(&output).expect("parse json");
    assert_eq!(payload["result"], "admin_reload_policies");

    let applied = payload["data"]["applied"]
        .as_array()
        .expect("applied array");
    let requires_restart = payload["data"]["requires_restart"]
        .as_array()
        .expect("requires_restart array");

    let applied_core = applied
        .iter()
        .find(|diff| diff["namespace"].as_str() == Some("core"))
        .expect("applied core diff");
    let applied_changes = applied_core["changes"].as_array().expect("applied changes");
    assert!(
        applied_changes
            .iter()
            .any(|change| change["field"].as_str() == Some("ready_eligible"))
    );

    let restart_core = requires_restart
        .iter()
        .find(|diff| diff["namespace"].as_str() == Some("core"))
        .expect("restart core diff");
    let restart_changes = restart_core["changes"].as_array().expect("restart changes");
    assert!(
        restart_changes
            .iter()
            .any(|change| change["field"].as_str() == Some("replicate_mode"))
    );
}

#[test]
fn admin_fingerprint_full_includes_shards() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin fingerprint full");

    let output = fixture
        .bd()
        .args(["admin", "fingerprint", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let payload: serde_json::Value = serde_json::from_slice(&output).expect("parse json");
    assert_eq!(payload["result"], "admin_fingerprint");
    assert_eq!(payload["data"]["mode"], "full");
    let namespaces = payload["data"]["namespaces"]
        .as_array()
        .expect("namespaces array");
    assert!(!namespaces.is_empty(), "expected at least one namespace");
    for namespace in namespaces {
        assert!(namespace["state_sha256"].is_string());
        assert!(namespace["tombstones_sha256"].is_string());
        assert!(namespace["deps_sha256"].is_string());
        assert!(namespace["namespace_root"].is_string());
        let shards = namespace["shards"].as_array().expect("shards array");
        assert_eq!(shards.len(), 256 * 3);
    }
}

#[test]
fn admin_fingerprint_sample_is_deterministic() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin fingerprint sample");

    let args = [
        "admin",
        "fingerprint",
        "--json",
        "--sample",
        "3",
        "--nonce",
        "fixed-nonce",
    ];
    let output_a = fixture
        .bd()
        .args(args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output_b = fixture
        .bd()
        .args(args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let payload_a: serde_json::Value = serde_json::from_slice(&output_a).expect("parse json");
    let payload_b: serde_json::Value = serde_json::from_slice(&output_b).expect("parse json");
    assert_eq!(payload_a["result"], "admin_fingerprint");
    assert_eq!(payload_a["data"]["mode"], "sample");
    assert_eq!(payload_a["data"]["sample"]["shard_count"], 3);
    assert_eq!(payload_a["data"]["sample"]["nonce"], "fixed-nonce");

    let ns_a = payload_a["data"]["namespaces"][0].clone();
    let ns_b = payload_b["data"]["namespaces"][0].clone();
    assert_eq!(ns_a["namespace_root"], ns_b["namespace_root"]);
    assert_eq!(ns_a["shards"], ns_b["shards"]);
    let shards = ns_a["shards"].as_array().expect("shards array");
    assert_eq!(shards.len(), 3 * 3);
}

#[test]
fn admin_clock_anomaly_serializes_in_status_and_doctor() {
    let anomaly = AdminClockAnomaly {
        at_wall_ms: 1_700_000_000_000,
        kind: AdminClockAnomalyKind::ForwardJumpClamped,
        delta_ms: 42,
    };
    let status = AdminStatusOutput {
        store_id: StoreId::new(Uuid::from_bytes([1u8; 16])),
        replica_id: ReplicaId::new(Uuid::from_bytes([2u8; 16])),
        namespaces: Vec::new(),
        watermarks_applied: Watermarks::<Applied>::new(),
        watermarks_durable: Watermarks::<Durable>::new(),
        last_clock_anomaly: Some(anomaly.clone()),
        wal: Vec::new(),
        replication: Vec::new(),
        replica_liveness: Vec::new(),
        checkpoints: Vec::new(),
    };
    let status_json = serde_json::to_value(status).expect("serialize status");
    assert_eq!(
        status_json["last_clock_anomaly"]["kind"],
        "forward_jump_clamped"
    );
    assert_eq!(status_json["last_clock_anomaly"]["delta_ms"], 42);

    let report = AdminHealthReport {
        checked_at_ms: 1_700_000_000_001,
        stats: AdminHealthStats::default(),
        checks: Vec::new(),
        summary: AdminHealthSummary {
            risk: AdminHealthRisk::Low,
            safe_to_accept_writes: true,
            safe_to_prune_wal: true,
            safe_to_rebuild_index: true,
        },
        last_clock_anomaly: Some(anomaly),
    };
    let report_json = serde_json::to_value(report).expect("serialize report");
    assert!(report_json["last_clock_anomaly"].is_object());
}

#[test]
fn admin_maintenance_blocks_mutations() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("maintenance baseline");

    fixture
        .bd()
        .args(["admin", "maintenance", "on"])
        .assert()
        .success();

    fixture
        .bd()
        .args(["create", "maintenance blocked"])
        .assert()
        .failure();

    fixture
        .bd()
        .args(["admin", "maintenance", "off"])
        .assert()
        .success();

    fixture
        .bd()
        .args(["create", "maintenance allowed"])
        .assert()
        .success();
}

#[test]
fn admin_rebuild_index_requires_maintenance() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("rebuild baseline");

    fixture
        .bd()
        .args(["admin", "rebuild-index"])
        .assert()
        .failure();

    fixture
        .bd()
        .args(["admin", "maintenance", "on"])
        .assert()
        .success();

    fixture
        .bd()
        .args(["admin", "rebuild-index"])
        .assert()
        .success();
}
