//! Admin IPC ops: status and metrics.
//!
//! These tests intentionally remain in the default suite: they start a daemon
//! but avoid time-based sleeps to keep coverage fast and deterministic.
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::fixtures::bd_runtime::{BdCommandProfile, BdRuntimeRepo, CheckpointMode};
use beads_api::{
    AdminClockAnomaly, AdminClockAnomalyKind, AdminHealthReport, AdminHealthRisk, AdminHealthStats,
    AdminHealthSummary, AdminMetricsOutput, AdminReloadPoliciesOutput, AdminScrubOutput,
    AdminStatusOutput,
};
use beads_api::{AdminFingerprintMode, AdminFingerprintOutput, AdminFingerprintSample};
use beads_core::BeadType;
use beads_core::{
    Applied, Durable, NamespaceId, NamespacePolicies, NamespacePolicy, Priority, ReplicaId,
    ReplicateMode, StoreId, Watermarks,
};
use beads_surface::ipc::{
    AdminDoctorPayload, AdminFingerprintPayload, AdminMaintenanceModePayload, AdminOp,
    AdminScrubPayload, CreatePayload, EmptyPayload, IpcClient, MutationCtx, MutationMeta,
    ReadConsistency, ReadCtx, RepoCtx, Request, Response, ResponsePayload,
};
use beads_surface::ops::OpResult;
use uuid::Uuid;

struct AdminFixture {
    runtime: BdRuntimeRepo,
    checkpoint_mode: CheckpointMode,
}

impl AdminFixture {
    fn new() -> Self {
        Self::with_checkpoints(CheckpointMode::Disabled)
    }

    fn with_checkpoints(checkpoints_enabled: CheckpointMode) -> Self {
        Self {
            runtime: BdRuntimeRepo::new_with_origin(),
            checkpoint_mode: checkpoints_enabled,
        }
    }

    fn repo_path(&self) -> &Path {
        self.runtime.path()
    }

    fn data_dir(&self) -> &Path {
        self.runtime.data_dir()
    }

    fn ipc_client(&self) -> IpcClient {
        self.runtime.ipc_client_no_autostart()
    }

    fn start_daemon(&self) {
        let profile = BdCommandProfile::fast_daemon().with_checkpoints(self.checkpoint_mode);
        self.runtime.start_daemon(profile);
    }

    fn create_issue(&self, title: &str) {
        let request = Request::Create {
            ctx: MutationCtx::new(self.repo_path().to_path_buf(), MutationMeta::default()),
            payload: CreatePayload {
                id: None,
                parent: None,
                title: title.to_string(),
                bead_type: BeadType::Task,
                priority: Priority::default(),
                description: None,
                design: None,
                acceptance_criteria: None,
                assignee: None,
                external_ref: None,
                estimated_minutes: None,
                labels: Vec::new(),
                dependencies: Vec::new(),
            },
        };
        let response = self.send_request(&request);
        match response {
            Response::Ok {
                ok: ResponsePayload::Op(op),
            } => match op.result {
                OpResult::Created { .. } => {}
                other => panic!("unexpected create result: {other:?}"),
            },
            Response::Err { err } => panic!("create error: {err:?}"),
            other => panic!("unexpected create response: {other:?}"),
        }
    }

    fn create_issue_result(&self, title: &str) -> Response {
        let request = Request::Create {
            ctx: MutationCtx::new(self.repo_path().to_path_buf(), MutationMeta::default()),
            payload: CreatePayload {
                id: None,
                parent: None,
                title: title.to_string(),
                bead_type: BeadType::Task,
                priority: Priority::default(),
                description: None,
                design: None,
                acceptance_criteria: None,
                assignee: None,
                external_ref: None,
                estimated_minutes: None,
                labels: Vec::new(),
                dependencies: Vec::new(),
            },
        };
        self.send_request(&request)
    }

    fn admin_status(&self) -> AdminStatusOutput {
        match self.send_query(Request::Admin(AdminOp::Status {
            ctx: ReadCtx::new(self.repo_path().to_path_buf(), ReadConsistency::default()),
            payload: EmptyPayload {},
        })) {
            beads_api::QueryResult::AdminStatus(status) => status,
            other => panic!("unexpected admin status payload: {other:?}"),
        }
    }

    fn admin_metrics(&self) -> AdminMetricsOutput {
        match self.send_query(Request::Admin(AdminOp::Metrics {
            ctx: ReadCtx::new(self.repo_path().to_path_buf(), ReadConsistency::default()),
            payload: EmptyPayload {},
        })) {
            beads_api::QueryResult::AdminMetrics(metrics) => metrics,
            other => panic!("unexpected admin metrics payload: {other:?}"),
        }
    }

    fn admin_doctor(&self) -> beads_api::AdminDoctorOutput {
        match self.send_query(Request::Admin(AdminOp::Doctor {
            ctx: ReadCtx::new(self.repo_path().to_path_buf(), ReadConsistency::default()),
            payload: AdminDoctorPayload {
                max_records_per_namespace: None,
                verify_checkpoint_cache: false,
            },
        })) {
            beads_api::QueryResult::AdminDoctor(output) => output,
            other => panic!("unexpected admin doctor payload: {other:?}"),
        }
    }

    fn admin_scrub(&self, max_records_per_namespace: Option<u64>) -> AdminScrubOutput {
        match self.send_query(Request::Admin(AdminOp::Scrub {
            ctx: ReadCtx::new(self.repo_path().to_path_buf(), ReadConsistency::default()),
            payload: AdminScrubPayload {
                max_records_per_namespace,
                verify_checkpoint_cache: false,
            },
        })) {
            beads_api::QueryResult::AdminScrub(output) => output,
            other => panic!("unexpected admin scrub payload: {other:?}"),
        }
    }

    fn admin_reload_policies(&self) -> AdminReloadPoliciesOutput {
        match self.send_query(Request::Admin(AdminOp::ReloadPolicies {
            ctx: RepoCtx::new(self.repo_path().to_path_buf()),
            payload: EmptyPayload {},
        })) {
            beads_api::QueryResult::AdminReloadPolicies(output) => output,
            other => panic!("unexpected admin reload policies payload: {other:?}"),
        }
    }

    fn admin_fingerprint(
        &self,
        mode: AdminFingerprintMode,
        sample: Option<AdminFingerprintSample>,
    ) -> AdminFingerprintOutput {
        match self.send_query(Request::Admin(AdminOp::Fingerprint {
            ctx: ReadCtx::new(self.repo_path().to_path_buf(), ReadConsistency::default()),
            payload: AdminFingerprintPayload { mode, sample },
        })) {
            beads_api::QueryResult::AdminFingerprint(output) => output,
            other => panic!("unexpected admin fingerprint payload: {other:?}"),
        }
    }

    fn admin_maintenance(&self, enabled: bool) -> Response {
        self.send_request(&Request::Admin(AdminOp::MaintenanceMode {
            ctx: RepoCtx::new(self.repo_path().to_path_buf()),
            payload: AdminMaintenanceModePayload { enabled },
        }))
    }

    fn admin_rebuild_index(&self) -> Response {
        self.send_request(&Request::Admin(AdminOp::RebuildIndex {
            ctx: RepoCtx::new(self.repo_path().to_path_buf()),
            payload: EmptyPayload {},
        }))
    }

    fn send_query(&self, request: Request) -> beads_api::QueryResult {
        let response = self.send_request(&request);
        match response {
            Response::Ok {
                ok: ResponsePayload::Query(result),
            } => result,
            Response::Err { err } => panic!("admin request failed: {err:?}"),
            other => panic!("unexpected admin response: {other:?}"),
        }
    }

    fn send_request(&self, request: &Request) -> Response {
        self.ipc_client()
            .send_request_no_autostart(request)
            .expect("ipc request")
    }
}

#[test]
fn admin_status_includes_expected_fields() {
    let fixture = AdminFixture::with_checkpoints(CheckpointMode::Enabled);
    fixture.start_daemon();
    fixture.create_issue("admin status test");

    let status = fixture.admin_status();
    assert!(status.namespaces.contains(&NamespaceId::core()));
    assert!(!status.wal.is_empty());
    assert!(!status.checkpoints.is_empty());
}

#[test]
fn admin_metrics_includes_counters() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin metrics test");

    let metrics = fixture.admin_metrics();
    let has_wal_append = metrics
        .counters
        .iter()
        .any(|counter| counter.name == "wal_append_ok");
    assert!(
        has_wal_append || !metrics.counters.is_empty(),
        "expected wal_append_ok or any counters"
    );
}

#[test]
fn admin_metrics_includes_ipc_request_histograms() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin metrics ipc request");

    let metrics = fixture.admin_metrics();
    assert!(metrics.histograms.iter().any(|histogram| {
        histogram.name == "ipc_request_duration"
            && histogram
                .labels
                .iter()
                .any(|label| label.key == "request_type")
    }));
    assert!(metrics.counters.iter().any(|counter| {
        counter.name == "ipc_request_total"
            && counter
                .labels
                .iter()
                .any(|label| label.key == "request_type")
    }));
}

#[test]
fn admin_doctor_includes_checks() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin doctor test");

    let report = fixture.admin_doctor().report;
    assert!(report.checked_at_ms > 0);
    assert!(!report.checks.is_empty());
}

#[test]
fn admin_scrub_reports_segment_header_failure() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin scrub test");

    let status = fixture.admin_status();
    let store_id = status.store_id.to_string();

    let wal_dir = fixture
        .data_dir()
        .join("stores")
        .join(store_id)
        .join("wal")
        .join("core");
    fs::create_dir_all(&wal_dir).expect("create wal dir");
    let bad_path = wal_dir.join("segment-invalid.wal");
    fs::write(&bad_path, b"bad wal").expect("write invalid wal segment");

    let report = fixture.admin_scrub(Some(1)).report;
    let wal_frames = report
        .checks
        .iter()
        .find(|check| check.id == beads_api::AdminHealthCheckId::WalFrames)
        .expect("wal_frames check");
    assert_eq!(wal_frames.status, beads_api::AdminHealthStatus::Fail);
}

#[test]
fn admin_reload_policies_reports_safe_and_restart_changes() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin reload policies");

    let store_id = fixture.admin_status().store_id.to_string();

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

    let output = fixture.admin_reload_policies();
    let applied = output.applied;
    let requires_restart = output.requires_restart;

    let applied_core = applied
        .iter()
        .find(|diff| diff.namespace == NamespaceId::core())
        .expect("applied core diff");
    let applied_changes = &applied_core.changes;
    assert!(
        applied_changes
            .iter()
            .any(|change| change.field == "ready_eligible")
    );
    assert!(
        applied_changes
            .iter()
            .any(|change| change.field == "replicate_mode")
    );

    assert!(
        requires_restart
            .iter()
            .all(|diff| diff.namespace != NamespaceId::core()),
        "core replicate_mode should hot-reload instead of requiring restart"
    );
}

#[test]
fn admin_fingerprint_full_includes_shards() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin fingerprint full");

    let output = fixture.admin_fingerprint(AdminFingerprintMode::Full, None);
    assert_eq!(output.mode, AdminFingerprintMode::Full);
    assert!(
        !output.namespaces.is_empty(),
        "expected at least one namespace"
    );
    for namespace in &output.namespaces {
        assert_eq!(namespace.shards.len(), 256 * 3);
    }
}

#[test]
fn admin_fingerprint_sample_is_deterministic() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("admin fingerprint sample");

    let sample = AdminFingerprintSample {
        shard_count: 3,
        nonce: "fixed-nonce".to_string(),
    };
    let output_a = fixture.admin_fingerprint(AdminFingerprintMode::Sample, Some(sample.clone()));
    let output_b = fixture.admin_fingerprint(AdminFingerprintMode::Sample, Some(sample));

    assert_eq!(output_a.mode, AdminFingerprintMode::Sample);
    assert_eq!(output_a.sample.as_ref().expect("sample").shard_count, 3);
    assert_eq!(
        output_a.sample.as_ref().expect("sample").nonce,
        "fixed-nonce"
    );

    let ns_a = &output_a.namespaces[0];
    let ns_b = &output_b.namespaces[0];
    assert_eq!(ns_a.namespace_root, ns_b.namespace_root);
    assert_eq!(ns_a.shards.len(), 3 * 3);
    assert_eq!(ns_a.shards.len(), ns_b.shards.len());
    for (a, b) in ns_a.shards.iter().zip(&ns_b.shards) {
        assert_eq!(a.kind, b.kind);
        assert_eq!(a.index, b.index);
        assert_eq!(a.sha256, b.sha256);
    }
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
        replication_listen_addr: None,
        namespaces: Vec::new(),
        watermarks_applied: Watermarks::<Applied>::new(),
        watermarks_durable: Watermarks::<Durable>::new(),
        last_clock_anomaly: Some(anomaly.clone()),
        wal: Vec::new(),
        wal_warnings: Vec::new(),
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

    match fixture.admin_maintenance(true) {
        Response::Ok {
            ok: ResponsePayload::Query(beads_api::QueryResult::AdminMaintenanceMode(_)),
        } => {}
        other => panic!("unexpected maintenance on response: {other:?}"),
    }

    match fixture.create_issue_result("maintenance blocked") {
        Response::Err { .. } => {}
        other => panic!("unexpected create response while in maintenance: {other:?}"),
    }

    match fixture.admin_maintenance(false) {
        Response::Ok {
            ok: ResponsePayload::Query(beads_api::QueryResult::AdminMaintenanceMode(_)),
        } => {}
        other => panic!("unexpected maintenance off response: {other:?}"),
    }

    fixture.create_issue("maintenance allowed");
}

#[test]
fn admin_rebuild_index_requires_maintenance() {
    let fixture = AdminFixture::new();
    fixture.start_daemon();
    fixture.create_issue("rebuild baseline");

    match fixture.admin_rebuild_index() {
        Response::Err { .. } => {}
        other => panic!("unexpected rebuild-index response: {other:?}"),
    }

    match fixture.admin_maintenance(true) {
        Response::Ok {
            ok: ResponsePayload::Query(beads_api::QueryResult::AdminMaintenanceMode(_)),
        } => {}
        other => panic!("unexpected maintenance on response: {other:?}"),
    }

    match fixture.admin_rebuild_index() {
        Response::Ok {
            ok: ResponsePayload::Query(beads_api::QueryResult::AdminRebuildIndex(_)),
        } => {}
        other => panic!("unexpected rebuild-index response: {other:?}"),
    }
}
