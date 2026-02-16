#![cfg(feature = "slow-tests")]

use std::num::NonZeroU32;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::fixtures::load_gen::LoadGenerator;
use crate::fixtures::receipt;
use crate::fixtures::repl_rig::{
    DurabilityEligibility, FaultProfile, ReplRig, ReplRigOptions, TailnetTraceConfig,
};
use crate::fixtures::tailnet_proxy::TailnetTraceMode;
use beads_rs::api::QueryResult;
use beads_rs::core::error::details::{DurabilityTimeoutDetails, RequireMinSeenUnsatisfiedDetails};
use beads_rs::core::{
    BeadId, BeadType, DurabilityClass, HeadStatus, NamespaceId, Priority, ProtocolErrorCode,
    ReplicaDurabilityRole, ReplicaEntry, Seq0,
};
use beads_rs::surface::ipc::{
    AdminCheckpointWaitPayload, AdminOp, CreatePayload, IdPayload, IpcClient, MutationCtx,
    MutationMeta, ReadConsistency, ReadCtx, RepoCtx, Request, Response, ResponsePayload,
};
use beads_rs::surface::ops::OpResult;
use tempfile::TempDir;

fn sample_ids<'a>(ids: &'a [String]) -> Vec<&'a String> {
    match ids.len() {
        0 => Vec::new(),
        1..=3 => ids.iter().collect(),
        _ => {
            let mid = ids.len() / 2;
            vec![&ids[0], &ids[mid], &ids[ids.len() - 1]]
        }
    }
}

fn wait_for_sample_on(rig: &ReplRig, ids: &[String], nodes: &[usize], timeout: Duration) {
    assert!(!nodes.is_empty(), "nodes must not be empty");
    let samples = sample_ids(ids);
    for (idx, id) in samples.into_iter().enumerate() {
        let node_idx = nodes[(idx + 1) % nodes.len()];
        rig.wait_for_show(node_idx, id, timeout);
    }
}

fn wait_for_sample(rig: &ReplRig, ids: &[String], timeout: Duration) {
    let nodes: Vec<usize> = (0..rig.nodes().len()).collect();
    wait_for_sample_on(rig, ids, &nodes, timeout);
}

fn wal_total_bytes(rig: &ReplRig, node_idx: usize) -> u64 {
    let status = rig.admin_status(node_idx);
    let wal = status
        .wal
        .iter()
        .find(|entry| entry.namespace == NamespaceId::core())
        .expect("core namespace wal");
    wal.total_bytes
}

fn churn_node(node: &crate::fixtures::repl_rig::Node, total: usize, workers: usize) {
    let client = IpcClient::for_runtime_dir(node.runtime_dir()).with_autostart(false);
    let mut generator = LoadGenerator::with_client(node.repo_dir().to_path_buf(), client);
    generator.config_mut().total_requests = total;
    generator.config_mut().workers = workers;
    generator.config_mut().max_errors = 4;
    let report = generator.run();
    assert_eq!(
        report.failures, 0,
        "load generator failures: {:#?}",
        report.errors
    );
}

fn wait_for_checkpoint(rig: &ReplRig, idx: usize, namespace: &NamespaceId, timeout: Duration) {
    let node = rig.node(idx);
    let runtime_dir = node.runtime_dir().to_path_buf();
    let repo_dir = node.repo_dir().to_path_buf();
    let namespace_label = namespace.as_str().to_string();
    let request_namespace = namespace.clone();

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let client = IpcClient::for_runtime_dir(&runtime_dir).with_autostart(false);
        let request = Request::Admin(AdminOp::CheckpointWait {
            ctx: RepoCtx::new(repo_dir),
            payload: AdminCheckpointWaitPayload {
                namespace: Some(request_namespace),
            },
        });
        let response = client.send_request_no_autostart(&request);
        let _ = tx.send(response);
    });

    let response = match rx.recv_timeout(timeout) {
        Ok(response) => response,
        Err(_) => panic!("checkpoint for {namespace_label} did not complete in time",),
    };

    match response {
        Ok(Response::Ok {
            ok: ResponsePayload::Query(QueryResult::AdminCheckpoint(_)),
        }) => {}
        Ok(Response::Err { err }) => panic!("checkpoint wait error: {err:?}"),
        Ok(other) => panic!("unexpected checkpoint wait response: {other:?}"),
        Err(err) => panic!("checkpoint wait ipc error: {err:?}"),
    }
}

fn wait_for_wal_segments(
    rig: &ReplRig,
    idx: usize,
    namespace: &NamespaceId,
    min_segments: usize,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let status = rig.node(idx).admin_status();
        let (segments, total_bytes) = status
            .wal
            .iter()
            .find(|row| &row.namespace == namespace)
            .map(|row| (row.segment_count, row.total_bytes))
            .unwrap_or((0, 0));
        if segments >= min_segments {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "wal segments did not reach {min_segments} within {timeout:?}: segments={segments} total_bytes={total_bytes}"
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn trace_path(trace_dir: &Path, from: usize, to: usize) -> std::path::PathBuf {
    trace_dir.join(format!("trace-{from}-{to}.jsonl"))
}

fn run_trace_harness(mode: TailnetTraceMode, trace_dir: &Path) {
    let mut options = ReplRigOptions::default();
    options.seed = 101;
    options.fault_profile = Some(FaultProfile::none());
    options.keepalive_ms = Some(60_000);
    options.tailnet_trace = Some(TailnetTraceConfig {
        mode,
        dir: trace_dir.to_path_buf(),
        timeout_ms: Some(30_000),
    });

    let rig = ReplRig::new(2, options);
    rig.assert_replication_ready(Duration::from_secs(60));

    rig.create_issue(0, "trace-0");
    rig.create_issue(1, "trace-1");
    for idx in 0..2 {
        rig.reload_replication(idx);
    }
    std::thread::sleep(Duration::from_millis(500));

    if matches!(mode, TailnetTraceMode::Record) {
        assert!(
            trace_path(trace_dir, 0, 1).exists(),
            "missing trace for 0->1"
        );
        assert!(
            trace_path(trace_dir, 1, 0).exists(),
            "missing trace for 1->0"
        );
    }
}

#[test]
fn repl_daemon_to_daemon_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.fault_profile = None;
    options.seed = 7;

    let rig = ReplRig::new(2, options);

    let ids = [rig.create_issue(0, "from-0"), rig.create_issue(1, "from-1")];

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(30));
    wait_for_sample(&rig, &ids, Duration::from_secs(10));
}

#[test]
fn repl_daemon_to_daemon_tailnet_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.fault_profile = Some(FaultProfile::tailnet());
    options.seed = 19;

    let rig = ReplRig::new(2, options);
    rig.assert_replication_ready(Duration::from_secs(60));

    let ids = [
        rig.create_issue(0, "tailnet-0"),
        rig.create_issue(1, "tailnet-1"),
    ];
    for idx in 0..2 {
        rig.reload_replication(idx);
    }

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(90));
    wait_for_sample(&rig, &ids, Duration::from_secs(30));
}

#[test]
fn repl_daemon_pathological_tailnet_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.seed = 41;

    let mut profile = FaultProfile::pathological();
    profile.base_latency_ms = Some(10);
    profile.jitter_ms = Some(25);
    profile.loss_rate = Some(0.12);
    profile.duplicate_rate = Some(0.01);
    profile.reorder_rate = Some(0.05);
    profile.blackhole_after_frames = Some(5);
    profile.blackhole_for_ms = Some(200);
    profile.reset_after_frames = Some(20);
    profile.one_way_loss = Some("a->b".to_string());

    let mut by_link = vec![vec![None; 2]; 2];
    for from in 0..2 {
        for to in 0..2 {
            if from == to {
                continue;
            }
            by_link[from][to] = Some(profile.clone());
        }
    }
    options.fault_profile_by_link = Some(by_link);

    let rig = ReplRig::new(2, options);
    rig.assert_replication_ready(Duration::from_secs(30));

    let ids = [
        rig.create_issue(0, "pathology-0"),
        rig.create_issue(1, "pathology-1"),
    ];

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(90));
    wait_for_sample(&rig, &ids, Duration::from_secs(20));
}

#[test]
fn repl_daemon_store_discovery_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.use_store_id_override = false;
    options.seed = 23;

    let rig = ReplRig::new(2, options);

    let ids = [
        rig.create_issue(0, "discover-0"),
        rig.create_issue(1, "discover-1"),
    ];

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(60));
    wait_for_sample(&rig, &ids, Duration::from_secs(15));

    let expected = rig.store_id();
    for node in rig.nodes() {
        let status = node.admin_status();
        assert_eq!(
            status.store_id, expected,
            "store discovery mismatch: expected {expected} got {}",
            status.store_id
        );
    }
}

#[test]
fn repl_checkpoint_bootstrap_under_churn() {
    let mut options = ReplRigOptions::default();
    options.seed = 73;
    options.wal_segment_max_bytes = Some(64 * 1024);
    // Note: checkpoints are now always enabled by default

    let rig = ReplRig::new(2, options);

    rig.shutdown_node(1);

    let warm_ids = [rig.create_issue(0, "bootstrap-pre-0")];

    churn_node(rig.node(0), 400, 2);

    wait_for_wal_segments(&rig, 0, &NamespaceId::core(), 2, Duration::from_secs(20));
    wait_for_checkpoint(&rig, 0, &NamespaceId::core(), Duration::from_secs(60));

    let tail_ids = [rig.create_issue(0, "bootstrap-tail-0")];

    rig.restart_node(1);

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(120));
    let combined: Vec<String> = warm_ids.iter().chain(tail_ids.iter()).cloned().collect();
    wait_for_sample(&rig, &combined, Duration::from_secs(20));
}

#[test]
fn repl_tailnet_proxy_smoke() {
    let tmp_root = std::env::current_dir().expect("cwd").join("tmp");
    std::fs::create_dir_all(&tmp_root).expect("trace tmp root");
    let trace_root = TempDir::new_in(&tmp_root).expect("trace tmp dir");
    let trace_dir = trace_root.path().to_path_buf();

    run_trace_harness(TailnetTraceMode::Record, &trace_dir);
}

#[test]
fn repl_daemon_crash_restart_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.seed = 29;

    let rig = ReplRig::new(2, options);

    let initial = [
        rig.create_issue(0, "crash-pre-0"),
        rig.create_issue(1, "crash-pre-1"),
    ];

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(30));
    wait_for_sample(&rig, &initial, Duration::from_secs(10));

    rig.crash_node(1);

    let post = [rig.create_issue(0, "crash-post-0")];

    wait_for_sample_on(&rig, &post, &[0], Duration::from_secs(10));

    rig.restart_node(1);
    rig.wait_for_admin_ready(1, Duration::from_secs(20));

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(60));
    let combined: Vec<String> = initial.iter().chain(post.iter()).cloned().collect();
    wait_for_sample(&rig, &combined, Duration::from_secs(20));
}

#[test]
fn repl_daemon_crash_restart_tailnet_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.seed = 51;
    options.fault_profile = Some(FaultProfile::tailnet());

    let rig = ReplRig::new(2, options);
    rig.assert_replication_ready(Duration::from_secs(60));

    let initial = [
        rig.create_issue(0, "tailnet-crash-pre-0"),
        rig.create_issue(1, "tailnet-crash-pre-1"),
    ];
    for idx in 0..2 {
        rig.reload_replication(idx);
    }

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(60));
    wait_for_sample(&rig, &initial, Duration::from_secs(20));

    rig.crash_node(1);

    let post = [rig.create_issue(0, "tailnet-crash-post-0")];

    wait_for_sample_on(&rig, &post, &[0], Duration::from_secs(20));

    rig.restart_node(1);
    rig.reload_replication(0);
    rig.reload_replication(1);

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(120));
    let combined: Vec<String> = initial.iter().chain(post.iter()).cloned().collect();
    wait_for_sample(&rig, &combined, Duration::from_secs(20));
}

#[test]
fn repl_daemon_roster_reload_and_epoch_bump_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.seed = 37;

    let rig = ReplRig::new(2, options);

    let initial = [
        rig.create_issue(0, "roster-pre-0"),
        rig.create_issue(1, "roster-pre-1"),
    ];

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(60));
    wait_for_sample(&rig, &initial, Duration::from_secs(10));

    let mut entries = roster_entries(&rig);
    entries[1].role = ReplicaDurabilityRole::peer(false);
    rig.overwrite_roster(entries);
    for idx in 0..2 {
        rig.reload_replication(idx);
    }

    let replica = rig.node(1).replica_id();
    rig.wait_for_durability_eligible(
        0,
        replica,
        DurabilityEligibility::Ineligible,
        Duration::from_secs(30),
    );

    let post_roster = [
        rig.create_issue(0, "roster-post-0"),
        rig.create_issue(1, "roster-post-1"),
    ];

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(60));
    wait_for_sample(&rig, &post_roster, Duration::from_secs(10));

    for idx in 0..2 {
        rig.shutdown_node(idx);
    }

    let bumped = rig.bump_store_epoch();
    assert!(bumped.get() > 0, "store_epoch should advance");

    for idx in 0..2 {
        rig.restart_node(idx);
    }

    let post_epoch = [
        rig.create_issue(0, "epoch-post-0"),
        rig.create_issue(1, "epoch-post-1"),
    ];

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(120));
    let combined: Vec<String> = initial
        .iter()
        .chain(post_roster.iter())
        .chain(post_epoch.iter())
        .cloned()
        .collect();
    wait_for_sample(&rig, &combined, Duration::from_secs(20));
}

#[test]
fn repl_daemon_stress_wal_rotation_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.seed = 43;
    options.wal_segment_max_bytes = Some(8 * 1024);
    let segment_max_bytes = options
        .wal_segment_max_bytes
        .expect("wal_segment_max_bytes set") as u64;

    let rig = ReplRig::new(2, options);

    let mut ids = Vec::new();
    let payload = "x".repeat(64);

    let title = format!("stress-0-0-{payload}");
    ids.push(rig.create_issue(0, &title));
    let first_bytes = wal_total_bytes(&rig, 0);

    let title = format!("stress-0-1-{payload}");
    ids.push(rig.create_issue(0, &title));
    let second_bytes = wal_total_bytes(&rig, 0);

    let record_bytes = std::cmp::max(1, second_bytes.saturating_sub(first_bytes));
    let remaining_bytes = segment_max_bytes.saturating_sub(second_bytes);
    let extra_events = (remaining_bytes / record_bytes) + 1;
    let events_per_node = 2 + extra_events as usize;

    for node_idx in 0..2 {
        let start_seq = if node_idx == 0 { 2 } else { 0 };
        for seq in start_seq..events_per_node {
            let title = format!("stress-{node_idx}-{seq}-{payload}");
            ids.push(rig.create_issue(node_idx, &title));
        }
    }

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(90));
    wait_for_sample(&rig, &ids, Duration::from_secs(20));

    for node_idx in 0..2 {
        let status = rig.admin_status(node_idx);
        let wal = status
            .wal
            .iter()
            .find(|entry| entry.namespace == NamespaceId::core())
            .expect("core namespace wal");
        assert!(
            wal.segment_count > 1,
            "expected WAL rotation on node {node_idx}, saw {} segments",
            wal.segment_count
        );
    }

    let tail_id = rig.create_issue(1, "stress-tail");
    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(40));
    wait_for_sample(&rig, &[tail_id], Duration::from_secs(20));
}

#[test]
fn repl_daemon_replicated_fsync_receipt() {
    let mut options = ReplRigOptions::default();
    options.seed = 31;

    let rig = ReplRig::new(3, options);
    rig.assert_peers_seen(Duration::from_secs(30));
    rig.assert_replication_ready(Duration::from_secs(60));

    let (issue_id, receipt) =
        create_issue_with_durability(&rig, 0, "durability-ok", NonZeroU32::new(2).unwrap());

    let requested = receipt::requested_durability(&receipt);
    assert_eq!(
        requested,
        DurabilityClass::ReplicatedFsync {
            k: NonZeroU32::new(2).unwrap(),
        }
    );
    assert!(receipt.outcome().is_achieved());
    let replicated = receipt
        .durability_proof()
        .replicated
        .clone()
        .expect("replicated proof");
    assert_eq!(replicated.k, NonZeroU32::new(2).unwrap());
    let mut acked_by = replicated.acked_by.clone();
    acked_by.sort();
    let mut expected = vec![rig.node(1).replica_id(), rig.node(2).replica_id()];
    expected.sort();
    assert_eq!(acked_by, expected, "acked_by mismatch");

    let response = show_issue_with_read(
        &rig,
        1,
        &issue_id,
        ReadConsistency {
            require_min_seen: Some(receipt.min_seen().clone()),
            wait_timeout_ms: Some(30_000),
            ..Default::default()
        },
    );
    match response {
        Response::Ok {
            ok: ResponsePayload::Query(QueryResult::Issue(issue)),
        } => assert_eq!(issue.id, issue_id.as_str()),
        other => panic!("unexpected show response: {other:?}"),
    }

    let mut impossible = receipt.min_seen().clone();
    let event_id = receipt.event_ids().first().expect("event id");
    let next_seq = event_id.origin_seq.get() + 1;
    impossible
        .observe_at_least(
            &event_id.namespace,
            &event_id.origin_replica_id,
            Seq0::new(next_seq),
            HeadStatus::Known([0u8; 32]),
        )
        .expect("advance min_seen");

    let response = show_issue_with_read(
        &rig,
        1,
        &issue_id,
        ReadConsistency {
            require_min_seen: Some(impossible),
            wait_timeout_ms: Some(0),
            ..Default::default()
        },
    );
    match response {
        Response::Err { err } => {
            assert_eq!(
                err.code,
                ProtocolErrorCode::RequireMinSeenUnsatisfied.into()
            );
            let details = err
                .details_as::<RequireMinSeenUnsatisfiedDetails>()
                .expect("require_min_seen details");
            let details = details.expect("require_min_seen details missing");
            let required = details
                .required
                .get(&event_id.namespace, &event_id.origin_replica_id)
                .expect("required watermark");
            assert_eq!(required.seq().get(), next_seq);
        }
        other => panic!("unexpected require_min_seen response: {other:?}"),
    }
}

fn roster_entries(rig: &ReplRig) -> Vec<ReplicaEntry> {
    rig.nodes()
        .iter()
        .enumerate()
        .map(|(idx, node)| ReplicaEntry {
            replica_id: node.replica_id(),
            name: format!("node-{idx}"),
            role: ReplicaDurabilityRole::peer(true),
            allowed_namespaces: Some(vec![NamespaceId::core()]),
            expire_after_ms: None,
        })
        .collect()
}

#[test]
fn repl_daemon_replicated_fsync_timeout_receipt() {
    let mut options = ReplRigOptions::default();
    options.seed = 37;
    options.dead_ms = Some(1_500);

    let rig = ReplRig::new(3, options);
    rig.crash_node(2);

    let response = create_issue_with_durability_result(
        &rig,
        0,
        "durability-timeout",
        NonZeroU32::new(2).unwrap(),
    );

    match response {
        Response::Err { err } => {
            assert_eq!(err.code, ProtocolErrorCode::DurabilityTimeout.into());
            let details = err
                .details_as::<DurabilityTimeoutDetails>()
                .expect("durability timeout details");
            let details = details.expect("durability timeout details missing");
            assert_eq!(
                details.requested,
                DurabilityClass::ReplicatedFsync {
                    k: NonZeroU32::new(2).unwrap(),
                }
            );
            let pending = details.pending_replica_ids.expect("pending replica ids");
            assert!(
                pending.contains(&rig.node(2).replica_id()),
                "pending replicas did not include crashed peer"
            );

            let receipt = err
                .receipt_as::<beads_rs::DurabilityReceipt>()
                .expect("receipt decode");
            let receipt = receipt.expect("receipt missing");
            assert!(receipt.outcome().is_pending());
        }
        other => panic!("unexpected durability timeout response: {other:?}"),
    }
}

fn create_issue_with_durability(
    rig: &ReplRig,
    node_idx: usize,
    title: &str,
    k: NonZeroU32,
) -> (BeadId, beads_rs::DurabilityReceipt) {
    let mut attempts = 0u8;
    loop {
        let response = create_issue_with_durability_result(rig, node_idx, title, k);
        match response {
            Response::Ok {
                ok: ResponsePayload::Op(op),
            } => {
                let issue_id = match op.result {
                    OpResult::Created { id } => id,
                    other => panic!("unexpected op result: {other:?}"),
                };
                return (issue_id, op.receipt);
            }
            Response::Err { err }
                if err.code == ProtocolErrorCode::DurabilityTimeout.into() && attempts < 5 =>
            {
                attempts += 1;
                std::thread::sleep(Duration::from_millis(500));
            }
            other => panic!("unexpected create response: {other:?}"),
        }
    }
}

fn create_issue_with_durability_result(
    rig: &ReplRig,
    node_idx: usize,
    title: &str,
    k: NonZeroU32,
) -> Response {
    let node = rig.node(node_idx);
    let client = IpcClient::for_runtime_dir(node.runtime_dir()).with_autostart(false);
    let request = Request::Create {
        ctx: MutationCtx::new(
            node.repo_dir().to_path_buf(),
            MutationMeta {
                durability: Some(DurabilityClass::ReplicatedFsync { k }),
                ..Default::default()
            },
        ),
        payload: CreatePayload {
            id: None,
            parent: None,
            title: title.to_string(),
            bead_type: BeadType::Task,
            priority: Priority::MEDIUM,
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
    client.send_request(&request).expect("create response")
}

fn show_issue_with_read(
    rig: &ReplRig,
    node_idx: usize,
    issue_id: &BeadId,
    read: ReadConsistency,
) -> Response {
    let node = rig.node(node_idx);
    let client = IpcClient::for_runtime_dir(node.runtime_dir()).with_autostart(false);
    let request = Request::Show {
        ctx: ReadCtx::new(node.repo_dir().to_path_buf(), read),
        payload: IdPayload {
            id: issue_id.clone(),
        },
    };
    client.send_request(&request).expect("show response")
}
