#![cfg(feature = "slow-tests")]

use std::num::NonZeroU32;
use std::time::Duration;

use crate::fixtures::receipt;
use crate::fixtures::repl_rig::{FaultProfile, ReplRig, ReplRigOptions};
use beads_rs::api::QueryResult;
use beads_rs::core::error::details::{DurabilityTimeoutDetails, RequireMinSeenUnsatisfiedDetails};
use beads_rs::core::{
    BeadType, DurabilityClass, DurabilityOutcome, HeadStatus, NamespaceId, Priority,
    ProtocolErrorCode, ReplicaEntry, ReplicaRole, Seq0,
};
use beads_rs::daemon::ipc::{
    IpcClient, MutationMeta, ReadConsistency, Request, Response, ResponsePayload,
};
use beads_rs::daemon::ops::OpResult;

#[test]
fn repl_daemon_to_daemon_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.fault_profile = None;
    options.seed = 7;

    let rig = ReplRig::new(3, options);

    let ids = [
        rig.create_issue(0, "from-0"),
        rig.create_issue(1, "from-1"),
        rig.create_issue(2, "from-2"),
    ];

    for node_idx in 0..3 {
        for id in &ids {
            rig.wait_for_show(node_idx, id, Duration::from_secs(30));
        }
    }

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(60));
}

#[test]
fn repl_daemon_to_daemon_tailnet_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.fault_profile = Some(FaultProfile::tailnet());
    options.seed = 19;

    let rig = ReplRig::new(3, options);

    let ids = [
        rig.create_issue(0, "tailnet-0"),
        rig.create_issue(1, "tailnet-1"),
        rig.create_issue(2, "tailnet-2"),
    ];

    for node_idx in 0..3 {
        for id in &ids {
            rig.wait_for_show(node_idx, id, Duration::from_secs(60));
        }
    }

    rig.assert_peers_seen(Duration::from_secs(60));
    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(120));
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

    let mut by_link = vec![vec![None; 3]; 3];
    for from in 0..3 {
        for to in 0..3 {
            if from == to {
                continue;
            }
            by_link[from][to] = Some(profile.clone());
        }
    }
    options.fault_profile_by_link = Some(by_link);

    let rig = ReplRig::new(3, options);

    let ids = [
        rig.create_issue(0, "pathology-0"),
        rig.create_issue(1, "pathology-1"),
        rig.create_issue(2, "pathology-2"),
    ];

    for node_idx in 0..3 {
        for id in &ids {
            rig.wait_for_show(node_idx, id, Duration::from_secs(90));
        }
    }

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(180));
}

#[test]
fn repl_daemon_store_discovery_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.use_store_id_override = false;
    options.seed = 23;

    let rig = ReplRig::new(3, options);

    let ids = [
        rig.create_issue(0, "discover-0"),
        rig.create_issue(1, "discover-1"),
        rig.create_issue(2, "discover-2"),
    ];

    for node_idx in 0..3 {
        for id in &ids {
            rig.wait_for_show(node_idx, id, Duration::from_secs(60));
        }
    }

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(120));

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
fn repl_daemon_crash_restart_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.seed = 29;

    let rig = ReplRig::new(3, options);

    let initial = [
        rig.create_issue(0, "crash-pre-0"),
        rig.create_issue(1, "crash-pre-1"),
        rig.create_issue(2, "crash-pre-2"),
    ];

    for node_idx in 0..3 {
        for id in &initial {
            rig.wait_for_show(node_idx, id, Duration::from_secs(30));
        }
    }

    rig.crash_node(2);

    let post = [
        rig.create_issue(0, "crash-post-0"),
        rig.create_issue(1, "crash-post-1"),
    ];

    for node_idx in 0..2 {
        for id in &post {
            rig.wait_for_show(node_idx, id, Duration::from_secs(30));
        }
    }

    rig.restart_node(2);

    for id in initial.iter().chain(post.iter()) {
        rig.wait_for_show(2, id, Duration::from_secs(60));
    }

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(120));
}

#[test]
fn repl_daemon_roster_reload_and_epoch_bump_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.seed = 37;

    let rig = ReplRig::new(3, options);

    let initial = [
        rig.create_issue(0, "roster-pre-0"),
        rig.create_issue(1, "roster-pre-1"),
        rig.create_issue(2, "roster-pre-2"),
    ];

    for node_idx in 0..3 {
        for id in &initial {
            rig.wait_for_show(node_idx, id, Duration::from_secs(30));
        }
    }

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(120));

    let mut entries = roster_entries(&rig);
    entries[2].durability_eligible = false;
    rig.overwrite_roster(entries);
    for idx in 0..3 {
        rig.reload_replication(idx);
    }

    let replica = rig.node(2).replica_id();
    rig.wait_for_durability_eligible(0, replica, false, Duration::from_secs(30));

    let post_roster = [
        rig.create_issue(0, "roster-post-0"),
        rig.create_issue(1, "roster-post-1"),
    ];

    for node_idx in 0..3 {
        for id in &post_roster {
            rig.wait_for_show(node_idx, id, Duration::from_secs(30));
        }
    }

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(120));

    for idx in 0..3 {
        rig.shutdown_node(idx);
    }

    let bumped = rig.bump_store_epoch();
    assert!(bumped.get() > 0, "store_epoch should advance");

    for idx in 0..3 {
        rig.restart_node(idx);
    }

    let post_epoch = [
        rig.create_issue(0, "epoch-post-0"),
        rig.create_issue(2, "epoch-post-2"),
    ];

    for node_idx in 0..3 {
        for id in initial
            .iter()
            .chain(post_roster.iter())
            .chain(post_epoch.iter())
        {
            rig.wait_for_show(node_idx, id, Duration::from_secs(60));
        }
    }

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(180));
}

#[test]
fn repl_daemon_stress_wal_rotation_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.seed = 43;
    options.wal_segment_max_bytes = Some(8 * 1024);

    let rig = ReplRig::new(3, options);

    let mut ids = Vec::new();
    for node_idx in 0..3 {
        for seq in 0..40 {
            let title = format!("stress-{node_idx}-{seq}-{}", "x".repeat(64));
            ids.push(rig.create_issue(node_idx, &title));
        }
    }

    for node_idx in 0..3 {
        for id in &ids {
            rig.wait_for_show(node_idx, id, Duration::from_secs(60));
        }
    }

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(180));

    for node_idx in 0..3 {
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
    for node_idx in 0..3 {
        rig.wait_for_show(node_idx, &tail_id, Duration::from_secs(60));
    }
}

#[test]
fn repl_daemon_replicated_fsync_receipt() {
    let mut options = ReplRigOptions::default();
    options.seed = 31;

    let rig = ReplRig::new(3, options);

    let (issue_id, receipt) =
        create_issue_with_durability(&rig, 0, "durability-ok", NonZeroU32::new(2).unwrap());

    let requested = receipt::requested_durability(&receipt);
    assert_eq!(
        requested,
        DurabilityClass::ReplicatedFsync {
            k: NonZeroU32::new(2).unwrap(),
        }
    );
    assert!(matches!(
        receipt.outcome,
        DurabilityOutcome::Achieved { .. }
    ));
    let replicated = receipt
        .durability_proof
        .replicated
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
            require_min_seen: Some(receipt.min_seen.clone()),
            wait_timeout_ms: Some(30_000),
            ..Default::default()
        },
    );
    match response {
        Response::Ok {
            ok: ResponsePayload::Query(QueryResult::Issue(issue)),
        } => assert_eq!(issue.id, issue_id),
        other => panic!("unexpected show response: {other:?}"),
    }

    let mut impossible = receipt.min_seen.clone();
    let event_id = receipt.event_ids.first().expect("event id");
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
            role: ReplicaRole::Peer,
            durability_eligible: true,
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
            assert!(matches!(receipt.outcome, DurabilityOutcome::Pending { .. }));
        }
        other => panic!("unexpected durability timeout response: {other:?}"),
    }
}

fn create_issue_with_durability(
    rig: &ReplRig,
    node_idx: usize,
    title: &str,
    k: NonZeroU32,
) -> (String, beads_rs::DurabilityReceipt) {
    let response = create_issue_with_durability_result(rig, node_idx, title, k);
    match response {
        Response::Ok {
            ok: ResponsePayload::Op(op),
        } => {
            let issue_id = match op.result {
                OpResult::Created { id } => id.to_string(),
                other => panic!("unexpected op result: {other:?}"),
            };
            (issue_id, op.receipt)
        }
        other => panic!("unexpected create response: {other:?}"),
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
        repo: node.repo_dir().to_path_buf(),
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
        meta: MutationMeta {
            durability: Some(format!("replicated_fsync({})", k)),
            ..Default::default()
        },
    };
    client.send_request(&request).expect("create response")
}

fn show_issue_with_read(
    rig: &ReplRig,
    node_idx: usize,
    issue_id: &str,
    read: ReadConsistency,
) -> Response {
    let node = rig.node(node_idx);
    let client = IpcClient::for_runtime_dir(node.runtime_dir()).with_autostart(false);
    let request = Request::Show {
        repo: node.repo_dir().to_path_buf(),
        id: issue_id.to_string(),
        read,
    };
    client.send_request(&request).expect("show response")
}
