#![cfg(feature = "e2e-tests")]

use std::num::NonZeroU32;

use beads_rs::api::QueryResult;
use beads_rs::core::error::details::{DurabilityTimeoutDetails, RequireMinSeenUnsatisfiedDetails};
use beads_rs::core::{
    BeadId, BeadType, DurabilityClass, HeadStatus, NamespaceId, Priority, ProtocolErrorCode, Seq0,
    StoreId,
};
use beads_rs::daemon::ipc::{
    CreatePayload, IdPayload, MutationCtx, MutationMeta, ReadConsistency, ReadCtx, Request,
    Response, ResponsePayload,
};
use beads_rs::daemon::ops::OpResult;
use beads_rs::test_harness::{
    Direction, NetworkProfile, NodeOptions, ReplicationRig, TestWorld, latency_budget_ms,
    measure_latency, replication_latency_budget_ms,
};
use uuid::Uuid;

const DURABILITY_STEPS: usize = 2_000;

#[test]
fn e2e_local_mutation_latency_under_budget() {
    let world = TestWorld::new(1_700_000_000_000);
    let store_id = StoreId::new(Uuid::new_v4());
    let node = world.node("solo", store_id, NodeOptions::default());

    let latency = measure_latency(|| {
        let id = node.create_issue("latency check");
        let bead_id = BeadId::parse(&id).expect("bead id");
        assert!(node.has_bead(&bead_id));
    });

    assert!(
        latency <= latency_budget_ms(),
        "local mutation latency {latency}ms exceeded budget"
    );
}

#[test]
fn e2e_replication_converges_with_network_faults() {
    let mut rig = ReplicationRig::new(2, 1_700_000_000_100);
    let node_a = rig.node(0);
    let node_b = rig.node(1);
    let start_ms = rig.clock().now_ms();

    rig.set_network_profile_all(NetworkProfile::tailnet(), 42);
    rig.pump(10);
    rig.assert_replication_ready(200);

    if let Some(net) = rig.link_network(0, 1) {
        net.delay_next(Direction::AtoB, 10);
        net.reorder_next(Direction::AtoB);
        net.duplicate_next(Direction::AtoB);
    }

    let id = node_a.create_issue("replicate me");
    let bead_id = BeadId::parse(&id).expect("bead id");
    rig.advance_ms(10);

    rig.pump_until(200, |rig| rig.node(1).has_bead(&bead_id));
    assert!(node_b.has_bead(&bead_id));
    let elapsed_ms = rig.clock().now_ms().saturating_sub(start_ms);
    assert!(
        elapsed_ms <= replication_latency_budget_ms(),
        "replication latency {elapsed_ms}ms exceeded budget"
    );

    let origin = node_a.replica_id();
    rig.pump_until(200, |rig| {
        rig.node(1).applied_seq(&NamespaceId::core(), origin) >= 1
    });
}

#[test]
fn e2e_replication_roundtrip() {
    let mut rig = ReplicationRig::new(2, 1_700_000_000_200);
    let node_a = rig.node(0);
    let node_b = rig.node(1);

    let id_a = node_a.create_issue("from-a");
    let id_b = node_b.create_issue("from-b");
    let bead_a = BeadId::parse(&id_a).expect("bead id");
    let bead_b = BeadId::parse(&id_b).expect("bead id");

    rig.pump_until(300, |rig| {
        rig.node(0).has_bead(&bead_b) && rig.node(1).has_bead(&bead_a)
    });

    assert!(node_a.has_bead(&bead_b));
    assert!(node_b.has_bead(&bead_a));
}

#[test]
fn e2e_replication_converges_under_tailnet_profile() {
    let mut rig = ReplicationRig::new(3, 1_700_000_000_300);
    rig.set_network_profile_all(NetworkProfile::tailnet(), 7);
    rig.pump(10);
    rig.assert_replication_ready(300);

    let ids = [
        rig.node(0).create_issue("tailnet-0"),
        rig.node(1).create_issue("tailnet-1"),
        rig.node(2).create_issue("tailnet-2"),
    ];
    let beads: Vec<BeadId> = ids
        .into_iter()
        .map(|id| BeadId::parse(&id).expect("bead id"))
        .collect();

    rig.pump_until(500, |rig| {
        rig.nodes()
            .iter()
            .all(|node| beads.iter().all(|bead| node.has_bead(bead)))
    });
    rig.pump_until_converged(800, &[NamespaceId::core()]);
}

#[test]
fn e2e_in_memory_wal_replication_roundtrip() {
    let options = NodeOptions::in_memory();
    let mut rig = ReplicationRig::new_with_options(2, 1_700_000_000_400, options);
    let node_a = rig.node(0);
    let node_b = rig.node(1);

    let id = node_a.create_issue("in-memory");
    let bead = BeadId::parse(&id).expect("bead id");

    rig.pump_until(300, |rig| rig.node(1).has_bead(&bead));
    assert!(node_b.has_bead(&bead));
    rig.pump_until_converged(400, &[NamespaceId::core()]);
}

#[test]
fn e2e_replication_restart_roundtrip() {
    let mut rig = ReplicationRig::new(2, 1_700_000_000_500);

    let ids = [
        rig.node(0).create_issue("restart-pre-0"),
        rig.node(1).create_issue("restart-pre-1"),
    ];
    let mut beads: Vec<BeadId> = ids
        .iter()
        .map(|id| BeadId::parse(id).expect("bead id"))
        .collect();

    rig.pump_until(300, |rig| {
        rig.nodes()
            .iter()
            .all(|node| beads.iter().all(|bead| node.has_bead(bead)))
    });
    rig.pump_until_converged(400, &[NamespaceId::core()]);

    rig.restart_node(1);

    let post_id = rig.node(0).create_issue("restart-post-0");
    beads.push(BeadId::parse(&post_id).expect("bead id"));

    rig.pump_until(600, |rig| {
        rig.nodes()
            .iter()
            .all(|node| beads.iter().all(|bead| node.has_bead(bead)))
    });
    rig.pump_until_converged(700, &[NamespaceId::core()]);
}

#[test]
fn e2e_replicated_fsync_receipt() {
    let mut rig = ReplicationRig::new(3, 1_700_000_000_600);
    rig.write_replica_roster();

    let (issue_id, receipt) =
        create_issue_with_durability(&mut rig, 0, "durability-ok", NonZeroU32::new(2).expect("k"));

    let requested = receipt.outcome().requested();
    assert_eq!(
        requested,
        DurabilityClass::ReplicatedFsync {
            k: NonZeroU32::new(2).expect("k"),
        }
    );
    assert!(receipt.outcome().is_achieved());
    let replicated = receipt
        .durability_proof()
        .replicated
        .expect("replicated proof");
    assert_eq!(replicated.k, NonZeroU32::new(2).expect("k"));
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

#[test]
fn e2e_replicated_fsync_timeout_receipt() {
    let mut options = NodeOptions::default();
    options.limits.dead_ms = 50;

    let mut rig = ReplicationRig::new_with_options(3, 1_700_000_000_700, options);
    rig.write_replica_roster();

    let blackhole = NetworkProfile {
        loss_rate: 1.0,
        ..NetworkProfile::default()
    };
    for idx in 0..3 {
        if idx == 2 {
            continue;
        }
        if let Some(net) = rig.link_network(idx, 2) {
            net.set_profile(blackhole);
        }
        if let Some(net) = rig.link_network(2, idx) {
            net.set_profile(blackhole);
        }
    }

    let response = create_issue_with_durability_result(
        &mut rig,
        0,
        "durability-timeout",
        NonZeroU32::new(2).expect("k"),
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
                    k: NonZeroU32::new(2).expect("k"),
                }
            );
            let pending = details.pending_replica_ids.expect("pending replica ids");
            assert!(
                pending.contains(&rig.node(2).replica_id()),
                "pending replicas did not include blackholed peer"
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
    rig: &mut ReplicationRig,
    node_idx: usize,
    title: &str,
    k: NonZeroU32,
) -> (BeadId, beads_rs::DurabilityReceipt) {
    let response = create_issue_with_durability_result(rig, node_idx, title, k);
    match response {
        Response::Ok {
            ok: ResponsePayload::Op(op),
        } => {
            let issue_id = match op.result {
                OpResult::Created { id } => id,
                other => panic!("unexpected op result: {other:?}"),
            };
            (issue_id, op.receipt)
        }
        other => panic!("unexpected create response: {other:?}"),
    }
}

fn create_issue_with_durability_result(
    rig: &mut ReplicationRig,
    node_idx: usize,
    title: &str,
    k: NonZeroU32,
) -> Response {
    let node = rig.node(node_idx);
    let request = Request::Create {
        ctx: MutationCtx::new(
            node.repo_path(),
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
    rig.apply_request_with_wait(node_idx, request, DURABILITY_STEPS)
}

fn show_issue_with_read(
    rig: &ReplicationRig,
    node_idx: usize,
    issue_id: &BeadId,
    read: ReadConsistency,
) -> Response {
    let node = rig.node(node_idx);
    let request = Request::Show {
        ctx: ReadCtx::new(node.repo_path(), read),
        payload: IdPayload {
            id: issue_id.clone(),
        },
    };
    node.apply_request(request)
}
