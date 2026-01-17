#![cfg(feature = "e2e-tests")]

use beads_rs::core::{BeadId, NamespaceId, StoreId};
use beads_rs::test_harness::{
    Direction, NetworkProfile, NodeOptions, ReplicationRig, TestWorld, latency_budget_ms,
    measure_latency, replication_latency_budget_ms,
};
use uuid::Uuid;

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
fn e2e_replication_bidirectional() {
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
