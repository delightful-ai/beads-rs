//! Deterministic replication-rig end-to-end coverage.

use beads_core::{BeadId, NamespaceId};
use beads_daemon::testkit::e2e::{
    Direction, LinkFaultProfile, ReplicationReadySnapshot, ReplicationRig,
};

const START_MS: u64 = 1_700_000_000_000;
const READY_STEPS: usize = 50_000;
const CONVERGE_STEPS: usize = 400_000;

fn parse_bead_id(raw: &str) -> BeadId {
    BeadId::parse(raw).expect("bead id")
}

fn wait_for_all_beads(
    rig: &mut ReplicationRig,
    bead_ids: &[BeadId],
    nodes: &[usize],
    max_steps: usize,
) {
    rig.pump_until(max_steps, |rig| {
        nodes
            .iter()
            .all(|idx| bead_ids.iter().all(|id| rig.node(*idx).has_bead(id)))
    });
}

fn assert_core_converged(rig: &mut ReplicationRig, max_steps: usize) {
    rig.pump_until_converged(max_steps, &[NamespaceId::core()]);
    rig.assert_converged(&[NamespaceId::core()]);
}

#[test]
fn replication_rig_pathological_tailnet_recovers_without_external_proxies() {
    let mut rig = ReplicationRig::new(2, START_MS);
    rig.write_replica_roster();
    let mut profile = LinkFaultProfile::pathological();
    profile.one_way_loss = Some(Direction::AtoB);
    profile.blackhole_after_frames = Some(5);
    profile.blackhole_for_ms = Some(200);
    profile.reset_after_frames = Some(20);
    rig.set_link_fault_profile_all(profile, 41);

    rig.assert_replication_ready(READY_STEPS);

    let bead_ids = [
        parse_bead_id(&rig.node(0).create_issue("pathology-0")),
        parse_bead_id(&rig.node(1).create_issue("pathology-1")),
    ];

    rig.assert_replication_ready(CONVERGE_STEPS);
    assert_core_converged(&mut rig, CONVERGE_STEPS);
    wait_for_all_beads(&mut rig, &bead_ids, &[0, 1], READY_STEPS);
}

#[test]
fn replication_rig_tailnet_restart_requires_fresh_handshakes() {
    let mut rig = ReplicationRig::new(2, START_MS);
    rig.write_replica_roster();
    rig.set_link_fault_profile_all(LinkFaultProfile::tailnet(), 51);

    rig.assert_replication_ready(READY_STEPS);

    let initial = [
        parse_bead_id(&rig.node(0).create_issue("tailnet-crash-pre-0")),
        parse_bead_id(&rig.node(1).create_issue("tailnet-crash-pre-1")),
    ];

    assert_core_converged(&mut rig, READY_STEPS);
    wait_for_all_beads(&mut rig, &initial, &[0, 1], READY_STEPS);

    let snapshot: ReplicationReadySnapshot = rig.replication_ready_snapshot();
    rig.crash_node(1);

    let post = parse_bead_id(&rig.node(0).create_issue("tailnet-crash-post-0"));
    wait_for_all_beads(&mut rig, std::slice::from_ref(&post), &[0], READY_STEPS);
    assert!(
        !rig.node(1).has_bead(&post),
        "crashed replica should not observe post-crash writes before restart"
    );

    rig.restart_node(1);
    rig.assert_replication_ready_since(&snapshot, READY_STEPS);

    let all = vec![initial[0].clone(), initial[1].clone(), post];
    assert_core_converged(&mut rig, CONVERGE_STEPS);
    wait_for_all_beads(&mut rig, &all, &[0, 1], READY_STEPS);
}
