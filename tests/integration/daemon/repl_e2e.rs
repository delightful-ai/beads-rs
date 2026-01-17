#![cfg(feature = "slow-tests")]

use std::time::Duration;

use crate::fixtures::repl_rig::{FaultProfile, ReplRig, ReplRigOptions};
use beads_rs::core::NamespaceId;

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
