#![cfg(feature = "slow-tests")]

use std::time::Duration;

use crate::fixtures::repl_rig::{FaultProfile, ReplRig, ReplRigOptions};
use beads_rs::core::NamespaceId;

#[test]
fn repl_daemon_to_daemon_roundtrip() {
    let mut options = ReplRigOptions::default();
    let mut profile = FaultProfile::tailnet();
    profile.loss_rate = Some(0.0);
    profile.duplicate_rate = Some(0.0);
    profile.reorder_rate = Some(0.0);
    options.fault_profile = Some(profile);
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
