#![cfg(feature = "slow-tests")]

use std::time::Duration;

use beads_rs::core::NamespaceId;

use crate::fixtures::repl_rig::{FaultProfile, ReplRig, ReplRigOptions};

#[test]
fn repl_daemon_to_daemon_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.fault_profile = Some(FaultProfile::tailnet());
    options.seed = 7;

    let rig = ReplRig::new(3, options);

    let ids = [
        rig.create_issue(0, "from-0"),
        rig.create_issue(1, "from-1"),
        rig.create_issue(2, "from-2"),
    ];

    for issue_id in &ids {
        rig.wait_for_show(0, issue_id, Duration::from_secs(6));
        rig.wait_for_show(1, issue_id, Duration::from_secs(6));
        rig.wait_for_show(2, issue_id, Duration::from_secs(6));
    }

    rig.assert_converged(&[NamespaceId::core()], Duration::from_secs(10));
}
