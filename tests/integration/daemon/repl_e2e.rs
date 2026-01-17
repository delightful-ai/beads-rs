#![cfg(feature = "slow-tests")]

use std::time::Duration;

use crate::fixtures::repl_rig::{FaultProfile, ReplRig, ReplRigOptions};

#[test]
fn repl_daemon_to_daemon_roundtrip() {
    let mut options = ReplRigOptions::default();
    options.fault_profile = Some(FaultProfile::none());
    options.seed = 7;

    let rig = ReplRig::new(3, options);

    let id0 = rig.create_issue(0, "from-0");
    rig.wait_for_show(1, &id0, Duration::from_secs(15));

    let id1 = rig.create_issue(1, "from-1");
    rig.wait_for_show(2, &id1, Duration::from_secs(15));

    let id2 = rig.create_issue(2, "from-2");
    rig.wait_for_show(0, &id2, Duration::from_secs(15));

    rig.assert_peers_seen(Duration::from_secs(30));
}
