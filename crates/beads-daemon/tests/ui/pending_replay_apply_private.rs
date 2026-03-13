use beads_daemon::testkit::core::PendingReplayApply;

fn bypass_ack(pending: PendingReplayApply) {
    let _ = pending.state;
}

fn main() {}
