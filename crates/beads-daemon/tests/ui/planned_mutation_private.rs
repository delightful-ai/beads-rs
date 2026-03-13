use beads_daemon::testkit::mutation_engine::PlannedMutation;

fn bypass_ack(planned: PlannedMutation) {
    let _ = planned.draft;
}

fn main() {}
