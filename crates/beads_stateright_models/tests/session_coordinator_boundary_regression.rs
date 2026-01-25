#[allow(dead_code)]
#[path = "../examples/session_coordinator_boundary.rs"]
mod session_coordinator_boundary;

use session_coordinator_boundary::{Action, SessionCoordinator};
use stateright::Model;

#[test]
fn pending_ingest_is_not_stale_after_ack_jump() {
    let model = SessionCoordinator;
    let mut state = model.init_states().pop().expect("init state");

    let actions = [
        Action::DeliverEvent(3),
        Action::DeliverEvent(2),
        Action::DeliverEvent(1),
        Action::CoordinatorIngest,
        Action::DeliverEvent(1),
        Action::DeliverEvent(2),
        Action::DeliverAck,
    ];

    for action in actions {
        state = model.next_state(&state, action).expect("state transition");
    }

    if let Some(batch) = &state.pending_ingest {
        let expected = state.session_durable + 1;
        assert_eq!(
            batch.first().copied(),
            Some(expected),
            "pending ingest should start at the next expected seq"
        );
    }
}
