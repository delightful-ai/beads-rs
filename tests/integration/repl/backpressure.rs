//! Replication backpressure behavior.


use uuid::Uuid;

use beads_rs::daemon::admission::AdmissionController;
use beads_rs::daemon::repl::{
    Events, ReplMessage, Session, SessionAction, SessionConfig, SessionPhase, SessionRole,
};
use beads_rs::{ErrorCode, Limits, NamespaceId, ReplicaId, StoreIdentity};

use crate::fixtures::repl_frames;
use crate::fixtures::repl_peer::MockStore;

fn inbound_session_with_limits(limits: Limits) -> (Session, MockStore, StoreIdentity) {
    let identity = repl_frames::store_identity(2);
    let local_replica = ReplicaId::new(Uuid::from_bytes([10u8; 16]));
    let mut config = SessionConfig::new(identity, local_replica, &limits);
    config.requested_namespaces = vec![NamespaceId::core()];
    config.offered_namespaces = vec![NamespaceId::core()];
    let admission = AdmissionController::new(&limits);
    let mut session = Session::new(SessionRole::Inbound, config, limits, admission);
    let mut store = MockStore::default();

    let peer_replica = ReplicaId::new(Uuid::from_bytes([11u8; 16]));
    let hello = repl_frames::hello(identity, peer_replica);
    session.handle_message(ReplMessage::Hello(hello), &mut store, 0);
    assert!(matches!(session.phase(), SessionPhase::Streaming));

    (session, store, identity)
}

#[test]
fn repl_backpressure_overload_emits_error() {
    let mut limits = Limits::default();
    limits.max_repl_ingest_queue_bytes = 1;
    limits.max_repl_ingest_queue_events = 1;

    let (mut session, mut store, identity) = inbound_session_with_limits(limits);
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([12u8; 16]));

    let event = repl_frames::event_frame(identity, namespace, origin, 1, None);
    let actions = session.handle_message(
        ReplMessage::Events(Events {
            events: vec![event],
        }),
        &mut store,
        10,
    );

    let error = actions
        .iter()
        .find_map(|action| match action {
            SessionAction::Send(ReplMessage::Error(payload)) => Some(payload),
            _ => None,
        })
        .expect("error");

    assert_eq!(error.code, ProtocolErrorCode::Overloaded.into());
    assert!(matches!(session.phase(), SessionPhase::Draining));
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, SessionAction::Close { .. }))
    );
}
