//! Replication backpressure behavior.

use uuid::Uuid;

use beads_rs::daemon::admission::AdmissionController;
use beads_rs::daemon::repl::session::{
    Inbound, InboundConnecting, SessionState, handle_inbound_message,
};
use beads_rs::daemon::repl::{Events, ReplMessage, SessionAction, SessionConfig};
use beads_rs::{Limits, NamespaceId, ProtocolErrorCode, ReplicaId, StoreIdentity};

use crate::fixtures::identity;
use crate::fixtures::repl_frames;
use crate::fixtures::repl_peer::MockStore;

fn inbound_session_with_limits(
    limits: Limits,
) -> (SessionState<Inbound>, MockStore, StoreIdentity) {
    let identity = identity::store_identity_with_epoch(2, 1);
    let local_replica = ReplicaId::new(Uuid::from_bytes([10u8; 16]));
    let mut config = SessionConfig::new(identity, local_replica, &limits);
    config.requested_namespaces = vec![NamespaceId::core()];
    config.offered_namespaces = vec![NamespaceId::core()];
    let admission = AdmissionController::new(&limits);
    let session = InboundConnecting::new(config, limits, admission);
    let mut store = MockStore::default();

    let peer_replica = ReplicaId::new(Uuid::from_bytes([11u8; 16]));
    let hello = repl_frames::hello(identity, peer_replica);
    let (session, _) = handle_inbound_message(
        SessionState::Connecting(session),
        ReplMessage::Hello(hello),
        &mut store,
        0,
    );
    assert!(matches!(session, SessionState::Streaming(_)));

    (session, store, identity)
}

#[test]
fn repl_backpressure_overload_emits_error() {
    let limits = Limits {
        max_repl_ingest_queue_bytes: 1,
        max_repl_ingest_queue_events: 1,
        ..Default::default()
    };

    let (session, mut store, identity) = inbound_session_with_limits(limits);
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([12u8; 16]));

    let event = repl_frames::event_frame(identity, namespace, origin, 1, None);
    let (session, actions) = handle_inbound_message(
        session,
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
    assert!(matches!(session, SessionState::Draining(_)));
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, SessionAction::Close { .. }))
    );
}
