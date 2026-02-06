#![allow(dead_code)]

use std::collections::BTreeMap;

use beads_rs::Limits;
use beads_rs::core::{
    Applied, Durable, EventFrameV1, EventId, EventShaLookupError, HeadStatus, NamespaceId,
    ReplicaId, Seq0, Seq1, Sha256, StoreIdentity, Watermark,
};
use beads_rs::daemon::admission::{AdmissionController, AdmissionPermit};
use beads_rs::daemon::repl::proto::WatermarkState;
use beads_rs::daemon::repl::session::{
    Inbound, InboundConnecting, Outbound, OutboundConnecting, SessionState, handle_inbound_message,
    handle_outbound_message,
};
use beads_rs::daemon::repl::{
    Events, IngestOutcome, ReplError, SessionAction, SessionConfig, SessionPhase, SessionStore,
    ValidatedAck, Want, WatermarkSnapshot,
};
use beads_rs::daemon::wal::ReplicaDurabilityRole;

use super::identity;
use super::repl_frames;
use super::repl_transport::ChannelEndpoint;

#[derive(Clone, Debug, Default)]
pub struct MockStore {
    lookup: BTreeMap<EventId, Sha256>,
    durable: WatermarkState<Durable>,
    applied: WatermarkState<Applied>,
}

impl MockStore {
    pub fn has_event(&self, eid: &EventId) -> bool {
        self.lookup.contains_key(eid)
    }

    fn snapshot_for(&self, namespaces: &[NamespaceId]) -> WatermarkSnapshot {
        let mut durable = WatermarkState::new();
        let mut applied = WatermarkState::new();

        for ns in namespaces {
            if let Some(origins) = self.durable.get(ns) {
                durable.insert(ns.clone(), origins.clone());
            }
            if let Some(origins) = self.applied.get(ns) {
                applied.insert(ns.clone(), origins.clone());
            }
        }

        WatermarkSnapshot { durable, applied }
    }
}

impl SessionStore for MockStore {
    fn watermark_snapshot(&self, namespaces: &[NamespaceId]) -> WatermarkSnapshot {
        self.snapshot_for(namespaces)
    }

    fn lookup_event_sha(&self, eid: &EventId) -> Result<Option<Sha256>, EventShaLookupError> {
        Ok(self.lookup.get(eid).copied())
    }

    fn ingest_remote_batch(
        &mut self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        batch: &[beads_rs::core::VerifiedEvent<beads_rs::core::PrevVerified>],
        _now_ms: u64,
    ) -> Result<IngestOutcome, ReplError> {
        for ev in batch {
            let eid = EventId::new(
                ev.body.origin_replica_id,
                ev.body.namespace.clone(),
                ev.body.origin_seq,
            );
            self.lookup.insert(eid, ev.sha256);
        }

        let last = batch.last().expect("batch not empty");
        let head = HeadStatus::Known(last.sha256.0);
        let durable = Watermark::new(Seq0::new(last.seq().get()), head).expect("durable");
        let applied = Watermark::new(Seq0::new(last.seq().get()), head).expect("applied");
        self.durable
            .entry(namespace.clone())
            .or_default()
            .insert(*origin, durable);
        self.applied
            .entry(namespace.clone())
            .or_default()
            .insert(*origin, applied);

        Ok(IngestOutcome { durable, applied })
    }

    fn update_replica_liveness(
        &mut self,
        _replica_id: ReplicaId,
        _last_seen_ms: u64,
        _last_handshake_ms: u64,
        _role: ReplicaDurabilityRole,
    ) -> Result<(), beads_rs::daemon::wal::WalIndexError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MockPeerOutput {
    pub sent: Vec<beads_rs::daemon::repl::ReplMessage>,
    pub peer_acks: Vec<ValidatedAck>,
    pub peer_wants: Vec<Want>,
    pub peer_errors: Vec<beads_rs::ErrorPayload>,
    pub closed: Option<beads_rs::ErrorPayload>,
}

pub struct MockPeer<R> {
    session: Option<SessionState<R>>,
    store: MockStore,
    endpoint: ChannelEndpoint,
    now_ms: u64,
}

impl MockPeer<Inbound> {
    pub fn new_inbound(
        endpoint: ChannelEndpoint,
        identity: StoreIdentity,
        replica_id: ReplicaId,
        limits: Limits,
    ) -> Self {
        let mut config = SessionConfig::new(identity, replica_id, &limits);
        config.requested_namespaces = vec![NamespaceId::core()];
        config.offered_namespaces = vec![NamespaceId::core()];
        let admission = AdmissionController::new(&limits);
        let session = InboundConnecting::new(config, limits, admission);
        Self {
            session: Some(SessionState::Connecting(session)),
            store: MockStore::default(),
            endpoint,
            now_ms: 0,
        }
    }
}

impl MockPeer<Outbound> {
    pub fn new_outbound(
        endpoint: ChannelEndpoint,
        identity: StoreIdentity,
        replica_id: ReplicaId,
        limits: Limits,
    ) -> Self {
        let mut config = SessionConfig::new(identity, replica_id, &limits);
        config.requested_namespaces = vec![NamespaceId::core()];
        config.offered_namespaces = vec![NamespaceId::core()];
        let admission = AdmissionController::new(&limits);
        let session = OutboundConnecting::new(config, limits, admission);
        Self {
            session: Some(SessionState::Connecting(session)),
            store: MockStore::default(),
            endpoint,
            now_ms: 0,
        }
    }
}

impl<R> MockPeer<R> {
    pub fn phase(&self) -> SessionPhase {
        self.session.as_ref().expect("session").phase()
    }

    pub fn store(&self) -> &MockStore {
        &self.store
    }

    pub fn send_events(&self, events: Vec<EventFrameV1>) {
        let message = beads_rs::daemon::repl::ReplMessage::Events(Events { events });
        self.endpoint.send_message(&message);
    }

    pub fn send_message(&self, message: &beads_rs::daemon::repl::ReplMessage) {
        self.endpoint.send_message(message);
    }

    fn apply_action(&mut self, action: SessionAction, output: &mut MockPeerOutput) {
        match action {
            SessionAction::Send(msg) => {
                self.endpoint.send_message(&msg);
                output.sent.push(msg);
            }
            SessionAction::PeerAck(ack) => output.peer_acks.push(ack),
            SessionAction::PeerWant(want) => output.peer_wants.push(want),
            SessionAction::PeerError(err) => output.peer_errors.push(err),
            SessionAction::Close { error } => output.closed = error,
        }
    }
}

impl MockPeer<Outbound> {
    pub fn start_handshake(&mut self) -> MockPeerOutput {
        let mut output = MockPeerOutput::default();
        let session = self.session.take().expect("session");
        let SessionState::Connecting(session) = session else {
            panic!("outbound session not connecting");
        };
        let (session, action) = session.begin_handshake(&self.store, self.now_ms);
        self.session = Some(SessionState::Handshaking(session));
        self.apply_action(action, &mut output);
        output
    }

    pub fn drain(&mut self) -> MockPeerOutput {
        let mut output = MockPeerOutput::default();
        while let Some(envelope) = self.endpoint.try_recv_message() {
            let session = self.session.take().expect("session");
            let (session, actions) =
                handle_outbound_message(session, envelope.message, &mut self.store, self.now_ms);
            self.session = Some(session);
            for action in actions {
                self.apply_action(action, &mut output);
            }
        }
        output
    }
}

impl MockPeer<Inbound> {
    pub fn drain(&mut self) -> MockPeerOutput {
        let mut output = MockPeerOutput::default();
        while let Some(envelope) = self.endpoint.try_recv_message() {
            let session = self.session.take().expect("session");
            let (session, actions) =
                handle_inbound_message(session, envelope.message, &mut self.store, self.now_ms);
            self.session = Some(session);
            for action in actions {
                self.apply_action(action, &mut output);
            }
        }
        output
    }
}

pub fn fill_repl_ingest_queue(admission: &AdmissionController, limits: &Limits) -> AdmissionPermit {
    admission
        .try_admit_repl_ingest(
            limits.max_repl_ingest_queue_bytes as u64,
            limits.max_repl_ingest_queue_events as u64,
        )
        .expect("fill repl ingest queue")
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    use beads_rs::core::EventId;

    use super::super::repl_transport::ChannelTransport;

    #[test]
    fn fixtures_repl_peer_handshake_and_events() {
        let limits = Limits::default();
        let transport = ChannelTransport::with_limits(&limits);
        let identity = identity::store_identity_with_epoch(9, 1);
        let outbound_replica = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
        let inbound_replica = ReplicaId::new(Uuid::from_bytes([2u8; 16]));

        let mut outbound =
            MockPeer::new_outbound(transport.a, identity, outbound_replica, limits.clone());
        let mut inbound =
            MockPeer::new_inbound(transport.b, identity, inbound_replica, limits.clone());

        outbound.start_handshake();
        transport.network.flush();
        inbound.drain();
        transport.network.flush();
        outbound.drain();

        assert_eq!(outbound.phase(), SessionPhase::Streaming);
        assert_eq!(inbound.phase(), SessionPhase::Streaming);

        let event =
            repl_frames::event_frame(identity, NamespaceId::core(), outbound_replica, 1, None);
        outbound.send_events(vec![event]);
        transport.network.flush();
        inbound.drain();

        let eid = EventId::new(
            outbound_replica,
            NamespaceId::core(),
            Seq1::from_u64(1).expect("seq1"),
        );
        assert!(inbound.store().has_event(&eid));
    }
}
