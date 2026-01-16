//! Replication session state machine.

use std::collections::{BTreeMap, BTreeSet};

use crate::core::error::details::{
    EventIdDetails, FrameTooLargeDetails, HashMismatchDetails, InternalErrorDetails,
    InvalidRequestDetails, NamespacePolicyViolationDetails, NonCanonicalDetails, OverloadedDetails,
    PrevShaMismatchDetails, ReplicaIdCollisionDetails, ReplRejectReason,
    StoreEpochMismatchDetails, SubscriberLaggedDetails, VersionIncompatibleDetails,
    WrongStoreDetails,
};
use crate::core::{
    Applied, DecodeError, Durable, ErrorCode, ErrorPayload, EventFrameError, EventFrameV1, EventId,
    EventShaLookup, EventShaLookupError, HeadStatus, Limits, NamespaceId, PrevVerified, ReplicaId,
    ReplicaRole, Seq0, Seq1, Sha256, StoreEpoch, StoreId, StoreIdentity, VerifiedEvent, Watermark,
    WatermarkError, hash_event_body, verify_event_frame,
};
use crate::daemon::admission::{AdmissionController, AdmissionRejection};
use crate::daemon::metrics;
use crate::daemon::wal::WalIndexError;

use super::error::{ReplError, ReplErrorDetails};
use super::gap_buffer::{DrainError, GapBufferByNsOrigin, IngestDecision};
use super::proto::{
    Ack, Capabilities, Events, Hello, PROTOCOL_VERSION_V1, Ping, Pong, ReplMessage, Want,
    WatermarkHeads, WatermarkMap,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionRole {
    Inbound,
    Outbound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionPhase {
    Connecting,
    Handshaking,
    Streaming,
    Draining,
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtocolRange {
    pub min: u32,
    pub max: u32,
}

impl ProtocolRange {
    pub fn new(min: u32, max: u32) -> Self {
        Self { min, max }
    }
}

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub local_store: StoreIdentity,
    pub local_replica_id: ReplicaId,
    pub protocol: ProtocolRange,
    pub requested_namespaces: Vec<NamespaceId>,
    pub offered_namespaces: Vec<NamespaceId>,
    pub capabilities: Capabilities,
    pub max_frame_bytes: u32,
}

impl SessionConfig {
    pub fn new(local_store: StoreIdentity, local_replica_id: ReplicaId, limits: &Limits) -> Self {
        Self {
            local_store,
            local_replica_id,
            protocol: ProtocolRange::new(PROTOCOL_VERSION_V1, PROTOCOL_VERSION_V1),
            requested_namespaces: Vec::new(),
            offered_namespaces: Vec::new(),
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
            max_frame_bytes: limits.max_frame_bytes.min(u32::MAX as usize) as u32,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SessionPeer {
    pub replica_id: ReplicaId,
    pub store_epoch: StoreEpoch,
    pub protocol_version: u32,
    pub max_frame_bytes: u32,
    pub incoming_namespaces: Vec<NamespaceId>,
    pub accepted_namespaces: Vec<NamespaceId>,
    pub live_stream_enabled: bool,
}

#[derive(Clone, Debug)]
enum SessionState {
    Connecting,
    Handshaking,
    Streaming { peer: SessionPeer },
    Draining { peer: SessionPeer },
    Closed,
}

impl SessionState {
    fn phase(&self) -> SessionPhase {
        match self {
            SessionState::Connecting => SessionPhase::Connecting,
            SessionState::Handshaking => SessionPhase::Handshaking,
            SessionState::Streaming { .. } => SessionPhase::Streaming,
            SessionState::Draining { .. } => SessionPhase::Draining,
            SessionState::Closed => SessionPhase::Closed,
        }
    }

    fn peer(&self) -> Option<&SessionPeer> {
        match self {
            SessionState::Streaming { peer } | SessionState::Draining { peer } => Some(peer),
            SessionState::Connecting | SessionState::Handshaking | SessionState::Closed => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WatermarkSnapshot {
    pub durable: WatermarkMap,
    pub durable_heads: WatermarkHeads,
    pub applied: WatermarkMap,
    pub applied_heads: WatermarkHeads,
}

#[derive(Clone, Debug)]
pub struct IngestOutcome {
    pub durable: Watermark<Durable>,
    pub applied: Watermark<Applied>,
}

pub trait SessionStore {
    fn watermark_snapshot(&self, namespaces: &[NamespaceId]) -> WatermarkSnapshot;

    fn lookup_event_sha(&self, eid: &EventId) -> Result<Option<Sha256>, EventShaLookupError>;

    fn ingest_remote_batch(
        &mut self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        batch: &[VerifiedEvent<PrevVerified>],
        _now_ms: u64,
    ) -> SessionResult<IngestOutcome>;

    fn update_replica_liveness(
        &mut self,
        replica_id: ReplicaId,
        last_seen_ms: u64,
        last_handshake_ms: u64,
        role: ReplicaRole,
        durability_eligible: bool,
    ) -> Result<(), WalIndexError>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum SessionAction {
    Send(ReplMessage),
    Close { error: Option<ErrorPayload> },
    PeerAck(Ack),
    PeerWant(Want),
    PeerError(ErrorPayload),
}

type WatermarkState<K> = BTreeMap<NamespaceId, BTreeMap<ReplicaId, Watermark<K>>>;
type SessionResult<T> = Result<T, ReplError>;

struct IngestUpdates<'a> {
    ack_updates: &'a mut WatermarkState<Durable>,
    applied_updates: &'a mut WatermarkState<Applied>,
}

#[derive(Debug)]
pub struct Session {
    role: SessionRole,
    state: SessionState,
    config: SessionConfig,
    limits: Limits,
    admission: AdmissionController,
    gap_buffer: GapBufferByNsOrigin,
    durable: WatermarkState<Durable>,
    applied: WatermarkState<Applied>,
    next_nonce: u64,
}

impl Session {
    pub fn new(
        role: SessionRole,
        mut config: SessionConfig,
        limits: Limits,
        admission: AdmissionController,
    ) -> Self {
        normalize_namespaces(&mut config.requested_namespaces);
        normalize_namespaces(&mut config.offered_namespaces);
        Self {
            role,
            state: SessionState::Connecting,
            config,
            gap_buffer: GapBufferByNsOrigin::new(limits.clone()),
            limits,
            admission,
            durable: BTreeMap::new(),
            applied: BTreeMap::new(),
            next_nonce: 1,
        }
    }

    pub fn phase(&self) -> SessionPhase {
        self.state.phase()
    }

    pub fn peer(&self) -> Option<&SessionPeer> {
        self.state.peer()
    }

    pub fn negotiated_max_frame_bytes(&self) -> usize {
        self.peer()
            .map(|peer| peer.max_frame_bytes as usize)
            .unwrap_or(self.config.max_frame_bytes as usize)
    }

    pub fn begin_handshake(
        &mut self,
        store: &impl SessionStore,
        now_ms: u64,
    ) -> Option<SessionAction> {
        if self.role != SessionRole::Outbound
            || !matches!(self.state, SessionState::Connecting)
        {
            return None;
        }
        let hello = self.build_hello(store, now_ms);
        self.state = SessionState::Handshaking;
        Some(SessionAction::Send(ReplMessage::Hello(hello)))
    }

    pub fn handle_message(
        &mut self,
        msg: ReplMessage,
        store: &mut impl SessionStore,
        now_ms: u64,
    ) -> Vec<SessionAction> {
        match msg {
            ReplMessage::Hello(msg) => match &self.state {
                SessionState::Connecting => self.handle_hello(msg, store, now_ms),
                _ => self.invalid_request("unexpected HELLO"),
            },
            ReplMessage::Welcome(msg) => match &self.state {
                SessionState::Handshaking => self.handle_welcome(msg, store, now_ms),
                _ => self.invalid_request("unexpected WELCOME"),
            },
            ReplMessage::Events(msg) => match &self.state {
                SessionState::Streaming { peer } | SessionState::Draining { peer } => {
                    let incoming_namespaces = peer.incoming_namespaces.clone();
                    self.handle_events(msg, incoming_namespaces, store, now_ms)
                }
                _ => self.invalid_request("EVENTS before handshake"),
            },
            ReplMessage::Ack(msg) => match &self.state {
                SessionState::Streaming { .. } | SessionState::Draining { .. } => {
                    self.handle_ack(msg)
                }
                _ => self.invalid_request("ACK before handshake"),
            },
            ReplMessage::Want(msg) => match &self.state {
                SessionState::Streaming { .. } | SessionState::Draining { .. } => {
                    self.handle_want(msg)
                }
                _ => self.invalid_request("WANT before handshake"),
            },
            ReplMessage::Ping(msg) => match &self.state {
                SessionState::Streaming { .. } | SessionState::Draining { .. } => {
                    self.handle_ping(msg)
                }
                _ => self.invalid_request("PING before handshake"),
            },
            ReplMessage::Pong(msg) => match &self.state {
                SessionState::Streaming { .. } | SessionState::Draining { .. } => {
                    self.handle_pong(msg)
                }
                _ => self.invalid_request("PONG before handshake"),
            },
            ReplMessage::Error(msg) => self.handle_peer_error(msg),
        }
    }

    pub fn mark_closed(&mut self) {
        self.state = SessionState::Closed;
    }

    fn handle_ack(&mut self, ack: Ack) -> Vec<SessionAction> {
        vec![SessionAction::PeerAck(ack)]
    }

    fn handle_want(&mut self, want: Want) -> Vec<SessionAction> {
        vec![SessionAction::PeerWant(want)]
    }

    fn handle_ping(&mut self, ping: Ping) -> Vec<SessionAction> {
        vec![SessionAction::Send(ReplMessage::Pong(Pong {
            nonce: ping.nonce,
        }))]
    }

    fn handle_pong(&mut self, _pong: Pong) -> Vec<SessionAction> {
        Vec::new()
    }

    fn handle_peer_error(&mut self, payload: ErrorPayload) -> Vec<SessionAction> {
        self.state = match self.peer().cloned() {
            Some(peer) => SessionState::Draining { peer },
            None => SessionState::Closed,
        };
        vec![
            SessionAction::PeerError(payload.clone()),
            SessionAction::Close {
                error: Some(payload),
            },
        ]
    }

    fn handle_hello(
        &mut self,
        hello: Hello,
        store: &mut impl SessionStore,
        _now_ms: u64,
    ) -> Vec<SessionAction> {
        if self.role != SessionRole::Inbound {
            return self.invalid_request("unexpected HELLO");
        }

        if let Err(error) =
            self.validate_peer_store(hello.store_id, hello.store_epoch, hello.sender_replica_id)
        {
            return self.fail(error);
        }

        if hello.min_protocol_version > hello.protocol_version {
            return self.invalid_request("min_protocol_version exceeds protocol_version");
        }

        let negotiated = match negotiate_version(
            self.config.protocol,
            hello.protocol_version,
            hello.min_protocol_version,
        ) {
            Ok(version) => version,
            Err(err) => return self.fail(version_incompatible_error(&err)),
        };

        let accepted_namespaces =
            intersect_namespaces(&self.config.offered_namespaces, &hello.requested_namespaces);
        let incoming_namespaces =
            intersect_namespaces(&self.config.requested_namespaces, &hello.offered_namespaces);

        let snapshot = store.watermark_snapshot(&accepted_namespaces);
        let welcome = self.build_welcome(negotiated, &hello, snapshot, accepted_namespaces.clone());

        if let Err(error) = self.seed_watermarks(store, &incoming_namespaces) {
            return self.fail(error);
        }
        let peer = SessionPeer {
            replica_id: hello.sender_replica_id,
            store_epoch: hello.store_epoch,
            protocol_version: negotiated,
            max_frame_bytes: std::cmp::min(self.config.max_frame_bytes, hello.max_frame_bytes),
            incoming_namespaces,
            accepted_namespaces,
            live_stream_enabled: welcome.live_stream_enabled,
        };
        self.state = SessionState::Streaming { peer };

        vec![SessionAction::Send(ReplMessage::Welcome(welcome))]
    }

    fn handle_welcome(
        &mut self,
        welcome: super::proto::Welcome,
        store: &mut impl SessionStore,
        _now_ms: u64,
    ) -> Vec<SessionAction> {
        if self.role != SessionRole::Outbound {
            return self.invalid_request("unexpected WELCOME");
        }

        if let Err(error) = self.validate_peer_store(
            welcome.store_id,
            welcome.store_epoch,
            welcome.receiver_replica_id,
        ) {
            return self.fail(error);
        }

        if welcome.protocol_version < self.config.protocol.min
            || welcome.protocol_version > self.config.protocol.max
        {
            return self.fail(version_incompatible_error(&ProtocolIncompatible {
                local_min: self.config.protocol.min,
                local_max: self.config.protocol.max,
                peer_min: welcome.protocol_version,
                peer_max: welcome.protocol_version,
            }));
        }

        let incoming_namespaces = intersect_namespaces(
            &self.config.requested_namespaces,
            &welcome.accepted_namespaces,
        );
        if let Err(error) = self.seed_watermarks(store, &incoming_namespaces) {
            return self.fail(error);
        }

        let peer = SessionPeer {
            replica_id: welcome.receiver_replica_id,
            store_epoch: welcome.store_epoch,
            protocol_version: welcome.protocol_version,
            max_frame_bytes: std::cmp::min(self.config.max_frame_bytes, welcome.max_frame_bytes),
            incoming_namespaces,
            accepted_namespaces: welcome.accepted_namespaces.clone(),
            live_stream_enabled: welcome.live_stream_enabled,
        };
        self.state = SessionState::Streaming { peer };

        Vec::new()
    }

    fn handle_events(
        &mut self,
        events: Events,
        incoming_namespaces: Vec<NamespaceId>,
        store: &mut impl SessionStore,
        now_ms: u64,
    ) -> Vec<SessionAction> {

        let total_bytes: u64 = events
            .events
            .iter()
            .map(|frame| frame.bytes.len() as u64)
            .sum();
        let event_count = events.events.len() as u64;
        metrics::repl_events_in(event_count as usize);
        let permit = match self
            .admission
            .try_admit_repl_ingest(total_bytes, event_count)
        {
            Ok(permit) => permit,
            Err(err) => return self.fail(overloaded_error(err)),
        };

        let mut ack_updates: WatermarkState<Durable> = BTreeMap::new();
        let mut applied_updates: WatermarkState<Applied> = BTreeMap::new();
        let mut wants: WatermarkMap = BTreeMap::new();
        let mut updates = IngestUpdates {
            ack_updates: &mut ack_updates,
            applied_updates: &mut applied_updates,
        };
        for frame in events.events {
            let namespace = frame.eid.namespace.clone();
            let origin = frame.eid.origin_replica_id;
            if !incoming_namespaces.contains(&namespace) {
                return self.fail(namespace_policy_violation_error(&namespace));
            }
            let durable = self.durable_for(&namespace, &origin);
            let head_seq = durable.seq().get();
            let head_sha = match durable.head() {
                HeadStatus::Genesis => None,
                HeadStatus::Known(head) => Some(Sha256(head)),
                HeadStatus::Unknown => None,
            };

            let expected_prev =
                match self.expected_prev_head(durable, frame.eid.origin_seq) {
                    Ok(prev) => prev,
                    Err(error) => return self.fail(error),
                };

            let verified = {
                let lookup = SessionLookup { store };
                match verify_event_frame(
                    &frame,
                    &self.limits,
                    self.config.local_store,
                    expected_prev,
                    &lookup,
                ) {
                    Ok(event) => event,
                    Err(err) => {
                        return self.fail(event_frame_error_payload(
                            &frame,
                            &self.limits,
                            err,
                            head_sha,
                            head_seq,
                        ));
                    }
                }
            };

            match self
                .gap_buffer
                .ingest(namespace.clone(), origin, durable, verified, now_ms)
            {
                IngestDecision::ForwardContiguousBatch(batch) => {
                    if let Err(error) = self.ingest_contiguous_batch(
                        store,
                        &namespace,
                        &origin,
                        &batch,
                        now_ms,
                        &mut updates,
                    ) {
                        return self.fail(error);
                    }
                    if let Err(error) =
                        self.drain_gap_ready(store, &namespace, &origin, now_ms, &mut updates)
                    {
                        return self.fail(error);
                    }
                }
                IngestDecision::BufferedNeedWant { want_from } => {
                    insert_want(&mut wants, namespace, origin, want_from);
                }
                IngestDecision::DuplicateNoop => {}
                IngestDecision::Reject { reason } => {
                    return self.fail(repl_lagged_error(reason, &self.limits));
                }
            }
        }

        drop(permit);

        self.reconcile_wants(&mut wants);

        let mut actions = Vec::new();
        if let Some(ack) = build_ack(&ack_updates, &applied_updates) {
            actions.push(SessionAction::Send(ReplMessage::Ack(ack)));
        }
        if !wants.is_empty() {
            actions.push(SessionAction::Send(ReplMessage::Want(Want { want: wants })));
        }
        actions
    }

    fn build_hello(&mut self, store: &impl SessionStore, _now_ms: u64) -> Hello {
        let snapshot = store.watermark_snapshot(&self.config.requested_namespaces);
        Hello {
            protocol_version: self.config.protocol.max,
            min_protocol_version: self.config.protocol.min,
            store_id: self.config.local_store.store_id,
            store_epoch: self.config.local_store.store_epoch,
            sender_replica_id: self.config.local_replica_id,
            hello_nonce: self.next_nonce(),
            max_frame_bytes: self.config.max_frame_bytes,
            requested_namespaces: self.config.requested_namespaces.clone(),
            offered_namespaces: self.config.offered_namespaces.clone(),
            seen_durable: snapshot.durable,
            seen_durable_heads: optional_heads(snapshot.durable_heads),
            seen_applied: Some(snapshot.applied),
            seen_applied_heads: optional_heads(snapshot.applied_heads),
            capabilities: self.config.capabilities.clone(),
        }
    }

    fn build_welcome(
        &mut self,
        protocol_version: u32,
        hello: &Hello,
        snapshot: WatermarkSnapshot,
        accepted_namespaces: Vec<NamespaceId>,
    ) -> super::proto::Welcome {
        let live_stream_enabled = self.config.capabilities.supports_live_stream
            && hello.capabilities.supports_live_stream;
        super::proto::Welcome {
            protocol_version,
            store_id: hello.store_id,
            store_epoch: hello.store_epoch,
            receiver_replica_id: self.config.local_replica_id,
            welcome_nonce: self.next_nonce(),
            accepted_namespaces,
            receiver_seen_durable: snapshot.durable,
            receiver_seen_durable_heads: optional_heads(snapshot.durable_heads),
            receiver_seen_applied: Some(snapshot.applied),
            receiver_seen_applied_heads: optional_heads(snapshot.applied_heads),
            live_stream_enabled,
            max_frame_bytes: std::cmp::min(self.config.max_frame_bytes, hello.max_frame_bytes),
        }
    }

    fn next_nonce(&mut self) -> u64 {
        let nonce = self.next_nonce;
        self.next_nonce = self.next_nonce.saturating_add(1);
        nonce
    }

    fn seed_watermarks(
        &mut self,
        store: &impl SessionStore,
        namespaces: &[NamespaceId],
    ) -> SessionResult<()> {
        let snapshot = store.watermark_snapshot(namespaces);
        self.durable = watermark_state_from_snapshot(
            &snapshot.durable,
            Some(&snapshot.durable_heads),
        )
        .map_err(|err| internal_error(format!("invalid durable watermark snapshot: {err}")))?;
        self.applied = watermark_state_from_snapshot(
            &snapshot.applied,
            Some(&snapshot.applied_heads),
        )
        .map_err(|err| internal_error(format!("invalid applied watermark snapshot: {err}")))?;
        Ok(())
    }

    fn durable_for(&self, namespace: &NamespaceId, origin: &ReplicaId) -> Watermark<Durable> {
        self.durable
            .get(namespace)
            .and_then(|origins| origins.get(origin))
            .copied()
            .unwrap_or_else(Watermark::genesis)
    }

    fn expected_prev_head(
        &self,
        durable: Watermark<Durable>,
        seq: Seq1,
    ) -> SessionResult<Option<Sha256>> {
        if seq.get() != durable.seq().get().saturating_add(1) {
            return Ok(None);
        }

        match durable.head() {
            HeadStatus::Genesis => Ok(None),
            HeadStatus::Known(head) => Ok(Some(Sha256(head))),
            HeadStatus::Unknown => unreachable!("unknown head should never reach session state"),
        }
    }

    fn ingest_contiguous_batch(
        &mut self,
        store: &mut impl SessionStore,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        batch: &[VerifiedEvent<PrevVerified>],
        now_ms: u64,
        updates: &mut IngestUpdates<'_>,
    ) -> SessionResult<()> {
        let outcome = store.ingest_remote_batch(namespace, origin, batch, now_ms)?;

        if let Err(err) = self
            .gap_buffer
            .advance_durable_batch(namespace, origin, batch)
        {
            return Err(internal_error(format!(
                "gap buffer watermark advance failed: {err}"
            )));
        }

        update_watermark(&mut self.durable, namespace, origin, outcome.durable);
        update_watermark(&mut self.applied, namespace, origin, outcome.applied);

        update_watermark(updates.ack_updates, namespace, origin, outcome.durable);
        update_watermark(updates.applied_updates, namespace, origin, outcome.applied);

        Ok(())
    }

    fn drain_gap_ready(
        &mut self,
        store: &mut impl SessionStore,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        now_ms: u64,
        updates: &mut IngestUpdates<'_>,
    ) -> SessionResult<()> {
        loop {
            let batch = match self.gap_buffer.drain_ready(namespace, origin) {
                Ok(batch) => batch,
                Err(err) => return Err(drain_error_payload(err)),
            };
            let Some(batch) = batch else {
                return Ok(());
            };
            self.ingest_contiguous_batch(store, namespace, origin, &batch, now_ms, updates)?;
        }
    }

    fn reconcile_wants(&self, wants: &mut WatermarkMap) {
        let mut empty = Vec::new();
        for (namespace, origins) in wants.iter_mut() {
            origins.retain(|origin, seq| {
                if let Some(want_from) = self.gap_buffer.want_from(namespace, origin) {
                    *seq = want_from;
                    true
                } else {
                    false
                }
            });
            if origins.is_empty() {
                empty.push(namespace.clone());
            }
        }
        for namespace in empty {
            wants.remove(&namespace);
        }
    }

    fn validate_peer_store(
        &self,
        peer_store_id: StoreId,
        peer_epoch: StoreEpoch,
        peer_replica_id: ReplicaId,
    ) -> SessionResult<()> {
        if peer_replica_id == self.config.local_replica_id {
            return Err(replica_id_collision_error(peer_replica_id));
        }
        if peer_store_id != self.config.local_store.store_id {
            return Err(wrong_store_error(
                self.config.local_store.store_id,
                peer_store_id,
            ));
        }
        if peer_epoch != self.config.local_store.store_epoch {
            return Err(store_epoch_mismatch_error(
                self.config.local_store.store_id,
                self.config.local_store.store_epoch,
                peer_epoch,
            ));
        }
        Ok(())
    }

    fn invalid_request(&mut self, reason: impl Into<String>) -> Vec<SessionAction> {
        let error = invalid_request_error(reason);
        self.fail(error)
    }

    fn fail(&mut self, error: ReplError) -> Vec<SessionAction> {
        self.state = match self.peer().cloned() {
            Some(peer) => SessionState::Draining { peer },
            None => SessionState::Closed,
        };
        let payload = error.to_payload();
        vec![
            SessionAction::Send(ReplMessage::Error(payload.clone())),
            SessionAction::Close {
                error: Some(payload),
            },
        ]
    }
}

#[derive(Debug)]
struct SessionLookup<'a, S> {
    store: &'a S,
}

impl<S: SessionStore> EventShaLookup for SessionLookup<'_, S> {
    fn lookup_event_sha(&self, eid: &EventId) -> Result<Option<Sha256>, EventShaLookupError> {
        self.store.lookup_event_sha(eid)
    }
}

#[derive(Debug)]
struct ProtocolIncompatible {
    local_min: u32,
    local_max: u32,
    peer_min: u32,
    peer_max: u32,
}

fn negotiate_version(
    local: ProtocolRange,
    peer_max: u32,
    peer_min: u32,
) -> Result<u32, ProtocolIncompatible> {
    let v = local.max.min(peer_max);
    let min_ok = local.min.max(peer_min);
    if v >= min_ok {
        Ok(v)
    } else {
        Err(ProtocolIncompatible {
            local_min: local.min,
            local_max: local.max,
            peer_min,
            peer_max,
        })
    }
}

fn normalize_namespaces(namespaces: &mut Vec<NamespaceId>) {
    if namespaces.is_empty() {
        return;
    }
    let mut set = BTreeSet::new();
    set.extend(namespaces.drain(..));
    namespaces.extend(set);
}

fn intersect_namespaces(a: &[NamespaceId], b: &[NamespaceId]) -> Vec<NamespaceId> {
    if a.is_empty() || b.is_empty() {
        return Vec::new();
    }
    let bset: BTreeSet<_> = b.iter().cloned().collect();
    let mut out: Vec<_> = a.iter().filter(|ns| bset.contains(*ns)).cloned().collect();
    out.sort();
    out
}

fn optional_heads(mut heads: WatermarkHeads) -> Option<WatermarkHeads> {
    if heads.is_empty() {
        None
    } else {
        heads.retain(|_, origins| !origins.is_empty());
        if heads.is_empty() { None } else { Some(heads) }
    }
}

fn watermark_state_from_snapshot<K>(
    map: &WatermarkMap,
    heads: Option<&WatermarkHeads>,
) -> Result<WatermarkState<K>, WatermarkError> {
    let mut out: WatermarkState<K> = BTreeMap::new();
    for (namespace, origins) in map {
        let ns_map = out.entry(namespace.clone()).or_default();
        for (origin, seq) in origins {
            let head = match heads
                .and_then(|heads| heads.get(namespace))
                .and_then(|origins| origins.get(origin))
            {
                Some(sha) => HeadStatus::Known(sha.0),
                None if *seq == Seq0::ZERO => HeadStatus::Genesis,
                None => return Err(WatermarkError::MissingHead { seq: *seq }),
            };
            let watermark = Watermark::new(*seq, head)?;
            ns_map.insert(*origin, watermark);
        }
    }
    Ok(out)
}

fn insert_want(want: &mut WatermarkMap, namespace: NamespaceId, origin: ReplicaId, from: Seq0) {
    let ns = want.entry(namespace).or_default();
    ns.entry(origin)
        .and_modify(|seq| {
            if from < *seq {
                *seq = from;
            }
        })
        .or_insert(from);
}

fn update_watermark<K>(
    state: &mut WatermarkState<K>,
    namespace: &NamespaceId,
    origin: &ReplicaId,
    watermark: Watermark<K>,
) {
    state
        .entry(namespace.clone())
        .or_default()
        .insert(*origin, watermark);
}

fn build_ack(
    durable_updates: &WatermarkState<Durable>,
    applied_updates: &WatermarkState<Applied>,
) -> Option<Ack> {
    if durable_updates.is_empty() && applied_updates.is_empty() {
        return None;
    }

    let (durable, durable_heads) = watermark_maps_from_state(durable_updates);
    let (applied, applied_heads) = watermark_maps_from_state(applied_updates);

    Some(Ack {
        durable,
        durable_heads,
        applied: if applied.is_empty() {
            None
        } else {
            Some(applied)
        },
        applied_heads,
    })
}

fn watermark_maps_from_state<K>(
    state: &WatermarkState<K>,
) -> (WatermarkMap, Option<WatermarkHeads>) {
    let mut map: WatermarkMap = BTreeMap::new();
    let mut heads: WatermarkHeads = BTreeMap::new();

    for (namespace, origins) in state {
        let ns_map = map.entry(namespace.clone()).or_default();
        let ns_heads = heads.entry(namespace.clone()).or_default();
        for (origin, watermark) in origins {
            ns_map.insert(*origin, watermark.seq());
            if let HeadStatus::Known(head) = watermark.head() {
                ns_heads.insert(*origin, Sha256(head));
            }
        }
    }

    let heads = if heads.values().all(|origins| origins.is_empty()) {
        None
    } else {
        Some(heads)
    };
    (map, heads)
}

fn repl_lagged_error(reason: ReplRejectReason, limits: &Limits) -> ReplError {
    let message = repl_reject_reason_message(&reason);
    ReplError::new(ErrorCode::SubscriberLagged, message, true).with_details(
        ReplErrorDetails::SubscriberLagged(SubscriberLaggedDetails {
            reason: Some(reason),
            max_queue_bytes: Some(limits.max_repl_gap_bytes as u64),
            max_queue_events: Some(limits.max_repl_gap_events as u64),
        }),
    )
}

fn repl_reject_reason_message(reason: &ReplRejectReason) -> &'static str {
    match reason {
        ReplRejectReason::PrevUnknown => "prev_unknown",
        ReplRejectReason::GapTimeout => "gap_timeout",
        ReplRejectReason::GapBufferOverflow => "gap_buffer_overflow",
        ReplRejectReason::GapBufferBytesOverflow => "gap_buffer_bytes_overflow",
    }
}

fn wrong_store_error(expected: StoreId, got: StoreId) -> ReplError {
    ReplError::new(ErrorCode::WrongStore, "wrong store id", false).with_details(
        ReplErrorDetails::WrongStore(WrongStoreDetails {
            expected_store_id: expected,
            got_store_id: got,
        }),
    )
}

fn store_epoch_mismatch_error(
    store_id: StoreId,
    expected: StoreEpoch,
    got: StoreEpoch,
) -> ReplError {
    ReplError::new(ErrorCode::StoreEpochMismatch, "store epoch mismatch", false).with_details(
        ReplErrorDetails::StoreEpochMismatch(StoreEpochMismatchDetails {
            store_id,
            expected_epoch: expected.get(),
            got_epoch: got.get(),
        }),
    )
}

fn replica_id_collision_error(replica_id: ReplicaId) -> ReplError {
    ReplError::new(ErrorCode::ReplicaIdCollision, "replica_id collision", false).with_details(
        ReplErrorDetails::ReplicaIdCollision(ReplicaIdCollisionDetails { replica_id }),
    )
}

fn version_incompatible_error(err: &ProtocolIncompatible) -> ReplError {
    ReplError::new(
        ErrorCode::VersionIncompatible,
        "protocol versions incompatible",
        false,
    )
    .with_details(ReplErrorDetails::VersionIncompatible(VersionIncompatibleDetails {
        local_min: err.local_min,
        local_max: err.local_max,
        peer_min: err.peer_min,
        peer_max: err.peer_max,
    }))
}

fn invalid_request_error(reason: impl Into<String>) -> ReplError {
    let reason = reason.into();
    ReplError::new(ErrorCode::InvalidRequest, reason.clone(), false).with_details(
        ReplErrorDetails::InvalidRequest(InvalidRequestDetails {
            field: Some("type".to_string()),
            reason: Some(reason),
        }),
    )
}

fn namespace_policy_violation_error(namespace: &NamespaceId) -> ReplError {
    ReplError::new(
        ErrorCode::NamespacePolicyViolation,
        "namespace not accepted in replication handshake",
        false,
    )
    .with_details(ReplErrorDetails::NamespacePolicyViolation(
        NamespacePolicyViolationDetails {
        namespace: namespace.clone(),
        rule: "accepted_namespaces".to_string(),
        reason: Some("namespace not negotiated for this session".to_string()),
        },
    ))
}

fn internal_error(message: impl Into<String>) -> ReplError {
    ReplError::new(ErrorCode::InternalError, "internal error", false).with_details(
        ReplErrorDetails::InternalError(InternalErrorDetails {
            trace_id: None,
            component: Some(message.into()),
        }),
    )
}

fn overloaded_error(rejection: AdmissionRejection) -> ReplError {
    ReplError::new(ErrorCode::Overloaded, "overloaded", true).with_details(
        ReplErrorDetails::Overloaded(OverloadedDetails {
            subsystem: Some(rejection.subsystem),
            retry_after_ms: Some(rejection.retry_after_ms),
            queue_bytes: rejection.queue_bytes,
            queue_events: rejection.queue_events,
        }),
    )
}

fn event_id_details(eid: &EventId) -> EventIdDetails {
    EventIdDetails {
        namespace: eid.namespace.clone(),
        origin_replica_id: eid.origin_replica_id,
        origin_seq: eid.origin_seq.get(),
    }
}

fn sha256_hex(value: Sha256) -> String {
    hex::encode(value.as_bytes())
}

fn sha256_hex_or_zero(value: Option<Sha256>) -> String {
    sha256_hex(value.unwrap_or(Sha256([0u8; 32])))
}

fn event_frame_error_payload(
    frame: &EventFrameV1,
    limits: &Limits,
    err: EventFrameError,
    head_sha: Option<Sha256>,
    head_seq: u64,
) -> ReplError {
    match err {
        EventFrameError::WrongStore { expected, got } => {
            if expected.store_id != got.store_id {
                wrong_store_error(expected.store_id, got.store_id)
            } else {
                store_epoch_mismatch_error(
                    expected.store_id,
                    expected.store_epoch,
                    got.store_epoch,
                )
            }
        }
        EventFrameError::FrameMismatch => ReplError::new(
            ErrorCode::InvalidRequest,
            "event id does not match decoded body",
            false,
        )
        .with_details(ReplErrorDetails::InvalidRequest(InvalidRequestDetails {
            field: Some("event_id".to_string()),
            reason: Some("event_id does not match decoded body".to_string()),
        })),
        EventFrameError::HashMismatch => {
            let expected = hash_event_body(&frame.bytes);
            ReplError::new(ErrorCode::HashMismatch, "event sha256 mismatch", false).with_details(
                ReplErrorDetails::HashMismatch(HashMismatchDetails {
                    eid: event_id_details(&frame.eid),
                    expected_sha256: sha256_hex(expected),
                    got_sha256: sha256_hex(frame.sha256),
                }),
            )
        }
        EventFrameError::PrevMismatch => ReplError::new(
            ErrorCode::PrevShaMismatch,
            "prev_sha256 mismatch",
            false,
        )
        .with_details(ReplErrorDetails::PrevShaMismatch(PrevShaMismatchDetails {
            eid: event_id_details(&frame.eid),
            expected_prev_sha256: sha256_hex_or_zero(head_sha),
            got_prev_sha256: sha256_hex_or_zero(frame.prev_sha256),
            head_seq,
        })),
        EventFrameError::Validation(err) => {
            ReplError::new(ErrorCode::InvalidRequest, err.to_string(), false).with_details(
                ReplErrorDetails::InvalidRequest(InvalidRequestDetails {
                    field: None,
                    reason: Some(err.to_string()),
                }),
            )
        }
        EventFrameError::Decode(err) => decode_error_payload(&err, limits, frame.bytes.len()),
        EventFrameError::Lookup(err) => internal_error(err.to_string()),
        EventFrameError::Equivocation => {
            ReplError::new(ErrorCode::Equivocation, "equivocation detected", false)
        }
    }
}

fn drain_error_payload(err: DrainError) -> ReplError {
    match err {
        DrainError::PrevMismatch {
            namespace,
            origin,
            seq,
            expected,
            got,
            head_seq,
        } => ReplError::new(ErrorCode::PrevShaMismatch, "prev_sha256 mismatch", false)
            .with_details(ReplErrorDetails::PrevShaMismatch(PrevShaMismatchDetails {
                eid: EventIdDetails {
                    namespace,
                    origin_replica_id: origin,
                    origin_seq: seq.get(),
                },
                expected_prev_sha256: sha256_hex_or_zero(expected),
                got_prev_sha256: sha256_hex_or_zero(got),
                head_seq,
            })),
    }
}

fn decode_error_payload(err: &DecodeError, limits: &Limits, frame_bytes: usize) -> ReplError {
    match err {
        DecodeError::DecodeLimit(reason)
            if matches!(*reason, "max_wal_record_bytes" | "max_frame_bytes") =>
        {
            ReplError::new(ErrorCode::FrameTooLarge, "event frame too large", false).with_details(
                ReplErrorDetails::FrameTooLarge(FrameTooLargeDetails {
                    max_frame_bytes: limits.max_frame_bytes.min(limits.max_wal_record_bytes) as u64,
                    got_bytes: frame_bytes as u64,
                }),
            )
        }
        DecodeError::DecodeLimit(_) => invalid_request_decode_error(err),
        DecodeError::IndefiniteLength | DecodeError::TrailingBytes => ReplError::new(
            ErrorCode::NonCanonical,
            err.to_string(),
            false,
        )
        .with_details(ReplErrorDetails::NonCanonical(NonCanonicalDetails {
            format: "cbor".to_string(),
            reason: Some(err.to_string()),
        })),
        _ => invalid_request_decode_error(err),
    }
}

fn invalid_request_decode_error(err: &DecodeError) -> ReplError {
    ReplError::new(ErrorCode::InvalidRequest, err.to_string(), false).with_details(
        ReplErrorDetails::InvalidRequest(InvalidRequestDetails {
            field: None,
            reason: Some(err.to_string()),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    use crate::core::{
        ActorId, EventBody, EventBytes, EventFrameV1, EventKindV1, HlcMax, NamespaceId, ReplicaId,
        Seq1, StoreEpoch, StoreId, StoreIdentity, TxnDeltaV1, TxnId, TxnV1,
        encode_event_body_canonical, hash_event_body,
    };

    #[derive(Clone, Debug)]
    struct TestStore {
        lookup: BTreeMap<EventId, Sha256>,
        durable: WatermarkState<Durable>,
        applied: WatermarkState<Applied>,
    }

    impl TestStore {
        fn new() -> Self {
            Self {
                lookup: BTreeMap::new(),
                durable: BTreeMap::new(),
                applied: BTreeMap::new(),
            }
        }

        fn snapshot_for(&self, namespaces: &[NamespaceId]) -> WatermarkSnapshot {
            let mut durable: WatermarkMap = BTreeMap::new();
            let mut durable_heads: WatermarkHeads = BTreeMap::new();
            let mut applied: WatermarkMap = BTreeMap::new();
            let mut applied_heads: WatermarkHeads = BTreeMap::new();

            for ns in namespaces {
                if let Some(origins) = self.durable.get(ns) {
                    let ns_map = durable.entry(ns.clone()).or_default();
                    let ns_heads = durable_heads.entry(ns.clone()).or_default();
                    for (origin, wm) in origins {
                        ns_map.insert(*origin, wm.seq());
                        if let HeadStatus::Known(head) = wm.head() {
                            ns_heads.insert(*origin, Sha256(head));
                        }
                    }
                }
                if let Some(origins) = self.applied.get(ns) {
                    let ns_map = applied.entry(ns.clone()).or_default();
                    let ns_heads = applied_heads.entry(ns.clone()).or_default();
                    for (origin, wm) in origins {
                        ns_map.insert(*origin, wm.seq());
                        if let HeadStatus::Known(head) = wm.head() {
                            ns_heads.insert(*origin, Sha256(head));
                        }
                    }
                }
            }

            WatermarkSnapshot {
                durable,
                durable_heads,
                applied,
                applied_heads,
            }
        }
    }

    impl SessionStore for TestStore {
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
            batch: &[VerifiedEvent<PrevVerified>],
            _now_ms: u64,
        ) -> SessionResult<IngestOutcome> {
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
            _role: ReplicaRole,
            _durability_eligible: bool,
        ) -> Result<(), WalIndexError> {
            Ok(())
        }
    }

    fn base_store() -> (TestStore, StoreIdentity, ReplicaId) {
        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let identity = StoreIdentity::new(store_id, StoreEpoch::new(1));
        let replica = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        (TestStore::new(), identity, replica)
    }

    fn make_event(
        store: StoreIdentity,
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: u64,
        prev: Option<Sha256>,
    ) -> EventFrameV1 {
        let body = EventBody {
            envelope_v: 1,
            store,
            namespace: namespace.clone(),
            origin_replica_id: origin,
            origin_seq: Seq1::from_u64(seq).unwrap(),
            event_time_ms: 10,
            txn_id: TxnId::new(Uuid::from_bytes([seq as u8; 16])),
            client_request_id: None,
            kind: EventKindV1::TxnV1(TxnV1 {
                delta: TxnDeltaV1::new(),
                hlc_max: HlcMax {
                    actor_id: ActorId::new("alice").unwrap(),
                    physical_ms: 10,
                    logical: 0,
                },
            }),
        };
        let bytes = encode_event_body_canonical(&body).unwrap();
        let sha = hash_event_body(&bytes);
        EventFrameV1 {
            eid: EventId::new(origin, namespace.clone(), body.origin_seq),
            sha256: sha,
            prev_sha256: prev,
            bytes: EventBytes::<crate::core::Opaque>::from(bytes),
        }
    }

    #[test]
    fn handshake_negotiates_version_and_namespaces() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()];
        config.offered_namespaces = vec![NamespaceId::core()];

        let mut session = Session::new(SessionRole::Inbound, config, limits, admission);

        let peer_store = StoreIdentity::new(identity.store_id, identity.store_epoch);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: peer_store.store_id,
            store_epoch: peer_store.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([3u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()],
            offered_namespaces: vec![NamespaceId::core()],
            seen_durable: BTreeMap::new(),
            seen_durable_heads: None,
            seen_applied: None,
            seen_applied_heads: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };

        let actions = session.handle_message(ReplMessage::Hello(hello), &mut store, 0);
        assert!(matches!(session.phase(), SessionPhase::Streaming));
        assert!(session.peer().is_some());
        assert!(matches!(
            actions.as_slice(),
            [SessionAction::Send(ReplMessage::Welcome(_))]
        ));
        let SessionAction::Send(ReplMessage::Welcome(welcome)) = &actions[0] else {
            panic!("expected welcome");
        };
        assert_eq!(welcome.protocol_version, PROTOCOL_VERSION_V1);
        assert_eq!(welcome.accepted_namespaces, vec![NamespaceId::core()]);
    }

    #[test]
    fn handshake_rejects_version_incompatible() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()];
        config.offered_namespaces = vec![NamespaceId::core()];

        let mut session = Session::new(SessionRole::Inbound, config, limits, admission);
        let hello = Hello {
            protocol_version: 2,
            min_protocol_version: 2,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([9u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()],
            offered_namespaces: vec![NamespaceId::core()],
            seen_durable: BTreeMap::new(),
            seen_durable_heads: None,
            seen_applied: None,
            seen_applied_heads: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };

        let actions = session.handle_message(ReplMessage::Hello(hello), &mut store, 0);
        assert!(matches!(session.phase(), SessionPhase::Closed));
        assert!(session.peer().is_none());
        let SessionAction::Send(ReplMessage::Error(payload)) = &actions[0] else {
            panic!("expected error payload");
        };
        assert_eq!(payload.code, ErrorCode::VersionIncompatible);
    }

    #[test]
    fn events_apply_and_ack() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()];
        config.offered_namespaces = vec![NamespaceId::core()];

        let mut session = Session::new(SessionRole::Inbound, config, limits, admission);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([4u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()],
            offered_namespaces: vec![NamespaceId::core()],
            seen_durable: BTreeMap::new(),
            seen_durable_heads: None,
            seen_applied: None,
            seen_applied_heads: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };
        session.handle_message(ReplMessage::Hello(hello), &mut store, 0);

        let origin = ReplicaId::new(Uuid::from_bytes([5u8; 16]));
        let e1 = make_event(identity, NamespaceId::core(), origin, 1, None);
        let e2 = make_event(identity, NamespaceId::core(), origin, 2, Some(e1.sha256));
        let e2_sha = e2.sha256;

        let actions = session.handle_message(
            ReplMessage::Events(Events {
                events: vec![e1, e2],
            }),
            &mut store,
            10,
        );

        assert!(
            !actions
                .iter()
                .any(|action| matches!(action, SessionAction::Send(ReplMessage::Want(_)))),
            "contiguous batch should not emit WANT"
        );

        let ack = actions
            .iter()
            .find_map(|action| match action {
                SessionAction::Send(ReplMessage::Ack(ack)) => Some(ack),
                _ => None,
            })
            .expect("ack");

        let seq = ack
            .durable
            .get(&NamespaceId::core())
            .and_then(|m| m.get(&origin))
            .copied()
            .unwrap_or(Seq0::ZERO);
        assert_eq!(seq, Seq0::new(2));
        let heads = ack.durable_heads.as_ref().expect("heads");
        let head = heads
            .get(&NamespaceId::core())
            .and_then(|m| m.get(&origin))
            .copied()
            .expect("head");
        assert_eq!(head, e2_sha);
    }

    #[test]
    fn events_buffer_gap_sends_want() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()];
        config.offered_namespaces = vec![NamespaceId::core()];

        let mut session = Session::new(SessionRole::Inbound, config, limits, admission);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([6u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()],
            offered_namespaces: vec![NamespaceId::core()],
            seen_durable: BTreeMap::new(),
            seen_durable_heads: None,
            seen_applied: None,
            seen_applied_heads: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };
        session.handle_message(ReplMessage::Hello(hello), &mut store, 0);

        let origin = ReplicaId::new(Uuid::from_bytes([7u8; 16]));
        let e1 = make_event(identity, NamespaceId::core(), origin, 1, None);
        let e3 = make_event(identity, NamespaceId::core(), origin, 3, Some(e1.sha256));

        let actions = session.handle_message(
            ReplMessage::Events(Events { events: vec![e3] }),
            &mut store,
            10,
        );

        let want = actions
            .iter()
            .find_map(|action| match action {
                SessionAction::Send(ReplMessage::Want(want)) => Some(want),
                _ => None,
            })
            .expect("want");

        let seq = want
            .want
            .get(&NamespaceId::core())
            .and_then(|m| m.get(&origin))
            .copied()
            .unwrap_or(Seq0::ZERO);
        assert_eq!(seq, Seq0::ZERO);
    }

    #[test]
    fn repl_lagged_payload_includes_reason() {
        let limits = Limits::default();
        let payload = repl_lagged_error(ReplRejectReason::GapTimeout, &limits).to_payload();

        assert_eq!(payload.code, ErrorCode::SubscriberLagged);
        assert_eq!(payload.message, "gap_timeout");
        let details = payload
            .details_as::<SubscriberLaggedDetails>()
            .unwrap()
            .expect("lagged details");
        assert_eq!(details.reason, Some(ReplRejectReason::GapTimeout));
        assert_eq!(
            details.max_queue_bytes,
            Some(limits.max_repl_gap_bytes as u64)
        );
        assert_eq!(
            details.max_queue_events,
            Some(limits.max_repl_gap_events as u64)
        );
    }

    #[test]
    fn events_gap_drains_when_missing_event_arrives() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()];
        config.offered_namespaces = vec![NamespaceId::core()];

        let mut session = Session::new(SessionRole::Inbound, config, limits, admission);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([8u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()],
            offered_namespaces: vec![NamespaceId::core()],
            seen_durable: BTreeMap::new(),
            seen_durable_heads: None,
            seen_applied: None,
            seen_applied_heads: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };
        session.handle_message(ReplMessage::Hello(hello), &mut store, 0);

        let origin = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
        let e1 = make_event(identity, NamespaceId::core(), origin, 1, None);
        let e2 = make_event(identity, NamespaceId::core(), origin, 2, Some(e1.sha256));

        let actions = session.handle_message(
            ReplMessage::Events(Events { events: vec![e2] }),
            &mut store,
            10,
        );
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, SessionAction::Send(ReplMessage::Want(_))))
        );

        let actions = session.handle_message(
            ReplMessage::Events(Events { events: vec![e1] }),
            &mut store,
            11,
        );

        assert!(
            !actions
                .iter()
                .any(|action| matches!(action, SessionAction::Send(ReplMessage::Want(_)))),
            "drained batch should not re-emit WANT"
        );

        let ack = actions
            .iter()
            .find_map(|action| match action {
                SessionAction::Send(ReplMessage::Ack(ack)) => Some(ack),
                _ => None,
            })
            .expect("ack");

        let seq = ack
            .durable
            .get(&NamespaceId::core())
            .and_then(|m| m.get(&origin))
            .copied()
            .unwrap_or(Seq0::ZERO);
        assert_eq!(seq, Seq0::new(2));
    }

    #[test]
    fn events_rejects_unaccepted_namespace() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()];
        config.offered_namespaces = vec![NamespaceId::core()];

        let mut session = Session::new(SessionRole::Inbound, config, limits, admission);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([10u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()],
            offered_namespaces: vec![NamespaceId::core()],
            seen_durable: BTreeMap::new(),
            seen_durable_heads: None,
            seen_applied: None,
            seen_applied_heads: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };
        session.handle_message(ReplMessage::Hello(hello), &mut store, 0);

        let origin = ReplicaId::new(Uuid::from_bytes([11u8; 16]));
        let other_namespace = NamespaceId::parse("tmp").unwrap();
        let e1 = make_event(identity, other_namespace, origin, 1, None);

        let actions = session.handle_message(
            ReplMessage::Events(Events { events: vec![e1] }),
            &mut store,
            10,
        );

        assert!(matches!(session.phase(), SessionPhase::Draining));
        assert!(session.peer().is_some());
        let payload = actions
            .iter()
            .find_map(|action| match action {
                SessionAction::Send(ReplMessage::Error(payload)) => Some(payload),
                _ => None,
            })
            .expect("error payload");
        assert_eq!(payload.code, ErrorCode::NamespacePolicyViolation);
    }

    #[test]
    fn events_wrong_store_errors() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()];
        config.offered_namespaces = vec![NamespaceId::core()];

        let mut session = Session::new(SessionRole::Inbound, config, limits, admission);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([8u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()],
            offered_namespaces: vec![NamespaceId::core()],
            seen_durable: BTreeMap::new(),
            seen_durable_heads: None,
            seen_applied: None,
            seen_applied_heads: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };
        session.handle_message(ReplMessage::Hello(hello), &mut store, 0);

        let origin = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
        let wrong_store = StoreIdentity::new(
            StoreId::new(Uuid::from_bytes([10u8; 16])),
            StoreEpoch::new(1),
        );
        let e1 = make_event(wrong_store, NamespaceId::core(), origin, 1, None);

        let actions = session.handle_message(
            ReplMessage::Events(Events { events: vec![e1] }),
            &mut store,
            10,
        );

        let SessionAction::Send(ReplMessage::Error(payload)) = &actions[0] else {
            panic!("expected error payload");
        };
        assert_eq!(payload.code, ErrorCode::WrongStore);
    }

    #[test]
    fn decode_error_indefinite_length_maps_to_non_canonical() {
        let (_store, identity, origin) = base_store();
        let frame = make_event(identity, NamespaceId::core(), origin, 1, None);
        let limits = Limits::default();
        let payload = event_frame_error_payload(
            &frame,
            &limits,
            EventFrameError::Decode(DecodeError::IndefiniteLength),
            None,
            0,
        )
        .to_payload();
        assert_eq!(payload.code, ErrorCode::NonCanonical);
        let details = payload
            .details_as::<NonCanonicalDetails>()
            .unwrap()
            .expect("non canonical details");
        assert_eq!(details.format, "cbor");
    }

    #[test]
    fn decode_error_frame_too_large_maps_to_frame_too_large() {
        let (_store, identity, origin) = base_store();
        let frame = make_event(identity, NamespaceId::core(), origin, 1, None);
        let mut limits = Limits::default();
        limits.max_frame_bytes = 1;

        let payload = event_frame_error_payload(
            &frame,
            &limits,
            EventFrameError::Decode(DecodeError::DecodeLimit("max_wal_record_bytes")),
            None,
            0,
        )
        .to_payload();
        assert_eq!(payload.code, ErrorCode::FrameTooLarge);
        let details = payload
            .details_as::<FrameTooLargeDetails>()
            .unwrap()
            .expect("frame too large details");
        assert_eq!(details.max_frame_bytes, limits.max_frame_bytes as u64);
        assert_eq!(details.got_bytes, frame.bytes.len() as u64);
    }

    #[test]
    fn decode_error_invalid_field_maps_to_invalid_request() {
        let (_store, identity, origin) = base_store();
        let frame = make_event(identity, NamespaceId::core(), origin, 1, None);
        let limits = Limits::default();
        let payload = event_frame_error_payload(
            &frame,
            &limits,
            EventFrameError::Decode(DecodeError::InvalidField {
                field: "store_id",
                reason: "bad".to_string(),
            }),
            None,
            0,
        )
        .to_payload();
        assert_eq!(payload.code, ErrorCode::InvalidRequest);
        let details = payload
            .details_as::<InvalidRequestDetails>()
            .unwrap()
            .expect("invalid request details");
        assert!(
            details
                .reason
                .unwrap_or_default()
                .contains("invalid field store_id")
        );
    }

    #[test]
    fn hash_mismatch_maps_to_hash_mismatch_details() {
        let (_store, identity, origin) = base_store();
        let mut frame = make_event(identity, NamespaceId::core(), origin, 1, None);
        frame.sha256 = Sha256([9u8; 32]);

        let payload = event_frame_error_payload(
            &frame,
            &Limits::default(),
            EventFrameError::HashMismatch,
            None,
            0,
        )
        .to_payload();

        assert_eq!(payload.code, ErrorCode::HashMismatch);
        let details = payload
            .details_as::<HashMismatchDetails>()
            .unwrap()
            .expect("hash mismatch details");
        assert_eq!(details.eid.namespace, frame.eid.namespace);
        assert_eq!(details.eid.origin_replica_id, frame.eid.origin_replica_id);
        assert_eq!(details.eid.origin_seq, frame.eid.origin_seq.get());
        assert_eq!(
            details.expected_sha256,
            hex::encode(hash_event_body(&frame.bytes).as_bytes())
        );
        assert_eq!(
            details.got_sha256,
            hex::encode(frame.sha256.as_bytes())
        );
    }

    #[test]
    fn prev_mismatch_maps_to_prev_sha_mismatch_details() {
        let (_store, identity, origin) = base_store();
        let expected_prev = Sha256([1u8; 32]);
        let got_prev = Sha256([2u8; 32]);
        let frame = make_event(
            identity,
            NamespaceId::core(),
            origin,
            2,
            Some(got_prev),
        );

        let payload = event_frame_error_payload(
            &frame,
            &Limits::default(),
            EventFrameError::PrevMismatch,
            Some(expected_prev),
            1,
        )
        .to_payload();

        assert_eq!(payload.code, ErrorCode::PrevShaMismatch);
        let details = payload
            .details_as::<PrevShaMismatchDetails>()
            .unwrap()
            .expect("prev mismatch details");
        assert_eq!(details.eid.namespace, frame.eid.namespace);
        assert_eq!(details.eid.origin_replica_id, frame.eid.origin_replica_id);
        assert_eq!(details.eid.origin_seq, frame.eid.origin_seq.get());
        assert_eq!(
            details.expected_prev_sha256,
            hex::encode(expected_prev.as_bytes())
        );
        assert_eq!(details.got_prev_sha256, hex::encode(got_prev.as_bytes()));
        assert_eq!(details.head_seq, 1);
    }
}
