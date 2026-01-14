//! Replication session state machine.

use std::collections::{BTreeMap, BTreeSet};

use crate::core::error::details::{
    CorruptionDetails, InternalErrorDetails, InvalidRequestDetails, ReplicaIdCollisionDetails,
    StoreEpochMismatchDetails, SubscriberLaggedDetails, VersionIncompatibleDetails,
    WrongStoreDetails,
};
use crate::core::{
    Applied, Durable, ErrorCode, ErrorPayload, EventFrameError, EventId, EventShaLookup,
    EventShaLookupError, HeadStatus, Limits, NamespaceId, PrevVerified, ReplicaId, Seq0, Seq1,
    Sha256, StoreEpoch, StoreId, StoreIdentity, VerifiedEvent, Watermark, verify_event_frame,
};
use crate::daemon::admission::AdmissionController;

use super::gap_buffer::{GapBufferByNsOrigin, IngestDecision};
use super::proto::{
    Ack, Capabilities, Events, Hello, ReplMessage, Want, WatermarkHeads, WatermarkMap,
    PROTOCOL_VERSION_V1,
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

    fn lookup_event_sha(
        &self,
        eid: &EventId,
    ) -> Result<Option<Sha256>, EventShaLookupError>;

    fn ingest_remote_batch(
        &mut self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        batch: &[VerifiedEvent<PrevVerified>],
        _now_ms: u64,
    ) -> SessionResult<IngestOutcome>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum SessionAction {
    Send(ReplMessage),
    Close {
        error: Option<ErrorPayload>,
    },
    PeerAck(Ack),
    PeerWant(Want),
    PeerError(ErrorPayload),
}

type WatermarkState<K> = BTreeMap<NamespaceId, BTreeMap<ReplicaId, Watermark<K>>>;
type SessionResult<T> = Result<T, Box<ErrorPayload>>;

#[derive(Debug)]
pub struct Session {
    role: SessionRole,
    phase: SessionPhase,
    config: SessionConfig,
    peer: Option<SessionPeer>,
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
            phase: SessionPhase::Connecting,
            config,
            peer: None,
            gap_buffer: GapBufferByNsOrigin::new(limits.clone()),
            limits,
            admission,
            durable: BTreeMap::new(),
            applied: BTreeMap::new(),
            next_nonce: 1,
        }
    }

    pub fn phase(&self) -> SessionPhase {
        self.phase
    }

    pub fn peer(&self) -> Option<&SessionPeer> {
        self.peer.as_ref()
    }

    pub fn begin_handshake(
        &mut self,
        store: &impl SessionStore,
        now_ms: u64,
    ) -> Option<SessionAction> {
        if self.role != SessionRole::Outbound || self.phase != SessionPhase::Connecting {
            return None;
        }
        let hello = self.build_hello(store, now_ms);
        self.phase = SessionPhase::Handshaking;
        Some(SessionAction::Send(ReplMessage::Hello(hello)))
    }

    pub fn handle_message(
        &mut self,
        msg: ReplMessage,
        store: &mut impl SessionStore,
        now_ms: u64,
    ) -> Vec<SessionAction> {
        match msg {
            ReplMessage::Hello(msg) => self.handle_hello(msg, store, now_ms),
            ReplMessage::Welcome(msg) => self.handle_welcome(msg, store, now_ms),
            ReplMessage::Events(msg) => self.handle_events(msg, store, now_ms),
            ReplMessage::Ack(msg) => self.handle_ack(msg),
            ReplMessage::Want(msg) => self.handle_want(msg),
            ReplMessage::Error(msg) => self.handle_peer_error(msg),
        }
    }

    pub fn mark_closed(&mut self) {
        self.phase = SessionPhase::Closed;
    }

    fn handle_ack(&mut self, ack: Ack) -> Vec<SessionAction> {
        if self.phase != SessionPhase::Streaming {
            return self.invalid_request("ACK before handshake");
        }
        vec![SessionAction::PeerAck(ack)]
    }

    fn handle_want(&mut self, want: Want) -> Vec<SessionAction> {
        if self.phase != SessionPhase::Streaming {
            return self.invalid_request("WANT before handshake");
        }
        vec![SessionAction::PeerWant(want)]
    }

    fn handle_peer_error(&mut self, payload: ErrorPayload) -> Vec<SessionAction> {
        self.phase = SessionPhase::Draining;
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
        now_ms: u64,
    ) -> Vec<SessionAction> {
        if self.role != SessionRole::Inbound || self.phase != SessionPhase::Connecting {
            return self.invalid_request("unexpected HELLO");
        }

        if let Err(payload) =
            self.validate_peer_store(hello.store_id, hello.store_epoch, hello.sender_replica_id)
        {
            return self.fail(*payload);
        }

        if hello.min_protocol_version > hello.protocol_version {
            return self.invalid_request("min_protocol_version exceeds protocol_version");
        }

        let negotiated =
            match negotiate_version(self.config.protocol, hello.protocol_version, hello.min_protocol_version) {
                Ok(version) => version,
                Err(err) => return self.fail(version_incompatible_payload(&err)),
            };

        let accepted_namespaces =
            intersect_namespaces(&self.config.offered_namespaces, &hello.requested_namespaces);
        let incoming_namespaces =
            intersect_namespaces(&self.config.requested_namespaces, &hello.offered_namespaces);

        let snapshot = store.watermark_snapshot(&accepted_namespaces);
        let welcome = self.build_welcome(
            negotiated,
            &hello,
            snapshot,
            accepted_namespaces.clone(),
        );

        self.seed_watermarks(store, &incoming_namespaces);
        self.peer = Some(SessionPeer {
            replica_id: hello.sender_replica_id,
            store_epoch: hello.store_epoch,
            protocol_version: negotiated,
            max_frame_bytes: std::cmp::min(self.config.max_frame_bytes, hello.max_frame_bytes),
            incoming_namespaces,
            accepted_namespaces,
            live_stream_enabled: welcome.live_stream_enabled,
        });
        self.phase = SessionPhase::Streaming;

        vec![SessionAction::Send(ReplMessage::Welcome(welcome))]
    }

    fn handle_welcome(
        &mut self,
        welcome: super::proto::Welcome,
        store: &mut impl SessionStore,
        _now_ms: u64,
    ) -> Vec<SessionAction> {
        if self.role != SessionRole::Outbound || self.phase != SessionPhase::Handshaking {
            return self.invalid_request("unexpected WELCOME");
        }

        if let Err(payload) =
            self.validate_peer_store(welcome.store_id, welcome.store_epoch, welcome.receiver_replica_id)
        {
            return self.fail(*payload);
        }

        if welcome.protocol_version < self.config.protocol.min
            || welcome.protocol_version > self.config.protocol.max
        {
            return self.fail(version_incompatible_payload(&ProtocolIncompatible {
                local_min: self.config.protocol.min,
                local_max: self.config.protocol.max,
                peer_min: welcome.protocol_version,
                peer_max: welcome.protocol_version,
            }));
        }

        let incoming_namespaces =
            intersect_namespaces(&self.config.requested_namespaces, &welcome.accepted_namespaces);
        self.seed_watermarks(store, &incoming_namespaces);

        self.peer = Some(SessionPeer {
            replica_id: welcome.receiver_replica_id,
            store_epoch: welcome.store_epoch,
            protocol_version: welcome.protocol_version,
            max_frame_bytes: std::cmp::min(self.config.max_frame_bytes, welcome.max_frame_bytes),
            incoming_namespaces,
            accepted_namespaces: welcome.accepted_namespaces.clone(),
            live_stream_enabled: welcome.live_stream_enabled,
        });
        self.phase = SessionPhase::Streaming;

        Vec::new()
    }

    fn handle_events(
        &mut self,
        events: Events,
        store: &mut impl SessionStore,
        now_ms: u64,
    ) -> Vec<SessionAction> {
        if self.phase != SessionPhase::Streaming {
            return self.invalid_request("EVENTS before handshake");
        }

        let total_bytes: u64 = events
            .events
            .iter()
            .map(|frame| frame.bytes.len() as u64)
            .sum();
        let event_count = events.events.len() as u64;
        let permit = match self.admission.try_admit_repl_ingest(total_bytes, event_count) {
            Ok(permit) => permit,
            Err(err) => return self.fail(err.to_error_payload()),
        };

        let mut ack_updates: WatermarkState<Durable> = BTreeMap::new();
        let mut applied_updates: WatermarkState<Applied> = BTreeMap::new();
        let mut wants: WatermarkMap = BTreeMap::new();

        for frame in events.events {
            let namespace = frame.eid.namespace.clone();
            let origin = frame.eid.origin_replica_id;
            let durable = self.durable_for(&namespace, &origin);

            let expected_prev = match self.expected_prev_head(&namespace, &origin, durable, frame.eid.origin_seq) {
                Ok(prev) => prev,
                Err(payload) => return self.fail(*payload),
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
                    Err(err) => return self.fail(event_frame_error_payload(err)),
                }
            };

            match self
                .gap_buffer
                .ingest(namespace.clone(), origin, durable, verified, now_ms)
            {
                IngestDecision::ForwardContiguousBatch(batch) => {
                    let outcome =
                        match store.ingest_remote_batch(&namespace, &origin, &batch, now_ms) {
                            Ok(outcome) => outcome,
                            Err(payload) => return self.fail(*payload),
                        };

                    if let Err(err) = self
                        .gap_buffer
                        .advance_durable_batch(&namespace, &origin, &batch)
                    {
                        return self.fail(internal_error(format!(
                            "gap buffer watermark advance failed: {err}"
                        )));
                    }

                    update_watermark(&mut self.durable, &namespace, &origin, outcome.durable);
                    update_watermark(&mut self.applied, &namespace, &origin, outcome.applied);

                    update_watermark(&mut ack_updates, &namespace, &origin, outcome.durable);
                    update_watermark(&mut applied_updates, &namespace, &origin, outcome.applied);
                }
                IngestDecision::BufferedNeedWant { want_from } => {
                    insert_want(&mut wants, namespace, origin, want_from);
                }
                IngestDecision::DuplicateNoop => {}
                IngestDecision::Reject { code } => {
                    return self.fail(repl_lagged_payload(code, &self.limits));
                }
            }
        }

        drop(permit);

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

    fn seed_watermarks(&mut self, store: &impl SessionStore, namespaces: &[NamespaceId]) {
        let snapshot = store.watermark_snapshot(namespaces);
        self.durable = watermark_state_from_snapshot(&snapshot.durable, Some(&snapshot.durable_heads));
        self.applied =
            watermark_state_from_snapshot(&snapshot.applied, Some(&snapshot.applied_heads));
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
        namespace: &NamespaceId,
        origin: &ReplicaId,
        durable: Watermark<Durable>,
        seq: Seq1,
    ) -> SessionResult<Option<Sha256>> {
        if seq.get() != durable.seq().get().saturating_add(1) {
            return Ok(None);
        }

        match durable.head() {
            HeadStatus::Genesis => Ok(None),
            HeadStatus::Known(head) => Ok(Some(Sha256(head))),
            HeadStatus::Unknown => Err(Box::new(internal_error(format!(
                "missing durable head for {namespace} {origin} seq {}",
                durable.seq().get()
            )))),
        }
    }

    fn validate_peer_store(
        &self,
        peer_store_id: StoreId,
        peer_epoch: StoreEpoch,
        peer_replica_id: ReplicaId,
    ) -> SessionResult<()> {
        if peer_replica_id == self.config.local_replica_id {
            return Err(Box::new(replica_id_collision_payload(peer_replica_id)));
        }
        if peer_store_id != self.config.local_store.store_id {
            return Err(wrong_store_payload(
                self.config.local_store.store_id,
                peer_store_id,
            ).into());
        }
        if peer_epoch != self.config.local_store.store_epoch {
            return Err(Box::new(store_epoch_mismatch_payload(
                self.config.local_store.store_id,
                self.config.local_store.store_epoch,
                peer_epoch,
            )));
        }
        Ok(())
    }

    fn invalid_request(&mut self, reason: impl Into<String>) -> Vec<SessionAction> {
        let payload = invalid_request_payload(reason);
        self.fail(payload)
    }

    fn fail(&mut self, payload: ErrorPayload) -> Vec<SessionAction> {
        self.phase = SessionPhase::Draining;
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
    fn lookup_event_sha(
        &self,
        eid: &EventId,
    ) -> Result<Option<Sha256>, EventShaLookupError> {
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
        if heads.is_empty() {
            None
        } else {
            Some(heads)
        }
    }
}

fn watermark_state_from_snapshot<K>(
    map: &WatermarkMap,
    heads: Option<&WatermarkHeads>,
) -> WatermarkState<K> {
        let mut out: WatermarkState<K> = BTreeMap::new();
    for (namespace, origins) in map {
        let ns_map = out.entry(namespace.clone()).or_default();
        for (origin, seq) in origins {
            let head = heads
                .and_then(|heads| heads.get(namespace))
                .and_then(|origins| origins.get(origin))
                .map(|sha| HeadStatus::Known(sha.0))
                .unwrap_or_else(|| {
                    if *seq == 0 {
                        HeadStatus::Genesis
                    } else {
                        HeadStatus::Unknown
                    }
                });
            if let Ok(watermark) = Watermark::new(Seq0::new(*seq), head) {
                ns_map.insert(*origin, watermark);
            }
        }
    }
    out
}

fn insert_want(
    want: &mut WatermarkMap,
    namespace: NamespaceId,
    origin: ReplicaId,
    from: Seq0,
) {
    let ns = want.entry(namespace).or_default();
    ns.entry(origin)
        .and_modify(|seq| {
            if from.get() < *seq {
                *seq = from.get();
            }
        })
        .or_insert_with(|| from.get());
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
        applied: if applied.is_empty() { None } else { Some(applied) },
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
            ns_map.insert(*origin, watermark.seq().get());
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

fn repl_lagged_payload(reason: String, limits: &Limits) -> ErrorPayload {
    ErrorPayload::new(ErrorCode::SubscriberLagged, reason, true).with_details(
        SubscriberLaggedDetails {
            max_queue_bytes: Some(limits.max_repl_gap_bytes as u64),
            max_queue_events: Some(limits.max_repl_gap_events as u64),
        },
    )
}

fn wrong_store_payload(expected: StoreId, got: StoreId) -> ErrorPayload {
    ErrorPayload::new(ErrorCode::WrongStore, "wrong store id", false)
        .with_details(WrongStoreDetails {
            expected_store_id: expected,
            got_store_id: got,
        })
}

fn store_epoch_mismatch_payload(
    store_id: StoreId,
    expected: StoreEpoch,
    got: StoreEpoch,
) -> ErrorPayload {
    ErrorPayload::new(ErrorCode::StoreEpochMismatch, "store epoch mismatch", false)
        .with_details(StoreEpochMismatchDetails {
            store_id,
            expected_epoch: expected.get(),
            got_epoch: got.get(),
        })
}

fn replica_id_collision_payload(replica_id: ReplicaId) -> ErrorPayload {
    ErrorPayload::new(ErrorCode::ReplicaIdCollision, "replica_id collision", false)
        .with_details(ReplicaIdCollisionDetails { replica_id })
}

fn version_incompatible_payload(err: &ProtocolIncompatible) -> ErrorPayload {
    ErrorPayload::new(ErrorCode::VersionIncompatible, "protocol versions incompatible", false)
        .with_details(VersionIncompatibleDetails {
            local_min: err.local_min,
            local_max: err.local_max,
            peer_min: err.peer_min,
            peer_max: err.peer_max,
        })
}

fn invalid_request_payload(reason: impl Into<String>) -> ErrorPayload {
    let reason = reason.into();
    ErrorPayload::new(ErrorCode::InvalidRequest, reason.clone(), false).with_details(
        InvalidRequestDetails {
            field: Some("type".to_string()),
            reason: Some(reason),
        },
    )
}

fn internal_error(message: impl Into<String>) -> ErrorPayload {
    ErrorPayload::new(ErrorCode::InternalError, "internal error", false).with_details(
        InternalErrorDetails {
            trace_id: None,
            component: Some(message.into()),
        },
    )
}

fn event_frame_error_payload(err: EventFrameError) -> ErrorPayload {
    match err {
        EventFrameError::WrongStore { expected, got } => {
            if expected.store_id != got.store_id {
                wrong_store_payload(expected.store_id, got.store_id)
            } else {
                store_epoch_mismatch_payload(
                    expected.store_id,
                    expected.store_epoch,
                    got.store_epoch,
                )
            }
        }
        EventFrameError::FrameMismatch => ErrorPayload::new(
            ErrorCode::InvalidRequest,
            "event id does not match decoded body",
            false,
        )
        .with_details(InvalidRequestDetails {
            field: Some("event_id".to_string()),
            reason: Some("event_id does not match decoded body".to_string()),
        }),
        EventFrameError::HashMismatch => ErrorPayload::new(
            ErrorCode::Corruption,
            "event sha256 mismatch",
            false,
        )
        .with_details(CorruptionDetails {
            reason: "event sha256 mismatch".to_string(),
        }),
        EventFrameError::PrevMismatch => ErrorPayload::new(
            ErrorCode::Corruption,
            "prev_sha256 mismatch",
            false,
        )
        .with_details(CorruptionDetails {
            reason: "prev_sha256 mismatch".to_string(),
        }),
        EventFrameError::Validation(err) => ErrorPayload::new(
            ErrorCode::InvalidRequest,
            err.to_string(),
            false,
        )
        .with_details(InvalidRequestDetails {
            field: None,
            reason: Some(err.to_string()),
        }),
        EventFrameError::Decode(err) => ErrorPayload::new(
            ErrorCode::InvalidRequest,
            err.to_string(),
            false,
        )
        .with_details(InvalidRequestDetails {
            field: None,
            reason: Some(err.to_string()),
        }),
        EventFrameError::Lookup(err) => internal_error(err.to_string()),
        EventFrameError::Equivocation => {
            ErrorPayload::new(ErrorCode::Equivocation, "equivocation detected", false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    use crate::core::{
        ActorId, EventBody, EventBytes, EventFrameV1, EventKindV1, HlcMax, NamespaceId, ReplicaId,
        Seq1, StoreEpoch, StoreId, StoreIdentity, TxnDeltaV1, TxnId,
        encode_event_body_canonical, hash_event_body,
    };

    #[derive(Clone, Debug)]
    struct TestStore {
        identity: StoreIdentity,
        replica_id: ReplicaId,
        lookup: BTreeMap<EventId, Sha256>,
        durable: WatermarkState<Durable>,
        applied: WatermarkState<Applied>,
    }

    impl TestStore {
        fn new(identity: StoreIdentity, replica_id: ReplicaId) -> Self {
            Self {
                identity,
                replica_id,
                lookup: BTreeMap::new(),
                durable: BTreeMap::new(),
                applied: BTreeMap::new(),
            }
        }

        fn set_durable(
            &mut self,
            namespace: NamespaceId,
            origin: ReplicaId,
            seq: u64,
            head: HeadStatus,
        ) {
            let wm = Watermark::new(Seq0::new(seq), head).expect("watermark");
            self.durable.entry(namespace).or_default().insert(origin, wm);
        }

        fn snapshot_for(&self, namespaces: &[NamespaceId]) -> WatermarkSnapshot {
            let mut durable = BTreeMap::new();
            let mut durable_heads = BTreeMap::new();
            let mut applied = BTreeMap::new();
            let mut applied_heads = BTreeMap::new();

            for ns in namespaces {
                if let Some(origins) = self.durable.get(ns) {
                    let ns_map = durable.entry(ns.clone()).or_default();
                    let ns_heads = durable_heads.entry(ns.clone()).or_default();
                    for (origin, wm) in origins {
                        ns_map.insert(*origin, wm.seq().get());
                        if let HeadStatus::Known(head) = wm.head() {
                            ns_heads.insert(*origin, Sha256(head));
                        }
                    }
                }
                if let Some(origins) = self.applied.get(ns) {
                    let ns_map = applied.entry(ns.clone()).or_default();
                    let ns_heads = applied_heads.entry(ns.clone()).or_default();
                    for (origin, wm) in origins {
                        ns_map.insert(*origin, wm.seq().get());
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

        fn lookup_event_sha(
            &self,
            eid: &EventId,
        ) -> Result<Option<Sha256>, EventShaLookupError> {
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
    }

    fn base_store() -> (TestStore, StoreIdentity, ReplicaId) {
        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let identity = StoreIdentity::new(store_id, StoreEpoch::new(1));
        let replica = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        (TestStore::new(identity, replica), identity, replica)
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
            kind: EventKindV1::TxnV1,
            delta: TxnDeltaV1::new(),
            hlc_max: Some(HlcMax {
                actor_id: ActorId::new("alice").unwrap(),
                physical_ms: 10,
                logical: 0,
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
        assert!(matches!(actions.as_slice(), [SessionAction::Send(ReplMessage::Welcome(_))]));
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
        assert!(matches!(session.phase(), SessionPhase::Draining));
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

        let actions = session.handle_message(
            ReplMessage::Events(Events {
                events: vec![e1, e2],
            }),
            &mut store,
            10,
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
            .unwrap_or_default();
        assert_eq!(seq, 2);
        let heads = ack.durable_heads.as_ref().expect("heads");
        let head = heads
            .get(&NamespaceId::core())
            .and_then(|m| m.get(&origin))
            .copied()
            .expect("head");
        assert_eq!(head, e2.sha256);
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
            .unwrap_or_default();
        assert_eq!(seq, 0);
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
        let wrong_store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([10u8; 16])), StoreEpoch::new(1));
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
}
