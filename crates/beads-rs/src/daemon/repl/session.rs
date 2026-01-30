//! Replication session state machine.

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use thiserror::Error;

use crate::core::error::details::{
    EventIdDetails, FrameTooLargeDetails, HashMismatchDetails, InternalErrorDetails,
    InvalidRequestDetails, NamespacePolicyViolationDetails, NonCanonicalDetails, OverloadedDetails,
    PrevShaMismatchDetails, ReplRejectReason, ReplicaIdCollisionDetails, StoreEpochMismatchDetails,
    SubscriberLaggedDetails, VersionIncompatibleDetails, WrongStoreDetails,
};
use crate::core::{
    Applied, DecodeError, Durable, ErrorPayload, EventFrameError, EventFrameV1, EventId,
    EventShaLookup, EventShaLookupError, HeadStatus, Limits, NamespaceId, ProtocolErrorCode,
    ReplicaId, Seq0, Seq1, Sha256, StoreEpoch, StoreId, StoreIdentity, Watermark, hash_event_body,
    verify_event_frame,
};
use crate::daemon::admission::{AdmissionController, AdmissionRejection};
use crate::daemon::metrics;
use crate::daemon::wal::{ReplicaDurabilityRole, WalIndexError};

use super::ContiguousBatch;
use super::error::{ReplError, ReplErrorDetails};
use super::frame::NegotiatedFrameLimit;
use super::gap_buffer::{DrainError, GapBufferByNsOrigin, IngestDecision};
use super::proto::{
    Ack, Capabilities, Hello, NamespaceSet, PROTOCOL_VERSION_V1, Ping, Pong, ReplMessage, Want,
    WatermarkMap, WatermarkState, WireEvents, WireReplMessage,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedAck {
    durable: WatermarkState<Durable>,
    applied: Option<WatermarkState<Applied>>,
}

impl ValidatedAck {
    pub fn durable(&self) -> &WatermarkState<Durable> {
        &self.durable
    }

    pub fn applied(&self) -> Option<&WatermarkState<Applied>> {
        self.applied.as_ref()
    }
}

#[derive(Debug, Clone)]
struct AllowedNamespaces(BTreeSet<NamespaceId>);

impl AllowedNamespaces {
    fn new(namespaces: &[NamespaceId]) -> Self {
        let mut set = BTreeSet::new();
        set.extend(namespaces.iter().cloned());
        Self(set)
    }

    fn validate_ack(&self, ack: Ack) -> Result<ValidatedAck, ReplError> {
        if let Some(namespace) = ack
            .durable
            .keys()
            .find(|namespace| !self.0.contains(*namespace))
        {
            return Err(namespace_policy_violation_error(namespace));
        }
        if let Some(applied) = ack.applied.as_ref()
            && let Some(namespace) = applied
                .keys()
                .find(|namespace| !self.0.contains(*namespace))
        {
            return Err(namespace_policy_violation_error(namespace));
        }

        Ok(ValidatedAck {
            durable: ack.durable,
            applied: ack.applied,
        })
    }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[error("protocol range invalid: min {min} greater than max {max}")]
pub struct ProtocolRangeError {
    min: u32,
    max: u32,
}

impl ProtocolRange {
    pub fn new(min: u32, max: u32) -> Result<Self, ProtocolRangeError> {
        if min <= max {
            Ok(Self { min, max })
        } else {
            Err(ProtocolRangeError { min, max })
        }
    }

    pub fn exact(version: u32) -> Self {
        Self {
            min: version,
            max: version,
        }
    }

    pub fn range(min: u32, max: u32) -> Result<Self, ProtocolRangeError> {
        Self::new(min, max)
    }
}

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub local_store: StoreIdentity,
    pub local_replica_id: ReplicaId,
    pub protocol: ProtocolRange,
    pub requested_namespaces: NamespaceSet,
    pub offered_namespaces: NamespaceSet,
    pub capabilities: Capabilities,
    pub max_frame_bytes: u32,
}

impl SessionConfig {
    pub fn new(local_store: StoreIdentity, local_replica_id: ReplicaId, limits: &Limits) -> Self {
        Self {
            local_store,
            local_replica_id,
            protocol: ProtocolRange::exact(PROTOCOL_VERSION_V1),
            requested_namespaces: NamespaceSet::default(),
            offered_namespaces: NamespaceSet::default(),
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
    pub incoming_namespaces: NamespaceSet,
    pub accepted_namespaces: NamespaceSet,
    pub live_stream_enabled: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct Inbound;

#[derive(Clone, Copy, Debug)]
pub struct Outbound;

#[derive(Clone, Copy, Debug)]
pub struct Connecting;

#[derive(Clone, Copy, Debug)]
pub struct Handshaking;

#[derive(Clone, Copy, Debug)]
pub struct LiveStream;

#[derive(Clone, Copy, Debug)]
pub struct SnapshotOnly;

#[derive(Clone, Debug)]
pub struct Streaming<Mode> {
    peer: SessionPeer,
    mode: PhantomData<Mode>,
}

impl<Mode> Streaming<Mode> {
    fn new(peer: SessionPeer) -> Self {
        Self {
            peer,
            mode: PhantomData,
        }
    }
}

pub type StreamingLive = Streaming<LiveStream>;
pub type StreamingSnapshot = Streaming<SnapshotOnly>;

#[derive(Clone, Debug)]
pub struct Draining {
    peer: SessionPeer,
}

#[derive(Clone, Copy, Debug)]
pub struct Closed;

pub trait PhaseMarker {
    const PHASE: SessionPhase;
}

impl PhaseMarker for Connecting {
    const PHASE: SessionPhase = SessionPhase::Connecting;
}

impl PhaseMarker for Handshaking {
    const PHASE: SessionPhase = SessionPhase::Handshaking;
}

impl<Mode> PhaseMarker for Streaming<Mode> {
    const PHASE: SessionPhase = SessionPhase::Streaming;
}

impl PhaseMarker for Draining {
    const PHASE: SessionPhase = SessionPhase::Draining;
}

impl PhaseMarker for Closed {
    const PHASE: SessionPhase = SessionPhase::Closed;
}

pub trait PhasePeer {
    fn peer(&self) -> &SessionPeer;
}

impl<Mode> PhasePeer for Streaming<Mode> {
    fn peer(&self) -> &SessionPeer {
        &self.peer
    }
}

impl PhasePeer for Draining {
    fn peer(&self) -> &SessionPeer {
        &self.peer
    }
}

pub trait PhasePeerOpt {
    fn into_peer(self) -> Option<SessionPeer>;
}

impl PhasePeerOpt for Connecting {
    fn into_peer(self) -> Option<SessionPeer> {
        None
    }
}

impl PhasePeerOpt for Handshaking {
    fn into_peer(self) -> Option<SessionPeer> {
        None
    }
}

impl<Mode> PhasePeerOpt for Streaming<Mode> {
    fn into_peer(self) -> Option<SessionPeer> {
        Some(self.peer)
    }
}

impl PhasePeerOpt for Draining {
    fn into_peer(self) -> Option<SessionPeer> {
        Some(self.peer)
    }
}

impl PhasePeerOpt for Closed {
    fn into_peer(self) -> Option<SessionPeer> {
        None
    }
}

trait PhaseWire {
    fn frame_limit(&self, config: &SessionConfig) -> usize;
    fn wire_version(&self, _config: &SessionConfig) -> u32;
}

impl PhaseWire for Connecting {
    fn frame_limit(&self, config: &SessionConfig) -> usize {
        config.max_frame_bytes as usize
    }

    fn wire_version(&self, _config: &SessionConfig) -> u32 {
        PROTOCOL_VERSION_V1
    }
}

impl PhaseWire for Handshaking {
    fn frame_limit(&self, config: &SessionConfig) -> usize {
        config.max_frame_bytes as usize
    }

    fn wire_version(&self, _config: &SessionConfig) -> u32 {
        PROTOCOL_VERSION_V1
    }
}

impl<Mode> PhaseWire for Streaming<Mode> {
    fn frame_limit(&self, _config: &SessionConfig) -> usize {
        self.peer.max_frame_bytes as usize
    }

    fn wire_version(&self, _config: &SessionConfig) -> u32 {
        self.peer.protocol_version
    }
}

impl PhaseWire for Draining {
    fn frame_limit(&self, _config: &SessionConfig) -> usize {
        self.peer.max_frame_bytes as usize
    }

    fn wire_version(&self, _config: &SessionConfig) -> u32 {
        self.peer.protocol_version
    }
}

impl PhaseWire for Closed {
    fn frame_limit(&self, config: &SessionConfig) -> usize {
        config.max_frame_bytes as usize
    }

    fn wire_version(&self, _config: &SessionConfig) -> u32 {
        PROTOCOL_VERSION_V1
    }
}

trait PhaseWrap: PhaseMarker + PhasePeer + PhasePeerOpt + PhaseWire + Sized {
    fn wrap<R>(session: Session<R, Self>) -> SessionState<R>;
}

impl PhaseWrap for StreamingLive {
    fn wrap<R>(session: Session<R, StreamingLive>) -> SessionState<R> {
        SessionState::StreamingLive(session)
    }
}

impl PhaseWrap for StreamingSnapshot {
    fn wrap<R>(session: Session<R, StreamingSnapshot>) -> SessionState<R> {
        SessionState::StreamingSnapshot(session)
    }
}

impl PhaseWrap for Draining {
    fn wrap<R>(session: Session<R, Draining>) -> SessionState<R> {
        SessionState::Draining(session)
    }
}

pub enum SessionState<R> {
    Connecting(Session<R, Connecting>),
    Handshaking(Session<R, Handshaking>),
    StreamingLive(Session<R, StreamingLive>),
    StreamingSnapshot(Session<R, StreamingSnapshot>),
    Draining(Session<R, Draining>),
    Closed(Session<R, Closed>),
}

pub type InboundConnecting = Session<Inbound, Connecting>;
pub type OutboundConnecting = Session<Outbound, Connecting>;

pub(crate) trait SessionWire {
    fn wire_version(&self) -> u32;
    fn frame_limit(&self) -> usize;
}

impl<R> SessionState<R> {
    #[allow(dead_code)]
    pub fn phase(&self) -> SessionPhase {
        match self {
            SessionState::Connecting(_) => SessionPhase::Connecting,
            SessionState::Handshaking(_) => SessionPhase::Handshaking,
            SessionState::StreamingLive(_) | SessionState::StreamingSnapshot(_) => {
                SessionPhase::Streaming
            }
            SessionState::Draining(_) => SessionPhase::Draining,
            SessionState::Closed(_) => SessionPhase::Closed,
        }
    }

    pub(crate) fn wire_version(&self) -> u32 {
        match self {
            SessionState::Connecting(session) => session.wire_version(),
            SessionState::Handshaking(session) => session.wire_version(),
            SessionState::StreamingLive(session) => session.wire_version(),
            SessionState::StreamingSnapshot(session) => session.wire_version(),
            SessionState::Draining(session) => session.wire_version(),
            SessionState::Closed(session) => session.wire_version(),
        }
    }

    pub(crate) fn frame_limit(&self) -> usize {
        match self {
            SessionState::Connecting(session) => session.frame_limit(),
            SessionState::Handshaking(session) => session.frame_limit(),
            SessionState::StreamingLive(session) => session.frame_limit(),
            SessionState::StreamingSnapshot(session) => session.frame_limit(),
            SessionState::Draining(session) => session.frame_limit(),
            SessionState::Closed(session) => session.frame_limit(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn close(self) -> SessionState<R> {
        match self {
            SessionState::Connecting(session) => SessionState::Closed(session.with_phase(Closed)),
            SessionState::Handshaking(session) => SessionState::Closed(session.with_phase(Closed)),
            SessionState::StreamingLive(session) => {
                SessionState::Closed(session.with_phase(Closed))
            }
            SessionState::StreamingSnapshot(session) => {
                SessionState::Closed(session.with_phase(Closed))
            }
            SessionState::Draining(session) => SessionState::Closed(session.with_phase(Closed)),
            SessionState::Closed(session) => SessionState::Closed(session),
        }
    }
}

impl<R> SessionWire for SessionState<R> {
    fn wire_version(&self) -> u32 {
        SessionState::wire_version(self)
    }

    fn frame_limit(&self) -> usize {
        SessionState::frame_limit(self)
    }
}

#[derive(Clone, Debug)]
pub struct WatermarkSnapshot {
    pub durable: WatermarkState<Durable>,
    pub applied: WatermarkState<Applied>,
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
        batch: &ContiguousBatch,
        _now_ms: u64,
    ) -> SessionResult<IngestOutcome>;

    fn update_replica_liveness(
        &mut self,
        replica_id: ReplicaId,
        last_seen_ms: u64,
        last_handshake_ms: u64,
        role: ReplicaDurabilityRole,
    ) -> Result<(), WalIndexError>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum SessionAction {
    Send(ReplMessage),
    Close { error: Option<ErrorPayload> },
    PeerAck(ValidatedAck),
    PeerWant(Want),
    PeerError(ErrorPayload),
}

type SessionResult<T> = Result<T, ReplError>;

struct IngestUpdates<'a> {
    ack_updates: &'a mut WatermarkState<Durable>,
    applied_updates: &'a mut WatermarkState<Applied>,
}

#[derive(Debug)]
pub struct Session<R, P> {
    role: PhantomData<R>,
    phase: P,
    config: SessionConfig,
    limits: Limits,
    admission: AdmissionController,
    gap_buffer: GapBufferByNsOrigin,
    durable: WatermarkState<Durable>,
    applied: WatermarkState<Applied>,
    next_nonce: u64,
}

impl<R> Session<R, Connecting> {
    pub fn new(config: SessionConfig, limits: Limits, admission: AdmissionController) -> Self {
        Self {
            role: PhantomData,
            phase: Connecting,
            config,
            gap_buffer: GapBufferByNsOrigin::new(limits.clone()),
            limits,
            admission,
            durable: BTreeMap::new(),
            applied: BTreeMap::new(),
            next_nonce: 1,
        }
    }
}

impl<R, P> Session<R, P> {
    fn with_phase<Next>(self, phase: Next) -> Session<R, Next> {
        Session {
            role: PhantomData,
            phase,
            config: self.config,
            limits: self.limits,
            admission: self.admission,
            gap_buffer: self.gap_buffer,
            durable: self.durable,
            applied: self.applied,
            next_nonce: self.next_nonce,
        }
    }

    fn with_streaming(self, peer: SessionPeer) -> SessionState<R> {
        if peer.live_stream_enabled {
            SessionState::StreamingLive(self.with_phase(Streaming::<LiveStream>::new(peer)))
        } else {
            SessionState::StreamingSnapshot(self.with_phase(Streaming::<SnapshotOnly>::new(peer)))
        }
    }

    fn wire_version(&self) -> u32
    where
        P: PhaseWire,
    {
        self.phase.wire_version(&self.config)
    }

    fn frame_limit(&self) -> usize
    where
        P: PhaseWire,
    {
        self.phase.frame_limit(&self.config)
    }
}

impl<R, P: PhaseWire> SessionWire for Session<R, P> {
    fn wire_version(&self) -> u32 {
        Session::wire_version(self)
    }

    fn frame_limit(&self) -> usize {
        Session::frame_limit(self)
    }
}

impl<R, P: PhaseMarker> Session<R, P> {
    pub fn phase(&self) -> SessionPhase {
        P::PHASE
    }
}

impl<R, P: PhasePeer> Session<R, P> {
    pub fn peer(&self) -> &SessionPeer {
        self.phase.peer()
    }

    pub fn negotiated_max_frame_bytes(&self) -> usize {
        self.peer().max_frame_bytes as usize
    }

    pub fn negotiated_frame_limit(&self) -> NegotiatedFrameLimit {
        NegotiatedFrameLimit::new(self.peer().max_frame_bytes as usize)
    }
}

impl Session<Outbound, Connecting> {
    pub fn begin_handshake(
        mut self,
        store: &impl SessionStore,
        now_ms: u64,
    ) -> (Session<Outbound, Handshaking>, SessionAction) {
        let hello = self.build_hello(store, now_ms);
        (
            self.with_phase(Handshaking),
            SessionAction::Send(ReplMessage::Hello(hello)),
        )
    }
}

impl Session<Outbound, Handshaking> {
    pub fn resend_handshake(&mut self, store: &impl SessionStore, now_ms: u64) -> SessionAction {
        let hello = self.build_hello(store, now_ms);
        SessionAction::Send(ReplMessage::Hello(hello))
    }
}

#[allow(private_bounds)]
impl<R, P: PhaseWrap + PhasePeer + PhasePeerOpt> Session<R, P> {
    fn handle_ack(self, ack: Ack) -> (SessionState<R>, Vec<SessionAction>) {
        let allowed = AllowedNamespaces::new(&self.peer().accepted_namespaces);
        match allowed.validate_ack(ack) {
            Ok(ack) => (P::wrap(self), vec![SessionAction::PeerAck(ack)]),
            Err(err) => self.fail(err),
        }
    }

    fn handle_want(self, want: Want) -> (SessionState<R>, Vec<SessionAction>) {
        (P::wrap(self), vec![SessionAction::PeerWant(want)])
    }

    fn handle_ping(self, ping: Ping) -> (SessionState<R>, Vec<SessionAction>) {
        let action = SessionAction::Send(ReplMessage::Pong(Pong { nonce: ping.nonce }));
        (P::wrap(self), vec![action])
    }

    fn handle_pong(self, _pong: Pong) -> (SessionState<R>, Vec<SessionAction>) {
        (P::wrap(self), Vec::new())
    }
}

impl<R, P: PhasePeerOpt> Session<R, P> {
    fn invalid_request(self, reason: impl Into<String>) -> (SessionState<R>, Vec<SessionAction>) {
        let error = invalid_request_error(reason);
        self.fail(error)
    }

    fn fail(self, error: ReplError) -> (SessionState<R>, Vec<SessionAction>) {
        let payload = error.to_payload();
        let Session {
            role: _,
            phase,
            config,
            limits,
            admission,
            gap_buffer,
            durable,
            applied,
            next_nonce,
        } = self;
        let next = match phase.into_peer() {
            Some(peer) => SessionState::Draining(Session {
                role: PhantomData,
                phase: Draining { peer },
                config,
                limits,
                admission,
                gap_buffer,
                durable,
                applied,
                next_nonce,
            }),
            None => SessionState::Closed(Session {
                role: PhantomData,
                phase: Closed,
                config,
                limits,
                admission,
                gap_buffer,
                durable,
                applied,
                next_nonce,
            }),
        };
        (
            next,
            vec![
                SessionAction::Send(ReplMessage::Error(payload.clone())),
                // Error frame already emitted above; close without re-emitting to
                // avoid duplicate error frames on transports that send close errors.
                SessionAction::Close { error: None },
            ],
        )
    }

    fn handle_peer_error(self, payload: ErrorPayload) -> (SessionState<R>, Vec<SessionAction>) {
        let Session {
            role: _,
            phase,
            config,
            limits,
            admission,
            gap_buffer,
            durable,
            applied,
            next_nonce,
        } = self;
        let next = match phase.into_peer() {
            Some(peer) => SessionState::Draining(Session {
                role: PhantomData,
                phase: Draining { peer },
                config,
                limits,
                admission,
                gap_buffer,
                durable,
                applied,
                next_nonce,
            }),
            None => SessionState::Closed(Session {
                role: PhantomData,
                phase: Closed,
                config,
                limits,
                admission,
                gap_buffer,
                durable,
                applied,
                next_nonce,
            }),
        };
        (
            next,
            vec![
                SessionAction::PeerError(payload.clone()),
                SessionAction::Close {
                    error: Some(payload),
                },
            ],
        )
    }
}

impl Session<Inbound, Connecting> {
    fn handle_hello(
        mut self,
        hello: Hello,
        store: &mut impl SessionStore,
        _now_ms: u64,
    ) -> (SessionState<Inbound>, Vec<SessionAction>) {
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
        let wants = self.initial_wants(&hello.seen_durable, &incoming_namespaces);
        let peer = SessionPeer {
            replica_id: hello.sender_replica_id,
            store_epoch: hello.store_epoch,
            protocol_version: negotiated,
            max_frame_bytes: std::cmp::min(self.config.max_frame_bytes, hello.max_frame_bytes),
            incoming_namespaces,
            accepted_namespaces,
            live_stream_enabled: welcome.live_stream_enabled,
        };
        let session = self.with_streaming(peer);
        let mut actions = vec![SessionAction::Send(ReplMessage::Welcome(welcome))];
        if !wants.is_empty() {
            actions.push(SessionAction::Send(ReplMessage::Want(Want { want: wants })));
        }
        (session, actions)
    }
}

#[allow(private_bounds)]
impl<M> Session<Inbound, Streaming<M>>
where
    Streaming<M>: PhaseWrap,
{
    fn handle_hello_replay(
        mut self,
        hello: Hello,
        store: &mut impl SessionStore,
    ) -> (SessionState<Inbound>, Vec<SessionAction>) {
        let peer = self.peer().clone();
        if let Err(error) =
            self.validate_peer_store(hello.store_id, hello.store_epoch, hello.sender_replica_id)
        {
            return self.fail(error);
        }
        if hello.sender_replica_id != peer.replica_id {
            return self.invalid_request("unexpected HELLO");
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
        if negotiated != peer.protocol_version {
            return self.invalid_request("HELLO protocol mismatch");
        }

        let accepted_namespaces =
            intersect_namespaces(&self.config.offered_namespaces, &hello.requested_namespaces);
        let incoming_namespaces =
            intersect_namespaces(&self.config.requested_namespaces, &hello.offered_namespaces);
        if accepted_namespaces != peer.accepted_namespaces
            || incoming_namespaces != peer.incoming_namespaces
        {
            return self.invalid_request("HELLO namespaces changed");
        }

        let snapshot = store.watermark_snapshot(&accepted_namespaces);
        let welcome = self.build_welcome(negotiated, &hello, snapshot, accepted_namespaces);
        if welcome.live_stream_enabled != peer.live_stream_enabled {
            return self.invalid_request("HELLO live_stream changed");
        }
        (
            <Streaming<M> as PhaseWrap>::wrap(self),
            vec![SessionAction::Send(ReplMessage::Welcome(welcome))],
        )
    }
}

impl Session<Outbound, Handshaking> {
    fn handle_welcome(
        mut self,
        welcome: super::proto::Welcome,
        store: &mut impl SessionStore,
        _now_ms: u64,
    ) -> (SessionState<Outbound>, Vec<SessionAction>) {
        if let Err(error) = self.validate_peer_store(
            welcome.store_id,
            welcome.store_epoch,
            welcome.receiver_replica_id,
        ) {
            return self.fail(error);
        }

        let local_min = self.config.protocol.min;
        let local_max = self.config.protocol.max;
        if welcome.protocol_version < local_min || welcome.protocol_version > local_max {
            return self.fail(version_incompatible_error(&ProtocolIncompatible {
                local_min,
                local_max,
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

        let wants = self.initial_wants(&welcome.receiver_seen_durable, &incoming_namespaces);
        let peer = SessionPeer {
            replica_id: welcome.receiver_replica_id,
            store_epoch: welcome.store_epoch,
            protocol_version: welcome.protocol_version,
            max_frame_bytes: std::cmp::min(self.config.max_frame_bytes, welcome.max_frame_bytes),
            incoming_namespaces,
            accepted_namespaces: welcome.accepted_namespaces.clone(),
            live_stream_enabled: welcome.live_stream_enabled,
        };
        let session = self.with_streaming(peer);
        let actions = if wants.is_empty() {
            Vec::new()
        } else {
            vec![SessionAction::Send(ReplMessage::Want(Want { want: wants }))]
        };
        (session, actions)
    }
}

#[allow(private_bounds)]
impl<M> Session<Outbound, Streaming<M>>
where
    Streaming<M>: PhaseWrap,
{
    fn handle_welcome_replay(
        self,
        welcome: super::proto::Welcome,
    ) -> (SessionState<Outbound>, Vec<SessionAction>) {
        let peer = self.peer().clone();
        if let Err(error) = self.validate_peer_store(
            welcome.store_id,
            welcome.store_epoch,
            welcome.receiver_replica_id,
        ) {
            return self.fail(error);
        }
        if welcome.receiver_replica_id != peer.replica_id {
            return self.invalid_request("unexpected WELCOME");
        }
        if welcome.protocol_version != peer.protocol_version {
            return self.invalid_request("WELCOME protocol mismatch");
        }
        if welcome.accepted_namespaces != peer.accepted_namespaces {
            return self.invalid_request("WELCOME namespaces changed");
        }
        if welcome.live_stream_enabled != peer.live_stream_enabled {
            return self.invalid_request("WELCOME live_stream changed");
        }

        (<Streaming<M> as PhaseWrap>::wrap(self), Vec::new())
    }
}

pub fn handle_outbound_message(
    session: SessionState<Outbound>,
    msg: WireReplMessage,
    store: &mut impl SessionStore,
    now_ms: u64,
) -> (SessionState<Outbound>, Vec<SessionAction>) {
    match msg {
        WireReplMessage::Hello(_) => match session {
            SessionState::Connecting(session) => session.invalid_request("unexpected HELLO"),
            SessionState::Handshaking(session) => session.invalid_request("unexpected HELLO"),
            SessionState::StreamingLive(session) => session.invalid_request("unexpected HELLO"),
            SessionState::StreamingSnapshot(session) => session.invalid_request("unexpected HELLO"),
            SessionState::Draining(session) => session.invalid_request("unexpected HELLO"),
            SessionState::Closed(session) => session.invalid_request("unexpected HELLO"),
        },
        WireReplMessage::Welcome(msg) => match session {
            SessionState::Handshaking(session) => session.handle_welcome(msg, store, now_ms),
            SessionState::StreamingLive(session) => session.handle_welcome_replay(msg),
            SessionState::StreamingSnapshot(session) => session.handle_welcome_replay(msg),
            SessionState::Connecting(session) => session.invalid_request("unexpected WELCOME"),
            SessionState::Draining(session) => session.invalid_request("unexpected WELCOME"),
            SessionState::Closed(session) => session.invalid_request("unexpected WELCOME"),
        },
        WireReplMessage::Events(msg) => match session {
            SessionState::StreamingLive(session) => session.handle_events(msg, store, now_ms),
            SessionState::StreamingSnapshot(session) => session.handle_events(msg, store, now_ms),
            SessionState::Draining(session) => session.handle_events(msg, store, now_ms),
            SessionState::Connecting(session) => session.invalid_request("EVENTS before handshake"),
            SessionState::Handshaking(session) => {
                session.invalid_request("EVENTS before handshake")
            }
            SessionState::Closed(session) => session.invalid_request("EVENTS before handshake"),
        },
        WireReplMessage::Ack(msg) => match session {
            SessionState::StreamingLive(session) => session.handle_ack(msg),
            SessionState::StreamingSnapshot(session) => session.handle_ack(msg),
            SessionState::Draining(session) => session.handle_ack(msg),
            SessionState::Connecting(session) => session.invalid_request("ACK before handshake"),
            SessionState::Handshaking(session) => session.invalid_request("ACK before handshake"),
            SessionState::Closed(session) => session.invalid_request("ACK before handshake"),
        },
        WireReplMessage::Want(msg) => match session {
            SessionState::StreamingLive(session) => session.handle_want(msg),
            SessionState::StreamingSnapshot(session) => session.handle_want(msg),
            SessionState::Draining(session) => session.handle_want(msg),
            SessionState::Connecting(session) => session.invalid_request("WANT before handshake"),
            SessionState::Handshaking(session) => session.invalid_request("WANT before handshake"),
            SessionState::Closed(session) => session.invalid_request("WANT before handshake"),
        },
        WireReplMessage::Ping(msg) => match session {
            SessionState::StreamingLive(session) => session.handle_ping(msg),
            SessionState::StreamingSnapshot(session) => session.handle_ping(msg),
            SessionState::Draining(session) => session.handle_ping(msg),
            SessionState::Connecting(session) => session.invalid_request("PING before handshake"),
            SessionState::Handshaking(session) => session.invalid_request("PING before handshake"),
            SessionState::Closed(session) => session.invalid_request("PING before handshake"),
        },
        WireReplMessage::Pong(msg) => match session {
            SessionState::StreamingLive(session) => session.handle_pong(msg),
            SessionState::StreamingSnapshot(session) => session.handle_pong(msg),
            SessionState::Draining(session) => session.handle_pong(msg),
            SessionState::Connecting(session) => session.invalid_request("PONG before handshake"),
            SessionState::Handshaking(session) => session.invalid_request("PONG before handshake"),
            SessionState::Closed(session) => session.invalid_request("PONG before handshake"),
        },
        WireReplMessage::Error(msg) => match session {
            SessionState::Connecting(session) => session.handle_peer_error(msg),
            SessionState::Handshaking(session) => session.handle_peer_error(msg),
            SessionState::StreamingLive(session) => session.handle_peer_error(msg),
            SessionState::StreamingSnapshot(session) => session.handle_peer_error(msg),
            SessionState::Draining(session) => session.handle_peer_error(msg),
            SessionState::Closed(session) => session.handle_peer_error(msg),
        },
    }
}

pub fn handle_inbound_message(
    session: SessionState<Inbound>,
    msg: WireReplMessage,
    store: &mut impl SessionStore,
    now_ms: u64,
) -> (SessionState<Inbound>, Vec<SessionAction>) {
    match msg {
        WireReplMessage::Hello(msg) => match session {
            SessionState::Connecting(session) => session.handle_hello(msg, store, now_ms),
            SessionState::StreamingLive(session) => session.handle_hello_replay(msg, store),
            SessionState::StreamingSnapshot(session) => session.handle_hello_replay(msg, store),
            SessionState::Draining(session) => session.invalid_request("unexpected HELLO"),
            SessionState::Handshaking(session) => session.invalid_request("unexpected HELLO"),
            SessionState::Closed(session) => session.invalid_request("unexpected HELLO"),
        },
        WireReplMessage::Welcome(_) => match session {
            SessionState::Connecting(session) => session.invalid_request("unexpected WELCOME"),
            SessionState::Handshaking(session) => session.invalid_request("unexpected WELCOME"),
            SessionState::StreamingLive(session) => session.invalid_request("unexpected WELCOME"),
            SessionState::StreamingSnapshot(session) => {
                session.invalid_request("unexpected WELCOME")
            }
            SessionState::Draining(session) => session.invalid_request("unexpected WELCOME"),
            SessionState::Closed(session) => session.invalid_request("unexpected WELCOME"),
        },
        WireReplMessage::Events(msg) => match session {
            SessionState::StreamingLive(session) => session.handle_events(msg, store, now_ms),
            SessionState::StreamingSnapshot(session) => session.handle_events(msg, store, now_ms),
            SessionState::Draining(session) => session.handle_events(msg, store, now_ms),
            SessionState::Connecting(session) => session.invalid_request("EVENTS before handshake"),
            SessionState::Handshaking(session) => {
                session.invalid_request("EVENTS before handshake")
            }
            SessionState::Closed(session) => session.invalid_request("EVENTS before handshake"),
        },
        WireReplMessage::Ack(msg) => match session {
            SessionState::StreamingLive(session) => session.handle_ack(msg),
            SessionState::StreamingSnapshot(session) => session.handle_ack(msg),
            SessionState::Draining(session) => session.handle_ack(msg),
            SessionState::Connecting(session) => session.invalid_request("ACK before handshake"),
            SessionState::Handshaking(session) => session.invalid_request("ACK before handshake"),
            SessionState::Closed(session) => session.invalid_request("ACK before handshake"),
        },
        WireReplMessage::Want(msg) => match session {
            SessionState::StreamingLive(session) => session.handle_want(msg),
            SessionState::StreamingSnapshot(session) => session.handle_want(msg),
            SessionState::Draining(session) => session.handle_want(msg),
            SessionState::Connecting(session) => session.invalid_request("WANT before handshake"),
            SessionState::Handshaking(session) => session.invalid_request("WANT before handshake"),
            SessionState::Closed(session) => session.invalid_request("WANT before handshake"),
        },
        WireReplMessage::Ping(msg) => match session {
            SessionState::StreamingLive(session) => session.handle_ping(msg),
            SessionState::StreamingSnapshot(session) => session.handle_ping(msg),
            SessionState::Draining(session) => session.handle_ping(msg),
            SessionState::Connecting(session) => session.invalid_request("PING before handshake"),
            SessionState::Handshaking(session) => session.invalid_request("PING before handshake"),
            SessionState::Closed(session) => session.invalid_request("PING before handshake"),
        },
        WireReplMessage::Pong(msg) => match session {
            SessionState::StreamingLive(session) => session.handle_pong(msg),
            SessionState::StreamingSnapshot(session) => session.handle_pong(msg),
            SessionState::Draining(session) => session.handle_pong(msg),
            SessionState::Connecting(session) => session.invalid_request("PONG before handshake"),
            SessionState::Handshaking(session) => session.invalid_request("PONG before handshake"),
            SessionState::Closed(session) => session.invalid_request("PONG before handshake"),
        },
        WireReplMessage::Error(msg) => match session {
            SessionState::Connecting(session) => session.handle_peer_error(msg),
            SessionState::Handshaking(session) => session.handle_peer_error(msg),
            SessionState::StreamingLive(session) => session.handle_peer_error(msg),
            SessionState::StreamingSnapshot(session) => session.handle_peer_error(msg),
            SessionState::Draining(session) => session.handle_peer_error(msg),
            SessionState::Closed(session) => session.handle_peer_error(msg),
        },
    }
}

#[allow(private_bounds)]
impl<R, P: PhaseWrap> Session<R, P> {
    fn handle_events(
        mut self,
        events: WireEvents,
        store: &mut impl SessionStore,
        now_ms: u64,
    ) -> (SessionState<R>, Vec<SessionAction>) {
        let limits = self.limits.clone();
        let local_store = self.config.local_store;
        let incoming_namespaces = self.peer().incoming_namespaces.clone();
        let total_bytes: u64 = events
            .events
            .iter()
            .map(|frame| frame.bytes().len() as u64)
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
            let namespace = frame.eid().namespace.clone();
            let origin = frame.eid().origin_replica_id;
            if !incoming_namespaces.contains(&namespace) {
                return self.fail(namespace_policy_violation_error(&namespace));
            }
            let durable = self.durable_for(&namespace, &origin);
            let head_seq = durable.seq().get();
            let head_sha = match durable.head() {
                HeadStatus::Genesis => None,
                HeadStatus::Known(head) => Some(Sha256(head)),
            };

            let expected_prev = match self.expected_prev_head(durable, frame.eid().origin_seq) {
                Ok(prev) => prev,
                Err(error) => return self.fail(error),
            };

            let verified = {
                let lookup = SessionLookup { store };
                match verify_event_frame(&frame, &limits, local_store, expected_prev, &lookup) {
                    Ok(event) => event,
                    Err(err) => {
                        return self.fail(event_frame_error_payload(
                            &frame, &limits, err, head_sha, head_seq,
                        ));
                    }
                }
            };

            match self
                .gap_buffer
                .ingest(namespace.clone(), origin, durable, verified, now_ms)
            {
                IngestDecision::ForwardContiguousBatch(batch) => {
                    if let Err(error) =
                        self.ingest_contiguous_batch(store, &batch, now_ms, &mut updates)
                    {
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
                    return self.fail(repl_lagged_error(reason, &limits));
                }
                IngestDecision::InvalidBatch(err) => {
                    return self.fail(internal_error(format!("contiguous batch invalid: {err}")));
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
        (P::wrap(self), actions)
    }
}

impl<R, P> Session<R, P> {
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
            seen_applied: Some(snapshot.applied),
            capabilities: self.config.capabilities.clone(),
        }
    }

    fn build_welcome(
        &mut self,
        protocol_version: u32,
        hello: &Hello,
        snapshot: WatermarkSnapshot,
        accepted_namespaces: NamespaceSet,
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
            receiver_seen_applied: Some(snapshot.applied),
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
        self.durable = snapshot.durable;
        self.applied = snapshot.applied;
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
        }
    }

    fn ingest_contiguous_batch(
        &mut self,
        store: &mut impl SessionStore,
        batch: &ContiguousBatch,
        now_ms: u64,
        updates: &mut IngestUpdates<'_>,
    ) -> SessionResult<()> {
        let outcome = store.ingest_remote_batch(batch, now_ms)?;

        if let Err(err) = self.gap_buffer.advance_durable_batch(batch) {
            return Err(internal_error(format!(
                "gap buffer watermark advance failed: {err}"
            )));
        }

        let namespace = batch.namespace();
        let origin = batch.origin();

        update_watermark(&mut self.durable, namespace, &origin, outcome.durable);
        update_watermark(&mut self.applied, namespace, &origin, outcome.applied);

        update_watermark(updates.ack_updates, namespace, &origin, outcome.durable);
        update_watermark(updates.applied_updates, namespace, &origin, outcome.applied);

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
            self.ingest_contiguous_batch(store, &batch, now_ms, updates)?;
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

    fn initial_wants(
        &self,
        peer_seen: &WatermarkState<Durable>,
        incoming_namespaces: &[NamespaceId],
    ) -> WatermarkMap {
        let mut wants = WatermarkMap::new();
        for namespace in incoming_namespaces {
            let Some(origins) = peer_seen.get(namespace) else {
                continue;
            };
            for (origin, peer_wm) in origins {
                let local_seq = self.durable_for(namespace, origin).seq();
                if peer_wm.seq().get() > local_seq.get() {
                    wants
                        .entry(namespace.clone())
                        .or_default()
                        .insert(*origin, local_seq);
                }
            }
        }
        wants
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

fn intersect_namespaces(a: &[NamespaceId], b: &[NamespaceId]) -> NamespaceSet {
    if a.is_empty() || b.is_empty() {
        return NamespaceSet::default();
    }
    let bset: BTreeSet<_> = b.iter().cloned().collect();
    let out: Vec<_> = a.iter().filter(|ns| bset.contains(*ns)).cloned().collect();
    NamespaceSet::from(out)
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

    Some(Ack {
        durable: durable_updates.clone(),
        applied: if applied_updates.is_empty() {
            None
        } else {
            Some(applied_updates.clone())
        },
    })
}

fn repl_lagged_error(reason: ReplRejectReason, limits: &Limits) -> ReplError {
    let message = repl_reject_reason_message(&reason);
    ReplError::new(ProtocolErrorCode::SubscriberLagged.into(), message, true).with_details(
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
    ReplError::new(
        ProtocolErrorCode::WrongStore.into(),
        "wrong store id",
        false,
    )
    .with_details(ReplErrorDetails::WrongStore(WrongStoreDetails {
        expected_store_id: expected,
        got_store_id: got,
    }))
}

fn store_epoch_mismatch_error(
    store_id: StoreId,
    expected: StoreEpoch,
    got: StoreEpoch,
) -> ReplError {
    ReplError::new(
        ProtocolErrorCode::StoreEpochMismatch.into(),
        "store epoch mismatch",
        false,
    )
    .with_details(ReplErrorDetails::StoreEpochMismatch(
        StoreEpochMismatchDetails {
            store_id,
            expected_epoch: expected.get(),
            got_epoch: got.get(),
        },
    ))
}

fn replica_id_collision_error(replica_id: ReplicaId) -> ReplError {
    ReplError::new(
        ProtocolErrorCode::ReplicaIdCollision.into(),
        "replica_id collision",
        false,
    )
    .with_details(ReplErrorDetails::ReplicaIdCollision(
        ReplicaIdCollisionDetails { replica_id },
    ))
}

fn version_incompatible_error(err: &ProtocolIncompatible) -> ReplError {
    ReplError::new(
        ProtocolErrorCode::VersionIncompatible.into(),
        "protocol versions incompatible",
        false,
    )
    .with_details(ReplErrorDetails::VersionIncompatible(
        VersionIncompatibleDetails {
            local_min: err.local_min,
            local_max: err.local_max,
            peer_min: err.peer_min,
            peer_max: err.peer_max,
        },
    ))
}

fn invalid_request_error(reason: impl Into<String>) -> ReplError {
    let reason = reason.into();
    ReplError::new(
        ProtocolErrorCode::InvalidRequest.into(),
        reason.clone(),
        false,
    )
    .with_details(ReplErrorDetails::InvalidRequest(InvalidRequestDetails {
        field: Some("type".to_string()),
        reason: Some(reason),
    }))
}

fn namespace_policy_violation_error(namespace: &NamespaceId) -> ReplError {
    ReplError::new(
        ProtocolErrorCode::NamespacePolicyViolation.into(),
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
    ReplError::new(
        ProtocolErrorCode::InternalError.into(),
        "internal error",
        false,
    )
    .with_details(ReplErrorDetails::InternalError(InternalErrorDetails {
        trace_id: None,
        component: Some(message.into()),
    }))
}

fn overloaded_error(rejection: AdmissionRejection) -> ReplError {
    ReplError::new(ProtocolErrorCode::Overloaded.into(), "overloaded", true).with_details(
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
                store_epoch_mismatch_error(expected.store_id, expected.store_epoch, got.store_epoch)
            }
        }
        EventFrameError::FrameMismatch => ReplError::new(
            ProtocolErrorCode::InvalidRequest.into(),
            "event id does not match decoded body",
            false,
        )
        .with_details(ReplErrorDetails::InvalidRequest(InvalidRequestDetails {
            field: Some("event_id".to_string()),
            reason: Some("event_id does not match decoded body".to_string()),
        })),
        EventFrameError::NonCanonical => ReplError::new(
            ProtocolErrorCode::NonCanonical.into(),
            "event body is not canonically encoded",
            false,
        )
        .with_details(ReplErrorDetails::NonCanonical(NonCanonicalDetails {
            format: "cbor".to_string(),
            reason: Some("event body is not canonically encoded".to_string()),
        })),
        EventFrameError::HashMismatch => {
            let expected = hash_event_body(frame.bytes());
            ReplError::new(
                ProtocolErrorCode::HashMismatch.into(),
                "event sha256 mismatch",
                false,
            )
            .with_details(ReplErrorDetails::HashMismatch(HashMismatchDetails {
                eid: event_id_details(frame.eid()),
                expected_sha256: sha256_hex(expected),
                got_sha256: sha256_hex(frame.sha256()),
            }))
        }
        EventFrameError::PrevMismatch => ReplError::new(
            ProtocolErrorCode::PrevShaMismatch.into(),
            "prev_sha256 mismatch",
            false,
        )
        .with_details(ReplErrorDetails::PrevShaMismatch(PrevShaMismatchDetails {
            eid: event_id_details(frame.eid()),
            expected_prev_sha256: sha256_hex_or_zero(head_sha),
            got_prev_sha256: sha256_hex_or_zero(frame.prev_sha256()),
            head_seq,
        })),
        EventFrameError::Validation(err) => ReplError::new(
            ProtocolErrorCode::InvalidRequest.into(),
            err.to_string(),
            false,
        )
        .with_details(ReplErrorDetails::InvalidRequest(InvalidRequestDetails {
            field: None,
            reason: Some(err.to_string()),
        })),
        EventFrameError::Decode(err) => decode_error_payload(&err, limits, frame.bytes().len()),
        EventFrameError::Encode(err) => internal_error(err.to_string()),
        EventFrameError::Lookup(err) => internal_error(err.to_string()),
        EventFrameError::Equivocation => ReplError::new(
            ProtocolErrorCode::Equivocation.into(),
            "equivocation detected",
            false,
        ),
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
        } => ReplError::new(
            ProtocolErrorCode::PrevShaMismatch.into(),
            "prev_sha256 mismatch",
            false,
        )
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
        DrainError::InvalidBatch(err) => {
            internal_error(format!("contiguous batch invalid during drain: {err}"))
        }
    }
}

fn decode_error_payload(err: &DecodeError, limits: &Limits, frame_bytes: usize) -> ReplError {
    match err {
        DecodeError::DecodeLimit(reason)
            if matches!(*reason, "max_wal_record_bytes" | "max_frame_bytes") =>
        {
            ReplError::new(
                ProtocolErrorCode::FrameTooLarge.into(),
                "event frame too large",
                false,
            )
            .with_details(ReplErrorDetails::FrameTooLarge(FrameTooLargeDetails {
                max_frame_bytes: limits.policy().max_wal_record_payload_bytes() as u64,
                got_bytes: frame_bytes as u64,
            }))
        }
        DecodeError::DecodeLimit(_) => invalid_request_decode_error(err),
        DecodeError::IndefiniteLength | DecodeError::TrailingBytes => ReplError::new(
            ProtocolErrorCode::NonCanonical.into(),
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
    ReplError::new(
        ProtocolErrorCode::InvalidRequest.into(),
        err.to_string(),
        false,
    )
    .with_details(ReplErrorDetails::InvalidRequest(InvalidRequestDetails {
        field: None,
        reason: Some(err.to_string()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    use crate::core::{
        ActorId, EventBody, EventBytes, EventFrameV1, EventKindV1, HlcMax, NamespaceId, ReplicaId,
        Seq0, Seq1, StoreEpoch, StoreId, StoreIdentity, TxnDeltaV1, TxnId, TxnV1,
        encode_event_body_canonical, hash_event_body,
    };
    use crate::daemon::repl::proto;
    use crate::daemon::repl::proto::NamespaceSet;

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
            let mut durable: WatermarkState<Durable> = BTreeMap::new();
            let mut applied: WatermarkState<Applied> = BTreeMap::new();

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

    impl SessionStore for TestStore {
        fn watermark_snapshot(&self, namespaces: &[NamespaceId]) -> WatermarkSnapshot {
            self.snapshot_for(namespaces)
        }

        fn lookup_event_sha(&self, eid: &EventId) -> Result<Option<Sha256>, EventShaLookupError> {
            Ok(self.lookup.get(eid).copied())
        }

        fn ingest_remote_batch(
            &mut self,
            batch: &ContiguousBatch,
            _now_ms: u64,
        ) -> SessionResult<IngestOutcome> {
            let namespace = batch.namespace();
            let origin = batch.origin();
            for ev in batch.events() {
                let eid = EventId::new(
                    ev.body.origin_replica_id,
                    ev.body.namespace.clone(),
                    ev.body.origin_seq,
                );
                self.lookup.insert(eid, ev.sha256);
            }
            let last = batch.last_event();
            let head = HeadStatus::Known(last.sha256.0);
            let durable = Watermark::new(Seq0::new(last.seq().get()), head).expect("durable");
            let applied = Watermark::new(Seq0::new(last.seq().get()), head).expect("applied");
            self.durable
                .entry(namespace.clone())
                .or_default()
                .insert(origin, durable);
            self.applied
                .entry(namespace.clone())
                .or_default()
                .insert(origin, applied);

            Ok(IngestOutcome { durable, applied })
        }

        fn update_replica_liveness(
            &mut self,
            _replica_id: ReplicaId,
            _last_seen_ms: u64,
            _last_handshake_ms: u64,
            _role: ReplicaDurabilityRole,
        ) -> Result<(), WalIndexError> {
            Ok(())
        }
    }

    fn apply_inbound_hello(
        session: InboundConnecting,
        hello: Hello,
        store: &mut TestStore,
    ) -> (SessionState<Inbound>, Vec<SessionAction>) {
        handle_inbound_message(
            SessionState::Connecting(session),
            WireReplMessage::Hello(hello),
            store,
            0,
        )
    }

    fn expect_inbound_streaming(session: SessionState<Inbound>) -> Session<Inbound, StreamingLive> {
        let SessionState::StreamingLive(session) = session else {
            panic!("expected streaming session");
        };
        session
    }

    fn base_store() -> (TestStore, StoreIdentity, ReplicaId) {
        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let identity = StoreIdentity::new(store_id, StoreEpoch::new(1));
        let replica = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        (TestStore::new(), identity, replica)
    }

    #[test]
    fn protocol_range_rejects_inverted() {
        let err = ProtocolRange::range(3, 2).unwrap_err();
        assert_eq!(err.min, 3);
        assert_eq!(err.max, 2);
    }

    #[test]
    fn protocol_range_exact_negotiates() {
        let negotiated = negotiate_version(ProtocolRange::exact(2), 3, 1).unwrap();
        assert_eq!(negotiated, 2);
    }

    fn inbound_streaming_session(
        namespaces: Vec<NamespaceId>,
    ) -> (SessionState<Inbound>, TestStore, StoreIdentity) {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = namespaces.clone().into();
        config.offered_namespaces = namespaces.clone().into();

        let session = InboundConnecting::new(config, limits, admission);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([3u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: namespaces.clone().into(),
            offered_namespaces: namespaces.into(),
            seen_durable: BTreeMap::new(),
            seen_applied: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };

        let (session, _actions) = apply_inbound_hello(session, hello, &mut store);
        (session, store, identity)
    }

    fn ack_for(namespace: NamespaceId, origin: ReplicaId, seq: u64, applied: bool) -> Ack {
        let head = HeadStatus::Known([seq as u8; 32]);
        let durable = Watermark::new(Seq0::new(seq), head).unwrap_or_else(|_| Watermark::genesis());
        let mut durable_state = WatermarkState::new();
        durable_state
            .entry(namespace.clone())
            .or_default()
            .insert(origin, durable);

        let applied_state = if applied {
            let applied =
                Watermark::new(Seq0::new(seq), head).unwrap_or_else(|_| Watermark::genesis());
            let mut state = WatermarkState::new();
            state.entry(namespace).or_default().insert(origin, applied);
            Some(state)
        } else {
            None
        };

        Ack {
            durable: durable_state,
            applied: applied_state,
        }
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
            trace_id: None,
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
        let eid = EventId::new(origin, namespace.clone(), body.origin_seq);
        EventFrameV1::try_from_parts(
            eid,
            sha,
            prev,
            EventBytes::<crate::core::Opaque>::from(bytes),
        )
        .expect("frame")
    }

    #[test]
    fn handshake_negotiates_version_and_namespaces() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()].into();
        config.offered_namespaces = vec![NamespaceId::core()].into();

        let session = InboundConnecting::new(config, limits, admission);

        let peer_store = StoreIdentity::new(identity.store_id, identity.store_epoch);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: peer_store.store_id,
            store_epoch: peer_store.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([3u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()].into(),
            offered_namespaces: vec![NamespaceId::core()].into(),
            seen_durable: BTreeMap::new(),
            seen_applied: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };

        let (session, actions) = apply_inbound_hello(session, hello, &mut store);
        let session = expect_inbound_streaming(session);
        assert!(matches!(session.phase(), SessionPhase::Streaming));
        assert!(matches!(
            actions.as_slice(),
            [SessionAction::Send(ReplMessage::Welcome(_))]
        ));
        let SessionAction::Send(ReplMessage::Welcome(welcome)) = &actions[0] else {
            panic!("expected welcome");
        };
        assert_eq!(welcome.protocol_version, PROTOCOL_VERSION_V1);
        assert_eq!(
            welcome.accepted_namespaces,
            NamespaceSet::from(vec![NamespaceId::core()])
        );
    }

    #[test]
    fn handshake_negotiates_snapshot_only_when_peer_disables_live_stream() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()].into();
        config.offered_namespaces = vec![NamespaceId::core()].into();

        let session = InboundConnecting::new(config, limits, admission);

        let peer_store = StoreIdentity::new(identity.store_id, identity.store_epoch);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: peer_store.store_id,
            store_epoch: peer_store.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([7u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()].into(),
            offered_namespaces: vec![NamespaceId::core()].into(),
            seen_durable: BTreeMap::new(),
            seen_applied: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: false,
                supports_compression: false,
            },
        };

        let (session, actions) = apply_inbound_hello(session, hello, &mut store);
        assert!(matches!(session, SessionState::StreamingSnapshot(_)));
        let SessionAction::Send(ReplMessage::Welcome(welcome)) = &actions[0] else {
            panic!("expected welcome");
        };
        assert!(!welcome.live_stream_enabled);
    }

    #[test]
    fn hello_replay_accepts_reordered_namespaces() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        let alpha = NamespaceId::parse("alpha").unwrap();
        let beta = NamespaceId::parse("beta").unwrap();
        config.requested_namespaces = vec![alpha.clone(), beta.clone()].into();
        config.offered_namespaces = vec![alpha.clone(), beta.clone()].into();

        let session = InboundConnecting::new(config, limits, admission);

        let peer_store = StoreIdentity::new(identity.store_id, identity.store_epoch);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: peer_store.store_id,
            store_epoch: peer_store.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([11u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![alpha.clone(), beta.clone()].into(),
            offered_namespaces: vec![alpha.clone(), beta.clone()].into(),
            seen_durable: BTreeMap::new(),
            seen_applied: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };

        let (session, _actions) = apply_inbound_hello(session, hello.clone(), &mut store);
        let session = expect_inbound_streaming(session);

        let hello_replay = Hello {
            requested_namespaces: vec![beta.clone(), alpha.clone(), beta.clone()].into(),
            offered_namespaces: vec![beta.clone(), alpha.clone()].into(),
            ..hello
        };

        let (session, actions) = handle_inbound_message(
            SessionState::StreamingLive(session),
            WireReplMessage::Hello(hello_replay),
            &mut store,
            0,
        );

        assert!(matches!(session, SessionState::StreamingLive(_)));
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, SessionAction::Send(ReplMessage::Welcome(_))))
        );
    }

    #[test]
    fn welcome_sends_initial_want_when_peer_ahead() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()].into();
        config.offered_namespaces = vec![NamespaceId::core()].into();

        let session = OutboundConnecting::new(config, limits, admission);
        let (session, _action) = session.begin_handshake(&store, 0);
        let session = SessionState::Handshaking(session);

        let peer_replica = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
        let mut receiver_seen: WatermarkState<Durable> = BTreeMap::new();
        receiver_seen
            .entry(NamespaceId::core())
            .or_default()
            .insert(
                peer_replica,
                Watermark::new(Seq0::new(2), HeadStatus::Known([2u8; 32])).unwrap(),
            );

        let welcome = proto::Welcome {
            protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            receiver_replica_id: peer_replica,
            welcome_nonce: 10,
            accepted_namespaces: vec![NamespaceId::core()].into(),
            receiver_seen_durable: receiver_seen,
            receiver_seen_applied: None,
            live_stream_enabled: true,
            max_frame_bytes: 1024,
        };

        let (_session, actions) =
            handle_outbound_message(session, WireReplMessage::Welcome(welcome), &mut store, 0);
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
            .and_then(|m| m.get(&peer_replica))
            .copied()
            .unwrap_or(Seq0::ZERO);
        assert_eq!(seq, Seq0::ZERO);
    }

    #[test]
    fn handshake_rejects_version_incompatible() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()].into();
        config.offered_namespaces = vec![NamespaceId::core()].into();

        let session = InboundConnecting::new(config, limits, admission);
        let hello = Hello {
            protocol_version: 2,
            min_protocol_version: 2,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([9u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()].into(),
            offered_namespaces: vec![NamespaceId::core()].into(),
            seen_durable: BTreeMap::new(),
            seen_applied: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };

        let (session, actions) = apply_inbound_hello(session, hello, &mut store);
        assert!(matches!(session, SessionState::Closed(_)));
        let SessionAction::Send(ReplMessage::Error(payload)) = &actions[0] else {
            panic!("expected error payload");
        };
        assert_eq!(payload.code, ProtocolErrorCode::VersionIncompatible.into());
    }

    #[test]
    fn typestate_outbound_reaches_streaming() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()].into();
        config.offered_namespaces = vec![NamespaceId::core()].into();

        let session = OutboundConnecting::new(config, limits, admission);
        let (session, _action) = session.begin_handshake(&store, 0);
        let session = SessionState::Handshaking(session);

        let welcome = proto::Welcome {
            protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            receiver_replica_id: ReplicaId::new(Uuid::from_bytes([12u8; 16])),
            welcome_nonce: 10,
            accepted_namespaces: vec![NamespaceId::core()].into(),
            receiver_seen_durable: BTreeMap::new(),
            receiver_seen_applied: None,
            live_stream_enabled: true,
            max_frame_bytes: 1024,
        };

        let (session, _actions) =
            handle_outbound_message(session, WireReplMessage::Welcome(welcome), &mut store, 0);
        let SessionState::StreamingLive(session) = session else {
            panic!("expected streaming session");
        };
        let _ = session.peer();
        let _ = session.negotiated_max_frame_bytes();
    }

    #[test]
    fn typestate_outbound_reaches_snapshot_streaming() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()].into();
        config.offered_namespaces = vec![NamespaceId::core()].into();

        let session = OutboundConnecting::new(config, limits, admission);
        let (session, _action) = session.begin_handshake(&store, 0);
        let session = SessionState::Handshaking(session);

        let welcome = proto::Welcome {
            protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            receiver_replica_id: ReplicaId::new(Uuid::from_bytes([13u8; 16])),
            welcome_nonce: 10,
            accepted_namespaces: vec![NamespaceId::core()].into(),
            receiver_seen_durable: BTreeMap::new(),
            receiver_seen_applied: None,
            live_stream_enabled: false,
            max_frame_bytes: 1024,
        };

        let (session, _actions) =
            handle_outbound_message(session, WireReplMessage::Welcome(welcome), &mut store, 0);
        assert!(matches!(session, SessionState::StreamingSnapshot(_)));
    }

    #[test]
    fn ack_allowed_namespace_forwards_validated_ack() {
        let namespace = NamespaceId::core();
        let (session, mut store, _identity) = inbound_streaming_session(vec![namespace.clone()]);
        let origin = ReplicaId::new(Uuid::from_bytes([4u8; 16]));
        let ack = ack_for(namespace.clone(), origin, 5, false);

        let (session, actions) =
            handle_inbound_message(session, WireReplMessage::Ack(ack), &mut store, 0);

        assert!(matches!(session, SessionState::StreamingLive(_)));
        let ack = actions
            .iter()
            .find_map(|action| match action {
                SessionAction::PeerAck(ack) => Some(ack),
                _ => None,
            })
            .expect("validated ack");
        assert!(ack.applied().is_none());
        assert!(ack.durable().contains_key(&namespace));
    }

    #[test]
    fn ack_rejects_disallowed_namespace() {
        let namespace = NamespaceId::core();
        let disallowed = NamespaceId::parse("other").expect("namespace");
        let (session, mut store, _identity) = inbound_streaming_session(vec![namespace.clone()]);
        let origin = ReplicaId::new(Uuid::from_bytes([5u8; 16]));
        let ack = ack_for(disallowed.clone(), origin, 2, false);

        let (session, actions) =
            handle_inbound_message(session, WireReplMessage::Ack(ack), &mut store, 0);

        assert!(matches!(session, SessionState::Draining(_)));
        let error = actions
            .iter()
            .find_map(|action| match action {
                SessionAction::Send(ReplMessage::Error(payload)) => Some(payload),
                _ => None,
            })
            .expect("error payload");
        assert_eq!(
            error.code,
            ProtocolErrorCode::NamespacePolicyViolation.into()
        );
    }

    #[test]
    fn ack_rejects_mixed_namespaces() {
        let namespace = NamespaceId::core();
        let disallowed = NamespaceId::parse("other").expect("namespace");
        let (session, mut store, _identity) = inbound_streaming_session(vec![namespace.clone()]);
        let origin = ReplicaId::new(Uuid::from_bytes([6u8; 16]));
        let mut ack = ack_for(namespace.clone(), origin, 1, false);
        let head = HeadStatus::Known([3u8; 32]);
        let other = Watermark::new(Seq0::new(3), head).unwrap();
        ack.durable
            .entry(disallowed.clone())
            .or_default()
            .insert(origin, other);

        let (session, actions) =
            handle_inbound_message(session, WireReplMessage::Ack(ack), &mut store, 0);

        assert!(matches!(session, SessionState::Draining(_)));
        let error = actions
            .iter()
            .find_map(|action| match action {
                SessionAction::Send(ReplMessage::Error(payload)) => Some(payload),
                _ => None,
            })
            .expect("error payload");
        assert_eq!(
            error.code,
            ProtocolErrorCode::NamespacePolicyViolation.into()
        );
    }

    #[test]
    fn events_apply_and_ack() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()].into();
        config.offered_namespaces = vec![NamespaceId::core()].into();

        let session = InboundConnecting::new(config, limits, admission);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([4u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()].into(),
            offered_namespaces: vec![NamespaceId::core()].into(),
            seen_durable: BTreeMap::new(),
            seen_applied: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };
        let (session, _actions) = apply_inbound_hello(session, hello, &mut store);
        let session = expect_inbound_streaming(session);

        let origin = ReplicaId::new(Uuid::from_bytes([5u8; 16]));
        let e1 = make_event(identity, NamespaceId::core(), origin, 1, None);
        let e2 = make_event(identity, NamespaceId::core(), origin, 2, Some(e1.sha256()));
        let e2_sha = e2.sha256();

        let (_session, actions) = handle_inbound_message(
            SessionState::StreamingLive(session),
            WireReplMessage::Events(WireEvents {
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

        let watermark = ack
            .durable
            .get(&NamespaceId::core())
            .and_then(|m| m.get(&origin))
            .copied()
            .unwrap_or_else(Watermark::genesis);
        assert_eq!(watermark.seq(), Seq0::new(2));
        assert_eq!(watermark.head(), HeadStatus::Known(e2_sha.0));
    }

    #[test]
    fn events_buffer_gap_sends_want() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()].into();
        config.offered_namespaces = vec![NamespaceId::core()].into();

        let session = InboundConnecting::new(config, limits, admission);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([6u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()].into(),
            offered_namespaces: vec![NamespaceId::core()].into(),
            seen_durable: BTreeMap::new(),
            seen_applied: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };
        let (session, _actions) = apply_inbound_hello(session, hello, &mut store);
        let session = expect_inbound_streaming(session);

        let origin = ReplicaId::new(Uuid::from_bytes([7u8; 16]));
        let e1 = make_event(identity, NamespaceId::core(), origin, 1, None);
        let e3 = make_event(identity, NamespaceId::core(), origin, 3, Some(e1.sha256()));

        let (_session, actions) = handle_inbound_message(
            SessionState::StreamingLive(session),
            WireReplMessage::Events(WireEvents { events: vec![e3] }),
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

        assert_eq!(payload.code, ProtocolErrorCode::SubscriberLagged.into());
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
        config.requested_namespaces = vec![NamespaceId::core()].into();
        config.offered_namespaces = vec![NamespaceId::core()].into();

        let session = InboundConnecting::new(config, limits, admission);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([8u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()].into(),
            offered_namespaces: vec![NamespaceId::core()].into(),
            seen_durable: BTreeMap::new(),
            seen_applied: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };
        let (session, _actions) = apply_inbound_hello(session, hello, &mut store);
        let session = expect_inbound_streaming(session);

        let origin = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
        let e1 = make_event(identity, NamespaceId::core(), origin, 1, None);
        let e2 = make_event(identity, NamespaceId::core(), origin, 2, Some(e1.sha256()));

        let (session, actions) = handle_inbound_message(
            SessionState::StreamingLive(session),
            WireReplMessage::Events(WireEvents { events: vec![e2] }),
            &mut store,
            10,
        );
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, SessionAction::Send(ReplMessage::Want(_))))
        );

        let session = expect_inbound_streaming(session);
        let (_session, actions) = handle_inbound_message(
            SessionState::StreamingLive(session),
            WireReplMessage::Events(WireEvents { events: vec![e1] }),
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

        let watermark = ack
            .durable
            .get(&NamespaceId::core())
            .and_then(|m| m.get(&origin))
            .copied()
            .unwrap_or_else(Watermark::genesis);
        assert_eq!(watermark.seq(), Seq0::new(2));
    }

    #[test]
    fn events_rejects_unaccepted_namespace() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()].into();
        config.offered_namespaces = vec![NamespaceId::core()].into();

        let session = InboundConnecting::new(config, limits, admission);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([10u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()].into(),
            offered_namespaces: vec![NamespaceId::core()].into(),
            seen_durable: BTreeMap::new(),
            seen_applied: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };
        let (session, _actions) = apply_inbound_hello(session, hello, &mut store);
        let session = expect_inbound_streaming(session);

        let origin = ReplicaId::new(Uuid::from_bytes([11u8; 16]));
        let other_namespace = NamespaceId::parse("tmp").unwrap();
        let e1 = make_event(identity, other_namespace, origin, 1, None);

        let (session, actions) = handle_inbound_message(
            SessionState::StreamingLive(session),
            WireReplMessage::Events(WireEvents { events: vec![e1] }),
            &mut store,
            10,
        );
        assert!(matches!(session, SessionState::Draining(_)));
        let payload = actions
            .iter()
            .find_map(|action| match action {
                SessionAction::Send(ReplMessage::Error(payload)) => Some(payload),
                _ => None,
            })
            .expect("error payload");
        assert_eq!(
            payload.code,
            ProtocolErrorCode::NamespacePolicyViolation.into()
        );
    }

    #[test]
    fn events_wrong_store_errors() {
        let (mut store, identity, replica) = base_store();
        let limits = Limits::default();
        let admission = AdmissionController::new(&limits);
        let mut config = SessionConfig::new(identity, replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()].into();
        config.offered_namespaces = vec![NamespaceId::core()].into();

        let session = InboundConnecting::new(config, limits, admission);
        let hello = Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([8u8; 16])),
            hello_nonce: 10,
            max_frame_bytes: 1024,
            requested_namespaces: vec![NamespaceId::core()].into(),
            offered_namespaces: vec![NamespaceId::core()].into(),
            seen_durable: BTreeMap::new(),
            seen_applied: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        };
        let (session, _actions) = apply_inbound_hello(session, hello, &mut store);
        let session = expect_inbound_streaming(session);

        let origin = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
        let wrong_store = StoreIdentity::new(
            StoreId::new(Uuid::from_bytes([10u8; 16])),
            StoreEpoch::new(1),
        );
        let e1 = make_event(wrong_store, NamespaceId::core(), origin, 1, None);

        let (_session, actions) = handle_inbound_message(
            SessionState::StreamingLive(session),
            WireReplMessage::Events(WireEvents { events: vec![e1] }),
            &mut store,
            10,
        );

        let SessionAction::Send(ReplMessage::Error(payload)) = &actions[0] else {
            panic!("expected error payload");
        };
        assert_eq!(payload.code, ProtocolErrorCode::WrongStore.into());
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
        assert_eq!(payload.code, ProtocolErrorCode::NonCanonical.into());
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
        let limits = Limits {
            max_frame_bytes: 1,
            ..Default::default()
        };

        let payload = event_frame_error_payload(
            &frame,
            &limits,
            EventFrameError::Decode(DecodeError::DecodeLimit("max_wal_record_bytes")),
            None,
            0,
        )
        .to_payload();
        assert_eq!(payload.code, ProtocolErrorCode::FrameTooLarge.into());
        let details = payload
            .details_as::<FrameTooLargeDetails>()
            .unwrap()
            .expect("frame too large details");
        assert_eq!(details.max_frame_bytes, limits.max_frame_bytes as u64);
        assert_eq!(details.got_bytes, frame.bytes().len() as u64);
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
        assert_eq!(payload.code, ProtocolErrorCode::InvalidRequest.into());
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
        let frame = make_event(identity, NamespaceId::core(), origin, 1, None);
        let (eid, _sha256, prev_sha256, bytes) = frame.into_parts();
        let frame = EventFrameV1::try_from_parts(eid, Sha256([9u8; 32]), prev_sha256, bytes)
            .expect("frame");

        let payload = event_frame_error_payload(
            &frame,
            &Limits::default(),
            EventFrameError::HashMismatch,
            None,
            0,
        )
        .to_payload();

        assert_eq!(payload.code, ProtocolErrorCode::HashMismatch.into());
        let details = payload
            .details_as::<HashMismatchDetails>()
            .unwrap()
            .expect("hash mismatch details");
        assert_eq!(details.eid.namespace, frame.eid().namespace);
        assert_eq!(details.eid.origin_replica_id, frame.eid().origin_replica_id);
        assert_eq!(details.eid.origin_seq, frame.eid().origin_seq.get());
        assert_eq!(
            details.expected_sha256,
            hex::encode(hash_event_body(frame.bytes()).as_bytes())
        );
        assert_eq!(details.got_sha256, hex::encode(frame.sha256().as_bytes()));
    }

    #[test]
    fn prev_mismatch_maps_to_prev_sha_mismatch_details() {
        let (_store, identity, origin) = base_store();
        let expected_prev = Sha256([1u8; 32]);
        let got_prev = Sha256([2u8; 32]);
        let frame = make_event(identity, NamespaceId::core(), origin, 2, Some(got_prev));

        let payload = event_frame_error_payload(
            &frame,
            &Limits::default(),
            EventFrameError::PrevMismatch,
            Some(expected_prev),
            1,
        )
        .to_payload();

        assert_eq!(payload.code, ProtocolErrorCode::PrevShaMismatch.into());
        let details = payload
            .details_as::<PrevShaMismatchDetails>()
            .unwrap()
            .expect("prev mismatch details");
        assert_eq!(details.eid.namespace, frame.eid().namespace);
        assert_eq!(details.eid.origin_replica_id, frame.eid().origin_replica_id);
        assert_eq!(details.eid.origin_seq, frame.eid().origin_seq.get());
        assert_eq!(
            details.expected_prev_sha256,
            hex::encode(expected_prev.as_bytes())
        );
        assert_eq!(details.got_prev_sha256, hex::encode(got_prev.as_bytes()));
        assert_eq!(details.head_seq, 1);
    }
}
