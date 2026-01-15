//! Outbound replication manager and peer lifecycle.

use std::collections::{BTreeMap, BTreeSet};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam::channel::Sender;
use thiserror::Error;

use crate::core::Opaque;
use crate::core::error::details::{BootstrapRequiredDetails, SnapshotRangeReason};
use crate::core::{
    ErrorCode, ErrorPayload, EventBytes, EventFrameV1, NamespaceId, NamespacePolicy, ReplicaId,
    ReplicaRole, ReplicaRoster, ReplicateMode, StoreIdentity,
};
use crate::daemon::admission::AdmissionController;
use crate::daemon::broadcast::{
    BroadcastError, BroadcastEvent, EventBroadcaster, EventSubscription, SubscriberLimits,
};
use crate::daemon::metrics;
use crate::daemon::repl::proto::{Ack, Events, PROTOCOL_VERSION_V1, Want, WatermarkMap};
use crate::daemon::repl::pending::PendingEvents;
use crate::daemon::repl::{
    FrameError, FrameReader, FrameWriter, ReplEnvelope, ReplMessage, Session, SessionAction,
    SessionConfig, SessionPhase, SessionRole, SessionStore, SharedSessionStore, WalRangeReader,
    decode_envelope, encode_envelope,
};
use crate::daemon::repl::want::{WantFramesOutcome, broadcast_to_frame, build_want_frames};

#[derive(Clone, Debug)]
pub struct PeerConfig {
    pub replica_id: ReplicaId,
    pub addr: String,
    pub role: Option<ReplicaRole>,
    pub allowed_namespaces: Option<Vec<NamespaceId>>,
}

#[derive(Clone)]
pub struct ReplicationManagerConfig {
    pub local_store: StoreIdentity,
    pub local_replica_id: ReplicaId,
    pub admission: AdmissionController,
    pub broadcaster: EventBroadcaster,
    pub peer_acks: Arc<Mutex<crate::daemon::repl::PeerAckTable>>,
    pub policies: BTreeMap<NamespaceId, NamespacePolicy>,
    pub roster: Option<ReplicaRoster>,
    pub peers: Vec<PeerConfig>,
    pub wal_reader: Option<WalRangeReader>,
    pub limits: crate::core::Limits,
    pub backoff: BackoffPolicy,
}

#[derive(Clone, Copy, Debug)]
pub struct BackoffPolicy {
    pub base: Duration,
    pub max: Duration,
}

#[derive(Clone)]
pub struct ReplicationManager<S> {
    local_store: StoreIdentity,
    local_replica_id: ReplicaId,
    store: SharedSessionStore<S>,
    admission: AdmissionController,
    broadcaster: EventBroadcaster,
    peer_acks: Arc<Mutex<crate::daemon::repl::PeerAckTable>>,
    policies: BTreeMap<NamespaceId, NamespacePolicy>,
    roster: Option<ReplicaRoster>,
    peers: Vec<PeerConfig>,
    wal_reader: Option<WalRangeReader>,
    limits: crate::core::Limits,
    backoff: BackoffPolicy,
}

pub struct ReplicationManagerHandle {
    shutdown: Arc<AtomicBool>,
    joins: Vec<JoinHandle<()>>,
}

impl ReplicationManagerHandle {
    pub fn shutdown(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        for join in self.joins {
            let _ = join.join();
        }
    }
}

impl<S> ReplicationManager<S>
where
    S: SessionStore + Send + 'static,
{
    pub fn new(store: SharedSessionStore<S>, config: ReplicationManagerConfig) -> Self {
        Self {
            local_store: config.local_store,
            local_replica_id: config.local_replica_id,
            store,
            admission: config.admission,
            broadcaster: config.broadcaster,
            peer_acks: config.peer_acks,
            policies: config.policies,
            roster: config.roster,
            peers: config.peers,
            wal_reader: config.wal_reader,
            limits: config.limits,
            backoff: config.backoff,
        }
    }

    pub fn start(self) -> ReplicationManagerHandle {
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut joins = Vec::new();

        for peer in self.peers.clone() {
            let Some(plan) = self.build_peer_plan(&peer) else {
                continue;
            };
            let runtime = PeerRuntime {
                local_store: self.local_store,
                local_replica_id: self.local_replica_id,
                store: self.store.clone(),
                admission: self.admission.clone(),
                broadcaster: self.broadcaster.clone(),
                peer_acks: Arc::clone(&self.peer_acks),
                wal_reader: self.wal_reader.clone(),
                limits: self.limits.clone(),
                backoff: self.backoff,
                shutdown: Arc::clone(&shutdown),
            };

            joins.push(thread::spawn(move || run_peer_loop(plan, runtime)));
        }

        ReplicationManagerHandle { shutdown, joins }
    }

    fn build_peer_plan(&self, peer: &PeerConfig) -> Option<PeerPlan> {
        let roster_entry = self
            .roster
            .as_ref()
            .and_then(|roster| roster.replica(&peer.replica_id));

        if self.roster.is_some() && roster_entry.is_none() {
            tracing::warn!(
                "replication peer {} not in roster; skipping",
                peer.replica_id
            );
            return None;
        }

        let role = roster_entry
            .map(|entry| entry.role)
            .or(peer.role)
            .unwrap_or(ReplicaRole::Peer);
        let allowed_namespaces = roster_entry
            .and_then(|entry| entry.allowed_namespaces.clone())
            .or_else(|| peer.allowed_namespaces.clone());

        let offered = eligible_namespaces(&self.policies, role, allowed_namespaces.as_ref());
        if offered.is_empty() {
            tracing::info!(
                "replication peer {} has no eligible namespaces; skipping",
                peer.replica_id
            );
            return None;
        }

        Some(PeerPlan {
            replica_id: peer.replica_id,
            addr: peer.addr.clone(),
            offered_namespaces: offered.clone(),
            requested_namespaces: offered,
        })
    }
}

#[derive(Clone, Debug)]
struct PeerPlan {
    replica_id: ReplicaId,
    addr: String,
    offered_namespaces: Vec<NamespaceId>,
    requested_namespaces: Vec<NamespaceId>,
}

struct PeerRuntime<S> {
    local_store: StoreIdentity,
    local_replica_id: ReplicaId,
    store: SharedSessionStore<S>,
    admission: AdmissionController,
    broadcaster: EventBroadcaster,
    peer_acks: Arc<Mutex<crate::daemon::repl::PeerAckTable>>,
    wal_reader: Option<WalRangeReader>,
    limits: crate::core::Limits,
    backoff: BackoffPolicy,
    shutdown: Arc<AtomicBool>,
}

fn eligible_namespaces(
    policies: &BTreeMap<NamespaceId, NamespacePolicy>,
    role: ReplicaRole,
    allowed: Option<&Vec<NamespaceId>>,
) -> Vec<NamespaceId> {
    let mut namespaces = Vec::new();
    let allowed_set = allowed.map(|list| list.iter().cloned().collect::<BTreeSet<_>>());

    for (namespace, policy) in policies {
        if !role_allows_policy(role, policy.replicate_mode) {
            continue;
        }
        if let Some(allowed) = &allowed_set
            && !allowed.contains(namespace)
        {
            continue;
        }
        namespaces.push(namespace.clone());
    }

    namespaces.sort();
    namespaces.dedup();
    namespaces
}

fn role_allows_policy(role: ReplicaRole, mode: ReplicateMode) -> bool {
    match mode {
        ReplicateMode::None => false,
        ReplicateMode::Anchors => role == ReplicaRole::Anchor,
        ReplicateMode::Peers => matches!(role, ReplicaRole::Anchor | ReplicaRole::Peer),
        ReplicateMode::P2p => true,
    }
}

fn run_peer_loop<S>(plan: PeerPlan, runtime: PeerRuntime<S>)
where
    S: SessionStore + Send + 'static,
{
    let mut backoff = Backoff::new(runtime.backoff);

    while !runtime.shutdown.load(Ordering::Relaxed) {
        let connect_start = Instant::now();
        match TcpStream::connect(&plan.addr) {
            Ok(stream) => {
                backoff.reset();
                if let Err(err) = run_outbound_session(stream, &plan, &runtime) {
                    tracing::warn!("replication peer {} disconnected: {err}", plan.replica_id);
                }
            }
            Err(err) => {
                tracing::warn!("replication peer {} connect failed: {err}", plan.replica_id);
            }
        }

        if runtime.shutdown.load(Ordering::Relaxed) {
            break;
        }

        let delay = backoff.next_delay();
        let elapsed = connect_start.elapsed();
        if delay > elapsed {
            thread::sleep(delay - elapsed);
        }
    }
}

#[derive(Debug, Error)]
enum PeerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame error: {0}")]
    Frame(#[from] FrameError),
    #[error("encode error: {0}")]
    Encode(#[from] crate::daemon::repl::proto::ProtoEncodeError),
    #[error("broadcast error: {0}")]
    Broadcast(#[from] BroadcastError),
}

#[derive(Clone, Debug)]
enum InboundMessage {
    Message(ReplMessage),
    Terminated { payload: Option<ErrorPayload> },
}

fn run_outbound_session<S>(
    stream: TcpStream,
    plan: &PeerPlan,
    runtime: &PeerRuntime<S>,
) -> Result<(), PeerError>
where
    S: SessionStore + Send + 'static,
{
    let shutdown_stream = stream;
    shutdown_stream.set_nodelay(true)?;

    let mut store = runtime.store.clone();
    let admission = runtime.admission.clone();
    let broadcaster = runtime.broadcaster.clone();
    let peer_acks = Arc::clone(&runtime.peer_acks);
    let limits = runtime.limits.clone();
    let shutdown = runtime.shutdown.clone();
    let local_store = runtime.local_store;
    let local_replica_id = runtime.local_replica_id;

    let reader_stream = shutdown_stream.try_clone()?;
    let writer_stream = shutdown_stream.try_clone()?;
    let mut reader = FrameReader::new(reader_stream, limits.max_frame_bytes);
    let mut writer = FrameWriter::new(writer_stream, limits.max_frame_bytes);

    let (inbound_tx, inbound_rx) = crossbeam::channel::unbounded::<InboundMessage>();
    let reader_shutdown = shutdown.clone();
    let reader_limits = limits.clone();
    let reader_handle = thread::spawn(move || {
        run_reader_loop(&mut reader, inbound_tx, reader_shutdown, reader_limits);
    });

    let subscription = broadcaster.subscribe(subscriber_limits(&limits))?;
    let (event_tx, event_rx) = crossbeam::channel::unbounded::<BroadcastEvent>();
    let event_shutdown = shutdown.clone();
    let event_handle = thread::spawn(move || {
        run_event_forwarder(subscription, event_tx, event_shutdown);
    });

    let mut config = SessionConfig::new(local_store, local_replica_id, &limits);
    config.requested_namespaces = plan.requested_namespaces.clone();
    config.offered_namespaces = plan.offered_namespaces.clone();

    let mut session = Session::new(SessionRole::Outbound, config, limits.clone(), admission);

    if let Some(action) = session.begin_handshake(&store, now_ms()) {
        apply_action(&mut writer, &session, action)?;
    }

    let mut accepted_set = BTreeSet::new();
    let mut streaming = false;
    let mut sent_hot_cache = false;
    let mut pending_events =
        PendingEvents::new(limits.max_event_batch_events, limits.max_event_batch_bytes);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let tick = crossbeam::channel::after(Duration::from_millis(50));
        crossbeam::select! {
            recv(inbound_rx) -> msg => {
                let msg = match msg {
                    Ok(msg) => msg,
                    Err(_) => break,
                };

                match msg {
                    InboundMessage::Message(msg) => {
                        let actions = session.handle_message(msg, &mut store, now_ms());
                        for action in actions {
                            if let SessionAction::PeerAck(ack) = &action
                                && let Err(err) =
                                    update_peer_ack(&store, &peer_acks, plan.replica_id, ack)
                            {
                                tracing::warn!("peer ack update failed: {err}");
                            }

                            if let SessionAction::PeerWant(want) = &action {
                                if let Err(err) = handle_want(
                                    &mut writer,
                                    &session,
                                    want,
                                    &broadcaster,
                                    runtime.wal_reader.as_ref(),
                                    &limits,
                                    Some(&accepted_set),
                                ) {
                                    tracing::warn!("peer want handling failed: {err}");
                                    return Err(err);
                                }
                            } else if apply_action(&mut writer, &session, action)? {
                                return Ok(());
                            }
                        }
                    }
                    InboundMessage::Terminated { payload } => {
                        if let Some(payload) = payload {
                            send_payload(&mut writer, &session, ReplMessage::Error(payload))?;
                        }
                        break;
                    }
                }
            }
            recv(event_rx) -> msg => {
                let event = match msg {
                    Ok(event) => event,
                    Err(_) => break,
                };

                if !streaming {
                    let drop = pending_events.push(event);
                    if drop.dropped_any() {
                        let dropped_events = drop.total_events();
                        let dropped_bytes = drop.total_bytes();
                        metrics::repl_pending_dropped_events("outbound", dropped_events);
                        metrics::repl_pending_dropped_bytes("outbound", dropped_bytes);
                        tracing::warn!(
                            target: "repl",
                            direction = "outbound",
                            dropped_events,
                            dropped_bytes,
                            dropped_new_event = drop.dropped_new_event(),
                            pending_events = pending_events.len(),
                            pending_bytes = pending_events.bytes(),
                            max_events = pending_events.max_events(),
                            max_bytes = pending_events.max_bytes(),
                            "replication pending queue overflow; dropping events"
                        );
                    }
                    continue;
                }
                if !accepted_set.contains(&event.namespace) {
                    continue;
                }
                let frame = broadcast_to_frame(event);
                send_events(&mut writer, &session, vec![frame], &limits)?;
            }
            recv(tick) -> _ => {}
        }

        if !streaming && session.phase() == SessionPhase::Streaming {
            streaming = true;
            if let Some(peer) = session.peer() {
                accepted_set = peer.accepted_namespaces.iter().cloned().collect();
                tracing::info!(
                    target: "repl",
                    direction = "outbound",
                    peer_replica_id = %peer.replica_id,
                    auth_identity = "none",
                    requested_namespaces = ?plan.requested_namespaces,
                    offered_namespaces = ?plan.offered_namespaces,
                    accepted_namespaces = ?peer.accepted_namespaces,
                    incoming_namespaces = ?peer.incoming_namespaces,
                    live_stream = peer.live_stream_enabled,
                    "replication handshake accepted"
                );
            }
        }
        if streaming && !pending_events.is_empty() {
            let frames = pending_events
                .drain()
                .filter(|event| accepted_set.contains(&event.namespace))
                .map(broadcast_to_frame)
                .collect::<Vec<_>>();
            send_events(&mut writer, &session, frames, &limits)?;
        }
        if streaming && !sent_hot_cache {
            send_hot_cache(&mut writer, &session, &broadcaster, &accepted_set, &limits)?;
            sent_hot_cache = true;
        }
    }

    let _ = shutdown_stream.shutdown(std::net::Shutdown::Both);
    let _ = reader_handle.join();
    let _ = event_handle.join();

    Ok(())
}

fn run_reader_loop(
    reader: &mut FrameReader<TcpStream>,
    inbound_tx: Sender<InboundMessage>,
    shutdown: Arc<AtomicBool>,
    limits: crate::core::Limits,
) {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            let _ = inbound_tx.send(InboundMessage::Terminated { payload: None });
            break;
        }

        match reader.read_next() {
            Ok(Some(bytes)) => match decode_envelope(&bytes, &limits) {
                Ok(envelope) => {
                    let _ = inbound_tx.send(InboundMessage::Message(envelope.message));
                }
                Err(err) => {
                    let payload = err.as_error_payload();
                    let _ = inbound_tx.send(InboundMessage::Terminated { payload });
                    break;
                }
            },
            Ok(None) => {
                let _ = inbound_tx.send(InboundMessage::Terminated { payload: None });
                break;
            }
            Err(err) => {
                let payload = err.as_error_payload();
                let _ = inbound_tx.send(InboundMessage::Terminated { payload });
                break;
            }
        }
    }
}

fn run_event_forwarder(
    subscription: EventSubscription,
    event_tx: Sender<BroadcastEvent>,
    shutdown: Arc<AtomicBool>,
) {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        match subscription.try_recv() {
            Ok(event) => {
                if event_tx.send(event).is_err() {
                    break;
                }
            }
            Err(crossbeam::channel::TryRecvError::Empty) => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(crossbeam::channel::TryRecvError::Disconnected) => break,
        }
    }
}

fn apply_action(
    writer: &mut FrameWriter<TcpStream>,
    session: &Session,
    action: SessionAction,
) -> Result<bool, PeerError> {
    match action {
        SessionAction::Send(message) => {
            send_payload(writer, session, message)?;
            Ok(false)
        }
        SessionAction::Close { error } => {
            if let Some(error) = error {
                send_payload(writer, session, ReplMessage::Error(error))?;
            }
            Ok(true)
        }
        SessionAction::PeerAck(_) | SessionAction::PeerWant(_) | SessionAction::PeerError(_) => {
            Ok(false)
        }
    }
}

fn send_payload(
    writer: &mut FrameWriter<TcpStream>,
    session: &Session,
    message: ReplMessage,
) -> Result<(), PeerError> {
    let version = session
        .peer()
        .map(|peer| peer.protocol_version)
        .unwrap_or(PROTOCOL_VERSION_V1);
    let envelope = ReplEnvelope { version, message };
    let bytes = encode_envelope(&envelope)?;
    writer.write_frame_with_limit(&bytes, session.negotiated_max_frame_bytes())?;
    Ok(())
}

fn send_events(
    writer: &mut FrameWriter<TcpStream>,
    session: &Session,
    frames: Vec<EventFrameV1>,
    limits: &crate::core::Limits,
) -> Result<(), PeerError> {
    if frames.is_empty() {
        return Ok(());
    }

    let max_frame_bytes = session.negotiated_max_frame_bytes();
    let mut batch = Vec::new();
    let mut batch_bytes = 0usize;

    for frame in frames {
        let frame_bytes = frame.bytes.len();
        if !batch.is_empty()
            && (batch.len() >= limits.max_event_batch_events
                || batch_bytes.saturating_add(frame_bytes) > limits.max_event_batch_bytes)
        {
            metrics::repl_events_out(batch.len());
            send_payload(
                writer,
                session,
                ReplMessage::Events(Events { events: batch }),
            )?;
            batch = Vec::new();
            batch_bytes = 0;
        }

        batch_bytes = batch_bytes.saturating_add(frame_bytes);
        batch.push(frame);

        let envelope_bytes = events_envelope_len(session, &batch)?;
        if envelope_bytes > max_frame_bytes {
            let frame = batch.pop().expect("batch not empty");
            if !batch.is_empty() {
                metrics::repl_events_out(batch.len());
                send_payload(
                    writer,
                    session,
                    ReplMessage::Events(Events { events: batch }),
                )?;
            }
            batch = vec![frame];
            batch_bytes = frame_bytes;
            let single_len = events_envelope_len(session, &batch)?;
            if single_len > max_frame_bytes {
                return Err(PeerError::Frame(FrameError::FrameTooLarge {
                    max_frame_bytes,
                    got_bytes: single_len,
                }));
            }
        }
    }

    if !batch.is_empty() {
        metrics::repl_events_out(batch.len());
        send_payload(
            writer,
            session,
            ReplMessage::Events(Events { events: batch }),
        )?;
    }

    Ok(())
}

fn events_envelope_len(session: &Session, batch: &[EventFrameV1]) -> Result<usize, PeerError> {
    let version = session
        .peer()
        .map(|peer| peer.protocol_version)
        .unwrap_or(PROTOCOL_VERSION_V1);
    let envelope = ReplEnvelope {
        version,
        message: ReplMessage::Events(Events {
            events: batch.to_vec(),
        }),
    };
    let bytes = encode_envelope(&envelope)?;
    Ok(bytes.len())
}

fn send_hot_cache(
    writer: &mut FrameWriter<TcpStream>,
    session: &Session,
    broadcaster: &EventBroadcaster,
    allowed_set: &BTreeSet<NamespaceId>,
    limits: &crate::core::Limits,
) -> Result<(), PeerError> {
    let cache = broadcaster.hot_cache()?;
    let frames = cache
        .into_iter()
        .filter(|event| allowed_set.contains(&event.namespace))
        .map(broadcast_to_frame)
        .collect::<Vec<_>>();
    send_events(writer, session, frames, limits)
}

fn update_peer_ack(
    store: &impl SessionStore,
    peer_acks: &Arc<Mutex<crate::daemon::repl::PeerAckTable>>,
    peer: ReplicaId,
    ack: &Ack,
) -> Result<(), Box<crate::daemon::repl::PeerAckError>> {
    let now_ms = now_ms();
    let mut table = peer_acks.lock().expect("peer ack lock poisoned");
    table.update_peer(
        peer,
        &ack.durable,
        ack.durable_heads.as_ref(),
        ack.applied.as_ref(),
        ack.applied_heads.as_ref(),
        now_ms,
    )?;
    let namespaces: Vec<NamespaceId> = ack.durable.keys().cloned().collect();
    let snapshot = store.watermark_snapshot(&namespaces);
    emit_peer_lag(peer, &snapshot.durable, &ack.durable);
    Ok(())
}

fn emit_peer_lag(peer: ReplicaId, local: &WatermarkMap, ack: &WatermarkMap) {
    for (namespace, origins) in local {
        let mut max_lag = 0u64;
        let acked = ack.get(namespace);
        for (origin, local_seq) in origins {
            let acked_seq = acked.and_then(|map| map.get(origin)).copied().unwrap_or(0);
            let lag = local_seq.saturating_sub(acked_seq);
            max_lag = max_lag.max(lag);
        }
        metrics::set_repl_peer_lag(peer, namespace, max_lag);
    }
}

fn handle_want(
    writer: &mut FrameWriter<TcpStream>,
    session: &Session,
    want: &Want,
    broadcaster: &EventBroadcaster,
    wal_reader: Option<&WalRangeReader>,
    limits: &crate::core::Limits,
    allowed_set: Option<&BTreeSet<NamespaceId>>,
) -> Result<(), PeerError> {
    if want.want.is_empty() {
        return Ok(());
    }

    let cache = broadcaster.hot_cache()?;
    let outcome = match build_want_frames(want, cache, wal_reader, limits, allowed_set) {
        Ok(outcome) => outcome,
        Err(err) => {
            let payload = err.as_error_payload();
            send_payload(writer, session, ReplMessage::Error(payload))?;
            return Ok(());
        }
    };

    match outcome {
        WantFramesOutcome::Frames(frames) => send_events(writer, session, frames, limits),
        WantFramesOutcome::BootstrapRequired { namespaces } => {
            let payload =
                ErrorPayload::new(ErrorCode::BootstrapRequired, "bootstrap required", false)
                    .with_details(BootstrapRequiredDetails {
                        namespaces: namespaces.into_iter().collect(),
                        reason: SnapshotRangeReason::RangeMissing,
                    });
            send_payload(writer, session, ReplMessage::Error(payload))?;
            Ok(())
        }
    }
}

fn subscriber_limits(limits: &crate::core::Limits) -> SubscriberLimits {
    let max_events = limits.max_event_batch_events.max(1);
    let max_bytes = limits.max_event_batch_bytes.max(1);
    SubscriberLimits::new(max_events, max_bytes).expect("subscriber limits")
}

fn now_ms() -> u64 {
    crate::core::WallClock::now().0
}

struct Backoff {
    base: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    fn new(policy: BackoffPolicy) -> Self {
        Self {
            base: policy.base,
            max: policy.max,
            current: policy.base,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        let next = self.current.checked_mul(2).unwrap_or(self.max);
        self.current = std::cmp::min(next, self.max);
        delay
    }

    fn reset(&mut self) {
        self.current = self.base;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crossbeam::channel::Receiver;
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant};
    use uuid::Uuid;

    use crate::core::{
        Applied, Canonical, Durable, ErrorCode, ErrorPayload, EventBytes, EventFrameV1, EventId,
        HeadStatus, NamespaceId, NamespacePolicy, ReplicaId, Seq0, Seq1, Sha256, StoreEpoch,
        StoreId, StoreIdentity, Watermark,
    };
    use crate::daemon::repl::IngestOutcome;
    use crate::daemon::repl::WatermarkSnapshot;
    use crate::daemon::repl::proto::{WatermarkHeads, WatermarkMap, Welcome};

    #[derive(Default)]
    struct TestStore;

    impl SessionStore for TestStore {
        fn watermark_snapshot(&self, _namespaces: &[NamespaceId]) -> WatermarkSnapshot {
            WatermarkSnapshot {
                durable: WatermarkMap::new(),
                durable_heads: WatermarkHeads::new(),
                applied: WatermarkMap::new(),
                applied_heads: WatermarkHeads::new(),
            }
        }

        fn lookup_event_sha(
            &self,
            _eid: &EventId,
        ) -> Result<Option<Sha256>, crate::core::EventShaLookupError> {
            Ok(None)
        }

        fn ingest_remote_batch(
            &mut self,
            _namespace: &NamespaceId,
            _origin: &ReplicaId,
            batch: &[crate::core::VerifiedEvent<crate::core::PrevVerified>],
            _now_ms: u64,
        ) -> Result<IngestOutcome, Box<ErrorPayload>> {
            let Some(last) = batch.last() else {
                let durable = Watermark::<Durable>::genesis();
                let applied = Watermark::<Applied>::genesis();
                return Ok(IngestOutcome { durable, applied });
            };
            let seq = Seq0::new(last.seq().get());
            let head = HeadStatus::Known(last.sha256.0);
            let watermark = Watermark::<Durable>::new(seq, head).expect("watermark");
            let applied = Watermark::<Applied>::new(seq, head).expect("watermark");
            Ok(IngestOutcome {
                durable: watermark,
                applied,
            })
        }
    }

    fn test_limits() -> crate::core::Limits {
        let mut limits = crate::core::Limits::default();
        limits.max_event_batch_events = 4;
        limits.max_event_batch_bytes = 1024;
        limits
    }

    fn test_policy() -> BTreeMap<NamespaceId, NamespacePolicy> {
        let mut policies = BTreeMap::new();
        policies.insert(NamespaceId::core(), NamespacePolicy::core_default());
        policies
    }

    fn test_peer_config(replica_id: ReplicaId, addr: String) -> PeerConfig {
        PeerConfig {
            replica_id,
            addr,
            role: Some(ReplicaRole::Peer),
            allowed_namespaces: None,
        }
    }

    fn spawn_peer_listener(
        peer_store: StoreIdentity,
        peer_replica: ReplicaId,
        respond_with_welcome: bool,
        accepted_override: Option<Vec<NamespaceId>>,
    ) -> (std::net::SocketAddr, Receiver<ReplMessage>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = crossbeam::channel::unbounded();

        thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let reader_stream = stream.try_clone().expect("clone");
                let mut reader = FrameReader::new(reader_stream, 1024 * 1024);
                let mut writer = FrameWriter::new(stream, 1024 * 1024);
                let mut welcome_sent = false;
                loop {
                    let Some(bytes) = (match reader.read_next() {
                        Ok(Some(bytes)) => Some(bytes),
                        Ok(None) => None,
                        Err(_) => None,
                    }) else {
                        break;
                    };
                    let envelope =
                        decode_envelope(&bytes, &crate::core::Limits::default()).expect("decode");
                    tx.send(envelope.message.clone()).expect("send");

                    if respond_with_welcome && !welcome_sent {
                        if let ReplMessage::Hello(hello) = envelope.message {
                            let mut config = SessionConfig::new(
                                peer_store,
                                peer_replica,
                                &crate::core::Limits::default(),
                            );
                            config.offered_namespaces = hello.requested_namespaces.clone();
                            config.requested_namespaces = hello.offered_namespaces.clone();
                            let mut session = Session::new(
                                SessionRole::Inbound,
                                config,
                                crate::core::Limits::default(),
                                AdmissionController::new(&crate::core::Limits::default()),
                            );
                            let actions = session.handle_message(
                                ReplMessage::Hello(hello),
                                &mut TestStore::default(),
                                now_ms(),
                            );
                            for action in actions {
                                if let SessionAction::Send(message) = action {
                                    let mut message = message;
                                    if let Some(override_namespaces) = accepted_override.clone() {
                                        if let ReplMessage::Welcome(ref mut welcome) = message {
                                            welcome.accepted_namespaces = override_namespaces;
                                        }
                                    }
                                    let envelope = ReplEnvelope {
                                        version: PROTOCOL_VERSION_V1,
                                        message,
                                    };
                                    let bytes = encode_envelope(&envelope).expect("encode");
                                    writer.write_frame(&bytes).expect("write");
                                }
                            }
                            welcome_sent = true;
                        }
                    }
                }
            }
        });

        (addr, rx)
    }

    #[test]
    fn backoff_exponentially_grows() {
        let policy = BackoffPolicy {
            base: Duration::from_millis(10),
            max: Duration::from_millis(40),
        };
        let mut backoff = Backoff::new(policy);
        assert_eq!(backoff.next_delay(), Duration::from_millis(10));
        assert_eq!(backoff.next_delay(), Duration::from_millis(20));
        assert_eq!(backoff.next_delay(), Duration::from_millis(40));
        assert_eq!(backoff.next_delay(), Duration::from_millis(40));
    }

    #[test]
    fn manager_connects_and_sends_hello() {
        let local_store = StoreIdentity::new(
            crate::core::StoreId::new(Uuid::from_bytes([1u8; 16])),
            crate::core::StoreEpoch::ZERO,
        );
        let local_replica = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let peer_replica = ReplicaId::new(Uuid::from_bytes([3u8; 16]));
        let (addr, rx) = spawn_peer_listener(local_store, peer_replica, false, None);
        let addr = addr.to_string();

        let config = ReplicationManagerConfig {
            local_store,
            local_replica_id: local_replica,
            admission: AdmissionController::new(&test_limits()),
            broadcaster: EventBroadcaster::new(crate::daemon::broadcast::BroadcasterLimits {
                max_subscribers: 4,
                hot_cache_max_events: 16,
                hot_cache_max_bytes: 1024,
            }),
            peer_acks: Arc::new(Mutex::new(crate::daemon::repl::PeerAckTable::new())),
            policies: test_policy(),
            roster: None,
            peers: vec![test_peer_config(peer_replica, addr)],
            wal_reader: None,
            limits: test_limits(),
            backoff: BackoffPolicy {
                base: Duration::from_millis(5),
                max: Duration::from_millis(10),
            },
        };
        let manager =
            ReplicationManager::new(SharedSessionStore::new(TestStore::default()), config);

        let handle = manager.start();
        let msg = rx.recv_timeout(Duration::from_secs(1)).expect("hello");
        match msg {
            ReplMessage::Hello(hello) => {
                assert_eq!(hello.sender_replica_id, local_replica);
                assert_eq!(hello.store_id, local_store.store_id);
            }
            _ => panic!("expected hello"),
        }

        handle.shutdown();
    }

    #[test]
    fn manager_fans_out_events_and_respects_policy() {
        let local_store = StoreIdentity::new(
            crate::core::StoreId::new(Uuid::from_bytes([4u8; 16])),
            crate::core::StoreEpoch::ZERO,
        );
        let local_replica = ReplicaId::new(Uuid::from_bytes([5u8; 16]));
        let peer_replica = ReplicaId::new(Uuid::from_bytes([6u8; 16]));
        let (addr, rx) = spawn_peer_listener(local_store, peer_replica, true, None);
        let addr = addr.to_string();

        let broadcaster = EventBroadcaster::new(crate::daemon::broadcast::BroadcasterLimits {
            max_subscribers: 4,
            hot_cache_max_events: 16,
            hot_cache_max_bytes: 1024,
        });

        let mut policies = BTreeMap::new();
        policies.insert(NamespaceId::core(), NamespacePolicy::core_default());
        let mut tmp_policy = NamespacePolicy::tmp_default();
        tmp_policy.replicate_mode = ReplicateMode::None;
        let tmp = NamespaceId::parse("tmp").unwrap();
        policies.insert(tmp.clone(), tmp_policy);

        let config = ReplicationManagerConfig {
            local_store,
            local_replica_id: local_replica,
            admission: AdmissionController::new(&test_limits()),
            broadcaster: broadcaster.clone(),
            peer_acks: Arc::new(Mutex::new(crate::daemon::repl::PeerAckTable::new())),
            policies,
            roster: None,
            peers: vec![test_peer_config(peer_replica, addr)],
            wal_reader: None,
            limits: test_limits(),
            backoff: BackoffPolicy {
                base: Duration::from_millis(5),
                max: Duration::from_millis(10),
            },
        };
        let manager =
            ReplicationManager::new(SharedSessionStore::new(TestStore::default()), config);

        let handle = manager.start();
        rx.recv_timeout(Duration::from_secs(1)).expect("hello");

        let core_event = BroadcastEvent::new(
            EventId::new(
                local_replica,
                NamespaceId::core(),
                Seq1::from_u64(1).unwrap(),
            ),
            Sha256([1u8; 32]),
            None,
            EventBytes::<Canonical>::new(Bytes::from_static(b"core")),
        );
        let tmp_event = BroadcastEvent::new(
            EventId::new(local_replica, tmp.clone(), Seq1::from_u64(2).unwrap()),
            Sha256([2u8; 32]),
            None,
            EventBytes::<Canonical>::new(Bytes::from_static(b"tmp")),
        );

        broadcaster.publish(core_event).unwrap();
        broadcaster.publish(tmp_event).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut received_events = None;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let received = rx.recv_timeout(remaining).expect("message");
            if let ReplMessage::Events(events) = received {
                received_events = Some(events);
                break;
            }
        }
        let events = received_events.expect("events");
        assert_eq!(events.events.len(), 1);
        assert_eq!(events.events[0].eid.namespace, NamespaceId::core());

        handle.shutdown();
    }

    #[test]
    fn send_events_respects_negotiated_max_frame_bytes() {
        let limits = test_limits();
        let local_store = StoreIdentity::new(
            StoreId::new(Uuid::from_bytes([20u8; 16])),
            StoreEpoch::ZERO,
        );
        let local_replica = ReplicaId::new(Uuid::from_bytes([21u8; 16]));
        let peer_replica = ReplicaId::new(Uuid::from_bytes([22u8; 16]));

        let mut config = SessionConfig::new(local_store, local_replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()];
        config.offered_namespaces = vec![NamespaceId::core()];
        let mut session = Session::new(
            SessionRole::Outbound,
            config,
            limits.clone(),
            AdmissionController::new(&limits),
        );
        let _ = session.begin_handshake(&TestStore::default(), now_ms());
        let welcome = Welcome {
            protocol_version: PROTOCOL_VERSION_V1,
            store_id: local_store.store_id,
            store_epoch: local_store.store_epoch,
            receiver_replica_id: peer_replica,
            welcome_nonce: 1,
            accepted_namespaces: vec![NamespaceId::core()],
            receiver_seen_durable: WatermarkMap::new(),
            receiver_seen_durable_heads: None,
            receiver_seen_applied: None,
            receiver_seen_applied_heads: None,
            live_stream_enabled: true,
            max_frame_bytes: 200,
        };
        session.handle_message(ReplMessage::Welcome(welcome), &mut TestStore::default(), now_ms());
        assert!(matches!(session.phase(), SessionPhase::Streaming));

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = crossbeam::channel::bounded(1);

        std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            let mut reader = FrameReader::new(stream, 1024 * 1024);
            let mut counts = Vec::new();
            while let Ok(Some(frame)) = reader.read_next() {
                let envelope =
                    decode_envelope(&frame, &crate::core::Limits::default()).expect("decode");
                if let ReplMessage::Events(events) = envelope.message {
                    counts.push(events.events.len());
                    if counts.len() == 2 {
                        break;
                    }
                }
            }
            let _ = tx.send(counts);
        });

        let stream = TcpStream::connect(addr).expect("connect");
        let mut writer = FrameWriter::new(stream, limits.max_frame_bytes);

        let origin = ReplicaId::new(Uuid::from_bytes([23u8; 16]));
        let namespace = NamespaceId::core();
        let max_frame = session.negotiated_max_frame_bytes();
        let mut payload_len = 10usize;
        let (frame1, frame2) = loop {
            let f1 = make_frame(namespace.clone(), origin, 1, payload_len);
            let f2 = make_frame(namespace.clone(), origin, 2, payload_len);
            let len_single = events_envelope_len(&session, &[f1.clone()]).expect("len");
            let len_double = events_envelope_len(&session, &[f1.clone(), f2.clone()]).expect("len");
            if len_single < max_frame && len_double > max_frame {
                break (f1, f2);
            }
            payload_len = payload_len.saturating_add(10);
            if payload_len > 10_000 {
                panic!("unable to craft frame sizes");
            }
        };

        send_events(&mut writer, &session, vec![frame1, frame2], &limits).expect("send");

        let counts = rx.recv_timeout(Duration::from_secs(1)).expect("counts");
        assert_eq!(counts, vec![1, 1]);
    }

    fn make_frame(
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: u64,
        payload_len: usize,
    ) -> EventFrameV1 {
        let bytes = EventBytes::<Opaque>::new(Bytes::from(vec![0u8; payload_len.max(1)]));
        EventFrameV1 {
            eid: EventId::new(origin, namespace, Seq1::from_u64(seq).unwrap()),
            sha256: Sha256([seq as u8; 32]),
            prev_sha256: None,
            bytes,
        }
    }

    #[test]
    fn manager_filters_to_accepted_namespaces() {
        let local_store = StoreIdentity::new(
            crate::core::StoreId::new(Uuid::from_bytes([12u8; 16])),
            crate::core::StoreEpoch::ZERO,
        );
        let local_replica = ReplicaId::new(Uuid::from_bytes([13u8; 16]));
        let peer_replica = ReplicaId::new(Uuid::from_bytes([14u8; 16]));
        let accepted = Some(vec![NamespaceId::core()]);
        let (addr, rx) = spawn_peer_listener(local_store, peer_replica, true, accepted);
        let addr = addr.to_string();

        let broadcaster = EventBroadcaster::new(crate::daemon::broadcast::BroadcasterLimits {
            max_subscribers: 4,
            hot_cache_max_events: 16,
            hot_cache_max_bytes: 1024,
        });

        let mut policies = BTreeMap::new();
        policies.insert(NamespaceId::core(), NamespacePolicy::core_default());
        let mut tmp_policy = NamespacePolicy::tmp_default();
        tmp_policy.replicate_mode = ReplicateMode::Peers;
        let tmp = NamespaceId::parse("tmp").unwrap();
        policies.insert(tmp.clone(), tmp_policy);

        let config = ReplicationManagerConfig {
            local_store,
            local_replica_id: local_replica,
            admission: AdmissionController::new(&test_limits()),
            broadcaster: broadcaster.clone(),
            peer_acks: Arc::new(Mutex::new(crate::daemon::repl::PeerAckTable::new())),
            policies,
            roster: None,
            peers: vec![test_peer_config(peer_replica, addr)],
            wal_reader: None,
            limits: test_limits(),
            backoff: BackoffPolicy {
                base: Duration::from_millis(5),
                max: Duration::from_millis(10),
            },
        };
        let manager =
            ReplicationManager::new(SharedSessionStore::new(TestStore::default()), config);

        let handle = manager.start();
        rx.recv_timeout(Duration::from_secs(1)).expect("hello");

        let core_event = BroadcastEvent::new(
            EventId::new(
                local_replica,
                NamespaceId::core(),
                Seq1::from_u64(1).unwrap(),
            ),
            Sha256([10u8; 32]),
            None,
            EventBytes::<Canonical>::new(Bytes::from_static(b"core")),
        );
        let tmp_event = BroadcastEvent::new(
            EventId::new(local_replica, tmp.clone(), Seq1::from_u64(2).unwrap()),
            Sha256([20u8; 32]),
            None,
            EventBytes::<Canonical>::new(Bytes::from_static(b"tmp")),
        );

        broadcaster.publish(core_event).unwrap();
        broadcaster.publish(tmp_event).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut received_events = None;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let received = rx.recv_timeout(remaining).expect("message");
            if let ReplMessage::Events(events) = received {
                received_events = Some(events);
                break;
            }
        }
        let events = received_events.expect("events");
        assert_eq!(events.events.len(), 1);
        assert_eq!(events.events[0].eid.namespace, NamespaceId::core());

        handle.shutdown();
    }

    #[test]
    fn reconnects_after_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().unwrap().to_string();
        let local_store = StoreIdentity::new(
            crate::core::StoreId::new(Uuid::from_bytes([8u8; 16])),
            crate::core::StoreEpoch::ZERO,
        );
        let local_replica = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
        let peer_replica = ReplicaId::new(Uuid::from_bytes([10u8; 16]));

        let config = ReplicationManagerConfig {
            local_store,
            local_replica_id: local_replica,
            admission: AdmissionController::new(&test_limits()),
            broadcaster: EventBroadcaster::new(crate::daemon::broadcast::BroadcasterLimits {
                max_subscribers: 4,
                hot_cache_max_events: 16,
                hot_cache_max_bytes: 1024,
            }),
            peer_acks: Arc::new(Mutex::new(crate::daemon::repl::PeerAckTable::new())),
            policies: test_policy(),
            roster: None,
            peers: vec![test_peer_config(peer_replica, addr)],
            wal_reader: None,
            limits: test_limits(),
            backoff: BackoffPolicy {
                base: Duration::from_millis(20),
                max: Duration::from_millis(40),
            },
        };
        let manager =
            ReplicationManager::new(SharedSessionStore::new(TestStore::default()), config);

        let handle = manager.start();
        let (seen_tx, seen_rx) = crossbeam::channel::bounded(2);
        thread::spawn(move || {
            for _ in 0..2 {
                let (stream, _) = listener.accept().expect("accept");
                let reader_stream = stream.try_clone().expect("clone");
                let mut reader = FrameReader::new(reader_stream, 1024 * 1024);
                let mut writer = FrameWriter::new(stream, 1024 * 1024);
                let _ = reader.read_next();
                let payload = ErrorPayload::new(ErrorCode::Overloaded, "overloaded", true);
                let envelope = ReplEnvelope {
                    version: PROTOCOL_VERSION_V1,
                    message: ReplMessage::Error(payload),
                };
                let bytes = encode_envelope(&envelope).expect("encode");
                writer.write_frame(&bytes).expect("write");
                let _ = seen_tx.send(Instant::now());
            }
        });

        let first_at = seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first connection");
        let second_at = seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second connection");
        assert!(second_at.duration_since(first_at) >= Duration::from_millis(20));
        handle.shutdown();
    }
}
