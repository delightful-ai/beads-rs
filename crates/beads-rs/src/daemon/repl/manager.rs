//! Outbound replication manager and peer lifecycle.

use std::collections::{BTreeMap, BTreeSet};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam::channel::Sender;
use thiserror::Error;

#[cfg(test)]
use std::sync::OnceLock;

use crate::core::error::details::{BootstrapRequiredDetails, SnapshotRangeReason};
use crate::core::{
    Durable, ErrorPayload, EventFrameError, EventFrameV1, NamespaceId, NamespacePolicy,
    ProtocolErrorCode, ReplicaId, ReplicaRole, ReplicaRoster, ReplicateMode, Seq0, StoreIdentity,
    VerifiedEventFrame,
};
use crate::daemon::admission::AdmissionController;
use crate::daemon::broadcast::{
    BroadcastError, BroadcastEvent, EventBroadcaster, EventSubscription, SubscriberLimits,
};
use crate::daemon::io_budget::TokenBucket;
use crate::daemon::metrics;
use crate::daemon::repl::keepalive::{KeepaliveDecision, KeepaliveTracker};
use crate::daemon::repl::pending::PendingEvents;
#[cfg(test)]
use crate::daemon::repl::proto::PROTOCOL_VERSION_V1;
use crate::daemon::repl::proto::{Events, Want, WatermarkState};
use crate::daemon::repl::session::{
    Outbound, OutboundConnecting, Session, SessionState, SessionWire, Streaming, StreamingLive,
    handle_outbound_message,
};
use crate::daemon::repl::want::{WantFramesOutcome, broadcast_to_frame, build_want_frames};
use crate::daemon::repl::{
    FrameError, FrameLimitState, FrameReader, FrameWriter, ReplEnvelope, ReplMessage,
    SessionAction, SessionConfig, SessionStore, SharedSessionStore, ValidatedAck, WalRangeReader,
    WireReplMessage, decode_envelope, encode_envelope,
};
use crate::daemon::wal::ReplicaDurabilityRole;

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

            let peer_span = tracing::Span::current();
            joins.push(thread::spawn(move || {
                peer_span.in_scope(|| run_peer_loop(plan, runtime));
            }));
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
            .map(|entry| entry.role())
            .or(peer.role)
            .unwrap_or(ReplicaRole::Peer);
        let durability_eligible = roster_entry
            .map(|entry| entry.durability_eligible())
            .unwrap_or(role != ReplicaRole::Observer);
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

        let durability_role = match ReplicaDurabilityRole::try_from((role, durability_eligible)) {
            Ok(role) => role,
            Err(err) => {
                tracing::warn!(
                    peer_replica_id = %peer.replica_id,
                    role = ?role,
                    durability_eligible,
                    "invalid replica durability role: {err}"
                );
                return None;
            }
        };

        Some(PeerPlan {
            replica_id: peer.replica_id,
            addr: peer.addr.clone(),
            offered_namespaces: offered.clone(),
            requested_namespaces: offered,
            durability_role,
        })
    }
}

#[derive(Clone, Debug)]
struct PeerPlan {
    replica_id: ReplicaId,
    addr: String,
    offered_namespaces: Vec<NamespaceId>,
    requested_namespaces: Vec<NamespaceId>,
    durability_role: ReplicaDurabilityRole,
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

#[cfg(test)]
#[derive(Clone, Debug)]
struct ConnectStartEvent {
    addr: String,
    at: Instant,
}

#[cfg(test)]
struct ConnectStartHookGuard {
    prev: Option<Sender<ConnectStartEvent>>,
}

#[cfg(test)]
static CONNECT_START_HOOK: OnceLock<Mutex<Option<Sender<ConnectStartEvent>>>> = OnceLock::new();

#[cfg(test)]
fn set_connect_start_hook(sender: Sender<ConnectStartEvent>) -> ConnectStartHookGuard {
    let lock = CONNECT_START_HOOK.get_or_init(|| Mutex::new(None));
    let mut guard = lock.lock().expect("connect start hook lock");
    let prev = guard.replace(sender);
    ConnectStartHookGuard { prev }
}

#[cfg(test)]
impl Drop for ConnectStartHookGuard {
    fn drop(&mut self) {
        let lock = CONNECT_START_HOOK.get_or_init(|| Mutex::new(None));
        let mut guard = lock.lock().expect("connect start hook lock");
        *guard = self.prev.take();
    }
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
    let span = tracing::info_span!(
        "repl_peer_loop",
        direction = "outbound",
        store_id = %runtime.local_store.store_id,
        store_epoch = runtime.local_store.store_epoch.get(),
        replica_id = %runtime.local_replica_id,
        peer_replica_id = %plan.replica_id,
        peer_addr = %plan.addr,
        peer_role = ?plan.durability_role
    );
    let _guard = span.enter();

    let mut backoff = Backoff::new(runtime.backoff);

    while !runtime.shutdown.load(Ordering::Relaxed) {
        let connect_start = Instant::now();
        #[cfg(test)]
        let hook = CONNECT_START_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("connect start hook lock")
            .clone();
        #[cfg(test)]
        if let Some(tx) = hook {
            let _ = tx.try_send(ConnectStartEvent {
                addr: plan.addr.clone(),
                at: connect_start,
            });
        }
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
    #[error("event frame error: {0}")]
    EventFrame(#[from] EventFrameError),
    #[error("broadcast error: {0}")]
    Broadcast(#[from] BroadcastError),
}

#[derive(Clone, Debug)]
enum InboundMessage {
    Message(WireReplMessage),
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
    let reader_limit_state = FrameLimitState::unnegotiated(limits.max_frame_bytes);
    let mut reader = FrameReader::new(reader_stream, reader_limit_state.clone());
    let mut writer = FrameWriter::new(writer_stream, limits.max_frame_bytes);

    let (inbound_tx, inbound_rx) = crossbeam::channel::unbounded::<InboundMessage>();
    let reader_shutdown = shutdown.clone();
    let reader_decode_limits = limits.clone();
    let reader_span = tracing::Span::current();
    let reader_handle = thread::spawn(move || {
        reader_span.in_scope(|| {
            run_reader_loop(
                &mut reader,
                inbound_tx,
                reader_shutdown,
                reader_decode_limits,
            );
        });
    });

    let mut event_rx: crossbeam::channel::Receiver<BroadcastEvent> = crossbeam::channel::never();
    let mut event_handle: Option<JoinHandle<()>> = None;

    let mut config = SessionConfig::new(local_store, local_replica_id, &limits);
    config.requested_namespaces = plan.requested_namespaces.clone();
    config.offered_namespaces = plan.offered_namespaces.clone();

    let session = OutboundConnecting::new(config, limits.clone(), admission);
    let mut keepalive = KeepaliveTracker::new(&limits, now_ms());

    let (session, action) = session.begin_handshake(&store, now_ms());
    let mut session = SessionState::Handshaking(session);
    apply_action(&mut writer, &session, action, &mut keepalive)?;
    let mut last_hello_at_ms = Some(now_ms());

    let mut sent_hot_cache = false;
    let mut handshake_at_ms = None;
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
                        let now_ms = now_ms();
                        keepalive.note_recv(now_ms);
                        let (next_session, actions) =
                            handle_outbound_message(session, msg, &mut store, now_ms);
                        session = next_session;
                        for action in actions {
                            if let SessionAction::PeerAck(ack) = &action
                                && let Err(err) =
                                    update_peer_ack(&store, &peer_acks, plan.replica_id, ack)
                            {
                                tracing::warn!("peer ack update failed: {err}");
                            }
                            if let SessionAction::PeerAck(_) = &action
                                && let Some(handshake_ms) = handshake_at_ms
                                && let Err(err) = store.update_replica_liveness(
                                    plan.replica_id,
                                    now_ms,
                                    handshake_ms,
                                    plan.durability_role,
                                )
                            {
                                tracing::warn!("replica liveness update failed: {err}");
                            }

                            if let SessionAction::PeerWant(want) = &action {
                                match &session {
                                    SessionState::StreamingLive(streaming_session) => {
                                        let allowed_set = streaming_session
                                            .peer()
                                            .accepted_namespaces
                                            .iter()
                                            .cloned()
                                            .collect::<BTreeSet<_>>();
                                        let mut ctx = WantContext {
                                            writer: &mut writer,
                                            session: streaming_session,
                                            broadcaster: &broadcaster,
                                            wal_reader: runtime.wal_reader.as_ref(),
                                            limits: &limits,
                                            allowed_set: Some(&allowed_set),
                                            keepalive: &mut keepalive,
                                        };
                                        if let Err(err) = handle_want(want, &mut ctx) {
                                            tracing::warn!("peer want handling failed: {err}");
                                            return Err(err);
                                        }
                                    }
                                    SessionState::StreamingSnapshot(streaming_session) => {
                                        let allowed_set = streaming_session
                                            .peer()
                                            .accepted_namespaces
                                            .iter()
                                            .cloned()
                                            .collect::<BTreeSet<_>>();
                                        let mut ctx = WantContext {
                                            writer: &mut writer,
                                            session: streaming_session,
                                            broadcaster: &broadcaster,
                                            wal_reader: runtime.wal_reader.as_ref(),
                                            limits: &limits,
                                            allowed_set: Some(&allowed_set),
                                            keepalive: &mut keepalive,
                                        };
                                        if let Err(err) = handle_want(want, &mut ctx) {
                                            tracing::warn!("peer want handling failed: {err}");
                                            return Err(err);
                                        }
                                    }
                                    _ => continue,
                                }
                            } else if apply_action(&mut writer, &session, action, &mut keepalive)? {
                                return Ok(());
                            }
                        }
                    }
                    InboundMessage::Terminated { payload } => {
                        if let Some(payload) = payload {
                            send_payload(
                                &mut writer,
                                &session,
                                ReplMessage::Error(payload),
                                &mut keepalive,
                            )?;
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

                let SessionState::StreamingLive(streaming_session) = &session else {
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
                };
                if !streaming_session
                    .peer()
                    .accepted_namespaces
                    .contains(&event.namespace)
                {
                    continue;
                }
                let frame = VerifiedEventFrame::try_from_frame(
                    broadcast_to_frame(event),
                    &limits,
                )?;
                send_events(
                    &mut writer,
                    streaming_session,
                    vec![frame],
                    &limits,
                    &mut keepalive,
                )?;
            }
            recv(tick) -> _ => {}
        }

        if let SessionState::StreamingLive(streaming_session) = &session {
            if handshake_at_ms.is_none() {
                reader_limit_state
                    .apply_negotiated_limit(streaming_session.negotiated_frame_limit());
                let now_ms = now_ms();
                handshake_at_ms = Some(now_ms);
                last_hello_at_ms = None;
                if let Err(err) = store.update_replica_liveness(
                    plan.replica_id,
                    now_ms,
                    now_ms,
                    plan.durability_role,
                ) {
                    tracing::warn!("replica liveness update failed: {err}");
                }
                let peer = streaming_session.peer();
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

            if event_handle.is_none() {
                let subscription = broadcaster.subscribe(subscriber_limits(&limits))?;
                let (event_tx, next_event_rx) = crossbeam::channel::unbounded::<BroadcastEvent>();
                let event_shutdown = shutdown.clone();
                let event_span = tracing::Span::current();
                event_handle = Some(thread::spawn(move || {
                    event_span.in_scope(|| {
                        run_event_forwarder(subscription, event_tx, event_shutdown);
                    });
                }));
                event_rx = next_event_rx;
            }

            if !pending_events.is_empty() {
                let peer = streaming_session.peer();
                let frames = pending_events
                    .drain()
                    .filter(|event| peer.accepted_namespaces.contains(&event.namespace))
                    .map(broadcast_to_frame)
                    .collect::<Vec<_>>();
                let frames = verify_frames(frames, &limits)?;
                send_events(
                    &mut writer,
                    streaming_session,
                    frames,
                    &limits,
                    &mut keepalive,
                )?;
            }
            if !sent_hot_cache {
                let peer = streaming_session.peer();
                send_hot_cache(
                    &mut writer,
                    streaming_session,
                    &broadcaster,
                    &peer.accepted_namespaces,
                    &limits,
                    &mut keepalive,
                )?;
                sent_hot_cache = true;
            }
        } else if let SessionState::StreamingSnapshot(streaming_session) = &session
            && handshake_at_ms.is_none()
        {
            reader_limit_state.apply_negotiated_limit(streaming_session.negotiated_frame_limit());
            let now_ms = now_ms();
            handshake_at_ms = Some(now_ms);
            last_hello_at_ms = None;
            if let Err(err) =
                store.update_replica_liveness(plan.replica_id, now_ms, now_ms, plan.durability_role)
            {
                tracing::warn!("replica liveness update failed: {err}");
            }
            let peer = streaming_session.peer();
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

        if let SessionState::Handshaking(session) = &mut session {
            let now_ms = now_ms();
            let retry_after_ms = limits.keepalive_ms;
            if retry_after_ms > 0 {
                let should_retry = last_hello_at_ms
                    .map(|last| now_ms.saturating_sub(last) >= retry_after_ms)
                    .unwrap_or(true);
                if should_retry {
                    let action = session.resend_handshake(&store, now_ms);
                    apply_action(&mut writer, session, action, &mut keepalive)?;
                    last_hello_at_ms = Some(now_ms);
                }
            }
        }

        if matches!(
            session,
            SessionState::StreamingLive(_) | SessionState::StreamingSnapshot(_)
        ) {
            let now_ms = now_ms();
            if let Some(decision) = keepalive.poll(now_ms) {
                match decision {
                    KeepaliveDecision::SendPing(ping) => {
                        send_payload(
                            &mut writer,
                            &session,
                            ReplMessage::Ping(ping),
                            &mut keepalive,
                        )?;
                    }
                    KeepaliveDecision::Close => {
                        tracing::warn!(
                            target: "repl",
                            direction = "outbound",
                            peer_replica_id = %plan.replica_id,
                            "replication keepalive timeout"
                        );
                        break;
                    }
                }
            }
        }
    }

    let _ = shutdown_stream.shutdown(std::net::Shutdown::Both);
    let _ = reader_handle.join();
    if let Some(handle) = event_handle {
        let _ = handle.join();
    }

    Ok(())
}

fn run_reader_loop(
    reader: &mut FrameReader<TcpStream>,
    inbound_tx: Sender<InboundMessage>,
    shutdown: Arc<AtomicBool>,
    limits: crate::core::Limits,
) {
    let mut ingest_budget = TokenBucket::new(limits.max_repl_ingest_bytes_per_sec as u64);
    loop {
        if shutdown.load(Ordering::Relaxed) {
            let _ = inbound_tx.send(InboundMessage::Terminated { payload: None });
            break;
        }

        match reader.read_next() {
            Ok(Some(bytes)) => match decode_envelope(&bytes, &limits) {
                Ok(envelope) => {
                    if let WireReplMessage::Events(events) = &envelope.message {
                        let total_bytes = events
                            .events
                            .iter()
                            .map(|frame| frame.bytes().len() as u64)
                            .sum();
                        if total_bytes > 0 {
                            let wait = ingest_budget.throttle(total_bytes);
                            if !wait.is_zero() {
                                metrics::repl_ingest_throttle(wait, total_bytes);
                            }
                        }
                    }
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

fn apply_action<S: SessionWire>(
    writer: &mut FrameWriter<TcpStream>,
    session: &S,
    action: SessionAction,
    keepalive: &mut KeepaliveTracker,
) -> Result<bool, PeerError> {
    match action {
        SessionAction::Send(message) => {
            send_payload(writer, session, message, keepalive)?;
            Ok(false)
        }
        SessionAction::Close { error } => {
            if let Some(error) = error {
                send_payload(writer, session, ReplMessage::Error(error), keepalive)?;
            }
            Ok(true)
        }
        SessionAction::PeerAck(_) | SessionAction::PeerWant(_) | SessionAction::PeerError(_) => {
            Ok(false)
        }
    }
}

fn send_payload<S: SessionWire>(
    writer: &mut FrameWriter<TcpStream>,
    session: &S,
    message: ReplMessage,
    keepalive: &mut KeepaliveTracker,
) -> Result<(), PeerError> {
    let version = SessionWire::wire_version(session);
    let envelope = ReplEnvelope { version, message };
    let bytes = encode_envelope(&envelope)?;
    writer.write_frame_with_limit(&bytes, SessionWire::frame_limit(session))?;
    keepalive.note_send(now_ms());
    Ok(())
}

fn send_events(
    writer: &mut FrameWriter<TcpStream>,
    session: &Session<Outbound, StreamingLive>,
    frames: Vec<VerifiedEventFrame>,
    limits: &crate::core::Limits,
    keepalive: &mut KeepaliveTracker,
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
                keepalive,
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
                    keepalive,
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
            keepalive,
        )?;
    }

    Ok(())
}

fn send_want_frames<M>(
    writer: &mut FrameWriter<TcpStream>,
    session: &Session<Outbound, Streaming<M>>,
    frames: Vec<VerifiedEventFrame>,
    limits: &crate::core::Limits,
    keepalive: &mut KeepaliveTracker,
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
                keepalive,
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
                    keepalive,
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
            keepalive,
        )?;
    }

    Ok(())
}

fn events_envelope_len<M>(
    session: &Session<Outbound, Streaming<M>>,
    batch: &[VerifiedEventFrame],
) -> Result<usize, PeerError> {
    let version = session.peer().protocol_version;
    let envelope = ReplEnvelope {
        version,
        message: ReplMessage::Events(Events {
            events: batch.to_vec(),
        }),
    };
    let bytes = encode_envelope(&envelope)?;
    Ok(bytes.len())
}

fn verify_frames(
    frames: Vec<EventFrameV1>,
    limits: &crate::core::Limits,
) -> Result<Vec<VerifiedEventFrame>, EventFrameError> {
    frames
        .into_iter()
        .map(|frame| VerifiedEventFrame::try_from_frame(frame, limits))
        .collect()
}

fn send_hot_cache(
    writer: &mut FrameWriter<TcpStream>,
    session: &Session<Outbound, StreamingLive>,
    broadcaster: &EventBroadcaster,
    allowed_set: &[NamespaceId],
    limits: &crate::core::Limits,
    keepalive: &mut KeepaliveTracker,
) -> Result<(), PeerError> {
    let cache = broadcaster.hot_cache()?;
    let frames = cache
        .into_iter()
        .filter(|event| allowed_set.contains(&event.namespace))
        .map(broadcast_to_frame)
        .collect::<Vec<_>>();
    let frames = verify_frames(frames, limits)?;
    send_events(writer, session, frames, limits, keepalive)
}

fn update_peer_ack(
    store: &impl SessionStore,
    peer_acks: &Arc<Mutex<crate::daemon::repl::PeerAckTable>>,
    peer: ReplicaId,
    ack: &ValidatedAck,
) -> Result<(), Box<crate::daemon::repl::PeerAckError>> {
    let now_ms = now_ms();
    let mut table = peer_acks.lock().expect("peer ack lock poisoned");
    table.update_peer(peer, ack.durable(), ack.applied(), now_ms)?;
    let namespaces: Vec<NamespaceId> = ack.durable().keys().cloned().collect();
    let snapshot = store.watermark_snapshot(&namespaces);
    emit_peer_lag(peer, &snapshot.durable, ack.durable());
    Ok(())
}

fn emit_peer_lag(peer: ReplicaId, local: &WatermarkState<Durable>, ack: &WatermarkState<Durable>) {
    for (namespace, origins) in local {
        let mut max_lag = 0u64;
        let acked = ack.get(namespace);
        for (origin, local_wm) in origins {
            let acked_seq = acked
                .and_then(|map| map.get(origin))
                .map(|wm| wm.seq())
                .unwrap_or(Seq0::ZERO);
            let lag = local_wm.seq().get().saturating_sub(acked_seq.get());
            max_lag = max_lag.max(lag);
        }
        metrics::set_repl_peer_lag(peer, namespace, max_lag);
    }
}

struct WantContext<'a, M> {
    writer: &'a mut FrameWriter<TcpStream>,
    session: &'a Session<Outbound, Streaming<M>>,
    broadcaster: &'a EventBroadcaster,
    wal_reader: Option<&'a WalRangeReader>,
    limits: &'a crate::core::Limits,
    allowed_set: Option<&'a BTreeSet<NamespaceId>>,
    keepalive: &'a mut KeepaliveTracker,
}

fn handle_want<M>(want: &Want, ctx: &mut WantContext<'_, M>) -> Result<(), PeerError> {
    if want.want.is_empty() {
        return Ok(());
    }

    let cache = ctx.broadcaster.hot_cache()?;
    let outcome = match build_want_frames(want, cache, ctx.wal_reader, ctx.limits, ctx.allowed_set)
    {
        Ok(outcome) => outcome,
        Err(err) => {
            let payload = err.as_error_payload();
            send_payload(
                ctx.writer,
                ctx.session,
                ReplMessage::Error(payload),
                ctx.keepalive,
            )?;
            return Ok(());
        }
    };

    match outcome {
        WantFramesOutcome::Frames(frames) => {
            let frames = verify_frames(frames, ctx.limits)?;
            send_want_frames(ctx.writer, ctx.session, frames, ctx.limits, ctx.keepalive)
        }
        WantFramesOutcome::BootstrapRequired { namespaces } => {
            let payload = ErrorPayload::new(
                ProtocolErrorCode::BootstrapRequired.into(),
                "bootstrap required",
                false,
            )
            .with_details(BootstrapRequiredDetails {
                namespaces: namespaces.into_iter().collect(),
                reason: SnapshotRangeReason::RangeMissing,
            });
            send_payload(
                ctx.writer,
                ctx.session,
                ReplMessage::Error(payload),
                ctx.keepalive,
            )?;
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
    use crossbeam::channel::Receiver;
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant};
    use uuid::Uuid;

    use crate::core::{
        ActorId, Applied, BeadId, Durable, EventBody, EventBytes, EventId, EventKindV1, HeadStatus,
        HlcMax, NamespaceId, NamespacePolicy, NoteAppendV1, NoteId, Opaque, ReplicaId, Seq0, Seq1,
        Sha256, StoreEpoch, StoreId, StoreIdentity, TxnDeltaV1, TxnOpV1, TxnV1, VerifiedEventFrame,
        Watermark, WireNoteV1, WireStamp, encode_event_body_canonical, hash_event_body,
    };
    use crate::daemon::repl::keepalive::KeepaliveTracker;
    use crate::daemon::repl::proto::Welcome;
    use crate::daemon::repl::{ContiguousBatch, IngestOutcome, ReplError, WatermarkSnapshot};

    #[derive(Default)]
    struct TestStore;

    impl SessionStore for TestStore {
        fn watermark_snapshot(&self, _namespaces: &[NamespaceId]) -> WatermarkSnapshot {
            WatermarkSnapshot {
                durable: BTreeMap::new(),
                applied: BTreeMap::new(),
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
            batch: &ContiguousBatch,
            _now_ms: u64,
        ) -> Result<IngestOutcome, ReplError> {
            let last = batch.last_event();
            let seq = Seq0::new(last.seq().get());
            let head = HeadStatus::Known(last.sha256.0);
            let watermark = Watermark::<Durable>::new(seq, head).expect("watermark");
            let applied = Watermark::<Applied>::new(seq, head).expect("watermark");
            Ok(IngestOutcome {
                durable: watermark,
                applied,
            })
        }

        fn update_replica_liveness(
            &mut self,
            _replica_id: ReplicaId,
            _last_seen_ms: u64,
            _last_handshake_ms: u64,
            _role: ReplicaDurabilityRole,
        ) -> Result<(), crate::daemon::wal::WalIndexError> {
            Ok(())
        }
    }

    fn test_limits() -> crate::core::Limits {
        crate::core::Limits {
            max_event_batch_events: 4,
            max_event_batch_bytes: 1024,
            ..Default::default()
        }
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
        live_stream_enabled_override: Option<bool>,
    ) -> (std::net::SocketAddr, Receiver<WireReplMessage>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = crossbeam::channel::unbounded();

        thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let reader_stream = stream.try_clone().expect("clone");
                let mut reader =
                    FrameReader::new(reader_stream, FrameLimitState::unnegotiated(1024 * 1024));
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

                    if respond_with_welcome
                        && !welcome_sent
                        && let WireReplMessage::Hello(hello) = envelope.message
                    {
                        let mut config = SessionConfig::new(
                            peer_store,
                            peer_replica,
                            &crate::core::Limits::default(),
                        );
                        config.offered_namespaces = hello.requested_namespaces.clone();
                        config.requested_namespaces = hello.offered_namespaces.clone();
                        let session = crate::daemon::repl::session::InboundConnecting::new(
                            config,
                            crate::core::Limits::default(),
                            AdmissionController::new(&crate::core::Limits::default()),
                        );
                        let mut store = TestStore;
                        let (_session, actions) =
                            crate::daemon::repl::session::handle_inbound_message(
                                SessionState::Connecting(session),
                                WireReplMessage::Hello(hello),
                                &mut store,
                                now_ms(),
                            );
                        for action in actions {
                            if let SessionAction::Send(message) = action {
                                let mut message = message;
                                if let Some(override_namespaces) = accepted_override.clone()
                                    && let ReplMessage::Welcome(ref mut welcome) = message
                                {
                                    welcome.accepted_namespaces = override_namespaces;
                                }
                                if let Some(live_stream_enabled) = live_stream_enabled_override
                                    && let ReplMessage::Welcome(ref mut welcome) = message
                                {
                                    welcome.live_stream_enabled = live_stream_enabled;
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
        let (addr, rx) = spawn_peer_listener(local_store, peer_replica, false, None, None);
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
            peers: vec![test_peer_config(peer_replica, addr.clone())],
            wal_reader: None,
            limits: test_limits(),
            backoff: BackoffPolicy {
                base: Duration::from_millis(5),
                max: Duration::from_millis(10),
            },
        };
        let manager = ReplicationManager::new(SharedSessionStore::new(TestStore), config);

        let handle = manager.start();
        let msg = rx.recv_timeout(Duration::from_secs(1)).expect("hello");
        match msg {
            WireReplMessage::Hello(hello) => {
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
        let (addr, rx) = spawn_peer_listener(local_store, peer_replica, true, None, None);
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
            peers: vec![test_peer_config(peer_replica, addr.clone())],
            wal_reader: None,
            limits: test_limits(),
            backoff: BackoffPolicy {
                base: Duration::from_millis(5),
                max: Duration::from_millis(10),
            },
        };
        let manager = ReplicationManager::new(SharedSessionStore::new(TestStore), config);

        let handle = manager.start();
        rx.recv_timeout(Duration::from_secs(1)).expect("hello");

        let core_event =
            make_broadcast_event(local_store, NamespaceId::core(), local_replica, 1, None, 4);
        let tmp_event = make_broadcast_event(local_store, tmp.clone(), local_replica, 2, None, 4);

        broadcaster.publish(core_event).unwrap();
        broadcaster.publish(tmp_event).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut received_events = None;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let received = rx.recv_timeout(remaining).expect("message");
            if let WireReplMessage::Events(events) = received {
                received_events = Some(events);
                break;
            }
        }
        let events = received_events.expect("events");
        assert_eq!(events.events.len(), 1);
        assert_eq!(events.events[0].eid().namespace, NamespaceId::core());

        handle.shutdown();
    }

    #[test]
    fn manager_skips_live_stream_when_disabled() {
        let local_store = StoreIdentity::new(
            crate::core::StoreId::new(Uuid::from_bytes([30u8; 16])),
            crate::core::StoreEpoch::ZERO,
        );
        let local_replica = ReplicaId::new(Uuid::from_bytes([31u8; 16]));
        let peer_replica = ReplicaId::new(Uuid::from_bytes([32u8; 16]));
        let (addr, rx) = spawn_peer_listener(local_store, peer_replica, true, None, Some(false));
        let addr = addr.to_string();

        let broadcaster = EventBroadcaster::new(crate::daemon::broadcast::BroadcasterLimits {
            max_subscribers: 4,
            hot_cache_max_events: 16,
            hot_cache_max_bytes: 1024,
        });

        let config = ReplicationManagerConfig {
            local_store,
            local_replica_id: local_replica,
            admission: AdmissionController::new(&test_limits()),
            broadcaster: broadcaster.clone(),
            peer_acks: Arc::new(Mutex::new(crate::daemon::repl::PeerAckTable::new())),
            policies: test_policy(),
            roster: None,
            peers: vec![test_peer_config(peer_replica, addr.clone())],
            wal_reader: None,
            limits: test_limits(),
            backoff: BackoffPolicy {
                base: Duration::from_millis(5),
                max: Duration::from_millis(10),
            },
        };
        let manager = ReplicationManager::new(SharedSessionStore::new(TestStore), config);

        let handle = manager.start();
        rx.recv_timeout(Duration::from_secs(1)).expect("hello");

        let core_event =
            make_broadcast_event(local_store, NamespaceId::core(), local_replica, 1, None, 4);
        broadcaster.publish(core_event).unwrap();

        let deadline = Instant::now() + Duration::from_millis(200);
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if let Ok(msg) = rx.recv_timeout(remaining) {
                if matches!(msg, WireReplMessage::Events(_)) {
                    panic!("unexpected live events in snapshot-only session");
                }
            }
        }

        handle.shutdown();
    }

    #[test]
    fn send_events_respects_negotiated_max_frame_bytes() {
        let limits = test_limits();
        let local_store =
            StoreIdentity::new(StoreId::new(Uuid::from_bytes([20u8; 16])), StoreEpoch::ZERO);
        let local_replica = ReplicaId::new(Uuid::from_bytes([21u8; 16]));
        let peer_replica = ReplicaId::new(Uuid::from_bytes([22u8; 16]));

        let mut config = SessionConfig::new(local_store, local_replica, &limits);
        config.requested_namespaces = vec![NamespaceId::core()];
        config.offered_namespaces = vec![NamespaceId::core()];
        let session =
            OutboundConnecting::new(config, limits.clone(), AdmissionController::new(&limits));
        let mut store = TestStore;
        let (session, _action) = session.begin_handshake(&store, now_ms());
        let session = SessionState::Handshaking(session);
        let origin = ReplicaId::new(Uuid::from_bytes([23u8; 16]));
        let namespace = NamespaceId::core();
        let mut payload_len = 10usize;
        let (frame1, frame2, target_max_frame) = loop {
            let f1 = make_frame(local_store, namespace.clone(), origin, 1, None, payload_len);
            let f2 = make_frame(
                local_store,
                namespace.clone(),
                origin,
                2,
                Some(f1.sha256),
                payload_len,
            );
            let single_one = ReplEnvelope {
                version: PROTOCOL_VERSION_V1,
                message: ReplMessage::Events(Events {
                    events: vec![f1.clone()],
                }),
            };
            let single_two = ReplEnvelope {
                version: PROTOCOL_VERSION_V1,
                message: ReplMessage::Events(Events {
                    events: vec![f2.clone()],
                }),
            };
            let double = ReplEnvelope {
                version: PROTOCOL_VERSION_V1,
                message: ReplMessage::Events(Events {
                    events: vec![f1.clone(), f2.clone()],
                }),
            };
            let len_single_one = encode_envelope(&single_one).expect("encode").len();
            let len_single_two = encode_envelope(&single_two).expect("encode").len();
            let len_double = encode_envelope(&double).expect("encode").len();
            let min_split = len_single_one.max(len_single_two);
            if len_double > min_split + 1 {
                break (f1, f2, min_split + 1);
            }
            payload_len = payload_len.saturating_add(10);
            if payload_len > 10_000 {
                panic!("unable to craft frame sizes");
            }
        };

        let welcome = Welcome {
            protocol_version: PROTOCOL_VERSION_V1,
            store_id: local_store.store_id,
            store_epoch: local_store.store_epoch,
            receiver_replica_id: peer_replica,
            welcome_nonce: 1,
            accepted_namespaces: vec![NamespaceId::core()],
            receiver_seen_durable: BTreeMap::new(),
            receiver_seen_applied: None,
            live_stream_enabled: true,
            max_frame_bytes: target_max_frame.try_into().expect("max frame fits u32"),
        };
        let (session, _actions) = handle_outbound_message(
            session,
            WireReplMessage::Welcome(welcome),
            &mut store,
            now_ms(),
        );
        let SessionState::StreamingLive(session) = session else {
            panic!("expected streaming session");
        };

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = crossbeam::channel::bounded(1);

        std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            let mut reader = FrameReader::new(stream, FrameLimitState::unnegotiated(1024 * 1024));
            let mut counts = Vec::new();
            while let Ok(Some(frame)) = reader.read_next() {
                let envelope =
                    decode_envelope(&frame, &crate::core::Limits::default()).expect("decode");
                if let WireReplMessage::Events(events) = envelope.message {
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

        let max_frame = session.negotiated_max_frame_bytes();
        assert_eq!(max_frame, target_max_frame);

        let mut keepalive = KeepaliveTracker::new(&limits, now_ms());
        send_events(
            &mut writer,
            &session,
            vec![frame1, frame2],
            &limits,
            &mut keepalive,
        )
        .expect("send");

        let counts = rx.recv_timeout(Duration::from_secs(1)).expect("counts");
        assert_eq!(counts, vec![1, 1]);
    }

    fn make_event_body(
        store: StoreIdentity,
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: u64,
        payload_len: usize,
    ) -> EventBody {
        let note = WireNoteV1 {
            id: NoteId::new(format!("note-{seq}")).expect("note id"),
            content: "x".repeat(payload_len.max(1)),
            author: ActorId::new("alice".to_string()).expect("actor"),
            at: WireStamp(10, 0),
        };
        let append = NoteAppendV1 {
            bead_id: BeadId::parse("bd-test1").expect("bead id"),
            note,
            lineage: None,
        };
        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::NoteAppend(append))
            .expect("note append");

        EventBody {
            envelope_v: 1,
            store,
            namespace,
            origin_replica_id: origin,
            origin_seq: Seq1::from_u64(seq).expect("seq"),
            event_time_ms: 10,
            txn_id: crate::core::TxnId::new(Uuid::from_bytes([seq as u8; 16])),
            client_request_id: None,
            trace_id: None,
            kind: EventKindV1::TxnV1(TxnV1 {
                delta,
                hlc_max: HlcMax {
                    actor_id: ActorId::new("alice".to_string()).expect("actor"),
                    physical_ms: 10,
                    logical: 0,
                },
            }),
        }
    }

    fn make_broadcast_event(
        store: StoreIdentity,
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: u64,
        prev: Option<Sha256>,
        payload_len: usize,
    ) -> BroadcastEvent {
        let body = make_event_body(store, namespace.clone(), origin, seq, payload_len);
        let bytes = encode_event_body_canonical(&body).expect("encode");
        let sha256 = hash_event_body(&bytes);
        BroadcastEvent::new(
            EventId::new(origin, namespace, body.origin_seq),
            sha256,
            prev,
            EventBytes::<Opaque>::from(bytes),
        )
    }

    fn make_frame(
        store: StoreIdentity,
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: u64,
        prev: Option<Sha256>,
        payload_len: usize,
    ) -> VerifiedEventFrame {
        let body = make_event_body(store, namespace.clone(), origin, seq, payload_len);
        let bytes = encode_event_body_canonical(&body).expect("encode");
        VerifiedEventFrame::new(
            EventId::new(origin, namespace, body.origin_seq),
            prev,
            bytes,
        )
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
        let (addr, rx) = spawn_peer_listener(local_store, peer_replica, true, accepted, None);
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
            peers: vec![test_peer_config(peer_replica, addr.clone())],
            wal_reader: None,
            limits: test_limits(),
            backoff: BackoffPolicy {
                base: Duration::from_millis(5),
                max: Duration::from_millis(10),
            },
        };
        let manager = ReplicationManager::new(SharedSessionStore::new(TestStore), config);

        let handle = manager.start();
        rx.recv_timeout(Duration::from_secs(1)).expect("hello");

        let core_event =
            make_broadcast_event(local_store, NamespaceId::core(), local_replica, 1, None, 4);
        let tmp_event = make_broadcast_event(local_store, tmp.clone(), local_replica, 2, None, 4);

        broadcaster.publish(core_event).unwrap();
        broadcaster.publish(tmp_event).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut received_events = None;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let received = rx.recv_timeout(remaining).expect("message");
            if let WireReplMessage::Events(events) = received {
                received_events = Some(events);
                break;
            }
        }
        let events = received_events.expect("events");
        assert_eq!(events.events.len(), 1);
        assert_eq!(events.events[0].eid().namespace, NamespaceId::core());

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
            peers: vec![test_peer_config(peer_replica, addr.clone())],
            wal_reader: None,
            limits: test_limits(),
            backoff: BackoffPolicy {
                base: Duration::from_millis(20),
                max: Duration::from_millis(40),
            },
        };
        let manager = ReplicationManager::new(SharedSessionStore::new(TestStore), config);

        let (start_tx, start_rx) = crossbeam::channel::bounded(8);
        let _hook = set_connect_start_hook(start_tx);
        let handle = manager.start();
        let (seen_tx, seen_rx) = crossbeam::channel::bounded(2);
        thread::spawn(move || {
            for _ in 0..2 {
                let (stream, _) = listener.accept().expect("accept");
                let reader_stream = stream.try_clone().expect("clone");
                let mut reader =
                    FrameReader::new(reader_stream, FrameLimitState::unnegotiated(1024 * 1024));
                let mut writer = FrameWriter::new(stream, 1024 * 1024);
                let _ = reader.read_next();
                let payload =
                    ErrorPayload::new(ProtocolErrorCode::Overloaded.into(), "overloaded", true);
                let envelope = ReplEnvelope {
                    version: PROTOCOL_VERSION_V1,
                    message: ReplMessage::Error(payload),
                };
                let bytes = encode_envelope(&envelope).expect("encode");
                writer.write_frame(&bytes).expect("write");
                let _ = seen_tx.send(Instant::now());
            }
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut starts = Vec::new();
        while starts.len() < 2 && Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let event = start_rx.recv_timeout(remaining).expect("connect start");
            if event.addr == addr {
                starts.push(event.at);
            }
        }
        assert_eq!(starts.len(), 2, "expected two connect attempts");
        let first_start = starts[0];
        let _first_at = seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first connection");
        let second_start = starts[1];
        let _second_at = seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second connection");
        assert!(
            second_start.duration_since(first_start) >= Duration::from_millis(20),
            "backoff should delay connect attempts by at least base duration"
        );
        handle.shutdown();
    }
}
