//! Replication server accept loop and inbound sessions.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam::channel::Sender;
use thiserror::Error;

use crate::core::error::details::{
    BootstrapRequiredDetails, SnapshotRangeReason, UnknownReplicaDetails,
};
use crate::core::{
    ErrorCode, ErrorPayload, EventBytes, EventFrameV1, Limits, NamespaceId, NamespacePolicy,
    Opaque, ReplicaId, ReplicaRole, ReplicaRoster, ReplicateMode, StoreIdentity,
};
use crate::daemon::admission::AdmissionController;
use crate::daemon::broadcast::{
    BroadcastError, BroadcastEvent, EventBroadcaster, EventSubscription, SubscriberLimits,
};
use crate::daemon::repl::proto::{Ack, Events, PROTOCOL_VERSION_V1, Want};
use crate::daemon::repl::{
    FrameError, FrameReader, FrameWriter, PeerAckTable, ReplEnvelope, ReplMessage, Session,
    SessionAction, SessionConfig, SessionPhase, SessionRole, SessionStore, SharedSessionStore,
    decode_envelope, encode_envelope,
};

#[derive(Clone, Debug)]
pub struct ReplicationServerConfig {
    pub listen_addr: String,
    pub local_store: StoreIdentity,
    pub local_replica_id: ReplicaId,
    pub admission: AdmissionController,
    pub broadcaster: EventBroadcaster,
    pub peer_acks: Arc<Mutex<PeerAckTable>>,
    pub policies: BTreeMap<NamespaceId, NamespacePolicy>,
    pub roster: Option<ReplicaRoster>,
    pub limits: Limits,
    pub max_connections: Option<NonZeroUsize>,
}

#[derive(Debug, Error)]
pub enum ReplicationServerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("max_connections must be configured when roster is absent")]
    MissingConnectionLimit,
}

pub struct ReplicationServer<S> {
    store: SharedSessionStore<S>,
    config: ReplicationServerConfig,
}

pub struct ReplicationServerHandle {
    shutdown: Arc<AtomicBool>,
    join: JoinHandle<()>,
    local_addr: SocketAddr,
}

impl ReplicationServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = self.join.join();
    }
}

impl<S> ReplicationServer<S>
where
    S: SessionStore + Send + 'static,
{
    pub fn new(store: SharedSessionStore<S>, config: ReplicationServerConfig) -> Self {
        Self { store, config }
    }

    pub fn start(self) -> Result<ReplicationServerHandle, ReplicationServerError> {
        let max_connections = resolve_max_connections(&self.config)?;
        let listener = TcpListener::bind(&self.config.listen_addr)?;
        let local_addr = listener.local_addr()?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let active_connections = Arc::new(AtomicUsize::new(0));

        let runtime = ServerRuntime {
            local_store: self.config.local_store,
            local_replica_id: self.config.local_replica_id,
            store: self.store,
            admission: self.config.admission,
            broadcaster: self.config.broadcaster,
            peer_acks: self.config.peer_acks,
            policies: self.config.policies,
            roster: self.config.roster,
            limits: self.config.limits,
            max_connections,
            shutdown: Arc::clone(&shutdown),
            active_connections,
        };

        let join = thread::spawn(move || run_accept_loop(listener, runtime));

        Ok(ReplicationServerHandle {
            shutdown,
            join,
            local_addr,
        })
    }
}

#[derive(Clone)]
struct ServerRuntime<S> {
    local_store: StoreIdentity,
    local_replica_id: ReplicaId,
    store: SharedSessionStore<S>,
    admission: AdmissionController,
    broadcaster: EventBroadcaster,
    peer_acks: Arc<Mutex<PeerAckTable>>,
    policies: BTreeMap<NamespaceId, NamespacePolicy>,
    roster: Option<ReplicaRoster>,
    limits: Limits,
    max_connections: NonZeroUsize,
    shutdown: Arc<AtomicBool>,
    active_connections: Arc<AtomicUsize>,
}

#[derive(Debug, Error)]
enum ConnectionError {
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

struct ConnectionGuard {
    active: Arc<AtomicUsize>,
}

impl ConnectionGuard {
    fn try_acquire(active: &Arc<AtomicUsize>, max: NonZeroUsize) -> Option<Self> {
        let mut current = active.load(Ordering::Acquire);
        loop {
            if current >= max.get() {
                return None;
            }
            match active.compare_exchange(
                current,
                current.saturating_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(Self {
                        active: Arc::clone(active),
                    });
                }
                Err(next) => current = next,
            }
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let prev = self.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "active connection counter underflow");
    }
}

fn resolve_max_connections(
    config: &ReplicationServerConfig,
) -> Result<NonZeroUsize, ReplicationServerError> {
    if let Some(limit) = config.max_connections {
        return Ok(limit);
    }
    if let Some(roster) = &config.roster {
        if let Some(limit) = NonZeroUsize::new(roster.replicas.len()) {
            return Ok(limit);
        }
    }
    Err(ReplicationServerError::MissingConnectionLimit)
}

fn run_accept_loop<S>(listener: TcpListener, runtime: ServerRuntime<S>)
where
    S: SessionStore + Send + 'static,
{
    if let Err(err) = listener.set_nonblocking(true) {
        tracing::error!("replication server failed to set nonblocking: {err}");
        return;
    }

    loop {
        if runtime.shutdown.load(Ordering::Relaxed) {
            break;
        }

        match listener.accept() {
            Ok((stream, _)) => {
                if let Some(guard) = ConnectionGuard::try_acquire(
                    &runtime.active_connections,
                    runtime.max_connections,
                ) {
                    let runtime = runtime.clone();
                    thread::spawn(move || {
                        if let Err(err) = run_inbound_session(stream, runtime, guard) {
                            tracing::warn!("replication inbound session error: {err}");
                        }
                    });
                } else {
                    send_overloaded(stream, &runtime.limits);
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(err) => {
                tracing::warn!("replication accept error: {err}");
                thread::sleep(Duration::from_millis(25));
            }
        }
    }
}

fn send_overloaded(mut stream: TcpStream, limits: &Limits) {
    let _ = stream.set_nodelay(true);
    let payload = ErrorPayload::new(
        ErrorCode::Overloaded,
        "replication connection limit reached",
        true,
    );
    let mut writer = FrameWriter::new(stream, limits.max_frame_bytes);
    let _ = send_pre_handshake_error(&mut writer, payload);
}

fn run_inbound_session<S>(
    stream: TcpStream,
    runtime: ServerRuntime<S>,
    _guard: ConnectionGuard,
) -> Result<(), ConnectionError>
where
    S: SessionStore + Send + 'static,
{
    let mut store = runtime.store.clone();
    let admission = runtime.admission.clone();
    let broadcaster = runtime.broadcaster.clone();
    let peer_acks = Arc::clone(&runtime.peer_acks);
    let limits = runtime.limits.clone();
    let shutdown = Arc::clone(&runtime.shutdown);

    stream.set_nodelay(true)?;

    let reader_stream = stream.try_clone()?;
    let writer_stream = stream.try_clone()?;
    let mut reader = FrameReader::new(reader_stream, limits.max_frame_bytes);
    let mut writer = FrameWriter::new(writer_stream, limits.max_frame_bytes);

    let Some(bytes) = reader.read_next()? else {
        return Ok(());
    };
    let envelope = match decode_envelope(&bytes, &limits) {
        Ok(envelope) => envelope,
        Err(err) => {
            if let Some(payload) = err.as_error_payload() {
                let _ = send_pre_handshake_error(&mut writer, payload);
            }
            return Ok(());
        }
    };

    let hello = match envelope.message {
        ReplMessage::Hello(hello) => hello,
        _ => {
            let payload = ErrorPayload::new(ErrorCode::InvalidRequest, "expected HELLO", false);
            let _ = send_pre_handshake_error(&mut writer, payload);
            return Ok(());
        }
    };

    let roster_entry = runtime
        .roster
        .as_ref()
        .and_then(|roster| roster.replica(&hello.sender_replica_id));
    if runtime.roster.is_some() && roster_entry.is_none() {
        let payload = ErrorPayload::new(ErrorCode::UnknownReplica, "unknown replica", false)
            .with_details(UnknownReplicaDetails {
                replica_id: hello.sender_replica_id,
                roster_hash: None,
            });
        let _ = send_pre_handshake_error(&mut writer, payload);
        return Ok(());
    }

    let role = roster_entry
        .map(|entry| entry.role)
        .unwrap_or(ReplicaRole::Peer);
    let allowed_namespaces = roster_entry.and_then(|entry| entry.allowed_namespaces.clone());
    let eligible = eligible_namespaces(&runtime.policies, role, allowed_namespaces.as_ref());

    let mut config = SessionConfig::new(runtime.local_store, runtime.local_replica_id, &limits);
    config.requested_namespaces = eligible.clone();
    config.offered_namespaces = eligible;

    let peer_replica_id = hello.sender_replica_id;
    let mut session = Session::new(SessionRole::Inbound, config, limits.clone(), admission);
    let actions = session.handle_message(ReplMessage::Hello(hello), &mut store, now_ms());

    for action in actions {
        if let SessionAction::PeerAck(ack) = &action
            && let Err(err) = update_peer_ack(&peer_acks, peer_replica_id, ack)
        {
            tracing::warn!("peer ack update failed: {err}");
        }

        if let SessionAction::PeerWant(want) = &action {
            if let Err(err) = handle_want(&mut writer, &session, want, &broadcaster, &limits, None)
            {
                tracing::warn!("peer want handling failed: {err}");
                return Err(err);
            }
        } else if apply_action(&mut writer, &session, action)? {
            return Ok(());
        }
    }

    let (inbound_tx, inbound_rx) = crossbeam::channel::unbounded::<InboundMessage>();
    let reader_shutdown = shutdown.clone();
    let reader_limits = limits.clone();
    let reader_handle = thread::spawn(move || {
        run_reader_loop(&mut reader, inbound_tx, reader_shutdown, reader_limits);
    });

    let mut accepted_set = BTreeSet::new();
    let mut live_stream_enabled = false;
    if session.phase() == SessionPhase::Streaming {
        if let Some(peer) = session.peer() {
            accepted_set = peer.accepted_namespaces.iter().cloned().collect();
            live_stream_enabled = peer.live_stream_enabled;
        }
    }

    let (event_rx, event_handle) = if live_stream_enabled {
        let subscription = broadcaster.subscribe(subscriber_limits(&limits))?;
        let (event_tx, event_rx) = crossbeam::channel::unbounded::<BroadcastEvent>();
        let event_shutdown = shutdown.clone();
        let event_handle = thread::spawn(move || {
            run_event_forwarder(subscription, event_tx, event_shutdown);
        });
        (event_rx, Some(event_handle))
    } else {
        (crossbeam::channel::never(), None)
    };

    let mut streaming = session.phase() == SessionPhase::Streaming;
    let mut sent_hot_cache = false;
    let mut pending_events: Vec<BroadcastEvent> = Vec::new();

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
                                && let Err(err) = update_peer_ack(&peer_acks, peer_replica_id, ack)
                            {
                                tracing::warn!("peer ack update failed: {err}");
                            }

                            if let SessionAction::PeerWant(want) = &action {
                                if let Err(err) = handle_want(
                                    &mut writer,
                                    &session,
                                    want,
                                    &broadcaster,
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
                    pending_events.push(event);
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
        }
        if streaming && !pending_events.is_empty() {
            let frames = pending_events
                .drain(..)
                .filter(|event| accepted_set.contains(&event.namespace))
                .map(broadcast_to_frame)
                .collect::<Vec<_>>();
            send_events(&mut writer, &session, frames, &limits)?;
        }
        if streaming && live_stream_enabled && !sent_hot_cache {
            send_hot_cache(&mut writer, &session, &broadcaster, &accepted_set, &limits)?;
            sent_hot_cache = true;
        }
    }

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
    limits: Limits,
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
) -> Result<bool, ConnectionError> {
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
) -> Result<(), ConnectionError> {
    let version = session
        .peer()
        .map(|peer| peer.protocol_version)
        .unwrap_or(PROTOCOL_VERSION_V1);
    let envelope = ReplEnvelope { version, message };
    let bytes = encode_envelope(&envelope)?;
    writer.write_frame(&bytes)?;
    Ok(())
}

fn send_pre_handshake_error(
    writer: &mut FrameWriter<TcpStream>,
    payload: ErrorPayload,
) -> Result<(), ConnectionError> {
    let envelope = ReplEnvelope {
        version: PROTOCOL_VERSION_V1,
        message: ReplMessage::Error(payload),
    };
    let bytes = encode_envelope(&envelope)?;
    writer.write_frame(&bytes)?;
    Ok(())
}

fn send_events(
    writer: &mut FrameWriter<TcpStream>,
    session: &Session,
    frames: Vec<EventFrameV1>,
    limits: &Limits,
) -> Result<(), ConnectionError> {
    if frames.is_empty() {
        return Ok(());
    }

    let mut batch = Vec::new();
    let mut batch_bytes = 0usize;

    for frame in frames {
        let frame_bytes = frame.bytes.len();
        if !batch.is_empty()
            && (batch.len() >= limits.max_event_batch_events
                || batch_bytes.saturating_add(frame_bytes) > limits.max_event_batch_bytes)
        {
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
    }

    if !batch.is_empty() {
        send_payload(
            writer,
            session,
            ReplMessage::Events(Events { events: batch }),
        )?;
    }

    Ok(())
}

fn send_hot_cache(
    writer: &mut FrameWriter<TcpStream>,
    session: &Session,
    broadcaster: &EventBroadcaster,
    allowed_set: &BTreeSet<NamespaceId>,
    limits: &Limits,
) -> Result<(), ConnectionError> {
    let cache = broadcaster.hot_cache()?;
    let frames = cache
        .into_iter()
        .filter(|event| allowed_set.contains(&event.namespace))
        .map(broadcast_to_frame)
        .collect::<Vec<_>>();
    send_events(writer, session, frames, limits)
}

fn broadcast_to_frame(event: BroadcastEvent) -> EventFrameV1 {
    EventFrameV1 {
        eid: event.event_id,
        sha256: event.sha256,
        prev_sha256: event.prev_sha256,
        bytes: EventBytes::<Opaque>::from(event.bytes),
    }
}

fn update_peer_ack(
    peer_acks: &Arc<Mutex<PeerAckTable>>,
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
    Ok(())
}

fn handle_want(
    writer: &mut FrameWriter<TcpStream>,
    session: &Session,
    want: &Want,
    broadcaster: &EventBroadcaster,
    limits: &Limits,
    allowed_set: Option<&BTreeSet<NamespaceId>>,
) -> Result<(), ConnectionError> {
    if want.want.is_empty() {
        return Ok(());
    }

    let cache = broadcaster.hot_cache()?;
    let mut needed: BTreeMap<(NamespaceId, ReplicaId), u64> = BTreeMap::new();
    for (namespace, origins) in &want.want {
        if let Some(allowed) = allowed_set
            && !allowed.contains(namespace)
        {
            continue;
        }
        for (origin, seq) in origins {
            needed.insert((namespace.clone(), *origin), *seq);
        }
    }

    if needed.is_empty() {
        return Ok(());
    }

    let mut started = BTreeSet::new();
    let mut frames = Vec::new();

    for event in cache {
        if let Some(allowed) = allowed_set
            && !allowed.contains(&event.namespace)
        {
            continue;
        }
        let key = (event.namespace.clone(), event.event_id.origin_replica_id);
        let Some(want_seq) = needed.get(&key) else {
            continue;
        };
        if event.event_id.origin_seq.get() <= *want_seq {
            continue;
        }
        if event.event_id.origin_seq.get() == want_seq.saturating_add(1) {
            started.insert(key.clone());
        }
        frames.push(broadcast_to_frame(event));
    }

    if started.len() != needed.len() {
        let namespaces: BTreeSet<NamespaceId> = needed.keys().map(|(ns, _)| ns.clone()).collect();
        let payload = ErrorPayload::new(ErrorCode::BootstrapRequired, "bootstrap required", false)
            .with_details(BootstrapRequiredDetails {
                namespaces: namespaces.into_iter().collect(),
                reason: SnapshotRangeReason::RangeMissing,
            });
        send_payload(writer, session, ReplMessage::Error(payload))?;
        return Ok(());
    }

    send_events(writer, session, frames, limits)
}

fn subscriber_limits(limits: &Limits) -> SubscriberLimits {
    let max_events = limits.max_event_batch_events.max(1);
    let max_bytes = limits.max_event_batch_bytes.max(1);
    SubscriberLimits::new(max_events, max_bytes).expect("subscriber limits")
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

fn now_ms() -> u64 {
    crate::core::WallClock::now().0
}
