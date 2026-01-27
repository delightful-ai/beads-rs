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
use tracing::field;

use crate::core::error::details::{
    BootstrapRequiredDetails, SnapshotRangeReason, UnknownReplicaDetails,
};
use crate::core::{
    Durable, ErrorPayload, EventFrameV1, Limits, NamespaceId, NamespacePolicy, ProtocolErrorCode,
    ReplicaId, ReplicaRole, ReplicaRoster, ReplicateMode, Seq0, StoreIdentity,
};
use crate::daemon::admission::AdmissionController;
use crate::daemon::broadcast::{
    BroadcastError, BroadcastEvent, EventBroadcaster, EventSubscription, SubscriberLimits,
};
use crate::daemon::io_budget::TokenBucket;
use crate::daemon::metrics;
use crate::daemon::repl::keepalive::{KeepaliveDecision, KeepaliveTracker};
use crate::daemon::repl::pending::PendingEvents;
use crate::daemon::repl::proto::{Events, PROTOCOL_VERSION_V1, Want, WatermarkState};
use crate::daemon::repl::session::{
    Inbound, InboundConnecting, Session, SessionState, SessionWire, Streaming, StreamingLive,
    handle_inbound_message,
};
use crate::daemon::repl::want::{WantFramesOutcome, broadcast_to_frame, build_want_frames};
use crate::daemon::repl::{
    FrameError, FrameReader, FrameWriter, PeerAckTable, ReplEnvelope, ReplMessage, SessionAction,
    SessionConfig, SessionStore, SharedSessionStore, ValidatedAck, WalRangeReader, decode_envelope,
    encode_envelope,
};
use crate::daemon::wal::ReplicaDurabilityRole;

#[derive(Clone)]
pub struct ReplicationServerConfig {
    pub listen_addr: String,
    pub local_store: StoreIdentity,
    pub local_replica_id: ReplicaId,
    pub admission: AdmissionController,
    pub broadcaster: EventBroadcaster,
    pub peer_acks: Arc<Mutex<PeerAckTable>>,
    pub policies: BTreeMap<NamespaceId, NamespacePolicy>,
    pub roster: Option<ReplicaRoster>,
    pub wal_reader: Option<WalRangeReader>,
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
            wal_reader: self.config.wal_reader,
            limits: self.config.limits,
            max_connections,
            shutdown: Arc::clone(&shutdown),
            active_connections,
        };

        let accept_span = tracing::Span::current();
        let join = thread::spawn(move || {
            accept_span.in_scope(|| run_accept_loop(listener, runtime));
        });

        Ok(ReplicationServerHandle {
            shutdown,
            join,
            local_addr,
        })
    }
}

struct ServerRuntime<S> {
    local_store: StoreIdentity,
    local_replica_id: ReplicaId,
    store: SharedSessionStore<S>,
    admission: AdmissionController,
    broadcaster: EventBroadcaster,
    peer_acks: Arc<Mutex<PeerAckTable>>,
    policies: BTreeMap<NamespaceId, NamespacePolicy>,
    roster: Option<ReplicaRoster>,
    wal_reader: Option<WalRangeReader>,
    limits: Limits,
    max_connections: NonZeroUsize,
    shutdown: Arc<AtomicBool>,
    active_connections: Arc<AtomicUsize>,
}

impl<S> Clone for ServerRuntime<S> {
    fn clone(&self) -> Self {
        Self {
            local_store: self.local_store,
            local_replica_id: self.local_replica_id,
            store: self.store.clone(),
            admission: self.admission.clone(),
            broadcaster: self.broadcaster.clone(),
            peer_acks: Arc::clone(&self.peer_acks),
            policies: self.policies.clone(),
            roster: self.roster.clone(),
            wal_reader: self.wal_reader.clone(),
            limits: self.limits.clone(),
            max_connections: self.max_connections,
            shutdown: Arc::clone(&self.shutdown),
            active_connections: Arc::clone(&self.active_connections),
        }
    }
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
    if let Some(roster) = &config.roster
        && let Some(limit) = NonZeroUsize::new(roster.replicas.len())
    {
        return Ok(limit);
    }
    Err(ReplicationServerError::MissingConnectionLimit)
}

fn run_accept_loop<S>(listener: TcpListener, runtime: ServerRuntime<S>)
where
    S: SessionStore + Send + 'static,
{
    let local_addr = listener.local_addr().ok();
    let span = tracing::info_span!(
        "repl_accept_loop",
        direction = "inbound",
        store_id = %runtime.local_store.store_id,
        store_epoch = runtime.local_store.store_epoch.get(),
        replica_id = %runtime.local_replica_id,
        listen_addr = ?local_addr
    );
    let _guard = span.enter();

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
                if let Err(err) = stream.set_nonblocking(false) {
                    tracing::warn!("replication inbound stream failed to set blocking: {err}");
                }
                if let Some(guard) = ConnectionGuard::try_acquire(
                    &runtime.active_connections,
                    runtime.max_connections,
                ) {
                    let runtime = runtime.clone();
                    let session_span = tracing::Span::current();
                    thread::spawn(move || {
                        session_span.in_scope(|| {
                            if let Err(err) = run_inbound_session(stream, runtime, guard) {
                                tracing::warn!("replication inbound session error: {err}");
                            }
                        });
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

fn send_overloaded(stream: TcpStream, limits: &Limits) {
    let _ = stream.set_nodelay(true);
    let payload = ErrorPayload::new(
        ProtocolErrorCode::Overloaded.into(),
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
    let peer_addr = stream.peer_addr().ok();
    let span = tracing::info_span!(
        "repl_session",
        direction = "inbound",
        store_id = %runtime.local_store.store_id,
        store_epoch = runtime.local_store.store_epoch.get(),
        replica_id = %runtime.local_replica_id,
        peer_replica_id = field::Empty,
        peer_addr = ?peer_addr
    );
    let _span_guard = span.enter();

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
            let payload = ErrorPayload::new(
                ProtocolErrorCode::InvalidRequest.into(),
                "expected HELLO",
                false,
            );
            let _ = send_pre_handshake_error(&mut writer, payload);
            return Ok(());
        }
    };

    let roster_entry = runtime
        .roster
        .as_ref()
        .and_then(|roster| roster.replica(&hello.sender_replica_id));
    if runtime.roster.is_some() && roster_entry.is_none() {
        let payload = ErrorPayload::new(
            ProtocolErrorCode::UnknownReplica.into(),
            "unknown replica",
            false,
        )
        .with_details(UnknownReplicaDetails {
            replica_id: hello.sender_replica_id,
            roster_hash: None,
        });
        let _ = send_pre_handshake_error(&mut writer, payload);
        return Ok(());
    }

    let role = roster_entry
        .map(|entry| entry.role())
        .unwrap_or(ReplicaRole::Peer);
    let durability_eligible = roster_entry
        .map(|entry| entry.durability_eligible())
        .unwrap_or(role != ReplicaRole::Observer);
    let durability_role = match ReplicaDurabilityRole::try_from((role, durability_eligible)) {
        Ok(role) => role,
        Err(err) => {
            tracing::warn!(
                peer_replica_id = %hello.sender_replica_id,
                role = ?role,
                durability_eligible,
                "invalid replica durability role: {err}"
            );
            let payload = ErrorPayload::new(
                ProtocolErrorCode::InvalidRequest.into(),
                "invalid durability eligibility for replica role",
                false,
            );
            let _ = send_pre_handshake_error(&mut writer, payload);
            return Ok(());
        }
    };
    let allowed_namespaces = roster_entry.and_then(|entry| entry.allowed_namespaces.clone());
    let eligible = eligible_namespaces(&runtime.policies, role, allowed_namespaces.as_ref());

    let mut config = SessionConfig::new(runtime.local_store, runtime.local_replica_id, &limits);
    config.requested_namespaces = eligible.clone();
    config.offered_namespaces = eligible;

    let peer_replica_id = hello.sender_replica_id;
    tracing::Span::current().record("peer_replica_id", tracing::field::display(peer_replica_id));
    let requested_namespaces = hello.requested_namespaces.clone();
    let offered_namespaces = hello.offered_namespaces.clone();
    let session = InboundConnecting::new(config, limits.clone(), admission);
    let mut keepalive = KeepaliveTracker::new(&limits, now_ms());
    let mut handshake_at_ms = None;
    let now_ms_val = now_ms();
    keepalive.note_recv(now_ms_val);
    let (mut session, actions) = handle_inbound_message(
        SessionState::Connecting(session),
        ReplMessage::Hello(hello),
        &mut store,
        now_ms_val,
    );

    for action in actions {
        if let SessionAction::PeerAck(ack) = &action
            && let Err(err) = update_peer_ack(&store, &peer_acks, peer_replica_id, ack)
        {
            tracing::warn!("peer ack update failed: {err}");
        }

        if let SessionAction::PeerWant(want) = &action {
            match &session {
                SessionState::StreamingLive(streaming_session) => {
                    let mut ctx = WantContext {
                        writer: &mut writer,
                        session: streaming_session,
                        broadcaster: &broadcaster,
                        wal_reader: runtime.wal_reader.as_ref(),
                        limits: &limits,
                        allowed_set: None,
                        keepalive: &mut keepalive,
                    };
                    if let Err(err) = handle_want(want, &mut ctx) {
                        tracing::warn!("peer want handling failed: {err}");
                        return Err(err);
                    }
                }
                SessionState::StreamingSnapshot(streaming_session) => {
                    let mut ctx = WantContext {
                        writer: &mut writer,
                        session: streaming_session,
                        broadcaster: &broadcaster,
                        wal_reader: runtime.wal_reader.as_ref(),
                        limits: &limits,
                        allowed_set: None,
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

    let (inbound_tx, inbound_rx) = crossbeam::channel::unbounded::<InboundMessage>();
    let reader_shutdown = shutdown.clone();
    let reader_limits = limits.clone();
    let reader_span = tracing::Span::current();
    let reader_handle = thread::spawn(move || {
        reader_span.in_scope(|| {
            run_reader_loop(&mut reader, inbound_tx, reader_shutdown, reader_limits);
        });
    });

    let mut live_stream_enabled = false;
    match &session {
        SessionState::StreamingLive(streaming_session) => {
            let peer = streaming_session.peer();
            live_stream_enabled = true;
            handshake_at_ms = Some(now_ms_val);
            if let Err(err) = store.update_replica_liveness(
                peer_replica_id,
                now_ms_val,
                now_ms_val,
                durability_role,
            ) {
                tracing::warn!("replica liveness update failed: {err}");
            }
            tracing::info!(
                target: "repl",
                direction = "inbound",
                peer_replica_id = %peer.replica_id,
                auth_identity = "none",
                role = ?durability_role,
                requested_namespaces = ?requested_namespaces,
                offered_namespaces = ?offered_namespaces,
                accepted_namespaces = ?peer.accepted_namespaces,
                incoming_namespaces = ?peer.incoming_namespaces,
                live_stream = peer.live_stream_enabled,
                "replication handshake accepted"
            );
        }
        SessionState::StreamingSnapshot(streaming_session) => {
            let peer = streaming_session.peer();
            handshake_at_ms = Some(now_ms_val);
            if let Err(err) = store.update_replica_liveness(
                peer_replica_id,
                now_ms_val,
                now_ms_val,
                durability_role,
            ) {
                tracing::warn!("replica liveness update failed: {err}");
            }
            tracing::info!(
                target: "repl",
                direction = "inbound",
                peer_replica_id = %peer.replica_id,
                auth_identity = "none",
                role = ?durability_role,
                requested_namespaces = ?requested_namespaces,
                offered_namespaces = ?offered_namespaces,
                accepted_namespaces = ?peer.accepted_namespaces,
                incoming_namespaces = ?peer.incoming_namespaces,
                live_stream = peer.live_stream_enabled,
                "replication handshake accepted"
            );
        }
        _ => {}
    }

    let (event_rx, event_handle) = if live_stream_enabled {
        let subscription = broadcaster.subscribe(subscriber_limits(&limits))?;
        let (event_tx, event_rx) = crossbeam::channel::unbounded::<BroadcastEvent>();
        let event_shutdown = shutdown.clone();
        let event_span = tracing::Span::current();
        let event_handle = thread::spawn(move || {
            event_span.in_scope(|| {
                run_event_forwarder(subscription, event_tx, event_shutdown);
            });
        });
        (event_rx, Some(event_handle))
    } else {
        (crossbeam::channel::never(), None)
    };

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
                        let now_ms = now_ms();
                        keepalive.note_recv(now_ms);
                        let (next_session, actions) =
                            handle_inbound_message(session, msg, &mut store, now_ms);
                        session = next_session;
                        for action in actions {
                            if let SessionAction::PeerAck(ack) = &action
                                && let Err(err) =
                                    update_peer_ack(&store, &peer_acks, peer_replica_id, ack)
                            {
                                tracing::warn!("peer ack update failed: {err}");
                            }
                            if let SessionAction::PeerAck(_) = &action
                                && let Some(handshake_ms) = handshake_at_ms
                                && let Err(err) = store.update_replica_liveness(
                                    peer_replica_id,
                                    now_ms,
                                    handshake_ms,
                                    durability_role,
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
                        metrics::repl_pending_dropped_events("inbound", dropped_events);
                        metrics::repl_pending_dropped_bytes("inbound", dropped_bytes);
                        tracing::warn!(
                            target: "repl",
                            direction = "inbound",
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
                let frame = broadcast_to_frame(event);
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
                let now_ms = now_ms();
                handshake_at_ms = Some(now_ms);
                if let Err(err) =
                    store.update_replica_liveness(peer_replica_id, now_ms, now_ms, durability_role)
                {
                    tracing::warn!("replica liveness update failed: {err}");
                }
            }
            if !pending_events.is_empty() {
                let peer = streaming_session.peer();
                let frames = pending_events
                    .drain()
                    .filter(|event| peer.accepted_namespaces.contains(&event.namespace))
                    .map(broadcast_to_frame)
                    .collect::<Vec<_>>();
                send_events(
                    &mut writer,
                    streaming_session,
                    frames,
                    &limits,
                    &mut keepalive,
                )?;
            }
            if live_stream_enabled && !sent_hot_cache {
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
        } else if let SessionState::StreamingSnapshot(_streaming_session) = &session
            && handshake_at_ms.is_none()
        {
            let now_ms = now_ms();
            handshake_at_ms = Some(now_ms);
            if let Err(err) =
                store.update_replica_liveness(peer_replica_id, now_ms, now_ms, durability_role)
            {
                tracing::warn!("replica liveness update failed: {err}");
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
                            direction = "inbound",
                            peer_replica_id = %peer_replica_id,
                            "replication keepalive timeout"
                        );
                        break;
                    }
                }
            }
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
    let mut ingest_budget = TokenBucket::new(limits.max_repl_ingest_bytes_per_sec as u64);
    loop {
        if shutdown.load(Ordering::Relaxed) {
            let _ = inbound_tx.send(InboundMessage::Terminated { payload: None });
            break;
        }

        match reader.read_next() {
            Ok(Some(bytes)) => match decode_envelope(&bytes, &limits) {
                Ok(envelope) => {
                    if let ReplMessage::Events(events) = &envelope.message {
                        let total_bytes = events
                            .events
                            .iter()
                            .map(|frame| frame.bytes.len() as u64)
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
) -> Result<bool, ConnectionError> {
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
) -> Result<(), ConnectionError> {
    let version = SessionWire::wire_version(session);
    let envelope = ReplEnvelope { version, message };
    let bytes = encode_envelope(&envelope)?;
    writer.write_frame_with_limit(&bytes, SessionWire::frame_limit(session))?;
    keepalive.note_send(now_ms());
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
    session: &Session<Inbound, StreamingLive>,
    frames: Vec<EventFrameV1>,
    limits: &Limits,
    keepalive: &mut KeepaliveTracker,
) -> Result<(), ConnectionError> {
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
                return Err(ConnectionError::Frame(FrameError::FrameTooLarge {
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
    session: &Session<Inbound, Streaming<M>>,
    frames: Vec<EventFrameV1>,
    limits: &Limits,
    keepalive: &mut KeepaliveTracker,
) -> Result<(), ConnectionError> {
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
                return Err(ConnectionError::Frame(FrameError::FrameTooLarge {
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
    session: &Session<Inbound, Streaming<M>>,
    batch: &[EventFrameV1],
) -> Result<usize, ConnectionError> {
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

fn send_hot_cache(
    writer: &mut FrameWriter<TcpStream>,
    session: &Session<Inbound, StreamingLive>,
    broadcaster: &EventBroadcaster,
    allowed_set: &[NamespaceId],
    limits: &Limits,
    keepalive: &mut KeepaliveTracker,
) -> Result<(), ConnectionError> {
    let cache = broadcaster.hot_cache()?;
    let frames = cache
        .into_iter()
        .filter(|event| allowed_set.contains(&event.namespace))
        .map(broadcast_to_frame)
        .collect::<Vec<_>>();
    send_events(writer, session, frames, limits, keepalive)
}

fn update_peer_ack(
    store: &impl SessionStore,
    peer_acks: &Arc<Mutex<PeerAckTable>>,
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
    session: &'a Session<Inbound, Streaming<M>>,
    broadcaster: &'a EventBroadcaster,
    wal_reader: Option<&'a WalRangeReader>,
    limits: &'a Limits,
    allowed_set: Option<&'a BTreeSet<NamespaceId>>,
    keepalive: &'a mut KeepaliveTracker,
}

fn handle_want<M>(want: &Want, ctx: &mut WantContext<'_, M>) -> Result<(), ConnectionError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpStream;
    use std::time::Duration;
    use uuid::Uuid;

    use crate::core::{
        Applied, Durable, EventId, HeadStatus, NamespacePolicy, Seq0, Sha256, StoreEpoch, StoreId,
        Watermark,
    };
    use crate::daemon::broadcast::BroadcasterLimits;
    use crate::daemon::repl::proto::{Capabilities, Hello};
    use crate::daemon::repl::{IngestOutcome, ReplError, WatermarkSnapshot};

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
            _namespace: &NamespaceId,
            _origin: &ReplicaId,
            batch: &[crate::core::VerifiedEvent<crate::core::PrevVerified>],
            _now_ms: u64,
        ) -> Result<IngestOutcome, ReplError> {
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

    fn test_limits() -> Limits {
        Limits::default()
    }

    fn test_policies() -> BTreeMap<NamespaceId, NamespacePolicy> {
        let mut policies = BTreeMap::new();
        policies.insert(NamespaceId::core(), NamespacePolicy::core_default());
        policies
    }

    fn test_identity() -> StoreIdentity {
        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let epoch = StoreEpoch::new(1);
        StoreIdentity::new(store_id, epoch)
    }

    fn test_broadcaster(limits: &Limits) -> EventBroadcaster {
        EventBroadcaster::new(BroadcasterLimits::from_limits(limits))
    }

    fn build_hello(identity: StoreIdentity, replica_id: ReplicaId, limits: &Limits) -> ReplMessage {
        ReplMessage::Hello(Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: identity.store_id,
            store_epoch: identity.store_epoch,
            sender_replica_id: replica_id,
            hello_nonce: 1,
            max_frame_bytes: limits.max_frame_bytes as u32,
            requested_namespaces: vec![NamespaceId::core()],
            offered_namespaces: vec![NamespaceId::core()],
            seen_durable: BTreeMap::new(),
            seen_applied: Some(BTreeMap::new()),
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        })
    }

    fn send_message(writer: &mut FrameWriter<TcpStream>, message: ReplMessage) {
        let envelope = ReplEnvelope {
            version: PROTOCOL_VERSION_V1,
            message,
        };
        let bytes = encode_envelope(&envelope).expect("encode");
        writer.write_frame(&bytes).expect("write");
    }

    fn read_message(reader: &mut FrameReader<TcpStream>, limits: &Limits) -> ReplMessage {
        let bytes = reader.read_next().expect("read").expect("frame");
        let envelope = decode_envelope(&bytes, limits).expect("decode");
        envelope.message
    }

    fn base_config(
        listen_addr: String,
        identity: StoreIdentity,
        local_replica: ReplicaId,
        limits: Limits,
    ) -> ReplicationServerConfig {
        ReplicationServerConfig {
            listen_addr,
            local_store: identity,
            local_replica_id: local_replica,
            admission: AdmissionController::new(&limits),
            broadcaster: test_broadcaster(&limits),
            peer_acks: Arc::new(Mutex::new(PeerAckTable::new())),
            policies: test_policies(),
            roster: None,
            wal_reader: None,
            limits,
            max_connections: NonZeroUsize::new(2),
        }
    }

    #[test]
    fn server_listens_and_spawns_session() {
        let limits = test_limits();
        let identity = test_identity();
        let local_replica = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let config = base_config(
            "127.0.0.1:0".to_string(),
            identity,
            local_replica,
            limits.clone(),
        );
        let server = ReplicationServer::new(SharedSessionStore::new(TestStore), config);
        let handle = server.start().expect("start");

        let stream = TcpStream::connect(handle.local_addr()).expect("connect");
        let mut writer =
            FrameWriter::new(stream.try_clone().expect("clone"), limits.max_frame_bytes);
        let mut reader = FrameReader::new(stream, limits.max_frame_bytes);
        let hello = build_hello(
            identity,
            ReplicaId::new(Uuid::from_bytes([3u8; 16])),
            &limits,
        );
        send_message(&mut writer, hello);

        match read_message(&mut reader, &limits) {
            ReplMessage::Welcome(_) => {}
            other => panic!("unexpected response: {other:?}"),
        }

        handle.shutdown();
    }

    #[test]
    fn connection_limit_enforced() {
        let limits = test_limits();
        let identity = test_identity();
        let local_replica = ReplicaId::new(Uuid::from_bytes([4u8; 16]));
        let mut config = base_config(
            "127.0.0.1:0".to_string(),
            identity,
            local_replica,
            limits.clone(),
        );
        config.max_connections = NonZeroUsize::new(1);
        let server = ReplicationServer::new(SharedSessionStore::new(TestStore), config);
        let handle = server.start().expect("start");

        let first = TcpStream::connect(handle.local_addr()).expect("connect first");
        let mut writer =
            FrameWriter::new(first.try_clone().expect("clone"), limits.max_frame_bytes);
        let mut reader =
            FrameReader::new(first.try_clone().expect("clone"), limits.max_frame_bytes);
        let hello = build_hello(
            identity,
            ReplicaId::new(Uuid::from_bytes([5u8; 16])),
            &limits,
        );
        send_message(&mut writer, hello);
        let response = read_message(&mut reader, &limits);
        assert!(matches!(response, ReplMessage::Welcome(_)));

        let second = TcpStream::connect(handle.local_addr()).expect("connect second");
        second
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("timeout");
        let mut reader = FrameReader::new(second, limits.max_frame_bytes);
        let response = read_message(&mut reader, &limits);
        match response {
            ReplMessage::Error(payload) => {
                assert_eq!(payload.code, ProtocolErrorCode::Overloaded.into())
            }
            other => panic!("unexpected response: {other:?}"),
        }

        drop(first);
        handle.shutdown();
    }

    #[test]
    fn unknown_replica_rejected_when_roster_present() {
        let limits = test_limits();
        let identity = test_identity();
        let local_replica = ReplicaId::new(Uuid::from_bytes([6u8; 16]));
        let mut config = base_config(
            "127.0.0.1:0".to_string(),
            identity,
            local_replica,
            limits.clone(),
        );
        let roster_replica = ReplicaId::new(Uuid::from_bytes([7u8; 16]));
        config.roster = Some(ReplicaRoster {
            replicas: vec![crate::core::ReplicaEntry {
                replica_id: roster_replica,
                name: "alpha".to_string(),
                role: crate::core::ReplicaDurabilityRole::peer(false),
                allowed_namespaces: None,
                expire_after_ms: None,
            }],
        });
        let server = ReplicationServer::new(SharedSessionStore::new(TestStore), config);
        let handle = server.start().expect("start");

        let stream = TcpStream::connect(handle.local_addr()).expect("connect");
        let mut writer =
            FrameWriter::new(stream.try_clone().expect("clone"), limits.max_frame_bytes);
        let mut reader = FrameReader::new(stream, limits.max_frame_bytes);
        let hello = build_hello(
            identity,
            ReplicaId::new(Uuid::from_bytes([8u8; 16])),
            &limits,
        );
        send_message(&mut writer, hello);
        let response = read_message(&mut reader, &limits);
        match response {
            ReplMessage::Error(payload) => {
                assert_eq!(payload.code, ProtocolErrorCode::UnknownReplica.into())
            }
            other => panic!("unexpected response: {other:?}"),
        }

        handle.shutdown();
    }
}
