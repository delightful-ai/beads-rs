//! Server thread loops.
//!
//! Three threads:
//! - Socket acceptor (main thread) - accepts connections, spawns handlers
//! - State thread - owns Daemon, processes requests sequentially
//! - Git thread - owns GitWorker, handles all git IO

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::Arc;
use std::time::Instant;

use crossbeam::channel::{Receiver, Sender};

use super::broadcast::{
    BroadcastError, BroadcastEvent, DropReason, EventSubscription, SubscriberLimits,
};
use super::core::Daemon;
use super::git_worker::{GitOp, GitResult};
use super::ipc::{
    Request, Response, ResponsePayload, decode_request_with_limits, encode_response, send_response,
};
use super::ops::OpError;
use super::remote::RemoteUrl;
use super::repl::{WalRangeError, WalRangeReader};
use crate::core::error::details as error_details;
use crate::core::{
    Applied, ErrorCode, ErrorPayload, EventFrameV1, EventId, Limits, NamespaceId, ReplicaId,
    Sha256, Watermark, Watermarks, decode_event_body,
};

/// Message sent from socket handlers to state thread.
pub enum ServerReply {
    Response(Response),
    Subscribe(SubscribeReply),
}

pub struct SubscribeReply {
    pub ack: Response,
    pub namespace: NamespaceId,
    pub subscription: EventSubscription,
    pub hot_cache: Vec<BroadcastEvent>,
    pub backfill: Vec<EventFrameV1>,
    pub backfill_end: HashMap<ReplicaId, u64>,
}

pub struct RequestMessage {
    pub request: Request,
    pub respond: Sender<ServerReply>,
}

/// Run the state thread loop.
///
/// This is THE serialization point - all state mutations go through here.
/// Uses crossbeam::select! for fair multi-channel receive.
pub fn run_state_loop(
    mut daemon: Daemon,
    req_rx: Receiver<RequestMessage>,
    git_tx: Sender<GitOp>,
    git_result_rx: Receiver<GitResult>,
) {
    let mut sync_waiters: HashMap<RemoteUrl, Vec<Sender<ServerReply>>> = HashMap::new();
    let repl_capacity = daemon.limits().max_repl_ingest_queue_events.max(1);
    let (repl_tx, repl_rx) = crossbeam::channel::bounded(repl_capacity);
    daemon.set_repl_ingest_tx(repl_tx);

    loop {
        let tick = match daemon.next_sync_deadline() {
            Some(deadline) => {
                let wait = deadline.saturating_duration_since(Instant::now());
                crossbeam::channel::after(wait)
            }
            None => crossbeam::channel::never(),
        };

        crossbeam::select! {
            // Client request
            recv(req_rx) -> msg => {
                match msg {
                    Ok(RequestMessage { request, respond }) => {
                        // Sync barrier: wait until repo is clean.
                        if let Request::SyncWait { repo } = request {
                            match daemon.ensure_loaded_and_maybe_start_sync(&repo, &git_tx) {
                                Ok(loaded) => {
                                    let repo_state = match daemon.repo_state(&loaded) {
                                        Ok(repo_state) => repo_state,
                                        Err(e) => {
                                            let _ = respond.send(ServerReply::Response(Response::err(e)));
                                            continue;
                                        }
                                    };
                                    let clean = !repo_state.dirty && !repo_state.sync_in_progress;

                                    if clean {
                                        let _ = respond.send(ServerReply::Response(Response::ok(
                                            super::ipc::ResponsePayload::synced(),
                                        )));
                                    } else {
                                        sync_waiters.entry(loaded.remote().clone()).or_default().push(respond);
                                    }
                                }
                                Err(e) => {
                                    let _ = respond.send(ServerReply::Response(Response::err(e)));
                                }
                            }
                            continue;
                        }

                        if let Request::Subscribe { repo, read } = request {
                            let read = match daemon.normalize_read_consistency(read) {
                                Ok(read) => read,
                                Err(e) => {
                                    let _ = respond.send(ServerReply::Response(Response::err(e)));
                                    continue;
                                }
                            };
                            let loaded = match daemon.ensure_repo_fresh(&repo, &git_tx) {
                                Ok(remote) => remote,
                                Err(e) => {
                                    let _ = respond.send(ServerReply::Response(Response::err(e)));
                                    continue;
                                }
                            };
                            if let Err(err) = daemon.check_read_gate(&loaded, &read) {
                                let _ = respond.send(ServerReply::Response(Response::err(err)));
                                continue;
                            }
                            let store_runtime = match daemon.store_runtime(&loaded) {
                                Ok(runtime) => runtime,
                                Err(e) => {
                                    let _ = respond.send(ServerReply::Response(Response::err(e)));
                                    continue;
                                }
                            };
                            let subscription = match store_runtime
                                .broadcaster
                                .subscribe(subscriber_limits(daemon.limits()))
                            {
                                Ok(subscription) => subscription,
                                Err(err) => {
                                    let _ = respond.send(ServerReply::Response(Response::err(
                                        broadcast_error_to_op(err),
                                    )));
                                    continue;
                                }
                            };
                            let hot_cache = match store_runtime.broadcaster.hot_cache() {
                                Ok(cache) => cache,
                                Err(err) => {
                                    let _ = respond.send(ServerReply::Response(Response::err(
                                        broadcast_error_to_op(err),
                                    )));
                                    continue;
                                }
                            };
                            let namespace = read.namespace().clone();
                            let watermarks_applied = store_runtime.watermarks_applied.clone();
                            let wal_reader = WalRangeReader::new(
                                store_runtime.meta.store_id(),
                                store_runtime.wal_index.clone(),
                                daemon.limits().clone(),
                            );
                            let backfill = match build_backfill_plan(
                                read.require_min_seen(),
                                &namespace,
                                &watermarks_applied,
                                &wal_reader,
                                daemon.limits(),
                            ) {
                                Ok(plan) => plan,
                                Err(err) => {
                                    let _ = respond
                                        .send(ServerReply::Response(Response::err(*err)));
                                    continue;
                                }
                            };
                            let info = crate::api::SubscribeInfo {
                                namespace: namespace.clone(),
                                watermarks_applied,
                            };
                            let ack = Response::ok(ResponsePayload::subscribed(info));
                            let _ = respond.send(ServerReply::Subscribe(SubscribeReply {
                                ack,
                                namespace,
                                subscription,
                                hot_cache,
                                backfill: backfill.frames,
                                backfill_end: backfill.last_seq,
                            }));
                            continue;
                        }

                        // Check for shutdown
                        let is_shutdown = matches!(request, Request::Shutdown);

                        let response = daemon.handle_request(request, &git_tx);
                        let _ = respond.send(ServerReply::Response(response));

                        if is_shutdown {
                            daemon.shutdown_replication();

                            // Sync all dirty repos before exiting (avoid double-borrow).
                            let remotes_to_sync: Vec<RemoteUrl> = daemon
                                .repos()
                                .filter(|(_, s)| s.dirty && !s.sync_in_progress)
                                .filter_map(|(store_id, _)| {
                                    daemon.primary_remote_for_store(store_id).cloned()
                                })
                                .collect();
                            for remote in remotes_to_sync {
                                daemon.maybe_start_sync(&remote, &git_tx);
                            }

                            // Wait for in-flight syncs
                            let mut pending = daemon
                                .repos()
                                .filter(|(_, s)| s.sync_in_progress)
                                .count();

                            while pending > 0 {
                                if let Ok(result) = git_result_rx.recv() {
                                    match result {
                                        GitResult::Sync(remote, sync_result) => {
                                            daemon.complete_sync(&remote, sync_result);
                                            pending -= 1;
                                        }
                                        GitResult::Refresh(remote, refresh_result) => {
                                            // Just complete any in-flight refreshes during shutdown
                                            daemon.complete_refresh(&remote, refresh_result);
                                        }
                                    }
                                } else {
                                    break;
                                }
                            }

                            // Signal git thread to shutdown
                            let _ = git_tx.send(GitOp::Shutdown);
                            return;
                        }

                        daemon.fire_due_syncs(&git_tx);
                        flush_sync_waiters(&daemon, &mut sync_waiters);
                    }
                    Err(_) => {
                        // Channel closed - time to exit
                        let _ = git_tx.send(GitOp::Shutdown);
                        return;
                    }
                }
            }

            recv(tick) -> _ => {
                daemon.fire_due_syncs(&git_tx);
                flush_sync_waiters(&daemon, &mut sync_waiters);
            }

            // Replication ingest request
            recv(repl_rx) -> msg => {
                if let Ok(request) = msg {
                    daemon.handle_repl_ingest(request);
                    daemon.fire_due_syncs(&git_tx);
                    flush_sync_waiters(&daemon, &mut sync_waiters);
                }
            }

            // Git operation completed
            recv(git_result_rx) -> msg => {
                if let Ok(result) = msg {
                    match result {
                        GitResult::Sync(remote, sync_result) => {
                            daemon.complete_sync(&remote, sync_result);
                        }
                        GitResult::Refresh(remote, refresh_result) => {
                            daemon.complete_refresh(&remote, refresh_result);
                        }
                    }
                }
                daemon.fire_due_syncs(&git_tx);
                flush_sync_waiters(&daemon, &mut sync_waiters);
            }
        }
    }
}

fn flush_sync_waiters(daemon: &Daemon, waiters: &mut HashMap<RemoteUrl, Vec<Sender<ServerReply>>>) {
    let ready: Vec<RemoteUrl> = waiters
        .keys()
        .filter(|remote| {
            daemon
                .repo_state_by_url(remote)
                .map(|s| !s.dirty && !s.sync_in_progress)
                .unwrap_or(true)
        })
        .cloned()
        .collect();

    for remote in ready {
        if let Some(list) = waiters.remove(&remote) {
            for respond in list {
                let _ = respond.send(ServerReply::Response(Response::ok(
                    super::ipc::ResponsePayload::synced(),
                )));
            }
        }
    }
}

/// Run the socket acceptor.
///
/// Accepts connections and spawns a handler thread for each.
pub fn run_socket_thread(
    listener: UnixListener,
    req_tx: Sender<RequestMessage>,
    limits: Arc<Limits>,
) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let req_tx = req_tx.clone();
                let limits = Arc::clone(&limits);
                std::thread::spawn(move || handle_client(stream, req_tx, limits));
            }
            Err(e) => {
                tracing::error!("accept error: {}", e);
            }
        }
    }
}

/// Handle a single client connection.
///
/// Reads requests, sends to state thread, waits for response, writes back.
pub(super) fn handle_client(
    stream: UnixStream,
    req_tx: Sender<RequestMessage>,
    limits: Arc<Limits>,
) {
    let reader = match stream.try_clone() {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("failed to clone stream: {}", e);
            return;
        }
    };
    let reader = BufReader::new(reader);
    let mut writer = stream;

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break, // Client disconnected
        };

        // Skip empty lines
        if line.trim().is_empty() {
            continue;
        }

        // Parse request
        let request = match decode_request_with_limits(&line, &limits) {
            Ok(r) => r,
            Err(e) => {
                let resp = Response::err(e);
                let bytes = match encode_response(&resp) {
                    Ok(b) => b,
                    Err(e) => {
                        let msg = e.to_string().replace('"', "\\\"");
                        format!(r#"{{"err":{{"code":"internal","message":"{}"}}}}\n"#, msg)
                            .into_bytes()
                    }
                };
                if writeln!(writer, "{}", String::from_utf8_lossy(&bytes)).is_err() {
                    break;
                }
                continue;
            }
        };

        // Check if this is a shutdown request
        let is_shutdown = matches!(request, Request::Shutdown);

        // Send to state thread, wait for response
        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        if req_tx
            .send(RequestMessage {
                request,
                respond: respond_tx,
            })
            .is_err()
        {
            break; // State thread died
        }

        let reply = match respond_rx.recv() {
            Ok(r) => r,
            Err(_) => break, // State thread died
        };

        match reply {
            ServerReply::Response(response) => {
                // Write response
                let bytes = match encode_response(&response) {
                    Ok(b) => b,
                    Err(e) => {
                        let msg = e.to_string().replace('"', "\\\"");
                        format!(r#"{{"err":{{"code":"internal","message":"{}"}}}}\n"#, msg)
                            .into_bytes()
                    }
                };
                // encode_response already includes newline, but let's be safe
                let response_str = String::from_utf8_lossy(&bytes);
                if write!(writer, "{}", response_str).is_err() {
                    break; // Client disconnected
                }
                if writer.flush().is_err() {
                    break;
                }

                // If shutdown, close connection
                if is_shutdown {
                    break;
                }
            }
            ServerReply::Subscribe(reply) => {
                if send_response(&mut writer, &reply.ack).is_err() {
                    break;
                }
                stream_subscription(&mut writer, reply, &limits);
                break;
            }
        }
    }
}

fn stream_subscription(writer: &mut UnixStream, reply: SubscribeReply, limits: &Limits) {
    let namespace = reply.namespace;
    let subscriber_limits = subscriber_limits(limits);
    let backfill_end = reply.backfill_end;

    for frame in reply.backfill {
        if !send_stream_frame(writer, frame, limits) {
            return;
        }
    }

    for event in reply.hot_cache {
        if event.namespace != namespace {
            continue;
        }
        if should_skip_backfill(&backfill_end, &event) {
            continue;
        }
        if !send_stream_event(writer, event, limits) {
            return;
        }
    }

    let subscription = reply.subscription;
    loop {
        match subscription.recv() {
            Ok(event) => {
                if event.namespace != namespace {
                    continue;
                }
                if should_skip_backfill(&backfill_end, &event) {
                    continue;
                }
                if !send_stream_event(writer, event, limits) {
                    return;
                }
            }
            Err(_) => {
                if matches!(
                    subscription.drop_reason(),
                    Some(DropReason::SubscriberLagged)
                ) {
                    let payload =
                        ErrorPayload::new(ErrorCode::SubscriberLagged, "subscriber lagged", true)
                            .with_details(error_details::SubscriberLaggedDetails {
                                max_queue_bytes: Some(subscriber_limits.max_bytes as u64),
                                max_queue_events: Some(subscriber_limits.max_events as u64),
                            });
                    let _ = send_response(writer, &Response::err(payload));
                }
                return;
            }
        }
    }
}

fn send_stream_event(writer: &mut UnixStream, event: BroadcastEvent, limits: &Limits) -> bool {
    let response = stream_event_response(event, limits);
    let should_continue = !matches!(response, Response::Err { .. });
    if send_response(writer, &response).is_err() {
        return false;
    }
    should_continue
}

fn stream_event_response(event: BroadcastEvent, limits: &Limits) -> Response {
    stream_event_response_from_parts(
        event.event_id,
        event.sha256,
        event.prev_sha256,
        event.bytes.as_ref(),
        limits,
    )
}

fn send_stream_frame(writer: &mut UnixStream, frame: EventFrameV1, limits: &Limits) -> bool {
    let response = stream_frame_response(&frame, limits);
    let should_continue = !matches!(response, Response::Err { .. });
    if send_response(writer, &response).is_err() {
        return false;
    }
    should_continue
}

fn stream_frame_response(frame: &EventFrameV1, limits: &Limits) -> Response {
    stream_event_response_from_parts(
        frame.eid.clone(),
        frame.sha256,
        frame.prev_sha256,
        frame.bytes.as_ref(),
        limits,
    )
}

fn stream_event_response_from_parts(
    event_id: EventId,
    sha256: Sha256,
    prev_sha256: Option<Sha256>,
    bytes: &[u8],
    limits: &Limits,
) -> Response {
    let (_, body) = match decode_event_body(bytes, limits) {
        Ok(body) => body,
        Err(err) => {
            return Response::err(
                ErrorPayload::new(ErrorCode::Corruption, "event body decode failed", false)
                    .with_details(error_details::CorruptionDetails {
                        reason: err.to_string(),
                    }),
            );
        }
    };

    let stream_event = crate::api::StreamEvent {
        event_id,
        sha256: hex::encode(sha256.as_bytes()),
        prev_sha256: prev_sha256.map(|prev| hex::encode(prev.as_bytes())),
        body: crate::api::EventBody::from(&body),
        body_bytes_hex: Some(hex::encode(bytes)),
    };

    Response::ok(ResponsePayload::event(stream_event))
}

fn should_skip_backfill(backfill_end: &HashMap<ReplicaId, u64>, event: &BroadcastEvent) -> bool {
    let origin = event.event_id.origin_replica_id;
    let seq = event.event_id.origin_seq.get();
    backfill_end
        .get(&origin)
        .is_some_and(|backfill_seq| seq <= *backfill_seq)
}

fn subscriber_limits(limits: &Limits) -> SubscriberLimits {
    let max_events = limits.max_event_batch_events.max(1);
    let max_bytes = limits.max_event_batch_bytes.max(1);
    SubscriberLimits::new(max_events, max_bytes).expect("subscriber limits")
}

fn broadcast_error_to_op(err: BroadcastError) -> OpError {
    match err {
        BroadcastError::SubscriberLimitReached { max_subscribers } => OpError::Overloaded {
            subsystem: error_details::OverloadedSubsystem::Ipc,
            retry_after_ms: None,
            queue_bytes: None,
            queue_events: Some(max_subscribers as u64),
        },
        BroadcastError::InvalidSubscriberLimits { .. } => {
            OpError::Internal("invalid IPC subscriber limits")
        }
        BroadcastError::LockPoisoned => OpError::Internal("broadcaster lock poisoned"),
    }
}

#[derive(Debug, Default)]
struct BackfillPlan {
    frames: Vec<EventFrameV1>,
    last_seq: HashMap<ReplicaId, u64>,
}

trait WalRangeRead {
    fn read_range(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        from_seq_excl: u64,
        max_bytes: usize,
    ) -> Result<Vec<EventFrameV1>, WalRangeError>;
}

impl WalRangeRead for WalRangeReader {
    fn read_range(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        from_seq_excl: u64,
        max_bytes: usize,
    ) -> Result<Vec<EventFrameV1>, WalRangeError> {
        WalRangeReader::read_range(self, namespace, origin, from_seq_excl, max_bytes)
    }
}

fn build_backfill_plan<R: WalRangeRead>(
    required: Option<&Watermarks<Applied>>,
    namespace: &NamespaceId,
    applied: &Watermarks<Applied>,
    wal_reader: &R,
    limits: &Limits,
) -> Result<BackfillPlan, Box<ErrorPayload>> {
    let Some(required) = required else {
        return Ok(BackfillPlan::default());
    };

    let mut plan = BackfillPlan::default();
    for (origin, required_mark) in required.origins(namespace) {
        let required_seq = required_mark.seq().get();
        let current_seq = applied
            .get(namespace, origin)
            .copied()
            .unwrap_or_else(Watermark::genesis)
            .seq()
            .get();
        debug_assert!(
            current_seq >= required_seq,
            "read gate should ensure applied >= required"
        );

        plan.last_seq.insert(*origin, current_seq);
        if current_seq <= required_seq {
            continue;
        }

        let mut from_seq_excl = required_seq;
        while from_seq_excl < current_seq {
            let frames = wal_reader
                .read_range(
                    namespace,
                    origin,
                    from_seq_excl,
                    limits.max_event_batch_bytes,
                )
                .map_err(wal_range_error_payload)?;
            let Some(last) = frames.last() else {
                return Err(wal_range_error_payload(WalRangeError::MissingRange {
                    namespace: namespace.clone(),
                    origin: *origin,
                    from_seq_excl,
                }));
            };
            from_seq_excl = last.eid.origin_seq.get();
            plan.frames.extend(frames);
        }
    }

    Ok(plan)
}

fn wal_range_error_payload(err: WalRangeError) -> Box<ErrorPayload> {
    Box::new(match err {
        WalRangeError::MissingRange { namespace, .. } => {
            ErrorPayload::new(ErrorCode::BootstrapRequired, "bootstrap required", false)
                .with_details(error_details::BootstrapRequiredDetails {
                    namespaces: vec![namespace],
                    reason: error_details::SnapshotRangeReason::RangeMissing,
                })
        }
        other => other.as_error_payload(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::cell::RefCell;
    use uuid::Uuid;

    use crate::core::{
        Applied, Canonical, EventBytes, EventFrameV1, EventId, HeadStatus, Opaque, Seq0, Seq1,
        Sha256, Watermarks,
    };

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct ReadCall {
        namespace: NamespaceId,
        origin: ReplicaId,
        from_seq_excl: u64,
        max_bytes: usize,
    }

    #[derive(Default)]
    struct FakeWalReader {
        responses: RefCell<HashMap<(ReplicaId, u64), Result<Vec<EventFrameV1>, WalRangeError>>>,
        calls: RefCell<Vec<ReadCall>>,
    }

    impl FakeWalReader {
        fn with_response(
            self,
            origin: ReplicaId,
            from_seq_excl: u64,
            response: Result<Vec<EventFrameV1>, WalRangeError>,
        ) -> Self {
            self.responses
                .borrow_mut()
                .insert((origin, from_seq_excl), response);
            self
        }

        fn calls(&self) -> Vec<ReadCall> {
            self.calls.borrow().clone()
        }
    }

    impl WalRangeRead for FakeWalReader {
        fn read_range(
            &self,
            namespace: &NamespaceId,
            origin: &ReplicaId,
            from_seq_excl: u64,
            max_bytes: usize,
        ) -> Result<Vec<EventFrameV1>, WalRangeError> {
            self.calls.borrow_mut().push(ReadCall {
                namespace: namespace.clone(),
                origin: *origin,
                from_seq_excl,
                max_bytes,
            });
            self.responses
                .borrow_mut()
                .remove(&(*origin, from_seq_excl))
                .expect("missing wal reader response")
        }
    }

    fn frame(origin: ReplicaId, namespace: NamespaceId, seq: u64) -> EventFrameV1 {
        EventFrameV1 {
            eid: EventId::new(origin, namespace, Seq1::from_u64(seq).unwrap()),
            sha256: Sha256([seq as u8; 32]),
            prev_sha256: None,
            bytes: EventBytes::<Opaque>::new(Bytes::from(vec![seq as u8])),
        }
    }

    fn watermark(seq: u64) -> Watermark<Applied> {
        Watermark::new(Seq0::new(seq), HeadStatus::Unknown).expect("watermark")
    }

    #[test]
    fn backfill_plan_reads_ranges_and_tracks_last_seq() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([5u8; 16]));

        let mut required = Watermarks::<Applied>::new();
        required
            .observe_at_least(&namespace, &origin, watermark(2).seq(), HeadStatus::Unknown)
            .unwrap();
        let mut applied = Watermarks::<Applied>::new();
        applied
            .observe_at_least(&namespace, &origin, watermark(4).seq(), HeadStatus::Unknown)
            .unwrap();

        let frame3 = frame(origin, namespace.clone(), 3);
        let frame4 = frame(origin, namespace.clone(), 4);
        let reader = FakeWalReader::default()
            .with_response(origin, 2, Ok(vec![frame3.clone()]))
            .with_response(origin, 3, Ok(vec![frame4.clone()]));
        let limits = Limits::default();

        let plan =
            build_backfill_plan(Some(&required), &namespace, &applied, &reader, &limits).unwrap();

        assert_eq!(plan.frames, vec![frame3, frame4]);
        assert_eq!(plan.last_seq.get(&origin), Some(&4));

        let calls = reader.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls,
            vec![
                ReadCall {
                    namespace: namespace.clone(),
                    origin,
                    from_seq_excl: 2,
                    max_bytes: limits.max_event_batch_bytes,
                },
                ReadCall {
                    namespace: namespace.clone(),
                    origin,
                    from_seq_excl: 3,
                    max_bytes: limits.max_event_batch_bytes,
                },
            ]
        );
    }

    #[test]
    fn backfill_missing_range_is_bootstrap_required() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([9u8; 16]));

        let mut required = Watermarks::<Applied>::new();
        required
            .observe_at_least(&namespace, &origin, watermark(1).seq(), HeadStatus::Unknown)
            .unwrap();
        let mut applied = Watermarks::<Applied>::new();
        applied
            .observe_at_least(&namespace, &origin, watermark(2).seq(), HeadStatus::Unknown)
            .unwrap();

        let reader = FakeWalReader::default().with_response(
            origin,
            1,
            Err(WalRangeError::MissingRange {
                namespace: namespace.clone(),
                origin,
                from_seq_excl: 1,
            }),
        );

        let err = *build_backfill_plan(
            Some(&required),
            &namespace,
            &applied,
            &reader,
            &Limits::default(),
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::BootstrapRequired);
        let details = err
            .details_as::<error_details::BootstrapRequiredDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.namespaces, vec![namespace]);
        assert_eq!(
            details.reason,
            error_details::SnapshotRangeReason::RangeMissing
        );
    }

    #[test]
    fn backfill_corrupt_range_is_corruption() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([11u8; 16]));

        let mut required = Watermarks::<Applied>::new();
        required
            .observe_at_least(&namespace, &origin, watermark(1).seq(), HeadStatus::Unknown)
            .unwrap();
        let mut applied = Watermarks::<Applied>::new();
        applied
            .observe_at_least(&namespace, &origin, watermark(2).seq(), HeadStatus::Unknown)
            .unwrap();

        let reader = FakeWalReader::default().with_response(
            origin,
            1,
            Err(WalRangeError::Corrupt {
                namespace: namespace.clone(),
                segment_id: None,
                offset: None,
                reason: "boom".to_string(),
            }),
        );

        let err = *build_backfill_plan(
            Some(&required),
            &namespace,
            &applied,
            &reader,
            &Limits::default(),
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Corruption);
    }

    #[test]
    fn stream_event_decode_failure_is_corruption() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([7u8; 16]));
        let event_id = EventId::new(origin, namespace, Seq1::from_u64(1).unwrap());
        let bytes = EventBytes::<Canonical>::new(Bytes::from(vec![0x01]));
        let event = BroadcastEvent::new(event_id, Sha256([0u8; 32]), None, bytes);

        let response = stream_event_response(event, &Limits::default());
        let Response::Err { err } = response else {
            panic!("expected corruption error");
        };
        assert_eq!(err.code, ErrorCode::Corruption);
        let details = err
            .details_as::<error_details::CorruptionDetails>()
            .unwrap()
            .expect("details");
        assert!(!details.reason.is_empty());
    }
}
