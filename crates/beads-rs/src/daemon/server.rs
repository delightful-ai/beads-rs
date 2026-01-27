//! Server thread loops.
//!
//! Three threads:
//! - Socket acceptor (main thread) - accepts connections, spawns handlers
//! - State thread - owns Daemon, processes requests sequentially
//! - Git thread - owns GitWorker, handles all git IO

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender};
use tracing::Span;

use super::QueryResult;
use super::broadcast::{BroadcastEvent, DropReason};
use super::core::{Daemon, HandleOutcome, NormalizedReadConsistency, ReadGateStatus};
use super::durability_coordinator::{DurabilityCoordinator, ReplicatedPoll};
use super::executor::DurabilityWait;
use super::git_worker::{GitOp, GitResult};
use super::ipc::{
    ReadConsistency, Request, RequestInfo, Response, ResponseExt, ResponsePayload,
    decode_request_with_limits, encode_response, send_response,
};
use super::ops::OpError;
use super::remote::RemoteUrl;
use super::subscription::{SubscribeReply, prepare_subscription, subscriber_limits};
use crate::api::{AdminCheckpointGroup, AdminCheckpointOutput};
use crate::core::error::details as error_details;
use crate::core::{
    CliErrorCode, DurabilityClass, ErrorPayload, EventFrameV1, EventId, Limits, NamespaceId,
    ProtocolErrorCode, ReplicaId, Sha256, StoreId, decode_event_body,
};

/// Message sent from socket handlers to state thread.
pub enum ServerReply {
    Response(Response),
    Subscribe(SubscribeReply),
}

pub struct RequestMessage {
    pub request: Request,
    pub respond: Sender<ServerReply>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadConsistencyTag {
    Default,
    RequireMinSeen,
}

impl ReadConsistencyTag {
    fn as_str(self) -> &'static str {
        match self {
            ReadConsistencyTag::Default => "default",
            ReadConsistencyTag::RequireMinSeen => "require_min_seen",
        }
    }
}

fn read_consistency_tag(read: &ReadConsistency) -> ReadConsistencyTag {
    if read.require_min_seen.is_some() {
        ReadConsistencyTag::RequireMinSeen
    } else {
        ReadConsistencyTag::Default
    }
}

fn request_span(info: &RequestInfo<'_>) -> Span {
    let span = tracing::info_span!(
        "ipc_request",
        request_type = info.op,
        repo = tracing::field::Empty,
        namespace = tracing::field::Empty,
        actor_id = tracing::field::Empty,
        client_request_id = tracing::field::Empty,
        read_consistency = tracing::field::Empty,
    );
    if let Some(repo) = info.repo {
        let repo_display = repo.display();
        span.record("repo", tracing::field::display(repo_display));
    }
    if let Some(namespace) = info.namespace {
        span.record("namespace", tracing::field::display(namespace));
    }
    if let Some(actor_id) = info.actor_id {
        span.record("actor_id", tracing::field::display(actor_id));
    }
    if let Some(client_request_id) = info.client_request_id {
        span.record(
            "client_request_id",
            tracing::field::display(client_request_id),
        );
    }
    if let Some(read) = info.read {
        let read_consistency = read_consistency_tag(read);
        span.record(
            "read_consistency",
            tracing::field::display(read_consistency.as_str()),
        );
    }
    span
}

struct ReadGateWaiter {
    request: Request,
    respond: Sender<ServerReply>,
    repo: PathBuf,
    read: NormalizedReadConsistency,
    span: Span,
    started_at: Instant,
    deadline: Instant,
}

struct DurabilityWaiter {
    respond: Sender<ServerReply>,
    wait: DurabilityWait,
    span: Span,
    started_at: Instant,
    deadline: Instant,
}

struct CheckpointWaiter {
    respond: Sender<ServerReply>,
    store_id: StoreId,
    namespace: NamespaceId,
    min_checkpoint_wall_ms: u64,
    groups: Vec<String>,
}

enum RequestOutcome {
    Continue,
    Shutdown,
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
    let mut checkpoint_waiters: Vec<CheckpointWaiter> = Vec::new();
    let mut read_gate_waiters: Vec<ReadGateWaiter> = Vec::new();
    let mut durability_waiters: Vec<DurabilityWaiter> = Vec::new();
    let repl_capacity = daemon.limits().max_repl_ingest_queue_events.max(1);
    let (repl_tx, repl_rx) = crossbeam::channel::bounded(repl_capacity);
    daemon.set_repl_ingest_tx(repl_tx);

    loop {
        let next_sync = daemon.next_sync_deadline();
        let next_checkpoint = daemon.next_checkpoint_deadline();
        let next_wal_checkpoint = daemon.next_wal_checkpoint_deadline();
        let next_lock_heartbeat = daemon.next_lock_heartbeat_deadline();
        let next_export = daemon.next_export_deadline();
        let next_read_gate = read_gate_waiters.iter().map(|waiter| waiter.deadline).min();
        let next_durability = durability_waiters
            .iter()
            .map(|waiter| waiter.deadline)
            .min();
        let mut next_deadline = None;
        for deadline in [
            next_sync,
            next_checkpoint,
            next_wal_checkpoint,
            next_lock_heartbeat,
            next_export,
            next_read_gate,
            next_durability,
        ]
        .into_iter()
        .flatten()
        {
            next_deadline = Some(match next_deadline {
                Some(current) => std::cmp::min(current, deadline),
                None => deadline,
            });
        }
        let tick = match next_deadline {
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
                        let (span, read_gate) = {
                            let info = request.info();
                            let span = request_span(&info);
                            let read_gate = if info.op == "dep_cycles" {
                                None
                            } else {
                                info.read.map(|read| {
                                    (
                                        info.repo.map(|repo| repo.to_path_buf()),
                                        read.clone(),
                                    )
                                })
                            };
                            (span, read_gate)
                        };
                        let _guard = span.enter();

                        if let Some((Some(repo), read)) = read_gate {
                            let loaded = match daemon.ensure_repo_fresh(&repo, &git_tx) {
                                Ok(loaded) => loaded,
                                Err(err) => {
                                    let _ = respond.send(ServerReply::Response(Response::err_from(err)));
                                    continue;
                                }
                            };
                            let read = match loaded.normalize_read_consistency(read) {
                                Ok(read) => read,
                                Err(err) => {
                                    let _ = respond.send(ServerReply::Response(Response::err_from(err)));
                                    continue;
                                }
                            };

                            if read.require_min_seen().is_some() {
                                match loaded.read_gate_status(&read) {
                                    Ok(ReadGateStatus::Satisfied) => {}
                                    Ok(ReadGateStatus::Unsatisfied {
                                        required,
                                        current_applied,
                                    }) => {
                                        if read.wait_timeout_ms() == 0 {
                                            let err = OpError::RequireMinSeenUnsatisfied {
                                                required: Box::new(required),
                                                current_applied: Box::new(current_applied),
                                            };
                                            let _ = respond.send(ServerReply::Response(Response::err_from(err)));
                                            continue;
                                        }

                                        let started_at = Instant::now();
                                        let timeout = Duration::from_millis(read.wait_timeout_ms());
                                        let deadline =
                                            started_at.checked_add(timeout).unwrap_or(started_at);
                                        read_gate_waiters.push(ReadGateWaiter {
                                            request,
                                            respond,
                                            repo,
                                            read,
                                            span: span.clone(),
                                            started_at,
                                            deadline,
                                        });
                                        continue;
                                    }
                                    Err(err) => {
                                        let _ = respond.send(ServerReply::Response(Response::err_from(err)));
                                        continue;
                                    }
                                }
                            }
                        }

                        let outcome = process_request_message(
                            &mut daemon,
                            request,
                            respond,
                            &git_tx,
                            &mut sync_waiters,
                            &mut checkpoint_waiters,
                            &mut durability_waiters,
                        );

                        if matches!(outcome, RequestOutcome::Shutdown) {
                            daemon.begin_shutdown();
                            daemon.shutdown_replication();
                            daemon.shutdown_export_worker();
                            daemon.drain_ipc_inflight(Duration::from_millis(daemon.limits().dead_ms));

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
                            let shutdown_deadline = Instant::now()
                                + Duration::from_millis(daemon.limits().dead_ms);

                            while pending > 0 {
                                let now = Instant::now();
                                if now >= shutdown_deadline {
                                    tracing::warn!(
                                        pending_syncs = pending,
                                        "shutdown timed out waiting for syncs"
                                    );
                                    break;
                                }

                                match git_result_rx.recv_timeout(shutdown_deadline - now) {
                                    Ok(result) => match result {
                                        GitResult::Sync(remote, sync_result) => {
                                            daemon.complete_sync(&remote, sync_result);
                                            pending -= 1;
                                        }
                                        GitResult::Refresh(remote, refresh_result) => {
                                            // Just complete any in-flight refreshes during shutdown
                                            daemon.complete_refresh(&remote, refresh_result);
                                        }
                                        GitResult::Checkpoint(store_id, group, result) => {
                                            daemon.complete_checkpoint(store_id, &group, result);
                                        }
                                    },
                                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                                        tracing::warn!(
                                            pending_syncs = pending,
                                            "shutdown timed out waiting for syncs"
                                        );
                                        break;
                                    }
                                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                                        break;
                                    }
                                }
                            }

                            // Signal git thread to shutdown
                            let _ = git_tx.send(GitOp::Shutdown);
                            return;
                        }

                        daemon.fire_due_syncs(&git_tx);
                        daemon.fire_due_checkpoints(&git_tx);
                        daemon.fire_due_wal_checkpoints();
                        daemon.fire_due_lock_heartbeats();
                        daemon.fire_due_exports();
                        flush_checkpoint_waiters(&daemon, &mut checkpoint_waiters);
                        flush_sync_waiters(&daemon, &mut sync_waiters);
                        flush_read_gate_waiters(
                            &mut daemon,
                            &mut read_gate_waiters,
                            &git_tx,
                            &mut sync_waiters,
                            &mut checkpoint_waiters,
                            &mut durability_waiters,
                        );
                        flush_durability_waiters(&mut durability_waiters);
                    }
                    Err(_) => {
                        // Channel closed - time to exit
                        daemon.shutdown_export_worker();
                        let _ = git_tx.send(GitOp::Shutdown);
                        return;
                    }
                }
            }

            recv(tick) -> _ => {
                daemon.fire_due_syncs(&git_tx);
                daemon.fire_due_checkpoints(&git_tx);
                daemon.fire_due_wal_checkpoints();
                daemon.fire_due_lock_heartbeats();
                daemon.fire_due_exports();
                flush_checkpoint_waiters(&daemon, &mut checkpoint_waiters);
                flush_sync_waiters(&daemon, &mut sync_waiters);
                flush_read_gate_waiters(
                    &mut daemon,
                    &mut read_gate_waiters,
                    &git_tx,
                    &mut sync_waiters,
                    &mut checkpoint_waiters,
                    &mut durability_waiters,
                );
                flush_durability_waiters(&mut durability_waiters);
            }

            // Replication ingest request
            recv(repl_rx) -> msg => {
                if let Ok(request) = msg {
                    daemon.handle_repl_ingest(request);
                    daemon.fire_due_syncs(&git_tx);
                    daemon.fire_due_checkpoints(&git_tx);
                    daemon.fire_due_wal_checkpoints();
                    daemon.fire_due_lock_heartbeats();
                    daemon.fire_due_exports();
                    flush_checkpoint_waiters(&daemon, &mut checkpoint_waiters);
                    flush_sync_waiters(&daemon, &mut sync_waiters);
                    flush_read_gate_waiters(
                        &mut daemon,
                        &mut read_gate_waiters,
                        &git_tx,
                        &mut sync_waiters,
                        &mut checkpoint_waiters,
                        &mut durability_waiters,
                    );
                    flush_durability_waiters(&mut durability_waiters);
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
                        GitResult::Checkpoint(store_id, group, result) => {
                            daemon.complete_checkpoint(store_id, &group, result);
                        }
                    }
                }
                daemon.fire_due_syncs(&git_tx);
                daemon.fire_due_checkpoints(&git_tx);
                daemon.fire_due_wal_checkpoints();
                daemon.fire_due_lock_heartbeats();
                daemon.fire_due_exports();
                flush_checkpoint_waiters(&daemon, &mut checkpoint_waiters);
                flush_sync_waiters(&daemon, &mut sync_waiters);
                flush_read_gate_waiters(
                    &mut daemon,
                    &mut read_gate_waiters,
                    &git_tx,
                    &mut sync_waiters,
                    &mut checkpoint_waiters,
                    &mut durability_waiters,
                );
                flush_durability_waiters(&mut durability_waiters);
            }
        }
    }
}

fn process_request_message(
    daemon: &mut Daemon,
    request: Request,
    respond: Sender<ServerReply>,
    git_tx: &Sender<GitOp>,
    sync_waiters: &mut HashMap<RemoteUrl, Vec<Sender<ServerReply>>>,
    checkpoint_waiters: &mut Vec<CheckpointWaiter>,
    durability_waiters: &mut Vec<DurabilityWaiter>,
) -> RequestOutcome {
    // Sync barrier: wait until repo is clean.
    if let Request::SyncWait { ctx, .. } = request {
        match daemon.ensure_loaded_and_maybe_start_sync(&ctx.path, git_tx) {
            Ok(loaded) => {
                let repo_state = loaded.lane();
                let clean = !repo_state.dirty && !repo_state.sync_in_progress;

                if clean {
                    let _ = respond.send(ServerReply::Response(Response::ok(
                        super::ipc::ResponsePayload::synced(),
                    )));
                } else {
                    sync_waiters
                        .entry(loaded.remote().clone())
                        .or_default()
                        .push(respond);
                }
            }
            Err(e) => {
                let _ = respond.send(ServerReply::Response(Response::err_from(e)));
            }
        }
        return RequestOutcome::Continue;
    }

    if let Request::AdminCheckpointWait { ctx, payload } = request {
        let proof = match daemon.ensure_repo_loaded_strict(&ctx.path, git_tx) {
            Ok(proof) => proof,
            Err(err) => {
                let _ = respond.send(ServerReply::Response(Response::err_from(err)));
                return RequestOutcome::Continue;
            }
        };
        let namespace = match proof.normalize_namespace(payload.namespace) {
            Ok(namespace) => namespace,
            Err(err) => {
                let _ = respond.send(ServerReply::Response(Response::err_from(err)));
                return RequestOutcome::Continue;
            }
        };
        let store_id = proof.store_id();
        drop(proof);
        let min_checkpoint_wall_ms = daemon.clock().wall_ms();
        let groups = daemon.force_checkpoint_for_namespace(store_id, &namespace);
        if groups.is_empty() {
            let _ = respond.send(ServerReply::Response(Response::err_from(
                OpError::InvalidRequest {
                    field: Some("checkpoint".into()),
                    reason: format!("no checkpoint groups scheduled for namespace {namespace}",),
                },
            )));
            return RequestOutcome::Continue;
        }

        match checkpoint_wait_ready(
            daemon,
            store_id,
            &namespace,
            min_checkpoint_wall_ms,
            &groups,
        ) {
            Ok(Some(output)) => {
                let _ = respond.send(ServerReply::Response(Response::ok(ResponsePayload::query(
                    QueryResult::AdminCheckpoint(output),
                ))));
            }
            Ok(None) => {
                checkpoint_waiters.push(CheckpointWaiter {
                    respond,
                    store_id,
                    namespace,
                    min_checkpoint_wall_ms,
                    groups,
                });
            }
            Err(err) => {
                let _ = respond.send(ServerReply::Response(Response::err_from(err)));
            }
        }

        return RequestOutcome::Continue;
    }

    if let Request::Subscribe { ctx, .. } = request {
        match prepare_subscription(daemon, &ctx.repo.path, ctx.read, git_tx) {
            Ok(reply) => {
                let _ = respond.send(ServerReply::Subscribe(reply));
            }
            Err(err) => {
                let _ = respond.send(ServerReply::Response(Response::err_from(*err)));
            }
        }
        return RequestOutcome::Continue;
    }

    let is_shutdown = matches!(request, Request::Shutdown);

    let outcome = daemon.handle_request(request, git_tx);
    match outcome {
        HandleOutcome::Response(response) => {
            let _ = respond.send(ServerReply::Response(response));
        }
        HandleOutcome::DurabilityWait(wait) => {
            let started_at = Instant::now();
            let deadline = started_at
                .checked_add(wait.wait_timeout)
                .unwrap_or(started_at);
            let span = tracing::Span::current();
            durability_waiters.push(DurabilityWaiter {
                respond,
                wait,
                span,
                started_at,
                deadline,
            });
        }
    }

    if is_shutdown {
        RequestOutcome::Shutdown
    } else {
        RequestOutcome::Continue
    }
}

fn flush_read_gate_waiters(
    daemon: &mut Daemon,
    waiters: &mut Vec<ReadGateWaiter>,
    git_tx: &Sender<GitOp>,
    sync_waiters: &mut HashMap<RemoteUrl, Vec<Sender<ServerReply>>>,
    checkpoint_waiters: &mut Vec<CheckpointWaiter>,
    durability_waiters: &mut Vec<DurabilityWaiter>,
) {
    if waiters.is_empty() {
        return;
    }

    let now = Instant::now();
    let mut remaining = Vec::new();
    for waiter in waiters.drain(..) {
        let span = waiter.span.clone();
        let _guard = span.enter();
        let loaded = match daemon.ensure_repo_fresh(&waiter.repo, git_tx) {
            Ok(loaded) => loaded,
            Err(err) => {
                let _ = waiter
                    .respond
                    .send(ServerReply::Response(Response::err_from(err)));
                continue;
            }
        };

        let status = loaded.read_gate_status(&waiter.read);
        drop(loaded);
        match status {
            Ok(ReadGateStatus::Satisfied) => {
                let _ = process_request_message(
                    daemon,
                    waiter.request,
                    waiter.respond,
                    git_tx,
                    sync_waiters,
                    checkpoint_waiters,
                    durability_waiters,
                );
            }
            Ok(ReadGateStatus::Unsatisfied {
                required,
                current_applied,
            }) => {
                if now >= waiter.deadline {
                    let waited_ms = (now.duration_since(waiter.started_at).as_millis())
                        .min(u64::MAX as u128) as u64;
                    let err = OpError::RequireMinSeenTimeout {
                        waited_ms,
                        required: Box::new(required),
                        current_applied: Box::new(current_applied),
                    };
                    let _ = waiter
                        .respond
                        .send(ServerReply::Response(Response::err_from(err)));
                    continue;
                }
                remaining.push(waiter);
            }
            Err(err) => {
                let _ = waiter
                    .respond
                    .send(ServerReply::Response(Response::err_from(err)));
            }
        }
    }

    *waiters = remaining;
}

fn flush_durability_waiters(waiters: &mut Vec<DurabilityWaiter>) {
    if waiters.is_empty() {
        return;
    }

    let now = Instant::now();
    let mut remaining = Vec::new();
    for waiter in waiters.drain(..) {
        let span = waiter.span.clone();
        let _guard = span.enter();
        let requested = waiter.wait.requested;
        let DurabilityClass::ReplicatedFsync { k } = requested else {
            let _ = waiter
                .respond
                .send(ServerReply::Response(Response::ok(ResponsePayload::Op(
                    waiter.wait.response,
                ))));
            continue;
        };

        match waiter.wait.coordinator.poll_replicated(
            &waiter.wait.namespace,
            waiter.wait.origin,
            waiter.wait.seq,
            k,
        ) {
            Ok(ReplicatedPoll::Satisfied { acked_by }) => {
                let mut response = waiter.wait.response;
                response.receipt = DurabilityCoordinator::achieved_receipt(
                    response.receipt,
                    requested,
                    k,
                    acked_by,
                );
                let _ =
                    waiter
                        .respond
                        .send(ServerReply::Response(Response::ok(ResponsePayload::Op(
                            response,
                        ))));
            }
            Ok(ReplicatedPoll::Pending { acked_by, eligible }) => {
                if now >= waiter.deadline {
                    let waited_ms = (now.duration_since(waiter.started_at).as_millis())
                        .min(u64::MAX as u128) as u64;
                    let pending = DurabilityCoordinator::pending_replica_ids(&eligible, &acked_by);
                    let pending_receipt = DurabilityCoordinator::pending_receipt(
                        waiter.wait.response.receipt,
                        requested,
                        acked_by,
                    );
                    let err = OpError::DurabilityTimeout {
                        requested,
                        waited_ms,
                        pending_replica_ids: Some(pending),
                        receipt: Box::new(pending_receipt),
                    };
                    let _ = waiter
                        .respond
                        .send(ServerReply::Response(Response::err_from(err)));
                    continue;
                }
                remaining.push(waiter);
            }
            Err(err) => {
                let _ = waiter
                    .respond
                    .send(ServerReply::Response(Response::err_from(err)));
            }
        }
    }

    *waiters = remaining;
}

fn checkpoint_wait_ready(
    daemon: &Daemon,
    store_id: StoreId,
    namespace: &NamespaceId,
    min_checkpoint_wall_ms: u64,
    groups: &[String],
) -> Result<Option<AdminCheckpointOutput>, OpError> {
    let snapshots = daemon.checkpoint_group_snapshots(store_id);
    let mut matched = Vec::new();
    for snapshot in snapshots {
        if groups.iter().any(|group| group == &snapshot.group) {
            matched.push(snapshot);
        }
    }

    if matched.len() != groups.len() {
        return Err(OpError::Internal("checkpoint group missing from scheduler"));
    }

    let ready = matched.iter().all(|snapshot| {
        !snapshot.dirty
            && !snapshot.in_flight
            && snapshot
                .last_checkpoint_wall_ms
                .is_some_and(|wall_ms| wall_ms >= min_checkpoint_wall_ms)
    });

    if !ready {
        return Ok(None);
    }

    let checkpoint_groups = matched
        .into_iter()
        .map(|snapshot| AdminCheckpointGroup {
            group: snapshot.group,
            namespaces: snapshot.namespaces,
            git_ref: snapshot.git_ref,
            dirty: snapshot.dirty,
            in_flight: snapshot.in_flight,
            last_checkpoint_wall_ms: snapshot.last_checkpoint_wall_ms,
        })
        .collect();

    Ok(Some(AdminCheckpointOutput {
        namespace: namespace.clone(),
        checkpoint_groups,
    }))
}

fn flush_checkpoint_waiters(daemon: &Daemon, waiters: &mut Vec<CheckpointWaiter>) {
    if waiters.is_empty() {
        return;
    }

    let mut remaining = Vec::new();
    for waiter in waiters.drain(..) {
        match checkpoint_wait_ready(
            daemon,
            waiter.store_id,
            &waiter.namespace,
            waiter.min_checkpoint_wall_ms,
            &waiter.groups,
        ) {
            Ok(Some(output)) => {
                let _ = waiter.respond.send(ServerReply::Response(Response::ok(
                    ResponsePayload::query(QueryResult::AdminCheckpoint(output)),
                )));
            }
            Ok(None) => remaining.push(waiter),
            Err(err) => {
                let _ = waiter
                    .respond
                    .send(ServerReply::Response(Response::err_from(err)));
            }
        }
    }

    *waiters = remaining;
}

fn flush_sync_waiters(daemon: &Daemon, waiters: &mut HashMap<RemoteUrl, Vec<Sender<ServerReply>>>) {
    let ready: Vec<RemoteUrl> = waiters
        .keys()
        .filter(|remote| {
            daemon
                .git_lane_state_by_url(remote)
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
                let resp = Response::err_from(e);
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
                    let payload = ErrorPayload::new(
                        ProtocolErrorCode::SubscriberLagged.into(),
                        "subscriber lagged",
                        true,
                    )
                    .with_details(error_details::SubscriberLaggedDetails {
                        reason: None,
                        max_queue_bytes: Some(subscriber_limits.max_bytes as u64),
                        max_queue_events: Some(subscriber_limits.max_events as u64),
                    });
                    let _ = send_response(writer, &Response::err_from(payload));
                } else {
                    let payload = ErrorPayload::new(
                        CliErrorCode::Disconnected.into(),
                        "subscription closed",
                        true,
                    );
                    let _ = send_response(writer, &Response::err_from(payload));
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
            return Response::err_from(
                ErrorPayload::new(
                    ProtocolErrorCode::WalCorrupt.into(),
                    "event body decode failed",
                    false,
                )
                .with_details(error_details::WalCorruptDetails {
                    namespace: event_id.namespace.clone(),
                    segment_id: None,
                    offset: None,
                    reason: err.to_string(),
                }),
            );
        }
    };

    let stream_event = crate::api::StreamEvent {
        event_id,
        sha256: hex::encode(sha256.as_bytes()),
        prev_sha256: prev_sha256.map(|prev| hex::encode(prev.as_bytes())),
        body: crate::api::EventBody::from(body.as_ref()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::BTreeMap;
    use std::num::NonZeroU32;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::core::replica_roster::ReplicaEntry;
    use crate::core::{
        ActorId, Applied, BeadId, BeadType, ClientRequestId, DurabilityClass, DurabilityReceipt,
        Durable, EventBytes, EventId, HeadStatus, NamespaceId, NamespacePolicy, Opaque, Priority,
        ReplicaDurabilityRole, ReplicaRole, ReplicaRoster, Seq0, Seq1, Sha256, StoreEpoch, StoreId,
        StoreIdentity, TxnId, Watermark, Watermarks,
    };
    use crate::daemon::core::insert_store_for_tests;
    use crate::daemon::ipc::MutationMeta;
    use crate::daemon::ipc::OpResponse;
    use crate::daemon::ops::OpResult;
    use crate::daemon::repl::PeerAckTable;
    use crate::daemon::repl::proto::WatermarkState;

    struct TestEnv {
        _temp: TempDir,
        _override: crate::paths::DataDirOverride,
        repo_path: PathBuf,
        git_tx: Sender<GitOp>,
        daemon: Daemon,
    }

    impl TestEnv {
        fn new() -> Self {
            let temp = TempDir::new().unwrap();
            let data_dir = temp.path().join("data");
            std::fs::create_dir_all(&data_dir).unwrap();
            let override_guard = crate::paths::override_data_dir_for_tests(Some(data_dir.clone()));
            let actor = ActorId::new("test@host".to_string()).unwrap();
            let mut daemon = Daemon::new(actor);
            let repo_path = temp.path().join("repo");
            std::fs::create_dir_all(&repo_path).unwrap();
            let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
            let remote = RemoteUrl("example.com/test/repo".into());
            insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).unwrap();
            let (git_tx, _git_rx) = crossbeam::channel::unbounded();

            Self {
                _temp: temp,
                _override: override_guard,
                repo_path,
                git_tx,
                daemon,
            }
        }
    }

    fn watermark(seq: u64) -> Watermark<Applied> {
        let head = if seq == 0 {
            HeadStatus::Genesis
        } else {
            HeadStatus::Known([seq as u8; 32])
        };
        Watermark::new(Seq0::new(seq), head).expect("watermark")
    }

    #[test]
    fn request_context_extracts_create_fields() {
        let repo = PathBuf::from("/tmp/repo");
        let namespace = NamespaceId::core();
        let actor = ActorId::new("actor@example.com").unwrap();
        let client_request_id = ClientRequestId::new(Uuid::from_bytes([7u8; 16]));
        let meta = MutationMeta {
            namespace: Some(namespace.clone()),
            client_request_id: Some(client_request_id),
            actor_id: Some(actor.clone()),
            durability: None,
        };
        let request = Request::Create {
            ctx: crate::daemon::ipc::MutationCtx::new(repo.clone(), meta),
            payload: crate::daemon::ipc::CreatePayload {
                id: None,
                parent: None,
                title: "title".to_string(),
                bead_type: BeadType::Task,
                priority: Priority::MEDIUM,
                description: None,
                design: None,
                acceptance_criteria: None,
                assignee: None,
                external_ref: None,
                estimated_minutes: None,
                labels: Vec::new(),
                dependencies: Vec::new(),
            },
        };

        let info = request.info();
        assert_eq!(info.op, "create");
        assert_eq!(info.repo, Some(repo.as_path()));
        assert_eq!(info.namespace, Some(&namespace));
        assert_eq!(info.actor_id, Some(&actor));
        assert_eq!(info.client_request_id, Some(&client_request_id));
        assert!(info.read.is_none());
    }

    #[test]
    fn request_context_extracts_show_fields() {
        let repo = PathBuf::from("/tmp/repo");
        let namespace = NamespaceId::core();
        let read = ReadConsistency {
            namespace: Some(namespace.clone()),
            require_min_seen: None,
            wait_timeout_ms: None,
        };
        let request = Request::Show {
            ctx: crate::daemon::ipc::ReadCtx::new(repo.clone(), read),
            payload: crate::daemon::ipc::IdPayload {
                id: BeadId::parse("bd-123").expect("bead id"),
            },
        };

        let info = request.info();
        assert_eq!(info.op, "show");
        assert_eq!(info.repo, Some(repo.as_path()));
        assert_eq!(info.namespace, Some(&namespace));
        assert!(info.read.is_some());
        assert_eq!(
            info.read.map(read_consistency_tag),
            Some(ReadConsistencyTag::Default)
        );
    }

    #[test]
    fn request_span_includes_schema_fields() {
        use crate::telemetry::schema;
        use std::collections::BTreeMap;
        use std::sync::{Arc, Mutex};
        use tracing::Subscriber;
        use tracing::field::{Field, Visit};
        use tracing_subscriber::Registry;
        use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
        use tracing_subscriber::registry::LookupSpan;

        #[derive(Default)]
        struct FieldVisitor {
            fields: BTreeMap<String, String>,
        }

        impl FieldVisitor {
            fn record(&mut self, field: &Field, value: String) {
                self.fields.insert(field.name().to_string(), value);
            }
        }

        impl Visit for FieldVisitor {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                self.record(field, format!("{value:?}"));
            }

            fn record_str(&mut self, field: &Field, value: &str) {
                self.record(field, value.to_string());
            }

            fn record_u64(&mut self, field: &Field, value: u64) {
                self.record(field, value.to_string());
            }
        }

        #[derive(Default)]
        struct SpanFields {
            fields: BTreeMap<String, String>,
        }

        struct CaptureLayer {
            spans: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
        }

        impl CaptureLayer {
            fn new(spans: Arc<Mutex<Vec<BTreeMap<String, String>>>>) -> Self {
                Self { spans }
            }
        }

        impl<S> Layer<S> for CaptureLayer
        where
            S: Subscriber + for<'a> LookupSpan<'a>,
        {
            fn on_new_span(
                &self,
                attrs: &tracing::span::Attributes<'_>,
                id: &tracing::Id,
                ctx: Context<'_, S>,
            ) {
                let mut visitor = FieldVisitor::default();
                attrs.record(&mut visitor);
                if let Some(span) = ctx.span(id) {
                    span.extensions_mut().insert(SpanFields {
                        fields: visitor.fields,
                    });
                }
            }

            fn on_record(
                &self,
                id: &tracing::Id,
                values: &tracing::span::Record<'_>,
                ctx: Context<'_, S>,
            ) {
                if let Some(span) = ctx.span(id) {
                    let mut visitor = FieldVisitor::default();
                    values.record(&mut visitor);
                    let mut extensions = span.extensions_mut();
                    if extensions.get_mut::<SpanFields>().is_none() {
                        extensions.insert(SpanFields::default());
                    }
                    let fields = extensions.get_mut::<SpanFields>().expect("span fields");
                    fields.fields.extend(visitor.fields);
                }
            }

            fn on_close(&self, id: tracing::Id, ctx: Context<'_, S>) {
                let Some(span) = ctx.span(&id) else {
                    return;
                };
                if span.metadata().name() != "ipc_request" {
                    return;
                }
                let fields = span
                    .extensions()
                    .get::<SpanFields>()
                    .map(|fields| fields.fields.clone())
                    .unwrap_or_default();
                self.spans.lock().expect("span capture").push(fields);
            }
        }

        let spans = Arc::new(Mutex::new(Vec::new()));
        let layer = CaptureLayer::new(spans.clone());
        let subscriber = Registry::default().with(layer);

        tracing::dispatcher::with_default(&tracing::Dispatch::new(subscriber), || {
            let repo = PathBuf::from("/tmp/repo");
            let meta = MutationMeta {
                namespace: Some(NamespaceId::core()),
                client_request_id: Some(ClientRequestId::new(Uuid::from_bytes([7u8; 16]))),
                actor_id: Some(ActorId::new("actor@example.com").unwrap()),
                durability: None,
            };
            let request = Request::Create {
                ctx: crate::daemon::ipc::MutationCtx::new(repo.clone(), meta),
                payload: crate::daemon::ipc::CreatePayload {
                    id: None,
                    parent: None,
                    title: "title".to_string(),
                    bead_type: BeadType::Task,
                    priority: Priority::MEDIUM,
                    description: None,
                    design: None,
                    acceptance_criteria: None,
                    assignee: None,
                    external_ref: None,
                    estimated_minutes: None,
                    labels: Vec::new(),
                    dependencies: Vec::new(),
                },
            };
            let info = request.info();
            let span = request_span(&info);
            let _guard = span.enter();
        });

        let captured = spans.lock().expect("span capture");
        let fields = captured.last().cloned().unwrap_or_default();
        for key in [
            schema::REQUEST_TYPE,
            schema::REPO,
            schema::NAMESPACE,
            schema::ACTOR_ID,
            schema::CLIENT_REQUEST_ID,
        ] {
            assert!(
                fields.contains_key(key),
                "ipc_request span missing {key}: {fields:?}"
            );
        }
    }

    #[test]
    fn stream_event_decode_failure_is_wal_corrupt() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([7u8; 16]));
        let event_id = EventId::new(origin, namespace.clone(), Seq1::from_u64(1).unwrap());
        let bytes = EventBytes::<Opaque>::new(Bytes::from(vec![0x01]));
        let event = BroadcastEvent::new(event_id, Sha256([0u8; 32]), None, bytes);

        let response = stream_event_response(event, &Limits::default());
        let Response::Err { err } = response else {
            panic!("expected corruption error");
        };
        assert_eq!(err.code, ProtocolErrorCode::WalCorrupt.into());
        let details = err
            .details_as::<error_details::WalCorruptDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.namespace, namespace);
        assert!(!details.reason.is_empty());
    }

    #[test]
    fn read_gate_waiter_releases_on_apply() {
        let mut env = TestEnv::new();
        let loaded = env
            .daemon
            .ensure_repo_fresh(&env.repo_path, &env.git_tx)
            .unwrap();
        let origin = loaded.runtime().meta.replica_id;
        let namespace = NamespaceId::core();

        let mut required = Watermarks::<Applied>::new();
        let required_wm = watermark(1);
        required
            .observe_at_least(&namespace, &origin, required_wm.seq(), required_wm.head())
            .unwrap();

        let read = ReadConsistency {
            namespace: Some(namespace.clone()),
            require_min_seen: Some(required),
            wait_timeout_ms: Some(200),
        };
        let request = Request::Status {
            ctx: crate::daemon::ipc::ReadCtx::new(env.repo_path.clone(), read.clone()),
            payload: crate::daemon::ipc::EmptyPayload {},
        };
        let normalized = loaded.normalize_read_consistency(read).unwrap();
        drop(loaded);
        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        let started_at = Instant::now();
        let deadline = started_at + Duration::from_millis(normalized.wait_timeout_ms());
        let waiter = ReadGateWaiter {
            request,
            respond: respond_tx,
            repo: env.repo_path.clone(),
            read: normalized,
            span: tracing::Span::none(),
            started_at,
            deadline,
        };

        let mut waiters = vec![waiter];
        let mut sync_waiters = HashMap::new();
        let mut checkpoint_waiters = Vec::new();
        let mut durability_waiters = Vec::new();
        flush_read_gate_waiters(
            &mut env.daemon,
            &mut waiters,
            &env.git_tx,
            &mut sync_waiters,
            &mut checkpoint_waiters,
            &mut durability_waiters,
        );
        assert_eq!(waiters.len(), 1);
        assert!(respond_rx.try_recv().is_err());

        let applied_wm = watermark(1);
        let mut loaded = env
            .daemon
            .ensure_repo_fresh(&env.repo_path, &env.git_tx)
            .unwrap();
        loaded
            .runtime_mut()
            .watermarks_applied
            .observe_at_least(&namespace, &origin, applied_wm.seq(), applied_wm.head())
            .unwrap();
        drop(loaded);

        flush_read_gate_waiters(
            &mut env.daemon,
            &mut waiters,
            &env.git_tx,
            &mut sync_waiters,
            &mut checkpoint_waiters,
            &mut durability_waiters,
        );
        assert!(waiters.is_empty());

        let reply = respond_rx.recv().unwrap();
        let ServerReply::Response(response) = reply else {
            panic!("expected response");
        };
        assert!(matches!(response, Response::Ok { .. }));
    }

    #[test]
    fn read_gate_waiter_times_out() {
        let mut env = TestEnv::new();
        let loaded = env
            .daemon
            .ensure_repo_fresh(&env.repo_path, &env.git_tx)
            .unwrap();
        let origin = loaded.runtime().meta.replica_id;
        let namespace = NamespaceId::core();

        let mut required = Watermarks::<Applied>::new();
        let required_wm = watermark(1);
        required
            .observe_at_least(&namespace, &origin, required_wm.seq(), required_wm.head())
            .unwrap();

        let read = ReadConsistency {
            namespace: Some(namespace.clone()),
            require_min_seen: Some(required),
            wait_timeout_ms: Some(10),
        };
        let request = Request::Status {
            ctx: crate::daemon::ipc::ReadCtx::new(env.repo_path.clone(), read.clone()),
            payload: crate::daemon::ipc::EmptyPayload {},
        };
        let normalized = loaded.normalize_read_consistency(read).unwrap();
        drop(loaded);
        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        let started_at = Instant::now() - Duration::from_millis(20);
        let deadline = started_at;
        let waiter = ReadGateWaiter {
            request,
            respond: respond_tx,
            repo: env.repo_path.clone(),
            read: normalized,
            span: tracing::Span::none(),
            started_at,
            deadline,
        };

        let mut waiters = vec![waiter];
        let mut sync_waiters = HashMap::new();
        let mut checkpoint_waiters = Vec::new();
        let mut durability_waiters = Vec::new();
        flush_read_gate_waiters(
            &mut env.daemon,
            &mut waiters,
            &env.git_tx,
            &mut sync_waiters,
            &mut checkpoint_waiters,
            &mut durability_waiters,
        );
        assert!(waiters.is_empty());

        let reply = respond_rx.recv().unwrap();
        let ServerReply::Response(response) = reply else {
            panic!("expected response");
        };
        let Response::Err { err } = response else {
            panic!("expected timeout error");
        };
        assert_eq!(err.code, ProtocolErrorCode::RequireMinSeenTimeout.into());
    }

    fn replica(seed: u128) -> ReplicaId {
        ReplicaId::new(Uuid::from_u128(seed))
    }

    fn roster(entries: Vec<ReplicaEntry>) -> ReplicaRoster {
        ReplicaRoster { replicas: entries }
    }

    #[test]
    fn durability_waiter_releases_on_quorum() {
        let namespace = NamespaceId::core();
        let local = replica(1);
        let peer_a = replica(2);
        let peer_b = replica(3);
        let roster = roster(vec![
            ReplicaEntry {
                replica_id: local,
                name: "local".to_string(),
                role: ReplicaDurabilityRole::anchor(true),
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer_a,
                name: "peer-a".to_string(),
                role: ReplicaDurabilityRole::peer(true),
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer_b,
                name: "peer-b".to_string(),
                role: ReplicaDurabilityRole::peer(true),
                allowed_namespaces: None,
                expire_after_ms: None,
            },
        ]);

        let mut policies = BTreeMap::new();
        policies.insert(namespace.clone(), NamespacePolicy::core_default());

        let peer_acks = Arc::new(Mutex::new(PeerAckTable::new()));
        let coordinator =
            DurabilityCoordinator::new(local, policies, Some(roster), Arc::clone(&peer_acks));

        let mut durable: WatermarkState<Durable> = BTreeMap::new();
        durable.entry(namespace.clone()).or_default().insert(
            local,
            Watermark::new(Seq0::new(2), HeadStatus::Known([2u8; 32])).unwrap(),
        );
        peer_acks
            .lock()
            .unwrap()
            .update_peer(peer_a, &durable, None, 10)
            .unwrap();
        peer_acks
            .lock()
            .unwrap()
            .update_peer(peer_b, &durable, None, 12)
            .unwrap();

        let store = StoreIdentity::new(StoreId::new(Uuid::from_u128(10)), StoreEpoch::ZERO);
        let receipt = DurabilityReceipt::local_fsync_defaults(
            store,
            TxnId::new(Uuid::from_u128(11)),
            Vec::new(),
            123,
        );
        let bead_id = BeadId::parse("bd-abc").unwrap();
        let response = OpResponse::new(OpResult::Updated { id: bead_id }, receipt);
        let wait = DurabilityWait {
            coordinator,
            namespace: namespace.clone(),
            origin: local,
            seq: Seq1::from_u64(2).unwrap(),
            requested: DurabilityClass::ReplicatedFsync {
                k: NonZeroU32::new(2).unwrap(),
            },
            wait_timeout: Duration::from_millis(50),
            response,
        };

        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        let started_at = Instant::now();
        let deadline = started_at + Duration::from_millis(50);
        let mut waiters = vec![DurabilityWaiter {
            respond: respond_tx,
            wait,
            span: tracing::Span::none(),
            started_at,
            deadline,
        }];

        flush_durability_waiters(&mut waiters);
        assert!(waiters.is_empty());

        let reply = respond_rx.recv().unwrap();
        let ServerReply::Response(response) = reply else {
            panic!("expected response");
        };
        let Response::Ok { ok } = response else {
            panic!("expected ok response");
        };
        let ResponsePayload::Op(op) = ok else {
            panic!("expected op response");
        };

        assert!(op.receipt.outcome().is_achieved());
        assert_eq!(
            op.receipt.outcome().requested(),
            DurabilityClass::ReplicatedFsync {
                k: NonZeroU32::new(2).unwrap()
            }
        );
        assert_eq!(
            op.receipt.outcome().achieved(),
            Some(DurabilityClass::ReplicatedFsync {
                k: NonZeroU32::new(2).unwrap()
            })
        );

        let proof = op
            .receipt
            .durability_proof()
            .replicated
            .as_ref()
            .expect("replicated proof");
        assert_eq!(proof.k.get(), 2);
        assert_eq!(proof.acked_by.len(), 2);
        assert!(proof.acked_by.contains(&peer_a));
        assert!(proof.acked_by.contains(&peer_b));
    }

    #[test]
    fn durability_waiter_times_out() {
        let namespace = NamespaceId::core();
        let local = replica(5);
        let peer = replica(6);
        let roster = roster(vec![
            ReplicaEntry {
                replica_id: local,
                name: "local".to_string(),
                role: ReplicaDurabilityRole::anchor(true),
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer,
                name: "peer".to_string(),
                role: ReplicaDurabilityRole::peer(true),
                allowed_namespaces: None,
                expire_after_ms: None,
            },
        ]);

        let mut policies = BTreeMap::new();
        policies.insert(namespace.clone(), NamespacePolicy::core_default());

        let peer_acks = Arc::new(Mutex::new(PeerAckTable::new()));
        let coordinator =
            DurabilityCoordinator::new(local, policies, Some(roster), Arc::clone(&peer_acks));

        let store = StoreIdentity::new(StoreId::new(Uuid::from_u128(12)), StoreEpoch::ZERO);
        let receipt = DurabilityReceipt::local_fsync_defaults(
            store,
            TxnId::new(Uuid::from_u128(13)),
            Vec::new(),
            123,
        );
        let bead_id = BeadId::parse("bd-def").unwrap();
        let response = OpResponse::new(OpResult::Updated { id: bead_id }, receipt);
        let wait = DurabilityWait {
            coordinator,
            namespace: namespace.clone(),
            origin: local,
            seq: Seq1::from_u64(1).unwrap(),
            requested: DurabilityClass::ReplicatedFsync {
                k: NonZeroU32::new(1).unwrap(),
            },
            wait_timeout: Duration::from_millis(10),
            response,
        };

        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        let started_at = Instant::now() - Duration::from_millis(20);
        let deadline = started_at;
        let mut waiters = vec![DurabilityWaiter {
            respond: respond_tx,
            wait,
            span: tracing::Span::none(),
            started_at,
            deadline,
        }];

        flush_durability_waiters(&mut waiters);
        assert!(waiters.is_empty());

        let reply = respond_rx.recv().unwrap();
        let ServerReply::Response(response) = reply else {
            panic!("expected response");
        };
        let Response::Err { err } = response else {
            panic!("expected error");
        };
        assert_eq!(err.code, ProtocolErrorCode::DurabilityTimeout.into());
        let receipt = err
            .receipt_as::<DurabilityReceipt>()
            .unwrap()
            .expect("receipt");
        assert!(receipt.outcome().is_pending());
    }
}
