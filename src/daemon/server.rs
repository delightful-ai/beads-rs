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

use super::broadcast::{
    BroadcastError, BroadcastEvent, DropReason, EventSubscription, SubscriberLimits,
};
use super::core::{Daemon, HandleOutcome, NormalizedReadConsistency, ReadGateStatus};
use super::durability_coordinator::{DurabilityCoordinator, ReplicatedPoll};
use super::executor::DurabilityWait;
use super::git_worker::{GitOp, GitResult};
use super::ipc::{
    ReadConsistency, Request, Response, ResponsePayload, decode_request_with_limits,
    encode_response, send_response,
};
use super::ops::OpError;
use super::remote::RemoteUrl;
use super::repl::{WalRangeError, WalRangeReader};
use crate::core::error::details as error_details;
use crate::core::{
    Applied, DurabilityClass, ErrorCode, ErrorPayload, EventFrameV1, EventId, Limits, NamespaceId,
    ReplicaId, Seq0, Sha256, Watermark, Watermarks, decode_event_body,
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

struct ReadGateRequest {
    repo: PathBuf,
    read: ReadConsistency,
}

struct ReadGateWaiter {
    request: Request,
    respond: Sender<ServerReply>,
    repo: PathBuf,
    read: NormalizedReadConsistency,
    started_at: Instant,
    deadline: Instant,
}

struct DurabilityWaiter {
    respond: Sender<ServerReply>,
    wait: DurabilityWait,
    started_at: Instant,
    deadline: Instant,
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
                        if let Some(read_gate) = read_gate_request(&request) {
                            let loaded = match daemon.ensure_repo_fresh(&read_gate.repo, &git_tx) {
                                Ok(remote) => remote,
                                Err(err) => {
                                    let _ = respond.send(ServerReply::Response(Response::err(err)));
                                    continue;
                                }
                            };
                            let read = match daemon.normalize_read_consistency(&loaded, read_gate.read) {
                                Ok(read) => read,
                                Err(err) => {
                                    let _ = respond.send(ServerReply::Response(Response::err(err)));
                                    continue;
                                }
                            };

                            if read.require_min_seen().is_some() {
                                match daemon.read_gate_status(&loaded, &read) {
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
                                            let _ = respond.send(ServerReply::Response(Response::err(err)));
                                            continue;
                                        }

                                        let started_at = Instant::now();
                                        let timeout = Duration::from_millis(read.wait_timeout_ms());
                                        let deadline =
                                            started_at.checked_add(timeout).unwrap_or(started_at);
                                        read_gate_waiters.push(ReadGateWaiter {
                                            request,
                                            respond,
                                            repo: read_gate.repo,
                                            read,
                                            started_at,
                                            deadline,
                                        });
                                        continue;
                                    }
                                    Err(err) => {
                                        let _ = respond.send(ServerReply::Response(Response::err(err)));
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
                            &mut durability_waiters,
                        );

                        if matches!(outcome, RequestOutcome::Shutdown) {
                            daemon.begin_shutdown();
                            daemon.shutdown_replication();
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
                        flush_sync_waiters(&daemon, &mut sync_waiters);
                        flush_read_gate_waiters(
                            &mut daemon,
                            &mut read_gate_waiters,
                            &git_tx,
                            &mut sync_waiters,
                            &mut durability_waiters,
                        );
                        flush_durability_waiters(&mut durability_waiters);
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
                daemon.fire_due_checkpoints(&git_tx);
                daemon.fire_due_wal_checkpoints();
                daemon.fire_due_lock_heartbeats();
                flush_sync_waiters(&daemon, &mut sync_waiters);
                flush_read_gate_waiters(
                    &mut daemon,
                    &mut read_gate_waiters,
                    &git_tx,
                    &mut sync_waiters,
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
                    flush_sync_waiters(&daemon, &mut sync_waiters);
                    flush_read_gate_waiters(
                        &mut daemon,
                        &mut read_gate_waiters,
                        &git_tx,
                        &mut sync_waiters,
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
                flush_sync_waiters(&daemon, &mut sync_waiters);
                flush_read_gate_waiters(
                    &mut daemon,
                    &mut read_gate_waiters,
                    &git_tx,
                    &mut sync_waiters,
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
    durability_waiters: &mut Vec<DurabilityWaiter>,
) -> RequestOutcome {
    // Sync barrier: wait until repo is clean.
    if let Request::SyncWait { repo } = request {
        match daemon.ensure_loaded_and_maybe_start_sync(&repo, git_tx) {
            Ok(loaded) => {
                let repo_state = match daemon.repo_state(&loaded) {
                    Ok(repo_state) => repo_state,
                    Err(e) => {
                        let _ = respond.send(ServerReply::Response(Response::err(e)));
                        return RequestOutcome::Continue;
                    }
                };
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
                let _ = respond.send(ServerReply::Response(Response::err(e)));
            }
        }
        return RequestOutcome::Continue;
    }

    if let Request::Subscribe { repo, read } = request {
        let loaded = match daemon.ensure_repo_fresh(&repo, git_tx) {
            Ok(remote) => remote,
            Err(e) => {
                let _ = respond.send(ServerReply::Response(Response::err(e)));
                return RequestOutcome::Continue;
            }
        };
        let read = match daemon.normalize_read_consistency(&loaded, read) {
            Ok(read) => read,
            Err(e) => {
                let _ = respond.send(ServerReply::Response(Response::err(e)));
                return RequestOutcome::Continue;
            }
        };
        if let Err(err) = daemon.check_read_gate(&loaded, &read) {
            let _ = respond.send(ServerReply::Response(Response::err(err)));
            return RequestOutcome::Continue;
        }
        let store_runtime = match daemon.store_runtime(&loaded) {
            Ok(runtime) => runtime,
            Err(e) => {
                let _ = respond.send(ServerReply::Response(Response::err(e)));
                return RequestOutcome::Continue;
            }
        };
        let subscription = match store_runtime
            .broadcaster
            .subscribe(subscriber_limits(daemon.limits()))
        {
            Ok(subscription) => subscription,
            Err(err) => {
                let _ = respond.send(ServerReply::Response(Response::err(broadcast_error_to_op(
                    err,
                ))));
                return RequestOutcome::Continue;
            }
        };
        let hot_cache = match store_runtime.broadcaster.hot_cache() {
            Ok(cache) => cache,
            Err(err) => {
                let _ = respond.send(ServerReply::Response(Response::err(broadcast_error_to_op(
                    err,
                ))));
                return RequestOutcome::Continue;
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
                let _ = respond.send(ServerReply::Response(Response::err(*err)));
                return RequestOutcome::Continue;
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
            durability_waiters.push(DurabilityWaiter {
                respond,
                wait,
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

fn read_gate_request(request: &Request) -> Option<ReadGateRequest> {
    match request {
        Request::Show { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::ShowMultiple { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::List { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::Ready { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::DepTree { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::Deps { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::Notes { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::Blocked { repo, read } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::Stale { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::Count { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::Deleted { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::EpicStatus { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::Status { repo, read } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::AdminStatus { repo, read } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::AdminMetrics { repo, read } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::AdminDoctor { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::AdminScrub { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::AdminFingerprint { repo, read, .. } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::Validate { repo, read } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        Request::Subscribe { repo, read } => Some(ReadGateRequest {
            repo: repo.clone(),
            read: read.clone(),
        }),
        _ => None,
    }
}

fn flush_read_gate_waiters(
    daemon: &mut Daemon,
    waiters: &mut Vec<ReadGateWaiter>,
    git_tx: &Sender<GitOp>,
    sync_waiters: &mut HashMap<RemoteUrl, Vec<Sender<ServerReply>>>,
    durability_waiters: &mut Vec<DurabilityWaiter>,
) {
    if waiters.is_empty() {
        return;
    }

    let now = Instant::now();
    let mut remaining = Vec::new();
    for waiter in waiters.drain(..) {
        let loaded = match daemon.ensure_repo_fresh(&waiter.repo, git_tx) {
            Ok(remote) => remote,
            Err(err) => {
                let _ = waiter
                    .respond
                    .send(ServerReply::Response(Response::err(err)));
                continue;
            }
        };

        match daemon.read_gate_status(&loaded, &waiter.read) {
            Ok(ReadGateStatus::Satisfied) => {
                let _ = process_request_message(
                    daemon,
                    waiter.request,
                    waiter.respond,
                    git_tx,
                    sync_waiters,
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
                        .send(ServerReply::Response(Response::err(err)));
                    continue;
                }
                remaining.push(waiter);
            }
            Err(err) => {
                let _ = waiter
                    .respond
                    .send(ServerReply::Response(Response::err(err)));
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
                    );
                    let err = OpError::DurabilityTimeout {
                        requested,
                        waited_ms,
                        pending_replica_ids: Some(pending),
                        receipt: Box::new(pending_receipt),
                    };
                    let _ = waiter
                        .respond
                        .send(ServerReply::Response(Response::err(err)));
                    continue;
                }
                remaining.push(waiter);
            }
            Err(err) => {
                let _ = waiter
                    .respond
                    .send(ServerReply::Response(Response::err(err)));
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
        from_seq_excl: Seq0,
        max_bytes: usize,
    ) -> Result<Vec<EventFrameV1>, WalRangeError>;
}

impl WalRangeRead for WalRangeReader {
    fn read_range(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        from_seq_excl: Seq0,
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

        let mut from_seq_excl = Seq0::new(required_seq);
        while from_seq_excl.get() < current_seq {
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
            from_seq_excl = Seq0::new(last.eid.origin_seq.get());
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
    use std::collections::BTreeMap;
    use std::num::NonZeroU32;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::core::replica_roster::ReplicaEntry;
    use crate::core::{
        ActorId, Applied, BeadId, DurabilityClass, DurabilityOutcome, DurabilityReceipt,
        EventBytes, EventFrameV1, EventId, HeadStatus, NamespacePolicy, Opaque, ReplicaRole,
        ReplicaRoster, Seq0, Seq1, Sha256, StoreEpoch, StoreId, StoreIdentity, TxnId, Watermarks,
    };
    use crate::daemon::core::insert_store_for_tests;
    use crate::daemon::ipc::OpResponse;
    use crate::daemon::ops::OpResult;
    use crate::daemon::repl::PeerAckTable;
    use crate::daemon::repl::proto::WatermarkMap;

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

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct ReadCall {
        namespace: NamespaceId,
        origin: ReplicaId,
        from_seq_excl: Seq0,
        max_bytes: usize,
    }

    #[derive(Default)]
    struct FakeWalReader {
        responses: RefCell<HashMap<(ReplicaId, Seq0), Result<Vec<EventFrameV1>, WalRangeError>>>,
        calls: RefCell<Vec<ReadCall>>,
    }

    impl FakeWalReader {
        fn with_response(
            self,
            origin: ReplicaId,
            from_seq_excl: Seq0,
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
            from_seq_excl: Seq0,
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
            .with_response(origin, Seq0::new(2), Ok(vec![frame3.clone()]))
            .with_response(origin, Seq0::new(3), Ok(vec![frame4.clone()]));
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
                    from_seq_excl: Seq0::new(2),
                    max_bytes: limits.max_event_batch_bytes,
                },
                ReadCall {
                    namespace: namespace.clone(),
                    origin,
                    from_seq_excl: Seq0::new(3),
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
            Seq0::new(1),
            Err(WalRangeError::MissingRange {
                namespace: namespace.clone(),
                origin,
                from_seq_excl: Seq0::new(1),
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
            Seq0::new(1),
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
        let bytes = EventBytes::<Opaque>::new(Bytes::from(vec![0x01]));
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

    #[test]
    fn read_gate_waiter_releases_on_apply() {
        let mut env = TestEnv::new();
        let loaded = env
            .daemon
            .ensure_repo_fresh(&env.repo_path, &env.git_tx)
            .unwrap();
        let origin = env.daemon.store_runtime(&loaded).unwrap().meta.replica_id;
        let namespace = NamespaceId::core();

        let mut required = Watermarks::<Applied>::new();
        required
            .observe_at_least(&namespace, &origin, Seq0::new(1), HeadStatus::Unknown)
            .unwrap();

        let read = ReadConsistency {
            namespace: Some("core".to_string()),
            require_min_seen: Some(required),
            wait_timeout_ms: Some(200),
        };
        let request = Request::Status {
            repo: env.repo_path.clone(),
            read: read.clone(),
        };
        let normalized = env
            .daemon
            .normalize_read_consistency(&loaded, read)
            .unwrap();
        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        let started_at = Instant::now();
        let deadline = started_at + Duration::from_millis(normalized.wait_timeout_ms());
        let waiter = ReadGateWaiter {
            request,
            respond: respond_tx,
            repo: env.repo_path.clone(),
            read: normalized,
            started_at,
            deadline,
        };

        let mut waiters = vec![waiter];
        let mut sync_waiters = HashMap::new();
        let mut durability_waiters = Vec::new();
        flush_read_gate_waiters(
            &mut env.daemon,
            &mut waiters,
            &env.git_tx,
            &mut sync_waiters,
            &mut durability_waiters,
        );
        assert_eq!(waiters.len(), 1);
        assert!(respond_rx.try_recv().is_err());

        env.daemon
            .store_runtime_mut(&loaded)
            .unwrap()
            .watermarks_applied
            .observe_at_least(&namespace, &origin, Seq0::new(1), HeadStatus::Unknown)
            .unwrap();

        flush_read_gate_waiters(
            &mut env.daemon,
            &mut waiters,
            &env.git_tx,
            &mut sync_waiters,
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
        let origin = env.daemon.store_runtime(&loaded).unwrap().meta.replica_id;
        let namespace = NamespaceId::core();

        let mut required = Watermarks::<Applied>::new();
        required
            .observe_at_least(&namespace, &origin, Seq0::new(1), HeadStatus::Unknown)
            .unwrap();

        let read = ReadConsistency {
            namespace: Some("core".to_string()),
            require_min_seen: Some(required),
            wait_timeout_ms: Some(10),
        };
        let request = Request::Status {
            repo: env.repo_path.clone(),
            read: read.clone(),
        };
        let normalized = env
            .daemon
            .normalize_read_consistency(&loaded, read)
            .unwrap();
        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        let started_at = Instant::now() - Duration::from_millis(20);
        let deadline = started_at;
        let waiter = ReadGateWaiter {
            request,
            respond: respond_tx,
            repo: env.repo_path.clone(),
            read: normalized,
            started_at,
            deadline,
        };

        let mut waiters = vec![waiter];
        let mut sync_waiters = HashMap::new();
        let mut durability_waiters = Vec::new();
        flush_read_gate_waiters(
            &mut env.daemon,
            &mut waiters,
            &env.git_tx,
            &mut sync_waiters,
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
        assert_eq!(err.code, ErrorCode::RequireMinSeenTimeout);
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
                role: ReplicaRole::Anchor,
                durability_eligible: true,
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer_a,
                name: "peer-a".to_string(),
                role: ReplicaRole::Peer,
                durability_eligible: true,
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer_b,
                name: "peer-b".to_string(),
                role: ReplicaRole::Peer,
                durability_eligible: true,
                allowed_namespaces: None,
                expire_after_ms: None,
            },
        ]);

        let mut policies = BTreeMap::new();
        policies.insert(namespace.clone(), NamespacePolicy::core_default());

        let peer_acks = Arc::new(Mutex::new(PeerAckTable::new()));
        let coordinator =
            DurabilityCoordinator::new(local, policies, Some(roster), Arc::clone(&peer_acks));

        let mut durable = WatermarkMap::new();
        durable
            .entry(namespace.clone())
            .or_default()
            .insert(local, Seq0::new(2));
        peer_acks
            .lock()
            .unwrap()
            .update_peer(peer_a, &durable, None, None, None, 10)
            .unwrap();
        peer_acks
            .lock()
            .unwrap()
            .update_peer(peer_b, &durable, None, None, None, 12)
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

        match op.receipt.outcome {
            DurabilityOutcome::Achieved {
                requested,
                achieved,
            } => {
                assert_eq!(
                    requested,
                    DurabilityClass::ReplicatedFsync {
                        k: NonZeroU32::new(2).unwrap()
                    }
                );
                assert_eq!(
                    achieved,
                    DurabilityClass::ReplicatedFsync {
                        k: NonZeroU32::new(2).unwrap()
                    }
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let proof = op
            .receipt
            .durability_proof
            .replicated
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
                role: ReplicaRole::Anchor,
                durability_eligible: true,
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer,
                name: "peer".to_string(),
                role: ReplicaRole::Peer,
                durability_eligible: true,
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
        assert_eq!(err.code, ErrorCode::DurabilityTimeout);
        let receipt = err
            .receipt_as::<DurabilityReceipt>()
            .unwrap()
            .expect("receipt");
        assert!(matches!(receipt.outcome, DurabilityOutcome::Pending { .. }));
    }
}
