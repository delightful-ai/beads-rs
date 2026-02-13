use std::collections::HashMap;
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender};

use super::super::QueryResult;
use super::super::core::{Daemon, HandleOutcome, ReadGateStatus};
use super::super::durability_coordinator::{DurabilityCoordinator, ReplicatedPoll};
use super::super::git_worker::{GitOp, GitResult};
use super::super::ipc::{AdminOp, Request, Response, ResponseExt, ResponsePayload};
use super::super::ops::OpError;
use super::super::subscription::prepare_subscription;
use super::spans::{record_ipc_request_metric, request_span};
use super::waiters::{
    CheckpointWaiter, DurabilityWaiter, ReadGateWaiter, RequestOutcome, RequestWaiters,
};
use super::{RequestMessage, ServerReply};
use crate::api::{AdminCheckpointGroup, AdminCheckpointOutput};
use crate::core::{DurabilityClass, NamespaceId, StoreId};
use crate::daemon::metrics;
use beads_daemon::remote::RemoteUrl;

/// Run the state thread loop.
///
/// This is THE serialization point - all state mutations go through here.
/// Uses crossbeam::select! for fair multi-channel receive.
pub(in crate::daemon) fn run_state_loop(
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
                        let (span, read_gate, request_type) = {
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
                            (span, read_gate, info.op)
                        };
                        let request_started_at = Instant::now();
                        let _guard = span.enter();

                        if let Some((Some(repo), read)) = read_gate {
                            let git_sync_policy = daemon.git_sync_policy();
                            let actor = daemon.actor().clone();
                            let mut loaded = match daemon.ensure_repo_fresh(&repo, &git_tx) {
                                Ok(loaded) => loaded,
                                Err(err) => {
                                    let _ = respond.send(ServerReply::Response(Response::err_from(err)));
                                    record_ipc_request_metric(
                                        request_type,
                                        request_started_at,
                                        "err",
                                    );
                                    continue;
                                }
                            };
                            let read = match loaded.read_scope(read) {
                                Ok(read) => read,
                                Err(err) => {
                                    let _ = respond.send(ServerReply::Response(Response::err_from(err)));
                                    record_ipc_request_metric(
                                        request_type,
                                        request_started_at,
                                        "err",
                                    );
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
                                            record_ipc_request_metric(
                                                request_type,
                                                request_started_at,
                                                "err",
                                            );
                                            continue;
                                        }

                                        // If this request is waiting on remote-applied watermarks,
                                        // start sync immediately when local state is dirty instead of
                                        // waiting for the debounce window.
                                        loaded.maybe_start_sync(git_sync_policy, actor, &git_tx);

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
                                        record_ipc_request_metric(
                                            request_type,
                                            request_started_at,
                                            "wait",
                                        );
                                        continue;
                                    }
                                    Err(err) => {
                                        let _ = respond.send(ServerReply::Response(Response::err_from(err)));
                                        record_ipc_request_metric(
                                            request_type,
                                            request_started_at,
                                            "err",
                                        );
                                        continue;
                                    }
                                }
                            }
                        }

                        let mut request_waiters = RequestWaiters {
                            sync_waiters: &mut sync_waiters,
                            checkpoint_waiters: &mut checkpoint_waiters,
                            durability_waiters: &mut durability_waiters,
                        };
                        let outcome = process_request_message(
                            &mut daemon,
                            request,
                            respond,
                            &git_tx,
                            &mut request_waiters,
                            request_type,
                            request_started_at,
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

pub(super) fn process_request_message(
    daemon: &mut Daemon,
    request: Request,
    respond: Sender<ServerReply>,
    git_tx: &Sender<GitOp>,
    waiters: &mut RequestWaiters<'_>,
    request_type: &'static str,
    request_started_at: Instant,
) -> RequestOutcome {
    // Sync barrier: wait until repo is clean.
    if let Request::SyncWait { ctx, .. } = request {
        match daemon.ensure_loaded_and_maybe_start_sync(&ctx.path, git_tx) {
            Ok(loaded) => {
                let repo_state = loaded.lane();
                let clean = !repo_state.dirty && !repo_state.sync_in_progress;

                if clean {
                    let _ = respond.send(ServerReply::Response(Response::ok(
                        ResponsePayload::synced(),
                    )));
                    record_ipc_request_metric(request_type, request_started_at, "ok");
                } else {
                    waiters
                        .sync_waiters
                        .entry(loaded.remote().clone())
                        .or_default()
                        .push(respond);
                    record_ipc_request_metric(request_type, request_started_at, "wait");
                }
            }
            Err(e) => {
                let _ = respond.send(ServerReply::Response(Response::err_from(e)));
                record_ipc_request_metric(request_type, request_started_at, "err");
            }
        }
        return RequestOutcome::Continue;
    }

    if let Request::Admin(AdminOp::CheckpointWait { ctx, payload }) = request {
        let proof = match daemon.ensure_repo_loaded_strict(&ctx.path, git_tx) {
            Ok(proof) => proof,
            Err(err) => {
                let _ = respond.send(ServerReply::Response(Response::err_from(err)));
                record_ipc_request_metric(request_type, request_started_at, "err");
                return RequestOutcome::Continue;
            }
        };
        let namespace = match proof.normalize_namespace(payload.namespace) {
            Ok(namespace) => namespace,
            Err(err) => {
                let _ = respond.send(ServerReply::Response(Response::err_from(err)));
                record_ipc_request_metric(request_type, request_started_at, "err");
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
            record_ipc_request_metric(request_type, request_started_at, "err");
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
                record_ipc_request_metric(request_type, request_started_at, "ok");
            }
            Ok(None) => {
                waiters.checkpoint_waiters.push(CheckpointWaiter {
                    respond,
                    store_id,
                    namespace,
                    min_checkpoint_wall_ms,
                    groups,
                });
                record_ipc_request_metric(request_type, request_started_at, "wait");
            }
            Err(err) => {
                let _ = respond.send(ServerReply::Response(Response::err_from(err)));
                record_ipc_request_metric(request_type, request_started_at, "err");
            }
        }

        return RequestOutcome::Continue;
    }

    if let Request::Subscribe { ctx, .. } = request {
        match prepare_subscription(daemon, &ctx.repo.path, ctx.read, git_tx) {
            Ok(reply) => {
                let _ = respond.send(ServerReply::Subscribe(reply));
                record_ipc_request_metric(request_type, request_started_at, "ok");
            }
            Err(err) => {
                let _ = respond.send(ServerReply::Response(Response::err_from(*err)));
                record_ipc_request_metric(request_type, request_started_at, "err");
            }
        }
        return RequestOutcome::Continue;
    }

    let is_shutdown = matches!(request, Request::Shutdown);

    let outcome = daemon.handle_request(request, git_tx);
    match outcome {
        HandleOutcome::Response(response) => {
            let metric_outcome = if matches!(response, Response::Err { .. }) {
                "err"
            } else {
                "ok"
            };
            let _ = respond.send(ServerReply::Response(response));
            record_ipc_request_metric(request_type, request_started_at, metric_outcome);
        }
        HandleOutcome::DurabilityWait(wait) => {
            let started_at = Instant::now();
            let deadline = started_at
                .checked_add(wait.wait_timeout)
                .unwrap_or(started_at);
            let span = tracing::Span::current();
            waiters.durability_waiters.push(DurabilityWaiter {
                respond,
                wait,
                span,
                started_at,
                deadline,
            });
            record_ipc_request_metric(request_type, request_started_at, "wait");
        }
    }

    if is_shutdown {
        RequestOutcome::Shutdown
    } else {
        RequestOutcome::Continue
    }
}

pub(super) fn flush_read_gate_waiters(
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
        let request_type = waiter.request.info().op;
        let span = waiter.span.clone();
        let _guard = span.enter();
        let loaded = match daemon.ensure_repo_fresh(&waiter.repo, git_tx) {
            Ok(loaded) => loaded,
            Err(err) => {
                metrics::ipc_read_gate_wait_completed(
                    request_type,
                    "err",
                    now.duration_since(waiter.started_at),
                );
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
                let request_started_at = waiter.started_at;
                metrics::ipc_read_gate_wait_completed(
                    request_type,
                    "satisfied",
                    now.duration_since(waiter.started_at),
                );
                let mut request_waiters = RequestWaiters {
                    sync_waiters,
                    checkpoint_waiters,
                    durability_waiters,
                };
                let _ = process_request_message(
                    daemon,
                    waiter.request,
                    waiter.respond,
                    git_tx,
                    &mut request_waiters,
                    request_type,
                    request_started_at,
                );
            }
            Ok(ReadGateStatus::Unsatisfied {
                required,
                current_applied,
            }) => {
                if now >= waiter.deadline {
                    let waited_ms = (now.duration_since(waiter.started_at).as_millis())
                        .min(u64::MAX as u128) as u64;
                    metrics::ipc_read_gate_wait_completed(
                        request_type,
                        "timeout",
                        now.duration_since(waiter.started_at),
                    );
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
                metrics::ipc_read_gate_wait_completed(
                    request_type,
                    "err",
                    now.duration_since(waiter.started_at),
                );
                let _ = waiter
                    .respond
                    .send(ServerReply::Response(Response::err_from(err)));
            }
        }
    }

    *waiters = remaining;
}

pub(super) fn flush_durability_waiters(waiters: &mut Vec<DurabilityWaiter>) {
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
                    ResponsePayload::synced(),
                )));
            }
        }
    }
}
