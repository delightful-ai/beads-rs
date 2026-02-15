use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use crossbeam::channel::Sender;
use tracing::Span;

use super::super::QueryResult;
use super::super::core::{Daemon, ReadGateStatus, ReadScope};
use super::super::durability_coordinator::{DurabilityCoordinator, ReplicatedPoll};
use super::super::executor::DurabilityWait;
use super::super::git_worker::GitOp;
use super::super::ipc::{Request, Response, ResponseExt, ResponsePayload};
use super::super::ops::OpError;
use super::ServerReply;
use super::dispatch::process_request_message;
use crate::api::{AdminCheckpointGroup, AdminCheckpointOutput};
use crate::core::{DurabilityClass, NamespaceId, StoreId};
use crate::daemon::metrics;
use beads_daemon::remote::RemoteUrl;

pub(super) struct ReadGateWaiter {
    pub(super) request: Request,
    pub(super) respond: Sender<ServerReply>,
    pub(super) repo: PathBuf,
    pub(super) read: ReadScope,
    pub(super) span: Span,
    pub(super) started_at: Instant,
    pub(super) deadline: Instant,
}

pub(super) struct DurabilityWaiter {
    pub(super) respond: Sender<ServerReply>,
    pub(super) wait: DurabilityWait,
    pub(super) span: Span,
    pub(super) started_at: Instant,
    pub(super) deadline: Instant,
}

pub(super) struct CheckpointWaiter {
    pub(super) respond: Sender<ServerReply>,
    pub(super) store_id: StoreId,
    pub(super) namespace: NamespaceId,
    pub(super) min_checkpoint_wall_ms: u64,
    pub(super) groups: Vec<String>,
}

pub(super) enum RequestOutcome {
    Continue,
    Shutdown,
}

pub(super) struct RequestWaiters<'a> {
    pub(super) sync_waiters: &'a mut HashMap<RemoteUrl, Vec<Sender<ServerReply>>>,
    pub(super) checkpoint_waiters: &'a mut Vec<CheckpointWaiter>,
    pub(super) durability_waiters: &'a mut Vec<DurabilityWaiter>,
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

pub(super) fn checkpoint_wait_ready(
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

pub(super) fn flush_checkpoint_waiters(daemon: &Daemon, waiters: &mut Vec<CheckpointWaiter>) {
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

pub(super) fn flush_sync_waiters(
    daemon: &Daemon,
    waiters: &mut HashMap<RemoteUrl, Vec<Sender<ServerReply>>>,
) {
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
