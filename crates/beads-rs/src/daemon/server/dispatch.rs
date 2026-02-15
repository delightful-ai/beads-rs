use std::time::Instant;

use crossbeam::channel::Sender;

use super::super::QueryResult;
use super::super::core::{Daemon, HandleOutcome};
use super::super::git_worker::GitOp;
use super::super::ipc::{AdminOp, Request, Response, ResponseExt, ResponsePayload};
use super::super::ops::OpError;
use super::super::subscription::prepare_subscription;
use super::ServerReply;
use super::spans::record_ipc_request_metric;
use super::waiters::{
    CheckpointWaiter, DurabilityWaiter, RequestOutcome, RequestWaiters, checkpoint_wait_ready,
};

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
