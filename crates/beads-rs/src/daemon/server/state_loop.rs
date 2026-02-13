use std::collections::HashMap;
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender};

use super::super::core::{Daemon, ReadGateStatus};
use super::super::git_worker::{GitOp, GitResult};
use super::super::ipc::{Response, ResponseExt};
use super::super::ops::OpError;
use super::dispatch::process_request_message;
use super::engine::StateLoopEngine;
use super::spans::{record_ipc_request_metric, request_span};
use super::waiters::{
    CheckpointWaiter, DurabilityWaiter, ReadGateWaiter, RequestOutcome, RequestWaiters,
    flush_checkpoint_waiters, flush_durability_waiters, flush_read_gate_waiters,
    flush_sync_waiters,
};
use super::{RequestMessage, ServerReply};
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
        let tick = StateLoopEngine::next_tick(&mut daemon, &read_gate_waiters, &durability_waiters);

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

                        run_housekeeping(
                            &mut daemon,
                            &git_tx,
                            &mut sync_waiters,
                            &mut checkpoint_waiters,
                            &mut read_gate_waiters,
                            &mut durability_waiters,
                            false,
                        );
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
                run_housekeeping(
                    &mut daemon,
                    &git_tx,
                    &mut sync_waiters,
                    &mut checkpoint_waiters,
                    &mut read_gate_waiters,
                    &mut durability_waiters,
                    true,
                );
            }

            // Replication ingest request
            recv(repl_rx) -> msg => {
                if let Ok(request) = msg {
                    daemon.handle_repl_ingest(request);
                    run_housekeeping(
                        &mut daemon,
                        &git_tx,
                        &mut sync_waiters,
                        &mut checkpoint_waiters,
                        &mut read_gate_waiters,
                        &mut durability_waiters,
                        false,
                    );
                }
            }

            // Git operation completed
            recv(git_result_rx) -> msg => {
                if let Ok(result) = msg {
                    StateLoopEngine::apply_git_result(&mut daemon, result);
                }
                run_housekeeping(
                    &mut daemon,
                    &git_tx,
                    &mut sync_waiters,
                    &mut checkpoint_waiters,
                    &mut read_gate_waiters,
                    &mut durability_waiters,
                    false,
                );
            }
        }
    }
}

fn run_housekeeping(
    daemon: &mut Daemon,
    git_tx: &Sender<GitOp>,
    sync_waiters: &mut HashMap<RemoteUrl, Vec<Sender<ServerReply>>>,
    checkpoint_waiters: &mut Vec<CheckpointWaiter>,
    read_gate_waiters: &mut Vec<ReadGateWaiter>,
    durability_waiters: &mut Vec<DurabilityWaiter>,
    include_wal_checkpoint: bool,
) {
    StateLoopEngine::fire_due(daemon, git_tx, include_wal_checkpoint);
    flush_checkpoint_waiters(daemon, checkpoint_waiters);
    flush_sync_waiters(daemon, sync_waiters);
    flush_read_gate_waiters(
        daemon,
        read_gate_waiters,
        git_tx,
        sync_waiters,
        checkpoint_waiters,
        durability_waiters,
    );
    flush_durability_waiters(durability_waiters);
}
