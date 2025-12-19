//! Server thread loops.
//!
//! Three threads:
//! - Socket acceptor (main thread) - accepts connections, spawns handlers
//! - State thread - owns Daemon, processes requests sequentially
//! - Git thread - owns GitWorker, handles all git IO

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::time::Instant;

use crossbeam::channel::{Receiver, Sender};

use super::core::Daemon;
use super::git_worker::{GitOp, GitResult};
use super::ipc::{Request, Response, decode_request, encode_response};
use super::remote::RemoteUrl;

/// Message sent from socket handlers to state thread.
pub struct RequestMessage {
    pub request: Request,
    pub respond: Sender<Response>,
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
    let mut sync_waiters: HashMap<RemoteUrl, Vec<Sender<Response>>> = HashMap::new();

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
                                            let _ = respond.send(Response::err(e));
                                            continue;
                                        }
                                    };
                                    let clean = !repo_state.dirty && !repo_state.sync_in_progress;

                                    if clean {
                                        let _ = respond.send(Response::ok(super::ipc::ResponsePayload::synced()));
                                    } else {
                                        sync_waiters.entry(loaded.remote().clone()).or_default().push(respond);
                                    }
                                }
                                Err(e) => {
                                    let _ = respond.send(Response::err(e));
                                }
                            }
                            continue;
                        }

                        // Check for shutdown
                        let is_shutdown = matches!(request, Request::Shutdown);

                        let response = daemon.handle_request(request, &git_tx);
                        let _ = respond.send(response);

                        if is_shutdown {
                            // Sync all dirty repos before exiting (avoid double-borrow).
                            let remotes_to_sync: Vec<RemoteUrl> = daemon
                                .repos()
                                .filter(|(_, s)| s.dirty && !s.sync_in_progress)
                                .map(|(r, _)| r.clone())
                                .collect();
                            for remote in remotes_to_sync {
                                daemon.maybe_start_sync(&remote, &git_tx);
                            }

                            // Wait for in-flight syncs
                            let mut pending = daemon.repos()
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

fn flush_sync_waiters(daemon: &Daemon, waiters: &mut HashMap<RemoteUrl, Vec<Sender<Response>>>) {
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
                let _ = respond.send(Response::ok(super::ipc::ResponsePayload::synced()));
            }
        }
    }
}

/// Run the socket acceptor.
///
/// Accepts connections and spawns a handler thread for each.
pub fn run_socket_thread(listener: UnixListener, req_tx: Sender<RequestMessage>) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let req_tx = req_tx.clone();
                std::thread::spawn(move || handle_client(stream, req_tx));
            }
            Err(e) => {
                eprintln!("accept error: {}", e);
            }
        }
    }
}

/// Handle a single client connection.
///
/// Reads requests, sends to state thread, waits for response, writes back.
fn handle_client(stream: UnixStream, req_tx: Sender<RequestMessage>) {
    let reader = match stream.try_clone() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("failed to clone stream: {}", e);
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
        let request = match decode_request(&line) {
            Ok(r) => r,
            Err(e) => {
                let resp = Response::err(super::ipc::ErrorPayload {
                    code: "parse_error".into(),
                    message: e.to_string(),
                    details: None,
                });
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

        let response = match respond_rx.recv() {
            Ok(r) => r,
            Err(_) => break, // State thread died
        };

        // Write response
        let bytes = match encode_response(&response) {
            Ok(b) => b,
            Err(e) => {
                let msg = e.to_string().replace('"', "\\\"");
                format!(r#"{{"err":{{"code":"internal","message":"{}"}}}}\n"#, msg).into_bytes()
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
}
