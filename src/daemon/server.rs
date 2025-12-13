//! Server thread loops.
//!
//! Three threads:
//! - Socket acceptor (main thread) - accepts connections, spawns handlers
//! - State thread - owns Daemon, processes requests sequentially
//! - Git thread - owns GitWorker, handles all git IO

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};

use crossbeam::channel::{Receiver, Sender};

use super::core::Daemon;
use super::git_worker::{GitOp, SyncResult};
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
    timer_rx: Receiver<RemoteUrl>,
    git_tx: Sender<GitOp>,
    git_result_rx: Receiver<(RemoteUrl, SyncResult)>,
) {
    let mut sync_waiters: HashMap<RemoteUrl, Vec<Sender<Response>>> = HashMap::new();

    loop {
        crossbeam::select! {
            // Client request
            recv(req_rx) -> msg => {
                match msg {
                    Ok(RequestMessage { request, respond }) => {
                        // Sync barrier: wait until repo is clean.
                        if let Request::SyncWait { repo } = request {
                            match daemon.ensure_repo_loaded(&repo, &git_tx) {
                                Ok(remote) => {
                                    daemon.maybe_start_sync(&remote, &git_tx);
                                    let clean = daemon
                                        .repo_state(&remote)
                                        .map(|s| !s.dirty && !s.sync_in_progress)
                                        .unwrap_or(true);

                                    if clean {
                                        let _ = respond.send(Response::ok(super::ipc::ResponsePayload::Synced));
                                    } else {
                                        sync_waiters.entry(remote).or_default().push(respond);
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
                                if let Ok((remote, result)) = git_result_rx.recv() {
                                    daemon.complete_sync(&remote, result);
                                    pending -= 1;
                                } else {
                                    break;
                                }
                            }

                            // Signal git thread to shutdown
                            let _ = git_tx.send(GitOp::Shutdown);
                            return;
                        }

                        flush_sync_waiters(&daemon, &mut sync_waiters);
                    }
                    Err(_) => {
                        // Channel closed - time to exit
                        let _ = git_tx.send(GitOp::Shutdown);
                        return;
                    }
                }
            }

            // Debounce timer fired
            recv(timer_rx) -> msg => {
                if let Ok(remote) = msg {
                    if daemon.handle_timer(&remote) {
                        daemon.maybe_start_sync(&remote, &git_tx);
                    }
                    flush_sync_waiters(&daemon, &mut sync_waiters);
                }
            }

            // Git operation completed
            recv(git_result_rx) -> msg => {
                if let Ok((remote, result)) = msg {
                    daemon.complete_sync(&remote, result);
                }
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
                .repo_state(remote)
                .map(|s| !s.dirty && !s.sync_in_progress)
                .unwrap_or(true)
        })
        .cloned()
        .collect();

    for remote in ready {
        if let Some(list) = waiters.remove(&remote) {
            for respond in list {
                let _ = respond.send(Response::ok(super::ipc::ResponsePayload::Synced));
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
