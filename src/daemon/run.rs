//! Daemon runner (single-binary mode).
//!
//! `bd daemon run` starts the background service.

use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::Result;
use crate::core::ActorId;
use crate::daemon::IpcError;
use crate::daemon::ipc::ErrorPayload;
use crate::daemon::ipc::{encode_response, ensure_socket_dir};
use crate::daemon::server::RequestMessage;
use crate::daemon::{Daemon, GitResult, GitWorker, run_git_loop, run_state_loop};
use crate::daemon::{Request, Response, decode_request};

/// Run the daemon in the current process.
///
/// This never returns on success until a shutdown signal is received.
pub fn run_daemon() -> Result<()> {
    // Ensure socket directory exists with safe permissions.
    let dir = ensure_socket_dir()?;
    let socket = dir.join("daemon.sock");
    let meta_path = dir.join("daemon.meta.json");

    // If another daemon is already listening, exit quietly.
    if UnixStream::connect(&socket).is_ok() {
        eprintln!("daemon already running on {:?}", socket);
        return Ok(());
    }

    // Remove stale socket file.
    let _ = std::fs::remove_file(&socket);

    // Bind socket.
    let listener = UnixListener::bind(&socket).map_err(IpcError::from)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600));
    }
    eprintln!("daemon listening on {:?}", socket);

    // Write daemon metadata for client version checks.
    let meta = crate::api::DaemonInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: crate::daemon::ipc::IPC_PROTOCOL_VERSION,
        pid: std::process::id(),
    };
    let _ = std::fs::write(
        &meta_path,
        serde_json::to_vec(&meta).unwrap_or_else(|_| b"{}".to_vec()),
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&meta_path, std::fs::Permissions::from_mode(0o600));
    }

    // Set up signal handling for graceful shutdown.
    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = shutdown.clone();
        let _ = signal_hook::flag::register(signal_hook::consts::SIGTERM, shutdown.clone());
        let _ = signal_hook::flag::register(signal_hook::consts::SIGINT, shutdown.clone());
    }

    // Create channels.
    let (req_tx, req_rx) = crossbeam::channel::unbounded::<RequestMessage>();
    let (git_tx, git_rx) = crossbeam::channel::unbounded();
    let (git_result_tx, git_result_rx) = crossbeam::channel::unbounded::<GitResult>();

    // Create actor ID (allow override via BD_ACTOR).
    let username = whoami::username();
    let hostname = whoami::fallible::hostname().unwrap_or_else(|_| "unknown".into());
    let default_actor = format!("{}@{}", username, hostname);
    let actor_raw = std::env::var("BD_ACTOR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(default_actor);
    let actor = ActorId::new(actor_raw)?;

    // Create daemon core and git worker.
    let daemon = Daemon::new(actor);
    let git_worker = GitWorker::new(git_result_tx);

    // Spawn state thread.
    let state_handle = std::thread::spawn(move || {
        run_state_loop(daemon, req_rx, git_tx, git_result_rx);
    });

    // Spawn git thread.
    let git_handle = std::thread::spawn(move || {
        run_git_loop(git_worker, git_rx);
    });

    // Set socket to non-blocking for shutdown checks.
    listener.set_nonblocking(true).map_err(IpcError::from)?;

    // Run socket acceptor with shutdown check.
    loop {
        if shutdown.load(Ordering::Relaxed) {
            eprintln!("shutdown signal received");
            break;
        }

        match listener.accept() {
            Ok((stream, _)) => {
                let req_tx = req_tx.clone();
                std::thread::spawn(move || {
                    let _ = stream.set_nonblocking(false);
                    handle_client(stream, req_tx);
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(e) => {
                eprintln!("accept error: {}", e);
            }
        }
    }

    // On signal shutdown, ask state thread to flush and exit cleanly.
    if shutdown.load(Ordering::Relaxed) {
        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        let _ = req_tx.send(RequestMessage {
            request: Request::Shutdown,
            respond: respond_tx,
        });
        let _ = respond_rx.recv_timeout(std::time::Duration::from_secs(10));
    }

    drop(req_tx);

    let _ = state_handle.join();
    let _ = git_handle.join();

    let _ = std::fs::remove_file(&socket);
    let _ = std::fs::remove_file(&meta_path);
    eprintln!("daemon stopped");
    Ok(())
}

fn handle_client(
    stream: std::os::unix::net::UnixStream,
    req_tx: crossbeam::channel::Sender<RequestMessage>,
) {
    use std::io::{BufRead, BufReader, Write};

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
            Err(_) => break,
        };

        if line.trim().is_empty() {
            continue;
        }

        let request = match decode_request(&line) {
            Ok(r) => r,
            Err(e) => {
                let resp = Response::err(ErrorPayload {
                    code: "parse_error".into(),
                    message: e.to_string(),
                    details: None,
                });
                let bytes = encode_response(&resp).unwrap_or_else(|e| {
                    let msg = e.to_string().replace('"', "\\\"");
                    format!(r#"{{"err":{{"code":"internal","message":"{}"}}}}\n"#, msg).into_bytes()
                });
                let _ = write!(writer, "{}", String::from_utf8_lossy(&bytes));
                let _ = writer.flush();
                continue;
            }
        };

        let is_shutdown = matches!(request, Request::Shutdown);

        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        if req_tx
            .send(RequestMessage {
                request,
                respond: respond_tx,
            })
            .is_err()
        {
            break;
        }

        let response = match respond_rx.recv() {
            Ok(r) => r,
            Err(_) => break,
        };

        let bytes = encode_response(&response).unwrap_or_else(|e| {
            let msg = e.to_string().replace('"', "\\\"");
            format!(r#"{{"err":{{"code":"internal","message":"{}"}}}}\n"#, msg).into_bytes()
        });
        if write!(writer, "{}", String::from_utf8_lossy(&bytes)).is_err() {
            break;
        }
        let _ = writer.flush();

        if is_shutdown {
            break;
        }
    }
}
