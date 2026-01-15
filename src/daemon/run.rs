//! Daemon runner (single-binary mode).
//!
//! `bd daemon run` starts the background service.

use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::Result;
use crate::core::ActorId;
use crate::daemon::IpcError;
use crate::daemon::Request;
use crate::daemon::ipc::ensure_socket_dir;
use crate::daemon::server::{RequestMessage, handle_client};
use crate::daemon::{Daemon, GitResult, GitWorker, run_git_loop, run_state_loop};

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
        tracing::warn!("daemon already running on {:?}", socket);
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
    tracing::info!("daemon listening on {:?}", socket);

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

    // Load config (limits, upgrade policy, etc).
    let config = crate::config::load_or_init();
    // Resolve actor ID (config/env override, fallback to username@hostname).
    let actor = match config.defaults.actor.clone() {
        Some(actor) => actor,
        None => {
            let username = whoami::username();
            let hostname = whoami::fallible::hostname().unwrap_or_else(|_| "unknown".into());
            let default_actor = format!("{}@{}", username, hostname);
            ActorId::new(default_actor)?
        }
    };
    let limits = Arc::new(config.limits.clone());

    // Create daemon core and git worker.
    let daemon = Daemon::new_with_config(actor, config.clone());
    let git_worker = GitWorker::new(git_result_tx, (*limits).clone());

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
            tracing::info!("shutdown signal received");
            break;
        }

        match listener.accept() {
            Ok((stream, _)) => {
                let req_tx = req_tx.clone();
                let limits = Arc::clone(&limits);
                std::thread::spawn(move || {
                    let _ = stream.set_nonblocking(false);
                    handle_client(stream, req_tx, limits);
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(e) => {
                tracing::error!("accept error: {}", e);
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
    tracing::info!("daemon stopped");
    Ok(())
}
