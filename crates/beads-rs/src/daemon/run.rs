//! Daemon runner (single-binary mode).
//!
//! `bd daemon run` starts the background service.

use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::Result;
use crate::core::ActorId;
use crate::daemon::IpcError;
use crate::daemon::Request;
use crate::daemon::ipc::ensure_socket_dir;
use crate::daemon::server::{RequestMessage, handle_client, run_state_loop};
use crate::daemon::{Daemon, GitResult, GitWorker, run_git_loop};

fn wake_listener(socket: &Path) {
    if let Err(err) = UnixStream::connect(socket) {
        tracing::debug!("shutdown wake connect failed: {}", err);
    }
}

/// Run the daemon in the current process.
///
/// This never returns on success until shutdown is requested by signal or IPC.
pub fn run_daemon() -> Result<()> {
    // Load config first so path overrides are available for socket directory.
    let config = crate::config::load_or_init();
    crate::paths::init_from_config(&config.paths);

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
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&shutdown))
        .map_err(IpcError::from)?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&shutdown))
        .map_err(IpcError::from)?;

    // Create channels.
    let (req_tx, req_rx) = crossbeam::channel::unbounded::<RequestMessage>();
    let (git_tx, git_rx) = crossbeam::channel::unbounded();
    let (git_result_tx, git_result_rx) = crossbeam::channel::unbounded::<GitResult>();

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
    let (state_exit_tx, state_exit_rx) = crossbeam::channel::bounded(1);
    let state_span = tracing::Span::current();
    let state_handle = std::thread::spawn(move || {
        state_span.in_scope(|| {
            run_state_loop(daemon, req_rx, git_tx, git_result_rx);
        });
        let _ = state_exit_tx.send(());
    });

    // Spawn git thread.
    let git_span = tracing::Span::current();
    let git_handle = std::thread::spawn(move || {
        git_span.in_scope(|| {
            run_git_loop(git_worker, git_rx);
        });
    });

    // Ensure listener is blocking; wake with a self-connect on shutdown.
    listener.set_nonblocking(false).map_err(IpcError::from)?;

    let accept_shutdown = Arc::clone(&shutdown);
    let accept_limits = Arc::clone(&limits);
    let accept_req_tx = req_tx.clone();
    let accept_span = tracing::Span::current();
    let accept_handle = std::thread::spawn(move || {
        accept_span.in_scope(|| {
            loop {
                if accept_shutdown.load(Ordering::Relaxed) {
                    tracing::info!("shutdown signal received (accept loop)");
                    break;
                }

                match listener.accept() {
                    Ok((stream, _)) => {
                        if accept_shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        let req_tx = accept_req_tx.clone();
                        let limits = Arc::clone(&accept_limits);
                        let client_span = tracing::Span::current();
                        std::thread::spawn(move || {
                            client_span.in_scope(|| {
                                let _ = stream.set_nonblocking(false);
                                handle_client(stream, req_tx, limits);
                            });
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {
                        if accept_shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    Err(e) => {
                        if accept_shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        tracing::error!("accept error: {}", e);
                    }
                }
            }
        });
    });

    let shutdown_via_signal = loop {
        if shutdown.load(Ordering::Relaxed) {
            break true;
        }
        match state_exit_rx.recv_timeout(std::time::Duration::from_millis(50)) {
            Ok(()) => break false,
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break false,
        }
    };

    shutdown.store(true, Ordering::Relaxed);
    if shutdown_via_signal {
        tracing::info!("shutdown signal received");
    } else {
        tracing::info!("state loop exited; shutting down daemon");
    }

    wake_listener(&socket);
    let _ = accept_handle.join();

    // On signal shutdown, ask state thread to flush and exit cleanly.
    if shutdown_via_signal {
        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        let _ = req_tx.send(RequestMessage::new(Request::Shutdown, respond_tx));
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
