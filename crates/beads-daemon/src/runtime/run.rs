//! Daemon runner (single-binary mode).
//!
//! `bd daemon run` starts the background service.

use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::DaemonRuntimeConfig;
use crate::layout::DaemonLayout;

use crate::Result;
use crate::core::ActorId;
use crate::runtime::IpcError;
use crate::runtime::Request;
use crate::runtime::server::{RequestMessage, handle_client, run_state_loop};
use crate::runtime::{Daemon, GitResult, GitWorker, run_git_loop};

fn wake_listener(socket: &Path) {
    if let Err(err) = UnixStream::connect(socket) {
        tracing::debug!("shutdown wake connect failed: {}", err);
    }
}

fn duration_ms_since_epoch(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn system_time_ms(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(duration_ms_since_epoch)
}

/// Run the daemon in the current process.
///
/// This never returns on success until shutdown is requested by signal or IPC.
pub fn run_daemon(
    actor: ActorId,
    layout: DaemonLayout,
    runtime_config: DaemonRuntimeConfig,
) -> Result<()> {
    let socket = layout.socket_path.clone();
    let meta_path = socket.with_file_name("daemon.meta.json");

    let socket_dir = socket.parent().ok_or_else(|| {
        IpcError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "daemon socket path must have a parent directory",
        ))
    })?;
    std::fs::create_dir_all(socket_dir).map_err(IpcError::from)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(socket_dir)
            .map_err(IpcError::from)?
            .permissions()
            .mode()
            & 0o777;
        if mode != 0o700 {
            std::fs::set_permissions(socket_dir, std::fs::Permissions::from_mode(0o700))
                .map_err(IpcError::from)?;
        }
    }

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
    let started_at_ms = system_time_ms(SystemTime::now());
    let meta = crate::api::DaemonInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: crate::runtime::ipc::IPC_PROTOCOL_VERSION,
        pid: std::process::id(),
        started_at_ms,
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

    let limits = Arc::new(runtime_config.limits.clone());

    // Create daemon core and git worker.
    let mut daemon = Daemon::new_with_runtime_config(actor, layout, runtime_config);
    daemon.set_started_at_ms(started_at_ms);
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

#[cfg(test)]
mod tests {
    use super::{duration_ms_since_epoch, system_time_ms};
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn duration_ms_since_epoch_saturates_large_durations() {
        assert_eq!(
            duration_ms_since_epoch(Duration::from_secs(u64::MAX)),
            u64::MAX
        );
    }

    #[test]
    fn system_time_ms_rejects_pre_epoch_timestamps() {
        let before_epoch = UNIX_EPOCH - Duration::from_secs(1);
        assert_eq!(system_time_ms(before_epoch), None);
    }
}
