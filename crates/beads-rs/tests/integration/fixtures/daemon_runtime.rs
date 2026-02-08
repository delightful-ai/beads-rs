#![allow(dead_code)]

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use beads_daemon::test_utils::{daemon_meta_path, daemon_socket_path, poll_until_with_backoff};
use beads_rs::surface::ipc::{Request, Response};

/// Best-effort daemon shutdown for tests.
///
/// Idempotent: missing socket or meta -> Ok.
pub fn shutdown_daemon(runtime_dir: &Path) {
    let socket = daemon_socket_path(runtime_dir);
    let meta = daemon_meta_path(runtime_dir);
    let _ = poll_until_with_backoff(
        Duration::from_millis(200),
        Duration::from_millis(5),
        Duration::from_millis(50),
        || socket.exists() || meta.exists(),
    );

    if socket.exists() {
        if let Ok(mut stream) = UnixStream::connect(&socket) {
            let mut json = serde_json::to_string(&Request::Shutdown)
                .unwrap_or_else(|_| r#"{"op":"shutdown"}"#.to_string());
            json.push('\n');
            let _ = stream.write_all(json.as_bytes());
            let _ = stream.flush();
            let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            let _ = reader.read_line(&mut line);
            let _ = serde_json::from_str::<Response>(&line);
        }
        wait_for_socket_removal(&socket, Duration::from_secs(2));
    }

    if socket.exists() || meta.exists() {
        if let Some(pid) = daemon_pid(runtime_dir) {
            terminate_process(pid, Duration::from_millis(500));
        }
        let _ = fs::remove_file(&socket);
        let _ = fs::remove_file(&meta);
    }
}

/// Force-kill the daemon process without cleanup (simulates a crash).
pub fn crash_daemon(runtime_dir: &Path) {
    if let Some(pid) = daemon_pid(runtime_dir) {
        force_kill(pid, Duration::from_millis(500));
    }
}

fn daemon_pid(runtime_dir: &Path) -> Option<u32> {
    let contents = fs::read_to_string(daemon_meta_path(runtime_dir)).ok()?;
    let meta: serde_json::Value = serde_json::from_str(&contents).ok()?;
    meta["pid"].as_u64().map(|pid| pid as u32)
}

fn wait_for_socket_removal(socket: &Path, timeout: Duration) {
    let _ = poll_until_with_backoff(
        timeout,
        Duration::from_millis(5),
        Duration::from_millis(50),
        || !socket.exists(),
    );
}

fn terminate_process(pid: u32, timeout: Duration) {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
    wait_for_exit(pid, timeout);
    if process_alive(pid) {
        let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
        wait_for_exit(pid, timeout);
    }
}

fn force_kill(pid: u32, timeout: Duration) {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
    wait_for_exit(pid, timeout);
}

fn wait_for_exit(pid: u32, timeout: Duration) {
    let _ = poll_until_with_backoff(
        timeout,
        Duration::from_millis(5),
        Duration::from_millis(50),
        || !process_alive(pid),
    );
}

fn process_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), None).is_ok()
}
