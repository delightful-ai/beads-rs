#![allow(dead_code)]

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use beads_rs::daemon::ipc::{Request, Response};

/// Best-effort daemon shutdown for tests.
///
/// Idempotent: missing socket or meta -> Ok.
pub fn shutdown_daemon(runtime_dir: &Path) {
    let socket = socket_path(runtime_dir);
    if !socket.exists() {
        return;
    }

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

    if socket.exists() {
        if let Some(pid) = daemon_pid(runtime_dir) {
            terminate_process(pid, Duration::from_millis(500));
        }
        let _ = fs::remove_file(&socket);
    }

    let _ = fs::remove_file(meta_path(runtime_dir));
}

fn socket_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("beads").join("daemon.sock")
}

fn meta_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("beads").join("daemon.meta.json")
}

fn daemon_pid(runtime_dir: &Path) -> Option<u32> {
    let contents = fs::read_to_string(meta_path(runtime_dir)).ok()?;
    let meta: serde_json::Value = serde_json::from_str(&contents).ok()?;
    meta["pid"].as_u64().map(|pid| pid as u32)
}

fn wait_for_socket_removal(socket: &Path, timeout: Duration) {
    let _ = poll_until(timeout, || !socket.exists());
}

fn terminate_process(pid: u32, timeout: Duration) {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;

    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
    wait_for_exit(pid, timeout);
    if process_alive(pid) {
        let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
        wait_for_exit(pid, timeout);
    }
}

fn wait_for_exit(pid: u32, timeout: Duration) {
    let _ = poll_until(timeout, || !process_alive(pid));
}

fn process_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), None).is_ok()
}

fn poll_until<F>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(5);
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        std::thread::sleep(backoff);
        backoff = std::cmp::min(backoff.saturating_mul(2), Duration::from_millis(50));
    }
    condition()
}
