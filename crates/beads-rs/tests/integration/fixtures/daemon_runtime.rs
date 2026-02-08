#![allow(dead_code)]

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use beads_daemon::test_utils::{daemon_meta_path, daemon_socket_path, poll_until_with_backoff};
use beads_rs::surface::ipc::{Request, Response};

/// Best-effort daemon shutdown for tests.
///
/// Idempotent: missing socket or meta -> Ok.
pub fn shutdown_daemon(runtime_dir: &Path) {
    let socket = daemon_socket_path(runtime_dir);
    let meta = daemon_meta_path(runtime_dir);
    let mut candidates = candidate_daemon_candidates(runtime_dir);
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

    if socket.exists() || meta.exists() || !candidates.is_empty() {
        candidates.extend(candidate_daemon_candidates(runtime_dir));
        candidates.sort_unstable_by_key(|candidate| candidate.pid);
        candidates.dedup_by_key(|candidate| candidate.pid);
        for candidate in candidates {
            terminate_process(&candidate, Duration::from_millis(500));
        }
        let _ = fs::remove_file(&socket);
        let _ = fs::remove_file(&meta);
    }
    cleanup_store_locks(runtime_dir);
}

/// Force-kill the daemon process without cleanup (simulates a crash).
pub fn crash_daemon(runtime_dir: &Path) {
    let mut candidates = candidate_daemon_candidates(runtime_dir);
    if candidates.is_empty() {
        if let Some(candidate) = daemon_candidate_from_meta(runtime_dir) {
            candidates.push(candidate);
        }
    }
    candidates.sort_unstable_by_key(|candidate| candidate.pid);
    candidates.dedup_by_key(|candidate| candidate.pid);
    for candidate in candidates {
        force_kill(&candidate, Duration::from_millis(500));
    }
}

#[derive(Clone, Copy)]
struct DaemonCandidate {
    pid: u32,
    started_at_ms: Option<u64>,
}

fn daemon_candidate_from_meta(runtime_dir: &Path) -> Option<DaemonCandidate> {
    let contents = fs::read_to_string(daemon_meta_path(runtime_dir)).ok()?;
    let meta: serde_json::Value = serde_json::from_str(&contents).ok()?;
    Some(DaemonCandidate {
        pid: meta["pid"].as_u64()? as u32,
        started_at_ms: meta["started_at_ms"].as_u64(),
    })
}

fn candidate_daemon_candidates(runtime_dir: &Path) -> Vec<DaemonCandidate> {
    let mut candidates = Vec::new();
    if let Some(candidate) = daemon_candidate_from_meta(runtime_dir) {
        candidates.push(candidate);
    }
    candidates.extend(
        store_lock_entries(runtime_dir)
            .into_iter()
            .filter_map(|entry| {
                entry.pid.map(|pid| DaemonCandidate {
                    pid,
                    started_at_ms: entry.started_at_ms,
                })
            }),
    );
    candidates
}

fn cleanup_store_locks(runtime_dir: &Path) {
    for entry in store_lock_entries(runtime_dir) {
        let stale_or_missing = match (entry.pid, entry.started_at_ms) {
            (None, _) => true,
            (Some(pid), _) if !process_alive(pid) => true,
            (Some(pid), Some(started_at_ms)) => {
                matches!(
                    pid_matches_started_at(pid, Some(started_at_ms)),
                    Some(false)
                )
            }
            (Some(pid), None) => matches!(process_looks_like_bd_daemon(pid), Some(false)),
        };
        if stale_or_missing {
            let _ = fs::remove_file(entry.path);
        }
    }
}

struct StoreLockEntry {
    path: std::path::PathBuf,
    pid: Option<u32>,
    started_at_ms: Option<u64>,
}

fn store_lock_entries(runtime_dir: &Path) -> Vec<StoreLockEntry> {
    let stores_dir = runtime_dir.join("data").join("stores");
    let Ok(entries) = fs::read_dir(stores_dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|entry| entry.path().join("store.lock"))
        .filter(|path| path.is_file())
        .map(|path| {
            let (pid, started_at_ms) = fs::read_to_string(&path)
                .ok()
                .and_then(|contents| serde_json::from_str::<serde_json::Value>(&contents).ok())
                .map(|json| {
                    (
                        json["pid"].as_u64().map(|pid| pid as u32),
                        json["started_at_ms"].as_u64(),
                    )
                })
                .unwrap_or((None, None));
            StoreLockEntry {
                path,
                pid,
                started_at_ms,
            }
        })
        .collect()
}

fn wait_for_socket_removal(socket: &Path, timeout: Duration) {
    let _ = poll_until_with_backoff(
        timeout,
        Duration::from_millis(5),
        Duration::from_millis(50),
        || !socket.exists(),
    );
}

fn terminate_process(candidate: &DaemonCandidate, timeout: Duration) {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    if !process_alive(candidate.pid)
        || !matches!(
            pid_matches_started_at(candidate.pid, candidate.started_at_ms),
            Some(true)
        )
    {
        return;
    }
    let _ = kill(Pid::from_raw(candidate.pid as i32), Signal::SIGTERM);
    wait_for_exit(candidate.pid, timeout);
    if process_alive(candidate.pid)
        && matches!(
            pid_matches_started_at(candidate.pid, candidate.started_at_ms),
            Some(true)
        )
    {
        let _ = kill(Pid::from_raw(candidate.pid as i32), Signal::SIGKILL);
        wait_for_exit(candidate.pid, timeout);
    }
}

fn force_kill(candidate: &DaemonCandidate, timeout: Duration) {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    if !process_alive(candidate.pid)
        || !matches!(
            pid_matches_started_at(candidate.pid, candidate.started_at_ms),
            Some(true)
        )
    {
        return;
    }
    let _ = kill(Pid::from_raw(candidate.pid as i32), Signal::SIGKILL);
    wait_for_exit(candidate.pid, timeout);
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

fn pid_matches_started_at(pid: u32, expected_started_at_ms: Option<u64>) -> Option<bool> {
    let Some(expected_started_at_ms) = expected_started_at_ms else {
        return None;
    };
    let Some(elapsed_secs) = process_elapsed_secs(pid) else {
        return None;
    };
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(expected_started_at_ms);
    let observed_started_at_ms = now_ms.saturating_sub(elapsed_secs.saturating_mul(1000));
    Some(observed_started_at_ms.abs_diff(expected_started_at_ms) <= 120_000)
}

fn process_elapsed_secs(pid: u32) -> Option<u64> {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "etime="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_etime_to_secs(std::str::from_utf8(&output.stdout).ok()?)
}

fn process_looks_like_bd_daemon(pid: u32) -> Option<bool> {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "command="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let command = std::str::from_utf8(&output.stdout).ok()?.trim();
    Some(command.contains("bd daemon run"))
}

fn parse_etime_to_secs(etime: &str) -> Option<u64> {
    let etime = etime.trim();
    if etime.is_empty() {
        return None;
    }
    let (days, hms) = if let Some((days, hms)) = etime.split_once('-') {
        (days.parse::<u64>().ok()?, hms)
    } else {
        (0, etime)
    };
    let parts: Vec<&str> = hms.split(':').collect();
    let (hours, minutes, seconds) = match parts.as_slice() {
        [minutes, seconds] => (
            0,
            minutes.parse::<u64>().ok()?,
            seconds.parse::<u64>().ok()?,
        ),
        [hours, minutes, seconds] => (
            hours.parse::<u64>().ok()?,
            minutes.parse::<u64>().ok()?,
            seconds.parse::<u64>().ok()?,
        ),
        _ => return None,
    };
    Some(days * 86_400 + hours * 3_600 + minutes * 60 + seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_daemon_removes_stale_store_lock() {
        let runtime_dir = tempfile::TempDir::new().expect("runtime dir");
        let store_dir = runtime_dir
            .path()
            .join("data")
            .join("stores")
            .join("store-a");
        fs::create_dir_all(&store_dir).expect("store dir");
        let lock_path = store_dir.join("store.lock");
        fs::write(
            &lock_path,
            r#"{"store_id":"s","replica_id":"r","pid":999999,"started_at_ms":1}"#,
        )
        .expect("write lock file");

        shutdown_daemon(runtime_dir.path());

        assert!(!lock_path.exists(), "expected stale lock to be removed");
    }

    #[test]
    fn parse_etime_to_secs_supports_ps_formats() {
        assert_eq!(parse_etime_to_secs("00:09"), Some(9));
        assert_eq!(parse_etime_to_secs("01:02:03"), Some(3723));
        assert_eq!(parse_etime_to_secs("2-03:04:05"), Some(183_845));
        assert_eq!(parse_etime_to_secs("not-a-time"), None);
    }
}
