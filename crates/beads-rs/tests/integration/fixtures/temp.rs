use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Once;

use serde::{Deserialize, Serialize};
use tempfile::{Builder, TempDir};

const TMP_ROOT_ENV: &str = "BD_TEST_TMP_ROOT";
const KEEP_TMP_ENV: &str = "BD_TEST_KEEP_TMP";
const DEFAULT_TMP_ROOT_DIR: &str = "beads-rs-tests";
const OWNER_FILE: &str = ".bd-test-owner.json";

static SWEEP_STALE_FIXTURES: Once = Once::new();

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct FixtureOwner {
    pid: u32,
}

pub(crate) fn keep_tmp_enabled() -> bool {
    std::env::var_os(KEEP_TMP_ENV).is_some()
}

pub(crate) fn fixture_tmp_root() -> PathBuf {
    let root = configured_tmp_root(std::env::var_os(TMP_ROOT_ENV).as_deref());
    fs::create_dir_all(&root).expect("create test tmp root");
    SWEEP_STALE_FIXTURES.call_once(|| sweep_stale_fixture_dirs(&root));
    root
}

pub(crate) fn fixture_tempdir(prefix: &str) -> TempDir {
    let tempdir = Builder::new()
        .prefix(prefix)
        .tempdir_in(fixture_tmp_root())
        .expect("create fixture temp dir");
    write_owner_file(tempdir.path(), process::id());
    tempdir
}

#[cfg(unix)]
pub(crate) fn assert_unix_socket_path_fits(path: &Path) {
    use std::os::unix::ffi::OsStrExt;

    const MAX_SUN_PATH_BYTES: usize = std::mem::size_of::<libc::sockaddr_un>()
        - std::mem::offset_of!(libc::sockaddr_un, sun_path);

    let actual = path.as_os_str().as_bytes().len();
    assert!(
        actual < MAX_SUN_PATH_BYTES,
        "unix socket path too long ({} bytes >= {}): {}",
        actual,
        MAX_SUN_PATH_BYTES,
        path.display()
    );
}

#[cfg(not(unix))]
pub(crate) fn assert_unix_socket_path_fits(_path: &Path) {}

fn configured_tmp_root(explicit_root: Option<&OsStr>) -> PathBuf {
    match explicit_root {
        Some(path) if !path.is_empty() => PathBuf::from(path),
        _ => default_tmp_root(),
    }
}

#[cfg(unix)]
fn default_tmp_root() -> PathBuf {
    let uid = nix::unistd::geteuid();
    PathBuf::from("/tmp").join(format!("{DEFAULT_TMP_ROOT_DIR}-{uid}"))
}

#[cfg(not(unix))]
fn default_tmp_root() -> PathBuf {
    std::env::temp_dir().join(DEFAULT_TMP_ROOT_DIR)
}

fn write_owner_file(dir: &Path, pid: u32) {
    let owner = FixtureOwner { pid };
    let bytes = serde_json::to_vec(&owner).expect("serialize fixture owner");
    fs::write(dir.join(OWNER_FILE), bytes).expect("write fixture owner");
}

fn sweep_stale_fixture_dirs(root: &Path) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(owner) = read_owner_file(&path) else {
            continue;
        };
        if process_alive(owner.pid) {
            continue;
        }
        cleanup_stale_fixture_dir(&path);
        let _ = fs::remove_dir_all(&path);
    }
}

fn read_owner_file(dir: &Path) -> Option<FixtureOwner> {
    let bytes = fs::read(dir.join(OWNER_FILE)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn cleanup_stale_fixture_dir(root: &Path) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(metadata) = fs::metadata(&path) else {
            continue;
        };
        if metadata.is_dir() {
            if let Ok(entries) = fs::read_dir(&path) {
                for entry in entries.flatten() {
                    stack.push(entry.path());
                }
            }
            continue;
        }
        match path.file_name() {
            Some(name) if name == OsStr::new("daemon.meta.json") => {
                kill_process_from_json_pid(&path, "bd daemon run");
            }
            Some(name) if name == OsStr::new("ready") => {
                kill_process_from_ready_file(&path);
            }
            _ => {}
        }
    }
}

fn kill_process_from_json_pid(path: &Path, expected_command: &str) {
    let Some(pid) = fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .and_then(|json| json["pid"].as_u64())
        .map(|pid| pid as u32)
    else {
        return;
    };
    kill_process_if_matches(pid, expected_command);
}

fn kill_process_from_ready_file(path: &Path) {
    let Some(pid) = fs::read_to_string(path).ok().and_then(|contents| {
        contents
            .lines()
            .find_map(|line| line.strip_prefix("pid="))
            .and_then(|pid| pid.parse::<u32>().ok())
    }) else {
        return;
    };
    kill_process_if_matches(pid, "tailnet_proxy");
}

fn kill_process_if_matches(pid: u32, expected_command: &str) {
    if !process_alive(pid) {
        return;
    }
    let Some(command) = process_command(pid) else {
        return;
    };
    if !command.contains(expected_command) {
        return;
    }
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
}

fn process_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    kill(Pid::from_raw(pid as i32), None).is_ok()
}

fn process_command(pid: u32) -> Option<String> {
    let output = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "command="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let command = String::from_utf8(output.stdout).ok()?;
    let command = command.trim();
    (!command.is_empty()).then(|| command.to_string())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    #[cfg(unix)]
    fn fixture_tmp_root_defaults_under_system_temp_dir() {
        let root = configured_tmp_root(None);
        assert!(root.starts_with("/tmp"));
        let file_name = root.file_name().expect("tmp root file name");
        assert!(
            file_name
                .to_string_lossy()
                .starts_with(&format!("{DEFAULT_TMP_ROOT_DIR}-")),
            "unexpected tmp root: {}",
            root.display()
        );
    }

    #[test]
    #[cfg(not(unix))]
    fn fixture_tmp_root_defaults_under_system_temp_dir() {
        let root = configured_tmp_root(None);
        assert!(root.starts_with(std::env::temp_dir()));
        assert_eq!(root.file_name(), Some(OsStr::new(DEFAULT_TMP_ROOT_DIR)));
    }

    #[test]
    fn fixture_tmp_root_uses_explicit_override() {
        let root = configured_tmp_root(Some(OsStr::new("/tmp/beads-custom")));
        assert_eq!(root, PathBuf::from("/tmp/beads-custom"));
    }

    #[cfg(unix)]
    #[test]
    fn fixture_tempdir_keeps_socket_paths_within_unix_limit() {
        let temp = fixture_tempdir("socket-path");
        let socket = temp
            .path()
            .join("node-0")
            .join("runtime")
            .join("beads")
            .join("daemon.sock");
        assert_unix_socket_path_fits(&socket);
    }

    #[test]
    fn sweep_stale_fixture_dirs_removes_dead_owner_dirs() {
        let root = TempDir::new().expect("temp root");
        let stale = root.path().join("stale-fixture");
        fs::create_dir_all(&stale).expect("create stale fixture dir");
        write_owner_file(&stale, 999_999);

        sweep_stale_fixture_dirs(root.path());

        assert!(!stale.exists(), "stale fixture dir should be removed");
    }

    #[test]
    fn sweep_stale_fixture_dirs_keeps_live_owner_dirs() {
        let root = TempDir::new().expect("temp root");
        let live = root.path().join("live-fixture");
        fs::create_dir_all(&live).expect("create live fixture dir");
        write_owner_file(&live, process::id());

        sweep_stale_fixture_dirs(root.path());

        assert!(live.exists(), "live fixture dir should be preserved");
    }
}
