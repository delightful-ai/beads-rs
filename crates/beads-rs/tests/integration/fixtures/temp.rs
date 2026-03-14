use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use tempfile::{Builder, TempDir};

const TMP_ROOT_ENV: &str = "BD_TEST_TMP_ROOT";
const KEEP_TMP_ENV: &str = "BD_TEST_KEEP_TMP";
const DEFAULT_TMP_ROOT_DIR: &str = "beads-rs-tests";

pub(crate) fn keep_tmp_enabled() -> bool {
    std::env::var_os(KEEP_TMP_ENV).is_some()
}

pub(crate) fn fixture_tmp_root() -> PathBuf {
    let root = configured_tmp_root(std::env::var_os(TMP_ROOT_ENV).as_deref());
    fs::create_dir_all(&root).expect("create test tmp root");
    root
}

pub(crate) fn fixture_tempdir(prefix: &str) -> TempDir {
    Builder::new()
        .prefix(prefix)
        .tempdir_in(fixture_tmp_root())
        .expect("create fixture temp dir")
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

#[cfg(test)]
mod tests {
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
}
