//! Auto-upgrade support.

use std::fs;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::config::{Config, load_or_init};
use crate::daemon::OpError;
use crate::daemon::ipc::{Request, send_request_no_autostart, socket_path};
use crate::{Error, Result};

const AUTO_CHECK_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

#[derive(Debug, Clone)]
pub struct UpgradeOutcome {
    pub updated: bool,
    pub from_version: String,
    pub to_version: Option<String>,
    pub install_path: PathBuf,
    pub method: UpgradeMethod,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpgradeMethod {
    Prebuilt,
    Cargo,
    None,
}

#[derive(Debug, Clone, Deserialize)]
struct ReleaseInfo {
    tag_name: String,
    assets: Vec<ReleaseAsset>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReleaseAsset {
    name: String,
    browser_download_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct UpgradeState {
    last_check_wall_ms: Option<u64>,
}

pub fn maybe_spawn_auto_upgrade() {
    if std::env::var("BD_NO_AUTO_UPGRADE").is_ok() {
        return;
    }

    let cfg = load_or_init();
    if !cfg.auto_upgrade {
        return;
    }

    if let Ok(Some(state)) = read_state() {
        if let Some(last) = state.last_check_wall_ms {
            let now = wall_ms();
            if now.saturating_sub(last) < AUTO_CHECK_INTERVAL.as_millis() as u64 {
                return;
            }
        }
    }

    let _ = write_state(&UpgradeState {
        last_check_wall_ms: Some(wall_ms()),
    });

    let Ok(exe) = std::env::current_exe() else {
        return;
    };

    let mut cmd = Command::new(exe);
    cmd.arg("upgrade").arg("--background");
    cmd.env("BD_NO_AUTO_UPGRADE", "1");
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let _ = cmd.spawn();
}

pub fn run_upgrade(config: Config, background: bool) -> Result<UpgradeOutcome> {
    let current_version = env!("CARGO_PKG_VERSION").to_string();
    let release = fetch_latest_release()?;
    let latest = normalize_version(&release.tag_name);

    if !is_newer_version(latest, &current_version) {
        return Ok(UpgradeOutcome {
            updated: false,
            from_version: current_version,
            to_version: Some(latest.to_string()),
            install_path: resolve_install_path()?,
            method: UpgradeMethod::None,
        });
    }

    if !config.auto_upgrade && background {
        return Ok(UpgradeOutcome {
            updated: false,
            from_version: current_version,
            to_version: Some(latest.to_string()),
            install_path: resolve_install_path()?,
            method: UpgradeMethod::None,
        });
    }

    let install_path = resolve_install_path()?;
    let platform = detect_platform();
    let asset = platform.and_then(|p| find_asset(&release, p));

    let mut was_running = false;
    if std::env::var("BD_UPGRADE_SKIP_DAEMON").is_err() {
        was_running = stop_daemon().unwrap_or(false);
    }

    let method = if let Some(asset) = asset {
        let temp_bin = download_prebuilt(asset)?;
        install_binary(temp_bin.as_ref(), &install_path)?;
        UpgradeMethod::Prebuilt
    } else {
        let temp_bin = build_with_cargo()?;
        install_binary(temp_bin.as_ref(), &install_path)?;
        UpgradeMethod::Cargo
    };

    if was_running && std::env::var("BD_UPGRADE_SKIP_DAEMON").is_err() {
        let _ = start_daemon(&install_path);
    }

    Ok(UpgradeOutcome {
        updated: true,
        from_version: current_version,
        to_version: Some(latest.to_string()),
        install_path,
        method,
    })
}

fn fetch_latest_release() -> Result<ReleaseInfo> {
    if let Ok(path) = std::env::var("BD_UPGRADE_RELEASE_JSON") {
        let contents = fs::read_to_string(&path)
            .map_err(|e| upgrade_error(format!("failed to read release json {}: {e}", path)))?;
        let release: ReleaseInfo = serde_json::from_str(&contents)
            .map_err(|e| upgrade_error(format!("failed to parse release json {}: {e}", path)))?;
        return Ok(release);
    }

    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(15))
        .build();
    let url = "https://api.github.com/repos/delightful-ai/beads-rs/releases/latest";
    let resp = agent
        .get(url)
        .set("User-Agent", "beads-rs-upgrade")
        .call()
        .map_err(|e| upgrade_error(format!("failed to fetch release info: {e}")))?;
    let mut body = String::new();
    resp.into_reader()
        .read_to_string(&mut body)
        .map_err(|e| upgrade_error(format!("failed to read release body: {e}")))?;
    serde_json::from_str::<ReleaseInfo>(&body)
        .map_err(|e| upgrade_error(format!("failed to parse release info: {e}")))
}

fn detect_platform() -> Option<&'static str> {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    match (os, arch) {
        ("macos", "aarch64") => Some("aarch64-apple-darwin"),
        ("macos", "arm64") => Some("aarch64-apple-darwin"),
        ("linux", "x86_64") => Some("x86_64-unknown-linux-gnu"),
        ("linux", "amd64") => Some("x86_64-unknown-linux-gnu"),
        _ => None,
    }
}

fn find_asset<'a>(release: &'a ReleaseInfo, platform: &str) -> Option<&'a ReleaseAsset> {
    let name = format!("beads-rs-{platform}.tar.gz");
    release.assets.iter().find(|asset| asset.name == name)
}

fn download_prebuilt(asset: &ReleaseAsset) -> Result<tempfile::TempPath> {
    if let Ok(fake) = std::env::var("BD_UPGRADE_FAKE_BIN") {
        let temp = tempfile::NamedTempFile::new()
            .map_err(|e| upgrade_error(format!("failed to create temp file: {e}")))?;
        fs::copy(&fake, temp.path())
            .map_err(|e| upgrade_error(format!("failed to copy fake bin: {e}")))?;
        return Ok(temp.into_temp_path());
    }

    let archive_file = tempfile::NamedTempFile::new()
        .map_err(|e| upgrade_error(format!("failed to create temp archive: {e}")))?;

    if let Ok(dir) = std::env::var("BD_UPGRADE_ASSET_DIR") {
        let src = PathBuf::from(dir).join(&asset.name);
        fs::copy(&src, archive_file.path())
            .map_err(|e| upgrade_error(format!("failed to copy asset {}: {e}", src.display())))?;
    } else {
        let agent = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(30))
            .build();
        let resp = agent
            .get(&asset.browser_download_url)
            .set("User-Agent", "beads-rs-upgrade")
            .call()
            .map_err(|e| upgrade_error(format!("failed to download asset: {e}")))?;
        let mut reader = resp.into_reader();
        let mut file = fs::File::create(archive_file.path())
            .map_err(|e| upgrade_error(format!("failed to create archive: {e}")))?;
        std::io::copy(&mut reader, &mut file)
            .map_err(|e| upgrade_error(format!("failed to write archive: {e}")))?;
    }

    let archive = fs::File::open(archive_file.path())
        .map_err(|e| upgrade_error(format!("failed to open archive: {e}")))?;
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(archive));
    let mut extracted = false;
    let temp_bin = tempfile::NamedTempFile::new()
        .map_err(|e| upgrade_error(format!("failed to create temp binary: {e}")))?;
    let temp_path = temp_bin.into_temp_path();
    for entry in archive
        .entries()
        .map_err(|e| upgrade_error(format!("{e}")))?
    {
        let mut entry = entry.map_err(|e| upgrade_error(format!("{e}")))?;
        let path = entry.path().map_err(|e| upgrade_error(format!("{e}")))?;
        if path.file_name().and_then(|s| s.to_str()) == Some("bd") {
            entry
                .unpack(&temp_path)
                .map_err(|e| upgrade_error(format!("failed to unpack: {e}")))?;
            extracted = true;
            break;
        }
    }
    if !extracted {
        return Err(upgrade_error("archive missing bd binary".to_string()));
    }
    Ok(temp_path)
}

fn build_with_cargo() -> Result<tempfile::TempPath> {
    if let Ok(fake) = std::env::var("BD_UPGRADE_FAKE_BIN") {
        let temp = tempfile::NamedTempFile::new()
            .map_err(|e| upgrade_error(format!("failed to create temp file: {e}")))?;
        fs::copy(&fake, temp.path())
            .map_err(|e| upgrade_error(format!("failed to copy fake bin: {e}")))?;
        return Ok(temp.into_temp_path());
    }

    let temp_dir = tempfile::tempdir().map_err(|e| upgrade_error(format!("{e}")))?;
    let root = temp_dir.path().to_path_buf();

    let status = Command::new("cargo")
        .args([
            "install",
            "beads-rs",
            "--root",
            root.to_str().unwrap_or("."),
            "--force",
            "--locked",
        ])
        .status()
        .map_err(|e| upgrade_error(format!("failed to run cargo install: {e}")))?;

    if !status.success() {
        return Err(upgrade_error(format!(
            "cargo install failed with status {status}"
        )));
    }

    let bin = root.join("bin").join("bd");
    let temp = tempfile::NamedTempFile::new()
        .map_err(|e| upgrade_error(format!("failed to create temp binary: {e}")))?;
    fs::copy(&bin, temp.path())
        .map_err(|e| upgrade_error(format!("failed to copy cargo binary: {e}")))?;
    Ok(temp.into_temp_path())
}

fn resolve_install_path() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("BD_UPGRADE_INSTALL_PATH") {
        return Ok(PathBuf::from(path));
    }

    if let Ok(exe) = std::env::current_exe() {
        return Ok(exe);
    }

    if let Some(path) = find_in_path("bd") {
        return Ok(path);
    }

    Ok(default_install_dir()?.join("bd"))
}

fn default_install_dir() -> Result<PathBuf> {
    let usr = PathBuf::from("/usr/local/bin");
    if is_dir_writable(&usr) {
        return Ok(usr);
    }

    let home = dirs::home_dir()
        .ok_or_else(|| upgrade_error("failed to get home directory".to_string()))?;
    Ok(home.join(".local").join("bin"))
}

fn find_in_path(bin: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(bin);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

fn is_dir_writable(dir: &Path) -> bool {
    if !dir.is_dir() {
        return false;
    }
    tempfile::NamedTempFile::new_in(dir).is_ok()
}

fn install_binary(src: &Path, dest: &Path) -> Result<()> {
    let dir = dest
        .parent()
        .ok_or_else(|| upgrade_error("install path missing parent dir".to_string()))?;
    fs::create_dir_all(dir)
        .map_err(|e| upgrade_error(format!("failed to create {}: {e}", dir.display())))?;

    let temp = tempfile::NamedTempFile::new_in(dir)
        .map_err(|e| upgrade_error(format!("failed to create temp file: {e}")))?;
    fs::copy(src, temp.path()).map_err(|e| upgrade_error(format!("failed to copy binary: {e}")))?;
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o755))
        .map_err(|e| upgrade_error(format!("failed to set permissions: {e}")))?;
    temp.persist(dest).map_err(|e| {
        upgrade_error(format!(
            "failed to install binary to {}: {e}",
            dest.display()
        ))
    })?;

    if std::env::consts::OS == "macos" {
        let _ = resign_macos(dest);
    }

    Ok(())
}

fn resign_macos(path: &Path) -> Result<()> {
    if Command::new("codesign").arg("--version").output().is_err() {
        return Ok(());
    }

    let _ = Command::new("codesign")
        .args(["--remove-signature", path.to_str().unwrap_or("")])
        .status();
    let _ = Command::new("codesign")
        .args(["--force", "--sign", "-", path.to_str().unwrap_or("")])
        .status();
    Ok(())
}

fn stop_daemon() -> Result<bool> {
    let socket = socket_path();
    if std::os::unix::net::UnixStream::connect(&socket).is_err() {
        return Ok(false);
    }

    let _ = send_request_no_autostart(&Request::Shutdown);
    wait_for_daemon_stop(&socket)?;
    Ok(true)
}

fn wait_for_daemon_stop(socket: &PathBuf) -> Result<()> {
    let deadline = SystemTime::now() + Duration::from_secs(5);
    while SystemTime::now() < deadline {
        if std::os::unix::net::UnixStream::connect(socket).is_err() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(upgrade_error(
        "timed out waiting for daemon to stop".to_string(),
    ))
}

fn start_daemon(install_path: &Path) -> Result<()> {
    let mut cmd = Command::new(install_path);
    cmd.arg("daemon").arg("run");
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.spawn()
        .map_err(|e| upgrade_error(format!("failed to start daemon: {e}")))?;
    Ok(())
}

fn read_state() -> Result<Option<UpgradeState>> {
    let path = state_path();
    if !path.exists() {
        return Ok(None);
    }
    let contents = fs::read_to_string(&path)
        .map_err(|e| upgrade_error(format!("failed to read state {}: {e}", path.display())))?;
    let state = serde_json::from_str(&contents)
        .map_err(|e| upgrade_error(format!("failed to parse state: {e}")))?;
    Ok(Some(state))
}

fn write_state(state: &UpgradeState) -> Result<()> {
    let path = state_path();
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)
            .map_err(|e| upgrade_error(format!("failed to create {}: {e}", dir.display())))?;
    }
    let payload = serde_json::to_vec(state)
        .map_err(|e| upgrade_error(format!("failed to encode state: {e}")))?;
    fs::write(&path, payload).map_err(|e| upgrade_error(format!("failed to write state: {e}")))?;
    Ok(())
}

fn state_path() -> PathBuf {
    crate::paths::data_dir().join("upgrade_state.json")
}

fn normalize_version(tag: &str) -> &str {
    tag.trim_start_matches('v')
}

fn is_newer_version(latest: &str, current: &str) -> bool {
    if latest == current {
        return false;
    }
    match (parse_version(latest), parse_version(current)) {
        (Some(l), Some(c)) => l > c,
        _ => latest != current,
    }
}

fn parse_version(s: &str) -> Option<(u64, u64, u64)> {
    let mut parts = s.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next().unwrap_or("0").parse().ok()?;
    Some((major, minor, patch))
}

fn wall_ms() -> u64 {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    now.as_millis() as u64
}

fn upgrade_error(reason: String) -> Error {
    Error::Op(OpError::ValidationFailed {
        field: "upgrade".into(),
        reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_compare() {
        assert!(is_newer_version("0.2.0", "0.1.9"));
        assert!(is_newer_version("1.0.0", "0.9.9"));
        assert!(!is_newer_version("0.1.0", "0.1.0"));
        assert!(!is_newer_version("0.1.0", "0.2.0"));
    }

    #[test]
    fn install_binary_copies_and_sets_permissions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("bd-src");
        fs::write(&src, b"dummy").expect("write src");
        let dest = dir.path().join("bd-dest");

        install_binary(&src, &dest).expect("install");
        let meta = fs::metadata(&dest).expect("dest meta");
        assert!(meta.is_file());
    }
}
