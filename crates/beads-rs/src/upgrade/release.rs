use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct ReleaseInfo {
    pub tag_name: String,
    pub assets: Vec<ReleaseAsset>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReleaseAsset {
    pub name: String,
    pub browser_download_url: String,
}

#[derive(Debug, Error)]
pub enum UpgradeSupportError {
    #[error("{0}")]
    Operation(String),
}

type Result<T> = std::result::Result<T, UpgradeSupportError>;

pub fn fetch_latest_release() -> Result<ReleaseInfo> {
    if let Ok(path) = std::env::var("BD_UPGRADE_RELEASE_JSON") {
        let contents = fs::read_to_string(&path)
            .map_err(|e| support_error(format!("failed to read release json {}: {e}", path)))?;
        let release: ReleaseInfo = serde_json::from_str(&contents)
            .map_err(|e| support_error(format!("failed to parse release json {}: {e}", path)))?;
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
        .map_err(|e| support_error(format!("failed to fetch release info: {e}")))?;
    let mut body = String::new();
    resp.into_reader()
        .read_to_string(&mut body)
        .map_err(|e| support_error(format!("failed to read release body: {e}")))?;
    serde_json::from_str::<ReleaseInfo>(&body)
        .map_err(|e| support_error(format!("failed to parse release info: {e}")))
}

pub fn detect_platform() -> Option<&'static str> {
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

pub fn find_asset<'a>(release: &'a ReleaseInfo, platform: &str) -> Option<&'a ReleaseAsset> {
    let name = format!("beads-rs-{platform}.tar.gz");
    release.assets.iter().find(|asset| asset.name == name)
}

pub fn fetch_asset_checksum(release: &ReleaseInfo, asset: &ReleaseAsset) -> Result<Option<String>> {
    if let Ok(path) = std::env::var("BD_UPGRADE_CHECKSUMS") {
        let contents = fs::read_to_string(&path).map_err(|e| {
            support_error(format!("failed to read upgrade checksums {}: {e}", path))
        })?;
        return Ok(parse_checksum_file(&contents, &asset.name));
    }

    let checksum_name = format!("{}.sha256", asset.name);
    let checksum_asset = release
        .assets
        .iter()
        .find(|entry| entry.name == checksum_name);
    let Some(checksum_asset) = checksum_asset else {
        return Ok(None);
    };

    let contents = download_asset_text(&checksum_asset.name, &checksum_asset.browser_download_url)?;
    Ok(parse_checksum_file(&contents, &asset.name))
}

pub fn normalize_version(tag: &str) -> &str {
    tag.trim_start_matches('v')
}

pub fn is_newer_version(latest: &str, current: &str) -> bool {
    if latest == current {
        return false;
    }
    match (parse_version(latest), parse_version(current)) {
        (Some(latest_version), Some(current_version)) => latest_version > current_version,
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

fn download_asset_text(name: &str, url: &str) -> Result<String> {
    if let Ok(dir) = std::env::var("BD_UPGRADE_ASSET_DIR") {
        let src = PathBuf::from(dir).join(name);
        return fs::read_to_string(&src)
            .map_err(|e| support_error(format!("failed to read asset {}: {e}", src.display())));
    }

    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(30))
        .build();
    let resp = agent
        .get(url)
        .set("User-Agent", "beads-rs-upgrade")
        .call()
        .map_err(|e| support_error(format!("failed to download asset: {e}")))?;
    let mut body = String::new();
    resp.into_reader()
        .read_to_string(&mut body)
        .map_err(|e| support_error(format!("failed to read asset body: {e}")))?;
    Ok(body)
}

fn parse_checksum_file(contents: &str, asset_name: &str) -> Option<String> {
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let hash = parts.next()?;
        let name = parts.next();
        if hash.len() != 64 || !hash.chars().all(|ch| ch.is_ascii_hexdigit()) {
            continue;
        }
        match name {
            None => return Some(hash.to_lowercase()),
            Some(name) => {
                if Path::new(name)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(|base| base == asset_name)
                {
                    return Some(hash.to_lowercase());
                }
            }
        }
    }
    None
}

fn support_error(reason: String) -> UpgradeSupportError {
    UpgradeSupportError::Operation(reason)
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
}
