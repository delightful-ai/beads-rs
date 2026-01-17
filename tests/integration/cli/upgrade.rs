//! Integration tests for `bd upgrade`.

use assert_cmd::Command;
use flate2::Compression;
use flate2::write::GzEncoder;
use predicates::str::contains;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use tar::Builder;
use tempfile::TempDir;

fn bd_cmd() -> Command {
    assert_cmd::cargo::cargo_bin_cmd!("bd")
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

fn write_archive(dir: &Path, asset_name: &str, payload: &[u8]) -> PathBuf {
    let path = dir.join(asset_name);
    let file = fs::File::create(&path).expect("create archive");
    let mut encoder = GzEncoder::new(file, Compression::default());
    {
        let mut builder = Builder::new(&mut encoder);
        let mut header = tar::Header::new_gnu();
        header.set_size(payload.len() as u64);
        header.set_mode(0o755);
        header.set_cksum();
        builder
            .append_data(&mut header, "bd", payload)
            .expect("append bd");
        builder.finish().expect("finish tar");
    }
    encoder.finish().expect("finish gzip");
    path
}

fn sha256_file_hex(path: &Path) -> String {
    let mut file = fs::File::open(path).expect("open file");
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let read = file.read(&mut buf).expect("read file");
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    hex::encode(hasher.finalize())
}

fn write_release_json(path: &Path, asset_name: &str, checksum_name: &str) {
    let body = format!(
        r#"{{"tag_name":"99.0.0","assets":[{{"name":"{asset_name}","browser_download_url":"https://example.com/{asset_name}"}},{{"name":"{checksum_name}","browser_download_url":"https://example.com/{checksum_name}"}}]}}"#
    );
    fs::write(path, body).expect("write release json");
}

#[test]
fn upgrade_installs_fake_binary() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("config");
    let data_dir = temp.path().join("data");
    let install_path = temp.path().join("bin").join("bd");

    fs::create_dir_all(install_path.parent().unwrap()).expect("create bin dir");

    let fake_bin = temp.path().join("bd-fake");
    fs::write(&fake_bin, b"fake-binary").expect("write fake bin");

    let release_json = temp.path().join("release.json");
    let release_body = r#"{"tag_name":"99.0.0","assets":[]}"#;
    fs::write(&release_json, release_body).expect("write release json");

    let mut cmd = bd_cmd();
    cmd.args(["upgrade", "--json"]);
    cmd.env("BD_UPGRADE_FAKE_BIN", &fake_bin);
    cmd.env("BD_UPGRADE_INSTALL_PATH", &install_path);
    cmd.env("BD_UPGRADE_RELEASE_JSON", &release_json);
    cmd.env("BD_UPGRADE_SKIP_DAEMON", "1");
    cmd.env("BD_CONFIG_DIR", &config_dir);
    cmd.env("BD_DATA_DIR", &data_dir);

    let output = cmd.assert().success().get_output().stdout.clone();
    let value: serde_json::Value = serde_json::from_slice(&output).expect("parse json output");
    assert!(value["updated"].as_bool().unwrap_or(false));

    let installed = fs::read(&install_path).expect("read installed bin");
    assert_eq!(installed, b"fake-binary");
}

#[test]
fn upgrade_verifies_checksum() {
    let Some(platform) = detect_platform() else {
        eprintln!("skipping checksum test on unsupported platform");
        return;
    };

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("config");
    let data_dir = temp.path().join("data");
    let asset_dir = temp.path().join("assets");
    let install_path = temp.path().join("bin").join("bd");
    fs::create_dir_all(&asset_dir).expect("create asset dir");

    let payload = b"prebuilt-binary";
    let asset_name = format!("beads-rs-{platform}.tar.gz");
    let archive_path = write_archive(&asset_dir, &asset_name, payload);
    let checksum = sha256_file_hex(&archive_path);
    let checksum_name = format!("{asset_name}.sha256");
    fs::write(
        asset_dir.join(&checksum_name),
        format!("{checksum}  {asset_name}\n"),
    )
    .expect("write checksum");

    let release_json = temp.path().join("release.json");
    write_release_json(&release_json, &asset_name, &checksum_name);

    let mut cmd = bd_cmd();
    cmd.args(["upgrade", "--json"]);
    cmd.env("BD_UPGRADE_RELEASE_JSON", &release_json);
    cmd.env("BD_UPGRADE_ASSET_DIR", &asset_dir);
    cmd.env("BD_UPGRADE_INSTALL_PATH", &install_path);
    cmd.env("BD_UPGRADE_SKIP_DAEMON", "1");
    cmd.env("BD_CONFIG_DIR", &config_dir);
    cmd.env("BD_DATA_DIR", &data_dir);

    let output = cmd.assert().success().get_output().stdout.clone();
    let value: serde_json::Value = serde_json::from_slice(&output).expect("parse json output");
    assert!(value["updated"].as_bool().unwrap_or(false));

    let installed = fs::read(&install_path).expect("read installed bin");
    assert_eq!(installed, payload);
}

#[test]
fn upgrade_rejects_checksum_mismatch() {
    let Some(platform) = detect_platform() else {
        eprintln!("skipping checksum mismatch test on unsupported platform");
        return;
    };

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("config");
    let data_dir = temp.path().join("data");
    let asset_dir = temp.path().join("assets");
    let install_path = temp.path().join("bin").join("bd");
    fs::create_dir_all(&asset_dir).expect("create asset dir");

    let payload = b"prebuilt-binary";
    let asset_name = format!("beads-rs-{platform}.tar.gz");
    let archive_path = write_archive(&asset_dir, &asset_name, payload);
    let _checksum = sha256_file_hex(&archive_path);
    let checksum_name = format!("{asset_name}.sha256");
    fs::write(
        asset_dir.join(&checksum_name),
        format!("{}  {asset_name}\n", "0".repeat(64)),
    )
    .expect("write checksum");

    let release_json = temp.path().join("release.json");
    write_release_json(&release_json, &asset_name, &checksum_name);

    let mut cmd = bd_cmd();
    cmd.args(["upgrade", "--json"]);
    cmd.env("BD_UPGRADE_RELEASE_JSON", &release_json);
    cmd.env("BD_UPGRADE_ASSET_DIR", &asset_dir);
    cmd.env("BD_UPGRADE_INSTALL_PATH", &install_path);
    cmd.env("BD_UPGRADE_SKIP_DAEMON", "1");
    cmd.env("BD_CONFIG_DIR", &config_dir);
    cmd.env("BD_DATA_DIR", &data_dir);

    cmd.assert().failure().stderr(contains("checksum mismatch"));
    assert!(!install_path.exists());
}
