//! Integration tests for `bd upgrade`.

use assert_cmd::Command;
use std::fs;
use tempfile::TempDir;

fn bd_cmd() -> Command {
    assert_cmd::cargo::cargo_bin_cmd!("bd")
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
