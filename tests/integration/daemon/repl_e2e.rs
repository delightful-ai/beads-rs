#![cfg(feature = "slow-tests")]

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::time::{Duration, Instant};

use assert_cmd::Command;
use tempfile::TempDir;
use uuid::Uuid;

use beads_rs::StoreId;
use beads_rs::core::{NamespaceId, ReplicaId, StoreMeta};

use crate::fixtures::daemon_runtime::shutdown_daemon;

struct NodeFixture {
    repo_dir: PathBuf,
    runtime_dir: PathBuf,
    data_dir: PathBuf,
    config_dir: PathBuf,
    listen_addr: String,
    replica_id: Option<ReplicaId>,
}

impl NodeFixture {
    fn bd_cmd(&self, store_id: StoreId) -> Command {
        let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("bd");
        cmd.current_dir(&self.repo_dir);
        cmd.env("XDG_RUNTIME_DIR", &self.runtime_dir);
        cmd.env("BD_DATA_DIR", &self.data_dir);
        cmd.env("BD_STORE_ID", store_id.to_string());
        cmd.env("BD_NO_AUTO_UPGRADE", "1");
        cmd.env("XDG_CONFIG_HOME", &self.config_dir);
        cmd
    }
}

impl Drop for NodeFixture {
    fn drop(&mut self) {
        shutdown_daemon(&self.runtime_dir);
    }
}

#[test]
fn repl_daemon_to_daemon_roundtrip() {
    let tmp_root = ensure_tmp_root();
    let root = TempDir::new_in(&tmp_root).expect("temp root");
    let remote_dir = root.path().join("remote.git");
    fs::create_dir_all(&remote_dir).expect("create remote dir");
    run_git(&["init", "--bare"], &remote_dir).expect("git init --bare");

    let store_id = StoreId::new(Uuid::new_v4());
    let port_a = pick_port();
    let port_b = pick_port();

    let mut node_a = build_node(root.path(), "a", &remote_dir, port_a);
    let mut node_b = build_node(root.path(), "b", &remote_dir, port_b);

    bootstrap_replica(&mut node_a, store_id);
    bootstrap_replica(&mut node_b, store_id);

    let peers_a = vec![(node_b.replica_id.unwrap(), node_b.listen_addr.clone())];
    let peers_b = vec![(node_a.replica_id.unwrap(), node_a.listen_addr.clone())];
    write_replication_config(&node_a.repo_dir, &node_a.listen_addr, &peers_a)
        .expect("write config a");
    write_replication_config(&node_b.repo_dir, &node_b.listen_addr, &peers_b)
        .expect("write config b");

    node_a.bd_cmd(store_id).args(["status"]).assert().success();
    node_b.bd_cmd(store_id).args(["status"]).assert().success();

    let id_a = create_issue(&node_a, store_id, "from-a");
    wait_for_show(&node_b, store_id, &id_a, Duration::from_secs(5));

    let id_b = create_issue(&node_b, store_id, "from-b");
    wait_for_show(&node_a, store_id, &id_b, Duration::from_secs(5));

    // Also verify we can read via list across peers.
    let output = node_a
        .bd_cmd(store_id)
        .args([
            "--json=true",
            "list",
            "--namespace",
            NamespaceId::core().as_str(),
        ])
        .output()
        .expect("bd list");
    assert!(output.status.success(), "bd list should succeed");
}

fn bootstrap_replica(node: &mut NodeFixture, store_id: StoreId) {
    node.bd_cmd(store_id).args(["init"]).assert().success();
    node.replica_id = Some(read_replica_id(&node.data_dir, store_id));
    shutdown_daemon(&node.runtime_dir);
}

fn create_issue(node: &NodeFixture, store_id: StoreId, title: &str) -> String {
    let output = node
        .bd_cmd(store_id)
        .args(["--json=true", "create", title])
        .output()
        .expect("bd create");
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("bd create failed: stdout={stdout} stderr={stderr}");
    }
    let payload: beads_rs::daemon::ipc::ResponsePayload =
        serde_json::from_slice(&output.stdout).expect("parse create payload");
    match payload {
        beads_rs::daemon::ipc::ResponsePayload::Op(op) => op.issue.expect("created issue").id,
        beads_rs::daemon::ipc::ResponsePayload::Query(beads_rs::api::QueryResult::Issue(issue)) => {
            issue.id
        }
        other => panic!("unexpected payload: {other:?}"),
    }
}

fn wait_for_show(node: &NodeFixture, store_id: StoreId, issue_id: &str, timeout: Duration) {
    let ok = poll_until(timeout, || {
        let output = node
            .bd_cmd(store_id)
            .args(["--json=true", "show", issue_id])
            .output()
            .expect("bd show");
        output.status.success()
    });
    assert!(ok, "issue {issue_id} failed to replicate");
}

fn read_replica_id(data_dir: &Path, store_id: StoreId) -> ReplicaId {
    let meta_path = data_dir
        .join("stores")
        .join(store_id.to_string())
        .join("meta.json");
    let ok = poll_until(Duration::from_secs(2), || meta_path.exists());
    assert!(ok, "store meta not written: {meta_path:?}");
    let raw = fs::read_to_string(&meta_path).expect("read meta");
    let meta: StoreMeta = serde_json::from_str(&raw).expect("parse meta");
    meta.replica_id
}

fn write_replication_config(
    repo_dir: &Path,
    listen_addr: &str,
    peers: &[(ReplicaId, String)],
) -> Result<(), String> {
    let mut out = String::new();
    out.push_str("[replication]\n");
    out.push_str(&format!("listen_addr = \"{listen_addr}\"\n"));
    out.push_str("backoff_base_ms = 50\n");
    out.push_str("backoff_max_ms = 500\n");
    out.push_str("max_connections = 8\n\n");
    for (replica_id, addr) in peers {
        out.push_str("[[replication.peers]]\n");
        out.push_str(&format!("replica_id = \"{}\"\n", replica_id));
        out.push_str(&format!("addr = \"{addr}\"\n"));
        out.push_str("role = \"peer\"\n");
        out.push_str("allowed_namespaces = [\"core\"]\n\n");
    }
    fs::write(repo_dir.join("beads.toml"), out)
        .map_err(|err| format!("write beads.toml failed: {err}"))?;
    Ok(())
}

fn build_node(root: &Path, suffix: &str, remote_dir: &Path, port: u16) -> NodeFixture {
    let base = root.join(format!("node-{suffix}"));
    let repo_dir = base.join("repo");
    let runtime_dir = base.join("runtime");
    let data_dir = base.join("data");
    let config_dir = base.join("config");
    fs::create_dir_all(&repo_dir).expect("create repo dir");
    fs::create_dir_all(&runtime_dir).expect("create runtime dir");
    fs::create_dir_all(&data_dir).expect("create data dir");
    fs::create_dir_all(&config_dir).expect("create config dir");
    init_git_repo(&repo_dir, remote_dir).expect("init git repo");
    NodeFixture {
        repo_dir,
        runtime_dir,
        data_dir,
        config_dir,
        listen_addr: format!("127.0.0.1:{port}"),
        replica_id: None,
    }
}

fn ensure_tmp_root() -> PathBuf {
    let root = std::env::current_dir().expect("cwd").join("tmp");
    fs::create_dir_all(&root).expect("create tmp root");
    root
}

fn pick_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind port");
    listener.local_addr().expect("local addr").port()
}

fn init_git_repo(repo_dir: &Path, remote_dir: &Path) -> Result<(), String> {
    run_git(&["init"], repo_dir)?;
    run_git(&["config", "user.email", "test@test.com"], repo_dir)?;
    run_git(&["config", "user.name", "Test"], repo_dir)?;
    let remote = remote_dir
        .to_str()
        .ok_or_else(|| format!("remote dir path invalid: {remote_dir:?}"))?;
    run_git(&["remote", "add", "origin", remote], repo_dir)?;
    Ok(())
}

fn run_git(args: &[&str], cwd: &Path) -> Result<(), String> {
    let output = StdCommand::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .map_err(|err| format!("git {:?} failed to start: {err}", args))?;
    if output.status.success() {
        return Ok(());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(format!(
        "git {:?} failed (status {}): stdout: {stdout} stderr: {stderr}",
        args, output.status
    ))
}

fn poll_until<F>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(10);
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        std::thread::sleep(backoff);
        backoff = std::cmp::min(backoff.saturating_mul(2), Duration::from_millis(100));
    }
    condition()
}
