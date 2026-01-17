#![allow(dead_code)]

use std::collections::BTreeSet;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::time::{Duration, Instant};

use assert_cmd::Command;
use tempfile::TempDir;
use uuid::Uuid;

use beads_rs::StoreId;
use beads_rs::api::{AdminStatusOutput, QueryResult};
use beads_rs::config::{Config, ReplicationPeerConfig};
use beads_rs::core::{
    NamespaceId, ReplicaEntry, ReplicaId, ReplicaRole, ReplicaRoster, StoreMeta, Watermarks,
};
use beads_rs::daemon::ipc::{IpcClient, ReadConsistency, Request, Response, ResponsePayload};

use super::daemon_runtime::{crash_daemon, shutdown_daemon};
use super::tailnet_proxy::{TailnetProfile, TailnetProxy};

pub type FaultProfile = TailnetProfile;

#[derive(Clone, Debug)]
pub struct ReplRigOptions {
    pub fault_profile: Option<FaultProfile>,
    pub fault_profile_by_link: Option<Vec<Vec<Option<FaultProfile>>>>,
    pub seed: u64,
    pub use_store_id_override: bool,
    pub dead_ms: Option<u64>,
}

impl Default for ReplRigOptions {
    fn default() -> Self {
        Self {
            fault_profile: None,
            fault_profile_by_link: None,
            seed: 42,
            use_store_id_override: true,
            dead_ms: None,
        }
    }
}

pub struct ReplRig {
    _root: Option<TempDir>,
    store_id: StoreId,
    nodes: Vec<Node>,
    _proxies: Vec<TailnetProxy>,
}

impl ReplRig {
    pub fn new(node_count: usize, options: ReplRigOptions) -> Self {
        assert!(node_count > 0, "node_count must be > 0");

        let tmp_root = ensure_tmp_root();
        let root = TempDir::new_in(&tmp_root).expect("temp root");
        let keep_tmp = std::env::var("BD_TEST_KEEP_TMP").is_ok();
        let (root_path, root_guard): (PathBuf, Option<TempDir>) = if keep_tmp {
            let path = root.keep();
            (path, None)
        } else {
            (root.path().to_path_buf(), Some(root))
        };
        let remote_dir = root_path.join("remote.git");
        fs::create_dir_all(&remote_dir).expect("create remote dir");
        run_git(&["init", "--bare"], &remote_dir).expect("git init --bare");

        let store_id_override = if options.use_store_id_override {
            Some(StoreId::new(Uuid::new_v4()))
        } else {
            None
        };
        let mut resolved_store_id = store_id_override;
        let mut nodes = Vec::with_capacity(node_count);
        for idx in 0..node_count {
            let seed = build_node(&root_path, idx, &remote_dir);
            let (store_id, replica_id) = bootstrap_replica(&seed, store_id_override);
            if let Some(existing) = resolved_store_id {
                assert_eq!(
                    existing, store_id,
                    "store id mismatch: expected {existing} got {store_id}"
                );
            } else {
                resolved_store_id = Some(store_id);
            }
            nodes.push(Node::new(seed, store_id_override, replica_id));
        }
        let store_id = resolved_store_id.expect("store id resolved");

        let roster_entries = build_roster_entries(&nodes);
        for node in &nodes {
            write_replica_roster(&node.data_dir, store_id, &roster_entries)
                .expect("write replica roster");
        }

        let (link_addrs, proxy_specs) = plan_links(&nodes, &options);
        for (idx, node) in nodes.iter().enumerate() {
            let mut peers = Vec::new();
            for (peer_idx, peer) in nodes.iter().enumerate() {
                if idx == peer_idx {
                    continue;
                }
                let addr = link_addrs[idx][peer_idx]
                    .as_ref()
                    .expect("link addr")
                    .clone();
                peers.push((peer.replica_id, addr));
            }
            write_replication_config(&node.repo_dir, &node.listen_addr, &peers)
                .expect("write replication config");
            write_replication_user_config(
                &node.config_dir,
                &node.listen_addr,
                &peers,
                options.dead_ms,
            )
            .expect("write user replication config");
        }

        for node in &nodes {
            shutdown_daemon(&node.runtime_dir);
            node.start_daemon();
        }

        let proxies = spawn_proxies(proxy_specs);

        Self {
            _root: root_guard,
            store_id,
            nodes,
            _proxies: proxies,
        }
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub fn node(&self, idx: usize) -> &Node {
        &self.nodes[idx]
    }

    pub fn store_id(&self) -> StoreId {
        self.store_id
    }

    pub fn create_issue(&self, idx: usize, title: &str) -> String {
        self.node(idx).create_issue(title)
    }

    pub fn wait_for_show(&self, idx: usize, issue_id: &str, timeout: Duration) {
        self.node(idx).wait_for_show(issue_id, timeout)
    }

    pub fn admin_status(&self, idx: usize) -> AdminStatusOutput {
        self.node(idx).admin_status()
    }

    pub fn crash_node(&self, idx: usize) {
        crash_daemon(&self.nodes[idx].runtime_dir);
    }

    pub fn restart_node(&self, idx: usize) {
        self.nodes[idx].unlock_store(self.store_id);
        self.nodes[idx].start_daemon();
    }

    pub fn assert_converged(&self, namespaces: &[NamespaceId], timeout: Duration) {
        let ok = poll_until(timeout, || self.converged(namespaces));
        if ok {
            return;
        }
        let statuses: Vec<AdminStatusOutput> =
            self.nodes.iter().map(|node| node.admin_status()).collect();
        panic!("replication did not converge: {statuses:?}");
    }

    pub fn assert_peers_seen(&self, timeout: Duration) {
        let ok = poll_until(timeout, || self.peers_seen());
        if ok {
            return;
        }
        let statuses: Vec<AdminStatusOutput> =
            self.nodes.iter().map(|node| node.admin_status()).collect();
        panic!("replication peers not observed: {statuses:?}");
    }

    fn converged(&self, namespaces: &[NamespaceId]) -> bool {
        if self.nodes.len() < 2 {
            return true;
        }
        let statuses: Vec<AdminStatusOutput> =
            self.nodes.iter().map(|node| node.admin_status()).collect();
        let base = &statuses[0];
        for status in &statuses[1..] {
            if status.store_id != base.store_id {
                return false;
            }
            for namespace in namespaces {
                if !watermarks_equal_for_namespace(
                    &base.watermarks_applied,
                    &status.watermarks_applied,
                    namespace,
                ) {
                    return false;
                }
                if !watermarks_equal_for_namespace(
                    &base.watermarks_durable,
                    &status.watermarks_durable,
                    namespace,
                ) {
                    return false;
                }
            }
        }
        true
    }

    fn peers_seen(&self) -> bool {
        if self.nodes.len() < 2 {
            return true;
        }
        let expected: BTreeSet<ReplicaId> = self.nodes.iter().map(|node| node.replica_id).collect();
        for node in &self.nodes {
            let status = node.admin_status();
            let seen: BTreeSet<ReplicaId> = status
                .replica_liveness
                .iter()
                .map(|row| row.replica_id)
                .collect();
            for peer in &expected {
                if *peer == status.replica_id {
                    continue;
                }
                if !seen.contains(peer) {
                    return false;
                }
            }
        }
        true
    }
}

impl Drop for ReplRig {
    fn drop(&mut self) {
        for node in &self.nodes {
            shutdown_daemon(&node.runtime_dir);
        }
    }
}

#[derive(Debug)]
pub struct Node {
    repo_dir: PathBuf,
    runtime_dir: PathBuf,
    data_dir: PathBuf,
    config_dir: PathBuf,
    listen_addr: String,
    store_id_override: Option<StoreId>,
    replica_id: ReplicaId,
}

impl Node {
    fn new(seed: NodeSeed, store_id_override: Option<StoreId>, replica_id: ReplicaId) -> Self {
        Self {
            repo_dir: seed.repo_dir,
            runtime_dir: seed.runtime_dir,
            data_dir: seed.data_dir,
            config_dir: seed.config_dir,
            listen_addr: seed.listen_addr,
            store_id_override,
            replica_id,
        }
    }

    pub fn replica_id(&self) -> ReplicaId {
        self.replica_id
    }

    pub fn repo_dir(&self) -> &Path {
        &self.repo_dir
    }

    pub fn runtime_dir(&self) -> &Path {
        &self.runtime_dir
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    pub fn listen_addr(&self) -> &str {
        &self.listen_addr
    }

    fn bd_cmd(&self) -> Command {
        let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("bd");
        cmd.current_dir(&self.repo_dir);
        cmd.env("XDG_RUNTIME_DIR", &self.runtime_dir);
        cmd.env("BD_DATA_DIR", &self.data_dir);
        if let Some(store_id) = self.store_id_override {
            cmd.env("BD_STORE_ID", store_id.to_string());
        }
        cmd.env("BD_NO_AUTO_UPGRADE", "1");
        cmd.env("XDG_CONFIG_HOME", &self.config_dir);
        cmd.env("BD_TESTING", "1");
        cmd
    }

    fn unlock_store(&self, store_id: StoreId) {
        let store_id = store_id.to_string();
        self.bd_cmd()
            .args(["store", "unlock", "--store-id", store_id.as_str()])
            .assert()
            .success();
    }

    fn start_daemon(&self) {
        self.bd_cmd().args(["status"]).assert().success();
    }

    pub fn create_issue(&self, title: &str) -> String {
        let output = self
            .bd_cmd()
            .args(["--json=true", "create", title])
            .output()
            .expect("bd create");
        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            panic!("bd create failed: stdout={stdout} stderr={stderr}");
        }
        let payload: ResponsePayload =
            serde_json::from_slice(&output.stdout).expect("parse create payload");
        match payload {
            ResponsePayload::Op(op) => op.issue.expect("created issue").id,
            ResponsePayload::Query(QueryResult::Issue(issue)) => issue.id,
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    pub fn wait_for_show(&self, issue_id: &str, timeout: Duration) {
        let ok = poll_until(timeout, || {
            let output = self
                .bd_cmd()
                .args(["--json=true", "show", issue_id])
                .output()
                .expect("bd show");
            output.status.success()
        });
        assert!(ok, "issue {issue_id} failed to replicate");
    }

    pub fn admin_status(&self) -> AdminStatusOutput {
        let client = IpcClient::for_runtime_dir(&self.runtime_dir).with_autostart(false);
        let request = Request::AdminStatus {
            repo: self.repo_dir.clone(),
            read: ReadConsistency::default(),
        };
        let response = client
            .send_request_no_autostart(&request)
            .expect("admin status");
        match response {
            Response::Ok { ok } => match ok {
                ResponsePayload::Query(QueryResult::AdminStatus(status)) => status,
                other => panic!("unexpected admin status payload: {other:?}"),
            },
            Response::Err { err } => panic!("admin status error: {err:?}"),
        }
    }
}

struct NodeSeed {
    repo_dir: PathBuf,
    runtime_dir: PathBuf,
    data_dir: PathBuf,
    config_dir: PathBuf,
    listen_addr: String,
}

impl NodeSeed {
    fn bd_cmd(&self, store_id_override: Option<StoreId>) -> Command {
        let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("bd");
        cmd.current_dir(&self.repo_dir);
        cmd.env("XDG_RUNTIME_DIR", &self.runtime_dir);
        cmd.env("BD_DATA_DIR", &self.data_dir);
        if let Some(store_id) = store_id_override {
            cmd.env("BD_STORE_ID", store_id.to_string());
        }
        cmd.env("BD_NO_AUTO_UPGRADE", "1");
        cmd.env("XDG_CONFIG_HOME", &self.config_dir);
        cmd.env("BD_TESTING", "1");
        cmd
    }
}

fn bootstrap_replica(node: &NodeSeed, store_id_override: Option<StoreId>) -> (StoreId, ReplicaId) {
    node.bd_cmd(store_id_override)
        .args(["init"])
        .assert()
        .success();
    let meta = read_store_meta(&node.data_dir, store_id_override);
    shutdown_daemon(&node.runtime_dir);
    (meta.store_id(), meta.replica_id)
}

fn read_store_meta(data_dir: &Path, store_id_override: Option<StoreId>) -> StoreMeta {
    let stores_dir = data_dir.join("stores");
    let mut meta_path: Option<PathBuf> = None;
    let ok = poll_until(Duration::from_secs(2), || {
        if meta_path.is_some() {
            return true;
        }
        meta_path = match store_id_override {
            Some(store_id) => {
                let path = stores_dir.join(store_id.to_string()).join("meta.json");
                path.exists().then_some(path)
            }
            None => discover_store_meta_path(&stores_dir),
        };
        meta_path.is_some()
    });
    assert!(ok, "store meta not written under {stores_dir:?}");
    let meta_path = meta_path.expect("store meta path");
    let raw = fs::read_to_string(&meta_path).expect("read meta");
    let meta: StoreMeta = serde_json::from_str(&raw).expect("parse meta");
    if let Some(expected) = store_id_override {
        assert_eq!(
            meta.store_id(),
            expected,
            "store id mismatch: expected {expected} got {}",
            meta.store_id()
        );
    }
    meta
}

fn discover_store_meta_path(stores_dir: &Path) -> Option<PathBuf> {
    let entries = fs::read_dir(stores_dir).ok()?;
    let mut store_dirs = Vec::new();
    for entry in entries {
        let entry = entry.ok()?;
        if entry.file_type().ok()?.is_dir() {
            store_dirs.push(entry.path());
        }
    }
    if store_dirs.len() != 1 {
        return None;
    }
    let meta_path = store_dirs[0].join("meta.json");
    meta_path.exists().then_some(meta_path)
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

fn write_replication_user_config(
    config_dir: &Path,
    listen_addr: &str,
    peers: &[(ReplicaId, String)],
    dead_ms: Option<u64>,
) -> Result<(), String> {
    let mut config = Config::default();
    config.replication.listen_addr = listen_addr.to_string();
    config.replication.backoff_base_ms = 50;
    config.replication.backoff_max_ms = 500;
    config.replication.max_connections = Some(8);
    config.replication.peers = peers
        .iter()
        .map(|(replica_id, addr)| ReplicationPeerConfig {
            replica_id: *replica_id,
            addr: addr.clone(),
            role: Some(ReplicaRole::Peer),
            allowed_namespaces: Some(vec![NamespaceId::core()]),
        })
        .collect();
    if let Some(dead_ms) = dead_ms {
        config.limits.dead_ms = dead_ms;
    }

    let config_path = config_dir.join("beads-rs").join("config.toml");
    beads_rs::config::write_config(&config_path, &config)
        .map_err(|err| format!("write config.toml failed: {err}"))?;
    Ok(())
}

fn build_roster_entries(nodes: &[Node]) -> Vec<ReplicaEntry> {
    nodes
        .iter()
        .enumerate()
        .map(|(idx, node)| ReplicaEntry {
            replica_id: node.replica_id,
            name: format!("node-{idx}"),
            role: ReplicaRole::Peer,
            durability_eligible: true,
            allowed_namespaces: Some(vec![NamespaceId::core()]),
            expire_after_ms: None,
        })
        .collect()
}

fn write_replica_roster(
    data_dir: &Path,
    store_id: StoreId,
    entries: &[ReplicaEntry],
) -> Result<(), String> {
    let roster = ReplicaRoster {
        replicas: entries.to_vec(),
    };
    let raw = toml::to_string(&roster).map_err(|err| format!("serialize roster failed: {err}"))?;
    let store_dir = data_dir.join("stores").join(store_id.to_string());
    fs::write(store_dir.join("replicas.toml"), raw)
        .map_err(|err| format!("write replicas.toml failed: {err}"))?;
    Ok(())
}

fn build_node(root: &Path, idx: usize, remote_dir: &Path) -> NodeSeed {
    let base = root.join(format!("node-{idx}"));
    let repo_dir = base.join("repo");
    let runtime_dir = base.join("runtime");
    let data_dir = base.join("data");
    let config_dir = base.join("config");
    fs::create_dir_all(&repo_dir).expect("create repo dir");
    fs::create_dir_all(&runtime_dir).expect("create runtime dir");
    fs::create_dir_all(&data_dir).expect("create data dir");
    fs::create_dir_all(&config_dir).expect("create config dir");
    init_git_repo(&repo_dir, remote_dir).expect("init git repo");
    NodeSeed {
        repo_dir,
        runtime_dir,
        data_dir,
        config_dir,
        listen_addr: format!("127.0.0.1:{}", pick_port()),
    }
}

fn plan_links(
    nodes: &[Node],
    options: &ReplRigOptions,
) -> (Vec<Vec<Option<String>>>, Vec<ProxySpec>) {
    let node_count = nodes.len();
    let mut link_addrs = vec![vec![None; node_count]; node_count];
    let mut proxies = Vec::new();
    if let Some(matrix) = options.fault_profile_by_link.as_ref() {
        assert_eq!(
            matrix.len(),
            node_count,
            "fault profile matrix size mismatch"
        );
        for row in matrix {
            assert_eq!(
                row.len(),
                node_count,
                "fault profile matrix size mismatch"
            );
        }
    }
    for (from, _) in nodes.iter().enumerate() {
        for (to, target) in nodes.iter().enumerate() {
            if from == to {
                continue;
            }
            let profile = if let Some(matrix) = options.fault_profile_by_link.as_ref() {
                matrix
                    .get(from)
                    .and_then(|row| row.get(to))
                    .cloned()
                    .flatten()
            } else {
                options.fault_profile.clone()
            };
            let addr = if let Some(profile) = profile {
                let listen_addr = format!("127.0.0.1:{}", pick_port());
                let seed = link_seed(options.seed, from, to);
                proxies.push(ProxySpec {
                    listen_addr: listen_addr.clone(),
                    upstream_addr: target.listen_addr.clone(),
                    seed,
                    profile,
                });
                listen_addr
            } else {
                target.listen_addr.clone()
            };
            link_addrs[from][to] = Some(addr);
        }
    }
    (link_addrs, proxies)
}

fn link_seed(seed: u64, from: usize, to: usize) -> u64 {
    seed ^ ((from as u64) << 32) ^ (to as u64) ^ 0x9E37_79B9_7F4A_7C15
}

fn spawn_proxies(specs: Vec<ProxySpec>) -> Vec<TailnetProxy> {
    specs
        .into_iter()
        .map(|spec| {
            TailnetProxy::spawn_with_profile(
                spec.listen_addr,
                spec.upstream_addr,
                spec.seed,
                spec.profile,
            )
        })
        .collect()
}

struct ProxySpec {
    listen_addr: String,
    upstream_addr: String,
    seed: u64,
    profile: FaultProfile,
}

fn watermarks_equal_for_namespace<K: PartialEq>(
    left: &Watermarks<K>,
    right: &Watermarks<K>,
    namespace: &NamespaceId,
) -> bool {
    let mut origins = BTreeSet::new();
    origins.extend(left.origins(namespace).map(|(origin, _)| *origin));
    origins.extend(right.origins(namespace).map(|(origin, _)| *origin));
    origins
        .into_iter()
        .all(|origin| left.get(namespace, &origin) == right.get(namespace, &origin))
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
