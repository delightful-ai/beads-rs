#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use assert_cmd::Command;
use rusqlite::Connection;
use tempfile::TempDir;
use uuid::Uuid;

use beads_rs::StoreId;
use beads_rs::api::{AdminStatusOutput, QueryResult};
use beads_rs::config::{Config, ReplicationPeerConfig};
use beads_rs::core::error::CliErrorCode;
use beads_rs::core::{
    Applied, BeadId, BeadType, ErrorPayload, NamespaceId, Priority, ProtocolErrorCode,
    ReplicaDurabilityRole, ReplicaEntry, ReplicaId, ReplicaRole, ReplicaRoster, StoreEpoch,
    StoreMeta, Watermarks,
};
use beads_rs::daemon::ipc::{
    CreatePayload, EmptyPayload, IdPayload, IpcClient, IpcConnection, MutationCtx, MutationMeta,
    ReadConsistency, ReadCtx, RepoCtx, Request, Response, ResponsePayload,
};
use beads_rs::daemon::wal::{SEGMENT_HEADER_PREFIX_LEN, SegmentHeader};

use super::daemon_runtime::{crash_daemon, shutdown_daemon};
use super::store_lock::unlock_store;
use super::tailnet_proxy::{TailnetProfile, TailnetProxy, TailnetTrace, TailnetTraceMode};

pub type FaultProfile = TailnetProfile;

#[derive(Clone, Debug)]
pub struct ReplRigOptions {
    pub fault_profile: Option<FaultProfile>,
    pub fault_profile_by_link: Option<Vec<Vec<Option<FaultProfile>>>>,
    pub seed: u64,
    pub use_store_id_override: bool,
    pub dead_ms: Option<u64>,
    pub keepalive_ms: Option<u64>,
    pub wal_segment_max_bytes: Option<usize>,
    pub tailnet_trace: Option<TailnetTraceConfig>,
}

#[derive(Clone, Debug)]
pub struct TailnetTraceConfig {
    pub mode: TailnetTraceMode,
    pub dir: PathBuf,
    pub timeout_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilityEligibility {
    Eligible,
    Ineligible,
}

impl DurabilityEligibility {
    fn matches(self, value: bool) -> bool {
        match self {
            DurabilityEligibility::Eligible => value,
            DurabilityEligibility::Ineligible => !value,
        }
    }
}

impl Default for ReplRigOptions {
    fn default() -> Self {
        Self {
            fault_profile: None,
            fault_profile_by_link: None,
            seed: 42,
            use_store_id_override: true,
            dead_ms: None,
            keepalive_ms: None,
            wal_segment_max_bytes: None,
            tailnet_trace: None,
        }
    }
}

pub struct ReplRig {
    _root: Option<TempDir>,
    store_id: StoreId,
    nodes: Vec<Node>,
    issue_min_seen: Arc<Mutex<BTreeMap<String, Watermarks<Applied>>>>,
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
        let issue_min_seen = Arc::new(Mutex::new(BTreeMap::new()));
        let mut seeds = Vec::with_capacity(node_count);
        for idx in 0..node_count {
            let seed = build_node(&root_path, idx, &remote_dir);
            // Write user config before init so the daemon boots with WAL limits.
            write_replication_user_config(
                &seed.config_dir,
                &seed.listen_addr,
                &[],
                options.dead_ms,
                options.keepalive_ms,
                options.wal_segment_max_bytes,
            )
            .expect("write initial user replication config");
            seeds.push(seed);
        }

        let mut nodes = Vec::with_capacity(node_count);
        for seed in seeds {
            let (store_id, replica_id) = bootstrap_replica(&seed, store_id_override);
            if let Some(existing) = resolved_store_id {
                assert_eq!(
                    existing, store_id,
                    "store id mismatch: expected {existing} got {store_id}"
                );
            } else {
                resolved_store_id = Some(store_id);
            }
            nodes.push(Node::new(
                seed,
                store_id_override,
                replica_id,
                issue_min_seen.clone(),
            ));
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
                options.keepalive_ms,
                options.wal_segment_max_bytes,
            )
            .expect("write user replication config");
        }

        for node in &nodes {
            node.start_daemon();
            node.reload_replication();
        }

        let proxies = spawn_proxies(proxy_specs);

        Self {
            _root: root_guard,
            store_id,
            nodes,
            issue_min_seen,
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
        self.nodes[idx].reset_ipc_connections();
    }

    pub fn restart_node(&self, idx: usize) {
        self.nodes[idx].unlock_store(self.store_id);
        self.nodes[idx].reset_ipc_connections();
        self.nodes[idx].start_daemon();
    }

    pub fn shutdown_node(&self, idx: usize) {
        shutdown_daemon(&self.nodes[idx].runtime_dir);
        self.nodes[idx].reset_ipc_connections();
    }

    pub fn reload_replication(&self, idx: usize) {
        self.nodes[idx].reload_replication();
    }

    pub fn overwrite_roster(&self, entries: Vec<ReplicaEntry>) {
        for node in &self.nodes {
            write_replica_roster(&node.data_dir, self.store_id, &entries)
                .expect("write replica roster");
        }
    }

    pub fn bump_store_epoch(&self) -> StoreEpoch {
        let base = read_store_meta(&self.nodes[0].data_dir, Some(self.store_id));
        for node in &self.nodes {
            let meta = read_store_meta(&node.data_dir, Some(self.store_id));
            assert_eq!(
                meta.store_epoch(),
                base.store_epoch(),
                "store_epoch mismatch before bump"
            );
        }
        let next_epoch = StoreEpoch::new(base.store_epoch().get() + 1);
        for node in &self.nodes {
            bump_store_epoch_on_disk(&node.data_dir, self.store_id, next_epoch)
                .expect("bump store epoch");
        }
        next_epoch
    }

    pub fn wait_for_durability_eligible(
        &self,
        idx: usize,
        peer: ReplicaId,
        expected: DurabilityEligibility,
        timeout: Duration,
    ) {
        let ok = poll_until(timeout, || {
            let status = self.nodes[idx].admin_status();
            status
                .replica_liveness
                .iter()
                .find(|row| row.replica_id == peer)
                .map(|row| expected.matches(row.durability_eligible))
                .unwrap_or(false)
        });
        if ok {
            return;
        }
        let status = self.nodes[idx].admin_status();
        panic!(
            "durability_eligible did not become {expected:?} for {peer} on node {idx}: {status:?}"
        );
    }

    pub fn assert_converged(&self, namespaces: &[NamespaceId], timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut statuses = match self.admin_statuses_with_read(ReadConsistency::default(), deadline)
        {
            Ok(statuses) => statuses,
            Err(err) => panic!("admin status error: {err:?}"),
        };
        loop {
            if self.converged_with_statuses(&statuses, namespaces) {
                return;
            }
            if Instant::now() >= deadline {
                break;
            }
            statuses = match self.admin_statuses_with_read(ReadConsistency::default(), deadline) {
                Ok(statuses) => statuses,
                Err(err) => panic!("admin status error: {err:?}"),
            };
        }
        if self.converged_with_statuses(&statuses, namespaces) {
            return;
        }
        panic!("replication did not converge: {statuses:?}");
    }

    pub fn assert_peers_seen(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut statuses = match self.admin_statuses_with_read(ReadConsistency::default(), deadline)
        {
            Ok(statuses) => statuses,
            Err(err) => panic!("admin status error: {err:?}"),
        };
        loop {
            if self.peers_seen_with_statuses(&statuses) {
                return;
            }
            if Instant::now() >= deadline {
                break;
            }
            statuses = match self.admin_statuses_with_read(ReadConsistency::default(), deadline) {
                Ok(statuses) => statuses,
                Err(err) => panic!("admin status error: {err:?}"),
            };
        }
        if self.peers_seen_with_statuses(&statuses) {
            return;
        }
        panic!("replication peers not observed: {statuses:?}");
    }

    pub fn assert_replication_ready(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut statuses = match self.admin_statuses_with_read(ReadConsistency::default(), deadline)
        {
            Ok(statuses) => statuses,
            Err(err) => panic!("admin status error: {err:?}"),
        };
        loop {
            if self.replication_ready_with_statuses(&statuses) {
                return;
            }
            if Instant::now() >= deadline {
                break;
            }
            statuses = match self.admin_statuses_with_read(ReadConsistency::default(), deadline) {
                Ok(statuses) => statuses,
                Err(err) => panic!("admin status error: {err:?}"),
            };
        }
        if self.replication_ready_with_statuses(&statuses) {
            return;
        }
        panic!("replication not ready: {statuses:?}");
    }

    fn admin_statuses_with_read(
        &self,
        mut read: ReadConsistency,
        deadline: Instant,
    ) -> Result<Vec<AdminStatusOutput>, ErrorPayload> {
        let wait_timeout_ms = deadline
            .saturating_duration_since(Instant::now())
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        read.wait_timeout_ms = Some(wait_timeout_ms);
        self.nodes
            .iter()
            .map(|node| node.admin_status_with_read(read.clone()))
            .collect()
    }

    fn converged_with_statuses(
        &self,
        statuses: &[AdminStatusOutput],
        namespaces: &[NamespaceId],
    ) -> bool {
        if self.nodes.len() < 2 {
            return true;
        }
        let base = match statuses.first() {
            Some(status) => status,
            None => return false,
        };
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

    fn peers_seen_with_statuses(&self, statuses: &[AdminStatusOutput]) -> bool {
        if self.nodes.len() < 2 {
            return true;
        }
        let expected: BTreeSet<ReplicaId> = self.nodes.iter().map(|node| node.replica_id).collect();
        for status in statuses {
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

    fn replication_ready_with_statuses(&self, statuses: &[AdminStatusOutput]) -> bool {
        if self.nodes.len() < 2 {
            return true;
        }
        let expected: BTreeSet<ReplicaId> = self.nodes.iter().map(|node| node.replica_id).collect();
        for status in statuses {
            for peer in &expected {
                if *peer == status.replica_id {
                    continue;
                }
                let Some(row) = status
                    .replica_liveness
                    .iter()
                    .find(|row| row.replica_id == *peer)
                else {
                    return false;
                };
                if row.last_handshake_ms == 0 {
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

pub struct Node {
    repo_dir: PathBuf,
    runtime_dir: PathBuf,
    data_dir: PathBuf,
    config_dir: PathBuf,
    listen_addr: String,
    store_id_override: Option<StoreId>,
    replica_id: ReplicaId,
    admin_conn: Mutex<Option<IpcConnection>>,
    ipc_conn: Mutex<Option<IpcConnection>>,
    issue_min_seen: Arc<Mutex<BTreeMap<String, Watermarks<Applied>>>>,
}

impl Node {
    fn new(
        seed: NodeSeed,
        store_id_override: Option<StoreId>,
        replica_id: ReplicaId,
        issue_min_seen: Arc<Mutex<BTreeMap<String, Watermarks<Applied>>>>,
    ) -> Self {
        Self {
            repo_dir: seed.repo_dir,
            runtime_dir: seed.runtime_dir,
            data_dir: seed.data_dir,
            config_dir: seed.config_dir,
            listen_addr: seed.listen_addr,
            store_id_override,
            replica_id,
            admin_conn: Mutex::new(None),
            ipc_conn: Mutex::new(None),
            issue_min_seen,
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
        unlock_store(&self.data_dir, store_id).expect("unlock store");
    }

    fn start_daemon(&self) {
        self.bd_cmd().args(["status"]).assert().success();
    }

    pub fn create_issue(&self, title: &str) -> String {
        let request = Request::Create {
            ctx: MutationCtx::new(self.repo_dir.clone(), MutationMeta::default()),
            payload: CreatePayload {
                id: None,
                parent: None,
                title: title.to_string(),
                bead_type: BeadType::Task,
                priority: Priority::default(),
                description: None,
                design: None,
                acceptance_criteria: None,
                assignee: None,
                external_ref: None,
                estimated_minutes: None,
                labels: Vec::new(),
                dependencies: Vec::new(),
            },
        };
        let response = self.send_request(&request).expect("bd create");
        match response {
            Response::Ok { ok } => match ok {
                ResponsePayload::Op(op) => {
                    let id = match op.result {
                        beads_rs::daemon::ops::OpResult::Created { id } => id,
                        other => panic!("unexpected create result: {other:?}"),
                    };
                    let id_str = id.as_str().to_string();
                    self.record_min_seen(&id_str, op.receipt.min_seen());
                    id_str
                }
                ResponsePayload::Query(QueryResult::Issue(issue)) => issue.id,
                other => panic!("unexpected create payload: {other:?}"),
            },
            Response::Err { err } => panic!("bd create failed: {err:?}"),
        }
    }

    pub fn wait_for_show(&self, issue_id: &str, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut backoff = Duration::from_millis(10);
        loop {
            let wait_timeout_ms = deadline
                .saturating_duration_since(Instant::now())
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX);
            let read = ReadConsistency {
                require_min_seen: self.min_seen_for(issue_id),
                wait_timeout_ms: Some(wait_timeout_ms),
                ..ReadConsistency::default()
            };
            let request = Request::Show {
                ctx: ReadCtx::new(self.repo_dir.clone(), read),
                payload: IdPayload {
                    id: BeadId::parse(issue_id).expect("bead id"),
                },
            };
            let response = self.send_request(&request).expect("bd show");
            match response {
                Response::Ok { ok } => match ok {
                    ResponsePayload::Query(QueryResult::Issue(issue)) => {
                        assert_eq!(issue.id, issue_id);
                        return;
                    }
                    other => panic!("unexpected show payload: {other:?}"),
                },
                Response::Err { err } => {
                    let retry = err.code == CliErrorCode::NotFound.into()
                        || err.code == ProtocolErrorCode::RequireMinSeenUnsatisfied.into()
                        || err.code == ProtocolErrorCode::RequireMinSeenTimeout.into();
                    if retry && Instant::now() < deadline {
                        std::thread::sleep(backoff);
                        backoff =
                            std::cmp::min(backoff.saturating_mul(2), Duration::from_millis(100));
                        continue;
                    }
                    panic!("issue {issue_id} failed to replicate: {err:?}");
                }
            }
        }
    }

    pub fn admin_status(&self) -> AdminStatusOutput {
        match self.admin_status_with_read(ReadConsistency::default()) {
            Ok(status) => status,
            Err(err) => panic!("admin status error: {err:?}"),
        }
    }

    pub fn reload_replication(&self) {
        let request = Request::AdminReloadReplication {
            ctx: RepoCtx::new(self.repo_dir.clone()),
            payload: EmptyPayload {},
        };
        let response = self
            .send_admin_request(&request)
            .expect("admin reload replication");
        match response {
            Response::Ok {
                ok: ResponsePayload::Query(QueryResult::AdminReloadReplication(_)),
            } => {}
            other => panic!("unexpected admin reload replication response: {other:?}"),
        }
    }

    fn reset_ipc_connections(&self) {
        *self.admin_conn.lock().expect("admin conn lock") = None;
        *self.ipc_conn.lock().expect("ipc conn lock") = None;
    }

    fn admin_status_with_read(
        &self,
        read: ReadConsistency,
    ) -> Result<AdminStatusOutput, ErrorPayload> {
        let request = Request::AdminStatus {
            ctx: ReadCtx::new(self.repo_dir.clone(), read),
            payload: EmptyPayload {},
        };
        let response = self
            .send_admin_request(&request)
            .map_err(|e| beads_rs::daemon::ipc::IntoErrorPayload::into_error_payload(e))?;
        match response {
            Response::Ok { ok } => match ok {
                ResponsePayload::Query(QueryResult::AdminStatus(status)) => Ok(status),
                other => Err(ErrorPayload::new(
                    ProtocolErrorCode::InternalError.into(),
                    format!("unexpected admin status payload: {other:?}"),
                    false,
                )),
            },
            Response::Err { err } => Err(err),
        }
    }

    fn record_min_seen(&self, issue_id: &str, min_seen: &Watermarks<Applied>) {
        let mut guard = self.issue_min_seen.lock().expect("issue min_seen lock");
        guard.insert(issue_id.to_string(), min_seen.clone());
    }

    fn min_seen_for(&self, issue_id: &str) -> Option<Watermarks<Applied>> {
        let guard = self.issue_min_seen.lock().expect("issue min_seen lock");
        guard.get(issue_id).cloned()
    }

    fn send_admin_request(
        &self,
        request: &Request,
    ) -> Result<Response, beads_rs::daemon::ipc::IpcError> {
        let mut guard = self.admin_conn.lock().expect("admin conn lock");
        if guard.is_none() {
            let client = IpcClient::for_runtime_dir(&self.runtime_dir).with_autostart(false);
            let conn = client.connect()?;
            *guard = Some(conn);
        }
        guard.as_mut().expect("admin conn").send_request(request)
    }

    fn send_request(&self, request: &Request) -> Result<Response, beads_rs::daemon::ipc::IpcError> {
        let mut guard = self.ipc_conn.lock().expect("ipc conn lock");
        if guard.is_none() {
            let client = IpcClient::for_runtime_dir(&self.runtime_dir).with_autostart(false);
            let conn = client.connect()?;
            *guard = Some(conn);
        }
        guard.as_mut().expect("ipc conn").send_request(request)
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
    keepalive_ms: Option<u64>,
    wal_segment_max_bytes: Option<usize>,
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
    if let Some(keepalive_ms) = keepalive_ms {
        config.limits.keepalive_ms = keepalive_ms;
    }
    if let Some(wal_segment_max_bytes) = wal_segment_max_bytes {
        config.limits.wal_segment_max_bytes = wal_segment_max_bytes;
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
            role: ReplicaDurabilityRole::peer(true),
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

fn bump_store_epoch_on_disk(
    data_dir: &Path,
    store_id: StoreId,
    store_epoch: StoreEpoch,
) -> Result<(), String> {
    let store_dir = data_dir.join("stores").join(store_id.to_string());
    update_store_meta_epoch(&store_dir, store_id, store_epoch)?;
    update_wal_index_epoch(&store_dir, store_epoch)?;
    update_wal_segments_epoch(&store_dir, store_id, store_epoch)?;
    Ok(())
}

fn update_store_meta_epoch(
    store_dir: &Path,
    store_id: StoreId,
    store_epoch: StoreEpoch,
) -> Result<(), String> {
    let meta_path = store_dir.join("meta.json");
    let bytes = fs::read(&meta_path).map_err(|err| format!("read meta.json failed: {err}"))?;
    let mut meta: StoreMeta =
        serde_json::from_slice(&bytes).map_err(|err| format!("parse meta.json failed: {err}"))?;
    if meta.store_id() != store_id {
        return Err(format!(
            "store id mismatch in meta.json: expected {store_id} got {}",
            meta.store_id()
        ));
    }
    meta.identity.store_epoch = store_epoch;
    let bytes =
        serde_json::to_vec(&meta).map_err(|err| format!("serialize meta.json failed: {err}"))?;
    fs::write(&meta_path, bytes).map_err(|err| format!("write meta.json failed: {err}"))?;
    Ok(())
}

fn update_wal_index_epoch(store_dir: &Path, store_epoch: StoreEpoch) -> Result<(), String> {
    let index_path = store_dir.join("index").join("wal.sqlite");
    if !index_path.exists() {
        return Ok(());
    }
    let conn =
        Connection::open(&index_path).map_err(|err| format!("open wal.sqlite failed: {err}"))?;
    let updated = conn
        .execute(
            "UPDATE meta SET value = ?1 WHERE key = 'store_epoch'",
            [store_epoch.get().to_string()],
        )
        .map_err(|err| format!("update wal.sqlite store_epoch failed: {err}"))?;
    if updated == 0 {
        return Err(format!(
            "wal.sqlite missing store_epoch meta at {index_path:?}"
        ));
    }
    Ok(())
}

fn update_wal_segments_epoch(
    store_dir: &Path,
    store_id: StoreId,
    store_epoch: StoreEpoch,
) -> Result<(), String> {
    let wal_dir = store_dir.join("wal");
    if !wal_dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(&wal_dir).map_err(|err| format!("read wal dir failed: {err}"))? {
        let entry = entry.map_err(|err| format!("read wal dir entry failed: {err}"))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        for segment in
            fs::read_dir(&path).map_err(|err| format!("read wal namespace dir failed: {err}"))?
        {
            let segment = segment.map_err(|err| format!("read wal segment entry failed: {err}"))?;
            let segment_path = segment.path();
            if !segment_path.is_file() {
                continue;
            }
            if segment_path.extension().and_then(|ext| ext.to_str()) != Some("wal") {
                continue;
            }
            rewrite_segment_epoch(&segment_path, store_id, store_epoch)?;
        }
    }
    Ok(())
}

fn rewrite_segment_epoch(
    path: &Path,
    store_id: StoreId,
    store_epoch: StoreEpoch,
) -> Result<(), String> {
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|err| format!("open segment failed: {err}"))?;
    let mut prefix = [0u8; SEGMENT_HEADER_PREFIX_LEN];
    file.read_exact(&mut prefix)
        .map_err(|err| format!("read segment header prefix failed: {err}"))?;
    let header_len = u32::from_le_bytes([
        prefix[SEGMENT_HEADER_PREFIX_LEN - 4],
        prefix[SEGMENT_HEADER_PREFIX_LEN - 3],
        prefix[SEGMENT_HEADER_PREFIX_LEN - 2],
        prefix[SEGMENT_HEADER_PREFIX_LEN - 1],
    ]) as usize;
    if header_len < SEGMENT_HEADER_PREFIX_LEN {
        return Err(format!(
            "segment header length too small ({header_len}) at {path:?}"
        ));
    }
    let mut header_bytes = vec![0u8; header_len];
    file.seek(SeekFrom::Start(0))
        .map_err(|err| format!("seek segment header failed: {err}"))?;
    file.read_exact(&mut header_bytes)
        .map_err(|err| format!("read segment header failed: {err}"))?;
    let mut header = SegmentHeader::decode(&header_bytes)
        .map_err(|err| format!("decode segment header failed: {err}"))?;
    if header.store_id != store_id {
        return Err(format!(
            "segment store id mismatch at {path:?}: expected {store_id} got {}",
            header.store_id
        ));
    }
    header.store_epoch = store_epoch;
    let encoded = header
        .encode()
        .map_err(|err| format!("encode segment header failed: {err}"))?;
    if encoded.len() != header_len {
        return Err(format!(
            "segment header length mismatch at {path:?}: expected {header_len} got {}",
            encoded.len()
        ));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|err| format!("seek segment header for write failed: {err}"))?;
    file.write_all(&encoded)
        .map_err(|err| format!("write segment header failed: {err}"))?;
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
            assert_eq!(row.len(), node_count, "fault profile matrix size mismatch");
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
            let trace = options.tailnet_trace.as_ref().map(|trace| TailnetTrace {
                mode: trace.mode,
                path: trace.dir.join(format!("trace-{from}-{to}.jsonl")),
                timeout_ms: trace.timeout_ms,
            });
            let needs_proxy = profile.is_some() || trace.is_some();
            let profile = profile.unwrap_or_else(FaultProfile::none);
            let addr = if needs_proxy {
                let listen_addr = format!("127.0.0.1:{}", pick_port());
                let seed = link_seed(options.seed, from, to);
                proxies.push(ProxySpec {
                    listen_addr: listen_addr.clone(),
                    upstream_addr: target.listen_addr.clone(),
                    seed,
                    profile,
                    trace,
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
            TailnetProxy::spawn_with_profile_and_trace(
                spec.listen_addr,
                spec.upstream_addr,
                spec.seed,
                spec.profile,
                spec.trace,
            )
        })
        .collect()
}

struct ProxySpec {
    listen_addr: String,
    upstream_addr: String,
    seed: u64,
    profile: FaultProfile,
    trace: Option<TailnetTrace>,
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
