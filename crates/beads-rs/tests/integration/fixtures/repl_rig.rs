#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusqlite::Connection;
use tempfile::TempDir;
use uuid::Uuid;

use beads_api::{AdminFingerprintMode, AdminFingerprintOutput, AdminStatusOutput, QueryResult};
use beads_core::StoreId;
use beads_core::error::CliErrorCode;
use beads_core::{
    Applied, BeadId, BeadType, ErrorPayload, IntoErrorPayload, NamespaceId, Priority,
    ProtocolErrorCode, ReplicaDurabilityRole, ReplicaEntry, ReplicaId, ReplicaRole, ReplicaRoster,
    StoreEpoch, StoreMeta, Watermarks,
};
use beads_rs::config::{Config, ReplicationPeerConfig};
use beads_surface::ipc::{
    AdminFingerprintPayload, AdminOp, CreatePayload, EmptyPayload, IdPayload, IpcClient,
    IpcConnection, IpcError, MutationCtx, MutationMeta, ReadConsistency, ReadCtx, RepoCtx, Request,
    Response, ResponsePayload,
};
use beads_surface::ops::OpResult;

use super::bd_runtime::{
    BdCommandProfile, daemon_socket_path, initialize_repo_with_client, spawn_daemon_with_paths,
    wait_for_daemon_ready,
};
use super::daemon_boundary::wal::{SEGMENT_HEADER_PREFIX_LEN, SegmentHeader};
use super::daemon_runtime::{crash_daemon, shutdown_daemon};
use super::git::{init_bare_repo, init_repo_with_origin};
use super::store_lock::unlock_store;
use super::tailnet_proxy::{TailnetProfile, TailnetProxy, TailnetTrace, TailnetTraceMode};
use super::temp;
use super::timing;
use super::wait;

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

const ADMIN_READY_POLL_TIMEOUT: Duration = Duration::from_millis(250);
const IPC_IO_TIMEOUT_FLOOR: Duration = Duration::from_secs(5);
const IPC_IO_TIMEOUT_SLACK: Duration = Duration::from_secs(2);
const IPC_IO_TIMEOUT_CEILING: Duration = Duration::from_secs(600);
const IPC_RELOAD_REPLICATION_TIMEOUT: Duration = Duration::from_secs(30);
const IPC_SEND_RETRY_ATTEMPTS: usize = 3;
const EPHEMERAL_LOOPBACK_LISTEN_ADDR: &str = "127.0.0.1:0";
const STATUS_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(10);
const STATUS_RETRY_MAX_BACKOFF: Duration = Duration::from_millis(100);
const IPC_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(20);
const IPC_RETRY_MAX_BACKOFF: Duration = Duration::from_millis(250);

fn debug_step(step: &str) {
    if std::env::var_os("BD_TEST_STEP_LOG").is_some() {
        eprintln!("STEP {step}");
    }
}

fn debug_detail(label: &str, detail: impl std::fmt::Display) {
    if std::env::var_os("BD_TEST_STEP_LOG").is_some() {
        eprintln!("STEP {label}: {detail}");
    }
}

pub struct ReplRig {
    _root: Option<TempDir>,
    store_id: StoreId,
    nodes: Vec<Node>,
    issue_min_seen: Arc<Mutex<BTreeMap<String, Watermarks<Applied>>>>,
    _proxies: Vec<TailnetProxy>,
}

#[derive(Clone, Debug)]
pub struct ReplicationReadySnapshot {
    handshakes: Vec<BTreeMap<ReplicaId, u64>>,
}

enum StatusWaitRetry {
    Pending,
    Retryable(ErrorPayload),
    Fatal(ErrorPayload),
}

enum StatusWaitFailure {
    TimedOut {
        last_statuses: Vec<AdminStatusOutput>,
        last_error: Option<ErrorPayload>,
    },
    Fatal(ErrorPayload),
}

enum FingerprintWaitFailure {
    TimedOut {
        last_fingerprints: Vec<AdminFingerprintOutput>,
        last_error: Option<ErrorPayload>,
    },
    Fatal(ErrorPayload),
}

impl ReplRig {
    pub fn new(node_count: usize, options: ReplRigOptions) -> Self {
        assert!(node_count > 0, "node_count must be > 0");
        let _phase = timing::scoped_phase_with_context(
            "fixture.repl_rig.new",
            format!("nodes={node_count}"),
        );

        let root = temp::fixture_tempdir("repl-rig");
        let keep_tmp = temp::keep_tmp_enabled();
        let (root_path, root_guard): (PathBuf, Option<TempDir>) = if keep_tmp {
            let path = root.keep();
            (path, None)
        } else {
            (root.path().to_path_buf(), Some(root))
        };
        let remote_dir = root_path.join("remote.git");
        fs::create_dir_all(&remote_dir).expect("create remote dir");
        {
            let _phase = timing::scoped_phase("fixture.repl_rig.init_bare_remote");
            init_bare_repo(&remote_dir).expect("git init --bare");
        }

        let store_id_override = if options.use_store_id_override {
            Some(StoreId::new(Uuid::new_v4()))
        } else {
            None
        };
        let mut resolved_store_id = store_id_override;
        let issue_min_seen = Arc::new(Mutex::new(BTreeMap::new()));
        let mut seeds = Vec::with_capacity(node_count);
        for idx in 0..node_count {
            let seed = {
                let _phase = timing::scoped_phase_with_context(
                    "fixture.repl_rig.build_node",
                    idx.to_string(),
                );
                build_node(&root_path, idx, &remote_dir)
            };
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
            let (store_id, replica_id, daemon_child) = {
                let _phase = timing::scoped_phase_with_context(
                    "fixture.repl_rig.bootstrap_replica",
                    seed.repo_dir.display(),
                );
                bootstrap_replica(&seed, store_id_override)
            };
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
                daemon_child,
                issue_min_seen.clone(),
            ));
        }
        let store_id = resolved_store_id.expect("store id resolved");

        let roster_entries = build_roster_entries(&nodes);
        for node in &nodes {
            write_replica_roster(&node.data_dir, store_id, &roster_entries)
                .expect("write replica roster");
        }

        for node in &mut nodes {
            {
                let _phase = timing::scoped_phase_with_context(
                    "fixture.repl_rig.start_daemon",
                    node.runtime_dir.display(),
                );
                node.start_daemon();
            }
        }

        let (link_addrs, proxy_specs) = plan_links(&nodes, &options);
        // Start proxies before publishing peer addresses so every proxied link points at a
        // listener the kernel has already bound.
        let (link_addrs, proxies) = {
            let _phase = timing::scoped_phase("fixture.repl_rig.spawn_proxies");
            spawn_proxies(link_addrs, proxy_specs)
        };
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
            write_replication_config(
                &node.repo_dir,
                &node.listen_addr,
                &peers,
                options.dead_ms,
                options.keepalive_ms,
                options.wal_segment_max_bytes,
            )
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
            {
                let _phase = timing::scoped_phase_with_context(
                    "fixture.repl_rig.reload_replication_peers",
                    node.runtime_dir.display(),
                );
                node.reload_replication_peers();
            }
        }

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
        self.nodes[idx].crash_owned_daemon();
        self.nodes[idx].reset_ipc_connections();
    }

    pub fn restart_node(&mut self, idx: usize) {
        self.nodes[idx].unlock_store(self.store_id);
        self.nodes[idx].reset_ipc_connections();
        self.nodes[idx].start_daemon();
    }

    pub fn wait_for_admin_ready(&self, idx: usize, timeout: Duration) {
        self.nodes[idx].wait_for_admin_ready(timeout);
    }

    pub fn shutdown_node(&self, idx: usize) {
        self.nodes[idx].shutdown_owned_daemon();
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
        let ok = wait::poll_until_with_phase(
            "fixture.repl_rig.wait_for_durability_eligible",
            format!("node={idx} peer={peer} expected={expected:?}"),
            timeout,
            || {
                let status = self.nodes[idx].admin_status();
                status
                    .replica_liveness
                    .iter()
                    .find(|row| row.replica_id == peer)
                    .map(|row| expected.matches(row.durability_eligible))
                    .unwrap_or(false)
            },
        );
        if ok {
            return;
        }
        let status = self.nodes[idx].admin_status();
        panic!(
            "durability_eligible did not become {expected:?} for {peer} on node {idx}: {status:?}"
        );
    }

    pub fn assert_converged(&self, namespaces: &[NamespaceId], timeout: Duration) {
        match self.wait_for_fingerprints(
            "fixture.repl_rig.assert_converged",
            format!("namespaces={namespaces:?}"),
            timeout,
            |fingerprints| self.converged_with_fingerprints(fingerprints, namespaces),
        ) {
            Ok(_) => {}
            Err(FingerprintWaitFailure::Fatal(err)) => panic!("admin fingerprint error: {err:?}"),
            Err(FingerprintWaitFailure::TimedOut {
                last_fingerprints,
                last_error,
            }) => match last_error {
                Some(err) => panic!(
                    "replication did not converge: last_error={err:?} last_fingerprints={last_fingerprints:?}"
                ),
                None => panic!("replication did not converge: {last_fingerprints:?}"),
            },
        }
    }

    pub fn assert_replication_ready(&self, timeout: Duration) {
        match self.wait_for_statuses(
            "fixture.repl_rig.assert_replication_ready",
            "replication-ready",
            timeout,
            |statuses| self.replication_ready_with_statuses(statuses),
        ) {
            Ok(_) => {}
            Err(StatusWaitFailure::Fatal(err)) => panic!("admin status error: {err:?}"),
            Err(StatusWaitFailure::TimedOut {
                last_statuses,
                last_error,
            }) => match last_error {
                Some(err) => panic!(
                    "replication not ready: last_error={err:?} last_statuses={last_statuses:?}"
                ),
                None => panic!("replication not ready: {last_statuses:?}"),
            },
        }
    }

    pub fn assert_replication_durability_ready(&self, timeout: Duration) {
        match self.wait_for_statuses(
            "fixture.repl_rig.assert_replication_durability_ready",
            "replication-durability-ready",
            timeout,
            |statuses| self.replication_durability_ready_with_statuses(statuses),
        ) {
            Ok(_) => {}
            Err(StatusWaitFailure::Fatal(err)) => panic!("admin status error: {err:?}"),
            Err(StatusWaitFailure::TimedOut {
                last_statuses,
                last_error,
            }) => match last_error {
                Some(err) => panic!(
                    "replication durability not ready: last_error={err:?} last_statuses={last_statuses:?}"
                ),
                None => panic!("replication durability not ready: {last_statuses:?}"),
            },
        }
    }

    pub fn replication_ready_snapshot(&self) -> ReplicationReadySnapshot {
        let handshakes = self
            .nodes
            .iter()
            .map(|node| handshake_rows(&node.admin_status()))
            .collect();
        ReplicationReadySnapshot { handshakes }
    }

    pub fn assert_replication_ready_since(
        &self,
        snapshot: &ReplicationReadySnapshot,
        timeout: Duration,
    ) {
        match self.wait_for_statuses(
            "fixture.repl_rig.assert_replication_ready_since",
            "replication-ready-since",
            timeout,
            |statuses| self.replication_ready_since_with_statuses(snapshot, statuses),
        ) {
            Ok(_) => {}
            Err(StatusWaitFailure::Fatal(err)) => panic!("admin status error: {err:?}"),
            Err(StatusWaitFailure::TimedOut {
                last_statuses,
                last_error,
            }) => match last_error {
                Some(err) => panic!(
                    "replication not freshly ready: last_error={err:?} last_statuses={last_statuses:?}"
                ),
                None => panic!("replication not freshly ready: {last_statuses:?}"),
            },
        }
    }

    fn wait_for_statuses<F>(
        &self,
        phase: &'static str,
        context: impl std::fmt::Display,
        timeout: Duration,
        mut predicate: F,
    ) -> Result<Vec<AdminStatusOutput>, StatusWaitFailure>
    where
        F: FnMut(&[AdminStatusOutput]) -> bool,
    {
        let deadline = Instant::now() + timeout;
        let mut last_statuses = Vec::new();
        let result = wait::retry_with_backoff(
            phase,
            context,
            timeout,
            STATUS_RETRY_INITIAL_BACKOFF,
            STATUS_RETRY_MAX_BACKOFF,
            || match self.admin_statuses_with_read(ReadConsistency::default(), deadline) {
                Ok(statuses) => {
                    if predicate(&statuses) {
                        Ok(statuses)
                    } else {
                        last_statuses = statuses;
                        Err(StatusWaitRetry::Pending)
                    }
                }
                Err(err) if err.retryable => Err(StatusWaitRetry::Retryable(err)),
                Err(err) => Err(StatusWaitRetry::Fatal(err)),
            },
            |err| {
                matches!(
                    err,
                    StatusWaitRetry::Pending | StatusWaitRetry::Retryable(_)
                )
            },
        );
        match result {
            Ok(statuses) => Ok(statuses),
            Err(StatusWaitRetry::Fatal(err)) => Err(StatusWaitFailure::Fatal(err)),
            Err(StatusWaitRetry::Pending) => Err(StatusWaitFailure::TimedOut {
                last_statuses,
                last_error: None,
            }),
            Err(StatusWaitRetry::Retryable(err)) => Err(StatusWaitFailure::TimedOut {
                last_statuses,
                last_error: Some(err),
            }),
        }
    }

    fn wait_for_fingerprints<F>(
        &self,
        phase: &'static str,
        context: impl std::fmt::Display,
        timeout: Duration,
        mut predicate: F,
    ) -> Result<Vec<AdminFingerprintOutput>, FingerprintWaitFailure>
    where
        F: FnMut(&[AdminFingerprintOutput]) -> bool,
    {
        let deadline = Instant::now() + timeout;
        let mut last_fingerprints = Vec::new();
        let mut last_debug_summary: Option<String> = None;
        let result = wait::retry_with_backoff(
            phase,
            context,
            timeout,
            STATUS_RETRY_INITIAL_BACKOFF,
            STATUS_RETRY_MAX_BACKOFF,
            || match self.admin_fingerprints_with_read(ReadConsistency::default(), deadline) {
                Ok(fingerprints) => {
                    if predicate(&fingerprints) {
                        Ok(fingerprints)
                    } else {
                        let summary = summarize_fingerprints(&fingerprints);
                        if last_debug_summary.as_deref() != Some(summary.as_str()) {
                            debug_detail("fingerprint-pending", &summary);
                            last_debug_summary = Some(summary);
                        }
                        last_fingerprints = fingerprints;
                        Err(StatusWaitRetry::Pending)
                    }
                }
                Err(err) if err.retryable => {
                    debug_detail("fingerprint-retryable-error", format!("{err:?}"));
                    Err(StatusWaitRetry::Retryable(err))
                }
                Err(err) => {
                    debug_detail("fingerprint-fatal-error", format!("{err:?}"));
                    Err(StatusWaitRetry::Fatal(err))
                }
            },
            |err| {
                matches!(
                    err,
                    StatusWaitRetry::Pending | StatusWaitRetry::Retryable(_)
                )
            },
        );
        match result {
            Ok(fingerprints) => Ok(fingerprints),
            Err(StatusWaitRetry::Fatal(err)) => Err(FingerprintWaitFailure::Fatal(err)),
            Err(StatusWaitRetry::Pending) => Err(FingerprintWaitFailure::TimedOut {
                last_fingerprints,
                last_error: None,
            }),
            Err(StatusWaitRetry::Retryable(err)) => Err(FingerprintWaitFailure::TimedOut {
                last_fingerprints,
                last_error: Some(err),
            }),
        }
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

    fn admin_fingerprints_with_read(
        &self,
        mut read: ReadConsistency,
        deadline: Instant,
    ) -> Result<Vec<AdminFingerprintOutput>, ErrorPayload> {
        let wait_timeout_ms = deadline
            .saturating_duration_since(Instant::now())
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        read.wait_timeout_ms = Some(wait_timeout_ms);
        self.nodes
            .iter()
            .map(|node| node.admin_fingerprint_with_read(read.clone()))
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

    fn converged_with_fingerprints(
        &self,
        fingerprints: &[AdminFingerprintOutput],
        namespaces: &[NamespaceId],
    ) -> bool {
        if self.nodes.len() < 2 {
            return true;
        }
        let Some(base) = fingerprints.first() else {
            return false;
        };
        for fingerprint in &fingerprints[1..] {
            for namespace in namespaces {
                if !watermarks_equal_for_namespace(
                    &base.watermarks_applied,
                    &fingerprint.watermarks_applied,
                    namespace,
                ) {
                    return false;
                }
                if !watermarks_equal_for_namespace(
                    &base.watermarks_durable,
                    &fingerprint.watermarks_durable,
                    namespace,
                ) {
                    return false;
                }
                let Some(base_namespace) = base
                    .namespaces
                    .iter()
                    .find(|row| row.namespace == *namespace)
                else {
                    return false;
                };
                let Some(namespace_fingerprint) = fingerprint
                    .namespaces
                    .iter()
                    .find(|row| row.namespace == *namespace)
                else {
                    return false;
                };
                if namespace_fingerprint.namespace_root != base_namespace.namespace_root {
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

    fn replication_durability_ready_with_statuses(&self, statuses: &[AdminStatusOutput]) -> bool {
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
                if row.last_handshake_ms == 0 || !row.durability_eligible {
                    return false;
                }
            }
        }
        true
    }

    fn replication_ready_since_with_statuses(
        &self,
        snapshot: &ReplicationReadySnapshot,
        statuses: &[AdminStatusOutput],
    ) -> bool {
        if self.nodes.len() < 2 {
            return true;
        }
        if snapshot.handshakes.len() != self.nodes.len() || statuses.len() != self.nodes.len() {
            return false;
        }
        let expected: BTreeSet<ReplicaId> = self.nodes.iter().map(|node| node.replica_id).collect();
        for ((status, node), previous) in statuses
            .iter()
            .zip(self.nodes.iter())
            .zip(snapshot.handshakes.iter())
        {
            for peer in &expected {
                if *peer == node.replica_id {
                    continue;
                }
                let Some(row) = status
                    .replica_liveness
                    .iter()
                    .find(|row| row.replica_id == *peer)
                else {
                    return false;
                };
                let baseline = previous.get(peer).copied().unwrap_or(0);
                if row.last_handshake_ms == 0 || row.last_handshake_ms <= baseline {
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
            node.shutdown_owned_daemon();
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
    daemon_child: Mutex<Option<Child>>,
    admin_conn: Mutex<Option<IpcConnection>>,
    ipc_conn: Mutex<Option<IpcConnection>>,
    issue_min_seen: Arc<Mutex<BTreeMap<String, Watermarks<Applied>>>>,
}

impl Node {
    fn new(
        seed: NodeSeed,
        store_id_override: Option<StoreId>,
        replica_id: ReplicaId,
        daemon_child: Child,
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
            daemon_child: Mutex::new(Some(daemon_child)),
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

    fn unlock_store(&self, store_id: StoreId) {
        unlock_store(&self.data_dir, store_id).expect("unlock store");
    }

    fn start_daemon(&mut self) {
        self.ensure_daemon_child_running();
        self.wait_for_admin_ready(IPC_RELOAD_REPLICATION_TIMEOUT);
        let status = self.admin_status();
        let listen_addr = status.replication_listen_addr.unwrap_or_else(|| {
            panic!(
                "admin status missing replication listen addr for {}",
                self.repo_dir.display()
            )
        });
        self.listen_addr = listen_addr;
    }

    fn ensure_daemon_child_running(&self) {
        let mut guard = self.daemon_child.lock().expect("daemon child lock");
        let needs_spawn = match guard.as_mut() {
            Some(child) => child.try_wait().expect("poll daemon child").is_some(),
            None => true,
        };
        if !needs_spawn {
            return;
        }

        let child = spawn_node_daemon(
            &self.repo_dir,
            &self.runtime_dir,
            &self.data_dir,
            &self.config_dir,
            self.store_id_override,
        );
        *guard = Some(child);
    }

    fn sync_daemon_child_exit(&self, timeout: Duration) -> bool {
        let mut guard = self.daemon_child.lock().expect("daemon child lock");
        let Some(child) = guard.as_mut() else {
            return true;
        };
        if wait::wait_for_child_exit(child, timeout) {
            let _ = guard.take();
            return true;
        }
        false
    }

    fn kill_owned_daemon_child(&self, timeout: Duration) {
        let mut guard = self.daemon_child.lock().expect("daemon child lock");
        let Some(child) = guard.as_mut() else {
            return;
        };
        if wait::kill_child_and_wait(child, timeout) {
            let _ = guard.take();
        }
    }

    fn crash_owned_daemon(&self) {
        crash_daemon(&self.runtime_dir);
        self.sync_daemon_child_exit(Duration::from_secs(1));
    }

    fn shutdown_owned_daemon(&self) {
        shutdown_daemon(&self.runtime_dir);
        if !self.sync_daemon_child_exit(Duration::from_secs(2)) {
            self.kill_owned_daemon_child(Duration::from_secs(1));
        }
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
                        OpResult::Created { id } => id,
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
        let _phase = timing::scoped_phase_with_context(
            "fixture.repl_rig.wait_for_show",
            format!("repo={} issue={issue_id}", self.repo_dir.display()),
        );
        let deadline = Instant::now() + timeout;
        let mut last_error: Option<String> = None;
        let ok = wait::poll_until_with_backoff(
            timeout,
            STATUS_RETRY_INITIAL_BACKOFF,
            STATUS_RETRY_MAX_BACKOFF,
            || {
                if Instant::now() >= deadline {
                    return false;
                }
                last_error = None;
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
                let response = match self.send_request(&request) {
                    Ok(response) => response,
                    Err(err) if err.transience().is_retryable() => {
                        last_error = Some(format!("transport error: {err:?}"));
                        return false;
                    }
                    Err(err) => panic!("bd show transport error for {issue_id}: {err:?}"),
                };
                match response {
                    Response::Ok { ok } => match ok {
                        ResponsePayload::Query(QueryResult::Issue(issue)) => {
                            assert_eq!(issue.id, issue_id);
                            true
                        }
                        other => panic!("unexpected show payload: {other:?}"),
                    },
                    Response::Err { err } => {
                        let retry = err.code == CliErrorCode::NotFound.into()
                            || err.code == ProtocolErrorCode::RequireMinSeenUnsatisfied.into()
                            || err.code == ProtocolErrorCode::RequireMinSeenTimeout.into();
                        if retry {
                            last_error = Some(format!("replication pending: {err:?}"));
                            false
                        } else {
                            panic!("issue {issue_id} failed to replicate: {err:?}");
                        }
                    }
                }
            },
        );
        if ok {
            return;
        }
        match last_error {
            Some(err) => panic!("issue {issue_id} failed to replicate within {timeout:?}: {err}"),
            None => panic!("issue {issue_id} failed to replicate within {timeout:?}"),
        }
    }

    fn wait_for_admin_ready(&self, timeout: Duration) {
        let _phase = timing::scoped_phase_with_context(
            "fixture.repl_rig.wait_for_admin_ready",
            self.repo_dir.display(),
        );
        let deadline = Instant::now() + timeout;
        let mut last_error = None;
        let ready = wait::poll_until_with_backoff(
            timeout,
            STATUS_RETRY_INITIAL_BACKOFF,
            STATUS_RETRY_MAX_BACKOFF,
            || {
                let now = Instant::now();
                if now >= deadline {
                    return false;
                }
                let remaining = deadline.saturating_duration_since(now);
                let read = ReadConsistency {
                    wait_timeout_ms: Some(wait_timeout_ms(std::cmp::min(
                        remaining,
                        ADMIN_READY_POLL_TIMEOUT,
                    ))),
                    ..ReadConsistency::default()
                };
                match self.admin_status_with_read(read) {
                    Ok(_) => {
                        last_error = None;
                        true
                    }
                    Err(err) => {
                        last_error = Some(err);
                        false
                    }
                }
            },
        );
        if ready {
            return;
        }
        match last_error {
            Some(err) => panic!(
                "admin status did not become ready under {} within {timeout:?}: {err:?}",
                self.repo_dir.display()
            ),
            None => panic!(
                "admin status did not become ready under {} within {timeout:?}",
                self.repo_dir.display()
            ),
        }
    }

    pub fn admin_status(&self) -> AdminStatusOutput {
        match self.admin_status_with_read(ReadConsistency::default()) {
            Ok(status) => status,
            Err(err) => panic!("admin status error: {err:?}"),
        }
    }

    pub fn reload_replication(&self) {
        debug_step("node-reload-replication-wait-ready-start");
        self.wait_for_admin_ready(IPC_RELOAD_REPLICATION_TIMEOUT);
        debug_step("node-reload-replication-wait-ready-done");
        let request = Request::Admin(AdminOp::ReloadReplication {
            ctx: RepoCtx::new(self.repo_dir.clone()),
            payload: EmptyPayload {},
        });
        debug_step("node-reload-replication-send-start");
        let response = self
            .send_admin_request_fresh(&request)
            .expect("admin reload replication");
        debug_step("node-reload-replication-send-done");
        match response {
            Response::Ok {
                ok: ResponsePayload::Query(QueryResult::AdminReloadReplication(_)),
            } => {}
            Response::Err { err } => panic!("admin reload replication failed: {err:?}"),
            other => panic!("unexpected admin reload replication response: {other:?}"),
        }
    }

    pub fn reload_replication_peers(&self) {
        self.wait_for_admin_ready(IPC_RELOAD_REPLICATION_TIMEOUT);
        let request = Request::Admin(AdminOp::ReloadReplicationPeers {
            ctx: RepoCtx::new(self.repo_dir.clone()),
            payload: EmptyPayload {},
        });
        let response = self
            .send_admin_request_fresh(&request)
            .expect("admin reload replication peers");
        match response {
            Response::Ok {
                ok: ResponsePayload::Query(QueryResult::AdminReloadReplication(_)),
            } => {}
            Response::Err { err } => panic!("admin reload replication peers failed: {err:?}"),
            other => panic!("unexpected admin reload replication peers response: {other:?}"),
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
        let request = Request::Admin(AdminOp::Status {
            ctx: ReadCtx::new(self.repo_dir.clone(), read),
            payload: EmptyPayload {},
        });
        let response = self
            .send_admin_request(&request)
            .map_err(IntoErrorPayload::into_error_payload)?;
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

    fn admin_fingerprint_with_read(
        &self,
        read: ReadConsistency,
    ) -> Result<AdminFingerprintOutput, ErrorPayload> {
        let request = Request::Admin(AdminOp::Fingerprint {
            ctx: ReadCtx::new(self.repo_dir.clone(), read),
            payload: AdminFingerprintPayload {
                mode: AdminFingerprintMode::Full,
                sample: None,
            },
        });
        let response = self
            .send_admin_request_resilient(&request)
            .map_err(IntoErrorPayload::into_error_payload)?;
        match response {
            Response::Ok { ok } => match ok {
                ResponsePayload::Query(QueryResult::AdminFingerprint(output)) => Ok(output),
                other => Err(ErrorPayload::new(
                    ProtocolErrorCode::InternalError.into(),
                    format!("unexpected admin fingerprint payload: {other:?}"),
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

    fn send_admin_request(&self, request: &Request) -> Result<Response, IpcError> {
        self.send_with_connection(&self.admin_conn, request)
    }

    fn send_admin_request_fresh(&self, request: &Request) -> Result<Response, IpcError> {
        self.send_with_fresh_connection(request)
    }

    fn send_admin_request_resilient(&self, request: &Request) -> Result<Response, IpcError> {
        match self.send_admin_request(request) {
            Ok(response) => Ok(response),
            Err(err) if err.transience().is_retryable() => {
                *self.admin_conn.lock().expect("admin conn lock") = None;
                self.send_admin_request_fresh(request)
            }
            Err(err) => Err(err),
        }
    }

    fn send_request(&self, request: &Request) -> Result<Response, IpcError> {
        self.send_with_connection(&self.ipc_conn, request)
    }

    fn send_with_connection(
        &self,
        conn_slot: &Mutex<Option<IpcConnection>>,
        request: &Request,
    ) -> Result<Response, IpcError> {
        let timeout = ipc_timeout_for_request(request);
        self.retry_ipc_send(request, || {
            let mut guard = conn_slot.lock().expect("ipc conn lock");
            if guard.is_none() {
                let client = IpcClient::for_runtime_dir(&self.runtime_dir).with_autostart(false);
                *guard = Some(client.connect()?);
            }
            let conn = guard.as_mut().expect("ipc conn");
            conn.set_read_timeout(Some(timeout))?;
            conn.set_write_timeout(Some(timeout))?;
            conn.send_request(request)
        })
    }

    fn send_with_fresh_connection(&self, request: &Request) -> Result<Response, IpcError> {
        let timeout = ipc_timeout_for_request(request);
        self.retry_ipc_send(request, || {
            let client = IpcClient::for_runtime_dir(&self.runtime_dir).with_autostart(false);
            let mut conn = client.connect()?;
            conn.set_read_timeout(Some(timeout))?;
            conn.set_write_timeout(Some(timeout))?;
            conn.send_request(request)
        })
    }

    fn retry_ipc_send<F>(&self, request: &Request, mut send: F) -> Result<Response, IpcError>
    where
        F: FnMut() -> Result<Response, IpcError>,
    {
        let retry_attempts = if request_is_retry_safe(request) {
            IPC_SEND_RETRY_ATTEMPTS
        } else {
            1
        };
        let mut backoff = IPC_RETRY_INITIAL_BACKOFF;
        let mut final_error: Option<IpcError> = None;
        for attempt in 1..=retry_attempts {
            match send() {
                Ok(response) => return Ok(response),
                Err(err) if err.transience().is_retryable() => {
                    if attempt == retry_attempts {
                        final_error = Some(err);
                        break;
                    }
                    std::thread::sleep(backoff);
                    backoff = std::cmp::min(backoff.saturating_mul(2), IPC_RETRY_MAX_BACKOFF);
                }
                Err(err) => return Err(err),
            }
        }
        Err(final_error.expect("retryable IPC error should be recorded"))
    }
}

struct NodeSeed {
    repo_dir: PathBuf,
    runtime_dir: PathBuf,
    data_dir: PathBuf,
    config_dir: PathBuf,
    listen_addr: String,
}

fn bootstrap_replica(
    node: &NodeSeed,
    store_id_override: Option<StoreId>,
) -> (StoreId, ReplicaId, Child) {
    let child = spawn_node_daemon(
        &node.repo_dir,
        &node.runtime_dir,
        &node.data_dir,
        &node.config_dir,
        store_id_override,
    );
    let client = IpcClient::for_runtime_dir(&node.runtime_dir).with_autostart(false);
    assert!(
        wait_for_daemon_ready(&client, Duration::from_secs(5)),
        "daemon failed to start for {}",
        node.repo_dir.display()
    );
    initialize_repo_with_client(&client, &node.repo_dir);
    let meta = read_store_meta(&node.data_dir, store_id_override);
    (meta.store_id(), meta.replica_id, child)
}

fn read_store_meta(data_dir: &Path, store_id_override: Option<StoreId>) -> StoreMeta {
    let stores_dir = data_dir.join("stores");
    let mut meta_path: Option<PathBuf> = None;
    let ok = wait::poll_until_with_phase(
        "fixture.repl_rig.wait_for_store_meta",
        stores_dir.display(),
        Duration::from_secs(2),
        || {
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
        },
    );
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

fn spawn_node_daemon(
    repo_dir: &Path,
    runtime_dir: &Path,
    data_dir: &Path,
    config_dir: &Path,
    store_id_override: Option<StoreId>,
) -> Child {
    spawn_daemon_with_paths(
        repo_dir,
        runtime_dir,
        data_dir,
        config_dir,
        store_id_override,
        BdCommandProfile::cli(),
    )
}

fn write_replication_config(
    repo_dir: &Path,
    listen_addr: &str,
    peers: &[(ReplicaId, String)],
    dead_ms: Option<u64>,
    keepalive_ms: Option<u64>,
    wal_segment_max_bytes: Option<usize>,
) -> Result<(), String> {
    let mut out = String::new();
    out.push_str("[replication]\n");
    out.push_str(&format!("listen_addr = \"{listen_addr}\"\n"));
    out.push_str("backoff_base_ms = 50\n");
    out.push_str("backoff_max_ms = 500\n");
    out.push_str("max_connections = 8\n\n");
    if dead_ms.is_some() || keepalive_ms.is_some() || wal_segment_max_bytes.is_some() {
        out.push_str("[limits]\n");
        if let Some(dead_ms) = dead_ms {
            out.push_str(&format!("dead_ms = {dead_ms}\n"));
        }
        if let Some(keepalive_ms) = keepalive_ms {
            out.push_str(&format!("keepalive_ms = {keepalive_ms}\n"));
        }
        if let Some(wal_segment_max_bytes) = wal_segment_max_bytes {
            out.push_str(&format!(
                "wal_segment_max_bytes = {wal_segment_max_bytes}\n"
            ));
        }
        out.push('\n');
    }
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

    let config_path = config_dir.join("config.toml");
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
    temp::assert_unix_socket_path_fits(&daemon_socket_path(&runtime_dir));
    init_repo_with_origin(&repo_dir, remote_dir).expect("init git repo");
    NodeSeed {
        repo_dir,
        runtime_dir,
        data_dir,
        config_dir,
        listen_addr: EPHEMERAL_LOOPBACK_LISTEN_ADDR.to_string(),
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
            if needs_proxy {
                let seed = link_seed(options.seed, from, to);
                proxies.push(ProxySpec {
                    from,
                    to,
                    upstream_addr: target.listen_addr.clone(),
                    seed,
                    profile,
                    trace,
                });
            } else {
                link_addrs[from][to] = Some(target.listen_addr.clone());
            }
        }
    }
    (link_addrs, proxies)
}

fn link_seed(seed: u64, from: usize, to: usize) -> u64 {
    seed ^ ((from as u64) << 32) ^ (to as u64) ^ 0x9E37_79B9_7F4A_7C15
}

fn spawn_proxies(
    mut link_addrs: Vec<Vec<Option<String>>>,
    specs: Vec<ProxySpec>,
) -> (Vec<Vec<Option<String>>>, Vec<TailnetProxy>) {
    let mut proxies = Vec::with_capacity(specs.len());
    for spec in specs {
        let proxy = TailnetProxy::spawn_with_profile_and_trace(
            EPHEMERAL_LOOPBACK_LISTEN_ADDR.to_string(),
            spec.upstream_addr,
            spec.seed,
            spec.profile,
            spec.trace,
        );
        link_addrs[spec.from][spec.to] = Some(proxy.listen_addr().to_string());
        proxies.push(proxy);
    }
    (link_addrs, proxies)
}

struct ProxySpec {
    from: usize,
    to: usize,
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

fn handshake_rows(status: &AdminStatusOutput) -> BTreeMap<ReplicaId, u64> {
    status
        .replica_liveness
        .iter()
        .map(|row| (row.replica_id, row.last_handshake_ms))
        .collect()
}

fn summarize_fingerprints(fingerprints: &[AdminFingerprintOutput]) -> String {
    let mut parts = Vec::with_capacity(fingerprints.len());
    for (idx, fingerprint) in fingerprints.iter().enumerate() {
        let namespaces = fingerprint
            .namespaces
            .iter()
            .map(|row| format!("{}={}", row.namespace, row.namespace_root))
            .collect::<Vec<_>>()
            .join(",");
        parts.push(format!(
            "node={idx} applied={:?} durable={:?} namespaces=[{namespaces}]",
            fingerprint.watermarks_applied, fingerprint.watermarks_durable
        ));
    }
    parts.join(" | ")
}

fn wait_timeout_ms(timeout: Duration) -> u64 {
    timeout.as_millis().clamp(1, u128::from(u64::MAX)) as u64
}

fn ipc_timeout_for_request(request: &Request) -> Duration {
    if matches!(
        request,
        Request::Admin(AdminOp::ReloadReplication { .. } | AdminOp::ReloadReplicationPeers { .. })
    ) {
        return IPC_RELOAD_REPLICATION_TIMEOUT;
    }
    request_read_wait_timeout(request)
        .and_then(|wait_ms| {
            if wait_ms == 0 {
                None
            } else {
                Some(Duration::from_millis(wait_ms))
            }
        })
        .unwrap_or(IPC_IO_TIMEOUT_FLOOR)
        .saturating_add(IPC_IO_TIMEOUT_SLACK)
        .clamp(IPC_IO_TIMEOUT_FLOOR, IPC_IO_TIMEOUT_CEILING)
}

fn request_read_wait_timeout(request: &Request) -> Option<u64> {
    match request {
        Request::Show { ctx, .. } => ctx.read.wait_timeout_ms,
        Request::Admin(AdminOp::Status { ctx, .. }) => ctx.read.wait_timeout_ms,
        Request::Admin(AdminOp::Fingerprint { ctx, .. }) => ctx.read.wait_timeout_ms,
        _ => None,
    }
}

fn request_is_retry_safe(request: &Request) -> bool {
    match request {
        Request::Show { .. } => true,
        Request::Admin(AdminOp::Status { .. }) => true,
        Request::Admin(AdminOp::Fingerprint { .. }) => true,
        Request::Admin(
            AdminOp::ReloadReplication { .. } | AdminOp::ReloadReplicationPeers { .. },
        ) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn admin_fingerprint_requests_are_retry_safe() {
        let request = Request::Admin(AdminOp::Fingerprint {
            ctx: ReadCtx::new(PathBuf::from("/test"), ReadConsistency::default()),
            payload: AdminFingerprintPayload {
                mode: AdminFingerprintMode::Full,
                sample: None,
            },
        });

        assert!(request_is_retry_safe(&request));
    }
}
