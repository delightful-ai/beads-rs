use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::core::{
    ActorId, ClientRequestId, DurabilityClass, Limits, NamespaceId, NamespacePolicies,
    NamespacePolicy, ReplicaId, ReplicaRole,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub auto_upgrade: bool,
    #[serde(default)]
    pub defaults: DefaultsConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    pub limits: Limits,
    pub replication: ReplicationConfig,
    #[serde(default = "default_namespace_policies")]
    pub namespace_defaults: NamespacePolicies,
    #[serde(default = "default_checkpoint_groups")]
    pub checkpoint_groups: BTreeMap<String, CheckpointGroupConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            auto_upgrade: true,
            defaults: DefaultsConfig::default(),
            logging: LoggingConfig::default(),
            limits: Limits::default(),
            replication: ReplicationConfig::default(),
            namespace_defaults: default_namespace_policies(),
            checkpoint_groups: default_checkpoint_groups(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DefaultsConfig {
    pub namespace: Option<NamespaceId>,
    pub durability: Option<DurabilityClass>,
    pub actor: Option<ActorId>,
    pub client_request_id: Option<ClientRequestId>,
}

impl DefaultsConfig {
    pub fn apply_to(&self, target: &mut DefaultsConfig) {
        if self.namespace.is_some() {
            target.namespace = self.namespace.clone();
        }
        if self.durability.is_some() {
            target.durability = self.durability;
        }
        if self.actor.is_some() {
            target.actor = self.actor.clone();
        }
        if self.client_request_id.is_some() {
            target.client_request_id = self.client_request_id;
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Tree,
    Pretty,
    Compact,
    Json,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogRotation {
    Daily,
    Hourly,
    Minutely,
    Never,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub stdout: bool,
    pub stdout_format: LogFormat,
    pub filter: Option<String>,
    pub file: FileLoggingConfig,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            stdout: true,
            stdout_format: LogFormat::Tree,
            filter: None,
            file: FileLoggingConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FileLoggingConfig {
    pub enabled: bool,
    pub dir: Option<PathBuf>,
    pub format: LogFormat,
    pub rotation: LogRotation,
    pub retention_max_age_days: Option<u64>,
    pub retention_max_files: Option<usize>,
}

impl Default for FileLoggingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dir: None,
            format: LogFormat::Json,
            rotation: LogRotation::Daily,
            retention_max_age_days: Some(7),
            retention_max_files: Some(10),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct LoggingConfigOverride {
    pub stdout: Option<bool>,
    pub stdout_format: Option<LogFormat>,
    pub filter: Option<String>,
    pub file: Option<FileLoggingConfigOverride>,
}

impl LoggingConfigOverride {
    pub fn apply_to(&self, target: &mut LoggingConfig) {
        if let Some(stdout) = self.stdout {
            target.stdout = stdout;
        }
        if let Some(format) = self.stdout_format {
            target.stdout_format = format;
        }
        if let Some(filter) = self.filter.as_ref() {
            target.filter = Some(filter.clone());
        }
        if let Some(file) = self.file.as_ref() {
            file.apply_to(&mut target.file);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct FileLoggingConfigOverride {
    pub enabled: Option<bool>,
    pub dir: Option<PathBuf>,
    pub format: Option<LogFormat>,
    pub rotation: Option<LogRotation>,
    pub retention_max_age_days: Option<u64>,
    pub retention_max_files: Option<usize>,
}

impl FileLoggingConfigOverride {
    pub fn apply_to(&self, target: &mut FileLoggingConfig) {
        if let Some(enabled) = self.enabled {
            target.enabled = enabled;
        }
        if let Some(dir) = self.dir.as_ref() {
            target.dir = Some(dir.clone());
        }
        if let Some(format) = self.format {
            target.format = format;
        }
        if let Some(rotation) = self.rotation {
            target.rotation = rotation;
        }
        if let Some(days) = self.retention_max_age_days {
            target.retention_max_age_days = Some(days);
        }
        if let Some(files) = self.retention_max_files {
            target.retention_max_files = Some(files);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ConfigLayer {
    pub auto_upgrade: Option<bool>,
    #[serde(default)]
    pub defaults: DefaultsConfig,
    #[serde(default)]
    pub logging: LoggingConfigOverride,
    #[serde(default)]
    pub limits: LimitsOverride,
    #[serde(default)]
    pub replication: ReplicationConfigOverride,
    pub namespace_defaults: Option<NamespacePolicies>,
    pub checkpoint_groups: Option<BTreeMap<String, CheckpointGroupConfigOverride>>,
}

impl ConfigLayer {
    pub fn apply_to(&self, base: &mut Config) {
        if let Some(auto_upgrade) = self.auto_upgrade {
            base.auto_upgrade = auto_upgrade;
        }
        self.defaults.apply_to(&mut base.defaults);
        self.logging.apply_to(&mut base.logging);
        self.limits.apply_to(&mut base.limits);
        self.replication.apply_to(&mut base.replication);
        if let Some(namespace_defaults) = &self.namespace_defaults {
            merge_namespace_policies(&mut base.namespace_defaults, namespace_defaults);
        }
        if let Some(checkpoint_groups) = &self.checkpoint_groups {
            for (group, override_cfg) in checkpoint_groups {
                let entry = base.checkpoint_groups.entry(group.clone()).or_default();
                override_cfg.apply_to(entry);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct LimitsOverride {
    pub max_frame_bytes: Option<usize>,
    pub max_event_batch_events: Option<usize>,
    pub max_event_batch_bytes: Option<usize>,
    pub max_wal_record_bytes: Option<usize>,

    pub max_repl_gap_events: Option<usize>,
    pub max_repl_gap_bytes: Option<usize>,
    pub repl_gap_timeout_ms: Option<u64>,

    pub keepalive_ms: Option<u64>,
    pub dead_ms: Option<u64>,

    pub wal_segment_max_bytes: Option<usize>,
    pub wal_segment_max_age_ms: Option<u64>,
    pub wal_guardrail_max_bytes: Option<u64>,
    pub wal_guardrail_max_segments: Option<u64>,
    pub wal_guardrail_growth_window_ms: Option<u64>,
    pub wal_guardrail_growth_max_bytes: Option<u64>,

    pub wal_group_commit_max_latency_ms: Option<u64>,
    pub wal_group_commit_max_events: Option<usize>,
    pub wal_group_commit_max_bytes: Option<usize>,
    pub wal_sqlite_checkpoint_interval_ms: Option<u64>,

    pub max_repl_ingest_queue_bytes: Option<usize>,
    pub max_repl_ingest_queue_events: Option<usize>,

    pub max_ipc_inflight_mutations: Option<usize>,
    pub max_checkpoint_job_queue: Option<usize>,
    pub max_broadcast_subscribers: Option<usize>,

    pub event_hot_cache_max_bytes: Option<usize>,
    pub event_hot_cache_max_events: Option<usize>,

    pub max_events_per_origin_per_batch: Option<usize>,
    pub max_bytes_per_origin_per_batch: Option<usize>,

    pub max_snapshot_bytes: Option<usize>,
    pub max_snapshot_entries: Option<usize>,
    pub max_snapshot_entry_bytes: Option<usize>,
    pub max_concurrent_snapshots: Option<usize>,
    pub max_jsonl_line_bytes: Option<usize>,
    pub max_jsonl_shard_bytes: Option<usize>,

    pub max_cbor_depth: Option<usize>,
    pub max_cbor_map_entries: Option<usize>,
    pub max_cbor_array_entries: Option<usize>,
    pub max_cbor_bytes_string_len: Option<usize>,
    pub max_cbor_text_string_len: Option<usize>,

    pub max_repl_ingest_bytes_per_sec: Option<usize>,
    pub max_background_io_bytes_per_sec: Option<usize>,

    pub hlc_max_forward_drift_ms: Option<u64>,

    pub max_note_bytes: Option<usize>,
    pub max_ops_per_txn: Option<usize>,
    pub max_note_appends_per_txn: Option<usize>,
    pub max_labels_per_bead: Option<usize>,
}

impl LimitsOverride {
    pub fn apply_to(&self, limits: &mut Limits) {
        if let Some(value) = self.max_frame_bytes {
            limits.max_frame_bytes = value;
        }
        if let Some(value) = self.max_event_batch_events {
            limits.max_event_batch_events = value;
        }
        if let Some(value) = self.max_event_batch_bytes {
            limits.max_event_batch_bytes = value;
        }
        if let Some(value) = self.max_wal_record_bytes {
            limits.max_wal_record_bytes = value;
        }

        if let Some(value) = self.max_repl_gap_events {
            limits.max_repl_gap_events = value;
        }
        if let Some(value) = self.max_repl_gap_bytes {
            limits.max_repl_gap_bytes = value;
        }
        if let Some(value) = self.repl_gap_timeout_ms {
            limits.repl_gap_timeout_ms = value;
        }

        if let Some(value) = self.keepalive_ms {
            limits.keepalive_ms = value;
        }
        if let Some(value) = self.dead_ms {
            limits.dead_ms = value;
        }

        if let Some(value) = self.wal_segment_max_bytes {
            limits.wal_segment_max_bytes = value;
        }
        if let Some(value) = self.wal_segment_max_age_ms {
            limits.wal_segment_max_age_ms = value;
        }
        if let Some(value) = self.wal_guardrail_max_bytes {
            limits.wal_guardrail_max_bytes = value;
        }
        if let Some(value) = self.wal_guardrail_max_segments {
            limits.wal_guardrail_max_segments = value;
        }
        if let Some(value) = self.wal_guardrail_growth_window_ms {
            limits.wal_guardrail_growth_window_ms = value;
        }
        if let Some(value) = self.wal_guardrail_growth_max_bytes {
            limits.wal_guardrail_growth_max_bytes = value;
        }

        if let Some(value) = self.wal_group_commit_max_latency_ms {
            limits.wal_group_commit_max_latency_ms = value;
        }
        if let Some(value) = self.wal_group_commit_max_events {
            limits.wal_group_commit_max_events = value;
        }
        if let Some(value) = self.wal_group_commit_max_bytes {
            limits.wal_group_commit_max_bytes = value;
        }
        if let Some(value) = self.wal_sqlite_checkpoint_interval_ms {
            limits.wal_sqlite_checkpoint_interval_ms = value;
        }

        if let Some(value) = self.max_repl_ingest_queue_bytes {
            limits.max_repl_ingest_queue_bytes = value;
        }
        if let Some(value) = self.max_repl_ingest_queue_events {
            limits.max_repl_ingest_queue_events = value;
        }

        if let Some(value) = self.max_ipc_inflight_mutations {
            limits.max_ipc_inflight_mutations = value;
        }
        if let Some(value) = self.max_checkpoint_job_queue {
            limits.max_checkpoint_job_queue = value;
        }
        if let Some(value) = self.max_broadcast_subscribers {
            limits.max_broadcast_subscribers = value;
        }

        if let Some(value) = self.event_hot_cache_max_bytes {
            limits.event_hot_cache_max_bytes = value;
        }
        if let Some(value) = self.event_hot_cache_max_events {
            limits.event_hot_cache_max_events = value;
        }

        if let Some(value) = self.max_events_per_origin_per_batch {
            limits.max_events_per_origin_per_batch = value;
        }
        if let Some(value) = self.max_bytes_per_origin_per_batch {
            limits.max_bytes_per_origin_per_batch = value;
        }

        if let Some(value) = self.max_snapshot_bytes {
            limits.max_snapshot_bytes = value;
        }
        if let Some(value) = self.max_snapshot_entries {
            limits.max_snapshot_entries = value;
        }
        if let Some(value) = self.max_snapshot_entry_bytes {
            limits.max_snapshot_entry_bytes = value;
        }
        if let Some(value) = self.max_concurrent_snapshots {
            limits.max_concurrent_snapshots = value;
        }
        if let Some(value) = self.max_jsonl_line_bytes {
            limits.max_jsonl_line_bytes = value;
        }
        if let Some(value) = self.max_jsonl_shard_bytes {
            limits.max_jsonl_shard_bytes = value;
        }

        if let Some(value) = self.max_cbor_depth {
            limits.max_cbor_depth = value;
        }
        if let Some(value) = self.max_cbor_map_entries {
            limits.max_cbor_map_entries = value;
        }
        if let Some(value) = self.max_cbor_array_entries {
            limits.max_cbor_array_entries = value;
        }
        if let Some(value) = self.max_cbor_bytes_string_len {
            limits.max_cbor_bytes_string_len = value;
        }
        if let Some(value) = self.max_cbor_text_string_len {
            limits.max_cbor_text_string_len = value;
        }

        if let Some(value) = self.max_repl_ingest_bytes_per_sec {
            limits.max_repl_ingest_bytes_per_sec = value;
        }
        if let Some(value) = self.max_background_io_bytes_per_sec {
            limits.max_background_io_bytes_per_sec = value;
        }

        if let Some(value) = self.hlc_max_forward_drift_ms {
            limits.hlc_max_forward_drift_ms = value;
        }

        if let Some(value) = self.max_note_bytes {
            limits.max_note_bytes = value;
        }
        if let Some(value) = self.max_ops_per_txn {
            limits.max_ops_per_txn = value;
        }
        if let Some(value) = self.max_note_appends_per_txn {
            limits.max_note_appends_per_txn = value;
        }
        if let Some(value) = self.max_labels_per_bead {
            limits.max_labels_per_bead = value;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplicationConfig {
    pub listen_addr: String,
    pub max_connections: Option<usize>,
    pub peers: Vec<ReplicationPeerConfig>,
    pub backoff_base_ms: u64,
    pub backoff_max_ms: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:0".to_string(),
            max_connections: Some(32),
            peers: Vec::new(),
            backoff_base_ms: 250,
            backoff_max_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ReplicationConfigOverride {
    pub listen_addr: Option<String>,
    pub max_connections: Option<usize>,
    pub peers: Option<Vec<ReplicationPeerConfig>>,
    pub backoff_base_ms: Option<u64>,
    pub backoff_max_ms: Option<u64>,
}

impl ReplicationConfigOverride {
    pub fn apply_to(&self, replication: &mut ReplicationConfig) {
        if let Some(listen_addr) = self.listen_addr.as_ref() {
            replication.listen_addr = listen_addr.clone();
        }
        if let Some(max_connections) = self.max_connections {
            replication.max_connections = Some(max_connections);
        }
        if let Some(peers) = self.peers.as_ref() {
            replication.peers = peers.clone();
        }
        if let Some(backoff_base_ms) = self.backoff_base_ms {
            replication.backoff_base_ms = backoff_base_ms;
        }
        if let Some(backoff_max_ms) = self.backoff_max_ms {
            replication.backoff_max_ms = backoff_max_ms;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationPeerConfig {
    pub replica_id: ReplicaId,
    pub addr: String,
    pub role: Option<ReplicaRole>,
    pub allowed_namespaces: Option<Vec<NamespaceId>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct CheckpointGroupConfig {
    pub namespaces: Vec<NamespaceId>,
    pub git_ref: Option<String>,
    pub checkpoint_writers: Vec<ReplicaId>,
    pub primary_writer: Option<ReplicaId>,
    pub debounce_ms: Option<u64>,
    pub max_interval_ms: Option<u64>,
    pub max_events: Option<u64>,
    pub durable_copy_via_git: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct CheckpointGroupConfigOverride {
    pub namespaces: Option<Vec<NamespaceId>>,
    pub git_ref: Option<String>,
    pub checkpoint_writers: Option<Vec<ReplicaId>>,
    pub primary_writer: Option<ReplicaId>,
    pub debounce_ms: Option<u64>,
    pub max_interval_ms: Option<u64>,
    pub max_events: Option<u64>,
    pub durable_copy_via_git: Option<bool>,
}

impl CheckpointGroupConfigOverride {
    pub fn apply_to(&self, config: &mut CheckpointGroupConfig) {
        if let Some(namespaces) = self.namespaces.as_ref() {
            config.namespaces = namespaces.clone();
        }
        if let Some(git_ref) = self.git_ref.as_ref() {
            config.git_ref = Some(git_ref.clone());
        }
        if let Some(checkpoint_writers) = self.checkpoint_writers.as_ref() {
            config.checkpoint_writers = checkpoint_writers.clone();
        }
        if let Some(primary_writer) = self.primary_writer {
            config.primary_writer = Some(primary_writer);
        }
        if let Some(debounce_ms) = self.debounce_ms {
            config.debounce_ms = Some(debounce_ms);
        }
        if let Some(max_interval_ms) = self.max_interval_ms {
            config.max_interval_ms = Some(max_interval_ms);
        }
        if let Some(max_events) = self.max_events {
            config.max_events = Some(max_events);
        }
        if let Some(durable_copy_via_git) = self.durable_copy_via_git {
            config.durable_copy_via_git = durable_copy_via_git;
        }
    }
}

pub fn default_namespace_policies() -> NamespacePolicies {
    let mut namespaces = BTreeMap::new();
    namespaces.insert(NamespaceId::core(), NamespacePolicy::core_default());
    namespaces.insert(
        NamespaceId::parse("sys").expect("sys namespace is valid"),
        NamespacePolicy::sys_default(),
    );
    namespaces.insert(
        NamespaceId::parse("wf").expect("wf namespace is valid"),
        NamespacePolicy::wf_default(),
    );
    namespaces.insert(
        NamespaceId::parse("tmp").expect("tmp namespace is valid"),
        NamespacePolicy::tmp_default(),
    );
    NamespacePolicies { namespaces }
}

pub fn default_checkpoint_groups() -> BTreeMap<String, CheckpointGroupConfig> {
    let mut groups = BTreeMap::new();
    let mut core = CheckpointGroupConfig::default();
    core.namespaces = vec![NamespaceId::core()];
    groups.insert("core".to_string(), core);
    groups
}

fn merge_namespace_policies(base: &mut NamespacePolicies, overlay: &NamespacePolicies) {
    for (namespace, policy) in &overlay.namespaces {
        base.namespaces.insert(namespace.clone(), policy.clone());
    }
}
