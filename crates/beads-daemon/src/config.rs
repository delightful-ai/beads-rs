use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use beads_core::{
    ActorId, ClientRequestId, DurabilityClass, Limits, NamespaceId, NamespacePolicies,
    NamespacePolicy, ReplicaId, ReplicaRole,
};
use serde::{Deserialize, Serialize};

use crate::env_flags::env_flag_truthy;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DefaultsConfig {
    pub namespace: Option<NamespaceId>,
    pub durability: Option<DurabilityClass>,
    pub actor: Option<ActorId>,
    pub client_request_id: Option<ClientRequestId>,
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PathsConfig {
    pub data_dir: Option<PathBuf>,
    pub runtime_dir: Option<PathBuf>,
}

/// Test/runtime policy switch for git synchronization.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GitSyncPolicy {
    #[default]
    Enabled,
    Disabled,
}

impl GitSyncPolicy {
    #[must_use]
    pub fn from_env() -> Self {
        if env_flag_truthy("BD_TEST_DISABLE_GIT_SYNC") {
            Self::Disabled
        } else {
            Self::Enabled
        }
    }

    #[must_use]
    pub fn allows_sync(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

/// Test/runtime policy switch for checkpoint scheduling.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CheckpointPolicy {
    #[default]
    Enabled,
    Disabled,
}

impl CheckpointPolicy {
    #[must_use]
    pub fn from_env() -> Self {
        if env_flag_truthy("BD_TEST_DISABLE_CHECKPOINTS") {
            Self::Disabled
        } else {
            Self::Enabled
        }
    }

    #[must_use]
    pub fn allows_checkpoints(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplicationPeerConfig {
    pub replica_id: ReplicaId,
    pub addr: String,
    pub role: Option<ReplicaRole>,
    pub allowed_namespaces: Option<Vec<NamespaceId>>,
}

impl Default for ReplicationPeerConfig {
    fn default() -> Self {
        Self {
            replica_id: ReplicaId::new(uuid::Uuid::nil()),
            addr: String::new(),
            role: None,
            allowed_namespaces: None,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub auto_upgrade: bool,
    #[serde(default)]
    pub defaults: DefaultsConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub paths: PathsConfig,
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
            paths: PathsConfig::default(),
            limits: Limits::default(),
            replication: ReplicationConfig::default(),
            namespace_defaults: default_namespace_policies(),
            checkpoint_groups: default_checkpoint_groups(),
        }
    }
}

/// Runtime daemon config with all file/layout-independent knobs.
#[derive(Debug, Clone)]
pub struct DaemonRuntimeConfig {
    pub limits: Limits,
    pub namespace_defaults: BTreeMap<NamespaceId, NamespacePolicy>,
    pub checkpoint_groups: BTreeMap<String, CheckpointGroupConfig>,
    pub replication: ReplicationConfig,
    pub git_sync_policy: GitSyncPolicy,
    pub checkpoint_policy: CheckpointPolicy,
}

impl Default for DaemonRuntimeConfig {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            namespace_defaults: default_namespace_policies().namespaces,
            checkpoint_groups: default_checkpoint_groups(),
            replication: ReplicationConfig::default(),
            git_sync_policy: GitSyncPolicy::Enabled,
            checkpoint_policy: CheckpointPolicy::Enabled,
        }
    }
}

#[must_use]
pub fn daemon_runtime_from_config(config: &Config) -> DaemonRuntimeConfig {
    DaemonRuntimeConfig {
        limits: config.limits.clone(),
        namespace_defaults: config.namespace_defaults.namespaces.clone(),
        checkpoint_groups: config.checkpoint_groups.clone(),
        replication: config.replication.clone(),
        git_sync_policy: GitSyncPolicy::from_env(),
        checkpoint_policy: CheckpointPolicy::from_env(),
    }
}

#[must_use]
pub fn config_path() -> PathBuf {
    crate::paths::config_dir().join("config.toml")
}

#[must_use]
pub fn repo_config_path(repo_root: &Path) -> PathBuf {
    repo_root.join("beads.toml")
}

#[must_use]
pub fn discover_repo_root() -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    crate::repo::discover_root_optional(cwd)
}

pub fn load_for_repo(repo_root: Option<&Path>) -> Result<Config, String> {
    // Keep semantics simple during runtime extraction: repo config takes precedence,
    // user config is used only when repo config is absent.
    if let Some(root) = repo_root {
        let path = repo_config_path(root);
        if path.exists() {
            return parse_config_file(&path);
        }
    }

    let user = config_path();
    if user.exists() {
        return parse_config_file(&user);
    }

    let mut cfg = Config::default();
    apply_env_overrides(&mut cfg);
    Ok(cfg)
}

pub fn load() -> Result<Config, String> {
    load_for_repo(discover_repo_root().as_deref())
}

#[must_use]
pub fn load_or_init() -> Config {
    match load() {
        Ok(cfg) => cfg,
        Err(err) => {
            tracing::warn!("config load failed, using defaults: {err}");
            let mut cfg = Config::default();
            apply_env_overrides(&mut cfg);
            cfg
        }
    }
}

pub fn write_config(path: &Path, cfg: &Config) -> Result<(), String> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|e| format!("failed to create {}: {e}", dir.display()))?;
    }
    let contents =
        toml::to_string_pretty(cfg).map_err(|e| format!("failed to render config: {e}"))?;
    fs::write(path, contents.as_bytes())
        .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
    Ok(())
}

pub fn apply_env_overrides(config: &mut Config) {
    if let Some(raw) = std::env::var("BD_DATA_DIR").ok().filter(|s| !s.is_empty()) {
        config.paths.data_dir = Some(PathBuf::from(raw));
    }
    if let Some(raw) = std::env::var("BD_RUNTIME_DIR")
        .ok()
        .filter(|s| !s.is_empty())
    {
        config.paths.runtime_dir = Some(PathBuf::from(raw));
    }
    if let Some(raw) = std::env::var("BD_REPL_LISTEN_ADDR")
        .ok()
        .filter(|s| !s.is_empty())
    {
        config.replication.listen_addr = raw;
    }
}

fn parse_config_file(path: &Path) -> Result<Config, String> {
    let raw =
        fs::read_to_string(path).map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    let mut cfg: Config =
        toml::from_str(&raw).map_err(|e| format!("failed to parse {}: {e}", path.display()))?;
    apply_env_overrides(&mut cfg);
    Ok(cfg)
}

fn default_namespace_policies() -> NamespacePolicies {
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

fn default_checkpoint_groups() -> BTreeMap<String, CheckpointGroupConfig> {
    let mut checkpoint_groups = BTreeMap::new();
    checkpoint_groups.insert(
        "core".to_string(),
        CheckpointGroupConfig {
            namespaces: vec![NamespaceId::core()],
            ..CheckpointGroupConfig::default()
        },
    );
    checkpoint_groups
}
