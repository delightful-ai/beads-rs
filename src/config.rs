//! Config loading and persistence.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::core::{
    Limits, NamespaceId, NamespacePolicies, NamespacePolicy, ReplicaId, ReplicaRole,
};
use crate::daemon::OpError;
use crate::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub auto_upgrade: bool,
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
            limits: Limits::default(),
            replication: ReplicationConfig::default(),
            namespace_defaults: default_namespace_policies(),
            checkpoint_groups: default_checkpoint_groups(),
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
    let mut groups = BTreeMap::new();
    let mut core = CheckpointGroupConfig::default();
    core.namespaces = vec![NamespaceId::core()];
    groups.insert("core".to_string(), core);
    groups
}

pub fn config_path() -> PathBuf {
    crate::paths::config_dir().join("config.toml")
}

pub fn load() -> Result<Config> {
    let path = config_path();
    let contents = fs::read_to_string(&path)
        .map_err(|e| config_error(format!("failed to read {}: {e}", path.display())))?;
    toml::from_str(&contents)
        .map_err(|e| config_error(format!("failed to parse {}: {e}", path.display())))
}

pub fn load_or_init() -> Config {
    let path = config_path();
    if path.exists() {
        match load() {
            Ok(cfg) => return cfg,
            Err(e) => {
                tracing::warn!("config load failed, using defaults: {e}");
                return Config::default();
            }
        }
    }

    let cfg = Config::default();
    if let Err(e) = write_config(&path, &cfg) {
        tracing::warn!("failed to write default config: {e}");
    }
    cfg
}

pub fn write_config(path: &Path, cfg: &Config) -> Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)
            .map_err(|e| config_error(format!("failed to create {}: {e}", dir.display())))?;
    }
    let contents = toml::to_string_pretty(cfg)
        .map_err(|e| config_error(format!("failed to render config: {e}")))?;
    atomic_write(path, contents.as_bytes())
}

fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| config_error("config path missing parent directory".to_string()))?;
    let temp = tempfile::NamedTempFile::new_in(dir).map_err(|e| {
        config_error(format!(
            "failed to create temp file in {}: {e}",
            dir.display()
        ))
    })?;
    fs::write(temp.path(), data)
        .map_err(|e| config_error(format!("failed to write config temp file: {e}")))?;
    temp.persist(path).map_err(|e| {
        config_error(format!(
            "failed to persist config to {}: {e}",
            path.display()
        ))
    })?;
    Ok(())
}

fn config_error(reason: String) -> Error {
    Error::Op(OpError::ValidationFailed {
        field: "config".into(),
        reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    #[test]
    fn config_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let peer = ReplicationPeerConfig {
            replica_id: ReplicaId::new(Uuid::from_bytes([7u8; 16])),
            addr: "127.0.0.1:9000".to_string(),
            role: Some(ReplicaRole::Anchor),
            allowed_namespaces: Some(vec![NamespaceId::core()]),
        };
        let mut checkpoint_groups = BTreeMap::new();
        let mut core_group = CheckpointGroupConfig::default();
        core_group.namespaces = vec![NamespaceId::core()];
        core_group.debounce_ms = Some(123);
        checkpoint_groups.insert("core".to_string(), core_group);
        let namespace_defaults = default_namespace_policies();
        let cfg = Config {
            auto_upgrade: false,
            limits: Limits::default(),
            replication: ReplicationConfig {
                listen_addr: "127.0.0.1:9999".to_string(),
                max_connections: Some(7),
                peers: vec![peer],
                backoff_base_ms: 111,
                backoff_max_ms: 222,
            },
            namespace_defaults,
            checkpoint_groups,
        };
        write_config(&path, &cfg).expect("write config");
        let loaded = {
            let contents = fs::read_to_string(&path).expect("read config");
            toml::from_str::<Config>(&contents).expect("parse config")
        };
        assert!(!loaded.auto_upgrade);
        assert_eq!(loaded.replication.listen_addr, "127.0.0.1:9999");
        assert_eq!(loaded.replication.peers.len(), 1);
        assert!(loaded.checkpoint_groups.contains_key("core"));
        assert_eq!(
            loaded
                .checkpoint_groups
                .get("core")
                .and_then(|group| group.debounce_ms),
            Some(123)
        );
    }

    #[test]
    fn config_defaults_match_plan() {
        let cfg = Config::default();
        assert!(
            cfg.namespace_defaults
                .namespaces
                .contains_key(&NamespaceId::core())
        );
        assert!(
            cfg.namespace_defaults
                .namespaces
                .contains_key(&NamespaceId::parse("sys").unwrap())
        );
        assert!(
            cfg.namespace_defaults
                .namespaces
                .contains_key(&NamespaceId::parse("wf").unwrap())
        );
        assert!(
            cfg.namespace_defaults
                .namespaces
                .contains_key(&NamespaceId::parse("tmp").unwrap())
        );
        assert!(cfg.checkpoint_groups.contains_key("core"));
    }
}
