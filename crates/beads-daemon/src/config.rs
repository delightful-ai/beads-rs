use std::collections::BTreeMap;

use beads_core::{Limits, NamespaceId, NamespacePolicy, ReplicaId, ReplicaRole};

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

#[derive(Debug, Clone)]
pub struct ReplicationPeerConfig {
    pub replica_id: ReplicaId,
    pub addr: String,
    pub role: Option<ReplicaRole>,
    pub allowed_namespaces: Option<Vec<NamespaceId>>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Default)]
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
        let mut namespace_defaults = BTreeMap::new();
        namespace_defaults.insert(NamespaceId::core(), NamespacePolicy::core_default());
        namespace_defaults.insert(
            NamespaceId::parse("sys").expect("sys namespace is valid"),
            NamespacePolicy::sys_default(),
        );
        namespace_defaults.insert(
            NamespaceId::parse("wf").expect("wf namespace is valid"),
            NamespacePolicy::wf_default(),
        );
        namespace_defaults.insert(
            NamespaceId::parse("tmp").expect("tmp namespace is valid"),
            NamespacePolicy::tmp_default(),
        );

        let mut checkpoint_groups = BTreeMap::new();
        checkpoint_groups.insert(
            "core".to_string(),
            CheckpointGroupConfig {
                namespaces: vec![NamespaceId::core()],
                ..CheckpointGroupConfig::default()
            },
        );

        Self {
            limits: Limits::default(),
            namespace_defaults,
            checkpoint_groups,
            replication: ReplicationConfig::default(),
            git_sync_policy: GitSyncPolicy::Enabled,
            checkpoint_policy: CheckpointPolicy::Enabled,
        }
    }
}

fn env_flag_truthy(name: &str) -> bool {
    let Ok(raw) = std::env::var(name) else {
        return false;
    };
    !matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "no" | "n" | "off"
    )
}
