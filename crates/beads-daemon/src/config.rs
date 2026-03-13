use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub use beads_bootstrap::config::{
    CheckpointGroupConfig, Config, ConfigLayer, DefaultsConfig, FileLoggingConfig,
    FileLoggingConfigOverride, LimitsOverride, LogFormat, LogRotation, LoggingConfig,
    LoggingConfigOverride, PathsConfig, PathsConfigOverride, ReplicationConfig,
    ReplicationConfigOverride, ReplicationPeerConfig, apply_env_overrides, config_path,
    load_repo_config, load_repo_config_full, load_user_config, load_user_config_full, merge_layers,
    repo_config_path, write_config,
};
use beads_core::{Limits, NamespaceId, NamespacePolicy};

use crate::env_flags::env_flag_truthy;

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
        let cfg = Config::default();
        Self {
            limits: cfg.limits,
            namespace_defaults: cfg.namespace_defaults.namespaces,
            checkpoint_groups: cfg.checkpoint_groups,
            replication: cfg.replication,
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
pub fn discover_repo_root() -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    beads_bootstrap::repo::discover_root_optional(cwd)
}

pub fn load_for_repo(repo_root: Option<&Path>) -> Result<Config, String> {
    // Keep daemon runtime reload semantics stable: repo config takes precedence,
    // user config is used only when repo config is absent.
    if let Some(root) = repo_root
        && let Some(repo_config) = load_repo_config_full(root)?
    {
        return Ok(finalize_loaded_config(repo_config));
    }

    if let Some(user_config) = load_user_config_full()? {
        return Ok(finalize_loaded_config(user_config));
    }

    Ok(finalize_loaded_config(Config::default()))
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

fn finalize_loaded_config(mut config: Config) -> Config {
    apply_env_overrides(&mut config);
    tracing::debug!(
        checkpoint_groups = ?config.checkpoint_groups.keys().collect::<Vec<_>>(),
        "config loaded with checkpoint groups"
    );
    config
}
