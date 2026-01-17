use std::fs;
use std::path::{Path, PathBuf};

use crate::daemon::OpError;
use crate::{Error, Result};

use super::merge::{apply_env_overrides, merge_layers};
use super::{Config, ConfigLayer};

pub fn config_path() -> PathBuf {
    crate::paths::config_dir().join("config.toml")
}

pub fn repo_config_path(repo_root: &Path) -> PathBuf {
    repo_root.join("beads.toml")
}

pub fn discover_repo_root() -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    let repo = git2::Repository::discover(cwd).ok()?;
    repo.workdir().map(|path| path.to_path_buf())
}

pub fn load_user_config() -> Result<Option<ConfigLayer>> {
    let path = config_path();
    if !path.exists() {
        return Ok(None);
    }
    let contents = fs::read_to_string(&path)
        .map_err(|e| config_error(format!("failed to read {}: {e}", path.display())))?;
    toml::from_str(&contents)
        .map(Some)
        .map_err(|e| config_error(format!("failed to parse {}: {e}", path.display())))
}

pub fn load_repo_config(repo_root: &Path) -> Result<Option<ConfigLayer>> {
    let path = repo_config_path(repo_root);
    if !path.exists() {
        return Ok(None);
    }
    let contents = fs::read_to_string(&path)
        .map_err(|e| config_error(format!("failed to read {}: {e}", path.display())))?;
    toml::from_str(&contents)
        .map(Some)
        .map_err(|e| config_error(format!("failed to parse {}: {e}", path.display())))
}

pub fn load() -> Result<Config> {
    load_for_repo(discover_repo_root().as_deref())
}

pub fn load_for_repo(repo_root: Option<&Path>) -> Result<Config> {
    let user = load_user_config()?;
    let repo = match repo_root {
        Some(root) => load_repo_config(root)?,
        None => None,
    };
    let mut config = merge_layers(user, repo);
    apply_env_overrides(&mut config);
    Ok(config)
}

pub fn load_or_init() -> Config {
    let path = config_path();
    let had_user_config = path.exists();
    let repo_root = discover_repo_root();

    let config = match load_for_repo(repo_root.as_deref()) {
        Ok(cfg) => cfg,
        Err(e) => {
            tracing::warn!("config load failed, using defaults: {e}");
            let mut cfg = Config::default();
            apply_env_overrides(&mut cfg);
            cfg
        }
    };

    if !had_user_config {
        let default_cfg = Config::default();
        if let Err(e) = write_config(&path, &default_cfg) {
            tracing::warn!("failed to write default config: {e}");
        }
    }

    config
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
    use std::path::PathBuf;

    use uuid::Uuid;

    use crate::config::{
        CheckpointGroupConfig, DefaultsConfig, LogFormat, LogRotation, LoggingConfig,
        ReplicationConfig, ReplicationPeerConfig,
    };
    use crate::core::{NamespaceId, ReplicaId, ReplicaRole};

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
        let namespace_defaults = Config::default().namespace_defaults;
        let cfg = Config {
            auto_upgrade: false,
            defaults: DefaultsConfig {
                namespace: Some(NamespaceId::parse("wf").unwrap()),
                ..DefaultsConfig::default()
            },
            logging: LoggingConfig {
                stdout: false,
                stdout_format: LogFormat::Compact,
                file: crate::config::FileLoggingConfig {
                    enabled: true,
                    dir: Some(PathBuf::from("/tmp/beads-test-logs")),
                    format: LogFormat::Json,
                    rotation: LogRotation::Hourly,
                    retention_max_age_days: Some(3),
                    retention_max_files: Some(7),
                },
            },
            limits: crate::core::Limits::default(),
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
        assert_eq!(loaded.defaults.namespace, cfg.defaults.namespace);
        assert_eq!(loaded.replication.listen_addr, "127.0.0.1:9999");
        assert_eq!(loaded.replication.peers.len(), 1);
        assert!(!loaded.logging.stdout);
        assert!(matches!(loaded.logging.stdout_format, LogFormat::Compact));
        assert!(loaded.logging.file.enabled);
        assert_eq!(
            loaded.logging.file.dir.as_ref().unwrap().to_string_lossy(),
            "/tmp/beads-test-logs"
        );
        assert!(matches!(loaded.logging.file.format, LogFormat::Json));
        assert!(matches!(loaded.logging.file.rotation, LogRotation::Hourly));
        assert_eq!(loaded.logging.file.retention_max_age_days, Some(3));
        assert_eq!(loaded.logging.file.retention_max_files, Some(7));
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
