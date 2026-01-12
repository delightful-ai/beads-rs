//! Config loading and persistence.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::core::Limits;
use crate::daemon::OpError;
use crate::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub auto_upgrade: bool,
    pub limits: Limits,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            auto_upgrade: true,
            limits: Limits::default(),
        }
    }
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

    #[test]
    fn config_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let cfg = Config {
            auto_upgrade: false,
            limits: Limits::default(),
        };
        write_config(&path, &cfg).expect("write config");
        let loaded = {
            let contents = fs::read_to_string(&path).expect("read config");
            toml::from_str::<Config>(&contents).expect("parse config")
        };
        assert!(!loaded.auto_upgrade);
    }
}
