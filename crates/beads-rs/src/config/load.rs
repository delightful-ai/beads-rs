use std::path::{Path, PathBuf};

use crate::OpError;
use crate::repo;
use crate::{Error, Result};

use beads_bootstrap::config as moved;

use super::{Config, ConfigLayer, apply_env_overrides};

pub fn config_path() -> PathBuf {
    moved::config_path()
}

pub fn repo_config_path(repo_root: &Path) -> PathBuf {
    moved::repo_config_path(repo_root)
}

pub fn discover_repo_root() -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    repo::discover_root_optional(cwd)
}

pub fn load_user_config() -> Result<Option<ConfigLayer>> {
    moved::load_user_config().map_err(config_error)
}

pub fn load_repo_config(repo_root: &Path) -> Result<Option<ConfigLayer>> {
    moved::load_repo_config(repo_root).map_err(config_error)
}

pub fn load() -> Result<Config> {
    load_for_repo(discover_repo_root().as_deref())
}

pub fn load_for_repo(repo_root: Option<&Path>) -> Result<Config> {
    moved::load_for_repo(repo_root).map_err(config_error)
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
    moved::write_config(path, cfg).map_err(config_error)
}

fn config_error(reason: String) -> Error {
    Error::Op(OpError::ValidationFailed {
        field: "config".into(),
        reason,
    })
}
