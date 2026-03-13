//! Canonical app config schema, merge logic, and file IO.

mod env;
mod load;
mod merge;
mod schema;

pub use env::{CONFIG_DIR_VAR, CONFIG_ENV_KEYS, ENV_OVERRIDE_KEYS};
pub use load::{
    config_path, load_for_repo, load_repo_config, load_repo_config_full, load_user_config,
    load_user_config_full, repo_config_path, write_config,
};
pub use merge::{apply_env_overrides, merge_layers};
pub use schema::{
    CheckpointGroupConfig, CheckpointGroupConfigOverride, Config, ConfigLayer, DefaultsConfig,
    FileLoggingConfig, FileLoggingConfigOverride, LimitsOverride, LogFormat, LogRotation,
    LoggingConfig, LoggingConfigOverride, PathsConfig, PathsConfigOverride, ReplicationConfig,
    ReplicationConfigOverride, ReplicationPeerConfig,
};
