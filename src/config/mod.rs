//! Config loading and persistence.

mod load;
mod merge;
mod schema;

pub use load::{
    config_path, discover_repo_root, load, load_for_repo, load_or_init, load_repo_config,
    load_user_config, repo_config_path, write_config,
};
pub use merge::{apply_env_overrides, merge_layers};
pub use schema::{
    CheckpointGroupConfig, CheckpointGroupConfigOverride, Config, ConfigLayer, DefaultsConfig,
    FileLoggingConfig, FileLoggingConfigOverride, LimitsOverride, LogFormat, LogRotation,
    LoggingConfig, LoggingConfigOverride, ReplicationConfig, ReplicationConfigOverride,
    ReplicationPeerConfig,
};
