use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to access config directory: {reason}")]
    SystemDir { reason: String },

    #[error("failed to read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to write config temp file at {path}: {source}")]
    TempIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to persist config to {path}: {source}")]
    Persist {
        path: PathBuf,
        #[source]
        source: tempfile::PersistError,
    },

    #[error("failed to parse {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("failed to serialize config: {source}")]
    Serialize {
        #[from]
        source: toml::ser::Error,
    },
}
