use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to create {path}: {source}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to render config: {source}")]
    Render {
        #[source]
        source: toml::ser::Error,
    },
    #[error("failed to create temp file in {path}: {source}")]
    CreateTemp {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write config temp file: {source}")]
    WriteTemp {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to persist config to {path}: {source}")]
    Persist {
        path: PathBuf,
        #[source]
        source: tempfile::PersistError,
    },
    #[error("config path missing parent directory")]
    MissingParent,
}
