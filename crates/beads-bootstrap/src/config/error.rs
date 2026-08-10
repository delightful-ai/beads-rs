use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to create {dir}: {source}")]
    CreateDir {
        dir: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to render config: {source}")]
    Render {
        #[source]
        source: toml::ser::Error,
    },
    #[error("config path missing parent directory: {path}")]
    MissingParentDir { path: PathBuf },
    #[error("failed to create temp file in {dir}: {source}")]
    CreateTempFile {
        dir: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write config temp file: {source}")]
    WriteTempFile {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to persist config to {path}: {source}")]
    PersistTempFile {
        path: PathBuf,
        #[source]
        source: tempfile::PersistError,
    },
    #[error("failed to read {path}: {source}")]
    ReadFile {
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
}
