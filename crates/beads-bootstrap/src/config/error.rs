use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("failed to open config path {path:?}: {source}")]
    Io {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config at {path:?}: {source}")]
    Parse {
        path: std::path::PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to serialize config: {source}")]
    Serialize {
        #[source]
        source: toml::ser::Error,
    },
    #[error("failed to create temp file in {dir:?}: {source}")]
    TempFile {
        dir: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to persist temp file to {path:?}: {source}")]
    Persist {
        path: std::path::PathBuf,
        #[source]
        source: tempfile::PersistError,
    },
    #[error("config path missing parent directory: {path:?}")]
    MissingParent { path: std::path::PathBuf },
}
