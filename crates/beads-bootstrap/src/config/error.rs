use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to parse config from `{path}`: {source}")]
    Parse {
        path: std::path::PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("failed to render config to string: {source}")]
    Render {
        #[source]
        source: toml::ser::Error,
    },

    #[error("io error at `{path}`: {source}")]
    Io {
        path: std::path::PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("config path `{path}` is missing parent directory")]
    MissingParent { path: std::path::PathBuf },
}
