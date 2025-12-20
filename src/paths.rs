//! XDG directory helpers for config/data locations.

use std::path::PathBuf;

/// Base directory for persistent data (WAL, exports, caches).
///
/// Uses `BD_DATA_DIR` if set, otherwise `$XDG_DATA_HOME/beads-rs` or
/// `~/.local/share/beads-rs`.
pub(crate) fn data_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("BD_DATA_DIR")
        && !dir.trim().is_empty()
    {
        return PathBuf::from(dir);
    }

    std::env::var("XDG_DATA_HOME")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join(".local")
                .join("share")
        })
        .join("beads-rs")
}

/// Base directory for configuration files.
///
/// Uses `BD_CONFIG_DIR` if set, otherwise `$XDG_CONFIG_HOME/beads-rs` or
/// `~/.config/beads-rs`.
#[allow(dead_code)]
pub(crate) fn config_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("BD_CONFIG_DIR")
        && !dir.trim().is_empty()
    {
        return PathBuf::from(dir);
    }

    std::env::var("XDG_CONFIG_HOME")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join(".config")
        })
        .join("beads-rs")
}
