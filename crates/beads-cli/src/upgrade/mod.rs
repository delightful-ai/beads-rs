//! Upgrade support utilities shared with the legacy `beads-rs` compatibility layer.

mod release;

pub use release::{
    ReleaseAsset, ReleaseInfo, UpgradeSupportError, detect_platform, fetch_asset_checksum,
    fetch_latest_release, find_asset, is_newer_version, normalize_version,
};
