//! Meta types for format versioning.
//!
//! FormatVersion: typed version number
//! Meta: repository metadata

use serde::{Deserialize, Serialize};

/// Format version for wire format.
///
/// Current version is 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FormatVersion(u32);

impl FormatVersion {
    /// Current format version.
    pub const CURRENT: FormatVersion = FormatVersion(1);

    pub fn new(v: u32) -> Self {
        Self(v)
    }

    pub fn get(&self) -> u32 {
        self.0
    }

    /// Check if this version is compatible with current.
    ///
    /// Currently only version 1 is supported.
    pub fn is_compatible(&self) -> bool {
        self.0 == Self::CURRENT.0
    }
}

impl Default for FormatVersion {
    fn default() -> Self {
        Self::CURRENT
    }
}

/// Repository metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meta {
    pub format_version: FormatVersion,
}

impl Meta {
    pub fn new(format_version: FormatVersion) -> Self {
        Self { format_version }
    }

    /// Create meta with current format version.
    pub fn current() -> Self {
        Self {
            format_version: FormatVersion::CURRENT,
        }
    }
}

impl Default for Meta {
    fn default() -> Self {
        Self::current()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_version_is_1() {
        assert_eq!(FormatVersion::CURRENT.get(), 1);
    }

    #[test]
    fn version_compatibility() {
        assert!(FormatVersion::new(1).is_compatible());
        assert!(!FormatVersion::new(0).is_compatible());
        assert!(!FormatVersion::new(2).is_compatible());
    }

    #[test]
    fn meta_serde_roundtrip() {
        let meta = Meta::current();
        let json = serde_json::to_string(&meta).unwrap();
        let parsed: Meta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, parsed);
    }
}
