//! Layer 2: Domain enums
//!
//! BeadType: bug, feature, task, epic, chore
//! DepKind: blocks, parent, related, discovered_from
//! Priority: 0-4 (0 = critical)

use serde::{Deserialize, Serialize};

use super::error::{CoreError, RangeError};

/// Issue type classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadType {
    Bug,
    Feature,
    Task,
    Epic,
    Chore,
}

impl BeadType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Bug => "bug",
            Self::Feature => "feature",
            Self::Task => "task",
            Self::Epic => "epic",
            Self::Chore => "chore",
        }
    }
}

/// Dependency relationship kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DepKind {
    Blocks,
    Parent,
    Related,
    DiscoveredFrom,
}

impl DepKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Blocks => "blocks",
            Self::Parent => "parent",
            Self::Related => "related",
            Self::DiscoveredFrom => "discovered_from",
        }
    }
}

/// Priority level: 0-4 inclusive, 0 = critical.
///
/// Validated at construction - invalid values are unrepresentable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Priority(u8);

impl Priority {
    pub const CRITICAL: Priority = Priority(0);
    pub const HIGH: Priority = Priority(1);
    pub const MEDIUM: Priority = Priority(2);
    pub const LOW: Priority = Priority(3);
    pub const LOWEST: Priority = Priority(4);

    pub fn new(n: u8) -> Result<Self, CoreError> {
        if n > 4 {
            Err(RangeError {
                field: "priority",
                value: n,
                min: 0,
                max: 4,
            }
            .into())
        } else {
            Ok(Self(n))
        }
    }

    pub fn value(&self) -> u8 {
        self.0
    }
}

impl Default for Priority {
    fn default() -> Self {
        Self::MEDIUM
    }
}
