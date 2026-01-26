//! Dependency schemas.

use serde::{Deserialize, Serialize};

use beads_core::DepKey as CoreDepKey;

// =============================================================================
// Dependencies
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepEdge {
    pub from: String,
    pub to: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepCycles {
    pub cycles: Vec<Vec<String>>,
}

impl From<&CoreDepKey> for DepEdge {
    fn from(key: &CoreDepKey) -> Self {
        Self {
            from: key.from().as_str().to_string(),
            to: key.to().as_str().to_string(),
            kind: key.kind().as_str().to_string(),
        }
    }
}
