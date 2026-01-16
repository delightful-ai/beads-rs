//! Dependency schemas.

use serde::{Deserialize, Serialize};

use crate::core::{DepEdge as CoreDepEdge, DepKey as CoreDepKey, WriteStamp};

// =============================================================================
// Dependencies
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepEdge {
    pub from: String,
    pub to: String,
    pub kind: String,
    pub created_at: WriteStamp,
    pub created_by: String,
    pub deleted_at: Option<WriteStamp>,
    pub deleted_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepCycles {
    pub cycles: Vec<Vec<String>>,
}

impl From<(&CoreDepKey, &CoreDepEdge)> for DepEdge {
    fn from((key, edge): (&CoreDepKey, &CoreDepEdge)) -> Self {
        Self {
            from: key.from().as_str().to_string(),
            to: key.to().as_str().to_string(),
            kind: key.kind().as_str().to_string(),
            created_at: edge.created.at.clone(),
            created_by: edge.created.by.as_str().to_string(),
            deleted_at: edge.deleted_stamp().map(|s| s.at.clone()),
            deleted_by: edge.deleted_stamp().map(|s| s.by.as_str().to_string()),
        }
    }
}
