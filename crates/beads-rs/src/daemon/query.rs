//! Query types and filters.
//!
//! Provides:
//! - `Query` - All query operations
//! - `Filters` - Filtering criteria for list queries
//! - `QueryResult` - Query results

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub use beads_surface::query::{Filters, SortField};

use crate::core::BeadId;

// =============================================================================
// Query - All query operations
// =============================================================================

/// Query operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "query", rename_all = "snake_case")]
pub enum Query {
    /// Get a single bead by ID.
    Show { repo: PathBuf, id: BeadId },

    /// List beads with filters.
    List {
        repo: PathBuf,
        #[serde(default)]
        filters: Filters,
    },

    /// Get beads that are ready to work on (no blockers, unclaimed or claimed by actor).
    Ready {
        repo: PathBuf,
        #[serde(default)]
        limit: Option<usize>,
    },

    /// Get the dependency tree for a bead.
    DepTree { repo: PathBuf, id: BeadId },

    /// Get all deps for a bead (both incoming and outgoing).
    Deps { repo: PathBuf, id: BeadId },

    /// Get notes for a bead.
    Notes { repo: PathBuf, id: BeadId },

    /// Get issue database status (overview counts).
    Status { repo: PathBuf },

    /// Get blocked issues.
    Blocked { repo: PathBuf },

    /// Get stale issues.
    Stale {
        repo: PathBuf,
        days: u32,
        #[serde(default)]
        status: Option<String>,
        #[serde(default)]
        limit: Option<usize>,
    },

    /// Count issues matching filters.
    Count {
        repo: PathBuf,
        #[serde(default)]
        filter: Filters,
        #[serde(default)]
        group_by: Option<String>,
    },

    /// Show deleted (tombstoned) issues.
    Deleted {
        repo: PathBuf,
        /// If set, only include deletions newer than (now - since_ms).
        #[serde(default)]
        since_ms: Option<u64>,
        /// Optional specific issue id lookup.
        #[serde(default)]
        id: Option<BeadId>,
    },

    /// Epic completion status.
    EpicStatus {
        repo: PathBuf,
        #[serde(default)]
        eligible_only: bool,
    },

    /// Validate the state (check for issues).
    Validate { repo: PathBuf },
}

impl Query {
    /// Get the repo path for this query.
    pub fn repo(&self) -> &PathBuf {
        match self {
            Query::Show { repo, .. } => repo,
            Query::List { repo, .. } => repo,
            Query::Ready { repo, .. } => repo,
            Query::DepTree { repo, .. } => repo,
            Query::Deps { repo, .. } => repo,
            Query::Notes { repo, .. } => repo,
            Query::Status { repo } => repo,
            Query::Blocked { repo } => repo,
            Query::Stale { repo, .. } => repo,
            Query::Count { repo, .. } => repo,
            Query::Deleted { repo, .. } => repo,
            Query::EpicStatus { repo, .. } => repo,
            Query::Validate { repo } => repo,
        }
    }
}

pub type QueryResult = beads_api::QueryResult;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_default_matches_all() {
        // Can't easily test without a bead, but at least verify default construction
        let filters = Filters::default();
        assert!(filters.status.is_none());
        assert!(filters.priority.is_none());
        assert!(!filters.unclaimed);
    }

    #[test]
    fn query_repo() {
        let query = Query::Show {
            repo: PathBuf::from("/test"),
            id: BeadId::parse("bd-abc").unwrap(),
        };
        assert_eq!(query.repo(), &PathBuf::from("/test"));
    }
}
