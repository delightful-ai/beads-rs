//! Query types and filters.
//!
//! Provides:
//! - `Query` - All query operations
//! - `Filters` - Filtering criteria for list queries
//! - `QueryResult` - Query results

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::query_model::{
    AdminCheckpointOutput, AdminDoctorOutput, AdminFingerprintOutput, AdminFlushOutput,
    AdminMaintenanceModeOutput, AdminMetricsOutput, AdminRebuildIndexOutput, AdminReloadLimitsOutput,
    AdminReloadPoliciesOutput, AdminReloadReplicationOutput, AdminRotateReplicaIdOutput,
    AdminScrubOutput, AdminStatusOutput, BlockedIssue, CountResult, DaemonInfo, DeletedLookup,
    DepCycles, DepEdge, EpicStatus, Issue, IssueSummary, Note, ReadyResult, StatusOutput,
    Tombstone,
};
use crate::core::{ActorId, BeadId, BeadType, BeadView, Claim, Priority};

// =============================================================================
// Filters - Filtering criteria
// =============================================================================

/// Filters for list queries.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Filters {
    /// Filter by status (open, in_progress, closed).
    #[serde(default)]
    pub status: Option<String>,

    /// Filter by priority.
    #[serde(default)]
    pub priority: Option<Priority>,

    /// Filter by minimum priority (inclusive).
    #[serde(default)]
    pub priority_min: Option<Priority>,

    /// Filter by maximum priority (inclusive).
    #[serde(default)]
    pub priority_max: Option<Priority>,

    /// Filter by bead type.
    #[serde(default, rename = "type")]
    pub bead_type: Option<BeadType>,

    /// Filter by assignee.
    #[serde(default)]
    pub assignee: Option<ActorId>,

    /// Filter by labels (AND: must have ALL).
    #[serde(default)]
    pub labels: Option<Vec<String>>,

    /// Filter by labels (OR: must have AT LEAST ONE).
    #[serde(default)]
    pub labels_any: Option<Vec<String>>,

    /// Filter by specific issue IDs.
    #[serde(default)]
    pub ids: Option<Vec<BeadId>>,

    /// Filter by title text (case-insensitive substring match).
    #[serde(default)]
    pub title: Option<String>,

    /// Pattern matching: title contains substring.
    #[serde(default)]
    pub title_contains: Option<String>,

    /// Pattern matching: description contains substring.
    #[serde(default)]
    pub desc_contains: Option<String>,

    /// Pattern matching: notes contains substring.
    #[serde(default)]
    pub notes_contains: Option<String>,

    /// Date ranges: created after/before (ms since epoch).
    #[serde(default)]
    pub created_after: Option<u64>,
    #[serde(default)]
    pub created_before: Option<u64>,
    #[serde(default)]
    pub updated_after: Option<u64>,
    #[serde(default)]
    pub updated_before: Option<u64>,
    #[serde(default)]
    pub closed_after: Option<u64>,
    #[serde(default)]
    pub closed_before: Option<u64>,

    /// Filter issues with empty description.
    #[serde(default)]
    pub empty_description: bool,

    /// Filter issues with no assignee.
    #[serde(default)]
    pub no_assignee: bool,

    /// Filter issues with no labels.
    #[serde(default)]
    pub no_labels: bool,

    /// Filter to only show unclaimed beads.
    #[serde(default)]
    pub unclaimed: bool,

    /// Text search in title/description.
    #[serde(default)]
    pub search: Option<String>,

    /// Limit number of results.
    #[serde(default)]
    pub limit: Option<usize>,

    /// Sort field.
    #[serde(default)]
    pub sort_by: Option<SortField>,

    /// Sort direction (default: descending).
    #[serde(default)]
    pub ascending: bool,

    /// Filter by parent epic ID (only show children).
    #[serde(default)]
    pub parent: Option<BeadId>,
}

/// Fields to sort by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortField {
    Priority,
    CreatedAt,
    UpdatedAt,
    Title,
}

impl Filters {
    /// Check if a bead matches these filters.
    pub fn matches(&self, view: &BeadView) -> bool {
        let bead = &view.bead;
        // Status filter
        if let Some(ref status) = self.status {
            // NOTE: "blocked" is derived and handled by higher-level queries when needed.
            if status != "all"
                && status != "blocked"
                && bead.fields.workflow.value.status() != status
            {
                return false;
            }
        }

        // Priority filter
        if let Some(priority) = self.priority
            && bead.fields.priority.value != priority
        {
            return false;
        }

        if let Some(min) = self.priority_min
            && bead.fields.priority.value < min
        {
            return false;
        }
        if let Some(max) = self.priority_max
            && bead.fields.priority.value > max
        {
            return false;
        }

        // Type filter
        if let Some(bead_type) = self.bead_type
            && bead.fields.bead_type.value != bead_type
        {
            return false;
        }

        // Assignee filter
        if let Some(ref assignee) = self.assignee {
            match &bead.fields.claim.value {
                Claim::Claimed { assignee: a, .. } if a == assignee => {}
                _ => return false,
            }
        }

        if self.no_assignee && bead.fields.claim.value.is_claimed() {
            return false;
        }

        // Labels filter (AND)
        if let Some(ref labels) = self.labels {
            let has_all = labels.iter().all(|l| view.labels.contains(l));
            if !has_all {
                return false;
            }
        }

        // LabelsAny filter (OR)
        if let Some(ref labels_any) = self.labels_any {
            let has_any = labels_any.iter().any(|l| view.labels.contains(l));
            if !has_any {
                return false;
            }
        }

        if self.no_labels && !view.labels.is_empty() {
            return false;
        }

        // IDs filter
        if let Some(ref ids) = self.ids
            && !ids.iter().any(|id| id == bead.id())
        {
            return false;
        }

        // Unclaimed filter
        if self.unclaimed && bead.fields.claim.value.is_claimed() {
            return false;
        }

        // Empty description filter
        if self.empty_description && !bead.fields.description.value.trim().is_empty() {
            return false;
        }

        // Title filter
        if let Some(ref title) = self.title {
            let needle = title.to_lowercase();
            if !bead.fields.title.value.to_lowercase().contains(&needle) {
                return false;
            }
        }

        // Text search
        if let Some(ref search) = self.search {
            let search_lower = search.to_lowercase();
            let title_match = bead
                .fields
                .title
                .value
                .to_lowercase()
                .contains(&search_lower);
            let desc_match = bead
                .fields
                .description
                .value
                .to_lowercase()
                .contains(&search_lower);
            if !title_match && !desc_match {
                return false;
            }
        }

        // Pattern matching
        if let Some(ref needle) = self.title_contains {
            let n = needle.to_lowercase();
            if !bead.fields.title.value.to_lowercase().contains(&n) {
                return false;
            }
        }
        if let Some(ref needle) = self.desc_contains {
            let n = needle.to_lowercase();
            if !bead.fields.description.value.to_lowercase().contains(&n) {
                return false;
            }
        }
        if let Some(ref needle) = self.notes_contains {
            let n = needle.to_lowercase();
            let any = view
                .notes
                .iter()
                .any(|note| note.content.to_lowercase().contains(&n));
            if !any {
                return false;
            }
        }

        // Date ranges
        let created_ms = bead.core.created().at.wall_ms;
        if let Some(after) = self.created_after
            && created_ms < after
        {
            return false;
        }
        if let Some(before) = self.created_before
            && created_ms > before
        {
            return false;
        }

        let updated_ms = view.updated_stamp().at.wall_ms;
        if let Some(after) = self.updated_after
            && updated_ms < after
        {
            return false;
        }
        if let Some(before) = self.updated_before
            && updated_ms > before
        {
            return false;
        }

        let closed_ms = bead
            .fields
            .workflow
            .value
            .is_closed()
            .then_some(bead.fields.workflow.stamp.at.wall_ms);
        if let Some(after) = self.closed_after
            && closed_ms.map(|t| t < after).unwrap_or(true)
        {
            return false;
        }
        if let Some(before) = self.closed_before
            && closed_ms.map(|t| t > before).unwrap_or(true)
        {
            return false;
        }

        true
    }
}

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

// =============================================================================
// QueryResult - Query results
// =============================================================================

/// Result of a query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "result", content = "data", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum QueryResult {
    /// Single issue result.
    Issue(Issue),

    /// List of issues (summaries).
    Issues(Vec<IssueSummary>),

    /// Dependency tree.
    DepTree { root: BeadId, edges: Vec<DepEdge> },

    /// Dependencies for a bead.
    Deps {
        incoming: Vec<DepEdge>,
        outgoing: Vec<DepEdge>,
    },

    /// Dependency cycles.
    DepCycles(DepCycles),

    /// Notes for a bead.
    Notes(Vec<Note>),

    /// Issue database status (counts, etc).
    Status(StatusOutput),

    /// Blocked issues.
    Blocked(Vec<BlockedIssue>),

    /// Ready issues with summary counts.
    Ready(ReadyResult),

    /// Stale issues.
    Stale(Vec<IssueSummary>),

    /// Count results.
    Count(CountResult),

    /// Deleted issues (tombstones) list.
    Deleted(Vec<Tombstone>),

    /// Deleted issue lookup by id.
    DeletedLookup(DeletedLookup),

    /// Epic completion status.
    EpicStatus(Vec<EpicStatus>),

    /// Validation result.
    Validation { warnings: Vec<String> },

    /// Daemon info (handshake).
    DaemonInfo(DaemonInfo),

    /// Admin status snapshot.
    AdminStatus(AdminStatusOutput),

    /// Admin metrics snapshot.
    AdminMetrics(AdminMetricsOutput),

    /// Admin doctor report.
    AdminDoctor(AdminDoctorOutput),

    /// Admin scrub report.
    AdminScrub(AdminScrubOutput),

    /// Admin flush report.
    AdminFlush(AdminFlushOutput),

    /// Admin checkpoint wait report.
    AdminCheckpoint(AdminCheckpointOutput),

    /// Admin fingerprint report.
    AdminFingerprint(AdminFingerprintOutput),

    /// Admin reload policies report.
    AdminReloadPolicies(AdminReloadPoliciesOutput),

    /// Admin reload replication report.
    AdminReloadReplication(AdminReloadReplicationOutput),

    /// Admin reload limits report.
    AdminReloadLimits(AdminReloadLimitsOutput),

    /// Admin rotate replica id report.
    AdminRotateReplicaId(AdminRotateReplicaIdOutput),

    /// Maintenance mode toggle.
    AdminMaintenanceMode(AdminMaintenanceModeOutput),

    /// Rebuild index outcome.
    AdminRebuildIndex(AdminRebuildIndexOutput),
}

// NOTE: daemon IPC uses the canonical `crate::api` schemas; query results are
// converted at the IPC boundary.

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
