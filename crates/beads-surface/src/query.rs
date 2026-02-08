use serde::{Deserialize, Serialize};

use beads_core::{ActorId, BeadId, BeadType, BeadView, Claim, Priority};

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
                Claim::Claimed { .. } | Claim::Unclaimed => return false,
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
