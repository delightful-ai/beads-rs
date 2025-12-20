//! Canonical API schemas for daemon IPC and CLI `--json`.
//!
//! These types are the *truthful boundary*: we avoid lossy “view” structs that
//! silently drop information. If a smaller payload is desirable, we define an
//! explicit summary type.

use serde::{Deserialize, Serialize};

use crate::core::{
    Bead, Claim, DepEdge as CoreDepEdge, Tombstone as CoreTombstone, WallClock, Workflow,
    WriteStamp,
};

// =============================================================================
// Daemon Info
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonInfo {
    pub version: String,
    pub protocol_version: u32,
    pub pid: u32,
}

// =============================================================================
// Status / Stats
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SyncWarning {
    Fetch {
        message: String,
        at_wall_ms: u64,
    },
    Diverged {
        local_oid: String,
        remote_oid: String,
        at_wall_ms: u64,
    },
    ForcePush {
        previous_remote_oid: String,
        remote_oid: String,
        at_wall_ms: u64,
    },
    ClockSkew {
        delta_ms: i64,
        at_wall_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncStatus {
    pub dirty: bool,
    pub sync_in_progress: bool,
    pub last_sync_wall_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_wall_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_in_ms: Option<u64>,
    pub consecutive_failures: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<SyncWarning>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusSummary {
    pub total_issues: usize,
    pub open_issues: usize,
    pub in_progress_issues: usize,
    pub blocked_issues: usize,
    pub closed_issues: usize,
    pub ready_issues: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tombstone_issues: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub epics_eligible_for_closure: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusOutput {
    pub summary: StatusSummary,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync: Option<SyncStatus>,
}

// =============================================================================
// Blocked / Stale
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockedIssue {
    #[serde(flatten)]
    pub issue: IssueSummary,

    pub blocked_by_count: usize,
    pub blocked_by: Vec<String>,
}

// =============================================================================
// Ready
// =============================================================================

/// Ready result with summary counts for context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyResult {
    pub issues: Vec<IssueSummary>,
    pub blocked_count: usize,
    pub closed_count: usize,
}

// =============================================================================
// Epic
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpicStatus {
    pub epic: IssueSummary,
    pub total_children: usize,
    pub closed_children: usize,
    pub eligible_for_close: bool,
}

// =============================================================================
// Count
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountGroup {
    pub group: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CountResult {
    Simple {
        count: usize,
    },
    Grouped {
        total: usize,
        groups: Vec<CountGroup>,
    },
}

// =============================================================================
// Deleted
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletedLookup {
    pub found: bool,
    pub id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<Tombstone>,
}

// =============================================================================
// Notes
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Note {
    pub id: String,
    pub content: String,
    pub author: String,
    pub at: WriteStamp,
}

impl From<&crate::core::Note> for Note {
    fn from(n: &crate::core::Note) -> Self {
        Self {
            id: n.id.as_str().to_string(),
            content: n.content.clone(),
            author: n.author.as_str().to_string(),
            at: n.at.clone(),
        }
    }
}

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

impl From<&CoreDepEdge> for DepEdge {
    fn from(edge: &CoreDepEdge) -> Self {
        Self {
            from: edge.key.from().as_str().to_string(),
            to: edge.key.to().as_str().to_string(),
            kind: edge.key.kind().as_str().to_string(),
            created_at: edge.created.at.clone(),
            created_by: edge.created.by.as_str().to_string(),
            deleted_at: edge.deleted_stamp().map(|s| s.at.clone()),
            deleted_by: edge.deleted_stamp().map(|s| s.by.as_str().to_string()),
        }
    }
}

// =============================================================================
// Tombstones
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tombstone {
    pub id: String,
    pub deleted_at: WriteStamp,
    pub deleted_by: String,
    pub reason: Option<String>,
}

impl From<&CoreTombstone> for Tombstone {
    fn from(t: &CoreTombstone) -> Self {
        Self {
            id: t.id.as_str().to_string(),
            deleted_at: t.deleted.at.clone(),
            deleted_by: t.deleted.by.as_str().to_string(),
            reason: t.reason.clone(),
        }
    }
}

// =============================================================================
// Issues
// =============================================================================

/// Full issue representation (includes notes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Issue {
    pub id: String,
    pub title: String,
    pub description: String,
    pub design: Option<String>,
    pub acceptance_criteria: Option<String>,
    pub status: String,
    pub priority: u8,
    #[serde(rename = "type")]
    pub issue_type: String,
    pub labels: Vec<String>,

    pub assignee: Option<String>,
    pub assignee_at: Option<WriteStamp>,
    pub assignee_expires: Option<WallClock>,

    pub created_at: WriteStamp,
    pub created_by: String,
    pub created_on_branch: Option<String>,

    pub updated_at: WriteStamp,
    pub updated_by: String,

    pub closed_at: Option<WriteStamp>,
    pub closed_by: Option<String>,
    pub closed_reason: Option<String>,
    pub closed_on_branch: Option<String>,

    pub external_ref: Option<String>,
    pub source_repo: Option<String>,

    /// Optional time estimate in minutes (beads-go parity).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_minutes: Option<u32>,

    pub content_hash: String,

    pub notes: Vec<Note>,
}

/// Summary issue representation (no note bodies).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssueSummary {
    pub id: String,
    pub title: String,
    pub description: String,
    pub design: Option<String>,
    pub acceptance_criteria: Option<String>,
    pub status: String,
    pub priority: u8,
    #[serde(rename = "type")]
    pub issue_type: String,
    pub labels: Vec<String>,

    pub assignee: Option<String>,
    pub assignee_expires: Option<WallClock>,

    pub created_at: WriteStamp,
    pub created_by: String,

    pub updated_at: WriteStamp,
    pub updated_by: String,

    /// Optional time estimate in minutes (beads-go parity).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_minutes: Option<u32>,

    pub content_hash: String,

    pub note_count: usize,
}

impl Issue {
    pub fn from_bead(bead: &Bead) -> Self {
        let updated = bead.updated_stamp();

        let (assignee, assignee_at, assignee_expires) = match &bead.fields.claim.value {
            Claim::Claimed { assignee, expires } => (
                Some(assignee.as_str().to_string()),
                Some(bead.fields.claim.stamp.at.clone()),
                *expires,
            ),
            Claim::Unclaimed => (None, None, None),
        };

        let (closed_at, closed_by, closed_reason, closed_on_branch) =
            match &bead.fields.workflow.value {
                Workflow::Closed(c) => (
                    Some(bead.fields.workflow.stamp.at.clone()),
                    Some(bead.fields.workflow.stamp.by.as_str().to_string()),
                    c.reason.clone(),
                    c.on_branch.clone(),
                ),
                _ => (None, None, None, None),
            };

        let notes = bead.notes.sorted().into_iter().map(Note::from).collect();

        Self {
            id: bead.core.id.as_str().to_string(),
            title: bead.fields.title.value.clone(),
            description: bead.fields.description.value.clone(),
            design: bead.fields.design.value.clone(),
            acceptance_criteria: bead.fields.acceptance_criteria.value.clone(),
            status: bead.fields.workflow.value.status().to_string(),
            priority: bead.fields.priority.value.value(),
            issue_type: bead.fields.bead_type.value.as_str().to_string(),
            labels: bead
                .fields
                .labels
                .value
                .iter()
                .map(|l| l.as_str().to_string())
                .collect(),
            assignee,
            assignee_at,
            assignee_expires,
            created_at: bead.core.created().at.clone(),
            created_by: bead.core.created().by.as_str().to_string(),
            created_on_branch: bead.core.created_on_branch().map(|s| s.to_string()),
            updated_at: updated.at.clone(),
            updated_by: updated.by.as_str().to_string(),
            closed_at,
            closed_by,
            closed_reason,
            closed_on_branch,
            external_ref: bead.fields.external_ref.value.clone(),
            source_repo: bead.fields.source_repo.value.clone(),
            estimated_minutes: bead.fields.estimated_minutes.value,
            content_hash: bead.content_hash().to_hex(),
            notes,
        }
    }
}

impl IssueSummary {
    pub fn from_bead(bead: &Bead) -> Self {
        let updated = bead.updated_stamp();
        Self {
            id: bead.core.id.as_str().to_string(),
            title: bead.fields.title.value.clone(),
            description: bead.fields.description.value.clone(),
            design: bead.fields.design.value.clone(),
            acceptance_criteria: bead.fields.acceptance_criteria.value.clone(),
            status: bead.fields.workflow.value.status().to_string(),
            priority: bead.fields.priority.value.value(),
            issue_type: bead.fields.bead_type.value.as_str().to_string(),
            labels: bead
                .fields
                .labels
                .value
                .iter()
                .map(|l| l.as_str().to_string())
                .collect(),
            assignee: bead
                .fields
                .claim
                .value
                .assignee()
                .map(|a| a.as_str().to_string()),
            assignee_expires: bead.fields.claim.value.expires(),
            created_at: bead.core.created().at.clone(),
            created_by: bead.core.created().by.as_str().to_string(),
            updated_at: updated.at.clone(),
            updated_by: updated.by.as_str().to_string(),
            estimated_minutes: bead.fields.estimated_minutes.value,
            content_hash: bead.content_hash().to_hex(),
            note_count: bead.notes.len(),
        }
    }

    pub fn from_issue(issue: &Issue) -> Self {
        Self {
            id: issue.id.clone(),
            title: issue.title.clone(),
            description: issue.description.clone(),
            design: issue.design.clone(),
            acceptance_criteria: issue.acceptance_criteria.clone(),
            status: issue.status.clone(),
            priority: issue.priority,
            issue_type: issue.issue_type.clone(),
            labels: issue.labels.clone(),
            assignee: issue.assignee.clone(),
            assignee_expires: issue.assignee_expires,
            created_at: issue.created_at.clone(),
            created_by: issue.created_by.clone(),
            updated_at: issue.updated_at.clone(),
            updated_by: issue.updated_by.clone(),
            estimated_minutes: issue.estimated_minutes,
            content_hash: issue.content_hash.clone(),
            note_count: issue.notes.len(),
        }
    }
}
