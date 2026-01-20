//! Go-compatible JSON schema types.
//!
//! These types serialize to JSON that matches the Go beads format exactly,
//! allowing tools built for Go beads to read Rust-generated JSONL.

use serde::Serialize;

use crate::core::BeadView;
use crate::core::composite::{Claim, Note, Workflow};
use crate::core::dep::{DepEdge, DepKey};
use crate::core::domain::DepKind;
use crate::core::state::CanonicalState;
use crate::core::tombstone::Tombstone;

/// Go-compatible issue representation.
///
/// Matches the Go `types.Issue` struct for JSONL export.
#[derive(Debug, Serialize)]
pub struct GoIssue {
    pub id: String,
    pub title: String,

    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub design: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub acceptance_criteria: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,

    pub status: String,
    pub priority: u8,
    pub issue_type: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_minutes: Option<u32>,

    pub created_at: String,
    pub updated_at: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_at: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_ref: Option<String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<GoDependency>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<GoComment>,

    // Tombstone fields (only set when status = "tombstone")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_by: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete_reason: Option<String>,
}

/// Go-compatible dependency representation.
#[derive(Debug, Serialize)]
pub struct GoDependency {
    pub issue_id: String,
    pub depends_on_id: String,
    #[serde(rename = "type")]
    pub dep_type: String,
    pub created_at: String,
    pub created_by: String,
}

/// Go-compatible comment representation.
#[derive(Debug, Serialize)]
pub struct GoComment {
    pub id: i64,
    pub issue_id: String,
    pub author: String,
    pub text: String,
    pub created_at: String,
}

impl GoIssue {
    /// Convert a Rust Bead to a Go-compatible issue.
    ///
    /// `is_blocked` should be true if the bead has unfinished blocking dependencies.
    pub fn from_view(view: &BeadView, deps: &[(DepKey, &DepEdge)], is_blocked: bool) -> Self {
        let bead = &view.bead;
        let status = derive_status(&bead.fields.workflow.value, is_blocked);

        let (closed_at, close_reason) = match bead.fields.workflow.value.closure() {
            Some(closure) => (
                Some(stamp_to_rfc3339(&bead.fields.workflow.stamp)),
                closure.reason.clone(),
            ),
            None => (None, None),
        };

        let assignee = match &bead.fields.claim.value {
            Claim::Claimed { assignee, .. } => Some(assignee.as_str().to_string()),
            Claim::Unclaimed => None,
        };

        let labels: Vec<String> = view.labels.iter().map(|l| l.as_str().to_string()).collect();

        let dependencies: Vec<GoDependency> = deps
            .iter()
            .filter(|(_, edge)| edge.is_active())
            .map(|(key, edge)| GoDependency::from_edge(key, edge, bead.core.id.as_str()))
            .collect();

        let comments: Vec<GoComment> = view
            .notes
            .iter()
            .enumerate()
            .map(|(i, note)| GoComment::from_note(note, bead.core.id.as_str(), i as i64 + 1))
            .collect();

        // Concatenate notes text for the legacy "notes" field (optional)
        let notes_text: String = view
            .notes
            .iter()
            .map(|n| n.content.as_str())
            .collect::<Vec<_>>()
            .join("\n\n");

        GoIssue {
            id: bead.core.id.as_str().to_string(),
            title: bead.fields.title.value.clone(),
            description: bead.fields.description.value.clone(),
            design: bead.fields.design.value.clone(),
            acceptance_criteria: bead.fields.acceptance_criteria.value.clone(),
            notes: if notes_text.is_empty() {
                None
            } else {
                Some(notes_text)
            },
            status,
            priority: bead.fields.priority.value.value(),
            issue_type: bead.fields.bead_type.value.as_str().to_string(),
            assignee,
            estimated_minutes: bead.fields.estimated_minutes.value,
            created_at: stamp_to_rfc3339(bead.core.created()),
            updated_at: stamp_to_rfc3339(view.updated_stamp()),
            closed_at,
            close_reason,
            external_ref: bead.fields.external_ref.value.clone(),
            labels,
            dependencies,
            comments,
            deleted_at: None,
            deleted_by: None,
            delete_reason: None,
        }
    }

    /// Convert a tombstone to a Go-compatible issue with status="tombstone".
    pub fn from_tombstone(tombstone: &Tombstone) -> Self {
        GoIssue {
            id: tombstone.id.as_str().to_string(),
            title: "(deleted)".to_string(),
            description: String::new(),
            design: None,
            acceptance_criteria: None,
            notes: None,
            status: "tombstone".to_string(),
            priority: 2, // default medium
            issue_type: "task".to_string(),
            assignee: None,
            estimated_minutes: None,
            created_at: stamp_to_rfc3339(&tombstone.deleted),
            updated_at: stamp_to_rfc3339(&tombstone.deleted),
            closed_at: None,
            close_reason: None,
            external_ref: None,
            labels: Vec::new(),
            dependencies: Vec::new(),
            comments: Vec::new(),
            deleted_at: Some(stamp_to_rfc3339(&tombstone.deleted)),
            deleted_by: Some(tombstone.deleted.by.as_str().to_string()),
            delete_reason: tombstone.reason.clone(),
        }
    }
}

impl GoDependency {
    fn from_edge(key: &DepKey, edge: &DepEdge, issue_id: &str) -> Self {
        GoDependency {
            issue_id: issue_id.to_string(),
            depends_on_id: key.to().as_str().to_string(),
            dep_type: dep_kind_to_go_type(key.kind()),
            created_at: stamp_to_rfc3339(&edge.created),
            created_by: edge.created.by.as_str().to_string(),
        }
    }
}

impl GoComment {
    fn from_note(note: &Note, issue_id: &str, id: i64) -> Self {
        GoComment {
            id,
            issue_id: issue_id.to_string(),
            author: note.author.as_str().to_string(),
            text: note.content.clone(),
            created_at: write_stamp_to_rfc3339(&note.at),
        }
    }
}

/// Derive Go status from Rust workflow + blocked state.
fn derive_status(workflow: &Workflow, is_blocked: bool) -> String {
    match workflow {
        Workflow::Closed(_) => "closed".to_string(),
        Workflow::InProgress => "in_progress".to_string(),
        Workflow::Open => {
            if is_blocked {
                "blocked".to_string()
            } else {
                "open".to_string()
            }
        }
    }
}

/// Map Rust DepKind to Go dependency type string.
fn dep_kind_to_go_type(kind: DepKind) -> String {
    match kind {
        DepKind::Blocks => "blocks".to_string(),
        DepKind::Parent => "parent-child".to_string(),
        DepKind::Related => "related".to_string(),
        DepKind::DiscoveredFrom => "discovered-from".to_string(),
    }
}

/// Convert Stamp to RFC3339 string.
fn stamp_to_rfc3339(stamp: &crate::core::time::Stamp) -> String {
    write_stamp_to_rfc3339(&stamp.at)
}

/// Convert WriteStamp to RFC3339 string.
fn write_stamp_to_rfc3339(ws: &crate::core::time::WriteStamp) -> String {
    use chrono::{TimeZone, Utc};

    let secs = (ws.wall_ms / 1000) as i64;
    let nanos = ((ws.wall_ms % 1000) * 1_000_000) as u32;

    match Utc.timestamp_opt(secs, nanos) {
        chrono::LocalResult::Single(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        _ => "1970-01-01T00:00:00.000000Z".to_string(), // fallback for invalid timestamps
    }
}

/// Check if a bead is blocked by unfinished dependencies.
pub fn is_bead_blocked(bead_id: &crate::core::identity::BeadId, state: &CanonicalState) -> bool {
    // Get all dependencies where this bead depends on something
    for (key, edge) in state.deps_from(bead_id) {
        if !edge.is_active() {
            continue;
        }

        // Only Blocks kind actually blocks
        if key.kind() != DepKind::Blocks {
            continue;
        }

        // Check if the target bead is not closed
        if let Some(target) = state.get(key.to())
            && !target.fields.workflow.value.is_closed()
        {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_stamp_to_rfc3339() {
        let ws = crate::core::time::WriteStamp::new(1700000000000, 0); // Nov 14, 2023
        let rfc = write_stamp_to_rfc3339(&ws);
        assert!(rfc.starts_with("2023-11-14"));
        assert!(rfc.ends_with("Z"));
    }

    #[test]
    fn test_dep_kind_mapping() {
        assert_eq!(dep_kind_to_go_type(DepKind::Blocks), "blocks");
        assert_eq!(dep_kind_to_go_type(DepKind::Parent), "parent-child");
        assert_eq!(dep_kind_to_go_type(DepKind::Related), "related");
        assert_eq!(
            dep_kind_to_go_type(DepKind::DiscoveredFrom),
            "discovered-from"
        );
    }

    #[test]
    fn test_derive_status() {
        use crate::core::composite::Closure;

        assert_eq!(derive_status(&Workflow::Open, false), "open");
        assert_eq!(derive_status(&Workflow::Open, true), "blocked");
        assert_eq!(derive_status(&Workflow::InProgress, false), "in_progress");
        assert_eq!(derive_status(&Workflow::InProgress, true), "in_progress");

        let closure = Closure::new(Some("done".into()), None);
        assert_eq!(derive_status(&Workflow::Closed(closure), false), "closed");
    }
}
