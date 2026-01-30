//! Issue and status schemas.

use serde::{Deserialize, Serialize};

use crate::deps::DepEdge;
use beads_core::{
    BeadProjection, BeadView, BranchName, NamespaceId, SegmentId, Tombstone as CoreTombstone,
    WallClock, WriteStamp,
};

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
    WalTailTruncated {
        namespace: NamespaceId,
        #[serde(skip_serializing_if = "Option::is_none")]
        segment_id: Option<SegmentId>,
        truncated_from_offset: u64,
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

impl From<&beads_core::Note> for Note {
    fn from(n: &beads_core::Note) -> Self {
        Self {
            id: n.id.as_str().to_string(),
            content: n.content.clone(),
            author: n.author.as_str().to_string(),
            at: n.at.clone(),
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
    pub namespace: NamespaceId,
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
    pub created_on_branch: Option<BranchName>,

    pub updated_at: WriteStamp,
    pub updated_by: String,

    pub closed_at: Option<WriteStamp>,
    pub closed_by: Option<String>,
    pub closed_reason: Option<String>,
    pub closed_on_branch: Option<BranchName>,

    pub external_ref: Option<String>,
    pub source_repo: Option<String>,

    /// Optional time estimate in minutes (beads-go parity).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_minutes: Option<u32>,

    pub content_hash: String,

    pub notes: Vec<Note>,

    /// Incoming dependencies (edges where this issue is the target).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deps_incoming: Vec<DepEdge>,

    /// Outgoing dependencies (edges where this issue is the source).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deps_outgoing: Vec<DepEdge>,
}

/// Summary issue representation (no note bodies).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssueSummary {
    pub id: String,
    pub namespace: NamespaceId,
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
    pub fn from_projection(namespace: &NamespaceId, projection: &BeadProjection) -> Self {
        let bead = &projection.bead;
        let notes = projection.notes.iter().map(Note::from).collect();

        Self {
            id: bead.core.id.as_str().to_string(),
            namespace: namespace.clone(),
            title: bead.fields.title.value.clone(),
            description: bead.fields.description.value.clone(),
            design: bead.fields.design.value.clone(),
            acceptance_criteria: bead.fields.acceptance_criteria.value.clone(),
            status: projection.status().to_string(),
            priority: bead.fields.priority.value.value(),
            issue_type: bead.fields.bead_type.value.as_str().to_string(),
            labels: projection
                .labels
                .iter()
                .map(|l| l.as_str().to_string())
                .collect(),
            assignee: projection
                .assignee
                .as_ref()
                .map(|assignee| assignee.as_str().to_string()),
            assignee_at: projection.assignee_at.clone(),
            assignee_expires: projection.assignee_expires,
            created_at: projection.created_at().clone(),
            created_by: projection.created_by().as_str().to_string(),
            created_on_branch: projection.created_on_branch().cloned(),
            updated_at: projection.updated_at().clone(),
            updated_by: projection.updated_by().as_str().to_string(),
            closed_at: projection.closed_at.clone(),
            closed_by: projection
                .closed_by
                .as_ref()
                .map(|actor| actor.as_str().to_string()),
            closed_reason: projection.closed_reason.clone(),
            closed_on_branch: projection.closed_on_branch.clone(),
            external_ref: bead.fields.external_ref.value.clone(),
            source_repo: bead.fields.source_repo.value.clone(),
            estimated_minutes: bead.fields.estimated_minutes.value,
            content_hash: projection.content_hash.to_hex(),
            notes,
            deps_incoming: Vec::new(),
            deps_outgoing: Vec::new(),
        }
    }

    pub fn from_view(namespace: &NamespaceId, view: &BeadView) -> Self {
        let projection = BeadProjection::from_view(view);
        Self::from_projection(namespace, &projection)
    }
}

impl IssueSummary {
    pub fn from_projection(namespace: &NamespaceId, projection: &BeadProjection) -> Self {
        let bead = &projection.bead;
        Self {
            id: bead.core.id.as_str().to_string(),
            namespace: namespace.clone(),
            title: bead.fields.title.value.clone(),
            description: bead.fields.description.value.clone(),
            design: bead.fields.design.value.clone(),
            acceptance_criteria: bead.fields.acceptance_criteria.value.clone(),
            status: projection.status().to_string(),
            priority: bead.fields.priority.value.value(),
            issue_type: bead.fields.bead_type.value.as_str().to_string(),
            labels: projection
                .labels
                .iter()
                .map(|l| l.as_str().to_string())
                .collect(),
            assignee: projection
                .assignee
                .as_ref()
                .map(|assignee| assignee.as_str().to_string()),
            assignee_expires: projection.assignee_expires,
            created_at: projection.created_at().clone(),
            created_by: projection.created_by().as_str().to_string(),
            updated_at: projection.updated_at().clone(),
            updated_by: projection.updated_by().as_str().to_string(),
            estimated_minutes: bead.fields.estimated_minutes.value,
            content_hash: projection.content_hash.to_hex(),
            note_count: projection.note_count(),
        }
    }

    pub fn from_view(namespace: &NamespaceId, view: &BeadView) -> Self {
        let projection = BeadProjection::from_view(view);
        Self::from_projection(namespace, &projection)
    }

    pub fn from_issue(issue: &Issue) -> Self {
        Self {
            id: issue.id.clone(),
            namespace: issue.namespace.clone(),
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

#[cfg(test)]
mod tests {
    use super::{Issue, IssueSummary};
    use beads_core::orset::Dot;
    use beads_core::{
        ActorId, Bead, BeadCore, BeadFields, BeadId, BeadType, CanonicalState, Claim, Label, Lww,
        NamespaceId, Note as CoreNote, NoteId, Priority, ReplicaId, Stamp, Workflow, WriteStamp,
    };
    use uuid::Uuid;

    fn actor_id(raw: &str) -> ActorId {
        ActorId::new(raw).unwrap_or_else(|e| panic!("invalid actor id {raw}: {e}"))
    }

    fn bead_id(raw: &str) -> BeadId {
        BeadId::parse(raw).unwrap_or_else(|e| panic!("invalid bead id {raw}: {e}"))
    }

    fn make_bead(id: &BeadId, stamp: &Stamp) -> Bead {
        let core = BeadCore::new(id.clone(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("title".to_string(), stamp.clone()),
            description: Lww::new("desc".to_string(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        Bead::new(core, fields)
    }

    fn dot(replica_byte: u8, counter: u64) -> Dot {
        Dot {
            replica: ReplicaId::from(Uuid::from_bytes([replica_byte; 16])),
            counter,
        }
    }

    #[test]
    fn issue_summary_updated_at_tracks_label_change() {
        let base_stamp = Stamp::new(WriteStamp::new(1_000, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        let id = bead_id("bd-label");
        state
            .insert(make_bead(&id, &base_stamp))
            .expect("insert bead");

        let before_view = state.bead_view(&id).expect("bead view");
        let before = IssueSummary::from_view(&NamespaceId::core(), &before_view);
        assert_eq!(before.updated_at, base_stamp.at);

        let label = Label::parse("urgent").expect("label");
        let label_stamp = Stamp::new(WriteStamp::new(2_000, 0), actor_id("bob"));
        state.apply_label_add(
            id.clone(),
            label,
            dot(1, 1),
            label_stamp.clone(),
            base_stamp.clone(),
        );

        let view = state.bead_view(&id).expect("bead view");
        let summary = IssueSummary::from_view(&NamespaceId::core(), &view);
        assert_eq!(summary.updated_at, label_stamp.at);
        assert_eq!(summary.updated_by, label_stamp.by.as_str());
    }

    #[test]
    fn issue_updated_at_tracks_note_change() {
        let base_stamp = Stamp::new(WriteStamp::new(1_000, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        let id = bead_id("bd-note");
        state
            .insert(make_bead(&id, &base_stamp))
            .expect("insert bead");

        let note_author = actor_id("carol");
        let note = CoreNote::new(
            NoteId::new("note-1").expect("note id"),
            "note".to_string(),
            note_author.clone(),
            WriteStamp::new(3_000, 0),
        );
        state.insert_note(id.clone(), base_stamp.clone(), note);

        let view = state.bead_view(&id).expect("bead view");
        let issue = Issue::from_view(&NamespaceId::core(), &view);
        assert_eq!(issue.updated_at, WriteStamp::new(3_000, 0));
        assert_eq!(issue.updated_by, note_author.as_str());
    }
}
