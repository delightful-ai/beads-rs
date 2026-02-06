//! Layer 8: The Bead
//!
//! Creation: immutable provenance (stamp + branch)
//! BeadCore: identity + creation
//! BeadFields: all mutable fields wrapped in Lww<T>
//! Bead: core + fields

use serde::{Deserialize, Serialize};

use super::collections::Labels;
use super::composite::{Claim, Note, Workflow};
use super::crdt::Lww;
use super::domain::{BeadType, Priority};
use super::error::{CollisionError, CoreError};
use super::identity::{ActorId, BeadId, BranchName, ContentHash};
use super::time::{Stamp, WallClock, WriteStamp};
use super::wire_bead::WorkflowStatus;

/// Immutable creation provenance.
///
/// Bundles the creation stamp and optional branch name together.
/// These are set at creation and never change.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Creation {
    stamp: Stamp,
    on_branch: Option<BranchName>,
}

impl Creation {
    pub fn new(stamp: Stamp, on_branch: Option<BranchName>) -> Self {
        Self { stamp, on_branch }
    }

    pub fn stamp(&self) -> &Stamp {
        &self.stamp
    }

    pub fn at(&self) -> &super::time::WriteStamp {
        &self.stamp.at
    }

    pub fn by(&self) -> &super::identity::ActorId {
        &self.stamp.by
    }

    pub fn on_branch(&self) -> Option<&BranchName> {
        self.on_branch.as_ref()
    }
}

/// Immutable core - set at creation, never changes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadCore {
    pub id: BeadId,
    #[serde(flatten)]
    creation: Creation,
}

impl BeadCore {
    pub fn new(id: BeadId, created: Stamp, created_on_branch: Option<BranchName>) -> Self {
        Self {
            id,
            creation: Creation::new(created, created_on_branch),
        }
    }

    /// Get the creation provenance.
    pub fn creation(&self) -> &Creation {
        &self.creation
    }

    /// Get the creation stamp.
    pub fn created(&self) -> &Stamp {
        self.creation.stamp()
    }

    /// Get the branch the bead was created on.
    pub fn created_on_branch(&self) -> Option<&BranchName> {
        self.creation.on_branch()
    }
}

/// All mutable fields wrapped in Lww for per-field merge.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BeadFields {
    pub title: Lww<String>,
    pub description: Lww<String>,
    pub design: Lww<Option<String>>,
    pub acceptance_criteria: Lww<Option<String>>,
    pub priority: Lww<Priority>,
    pub bead_type: Lww<BeadType>,
    pub external_ref: Lww<Option<String>>,
    pub source_repo: Lww<Option<String>>,
    pub estimated_minutes: Lww<Option<u32>>,
    pub workflow: Lww<Workflow>,
    pub claim: Lww<Claim>,
}

impl BeadFields {
    /// Per-field LWW merge.
    pub fn join(a: &Self, b: &Self) -> Self {
        Self {
            title: Lww::join(&a.title, &b.title),
            description: Lww::join(&a.description, &b.description),
            design: Lww::join(&a.design, &b.design),
            acceptance_criteria: Lww::join(&a.acceptance_criteria, &b.acceptance_criteria),
            priority: Lww::join(&a.priority, &b.priority),
            bead_type: Lww::join(&a.bead_type, &b.bead_type),
            external_ref: Lww::join(&a.external_ref, &b.external_ref),
            source_repo: Lww::join(&a.source_repo, &b.source_repo),
            estimated_minutes: Lww::join(&a.estimated_minutes, &b.estimated_minutes),
            workflow: Lww::join(&a.workflow, &b.workflow),
            claim: Lww::join(&a.claim, &b.claim),
        }
    }

    /// Collect all stamps for computing updated_stamp.
    pub fn all_stamps(&self) -> impl Iterator<Item = &Stamp> {
        [
            &self.title.stamp,
            &self.description.stamp,
            &self.design.stamp,
            &self.acceptance_criteria.stamp,
            &self.priority.stamp,
            &self.bead_type.stamp,
            &self.external_ref.stamp,
            &self.source_repo.stamp,
            &self.estimated_minutes.stamp,
            &self.workflow.stamp,
            &self.claim.stamp,
        ]
        .into_iter()
    }
}

/// The Bead - core + scalar fields.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bead {
    pub core: BeadCore,
    pub fields: BeadFields,
}

impl Bead {
    pub fn new(core: BeadCore, fields: BeadFields) -> Self {
        Self { core, fields }
    }

    /// Check that two beads share the same lineage (ID + creation stamp).
    ///
    /// Returns a typed pair that allows `join` without further runtime checks.
    pub fn same_lineage<'a>(
        a: &'a Self,
        b: &'a Self,
    ) -> Result<(SameLineageBead<'a>, SameLineageBead<'a>), CoreError> {
        if a.core.id != b.core.id || a.core.created() != b.core.created() {
            return Err(CollisionError {
                id: a.core.id.as_str().to_string(),
            }
            .into());
        }
        Ok((SameLineageBead { bead: a }, SameLineageBead { bead: b }))
    }

    // Convenience accessors
    pub fn id(&self) -> &BeadId {
        &self.core.id
    }

    pub fn title(&self) -> &str {
        &self.fields.title.value
    }

    pub fn status(&self) -> &'static str {
        self.fields.workflow.value.status()
    }

    pub fn priority(&self) -> Priority {
        self.fields.priority.value
    }

    pub fn bead_type(&self) -> BeadType {
        self.fields.bead_type.value
    }
}

/// Bead wrapper that proves shared lineage, required for join.
#[derive(Clone, Copy, Debug)]
pub struct SameLineageBead<'a> {
    bead: &'a Bead,
}

impl<'a> SameLineageBead<'a> {
    pub fn join(a: Self, b: Self) -> Result<Bead, CoreError> {
        if a.bead.core.id != b.bead.core.id || a.bead.core.created() != b.bead.core.created() {
            return Err(CollisionError {
                id: a.bead.core.id.as_str().to_string(),
            }
            .into());
        }
        Ok(Bead {
            core: a.bead.core.clone(), // immutable, should be identical
            fields: BeadFields::join(&a.bead.fields, &b.bead.fields),
        })
    }
}

/// Derived view: bead + labels + notes + computed metadata.
#[derive(Clone, Debug)]
pub struct BeadView {
    pub bead: Bead,
    pub labels: Labels,
    pub notes: Vec<Note>,
    pub label_stamp: Option<Stamp>,
    updated_stamp: Stamp,
    content_hash: ContentHash,
}

impl BeadView {
    pub fn new(bead: Bead, labels: Labels, notes: Vec<Note>, label_stamp: Option<Stamp>) -> Self {
        let updated_stamp = compute_updated_stamp(&bead, label_stamp.as_ref(), &notes);
        let content_hash = compute_content_hash(&bead, &labels, &notes);
        Self {
            bead,
            labels,
            notes,
            label_stamp,
            updated_stamp,
            content_hash,
        }
    }

    pub fn updated_stamp(&self) -> &Stamp {
        &self.updated_stamp
    }

    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }
}

/// Derived read projection for bead views.
#[derive(Clone, Debug)]
pub struct BeadProjection {
    pub bead: Bead,
    pub status: WorkflowStatus,
    pub issue_type: BeadType,
    pub labels: Labels,
    pub notes: Vec<Note>,
    pub label_stamp: Option<Stamp>,
    pub updated_stamp: Stamp,
    pub content_hash: ContentHash,
    pub assignee: Option<ActorId>,
    pub assignee_at: Option<WriteStamp>,
    pub assignee_expires: Option<WallClock>,
    pub closed_at: Option<WriteStamp>,
    pub closed_by: Option<ActorId>,
    pub closed_reason: Option<String>,
    pub closed_on_branch: Option<BranchName>,
}

impl BeadProjection {
    pub fn from_view(view: &BeadView) -> Self {
        Self::from(view)
    }

    pub fn status(&self) -> &'static str {
        self.status.as_str()
    }

    pub fn workflow_status(&self) -> WorkflowStatus {
        self.status
    }

    pub fn issue_type(&self) -> BeadType {
        self.issue_type
    }

    pub fn note_count(&self) -> usize {
        self.notes.len()
    }

    pub fn created_at(&self) -> &WriteStamp {
        &self.bead.core.created().at
    }

    pub fn created_by(&self) -> &ActorId {
        &self.bead.core.created().by
    }

    pub fn created_on_branch(&self) -> Option<&BranchName> {
        self.bead.core.created_on_branch()
    }

    pub fn updated_at(&self) -> &WriteStamp {
        &self.updated_stamp.at
    }

    pub fn updated_by(&self) -> &ActorId {
        &self.updated_stamp.by
    }
}

impl From<&BeadView> for BeadProjection {
    fn from(view: &BeadView) -> Self {
        let bead = view.bead.clone();
        let status = WorkflowStatus::from_workflow(&bead.fields.workflow.value);
        let issue_type = bead.fields.bead_type.value;
        let updated_stamp = view.updated_stamp().clone();

        let (assignee, assignee_at, assignee_expires) = match &bead.fields.claim.value {
            Claim::Claimed { assignee, expires } => (
                Some(assignee.clone()),
                Some(bead.fields.claim.stamp.at.clone()),
                *expires,
            ),
            Claim::Unclaimed => (None, None, None),
        };

        let (closed_at, closed_by, closed_reason, closed_on_branch) =
            match &bead.fields.workflow.value {
                Workflow::Closed(closure) => (
                    Some(bead.fields.workflow.stamp.at.clone()),
                    Some(bead.fields.workflow.stamp.by.clone()),
                    closure.reason.clone(),
                    closure.on_branch.clone(),
                ),
                _ => (None, None, None, None),
            };

        Self {
            bead,
            status,
            issue_type,
            labels: view.labels.clone(),
            notes: view.notes.clone(),
            label_stamp: view.label_stamp.clone(),
            updated_stamp,
            content_hash: *view.content_hash(),
            assignee,
            assignee_at,
            assignee_expires,
            closed_at,
            closed_by,
            closed_reason,
            closed_on_branch,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::Label;
    use crate::composite::Closure;
    use crate::identity::ActorId;
    use crate::time::WriteStamp;
    use crate::wire_bead::WorkflowStatus;

    fn stamp(wall_ms: u64, counter: u32, actor: &str) -> Stamp {
        Stamp::new(
            WriteStamp::new(wall_ms, counter),
            ActorId::new(actor).expect("actor id"),
        )
    }

    fn bead(id: &str, created: Stamp) -> Bead {
        let core = BeadCore::new(BeadId::parse(id).expect("bead id"), created.clone(), None);
        let fields = BeadFields {
            title: Lww::new("t".to_string(), created.clone()),
            description: Lww::new("d".to_string(), created.clone()),
            design: Lww::new(None, created.clone()),
            acceptance_criteria: Lww::new(None, created.clone()),
            priority: Lww::new(Priority::default(), created.clone()),
            bead_type: Lww::new(BeadType::Task, created.clone()),
            external_ref: Lww::new(None, created.clone()),
            source_repo: Lww::new(None, created.clone()),
            estimated_minutes: Lww::new(None, created.clone()),
            workflow: Lww::new(Workflow::Open, created.clone()),
            claim: Lww::new(Claim::Unclaimed, created),
        };
        Bead::new(core, fields)
    }

    #[test]
    fn same_lineage_rejects_mismatched_creation() {
        let a = bead("bd-join", stamp(10, 0, "alice"));
        let b = bead("bd-join", stamp(11, 0, "alice"));
        assert!(Bead::same_lineage(&a, &b).is_err());
    }

    #[test]
    fn same_lineage_join_merges_fields() {
        let base = stamp(10, 0, "alice");
        let mut a = bead("bd-join2", base.clone());
        let mut b = bead("bd-join2", base.clone());
        a.fields.title = Lww::new("left".to_string(), base.clone());
        b.fields.title = Lww::new("right".to_string(), stamp(12, 0, "bob"));

        let (sa, sb) = Bead::same_lineage(&a, &b).expect("same lineage");
        let merged = SameLineageBead::join(sa, sb).expect("join");
        assert_eq!(merged.title(), "right");
    }

    #[test]
    fn same_lineage_join_rejects_mixed_wrapper_pairs() {
        let left_base = stamp(10, 0, "alice");
        let right_base = stamp(20, 0, "bob");
        let a1 = bead("bd-join-left", left_base.clone());
        let a2 = bead("bd-join-left", left_base);
        let b1 = bead("bd-join-right", right_base.clone());
        let b2 = bead("bd-join-right", right_base);

        let (left, _) = Bead::same_lineage(&a1, &a2).expect("left pair");
        let (_, right) = Bead::same_lineage(&b1, &b2).expect("right pair");

        assert!(SameLineageBead::join(left, right).is_err());
    }

    #[test]
    fn projection_claim_and_closed_info() {
        let created = stamp(1_000, 0, "alice");
        let mut bead = bead("bd-proj", created.clone());

        let assignee = ActorId::new("bob").expect("assignee");
        let claim_stamp = stamp(1_200, 0, "bob");
        bead.fields.claim = Lww::new(
            Claim::Claimed {
                assignee: assignee.clone(),
                expires: Some(WallClock(9_999)),
            },
            claim_stamp.clone(),
        );

        let closed_by = ActorId::new("carol").expect("closed by");
        let closed_stamp = Stamp::new(WriteStamp::new(1_500, 0), closed_by.clone());
        let closure = Closure::new(
            Some("done".into()),
            Some(BranchName::parse("main").unwrap()),
        );
        bead.fields.workflow = Lww::new(Workflow::Closed(closure.clone()), closed_stamp.clone());

        let view = BeadView::new(bead, Labels::new(), Vec::new(), None);
        let projection = BeadProjection::from_view(&view);

        assert_eq!(projection.status, WorkflowStatus::Closed);
        assert_eq!(projection.issue_type, BeadType::Task);
        assert_eq!(projection.assignee.as_ref(), Some(&assignee));
        assert_eq!(projection.assignee_at, Some(claim_stamp.at.clone()));
        assert_eq!(projection.assignee_expires, Some(WallClock(9_999)));
        assert_eq!(projection.closed_at, Some(closed_stamp.at.clone()));
        assert_eq!(projection.closed_by.as_ref(), Some(&closed_by));
        assert_eq!(projection.closed_reason, closure.reason);
        assert_eq!(projection.closed_on_branch, closure.on_branch);
        assert_eq!(projection.updated_at(), &closed_stamp.at);
        assert_eq!(projection.updated_by(), &closed_by);
    }

    #[test]
    fn projection_includes_labels_and_timestamps() {
        let created = stamp(1_000, 0, "alice");
        let bead = bead("bd-proj-label", created.clone());

        let mut labels = Labels::new();
        labels.insert(Label::parse("urgent").unwrap());
        let label_stamp = stamp(2_000, 0, "bob");

        let view = BeadView::new(bead, labels, Vec::new(), Some(label_stamp.clone()));
        let projection = BeadProjection::from_view(&view);

        assert!(projection.labels.contains("urgent"));
        assert_eq!(projection.label_stamp.as_ref(), Some(&label_stamp));
        assert_eq!(projection.updated_at(), &label_stamp.at);
        assert_eq!(projection.updated_by(), &label_stamp.by);
    }
}

fn compute_updated_stamp(bead: &Bead, label_stamp: Option<&Stamp>, notes: &[Note]) -> Stamp {
    let field_max = bead.fields.all_stamps().max().cloned();
    let note_max = notes
        .iter()
        .map(|note| Stamp::new(note.at.clone(), note.author.clone()))
        .max();

    let mut max_stamp = field_max;
    if let Some(label_stamp) = label_stamp {
        max_stamp = Some(match max_stamp {
            Some(existing) => std::cmp::max(existing, label_stamp.clone()),
            None => label_stamp.clone(),
        });
    }
    if let Some(note_stamp) = note_max {
        max_stamp = Some(match max_stamp {
            Some(existing) => std::cmp::max(existing, note_stamp),
            None => note_stamp,
        });
    }

    max_stamp.unwrap_or_else(|| bead.core.created().clone())
}

fn compute_content_hash(bead: &Bead, labels: &Labels, notes: &[Note]) -> ContentHash {
    use sha2::{Digest, Sha256};

    let mut h = Sha256::new();

    // id
    h.update(bead.core.id.as_str().as_bytes());
    h.update([0]);

    // title
    h.update(bead.fields.title.value.as_bytes());
    h.update([0]);

    // description
    h.update(bead.fields.description.value.as_bytes());
    h.update([0]);

    // status
    h.update(bead.fields.workflow.value.status().as_bytes());
    h.update([0]);

    // priority (as decimal)
    h.update(bead.fields.priority.value.value().to_string().as_bytes());
    h.update([0]);

    // bead_type
    h.update(bead.fields.bead_type.value.as_str().as_bytes());
    h.update([0]);

    // labels (sorted for determinism)
    for label in labels.iter() {
        h.update(label.as_str().as_bytes());
        h.update(b",");
    }
    h.update([0]);

    // assignee (from claim if present)
    if let Some(assignee) = bead.fields.claim.value.assignee() {
        h.update(assignee.as_str().as_bytes());
    }
    h.update([0]);

    // assignee_expires (wall clock ms if present)
    if let Some(expires) = bead.fields.claim.value.expires() {
        h.update(expires.0.to_string().as_bytes());
    }
    h.update([0]);

    // design
    if let Some(ref design) = bead.fields.design.value {
        h.update(design.as_bytes());
    }
    h.update([0]);

    // acceptance_criteria
    if let Some(ref ac) = bead.fields.acceptance_criteria.value {
        h.update(ac.as_bytes());
    }
    h.update([0]);

    // notes (sorted by (at, id) for determinism)
    let mut notes_sorted: Vec<&Note> = notes.iter().collect();
    notes_sorted.sort_by(|a, b| a.at.cmp(&b.at).then_with(|| a.id.cmp(&b.id)));
    for note in notes_sorted {
        h.update(note.id.as_str().as_bytes());
        h.update(b":");
        h.update(note.content.as_bytes());
        h.update(b":");
        h.update(note.author.as_str().as_bytes());
        h.update(b":");
        h.update(note.at.wall_ms.to_string().as_bytes());
        h.update(b",");
        h.update(note.at.counter.to_string().as_bytes());
        h.update(b"\n");
    }
    h.update([0]);

    // created_at (wall_ms,counter)
    h.update(bead.core.created().at.wall_ms.to_string().as_bytes());
    h.update(b",");
    h.update(bead.core.created().at.counter.to_string().as_bytes());
    h.update([0]);

    // created_by
    h.update(bead.core.created().by.as_str().as_bytes());
    h.update([0]);

    // created_on_branch
    if let Some(branch) = bead.core.created_on_branch() {
        h.update(branch.as_str().as_bytes());
    }
    h.update([0]);

    // closed_at/by/reason/on_branch
    if let Workflow::Closed(closure) = &bead.fields.workflow.value {
        let closed_stamp = &bead.fields.workflow.stamp;
        h.update(closed_stamp.at.wall_ms.to_string().as_bytes());
        h.update(b",");
        h.update(closed_stamp.at.counter.to_string().as_bytes());
        h.update([0]);
        h.update(closed_stamp.by.as_str().as_bytes());
        h.update([0]);
        if let Some(reason) = closure.reason.as_ref() {
            h.update(reason.as_bytes());
        }
        h.update([0]);
        if let Some(branch) = closure.on_branch.as_ref() {
            h.update(branch.as_str().as_bytes());
        }
        h.update([0]);
    } else {
        // Not closed - emit 4 null separators for compatibility
        h.update([0]); // closed_at
        h.update([0]); // closed_by
        h.update([0]); // closed_reason
        h.update([0]); // closed_on_branch
    }

    // external_ref
    if let Some(ext) = bead.fields.external_ref.value.as_ref() {
        h.update(ext.as_bytes());
    }
    h.update([0]);

    // source_repo
    if let Some(repo) = bead.fields.source_repo.value.as_ref() {
        h.update(repo.as_bytes());
    }
    h.update([0]);

    ContentHash::from_bytes(h.finalize().into())
}
