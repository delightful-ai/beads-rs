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
use crate::identity::ContentHashable;

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

#[cfg(test)]
impl BeadCore {
    pub fn field_names() -> &'static [&'static str] {
        &["id", "creation"]
    }

    pub fn mutate_field(&mut self, name: &str) {
        match name {
            "id" => self.id.mutate_for_test(),
            "creation" => self.creation.mutate_for_test(),
            _ => panic!("unknown core field: {}", name),
        }
    }
}

/// All mutable fields wrapped in Lww for per-field merge.
macro_rules! define_bead_fields {
    (
        $(pub $name:ident : $type:ty),* $(,)?
    ) => {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct BeadFields {
            $(pub $name: Lww<$type>),*
        }

        impl BeadFields {
            /// Per-field LWW merge.
            pub fn join(a: &Self, b: &Self) -> Self {
                Self {
                    $($name: Lww::join(&a.$name, &b.$name)),*
                }
            }

            /// Collect all stamps for computing updated_stamp.
            pub fn all_stamps(&self) -> impl Iterator<Item = &Stamp> {
                [
                    $(&self.$name.stamp),*
                ]
                .into_iter()
            }
        }

        #[cfg(test)]
        impl BeadFields {
            pub fn field_names() -> &'static [&'static str] {
                &[
                    $(stringify!($name)),*
                ]
            }

            pub fn mutate_field(&mut self, name: &str) {
                match name {
                    $(stringify!($name) => self.$name.value.mutate_for_test(),)*
                    _ => panic!("unknown field: {}", name),
                }
            }
        }
    };
}

define_bead_fields! {
    pub title: String,
    pub description: String,
    pub design: Option<String>,
    pub acceptance_criteria: Option<String>,
    pub priority: Priority,
    pub bead_type: BeadType,
    pub external_ref: Option<String>,
    pub source_repo: Option<String>,
    pub estimated_minutes: Option<u32>,
    pub workflow: Workflow,
    pub claim: Claim,
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
                Workflow::Open | Workflow::InProgress => (None, None, None, None),
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
trait MutateForTest {
    fn mutate_for_test(&mut self);
}

#[cfg(test)]
impl MutateForTest for String {
    fn mutate_for_test(&mut self) {
        self.push_str(" changed");
    }
}

#[cfg(test)]
impl MutateForTest for Option<String> {
    fn mutate_for_test(&mut self) {
        *self = match self.take() {
            Some(s) => Some(s + " changed"),
            None => Some("new".to_string()),
        };
    }
}

#[cfg(test)]
impl MutateForTest for Option<u32> {
    fn mutate_for_test(&mut self) {
        *self = match self.take() {
            Some(v) => Some(v + 1),
            None => Some(10),
        };
    }
}

#[cfg(test)]
impl MutateForTest for Priority {
    fn mutate_for_test(&mut self) {
        // Cycle priority
        let next = (self.value() + 1) % 5;
        *self = Priority::try_from(next).unwrap();
    }
}

#[cfg(test)]
impl MutateForTest for BeadType {
    fn mutate_for_test(&mut self) {
        // Cycle types
        *self = match self {
            BeadType::Bug => BeadType::Feature,
            BeadType::Feature => BeadType::Task,
            BeadType::Task => BeadType::Epic,
            BeadType::Epic => BeadType::Chore,
            BeadType::Chore => BeadType::Bug,
        };
    }
}

#[cfg(test)]
impl MutateForTest for Workflow {
    fn mutate_for_test(&mut self) {
        *self = match self {
            Workflow::Open => Workflow::InProgress,
            Workflow::InProgress => Workflow::Closed(super::composite::Closure::new(
                Some("done".to_string()),
                None,
            )),
            Workflow::Closed(_) => Workflow::Open,
        };
    }
}

#[cfg(test)]
impl MutateForTest for Claim {
    fn mutate_for_test(&mut self) {
        *self = match self {
            Claim::Unclaimed => Claim::claimed(ActorId::new("test").unwrap(), None),
            Claim::Claimed { .. } => Claim::Unclaimed,
        };
    }
}

#[cfg(test)]
impl MutateForTest for BeadId {
    fn mutate_for_test(&mut self) {
        // Change suffix
        *self = BeadId::parse(&format!("{}-modified", self.slug())).unwrap();
    }
}

#[cfg(test)]
impl MutateForTest for Stamp {
    fn mutate_for_test(&mut self) {
        // Increment time
        let new_time = self.at.wall_ms + 1;
        *self = Stamp::new(WriteStamp::new(new_time, self.at.counter), self.by.clone());
    }
}

#[cfg(test)]
impl MutateForTest for BranchName {
    fn mutate_for_test(&mut self) {
        *self = BranchName::parse(format!("{}-mod", self.as_str())).unwrap();
    }
}

#[cfg(test)]
impl MutateForTest for Creation {
    fn mutate_for_test(&mut self) {
        // Mutate inner fields
        self.stamp.mutate_for_test();
        if let Some(branch) = &mut self.on_branch {
            branch.mutate_for_test();
        } else {
            self.on_branch = Some(BranchName::parse("new-branch").unwrap());
        }
    }
}

#[cfg(test)]
impl MutateForTest for BeadCore {
    fn mutate_for_test(&mut self) {
        self.id.mutate_for_test();
        self.creation.mutate_for_test();
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

    #[test]
    fn content_hash_depends_on_all_fields() {
        let base_stamp = stamp(1000, 0, "alice");
        let base_bead = bead("bd-hash-test", base_stamp.clone());
        let labels = Labels::new();
        let notes = Vec::new();

        let base_hash = compute_content_hash(&base_bead, &labels, &notes);

        for field_name in BeadFields::field_names() {
            let mut bead = base_bead.clone();
            // Mutate the field
            bead.fields.mutate_field(field_name);

            // Hash should change
            let new_hash = compute_content_hash(&bead, &labels, &notes);
            assert_ne!(
                base_hash, new_hash,
                "Changing field '{}' did not change content hash. Is it missing from compute_content_hash?",
                field_name
            );
        }
    }

    #[test]
    fn content_hash_depends_on_core_fields() {
        let base_stamp = stamp(1000, 0, "alice");
        let base_bead = bead("bd-hash-test-core", base_stamp.clone());
        let labels = Labels::new();
        let notes = Vec::new();

        let base_hash = compute_content_hash(&base_bead, &labels, &notes);

        for field_name in BeadCore::field_names() {
            let mut bead = base_bead.clone();
            // Mutate the field
            bead.core.mutate_field(field_name);

            // Hash should change
            let new_hash = compute_content_hash(&bead, &labels, &notes);
            assert_ne!(
                base_hash, new_hash,
                "Changing core field '{}' did not change content hash. Is it missing from compute_content_hash?",
                field_name
            );
        }
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
    bead.core.id.hash_content(&mut h);

    // title
    bead.fields.title.value.hash_content(&mut h);

    // description
    bead.fields.description.value.hash_content(&mut h);

    // status
    bead.fields.workflow.value.hash_status(&mut h);

    // priority
    bead.fields.priority.value.hash_content(&mut h);

    // bead_type
    bead.fields.bead_type.value.hash_content(&mut h);

    // labels (sorted for determinism)
    labels.hash_content(&mut h);

    // assignee (from claim if present) & assignee_expires (wall clock ms if present)
    bead.fields.claim.value.hash_content(&mut h);

    // design
    bead.fields.design.value.hash_content(&mut h);

    // acceptance_criteria
    bead.fields.acceptance_criteria.value.hash_content(&mut h);

    // notes (sorted by (at, id) for determinism)
    let mut notes_sorted: Vec<&Note> = notes.iter().collect();
    notes_sorted.sort_by(|a, b| a.at.cmp(&b.at).then_with(|| a.id.cmp(&b.id)));
    for note in notes_sorted {
        note.hash_content(&mut h);
    }
    h.update([0]);

    // created_at (wall_ms,counter)
    bead.core.created().at.hash_content(&mut h);

    // created_by
    bead.core.created().by.hash_content(&mut h);

    // created_on_branch
    if let Some(branch) = bead.core.created_on_branch() {
        branch.hash_content(&mut h);
    } else {
        h.update([0]);
    }

    // closed_at/by/reason/on_branch
    bead.fields
        .workflow
        .value
        .hash_closure(&bead.fields.workflow.stamp, &mut h);

    // external_ref
    bead.fields.external_ref.value.hash_content(&mut h);

    // source_repo
    bead.fields.source_repo.value.hash_content(&mut h);

    // estimated_minutes
    bead.fields.estimated_minutes.value.hash_content(&mut h);

    ContentHash::from_bytes(h.finalize().into())
}
