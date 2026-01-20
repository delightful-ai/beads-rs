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
use super::identity::{BeadId, ContentHash};
use super::time::Stamp;

/// Immutable creation provenance.
///
/// Bundles the creation stamp and optional branch name together.
/// These are set at creation and never change.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Creation {
    stamp: Stamp,
    on_branch: Option<String>,
}

impl Creation {
    pub fn new(stamp: Stamp, on_branch: Option<String>) -> Self {
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

    pub fn on_branch(&self) -> Option<&str> {
        self.on_branch.as_deref()
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
    pub fn new(id: BeadId, created: Stamp, created_on_branch: Option<String>) -> Self {
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
    pub fn created_on_branch(&self) -> Option<&str> {
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
    pub(crate) fn all_stamps(&self) -> impl Iterator<Item = &Stamp> {
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

    /// Merge two beads.
    ///
    /// PRECONDITION: Same ID and same core.created (otherwise it's a collision, not a merge).
    /// Returns Err(Collision) if core.created differs.
    pub fn join(a: &Self, b: &Self) -> Result<Self, CoreError> {
        // ID collision detection: different created = collision, not merge
        if a.core.created() != b.core.created() {
            return Err(CollisionError {
                id: a.core.id.as_str().to_string(),
            }
            .into());
        }

        Ok(Self {
            core: a.core.clone(), // immutable, should be identical
            fields: BeadFields::join(&a.fields, &b.fields),
        })
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
        h.update(branch.as_bytes());
    }
    h.update([0]);

    // closed_at/by/reason/on_branch
    if let Workflow::Closed(closure) = &bead.fields.workflow.value {
        if let Some(at) = closure.closed_at() {
            h.update(at.wall_ms.to_string().as_bytes());
            h.update(b",");
            h.update(at.counter.to_string().as_bytes());
        }
        h.update([0]);
        if let Some(by) = closure.closed_by() {
            h.update(by.as_str().as_bytes());
        }
        h.update([0]);
        if let Some(reason) = closure.reason() {
            h.update(reason.as_bytes());
        }
        h.update([0]);
        if let Some(branch) = closure.closed_on_branch() {
            h.update(branch.as_bytes());
        }
    }
    h.update([0]);

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

    ContentHash::from_sha256(h.finalize().into())
}
