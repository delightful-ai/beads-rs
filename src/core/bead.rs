//! Layer 8: The Bead
//!
//! Creation: immutable provenance (stamp + branch)
//! BeadCore: identity + creation
//! BeadFields: all mutable fields wrapped in Lww<T>
//! Bead: core + fields + notes

use serde::{Deserialize, Serialize};

use super::collections::{Labels, NoteLog};
use super::composite::{Claim, Workflow};
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
    pub labels: Lww<Labels>,
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
            labels: Lww::join(&a.labels, &b.labels),
            external_ref: Lww::join(&a.external_ref, &b.external_ref),
            source_repo: Lww::join(&a.source_repo, &b.source_repo),
            estimated_minutes: Lww::join(&a.estimated_minutes, &b.estimated_minutes),
            workflow: Lww::join(&a.workflow, &b.workflow),
            claim: Lww::join(&a.claim, &b.claim),
        }
    }

    /// Collect all stamps for computing updated_stamp.
    fn all_stamps(&self) -> impl Iterator<Item = &Stamp> {
        [
            &self.title.stamp,
            &self.description.stamp,
            &self.design.stamp,
            &self.acceptance_criteria.stamp,
            &self.priority.stamp,
            &self.bead_type.stamp,
            &self.labels.stamp,
            &self.external_ref.stamp,
            &self.source_repo.stamp,
            &self.estimated_minutes.stamp,
            &self.workflow.stamp,
            &self.claim.stamp,
        ]
        .into_iter()
    }
}

/// The Bead - core + fields + notes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bead {
    pub core: BeadCore,
    pub fields: BeadFields,
    pub notes: NoteLog,
}

impl Bead {
    pub fn new(core: BeadCore, fields: BeadFields) -> Self {
        Self {
            core,
            fields,
            notes: NoteLog::new(),
        }
    }

    /// Derived: max stamp across all fields + notes.
    ///
    /// This is the "last modified" time for the bead.
    pub fn updated_stamp(&self) -> Stamp {
        let field_max = self.fields.all_stamps().max();

        let note_max = self
            .notes
            .iter()
            .map(|(_, note)| Stamp::new(note.at.clone(), note.author.clone()))
            .max();

        match (field_max, note_max) {
            (Some(f), Some(n)) => {
                if *f >= n {
                    f.clone()
                } else {
                    n
                }
            }
            (Some(f), None) => f.clone(),
            (None, Some(n)) => n,
            (None, None) => self.core.created().clone(),
        }
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
            notes: NoteLog::join(&a.notes, &b.notes),
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

    /// Compute content hash for CAS (optimistic concurrency).
    ///
    /// SHA256 of public content fields (binary format with \0 separators).
    ///
    /// Fields included (per SPEC §8.5.1):
    /// - id, title, description, status, priority, type
    /// - labels (sorted)
    /// - assignee, assignee_expires
    /// - design, acceptance_criteria
    /// - notes (sorted by id)
    /// - created_at, created_by, created_on_branch
    /// - closed_at, closed_by, closed_reason, closed_on_branch
    /// - external_ref, source_repo
    ///
    /// Excluded: content_hash, updated_at, updated_by, assignee_at, _at/_by/_v
    pub fn content_hash(&self) -> ContentHash {
        use sha2::{Digest, Sha256};

        let mut h = Sha256::new();

        // id
        h.update(self.core.id.as_str().as_bytes());
        h.update([0]);

        // title
        h.update(self.fields.title.value.as_bytes());
        h.update([0]);

        // description
        h.update(self.fields.description.value.as_bytes());
        h.update([0]);

        // status
        h.update(self.fields.workflow.value.status().as_bytes());
        h.update([0]);

        // priority (as decimal)
        h.update(self.fields.priority.value.value().to_string().as_bytes());
        h.update([0]);

        // bead_type
        h.update(self.fields.bead_type.value.as_str().as_bytes());
        h.update([0]);

        // labels (sorted for determinism)
        for label in self.fields.labels.value.iter() {
            h.update(label.as_str().as_bytes());
            h.update(b",");
        }
        h.update([0]);

        // assignee (from claim if present)
        if let Some(assignee) = self.fields.claim.value.assignee() {
            h.update(assignee.as_str().as_bytes());
        }
        h.update([0]);

        // assignee_expires (wall clock ms if present)
        if let Some(expires) = self.fields.claim.value.expires() {
            h.update(expires.0.to_string().as_bytes());
        }
        h.update([0]);

        // design
        if let Some(ref design) = self.fields.design.value {
            h.update(design.as_bytes());
        }
        h.update([0]);

        // acceptance_criteria
        if let Some(ref ac) = self.fields.acceptance_criteria.value {
            h.update(ac.as_bytes());
        }
        h.update([0]);

        // notes (sorted by id for determinism)
        for note in self.notes.sorted() {
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
        h.update(self.core.created().at.wall_ms.to_string().as_bytes());
        h.update(b",");
        h.update(self.core.created().at.counter.to_string().as_bytes());
        h.update([0]);

        // created_by
        h.update(self.core.created().by.as_str().as_bytes());
        h.update([0]);

        // created_on_branch
        if let Some(branch) = self.core.created_on_branch() {
            h.update(branch.as_bytes());
        }
        h.update([0]);

        // closed_at, closed_by, closed_reason, closed_on_branch (from Closure if closed)
        if let Some(closure) = self.fields.workflow.value.closure() {
            // closed_at (from workflow stamp)
            h.update(self.fields.workflow.stamp.at.wall_ms.to_string().as_bytes());
            h.update(b",");
            h.update(self.fields.workflow.stamp.at.counter.to_string().as_bytes());
            h.update([0]);

            // closed_by (from workflow stamp)
            h.update(self.fields.workflow.stamp.by.as_str().as_bytes());
            h.update([0]);

            // closed_reason
            if let Some(ref reason) = closure.reason {
                h.update(reason.as_bytes());
            }
            h.update([0]);

            // closed_on_branch
            if let Some(ref branch) = closure.on_branch {
                h.update(branch.as_bytes());
            }
            h.update([0]);
        } else {
            // Not closed - empty fields
            h.update([0]); // closed_at
            h.update([0]); // closed_by
            h.update([0]); // closed_reason
            h.update([0]); // closed_on_branch
        }

        // external_ref
        if let Some(ref ext) = self.fields.external_ref.value {
            h.update(ext.as_bytes());
        }
        h.update([0]);

        // source_repo
        if let Some(ref sr) = self.fields.source_repo.value {
            h.update(sr.as_bytes());
        }

        ContentHash::from_bytes(h.finalize().into())
    }
}
