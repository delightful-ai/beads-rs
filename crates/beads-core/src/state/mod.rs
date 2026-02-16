//! Layer 9: Canonical State
//!
//! The single source of truth for beads, tombstones, and deps.
//!
//! INVARIANT: each BeadId maps to either a live bead or a global tombstone.
//! This is structural via BeadEntry; lineage-scoped collision tombstones are separate.
//!
//! Collision tombstones are lineage-scoped and may coexist with a live bead of
//! the same ID when the live bead is a different lineage (SPEC §4.1.1).
//!
//! Resurrection rule: modification strictly newer than deletion can resurrect.

use crate::time::Stamp;

pub mod canonical;
pub mod deps;
pub mod labels;
pub mod notes;

pub use canonical::{
    CanonicalState, LiveLookupError, bead_collision_cmp, bead_content_hash_for_collision,
    legacy_fallback_lineage,
};
pub use deps::{DepIndexes, DepStore};
pub use labels::{LabelState, LabelStore};
pub use notes::{NoteStore, note_collision_cmp};

pub(crate) fn max_stamp(a: Option<&Stamp>, b: Option<&Stamp>) -> Option<Stamp> {
    match (a, b) {
        (Some(left), Some(right)) => Some(if left >= right {
            left.clone()
        } else {
            right.clone()
        }),
        (Some(stamp), None) | (None, Some(stamp)) => Some(stamp.clone()),
        (None, None) => None,
    }
}
