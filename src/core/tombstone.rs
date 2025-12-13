//! Layer 7: Tombstone
//!
//! Soft-delete record for a bead.

use serde::{Deserialize, Serialize};

use super::identity::BeadId;
use super::time::Stamp;

/// Tombstone - soft-delete record for a bead.
///
/// Merge: keep later deletion stamp.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    pub id: BeadId,
    pub deleted: Stamp,
    pub reason: Option<String>,
}

impl Tombstone {
    pub fn new(id: BeadId, deleted: Stamp, reason: Option<String>) -> Self {
        Self {
            id,
            deleted,
            reason,
        }
    }

    /// Merge: keep later deletion stamp.
    pub fn join(a: &Self, b: &Self) -> Self {
        debug_assert_eq!(a.id, b.id, "join requires same id");
        if a.deleted >= b.deleted {
            a.clone()
        } else {
            b.clone()
        }
    }
}
