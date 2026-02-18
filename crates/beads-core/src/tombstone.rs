//! Layer 7: Tombstone
//!
//! Soft-delete record for a bead.

use serde::{Deserialize, Serialize};

use crate::crdt::Crdt;

use super::identity::BeadId;
use super::time::Stamp;

/// Tombstone key.
///
/// - `lineage: None` means a "global" deletion for the ID (normal delete).
/// - `lineage: Some(created_stamp)` means the tombstone applies only to that bead lineage
///   (used for ID collision resolution).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TombstoneKey {
    pub id: BeadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<Stamp>,
}

impl TombstoneKey {
    pub fn global(id: BeadId) -> Self {
        Self { id, lineage: None }
    }

    pub fn lineage(id: BeadId, lineage: Stamp) -> Self {
        Self {
            id,
            lineage: Some(lineage),
        }
    }
}

/// Tombstone - soft-delete record for a bead.
///
/// Merge: keep later deletion stamp.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    pub id: BeadId,
    pub deleted: Stamp,
    pub reason: Option<String>,
    /// Optional lineage scope (bead creation stamp).
    ///
    /// When set, the tombstone applies only to beads whose `core.created()` matches
    /// this stamp, allowing collision tombstones to coexist with the winning bead
    /// that retains the original ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<Stamp>,
}

impl Tombstone {
    pub fn new(id: BeadId, deleted: Stamp, reason: Option<String>) -> Self {
        Self {
            id,
            deleted,
            reason,
            lineage: None,
        }
    }

    pub fn new_collision(
        id: BeadId,
        deleted: Stamp,
        lineage: Stamp,
        reason: Option<String>,
    ) -> Self {
        Self {
            id,
            deleted,
            reason,
            lineage: Some(lineage),
        }
    }

    pub fn key(&self) -> TombstoneKey {
        TombstoneKey {
            id: self.id.clone(),
            lineage: self.lineage.clone(),
        }
    }
}

impl Crdt for Tombstone {
    /// Merge: keep later deletion stamp.
    fn join(&self, other: &Self) -> Self {
        debug_assert_eq!(self.id, other.id, "join requires same id");
        debug_assert_eq!(self.lineage, other.lineage, "join requires same lineage");
        if self.deleted >= other.deleted {
            self.clone()
        } else {
            other.clone()
        }
    }
}
