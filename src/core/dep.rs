//! Layer 6: Dependency edges
//!
//! DepKey: identity tuple (from, to, kind)
//! DepEdge: key + created + life (active/deleted via LWW)
//!
//! The `DepLife` enum with `Lww<DepLife>` makes delete-vs-restore
//! pure LWW comparison - algebraically cleaner than `Option<Stamp>`.

use serde::{Deserialize, Serialize};

use super::crdt::Lww;
use super::domain::DepKind;
use super::identity::BeadId;
use super::time::Stamp;

/// Dependency identity tuple.
///
/// Deps are unique by (from, to, kind).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DepKey {
    pub from: BeadId,
    pub to: BeadId,
    pub kind: DepKind,
}

impl DepKey {
    pub fn new(from: BeadId, to: BeadId, kind: DepKind) -> Self {
        Self { from, to, kind }
    }
}

/// Dependency lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DepLife {
    Active,
    Deleted,
}

impl Default for DepLife {
    fn default() -> Self {
        DepLife::Active
    }
}

/// Dependency edge with LWW lifecycle.
///
/// Merge semantics:
/// - `life` uses pure LWW comparison (higher stamp wins)
/// - Keep earlier `created` for stable provenance
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepEdge {
    pub key: DepKey,
    pub created: Stamp,
    pub life: Lww<DepLife>,
}

impl DepEdge {
    /// Create a new active dependency.
    pub fn new(key: DepKey, created: Stamp) -> Self {
        Self {
            key,
            created: created.clone(),
            life: Lww::new(DepLife::Active, created),
        }
    }

    /// Create a dependency with explicit life state.
    pub fn with_life(key: DepKey, created: Stamp, life: Lww<DepLife>) -> Self {
        Self { key, created, life }
    }

    /// Check if edge is active (not deleted).
    pub fn is_active(&self) -> bool {
        matches!(self.life.value, DepLife::Active)
    }

    /// Check if edge is deleted.
    pub fn is_deleted(&self) -> bool {
        matches!(self.life.value, DepLife::Deleted)
    }

    /// Mark edge as deleted with the given stamp.
    pub fn delete(&mut self, stamp: Stamp) {
        // Only update if this stamp is newer
        let new_life = Lww::new(DepLife::Deleted, stamp);
        self.life = Lww::join(&self.life, &new_life);
    }

    /// Restore a deleted edge (undelete) with the given stamp.
    pub fn restore(&mut self, stamp: Stamp) {
        // Only update if this stamp is newer
        let new_life = Lww::new(DepLife::Active, stamp);
        self.life = Lww::join(&self.life, &new_life);
    }

    /// Get deleted stamp if the edge is deleted.
    pub fn deleted_stamp(&self) -> Option<&Stamp> {
        if self.is_deleted() {
            Some(&self.life.stamp)
        } else {
            None
        }
    }

    /// Merge two edges with same key.
    ///
    /// - Life uses pure LWW comparison (higher stamp wins)
    /// - Keep earlier created for stable provenance
    pub fn join(a: &Self, b: &Self) -> Self {
        debug_assert_eq!(a.key, b.key, "join requires same key");

        // Keep earlier created for stable provenance
        let created = if a.created <= b.created {
            a.created.clone()
        } else {
            b.created.clone()
        };

        // Life is pure LWW
        let life = Lww::join(&a.life, &b.life);

        Self {
            key: a.key.clone(),
            created,
            life,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ActorId, WriteStamp};

    fn make_stamp(wall_ms: u64, actor: &str) -> Stamp {
        Stamp::new(WriteStamp::new(wall_ms, 0), ActorId::new(actor).unwrap())
    }

    fn make_key() -> DepKey {
        DepKey::new(
            BeadId::parse("bd-abc").unwrap(),
            BeadId::parse("bd-xyz").unwrap(),
            DepKind::Blocks,
        )
    }

    #[test]
    fn new_edge_is_active() {
        let key = make_key();
        let stamp = make_stamp(1000, "alice");
        let edge = DepEdge::new(key, stamp);
        assert!(edge.is_active());
        assert!(!edge.is_deleted());
    }

    #[test]
    fn delete_marks_deleted() {
        let key = make_key();
        let stamp = make_stamp(1000, "alice");
        let mut edge = DepEdge::new(key, stamp);

        let delete_stamp = make_stamp(2000, "bob");
        edge.delete(delete_stamp.clone());

        assert!(edge.is_deleted());
        assert_eq!(edge.deleted_stamp(), Some(&delete_stamp));
    }

    #[test]
    fn restore_undeletes() {
        let key = make_key();
        let stamp = make_stamp(1000, "alice");
        let mut edge = DepEdge::new(key, stamp);

        edge.delete(make_stamp(2000, "bob"));
        assert!(edge.is_deleted());

        edge.restore(make_stamp(3000, "charlie"));
        assert!(edge.is_active());
    }

    #[test]
    fn join_lww_later_wins() {
        let key = make_key();
        let stamp = make_stamp(1000, "alice");

        // Edge A: deleted at t=2000
        let mut edge_a = DepEdge::new(key.clone(), stamp.clone());
        edge_a.delete(make_stamp(2000, "bob"));

        // Edge B: active at t=3000 (restored)
        let mut edge_b = DepEdge::new(key.clone(), stamp.clone());
        edge_b.delete(make_stamp(2000, "bob"));
        edge_b.restore(make_stamp(3000, "charlie"));

        // Join: B wins because t=3000 > t=2000
        let merged = DepEdge::join(&edge_a, &edge_b);
        assert!(merged.is_active());
    }

    #[test]
    fn join_keeps_earlier_created() {
        let key = make_key();

        let edge_a = DepEdge::new(key.clone(), make_stamp(1000, "alice"));
        let edge_b = DepEdge::new(key.clone(), make_stamp(2000, "bob"));

        let merged = DepEdge::join(&edge_a, &edge_b);
        assert_eq!(merged.created, make_stamp(1000, "alice"));
    }
}
