//! Layer 7: Tombstone
//!
//! Soft-delete record for a bead.

use serde::{Deserialize, Serialize};

use super::crdt::Crdt;
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

    /// Merge: keep later deletion stamp.
    pub fn join(a: &Self, b: &Self) -> Self {
        Crdt::join(a, b)
    }
}

impl Crdt for Tombstone {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::ActorId;
    use crate::time::WriteStamp;
    use proptest::prelude::*;

    fn make_stamp(wall_ms: u64, counter: u32, actor: &str) -> Stamp {
        Stamp::new(
            WriteStamp::new(wall_ms, counter),
            ActorId::new(actor).unwrap(),
        )
    }

    fn bead_id(id: &str) -> BeadId {
        BeadId::parse(id).unwrap()
    }

    #[test]
    fn test_join_keeps_later_deletion() {
        let id = bead_id("bd-test");
        let stamp1 = make_stamp(1000, 0, "alice");
        let stamp2 = make_stamp(2000, 0, "alice");

        let t1 = Tombstone::new(id.clone(), stamp1, None);
        let t2 = Tombstone::new(id.clone(), stamp2, None);

        let merged = Tombstone::join(&t1, &t2);
        assert_eq!(merged.deleted.at.wall_ms, 2000);

        // Commutativity check for distinct stamps
        let merged_rev = Tombstone::join(&t2, &t1);
        assert_eq!(merged_rev.deleted.at.wall_ms, 2000);
    }

    #[test]
    fn test_join_same_stamp_left_wins() {
        let id = bead_id("bd-test");
        let stamp = make_stamp(1000, 0, "alice");

        let t1 = Tombstone::new(id.clone(), stamp.clone(), Some("reason A".to_string()));
        let t2 = Tombstone::new(id.clone(), stamp.clone(), Some("reason B".to_string()));

        let merged = Tombstone::join(&t1, &t2);
        assert_eq!(merged.reason.as_deref(), Some("reason A"));

        let merged_rev = Tombstone::join(&t2, &t1);
        assert_eq!(merged_rev.reason.as_deref(), Some("reason B"));
    }

    #[test]
    #[should_panic(expected = "join requires same id")]
    fn test_join_different_id_panics() {
        let t1 = Tombstone::new(bead_id("bd-a"), make_stamp(1000, 0, "alice"), None);
        let t2 = Tombstone::new(bead_id("bd-b"), make_stamp(2000, 0, "bob"), None);
        Tombstone::join(&t1, &t2);
    }

    #[test]
    #[should_panic(expected = "join requires same lineage")]
    fn test_join_different_lineage_panics() {
        let id = bead_id("bd-test");
        let stamp = make_stamp(1000, 0, "alice");

        let t1 = Tombstone::new(id.clone(), stamp.clone(), None);
        let t2 = Tombstone::new_collision(
            id.clone(),
            stamp.clone(),
            make_stamp(500, 0, "bob"), // lineage
            None,
        );
        Tombstone::join(&t1, &t2);
    }

    fn stamp_strategy() -> impl Strategy<Value = Stamp> {
        let actor = prop_oneof![Just("alice"), Just("bob"), Just("carol")];
        (0u64..10000, 0u32..10, actor).prop_map(|(wall_ms, counter, actor)| {
            Stamp::new(
                WriteStamp::new(wall_ms, counter),
                ActorId::new(actor).unwrap(),
            )
        })
    }

    fn tombstone_strategy() -> impl Strategy<Value = Tombstone> {
        (
            Just("bd-prop-test"),
            stamp_strategy(),
            prop::option::of(any::<String>()),
            prop::option::of(stamp_strategy()),
        )
            .prop_map(|(id_str, deleted, reason, lineage)| {
                let id = BeadId::parse(id_str).unwrap();
                if let Some(l) = lineage {
                    Tombstone::new_collision(id, deleted, l, reason)
                } else {
                    Tombstone::new(id, deleted, reason)
                }
            })
    }

    proptest! {
        #[test]
        fn prop_join_commutative_with_distinct_stamps(
            t1 in tombstone_strategy(),
            mut t2 in tombstone_strategy()
        ) {
            // Ensure same ID and lineage for valid join
            t2.id = t1.id.clone();
            t2.lineage = t1.lineage.clone();

            // Ensure distinct deleted stamps to guarantee commutativity
            if t1.deleted == t2.deleted {
                // If stamps equal, modify one to be different
                t2.deleted.at.counter += 1;
            }

            let m1 = Tombstone::join(&t1, &t2);
            let m2 = Tombstone::join(&t2, &t1);

            assert_eq!(m1, m2);
            assert!(m1.deleted >= t1.deleted);
            assert!(m1.deleted >= t2.deleted);
        }

        #[test]
        fn prop_join_idempotent(t in tombstone_strategy()) {
            let merged = Tombstone::join(&t, &t);
            assert_eq!(merged, t);
        }

        #[test]
        fn prop_join_associative(
            t1 in tombstone_strategy(),
            mut t2 in tombstone_strategy(),
            mut t3 in tombstone_strategy()
        ) {
            // Ensure same ID and lineage
            t2.id = t1.id.clone();
            t2.lineage = t1.lineage.clone();
            t3.id = t1.id.clone();
            t3.lineage = t1.lineage.clone();

            let m1 = Tombstone::join(&Tombstone::join(&t1, &t2), &t3);
            let m2 = Tombstone::join(&t1, &Tombstone::join(&t2, &t3));

            assert_eq!(m1, m2);
        }
    }
}
