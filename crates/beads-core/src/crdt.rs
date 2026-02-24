//! Layer 3: CRDT Primitives
//!
//! The fundamental merge primitive for conflict-free replicated data types.

use serde::{Deserialize, Serialize};

use super::time::Stamp;

/// A Conflict-Free Replicated Data Type.
///
/// Implementations must satisfy the semi-lattice properties:
/// - Commutative: join(a, b) == join(b, a)
/// - Associative: join(join(a, b), c) == join(a, join(b, c))
/// - Idempotent: join(a, a) == a
pub trait Crdt: Clone + std::fmt::Debug {
    /// Deterministic merge of two states.
    ///
    /// This operation must be infallible and total.
    fn join(&self, other: &Self) -> Self;
}

/// Last-Writer-Wins register.
///
/// This is your CRDT join for scalar/atomic fields.
/// Higher stamp wins; deterministic (stamp includes actor for tiebreak).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Lww<T> {
    pub value: T,
    pub stamp: Stamp,
}

impl<T> Lww<T> {
    pub fn new(value: T, stamp: Stamp) -> Self {
        Self { value, stamp }
    }
}

impl<T: Clone + std::fmt::Debug + Ord> Crdt for Lww<T> {
    fn join(&self, other: &Self) -> Self {
        if self.stamp > other.stamp {
            self.clone()
        } else if other.stamp > self.stamp {
            other.clone()
        } else if self.value >= other.value {
            self.clone()
        } else {
            other.clone()
        }
    }
}

impl<T: Clone> Lww<T> {
    /// Deterministic merge - higher stamp wins.
    ///
    /// Properties:
    /// - Commutative: join(a, b) == join(b, a)
    /// - Associative: join(join(a, b), c) == join(a, join(b, c))
    /// - Idempotent: join(a, a) == a
    ///
    /// Deprecated: Use Crdt::join instead.
    pub fn join(a: &Self, b: &Self) -> Self
    where
        T: std::fmt::Debug + Ord,
    {
        <Self as Crdt>::join(a, b)
    }
}

impl<T: PartialEq> PartialEq for Lww<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.stamp == other.stamp
    }
}

impl<T: Eq> Eq for Lww<T> {}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::StoreState;
    use crate::collections::Label;
    use crate::composite::Note;
    use crate::dep::DepKey;
    use crate::domain::DepKind;
    use crate::identity::ActorId;
    use crate::identity::{BeadId, NoteId, ReplicaId};
    use crate::namespace::NamespaceId;
    use crate::orset::{Dot, OrSet};
    use crate::state::{CanonicalState, DepStore, LabelState, LabelStore, NoteStore};
    use crate::time::{Stamp, WriteStamp};
    use crate::tombstone::Tombstone;
    use proptest::prelude::*;
    use uuid::Uuid;

    /// Contract tests for CRDT implementations.
    ///
    /// Verifies the three laws: commutativity, associativity, and idempotence.
    pub fn assert_crdt_laws<T, S>(strategy: S)
    where
        T: Crdt + PartialEq + Eq + 'static,
        S: Strategy<Value = T> + 'static,
    {
        let strategy = strategy.boxed();
        proptest!(|(a in strategy.clone(), b in strategy.clone(), c in strategy)| {
            // Commutative
            prop_assert_eq!(a.join(&b), b.join(&a));

            // Associative
            prop_assert_eq!(a.join(&b).join(&c), a.join(&b.join(&c)));

            // Idempotent
            prop_assert_eq!(a.join(&a), a.clone());
        });
    }

    pub fn assert_crdt_laws_by_projection<T, S, F, P>(strategy: S, project: F)
    where
        T: Crdt + 'static,
        S: Strategy<Value = T> + 'static,
        F: Fn(&T) -> P + Copy + 'static,
        P: PartialEq + std::fmt::Debug,
    {
        let strategy = strategy.boxed();
        proptest!(|(a in strategy.clone(), b in strategy.clone(), c in strategy)| {
            let ab = a.join(&b);
            let ba = b.join(&a);
            prop_assert_eq!(project(&ab), project(&ba));

            let ab_c = ab.join(&c);
            let a_bc = a.join(&b.join(&c));
            prop_assert_eq!(project(&ab_c), project(&a_bc));

            let aa = a.join(&a);
            prop_assert_eq!(project(&aa), project(&a));
        });
    }

    fn make_lww<T>(value: T, wall_ms: u64, actor: &str) -> Lww<T> {
        let stamp = Stamp::new(
            WriteStamp::new(wall_ms, 0),
            ActorId::new(actor).expect("valid actor id"),
        );
        Lww::new(value, stamp)
    }

    fn actor_from_idx(idx: u8) -> ActorId {
        let actor = match idx % 3 {
            0 => "alice",
            1 => "bob",
            _ => "carol",
        };
        ActorId::new(actor).expect("valid actor id")
    }

    fn replica_from_idx(idx: u8) -> ReplicaId {
        ReplicaId::new(Uuid::from_u128((idx as u128) + 1))
    }

    fn bead_id_from_idx(idx: u8) -> BeadId {
        let bead_id = format!("bd-law{}", idx % 4);
        BeadId::parse(&bead_id).expect("valid bead id")
    }

    fn lineage_from_idx(idx: u8) -> Stamp {
        Stamp::new(WriteStamp::new(1_000 + idx as u64, 0), actor_from_idx(idx))
    }

    fn debug_projection<T: std::fmt::Debug>(value: &T) -> String {
        format!("{value:?}")
    }

    fn lww_strategy() -> impl Strategy<Value = Lww<String>> {
        let wall_ms = 0u64..1000;
        let actor = prop_oneof![Just("alice"), Just("bob"), Just("carol")];
        // Keep value derivable from stamp identity so equal stamps don't generate
        // inconsistent payload pairs in law tests.
        (wall_ms, actor).prop_map(|(t, a)| make_lww(format!("{t}:{a}"), t, a))
    }

    fn lww_value_strategy() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("A".to_string()),
            Just("B".to_string()),
            Just("C".to_string())
        ]
    }

    fn dvv_strategy() -> impl Strategy<Value = crate::orset::Dvv> {
        prop::collection::vec((0u8..3, 1u64..8), 0..16).prop_map(|entries| {
            crate::orset::Dvv::from_dots(entries.into_iter().map(|(replica_idx, counter)| Dot {
                replica: replica_from_idx(replica_idx),
                counter,
            }))
        })
    }

    fn label_state_strategy() -> impl Strategy<Value = LabelState> {
        let ops = prop::collection::vec((0u8..4, 0u8..3, 1u64..4), 0..8);
        let stamp = prop::option::of((0u64..50, 0u8..3));
        (ops, stamp).prop_map(|(ops, stamp)| {
            let mut set = OrSet::new();
            for (label_idx, replica_idx, counter) in ops {
                let label = Label::parse(format!("label{}", label_idx)).expect("valid label");
                let dot = Dot {
                    replica: replica_from_idx(replica_idx),
                    counter,
                };
                set.apply_add(dot, label);
            }
            let stamp = stamp.map(|(wall_ms, actor_idx)| {
                Stamp::new(WriteStamp::new(wall_ms, 0), actor_from_idx(actor_idx))
            });
            LabelState::from_parts(set, stamp)
        })
    }

    fn label_store_strategy() -> impl Strategy<Value = LabelStore> {
        prop::collection::vec((0u8..4, 0u8..4, label_state_strategy()), 0..8).prop_map(|entries| {
            let mut store = LabelStore::new();
            for (bead_idx, lineage_idx, state) in entries {
                store.insert_state(
                    bead_id_from_idx(bead_idx),
                    lineage_from_idx(lineage_idx),
                    state,
                );
            }
            store
        })
    }

    fn dep_store_strategy() -> impl Strategy<Value = DepStore> {
        let ops = prop::collection::vec((0u8..4, 0u8..4, 0u8..3, 0u8..3, 1u64..4), 0..12);
        let stamp = prop::option::of((0u64..50, 0u8..3));
        (ops, stamp).prop_map(|(ops, stamp)| {
            let mut set = OrSet::new();
            for (from_idx, to_idx, kind_idx, replica_idx, counter) in ops {
                let from = bead_id_from_idx(from_idx);
                let mut to = bead_id_from_idx(to_idx);
                if from == to {
                    to = bead_id_from_idx(to_idx.wrapping_add(1));
                }
                let kind = match kind_idx % 3 {
                    0 => DepKind::Blocks,
                    1 => DepKind::Related,
                    _ => DepKind::Parent,
                };
                let key = DepKey::new(from, to, kind).expect("valid dep key");
                let dot = Dot {
                    replica: replica_from_idx(replica_idx),
                    counter,
                };
                set.apply_add(dot, key);
            }
            let stamp = stamp.map(|(wall_ms, actor_idx)| {
                Stamp::new(WriteStamp::new(wall_ms, 0), actor_from_idx(actor_idx))
            });
            DepStore::from_parts(set, stamp)
        })
    }

    fn note_store_strategy() -> impl Strategy<Value = NoteStore> {
        let entries = prop::collection::vec((0u8..4, 0u8..4, 0u8..4, 0u8..3, 0u64..50), 0..12);
        entries.prop_map(|entries| {
            let mut store = NoteStore::new();
            for (bead_idx, lineage_idx, note_idx, author_idx, at_ms) in entries {
                let bead_id = bead_id_from_idx(bead_idx);
                let lineage = lineage_from_idx(lineage_idx);
                let note_id = NoteId::new(format!("n{note_idx}")).expect("valid note id");
                let content = format!("content-{note_idx}-{author_idx}");
                let note = Note::new(
                    note_id,
                    content,
                    actor_from_idx(author_idx),
                    WriteStamp::new(at_ms, 0),
                );
                let _ = store.insert(bead_id, lineage, note);
            }
            store
        })
    }

    fn canonical_state_strategy() -> impl Strategy<Value = CanonicalState> {
        (
            label_store_strategy(),
            dep_store_strategy(),
            note_store_strategy(),
        )
            .prop_map(|(labels, deps, notes)| {
                let mut state = CanonicalState::new();
                state.set_label_store(labels);
                state.set_dep_store(deps);
                state.set_note_store(notes);
                state
            })
    }

    fn store_state_strategy() -> impl Strategy<Value = StoreState> {
        prop::collection::vec((0u8..3, canonical_state_strategy()), 0..6).prop_map(|entries| {
            let mut state = StoreState::new();
            for (namespace_idx, namespace_state) in entries {
                let namespace = if namespace_idx == 0 {
                    NamespaceId::core()
                } else {
                    NamespaceId::parse(format!("ns{namespace_idx}")).expect("valid namespace")
                };
                state.set_namespace_state(namespace, namespace_state);
            }
            state
        })
    }

    fn tombstone_global_strategy() -> impl Strategy<Value = Tombstone> {
        (
            0u64..100,
            0u8..3,
            prop::option::of(prop_oneof![
                Just("A".to_string()),
                Just("B".to_string()),
                Just("C".to_string())
            ]),
        )
            .prop_map(|(wall_ms, actor_idx, reason)| {
                Tombstone::new(
                    bead_id_from_idx(0),
                    Stamp::new(WriteStamp::new(wall_ms, 0), actor_from_idx(actor_idx)),
                    reason,
                )
            })
    }

    fn tombstone_lineage_strategy() -> impl Strategy<Value = Tombstone> {
        (
            0u64..100,
            0u8..3,
            prop::option::of(prop_oneof![
                Just("A".to_string()),
                Just("B".to_string()),
                Just("C".to_string())
            ]),
        )
            .prop_map(|(wall_ms, actor_idx, reason)| {
                Tombstone::new_collision(
                    bead_id_from_idx(0),
                    Stamp::new(WriteStamp::new(wall_ms, 0), actor_from_idx(actor_idx)),
                    lineage_from_idx(0),
                    reason,
                )
            })
    }

    #[test]
    fn lww_satisfies_laws() {
        assert_crdt_laws(lww_strategy());
    }

    #[test]
    fn dvv_satisfies_laws() {
        assert_crdt_laws(dvv_strategy());
    }

    #[test]
    fn label_state_satisfies_laws() {
        assert_crdt_laws_by_projection(label_state_strategy(), debug_projection);
    }

    #[test]
    fn label_store_satisfies_laws() {
        assert_crdt_laws_by_projection(label_store_strategy(), debug_projection);
    }

    #[test]
    fn dep_store_satisfies_laws() {
        assert_crdt_laws_by_projection(dep_store_strategy(), debug_projection);
    }

    #[test]
    fn note_store_satisfies_laws() {
        assert_crdt_laws_by_projection(note_store_strategy(), debug_projection);
    }

    #[test]
    fn canonical_state_satisfies_laws() {
        assert_crdt_laws_by_projection(canonical_state_strategy(), debug_projection);
    }

    #[test]
    fn store_state_satisfies_laws() {
        assert_crdt_laws_by_projection(store_state_strategy(), debug_projection);
    }

    #[test]
    fn tombstone_global_satisfies_laws() {
        assert_crdt_laws(tombstone_global_strategy());
    }

    #[test]
    fn tombstone_lineage_satisfies_laws() {
        assert_crdt_laws(tombstone_lineage_strategy());
    }

    proptest! {
        #[test]
        fn lww_same_stamp_value_tiebreak_satisfies_laws(
            a_val in lww_value_strategy(),
            b_val in lww_value_strategy(),
            c_val in lww_value_strategy()
        ) {
            let stamp = Stamp::new(
                WriteStamp::new(42, 0),
                ActorId::new("actor1").expect("valid actor id"),
            );
            let a = Lww::new(a_val, stamp.clone());
            let b = Lww::new(b_val, stamp.clone());
            let c = Lww::new(c_val, stamp);

            prop_assert_eq!(a.join(&b), b.join(&a));
            prop_assert_eq!(a.join(&b).join(&c), a.join(&b.join(&c)));
            prop_assert_eq!(a.join(&a), a.clone());
        }
    }

    #[test]
    fn test_join_tiebreak_actor() {
        // Same time, different actors
        let a = make_lww("A", 10, "actor1");
        let b = make_lww("B", 10, "actor2"); // "actor2" > "actor1"

        // b wins (higher actor)
        assert_eq!(a.join(&b), b);
        assert_eq!(b.join(&a), b);
    }

    #[test]
    fn test_join_identical_stamps_value_tiebreak() {
        // Same time, same actor, different values
        let a = make_lww("Val1", 10, "actor1");
        let b = Lww::new("Val2", a.stamp.clone());

        // Should be deterministic based on value (Val2 > Val1)
        assert_eq!(a.join(&b).value, "Val2");
        assert_eq!(b.join(&a).value, "Val2");
    }
}
