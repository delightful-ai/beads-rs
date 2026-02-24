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
    use crate::identity::ActorId;
    use crate::time::{Stamp, WriteStamp};
    use proptest::prelude::*;

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

    fn make_lww<T>(value: T, wall_ms: u64, actor: &str) -> Lww<T> {
        let stamp = Stamp::new(
            WriteStamp::new(wall_ms, 0),
            ActorId::new(actor).expect("valid actor id"),
        );
        Lww::new(value, stamp)
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

    #[test]
    fn lww_satisfies_laws() {
        assert_crdt_laws(lww_strategy());
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
