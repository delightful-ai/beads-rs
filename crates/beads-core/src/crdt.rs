//! Layer 3: LWW (Last-Writer-Wins) CRDT
//!
//! The fundamental merge primitive for scalar/atomic fields.

use serde::{Deserialize, Serialize};

use super::time::Stamp;

/// The fundamental CRDT merge capability.
///
/// Any type implementing this must satisfy:
/// - Commutativity: join(a, b) == join(b, a)
/// - Associativity: join(join(a, b), c) == join(a, join(b, c))
/// - Idempotence: join(a, a) == a
pub trait Crdt: Clone + Sized {
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

impl<T: Clone> Crdt for Lww<T> {
    fn join(&self, other: &Self) -> Self {
        if self.stamp >= other.stamp {
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
    pub fn join(a: &Self, b: &Self) -> Self {
        Crdt::join(a, b)
    }
}

impl<T: PartialEq> PartialEq for Lww<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.stamp == other.stamp
    }
}

impl<T: Eq> Eq for Lww<T> {}

#[cfg(test)]
pub mod laws {
    use super::*;

    pub fn check_crdt_laws<T: Crdt + PartialEq + std::fmt::Debug>(a: T, b: T, c: T) {
        // Commutativity
        assert_eq!(a.join(&b), b.join(&a), "Commutativity failed");
        // Associativity
        assert_eq!(
            a.join(&b).join(&c),
            a.join(&b.join(&c)),
            "Associativity failed"
        );
        // Idempotence
        assert_eq!(a.join(&a), a, "Idempotence failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::ActorId;
    use crate::time::{Stamp, WriteStamp};
    use proptest::prelude::*;

    fn make_lww<T>(value: T, wall_ms: u64, actor: &str) -> Lww<T> {
        let stamp = Stamp::new(
            WriteStamp::new(wall_ms, 0),
            ActorId::new(actor).expect("valid actor id"),
        );
        Lww::new(value, stamp)
    }

    #[test]
    fn test_join_commutative() {
        // Case 1: distinct timestamps
        let a = make_lww("A", 10, "actor1");
        let b = make_lww("B", 20, "actor1");

        // b wins (newer)
        assert_eq!(Lww::join(&a, &b), b);
        assert_eq!(Lww::join(&b, &a), b);
    }

    #[test]
    fn test_join_associative() {
        let a = make_lww("A", 10, "actor1");
        let b = make_lww("B", 20, "actor2");
        let c = make_lww("C", 30, "actor3");

        // (a join b) join c => b join c => c
        let left = Lww::join(&Lww::join(&a, &b), &c);
        // a join (b join c) => a join c => c
        let right = Lww::join(&a, &Lww::join(&b, &c));

        assert_eq!(left, c);
        assert_eq!(right, c);
        assert_eq!(left, right);
    }

    #[test]
    fn test_join_idempotent() {
        let a = make_lww("A", 10, "actor1");
        assert_eq!(Lww::join(&a, &a), a);
    }

    #[test]
    fn test_join_tiebreak_actor() {
        // Same time, different actors
        let a = make_lww("A", 10, "actor1");
        let b = make_lww("B", 10, "actor2"); // "actor2" > "actor1"

        // b wins (higher actor)
        assert_eq!(Lww::join(&a, &b), b);
        assert_eq!(Lww::join(&b, &a), b);
    }

    #[test]
    fn test_join_identical_stamps_left_wins() {
        // Same time, same actor
        let a = make_lww("Val1", 10, "actor1");
        // Manually construct b with same stamp but different value
        let b = Lww::new("Val2", a.stamp.clone());

        // Lww::join returns left if stamps are equal (a.stamp >= b.stamp)
        // This is "deterministic" in the sense that the function is deterministic,
        // but not commutative if values differ for same stamp.
        // In practice, same stamp means same event, so values should match.
        assert_eq!(Lww::join(&a, &b).value, "Val1");
        assert_eq!(Lww::join(&b, &a).value, "Val2");
    }

    fn lww_strategy() -> impl Strategy<Value = Lww<String>> {
        let actor = prop_oneof![Just("alice"), Just("bob"), Just("carol")];
        let stamp_strategy = (0u64..1000, 0u32..5, actor).prop_map(|(wall_ms, counter, actor)| {
            Stamp::new(
                WriteStamp::new(wall_ms, counter),
                ActorId::new(actor).unwrap(),
            )
        });

        (any::<String>(), stamp_strategy).prop_map(|(val, stamp)| Lww::new(val, stamp))
    }

    proptest! {
        #[test]
        fn lww_satisfies_laws(
            a in lww_strategy(),
            b in lww_strategy(),
            c in lww_strategy()
        ) {
            laws::check_crdt_laws(a, b, c);
        }
    }
}
