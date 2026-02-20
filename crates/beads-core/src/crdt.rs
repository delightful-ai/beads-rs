//! Layer 3: LWW (Last-Writer-Wins) CRDT
//!
//! The fundamental merge primitive for scalar/atomic fields.

use serde::{Deserialize, Serialize};

use super::time::Stamp;

/// A Conflict-Free Replicated Data Type (CRDT) that supports merging.
///
/// Implementations must satisfy the semilattice properties:
/// - Commutativity: join(a, b) == join(b, a)
/// - Associativity: join(join(a, b), c) == join(a, join(b, c))
/// - Idempotence: join(a, a) == a
pub trait Crdt {
    /// Merge with another state.
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
    /// Inherent join for backward compatibility or direct usage where trait is not imported.
    /// Prefer Crdt::join.
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
mod tests {
    use super::*;
    use crate::identity::ActorId;
    use crate::time::{Stamp, WriteStamp};

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
        assert_eq!(a.join(&b), b);
        assert_eq!(b.join(&a), b);
    }

    #[test]
    fn test_join_associative() {
        let a = make_lww("A", 10, "actor1");
        let b = make_lww("B", 20, "actor2");
        let c = make_lww("C", 30, "actor3");

        // (a join b) join c => b join c => c
        let left = a.join(&b).join(&c);
        // a join (b join c) => a join c => c
        let right = a.join(&b.join(&c));

        assert_eq!(left, c);
        assert_eq!(right, c);
        assert_eq!(left, right);
    }

    #[test]
    fn test_join_idempotent() {
        let a = make_lww("A", 10, "actor1");
        assert_eq!(a.join(&a), a);
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
    fn test_join_identical_stamps_left_wins() {
        // Same time, same actor
        let a = make_lww("Val1", 10, "actor1");
        // Manually construct b with same stamp but different value
        let b = Lww::new("Val2", a.stamp.clone());

        // Crdt::join returns self if stamps are equal (self.stamp >= other.stamp)
        assert_eq!(a.join(&b).value, "Val1");
        assert_eq!(b.join(&a).value, "Val2");
    }
}
