//! Layer 3: LWW (Last-Writer-Wins) CRDT
//!
//! The fundamental merge primitive for scalar/atomic fields.

use serde::{Deserialize, Serialize};

use super::time::Stamp;

/// A type that can be merged with another instance of itself deterministically.
///
/// Properties:
/// - Commutative: join(a, b) == join(b, a)
/// - Associative: join(join(a, b), c) == join(a, join(b, c))
/// - Idempotent: join(a, a) == a
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
pub mod tests {
    use super::*;
    use crate::identity::ActorId;
    use crate::time::WriteStamp;
    use std::fmt::Debug;

    /// Verify CRDT laws for a given type.
    ///
    /// - Commutative: join(a, b) == join(b, a)
    /// - Associative: join(join(a, b), c) == join(a, join(b, c))
    /// - Idempotent: join(a, a) == a
    pub fn crdt_laws<T: Crdt + PartialEq + Debug>(a: &T, b: &T, c: &T) {
        // Idempotence
        assert_eq!(a.join(a), *a, "idempotence failed for a");
        assert_eq!(b.join(b), *b, "idempotence failed for b");
        assert_eq!(c.join(c), *c, "idempotence failed for c");

        // Commutativity
        assert_eq!(a.join(b), b.join(a), "commutativity failed for a, b");
        assert_eq!(a.join(c), c.join(a), "commutativity failed for a, c");
        assert_eq!(b.join(c), c.join(b), "commutativity failed for b, c");

        // Associativity
        assert_eq!(
            a.join(b).join(c),
            a.join(&b.join(c)),
            "associativity failed for a, b, c"
        );
    }

    fn make_lww(val: &str, time: u64, actor: &str) -> Lww<String> {
        Lww::new(
            val.to_string(),
            Stamp::new(
                WriteStamp::new(time, 0),
                ActorId::new(actor).expect("valid actor"),
            ),
        )
    }

    #[test]
    fn lww_satisfies_laws() {
        let a = make_lww("a", 100, "alice");
        let b = make_lww("b", 200, "bob");
        let c = make_lww("c", 150, "carol");

        crdt_laws(&a, &b, &c);
    }
}
