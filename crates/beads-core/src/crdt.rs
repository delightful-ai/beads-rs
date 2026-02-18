//! Layer 3: CRDT Traits and Primitives
//!
//! The fundamental merge primitive for scalar/atomic fields.

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use super::time::Stamp;

/// A Conflict-Free Replicated Data Type.
///
/// CRDTs support merging concurrent updates deterministically.
///
/// Properties:
/// - Commutative: join(a, b) == join(b, a)
/// - Associative: join(join(a, b), c) == join(a, join(b, c))
/// - Idempotent: join(a, a) == a
pub trait Crdt: Sized {
    /// Merge two states into a new state that includes information from both.
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

impl<T: PartialEq> PartialEq for Lww<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.stamp == other.stamp
    }
}

impl<T: Eq> Eq for Lww<T> {}

#[cfg(test)]
pub mod laws {
    use super::*;

    /// verify CRDT laws: associativity, commutativity, idempotence.
    pub fn check_crdt_laws<T: Crdt + PartialEq + Clone + Debug>(a: T, b: T, c: T) {
        // Idempotence
        assert_eq!(a.join(&a), a, "idempotence failed for {a:?}");

        // Commutativity
        assert_eq!(
            a.join(&b),
            b.join(&a),
            "commutativity failed for {a:?} and {b:?}"
        );

        // Associativity
        assert_eq!(
            a.join(&b).join(&c),
            a.join(&b.join(&c)),
            "associativity failed for {a:?}, {b:?}, {c:?}"
        );
    }
}
