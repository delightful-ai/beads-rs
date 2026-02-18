//! Layer 3: LWW (Last-Writer-Wins) CRDT
//!
//! The fundamental merge primitive for scalar/atomic fields.

use serde::{Deserialize, Serialize};

use super::time::Stamp;

/// A Conflict-Free Replicated Data Type that can be merged deterministically.
pub trait Crdt: Sized {
    /// Merge with another state.
    ///
    /// Properties:
    /// - Commutative: a.join(b) == b.join(a)
    /// - Associative: a.join(b).join(c) == a.join(b.join(c))
    /// - Idempotent: a.join(a) == a
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

impl<T: Clone> Lww<T> {
    /// Deterministic merge - higher stamp wins.
    ///
    /// Properties:
    /// - Commutative: join(a, b) == join(b, a)
    /// - Associative: join(join(a, b), c) == join(a, join(b, c))
    /// - Idempotent: join(a, a) == a
    pub fn join(a: &Self, b: &Self) -> Self {
        if a.stamp >= b.stamp {
            a.clone()
        } else {
            b.clone()
        }
    }
}

impl<T: Clone> Crdt for Lww<T> {
    fn join(&self, other: &Self) -> Self {
        Lww::join(self, other)
    }
}

impl<T: PartialEq> PartialEq for Lww<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.stamp == other.stamp
    }
}

impl<T: Eq> Eq for Lww<T> {}
