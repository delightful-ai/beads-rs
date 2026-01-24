//! Layer 3: OR-Set (Observed-Remove Set, ORSWOT-style)
//!
//! OrSet stores add-wins membership with explicit dots and a causal context (DVV).
//!
//! State:
//! - entries: Map<Value, Set<Dot>> (active dots per value)
//! - cc: Dvv (dots observed/removed)
//!
//! Operations:
//! - apply_add(dot, value, op_hash): insert dot unless already dominated by cc
//! - apply_remove(value, ctx): remove dots for value dominated by ctx, merge ctx into cc
//! - join(a, b): merge entries + cc, drop dots dominated by cc, resolve dot collisions
//!
//! Deterministic dot-collision winner (same dot, different values):
//! 1) higher value (Ord)
//! 2) higher sha256(dot || value_bytes)

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256 as Sha256Hasher};

use super::event::Sha256;
use super::identity::ReplicaId;

/// Values stored in an OR-Set must provide a deterministic byte encoding for
/// collision hashing.
pub trait OrSetValue: Ord + Clone {
    fn collision_bytes(&self) -> Vec<u8>;
}

impl OrSetValue for String {
    fn collision_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

/// Dot (replica, counter) uniquely identifies an add operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Dot {
    pub replica: ReplicaId,
    pub counter: u64,
}

/// Dotted version vector.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dvv {
    pub max: BTreeMap<ReplicaId, u64>,
}

impl Dvv {
    pub fn dominates(&self, dot: &Dot) -> bool {
        self.max
            .get(&dot.replica)
            .is_some_and(|seen| *seen >= dot.counter)
    }

    pub fn observe(&mut self, dot: Dot) {
        let entry = self.max.entry(dot.replica).or_insert(0);
        if dot.counter > *entry {
            *entry = dot.counter;
        }
    }

    pub fn join(a: &Self, b: &Self) -> Self {
        let mut merged = a.clone();
        merged.merge(b);
        merged
    }

    pub fn merge(&mut self, other: &Self) {
        self.merge_with_change(other);
    }

    pub fn merge_with_change(&mut self, other: &Self) -> bool {
        let mut changed = false;
        for (replica, counter) in &other.max {
            let entry = self.max.entry(*replica).or_insert(0);
            if *counter > *entry {
                *entry = *counter;
                changed = true;
            }
        }
        changed
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrSetChange<V: Ord + Clone> {
    pub added: BTreeSet<V>,
    pub removed: BTreeSet<V>,
    changed: bool,
}

impl<V: Ord + Clone> Default for OrSetChange<V> {
    fn default() -> Self {
        Self {
            added: BTreeSet::new(),
            removed: BTreeSet::new(),
            changed: false,
        }
    }
}

impl<V: Ord + Clone> OrSetChange<V> {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }

    pub fn changed(&self) -> bool {
        self.changed
    }

    fn from_diff_with_change(
        before: BTreeSet<V>,
        after: BTreeSet<V>,
        internal_changed: bool,
    ) -> Self {
        let added: BTreeSet<V> = after.difference(&before).cloned().collect();
        let removed: BTreeSet<V> = before.difference(&after).cloned().collect();
        let changed = internal_changed || !added.is_empty() || !removed.is_empty();
        Self {
            added,
            removed,
            changed,
        }
    }
}

/// Observed-remove set with dot-based add ops and a causal context.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "V: Serialize + Ord",
    deserialize = "V: Deserialize<'de> + Ord"
))]
pub struct OrSet<V: OrSetValue> {
    entries: BTreeMap<V, BTreeSet<Dot>>,
    cc: Dvv,
}

impl<V: OrSetValue> OrSet<V> {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            cc: Dvv::default(),
        }
    }

    pub(crate) fn from_parts(entries: BTreeMap<V, BTreeSet<Dot>>, cc: Dvv) -> Self {
        Self { entries, cc }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains(&self, value: &V) -> bool {
        self.entries.contains_key(value)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.keys()
    }

    pub fn dots_for(&self, value: &V) -> Option<&BTreeSet<Dot>> {
        self.entries.get(value)
    }

    pub fn cc(&self) -> &Dvv {
        &self.cc
    }

    pub fn apply_add(&mut self, dot: Dot, value: V, _op_hash: Sha256) -> OrSetChange<V> {
        let before = self.membership_set();

        if self.cc.dominates(&dot) {
            return OrSetChange::default();
        }

        let existing_owner = self.owner_of_dot(dot);
        let internal_changed = match existing_owner.as_ref() {
            None => true,
            Some(owner) => {
                if *owner == value {
                    false
                } else if collision_cmp(dot, owner, &value) == Ordering::Less {
                    true
                } else {
                    false
                }
            }
        };

        self.insert_dot(dot, value);
        self.prune_dominated();

        let after = self.membership_set();
        OrSetChange::from_diff_with_change(before, after, internal_changed)
    }

    pub fn apply_remove(&mut self, value: &V, ctx: &Dvv) -> OrSetChange<V> {
        let before = self.membership_set();
        let mut internal_changed = false;

        if let Some(dots) = self.entries.get_mut(value) {
            let before_len = dots.len();
            dots.retain(|dot| !ctx.dominates(dot));
            if dots.len() != before_len {
                internal_changed = true;
            }
            if dots.is_empty() {
                self.entries.remove(value);
            }
        }

        if self.cc.merge_with_change(ctx) {
            internal_changed = true;
        }
        self.prune_dominated();

        let after = self.membership_set();
        OrSetChange::from_diff_with_change(before, after, internal_changed)
    }

    pub fn join(a: &Self, b: &Self) -> Self {
        let mut entries = a.entries.clone();
        for (value, dots) in &b.entries {
            entries
                .entry(value.clone())
                .or_default()
                .extend(dots.iter().copied());
        }

        let mut merged = Self {
            entries,
            cc: Dvv::join(&a.cc, &b.cc),
        };

        merged.prune_dominated();
        merged.resolve_all_collisions();
        merged
    }

    pub fn merge(&mut self, other: &Self) -> OrSetChange<V> {
        let before = self.membership_set();
        let before_cc = self.cc.clone();
        let before_entries = self.entries.clone();
        *self = Self::join(self, other);
        let after = self.membership_set();
        let internal_changed = self.cc != before_cc || self.entries != before_entries;
        OrSetChange::from_diff_with_change(before, after, internal_changed)
    }

    fn membership_set(&self) -> BTreeSet<V> {
        self.entries.keys().cloned().collect()
    }

    fn owner_of_dot(&self, dot: Dot) -> Option<V> {
        self.entries
            .iter()
            .find_map(|(value, dots)| dots.contains(&dot).then(|| value.clone()))
    }

    fn insert_dot(&mut self, dot: Dot, value: V) {
        self.entries.entry(value).or_default().insert(dot);
        self.resolve_dot_collision(dot);
    }

    fn prune_dominated(&mut self) {
        let cc = &self.cc;
        let mut empty = Vec::new();
        for (value, dots) in self.entries.iter_mut() {
            dots.retain(|dot| !cc.dominates(dot));
            if dots.is_empty() {
                empty.push(value.clone());
            }
        }
        for value in empty {
            self.entries.remove(&value);
        }
    }

    fn resolve_dot_collision(&mut self, dot: Dot) {
        let mut values = Vec::new();
        for (value, dots) in &self.entries {
            if dots.contains(&dot) {
                values.push(value.clone());
            }
        }
        if values.len() <= 1 {
            return;
        }
        let winner = values
            .iter()
            .max_by(|a, b| collision_cmp(dot, *a, *b))
            .cloned()
            .expect("winner exists");
        for value in values {
            if value == winner {
                continue;
            }
            if let Some(dots) = self.entries.get_mut(&value) {
                dots.remove(&dot);
                if dots.is_empty() {
                    self.entries.remove(&value);
                }
            }
        }
    }

    fn resolve_all_collisions(&mut self) {
        let mut by_dot: BTreeMap<Dot, Vec<V>> = BTreeMap::new();
        for (value, dots) in &self.entries {
            for dot in dots {
                by_dot.entry(*dot).or_default().push(value.clone());
            }
        }
        for (dot, values) in by_dot {
            if values.len() <= 1 {
                continue;
            }
            let winner = values
                .iter()
                .max_by(|a, b| collision_cmp(dot, *a, *b))
                .cloned()
                .expect("winner exists");
            for value in values {
                if value == winner {
                    continue;
                }
                if let Some(dots) = self.entries.get_mut(&value) {
                    dots.remove(&dot);
                    if dots.is_empty() {
                        self.entries.remove(&value);
                    }
                }
            }
        }
    }
}

impl<V: OrSetValue> Default for OrSet<V> {
    fn default() -> Self {
        Self::new()
    }
}

fn collision_cmp<V: OrSetValue>(dot: Dot, left: &V, right: &V) -> Ordering {
    match left.cmp(right) {
        Ordering::Equal => {
            let left_hash = dot_value_hash(dot, left);
            let right_hash = dot_value_hash(dot, right);
            left_hash.cmp(&right_hash)
        }
        other => other,
    }
}

fn dot_value_hash<V: OrSetValue>(dot: Dot, value: &V) -> [u8; 32] {
    let mut hasher = Sha256Hasher::new();
    hasher.update(dot.replica.as_uuid().as_bytes());
    hasher.update(dot.counter.to_be_bytes());
    hasher.update(value.collision_bytes());
    let out = hasher.finalize();
    let mut buf = [0u8; 32];
    buf.copy_from_slice(&out);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn replica(id: u8) -> ReplicaId {
        ReplicaId::new(Uuid::from_bytes([id; 16]))
    }

    fn dot(replica_seed: u8, counter: u64) -> Dot {
        Dot {
            replica: replica(replica_seed),
            counter,
        }
    }

    #[test]
    fn dvv_dominates_and_join() {
        let mut dvv = Dvv::default();
        dvv.observe(dot(1, 3));
        assert!(dvv.dominates(&dot(1, 2)));
        assert!(!dvv.dominates(&dot(1, 4)));

        let mut other = Dvv::default();
        other.observe(dot(1, 5));
        other.observe(dot(2, 1));

        let merged = Dvv::join(&dvv, &other);
        assert!(merged.dominates(&dot(1, 5)));
        assert!(merged.dominates(&dot(2, 1)));
        assert!(merged.dominates(&dot(1, 2)));
    }

    #[test]
    fn dvv_join_is_monotonic_for_dominance() {
        let mut dvv = Dvv::default();
        dvv.observe(dot(1, 2));
        let other = Dvv::default();
        let merged = Dvv::join(&dvv, &other);
        assert!(merged.dominates(&dot(1, 2)));
    }

    #[test]
    fn orset_add_remove_basic() {
        let mut set = OrSet::new();
        let added = set.apply_add(dot(1, 1), "alpha".to_string(), Sha256([0u8; 32]));
        assert!(added.added.contains(&"alpha".to_string()));
        assert!(set.contains(&"alpha".to_string()));

        let mut ctx = Dvv::default();
        ctx.observe(dot(1, 1));
        let removed = set.apply_remove(&"alpha".to_string(), &ctx);
        assert!(removed.removed.contains(&"alpha".to_string()));
        assert!(!set.contains(&"alpha".to_string()));
    }

    #[test]
    fn orset_join_commutative_and_idempotent() {
        let mut a = OrSet::new();
        a.apply_add(dot(1, 1), "a".to_string(), Sha256([1u8; 32]));
        let mut b = OrSet::new();
        b.apply_add(dot(2, 1), "b".to_string(), Sha256([2u8; 32]));

        let ab = OrSet::join(&a, &b);
        let ba = OrSet::join(&b, &a);
        assert_eq!(ab, ba);
        assert_eq!(OrSet::join(&a, &a), a);
    }

    #[test]
    fn orset_collision_picks_higher_value() {
        let mut set = OrSet::new();
        let shared = dot(1, 1);
        set.apply_add(shared, "alpha".to_string(), Sha256([1u8; 32]));
        set.apply_add(shared, "beta".to_string(), Sha256([2u8; 32]));

        assert!(!set.contains(&"alpha".to_string()));
        assert!(set.contains(&"beta".to_string()));
    }

    #[test]
    fn orset_join_drops_dominated_dots() {
        let mut a = OrSet::new();
        let dot_a = dot(1, 1);
        a.apply_add(dot_a, "a".to_string(), Sha256([1u8; 32]));

        let mut ctx = Dvv::default();
        ctx.observe(dot_a);
        let mut b = OrSet::new();
        b.cc = ctx;

        let joined = OrSet::join(&a, &b);
        assert!(!joined.contains(&"a".to_string()));
    }

    #[test]
    fn orset_add_ignores_dominated_dot() {
        let mut set = OrSet::new();
        let mut ctx = Dvv::default();
        ctx.observe(dot(1, 1));
        set.cc = ctx;

        set.apply_add(dot(1, 1), "a".to_string(), Sha256([3u8; 32]));
        assert!(set.is_empty());
    }

    #[test]
    fn orset_add_wins_over_remove_ctx() {
        let mut set = OrSet::new();
        let value = "x".to_string();
        let dot_a = dot(1, 1);
        let dot_b = dot(2, 1);
        set.apply_add(dot_a, value.clone(), Sha256([1u8; 32]));
        set.apply_add(dot_b, value.clone(), Sha256([2u8; 32]));

        let mut ctx = Dvv::default();
        ctx.observe(dot_a);
        set.apply_remove(&value, &ctx);

        assert!(set.contains(&value));
    }

    #[test]
    fn orset_remove_with_ctx_drops_all_dots() {
        let mut set = OrSet::new();
        let value = "y".to_string();
        let dot_a = dot(1, 1);
        let dot_b = dot(2, 1);
        set.apply_add(dot_a, value.clone(), Sha256([4u8; 32]));
        set.apply_add(dot_b, value.clone(), Sha256([5u8; 32]));

        let mut ctx = Dvv::default();
        ctx.observe(dot_a);
        ctx.observe(dot_b);
        set.apply_remove(&value, &ctx);

        assert!(!set.contains(&value));
    }

    #[test]
    fn orset_join_preserves_concurrent_add() {
        let value = "z".to_string();
        let dot_a = dot(1, 1);
        let dot_b = dot(2, 1);

        let mut a = OrSet::new();
        a.apply_add(dot_a, value.clone(), Sha256([6u8; 32]));

        let mut b = OrSet::new();
        b.apply_add(dot_b, value.clone(), Sha256([7u8; 32]));
        let mut ctx = Dvv::default();
        ctx.observe(dot_a);
        b.apply_remove(&value, &ctx);

        let joined = OrSet::join(&a, &b);
        assert!(joined.contains(&value));
    }
}
