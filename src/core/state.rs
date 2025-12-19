//! Layer 9: Canonical State
//!
//! The single source of truth for beads, tombstones, and deps.
//!
//! INVARIANT: live ∩ deletion-tombstones = ∅ (enforced by construction)
//!
//! Collision tombstones are lineage-scoped and may coexist with a live bead of
//! the same ID when the live bead is a different lineage (SPEC §4.1.1).
//!
//! Resurrection rule: modification strictly newer than deletion can resurrect.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::bead::Bead;
use super::dep::{DepEdge, DepKey};
use super::domain::DepKind;
use super::error::CoreError;
use super::identity::BeadId;
use super::time::WallClock;
use super::tombstone::{Tombstone, TombstoneKey};

/// Derived indexes for efficient dependency lookups.
///
/// These are rebuilt from `deps` on load and updated incrementally.
/// Not serialized - derived state only.
#[derive(Default, Debug, Clone)]
pub struct DepIndexes {
    /// from -> [(to, kind)] for active deps
    out_edges: BTreeMap<BeadId, Vec<(BeadId, DepKind)>>,
    /// to -> [(from, kind)] for active deps
    in_edges: BTreeMap<BeadId, Vec<(BeadId, DepKind)>>,
}

impl DepIndexes {
    /// Create empty indexes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an edge to both indexes.
    fn add(&mut self, from: &BeadId, to: &BeadId, kind: DepKind) {
        self.out_edges
            .entry(from.clone())
            .or_default()
            .push((to.clone(), kind));
        self.in_edges
            .entry(to.clone())
            .or_default()
            .push((from.clone(), kind));
    }

    /// Remove an edge from both indexes.
    fn remove(&mut self, from: &BeadId, to: &BeadId, kind: DepKind) {
        if let Some(edges) = self.out_edges.get_mut(from) {
            edges.retain(|(t, k)| !(t == to && *k == kind));
        }
        if let Some(edges) = self.in_edges.get_mut(to) {
            edges.retain(|(f, k)| !(f == from && *k == kind));
        }
    }

    /// Get outgoing edges from a bead.
    pub fn out_edges(&self, id: &BeadId) -> &[(BeadId, DepKind)] {
        self.out_edges.get(id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get incoming edges to a bead.
    pub fn in_edges(&self, id: &BeadId) -> &[(BeadId, DepKind)] {
        self.in_edges.get(id).map(|v| v.as_slice()).unwrap_or(&[])
    }
}

/// Error from requiring a live bead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveLookupError {
    /// Bead was never created or doesn't exist.
    NotFound,
    /// Bead exists but has been deleted.
    Deleted,
}

/// Canonical state - the CRDT for the entire bead store.
///
/// Invariant: An ID cannot be both live and globally deleted (tombstone lineage=None).
/// Collision tombstones (tombstone lineage=Some(created)) may coexist with a live bead
/// of the same ID, as long as the live bead has a different creation stamp.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CanonicalState {
    live: BTreeMap<BeadId, Bead>,
    tombstones: BTreeMap<TombstoneKey, Tombstone>,
    deps: BTreeMap<DepKey, DepEdge>,
    /// Derived indexes for O(1) dependency lookups.
    /// Not serialized - rebuilt on load and updated incrementally.
    #[serde(skip, default)]
    dep_indexes: DepIndexes,
}

impl CanonicalState {
    pub fn new() -> Self {
        Self::default()
    }

    // =========================================================================
    // Queries
    // =========================================================================

    pub fn get(&self, id: &BeadId) -> Option<&Bead> {
        self.live.get(id)
    }

    pub fn get_mut(&mut self, id: &BeadId) -> Option<&mut Bead> {
        self.live.get_mut(id)
    }

    pub fn is_deleted(&self, id: &BeadId) -> bool {
        self.tombstones
            .contains_key(&TombstoneKey::global(id.clone()))
    }

    pub fn get_tombstone(&self, id: &BeadId) -> Option<&Tombstone> {
        self.tombstones.get(&TombstoneKey::global(id.clone()))
    }

    pub fn contains(&self, id: &BeadId) -> bool {
        self.live.contains_key(id) || self.has_any_tombstone(id)
    }

    pub fn live_count(&self) -> usize {
        self.live.len()
    }

    pub fn tombstone_count(&self) -> usize {
        self.tombstones.len()
    }

    pub fn dep_count(&self) -> usize {
        self.deps.len()
    }

    pub fn iter_live(&self) -> impl Iterator<Item = (&BeadId, &Bead)> {
        self.live.iter()
    }

    pub fn iter_tombstones(&self) -> impl Iterator<Item = (&TombstoneKey, &Tombstone)> {
        self.tombstones.iter()
    }

    pub fn iter_deps(&self) -> impl Iterator<Item = (&DepKey, &DepEdge)> {
        self.deps.iter()
    }

    pub fn get_dep(&self, key: &DepKey) -> Option<&DepEdge> {
        self.deps.get(key)
    }

    /// Get a live bead by ID (alias for get).
    pub fn get_live(&self, id: &BeadId) -> Option<&Bead> {
        self.live.get(id)
    }

    /// Get a mutable live bead by ID.
    pub fn get_live_mut(&mut self, id: &BeadId) -> Option<&mut Bead> {
        self.live.get_mut(id)
    }

    /// Require a live bead, returning appropriate error if not found or deleted.
    ///
    /// This is a combined lookup that replaces the common pattern of:
    /// ```ignore
    /// if state.get_live(id).is_none() {
    ///     if state.get_tombstone(id).is_some() {
    ///         return Err(BeadDeleted);
    ///     }
    ///     return Err(NotFound);
    /// }
    /// let bead = state.get_live(id).unwrap();
    /// ```
    pub fn require_live(&self, id: &BeadId) -> Result<&Bead, LiveLookupError> {
        if let Some(bead) = self.live.get(id) {
            return Ok(bead);
        }
        if self
            .tombstones
            .contains_key(&TombstoneKey::global(id.clone()))
        {
            return Err(LiveLookupError::Deleted);
        }
        Err(LiveLookupError::NotFound)
    }

    /// Require a mutable live bead, returning appropriate error if not found or deleted.
    pub fn require_live_mut(&mut self, id: &BeadId) -> Result<&mut Bead, LiveLookupError> {
        // Check tombstone first to avoid borrowing issues
        if self
            .tombstones
            .contains_key(&TombstoneKey::global(id.clone()))
        {
            return Err(LiveLookupError::Deleted);
        }
        self.live.get_mut(id).ok_or(LiveLookupError::NotFound)
    }

    /// Get the maximum WriteStamp across all beads.
    ///
    /// Used for clock synchronization after sync.
    pub fn max_write_stamp(&self) -> Option<super::time::WriteStamp> {
        self.live
            .values()
            .map(|b| b.updated_stamp().at.clone())
            .max()
    }

    // =========================================================================
    // Mutations (enforce invariant)
    // =========================================================================

    /// Insert a bead - removes any tombstone for this ID.
    ///
    /// If bead already exists, merges via Bead::join.
    /// Returns Err on ID collision (same ID, different creation stamp).
    pub fn insert(&mut self, bead: Bead) -> Result<(), CoreError> {
        let id = bead.core.id.clone();

        // Remove tombstone if present (resurrection)
        self.tombstones.remove(&TombstoneKey::global(id.clone()));

        // Merge with existing or insert
        if let Some(existing) = self.live.get(&id) {
            let merged = Bead::join(existing, &bead)?;
            self.live.insert(id, merged);
        } else {
            self.live.insert(id, bead);
        }

        Ok(())
    }

    /// Delete a bead - adds tombstone, removes from live.
    ///
    /// If tombstone already exists, merges (keeps later deletion).
    pub fn delete(&mut self, tombstone: Tombstone) {
        let key = tombstone.key();
        let id = key.id.clone();

        // Remove from live
        self.live.remove(&id);

        // Merge with existing tombstone or insert
        if let Some(existing) = self.tombstones.get(&key) {
            let merged = Tombstone::join(existing, &tombstone);
            self.tombstones.insert(key, merged);
        } else {
            self.tombstones.insert(key, tombstone);
        }
    }

    /// Insert or update a dependency edge.
    ///
    /// Maintains the dep indexes incrementally.
    pub fn insert_dep(&mut self, edge: DepEdge) {
        let key = edge.key.clone();
        let from = key.from().clone();
        let to = key.to().clone();
        let kind = key.kind();

        if let Some(existing) = self.deps.get(&key) {
            let was_active = existing.is_active();
            let merged = DepEdge::join(existing, &edge);
            let is_active = merged.is_active();

            // Update index if activity state changed
            match (was_active, is_active) {
                (true, false) => {
                    // Active -> Deleted: remove from index
                    self.dep_indexes.remove(&from, &to, kind);
                }
                (false, true) => {
                    // Deleted -> Active: add to index
                    self.dep_indexes.add(&from, &to, kind);
                }
                _ => {
                    // No change in activity state
                }
            }

            self.deps.insert(key, merged);
        } else {
            // New edge
            if edge.is_active() {
                self.dep_indexes.add(&from, &to, kind);
            }
            self.deps.insert(key, edge);
        }
    }

    /// Remove a live bead by ID, returning it if present.
    pub fn remove_live(&mut self, id: &BeadId) -> Option<Bead> {
        self.live.remove(id)
    }

    /// Insert a bead directly without CRDT merge.
    ///
    /// Used for collision resolution when we've already handled the logic.
    /// Removes any tombstone for this ID.
    pub fn insert_live(&mut self, bead: Bead) {
        self.tombstones
            .remove(&TombstoneKey::global(bead.core.id.clone()));
        self.live.insert(bead.core.id.clone(), bead);
    }

    /// Remove a dependency edge.
    pub fn remove_dep(&mut self, key: &DepKey) -> Option<DepEdge> {
        self.deps.remove(key)
    }

    /// Insert a tombstone directly.
    ///
    /// Used for collision resolution. Does not remove live beads.
    pub fn insert_tombstone(&mut self, tombstone: Tombstone) {
        let key = tombstone.key();
        self.tombstones
            .entry(key)
            .and_modify(|t| *t = Tombstone::join(t, &tombstone))
            .or_insert(tombstone);
    }

    // =========================================================================
    // CRDT Merge
    // =========================================================================

    /// Merge two canonical states.
    ///
    /// Resurrection rule: if a bead's updated_stamp > tombstone.deleted,
    /// the bead wins (resurrection). Otherwise tombstone wins.
    ///
    /// Returns errors for ID collisions (collected, doesn't abort early).
    pub fn join(a: &Self, b: &Self) -> Result<Self, Vec<CoreError>> {
        let mut result = Self::default();
        let mut errors = Vec::new();

        // Merge tombstones by key; we may later drop global deletions on resurrection.
        for (key, tomb) in a.tombstones.iter().chain(b.tombstones.iter()) {
            result
                .tombstones
                .entry(key.clone())
                .and_modify(|t| *t = Tombstone::join(t, tomb))
                .or_insert_with(|| tomb.clone());
        }

        // Collect all bead IDs from both sides
        let all_ids: BTreeSet<_> = a
            .live
            .keys()
            .chain(b.live.keys())
            .chain(a.tombstones.keys().map(|k| &k.id))
            .chain(b.tombstones.keys().map(|k| &k.id))
            .cloned()
            .collect();

        for id in all_ids {
            let a_bead = a.live.get(&id);
            let b_bead = b.live.get(&id);

            // Merge beads if both exist (may error on collision)
            let merged_bead = match (a_bead, b_bead) {
                (Some(ab), Some(bb)) => match Bead::join(ab, bb) {
                    Ok(merged) => Some(merged),
                    Err(e) => {
                        errors.push(e);
                        // On collision, keep one arbitrarily (a's version)
                        Some(ab.clone())
                    }
                },
                (Some(b), None) | (None, Some(b)) => Some(b.clone()),
                (None, None) => None,
            };

            if let Some(bead) = merged_bead {
                // Collision tombstone: if a lineage-scoped tombstone exists for this
                // bead's creation stamp, it always wins and permanently suppresses
                // that lineage at this ID.
                let collision_key = TombstoneKey::lineage(id.clone(), bead.core.created().clone());
                if result.tombstones.contains_key(&collision_key) {
                    continue;
                }

                // Deletion tombstone: applies globally to the ID.
                let delete_key = TombstoneKey::global(id.clone());
                if let Some(tomb) = result.tombstones.get(&delete_key) {
                    // RESURRECTION RULE: bead wins if strictly newer than deletion
                    if bead.updated_stamp() > tomb.deleted {
                        result.tombstones.remove(&delete_key);
                        result.live.insert(id, bead);
                    } else {
                        // Keep tombstone, drop bead.
                    }
                } else {
                    result.live.insert(id, bead);
                }
            }
        }

        // Merge deps: union by key, join if both exist
        for (key, edge) in a.deps.iter().chain(b.deps.iter()) {
            result
                .deps
                .entry(key.clone())
                .and_modify(|e| *e = DepEdge::join(e, edge))
                .or_insert_with(|| edge.clone());
        }

        // Rebuild derived indexes from merged deps
        result.rebuild_dep_indexes();

        if errors.is_empty() {
            Ok(result)
        } else {
            Err(errors)
        }
    }

    // =========================================================================
    // Maintenance
    // =========================================================================

    /// Garbage collect tombstones older than TTL.
    ///
    /// Returns number of tombstones removed.
    pub fn gc_tombstones(&mut self, ttl_ms: u64, now: WallClock) -> usize {
        let before = self.tombstones.len();
        self.tombstones
            .retain(|_, t| t.deleted.at.wall_ms + ttl_ms > now.0);
        before - self.tombstones.len()
    }

    /// Remove soft-deleted deps (where deleted is Some).
    ///
    /// Returns number of deps removed.
    /// Note: Does not rebuild indexes since deleted deps aren't in indexes anyway.
    pub fn gc_deleted_deps(&mut self) -> usize {
        let before = self.deps.len();
        self.deps.retain(|_, e| e.is_active());
        before - self.deps.len()
    }

    /// Get all active deps for a bead (outgoing).
    ///
    /// Uses the derived index for O(neighbors) lookup instead of O(all deps).
    pub fn deps_from(&self, id: &BeadId) -> Vec<&DepEdge> {
        self.dep_indexes
            .out_edges(id)
            .iter()
            .filter_map(|(to, kind)| {
                let key = DepKey::new(id.clone(), to.clone(), *kind).ok()?;
                self.deps.get(&key)
            })
            .collect()
    }

    /// Get all active deps to a bead (incoming).
    ///
    /// Uses the derived index for O(neighbors) lookup instead of O(all deps).
    pub fn deps_to(&self, id: &BeadId) -> Vec<&DepEdge> {
        self.dep_indexes
            .in_edges(id)
            .iter()
            .filter_map(|(from, kind)| {
                let key = DepKey::new(from.clone(), id.clone(), *kind).ok()?;
                self.deps.get(&key)
            })
            .collect()
    }

    /// Rebuild the derived dep indexes from scratch.
    ///
    /// Call this after deserializing state or after `join()`.
    pub fn rebuild_dep_indexes(&mut self) {
        self.dep_indexes = DepIndexes::new();
        for edge in self.deps.values() {
            if edge.is_active() {
                self.dep_indexes
                    .add(edge.key.from(), edge.key.to(), edge.key.kind());
            }
        }
    }

    /// Access the dep indexes (for queries that need direct access).
    pub fn dep_indexes(&self) -> &DepIndexes {
        &self.dep_indexes
    }

    fn has_any_tombstone(&self, id: &BeadId) -> bool {
        self.tombstones.keys().any(|k| &k.id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::dep::DepLife;
    use crate::core::identity::ActorId;
    use crate::core::time::{Stamp, WriteStamp};
    use proptest::prelude::*;

    fn make_stamp(wall_ms: u64, counter: u32, actor: &str) -> Stamp {
        Stamp::new(
            WriteStamp::new(wall_ms, counter),
            ActorId::new(actor).unwrap(),
        )
    }

    fn actor_id(actor: &str) -> ActorId {
        ActorId::new(actor).unwrap_or_else(|e| panic!("invalid actor id {actor}: {e}"))
    }

    fn bead_id(id: &str) -> BeadId {
        BeadId::parse(id).unwrap_or_else(|e| panic!("invalid bead id {id}: {e}"))
    }

    fn make_bead(id: &BeadId, stamp: &Stamp) -> Bead {
        use crate::core::bead::{BeadCore, BeadFields};
        use crate::core::collections::Labels;
        use crate::core::composite::{Claim, Workflow};
        use crate::core::crdt::Lww;
        use crate::core::domain::{BeadType, Priority};

        let core = BeadCore::new(id.clone(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("title".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            labels: Lww::new(Labels::new(), stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        Bead::new(core, fields)
    }

    #[derive(Clone, Debug)]
    enum Entry {
        Live { id: String, stamp: Stamp },
        Tombstone { id: String, stamp: Stamp },
    }

    fn assert_invariants(state: &CanonicalState) {
        for id in state.live.keys() {
            assert!(
                !state
                    .tombstones
                    .contains_key(&TombstoneKey::global(id.clone())),
                "live bead has global tombstone: {id}"
            );
        }

        for (key, edge) in state.deps.iter() {
            let from = key.from();
            let to = key.to();
            let kind = key.kind();
            let out_has = state
                .dep_indexes
                .out_edges(from)
                .iter()
                .any(|(t, k)| t == to && *k == kind);
            let in_has = state
                .dep_indexes
                .in_edges(to)
                .iter()
                .any(|(f, k)| f == from && *k == kind);

            if edge.is_active() {
                assert!(out_has, "missing out-edge for {from}->{to:?}");
                assert!(in_has, "missing in-edge for {from:?}->{to}");
            } else {
                assert!(!out_has, "deleted edge still in out-index");
                assert!(!in_has, "deleted edge still in in-index");
            }
        }
    }

    fn state_fingerprint(state: &CanonicalState) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let state_bytes = crate::git::wire::serialize_state(state)
            .unwrap_or_else(|e| panic!("serialize state failed: {e}"));
        let tomb_bytes = crate::git::wire::serialize_tombstones(state)
            .unwrap_or_else(|e| panic!("serialize tombstones failed: {e}"));
        let deps_bytes = crate::git::wire::serialize_deps(state)
            .unwrap_or_else(|e| panic!("serialize deps failed: {e}"));
        (state_bytes, tomb_bytes, deps_bytes)
    }

    fn base58_id_strategy() -> impl Strategy<Value = String> {
        proptest::string::string_regex("[1-9A-HJ-NP-Za-km-z]{5,8}")
            .unwrap_or_else(|e| panic!("regex failed: {e}"))
            .prop_map(|suffix| format!("bd-{suffix}"))
    }

    fn stamp_strategy() -> impl Strategy<Value = Stamp> {
        let actor = prop_oneof![Just("alice"), Just("bob"), Just("carol")];
        (0u64..10_000, 0u32..5, actor).prop_map(|(wall_ms, counter, actor)| {
            Stamp::new(WriteStamp::new(wall_ms, counter), actor_id(actor))
        })
    }

    fn entry_strategy() -> impl Strategy<Value = Entry> {
        (base58_id_strategy(), stamp_strategy(), any::<bool>()).prop_map(|(id, stamp, is_live)| {
            if is_live {
                Entry::Live { id, stamp }
            } else {
                Entry::Tombstone { id, stamp }
            }
        })
    }

    fn dep_strategy() -> impl Strategy<Value = DepEdge> {
        let kind = prop_oneof![
            Just(DepKind::Blocks),
            Just(DepKind::Parent),
            Just(DepKind::Related),
            Just(DepKind::DiscoveredFrom),
        ];
        (
            base58_id_strategy(),
            base58_id_strategy(),
            kind,
            stamp_strategy(),
            prop::option::of(stamp_strategy()),
        )
            .prop_filter("deps cannot be self-referential", |(from, to, _, _, _)| {
                from != to
            })
            .prop_map(|(from, to, kind, created, deleted)| {
                let key = DepKey::new(bead_id(&from), bead_id(&to), kind)
                    .unwrap_or_else(|e| panic!("dep key invalid: {}", e.reason));
                let mut edge = DepEdge::new(key, created.clone());
                if let Some(deleted) = deleted {
                    edge.delete(deleted);
                }
                edge
            })
    }

    fn state_strategy() -> impl Strategy<Value = CanonicalState> {
        (
            prop::collection::vec(entry_strategy(), 0..12),
            prop::collection::vec(dep_strategy(), 0..12),
        )
            .prop_map(|(entries, deps)| {
                let mut state = CanonicalState::new();
                for entry in entries {
                    match entry {
                        Entry::Live { id, stamp } => {
                            let bead = make_bead(&bead_id(&id), &stamp);
                            if let Err(err) = state.insert(bead) {
                                panic!("insert bead failed: {err:?}");
                            }
                        }
                        Entry::Tombstone { id, stamp } => {
                            state.delete(Tombstone::new(bead_id(&id), stamp, None));
                        }
                    }
                }
                for dep in deps {
                    state.insert_dep(dep);
                }
                state
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

        #[test]
        fn join_commutative(a in state_strategy(), b in state_strategy()) {
            let ab = CanonicalState::join(&a, &b)
                .unwrap_or_else(|e| panic!("join failed: {e:?}"));
            let ba = CanonicalState::join(&b, &a)
                .unwrap_or_else(|e| panic!("join failed: {e:?}"));
            assert_invariants(&ab);
            assert_invariants(&ba);
            prop_assert_eq!(state_fingerprint(&ab), state_fingerprint(&ba));
        }

        #[test]
        fn join_idempotent(a in state_strategy()) {
            let aa = CanonicalState::join(&a, &a)
                .unwrap_or_else(|e| panic!("join failed: {e:?}"));
            assert_invariants(&aa);
            prop_assert_eq!(state_fingerprint(&aa), state_fingerprint(&a));
        }

        #[test]
        fn join_associative(a in state_strategy(), b in state_strategy(), c in state_strategy()) {
            let ab = CanonicalState::join(&a, &b)
                .unwrap_or_else(|e| panic!("join failed: {e:?}"));
            let left = CanonicalState::join(&ab, &c)
                .unwrap_or_else(|e| panic!("join failed: {e:?}"));
            let bc = CanonicalState::join(&b, &c)
                .unwrap_or_else(|e| panic!("join failed: {e:?}"));
            let right = CanonicalState::join(&a, &bc)
                .unwrap_or_else(|e| panic!("join failed: {e:?}"));
            assert_invariants(&left);
            assert_invariants(&right);
            prop_assert_eq!(state_fingerprint(&left), state_fingerprint(&right));
        }

        #[test]
        fn join_resurrection_rule_prefers_newer_bead(
            bead_stamp in stamp_strategy(),
            tomb_stamp in stamp_strategy(),
        ) {
            let id = bead_id("bd-resurrection");
            let bead = make_bead(&id, &bead_stamp);
            let tomb = Tombstone::new(id.clone(), tomb_stamp.clone(), None);

            let mut state_bead = CanonicalState::new();
            state_bead.insert_live(bead);
            let mut state_tomb = CanonicalState::new();
            state_tomb.insert_tombstone(tomb);

            let merged = CanonicalState::join(&state_bead, &state_tomb)
                .unwrap_or_else(|e| panic!("join failed: {e:?}"));

            let is_live = merged.get_live(&id).is_some();
            if bead_stamp > tomb_stamp {
                prop_assert!(is_live);
            } else {
                prop_assert!(!is_live);
            }
        }

        #[test]
        fn lineage_tombstone_only_kills_matching_lineage(
            bead_stamp in stamp_strategy(),
            tomb_stamp in stamp_strategy(),
            other_stamp in stamp_strategy(),
        ) {
            prop_assume!(other_stamp != bead_stamp);

            let id = bead_id("bd-lineage");
            let bead = make_bead(&id, &bead_stamp);
            let tomb = Tombstone::new_collision(id.clone(), tomb_stamp.clone(), other_stamp, None);

            let mut state_bead = CanonicalState::new();
            state_bead.insert_live(bead);
            let mut state_tomb = CanonicalState::new();
            state_tomb.insert_tombstone(tomb);

            let merged = CanonicalState::join(&state_bead, &state_tomb)
                .unwrap_or_else(|e| panic!("join failed: {e:?}"));

            prop_assert!(merged.get_live(&id).is_some());
        }
    }

    #[test]
    fn invariant_insert_removes_tombstone() {
        use crate::core::bead::{BeadCore, BeadFields};
        use crate::core::collections::Labels;
        use crate::core::composite::{Claim, Workflow};
        use crate::core::crdt::Lww;
        use crate::core::domain::{BeadType, Priority};

        let mut state = CanonicalState::new();
        let id = BeadId::parse("bd-abc").unwrap();
        let stamp = make_stamp(1000, 0, "alice");

        // Add tombstone first
        let tomb = Tombstone::new(id.clone(), stamp.clone(), None);
        state.delete(tomb);
        assert!(state.is_deleted(&id));
        assert!(!state.live.contains_key(&id));

        // Now insert bead - should remove tombstone
        let core = BeadCore::new(id.clone(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("test".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            labels: Lww::new(Labels::new(), stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        let bead = Bead::new(core, fields);
        state.insert(bead).unwrap();

        assert!(!state.is_deleted(&id));
        assert!(state.live.contains_key(&id));
        assert!(
            !state
                .tombstones
                .contains_key(&TombstoneKey::global(id.clone()))
        );
    }

    #[test]
    fn invariant_delete_removes_live() {
        use crate::core::bead::{BeadCore, BeadFields};
        use crate::core::collections::Labels;
        use crate::core::composite::{Claim, Workflow};
        use crate::core::crdt::Lww;
        use crate::core::domain::{BeadType, Priority};

        let mut state = CanonicalState::new();
        let id = BeadId::parse("bd-xyz").unwrap();
        let stamp = make_stamp(1000, 0, "bob");

        // Insert bead first
        let core = BeadCore::new(id.clone(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("test".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            labels: Lww::new(Labels::new(), stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        let bead = Bead::new(core, fields);
        state.insert(bead).unwrap();
        assert!(state.live.contains_key(&id));

        // Delete - should remove from live, add tombstone
        let tomb = Tombstone::new(id.clone(), stamp.clone(), Some("test delete".to_string()));
        state.delete(tomb);

        assert!(!state.live.contains_key(&id));
        assert!(
            state
                .tombstones
                .contains_key(&TombstoneKey::global(id.clone()))
        );
        assert!(state.is_deleted(&id));
    }

    #[test]
    fn resurrection_newer_bead_wins() {
        use crate::core::bead::{BeadCore, BeadFields};
        use crate::core::collections::Labels;
        use crate::core::composite::{Claim, Workflow};
        use crate::core::crdt::Lww;
        use crate::core::domain::{BeadType, Priority};

        let id = BeadId::parse("bd-res").unwrap();
        let old_stamp = make_stamp(1000, 0, "alice");
        let new_stamp = make_stamp(2000, 0, "bob");

        // State A: has tombstone at old time
        let mut state_a = CanonicalState::new();
        state_a.delete(Tombstone::new(id.clone(), old_stamp.clone(), None));

        // State B: has bead modified at new time
        let mut state_b = CanonicalState::new();
        let core = BeadCore::new(id.clone(), old_stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("resurrected".to_string(), new_stamp.clone()),
            description: Lww::new(String::new(), new_stamp.clone()),
            design: Lww::new(None, new_stamp.clone()),
            acceptance_criteria: Lww::new(None, new_stamp.clone()),
            priority: Lww::new(Priority::default(), new_stamp.clone()),
            bead_type: Lww::new(BeadType::Task, new_stamp.clone()),
            labels: Lww::new(Labels::new(), new_stamp.clone()),
            external_ref: Lww::new(None, new_stamp.clone()),
            source_repo: Lww::new(None, new_stamp.clone()),
            estimated_minutes: Lww::new(None, new_stamp.clone()),
            workflow: Lww::new(Workflow::default(), new_stamp.clone()),
            claim: Lww::new(Claim::default(), new_stamp.clone()),
        };
        state_b.insert(Bead::new(core, fields)).unwrap();

        // Merge: bead should win (resurrection)
        let merged = CanonicalState::join(&state_a, &state_b).unwrap();
        assert!(merged.live.contains_key(&id), "bead should be resurrected");
        assert!(
            !merged
                .tombstones
                .contains_key(&TombstoneKey::global(id.clone())),
            "tombstone should be gone"
        );
    }

    #[test]
    fn deletion_wins_when_newer() {
        use crate::core::bead::{BeadCore, BeadFields};
        use crate::core::collections::Labels;
        use crate::core::composite::{Claim, Workflow};
        use crate::core::crdt::Lww;
        use crate::core::domain::{BeadType, Priority};

        let id = BeadId::parse("bd-de1").unwrap(); // no 'l' in base58
        let old_stamp = make_stamp(1000, 0, "alice");
        let new_stamp = make_stamp(2000, 0, "bob");

        // State A: has bead at old time
        let mut state_a = CanonicalState::new();
        let core = BeadCore::new(id.clone(), old_stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("old".to_string(), old_stamp.clone()),
            description: Lww::new(String::new(), old_stamp.clone()),
            design: Lww::new(None, old_stamp.clone()),
            acceptance_criteria: Lww::new(None, old_stamp.clone()),
            priority: Lww::new(Priority::default(), old_stamp.clone()),
            bead_type: Lww::new(BeadType::Task, old_stamp.clone()),
            labels: Lww::new(Labels::new(), old_stamp.clone()),
            external_ref: Lww::new(None, old_stamp.clone()),
            source_repo: Lww::new(None, old_stamp.clone()),
            estimated_minutes: Lww::new(None, old_stamp.clone()),
            workflow: Lww::new(Workflow::default(), old_stamp.clone()),
            claim: Lww::new(Claim::default(), old_stamp.clone()),
        };
        state_a.insert(Bead::new(core, fields)).unwrap();

        // State B: has tombstone at new time
        let mut state_b = CanonicalState::new();
        state_b.delete(Tombstone::new(id.clone(), new_stamp.clone(), None));

        // Merge: tombstone should win
        let merged = CanonicalState::join(&state_a, &state_b).unwrap();
        assert!(!merged.live.contains_key(&id), "bead should be deleted");
        assert!(
            merged
                .tombstones
                .contains_key(&TombstoneKey::global(id.clone())),
            "tombstone should exist"
        );
    }

    #[test]
    fn require_live_returns_bead_when_exists() {
        use crate::core::bead::{BeadCore, BeadFields};
        use crate::core::collections::Labels;
        use crate::core::composite::{Claim, Workflow};
        use crate::core::crdt::Lww;
        use crate::core::domain::{BeadType, Priority};

        let mut state = CanonicalState::new();
        let id = BeadId::parse("bd-abc").unwrap();
        let stamp = make_stamp(1000, 0, "alice");

        let core = BeadCore::new(id.clone(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("test".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            labels: Lww::new(Labels::new(), stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        state.insert(Bead::new(core, fields)).unwrap();

        let result = state.require_live(&id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().fields.title.value, "test");
    }

    #[test]
    fn require_live_returns_not_found_for_unknown_id() {
        let state = CanonicalState::new();
        let id = BeadId::parse("bd-abc").unwrap();

        let result = state.require_live(&id);
        assert!(matches!(result, Err(LiveLookupError::NotFound)));
    }

    #[test]
    fn require_live_returns_deleted_for_tombstoned_id() {
        let mut state = CanonicalState::new();
        let id = BeadId::parse("bd-abc").unwrap();
        let stamp = make_stamp(1000, 0, "alice");

        state.delete(Tombstone::new(id.clone(), stamp, None));

        let result = state.require_live(&id);
        assert!(matches!(result, Err(LiveLookupError::Deleted)));
    }

    #[test]
    fn require_live_mut_returns_mutable_bead() {
        use crate::core::bead::{BeadCore, BeadFields};
        use crate::core::collections::Labels;
        use crate::core::composite::{Claim, Workflow};
        use crate::core::crdt::Lww;
        use crate::core::domain::{BeadType, Priority};

        let mut state = CanonicalState::new();
        let id = BeadId::parse("bd-abc").unwrap();
        let stamp = make_stamp(1000, 0, "alice");

        let core = BeadCore::new(id.clone(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("test".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            labels: Lww::new(Labels::new(), stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        state.insert(Bead::new(core, fields)).unwrap();

        // Modify via require_live_mut
        let new_stamp = make_stamp(2000, 0, "bob");
        let bead = state.require_live_mut(&id).unwrap();
        bead.fields.title = Lww::new("modified".to_string(), new_stamp);

        // Verify modification persisted
        assert_eq!(state.get_live(&id).unwrap().fields.title.value, "modified");
    }

    // =========================================================================
    // Dep Index Tests
    // =========================================================================

    #[test]
    fn dep_index_insert_active_edge() {
        let mut state = CanonicalState::new();
        let stamp = make_stamp(1000, 0, "alice");
        let from = BeadId::parse("bd-aaa").unwrap();
        let to = BeadId::parse("bd-bbb").unwrap();

        let key = DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap();
        let edge = DepEdge::new(key, stamp);

        state.insert_dep(edge);

        // Should be in out_edges for "from"
        let out = state.dep_indexes().out_edges(&from);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], (to.clone(), DepKind::Blocks));

        // Should be in in_edges for "to"
        let in_ = state.dep_indexes().in_edges(&to);
        assert_eq!(in_.len(), 1);
        assert_eq!(in_[0], (from.clone(), DepKind::Blocks));
    }

    #[test]
    fn dep_index_insert_deleted_edge() {
        let mut state = CanonicalState::new();
        let stamp = make_stamp(1000, 0, "alice");
        let from = BeadId::parse("bd-aaa").unwrap();
        let to = BeadId::parse("bd-bbb").unwrap();

        let key = DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap();
        let life = crate::core::crdt::Lww::new(DepLife::Deleted, stamp.clone());
        let edge = DepEdge::with_life(key, stamp, life);

        state.insert_dep(edge);

        // Deleted edge should NOT be in indexes
        assert!(state.dep_indexes().out_edges(&from).is_empty());
        assert!(state.dep_indexes().in_edges(&to).is_empty());
    }

    #[test]
    fn dep_index_transition_active_to_deleted() {
        let mut state = CanonicalState::new();
        let stamp1 = make_stamp(1000, 0, "alice");
        let stamp2 = make_stamp(2000, 0, "bob");
        let from = BeadId::parse("bd-aaa").unwrap();
        let to = BeadId::parse("bd-bbb").unwrap();

        // Insert active edge
        let key = DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap();
        let edge = DepEdge::new(key.clone(), stamp1);
        state.insert_dep(edge);

        // Should be in indexes
        assert_eq!(state.dep_indexes().out_edges(&from).len(), 1);

        // Now delete it
        let life = crate::core::crdt::Lww::new(DepLife::Deleted, stamp2.clone());
        let deleted_edge = DepEdge::with_life(key, stamp2, life);
        state.insert_dep(deleted_edge);

        // Should be removed from indexes
        assert!(state.dep_indexes().out_edges(&from).is_empty());
        assert!(state.dep_indexes().in_edges(&to).is_empty());
    }

    #[test]
    fn dep_index_transition_deleted_to_active() {
        let mut state = CanonicalState::new();
        let stamp1 = make_stamp(1000, 0, "alice");
        let stamp2 = make_stamp(2000, 0, "bob");
        let from = BeadId::parse("bd-aaa").unwrap();
        let to = BeadId::parse("bd-bbb").unwrap();

        // Insert deleted edge first
        let key = DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap();
        let life = crate::core::crdt::Lww::new(DepLife::Deleted, stamp1.clone());
        let edge = DepEdge::with_life(key.clone(), stamp1, life);
        state.insert_dep(edge);

        // Should NOT be in indexes
        assert!(state.dep_indexes().out_edges(&from).is_empty());

        // Now restore it (active edge with newer stamp)
        let restored_edge = DepEdge::new(key, stamp2);
        state.insert_dep(restored_edge);

        // Should be in indexes now
        assert_eq!(state.dep_indexes().out_edges(&from).len(), 1);
        assert_eq!(state.dep_indexes().in_edges(&to).len(), 1);
    }

    #[test]
    fn dep_index_deps_from_uses_index() {
        let mut state = CanonicalState::new();
        let stamp = make_stamp(1000, 0, "alice");
        let from = BeadId::parse("bd-aaa").unwrap();
        let to1 = BeadId::parse("bd-bbb").unwrap();
        let to2 = BeadId::parse("bd-ccc").unwrap();

        // Add two deps from the same source
        let key1 = DepKey::new(from.clone(), to1.clone(), DepKind::Blocks).unwrap();
        let key2 = DepKey::new(from.clone(), to2.clone(), DepKind::Parent).unwrap();
        state.insert_dep(DepEdge::new(key1, stamp.clone()));
        state.insert_dep(DepEdge::new(key2, stamp));

        let deps = state.deps_from(&from);
        assert_eq!(deps.len(), 2);

        // Verify the edges are correct
        let to_ids: std::collections::HashSet<_> =
            deps.iter().map(|e| e.key.to().clone()).collect();
        assert!(to_ids.contains(&to1));
        assert!(to_ids.contains(&to2));
    }

    #[test]
    fn dep_index_deps_to_uses_index() {
        let mut state = CanonicalState::new();
        let stamp = make_stamp(1000, 0, "alice");
        let from1 = BeadId::parse("bd-aaa").unwrap();
        let from2 = BeadId::parse("bd-bbb").unwrap();
        let to = BeadId::parse("bd-ccc").unwrap();

        // Add two deps to the same target
        let key1 = DepKey::new(from1.clone(), to.clone(), DepKind::Blocks).unwrap();
        let key2 = DepKey::new(from2.clone(), to.clone(), DepKind::Blocks).unwrap();
        state.insert_dep(DepEdge::new(key1, stamp.clone()));
        state.insert_dep(DepEdge::new(key2, stamp));

        let deps = state.deps_to(&to);
        assert_eq!(deps.len(), 2);

        // Verify the edges are correct
        let from_ids: std::collections::HashSet<_> =
            deps.iter().map(|e| e.key.from().clone()).collect();
        assert!(from_ids.contains(&from1));
        assert!(from_ids.contains(&from2));
    }

    #[test]
    fn dep_index_rebuild_from_deps() {
        let mut state = CanonicalState::new();
        let stamp = make_stamp(1000, 0, "alice");
        let from = BeadId::parse("bd-aaa").unwrap();
        let to = BeadId::parse("bd-bbb").unwrap();

        // Manually insert into deps map (simulating deserialization)
        let key = DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap();
        let edge = DepEdge::new(key, stamp);
        state.deps.insert(edge.key.clone(), edge);

        // Index should be empty (not maintained)
        assert!(state.dep_indexes().out_edges(&from).is_empty());

        // Rebuild indexes
        state.rebuild_dep_indexes();

        // Now index should be populated
        assert_eq!(state.dep_indexes().out_edges(&from).len(), 1);
        assert_eq!(state.dep_indexes().in_edges(&to).len(), 1);
    }

    #[test]
    fn dep_index_join_rebuilds_indexes() {
        let stamp = make_stamp(1000, 0, "alice");
        let from = BeadId::parse("bd-aaa").unwrap();
        let to = BeadId::parse("bd-bbb").unwrap();

        // State A: has one dep
        let mut state_a = CanonicalState::new();
        let key = DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap();
        state_a.insert_dep(DepEdge::new(key, stamp));

        // State B: empty
        let state_b = CanonicalState::new();

        // Join should rebuild indexes
        let merged = CanonicalState::join(&state_a, &state_b).unwrap();

        // Index should be populated in merged state
        assert_eq!(merged.dep_indexes().out_edges(&from).len(), 1);
        assert_eq!(merged.dep_indexes().in_edges(&to).len(), 1);
    }

    #[test]
    fn dep_index_multiple_kinds() {
        let mut state = CanonicalState::new();
        let stamp = make_stamp(1000, 0, "alice");
        let from = BeadId::parse("bd-aaa").unwrap();
        let to = BeadId::parse("bd-bbb").unwrap();

        // Add multiple deps of different kinds between same beads
        let key1 = DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap();
        let key2 = DepKey::new(from.clone(), to.clone(), DepKind::Related).unwrap();
        state.insert_dep(DepEdge::new(key1, stamp.clone()));
        state.insert_dep(DepEdge::new(key2, stamp));

        // Should have two edges in indexes
        let out = state.dep_indexes().out_edges(&from);
        assert_eq!(out.len(), 2);

        let kinds: std::collections::HashSet<_> = out.iter().map(|(_, k)| *k).collect();
        assert!(kinds.contains(&DepKind::Blocks));
        assert!(kinds.contains(&DepKind::Related));
    }
}
