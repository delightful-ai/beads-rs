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
use super::error::CoreError;
use super::identity::BeadId;
use super::time::WallClock;
use super::tombstone::{Tombstone, TombstoneKey};

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
    pub fn insert_dep(&mut self, edge: DepEdge) {
        let key = edge.key.clone();
        if let Some(existing) = self.deps.get(&key) {
            let merged = DepEdge::join(existing, &edge);
            self.deps.insert(key, merged);
        } else {
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
    pub fn gc_deleted_deps(&mut self) -> usize {
        let before = self.deps.len();
        self.deps.retain(|_, e| e.is_active());
        before - self.deps.len()
    }

    /// Get all active deps for a bead (outgoing).
    pub fn deps_from(&self, id: &BeadId) -> Vec<&DepEdge> {
        self.deps
            .values()
            .filter(|e| e.is_active() && e.key.from() == id)
            .collect()
    }

    /// Get all active deps to a bead (incoming).
    pub fn deps_to(&self, id: &BeadId) -> Vec<&DepEdge> {
        self.deps
            .values()
            .filter(|e| e.is_active() && e.key.to() == id)
            .collect()
    }

    fn has_any_tombstone(&self, id: &BeadId) -> bool {
        self.tombstones.keys().any(|k| &k.id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::identity::ActorId;
    use crate::core::time::{Stamp, WriteStamp};

    fn make_stamp(wall_ms: u64, counter: u32, actor: &str) -> Stamp {
        Stamp::new(
            WriteStamp::new(wall_ms, counter),
            ActorId::new(actor).unwrap(),
        )
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
}
