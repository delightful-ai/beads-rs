//! Store wrappers with auto-merge semantics.
//!
//! DepStore: dependency edges with upsert that auto-joins
//! TombstoneStore: tombstones with upsert that auto-joins
//!
//! These ensure CRDT merge semantics are always applied correctly.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::dep::{DepEdge, DepKey};
use super::tombstone::{Tombstone, TombstoneKey};

/// Canonical dependency store.
///
/// Keys are unique by construction. `upsert()` automatically joins
/// if the key already exists.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DepStore {
    by_key: BTreeMap<DepKey, DepEdge>,
}

impl DepStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or merge - auto-joins if key exists.
    pub fn upsert(&mut self, edge: DepEdge) {
        let key = edge.key.clone();
        self.by_key
            .entry(key)
            .and_modify(|existing| *existing = DepEdge::join(existing, &edge))
            .or_insert(edge);
    }

    pub fn get(&self, key: &DepKey) -> Option<&DepEdge> {
        self.by_key.get(key)
    }

    pub fn remove(&mut self, key: &DepKey) -> Option<DepEdge> {
        self.by_key.remove(key)
    }

    pub fn contains(&self, key: &DepKey) -> bool {
        self.by_key.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&DepKey, &DepEdge)> {
        self.by_key.iter()
    }

    /// Merge two dep stores.
    pub fn join(a: &Self, b: &Self) -> Self {
        let mut result = a.clone();
        for edge in b.by_key.values() {
            result.upsert(edge.clone());
        }
        result
    }
}

/// Canonical tombstone store.
///
/// Keys are unique by construction. `upsert()` automatically joins if the key already exists.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TombstoneStore {
    by_key: BTreeMap<TombstoneKey, Tombstone>,
}

impl TombstoneStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or merge - auto-joins if id exists.
    pub fn upsert(&mut self, tombstone: Tombstone) {
        let key = tombstone.key();
        self.by_key
            .entry(key)
            .and_modify(|existing| *existing = Tombstone::join(existing, &tombstone))
            .or_insert(tombstone);
    }

    pub fn get(&self, key: &TombstoneKey) -> Option<&Tombstone> {
        self.by_key.get(key)
    }

    pub fn remove(&mut self, key: &TombstoneKey) -> Option<Tombstone> {
        self.by_key.remove(key)
    }

    pub fn contains(&self, key: &TombstoneKey) -> bool {
        self.by_key.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&TombstoneKey, &Tombstone)> {
        self.by_key.iter()
    }

    /// Merge two tombstone stores.
    pub fn join(a: &Self, b: &Self) -> Self {
        let mut result = a.clone();
        for t in b.by_key.values() {
            result.upsert(t.clone());
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::BeadId;
    use crate::core::{ActorId, DepKind, Stamp, WriteStamp};

    fn make_stamp(wall_ms: u64, actor: &str) -> Stamp {
        Stamp::new(WriteStamp::new(wall_ms, 0), ActorId::new(actor).unwrap())
    }

    #[test]
    fn dep_store_upsert_merges() {
        let mut store = DepStore::new();
        let key = DepKey::new(
            BeadId::parse("bd-abc").unwrap(),
            BeadId::parse("bd-xyz").unwrap(),
            DepKind::Blocks,
        )
        .unwrap();

        // Insert edge
        let edge1 = DepEdge::new(key.clone(), make_stamp(1000, "alice"));
        store.upsert(edge1);
        assert!(store.get(&key).unwrap().is_active());

        // Upsert with deletion
        let mut edge2 = DepEdge::new(key.clone(), make_stamp(1000, "alice"));
        edge2.delete(make_stamp(2000, "bob"));
        store.upsert(edge2);

        // Should be deleted now (merged)
        assert!(store.get(&key).unwrap().is_deleted());
    }

    #[test]
    fn tombstone_store_upsert_merges() {
        let mut store = TombstoneStore::new();
        let id = BeadId::parse("bd-abc").unwrap();

        // Insert tombstone
        let t1 = Tombstone::new(
            id.clone(),
            make_stamp(1000, "alice"),
            Some("old".to_string()),
        );
        store.upsert(t1);

        // Upsert with newer stamp
        let t2 = Tombstone::new(id.clone(), make_stamp(2000, "bob"), Some("new".to_string()));
        store.upsert(t2);

        // Should have merged (newer reason)
        let result = store.get(&TombstoneKey::global(id)).unwrap();
        assert_eq!(result.reason, Some("new".to_string()));
    }
}
