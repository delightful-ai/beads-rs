//! Store wrappers with auto-merge semantics.
//!
//! TombstoneStore: tombstones with upsert that auto-joins
//!
//! These ensure CRDT merge semantics are always applied correctly.

use std::collections::BTreeMap;

use crate::crdt::Crdt;

use serde::{Deserialize, Serialize};

use super::tombstone::{Tombstone, TombstoneKey};

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
            .and_modify(|existing| *existing = existing.join(&tombstone))
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
}

impl Crdt for TombstoneStore {
    /// Merge two tombstone stores.
    fn join(&self, other: &Self) -> Self {
        let mut result = self.clone();
        for t in other.by_key.values() {
            result.upsert(t.clone());
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BeadId;
    use crate::{ActorId, Stamp, WriteStamp};

    fn make_stamp(wall_ms: u64, actor: &str) -> Stamp {
        Stamp::new(WriteStamp::new(wall_ms, 0), ActorId::new(actor).unwrap())
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
