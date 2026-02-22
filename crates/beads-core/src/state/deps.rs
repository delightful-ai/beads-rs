use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::crdt::Crdt;
use crate::dep::DepKey;
use crate::domain::DepKind;
use crate::identity::BeadId;
use crate::orset::{Dot, Dvv, OrSet};
use crate::time::Stamp;

use super::max_stamp;

/// Canonical dependency store (OR-Set membership only).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepStore {
    pub(crate) set: OrSet<DepKey>,
    pub(crate) stamp: Option<Stamp>,
}

impl DepStore {
    pub fn new() -> Self {
        Self {
            set: OrSet::new(),
            stamp: None,
        }
    }

    pub fn from_parts(set: OrSet<DepKey>, stamp: Option<Stamp>) -> Self {
        Self { set, stamp }
    }
    pub fn stamp(&self) -> Option<&Stamp> {
        self.stamp.as_ref()
    }

    pub fn cc(&self) -> &Dvv {
        self.set.cc()
    }

    pub fn contains(&self, key: &DepKey) -> bool {
        self.set.contains(key)
    }

    pub fn len(&self) -> usize {
        self.set.values().count()
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    pub fn values(&self) -> impl Iterator<Item = &DepKey> {
        self.set.values()
    }

    pub fn dots_for(&self, key: &DepKey) -> Option<&BTreeSet<Dot>> {
        self.set.dots_for(key)
    }

    pub fn join(a: &Self, b: &Self) -> Self {
        Crdt::join(a, b)
    }
}

impl Crdt for DepStore {
    fn join(&self, other: &Self) -> Self {
        Self {
            set: self.set.join(&other.set),
            stamp: max_stamp(self.stamp.as_ref(), other.stamp.as_ref()),
        }
    }
}

impl Default for DepStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Derived indexes for efficient dependency lookups.
///
/// These are rebuilt from `dep_store` on load and updated incrementally.
/// Not serialized - derived state only.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
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
    pub(crate) fn add(&mut self, from: &BeadId, to: &BeadId, kind: DepKind) {
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
    pub(crate) fn remove(&mut self, from: &BeadId, to: &BeadId, kind: DepKind) {
        if let Some(edges) = self.out_edges.get_mut(from) {
            edges.retain(|(t, k)| !(t == to && *k == kind));
        }
        if let Some(edges) = self.in_edges.get_mut(to) {
            edges.retain(|(f, k)| !(f == from && *k == kind));
        }
    }

    /// Get outgoing edges from a bead.
    pub fn out_edges(&self, id: &BeadId) -> &[(BeadId, DepKind)] {
        self.out_edges
            .get(id)
            .map(|v: &Vec<(BeadId, DepKind)>| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get incoming edges to a bead.
    pub fn in_edges(&self, id: &BeadId) -> &[(BeadId, DepKind)] {
        self.in_edges
            .get(id)
            .map(|v: &Vec<(BeadId, DepKind)>| v.as_slice())
            .unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::laws;
    use crate::identity::{ActorId, ReplicaId};
    use crate::time::WriteStamp;
    use proptest::prelude::*;
    use uuid::Uuid;

    fn replica(id: u8) -> ReplicaId {
        ReplicaId::new(Uuid::from_bytes([id; 16]))
    }

    fn bead_id(seed: u8) -> BeadId {
        BeadId::parse(&format!("bd-test-{}", seed)).unwrap()
    }

    fn stamp(wall_ms: u64, counter: u32, actor: &str) -> Stamp {
        Stamp::new(
            WriteStamp::new(wall_ms, counter),
            ActorId::new(actor).expect("actor"),
        )
    }

    fn dep_key_strat() -> impl Strategy<Value = DepKey> {
        let bead_strat = (0u8..5).prop_map(bead_id);
        let kind_strat = prop_oneof![
            Just(DepKind::Blocks),
            Just(DepKind::Related),
            Just(DepKind::Parent),
        ];
        (bead_strat.clone(), bead_strat, kind_strat).prop_filter_map(
            "cannot depend on self",
            |(from, to, kind)| DepKey::new(from, to, kind).ok(),
        )
    }

    fn dep_store_strategy() -> impl Strategy<Value = DepStore> {
        let key_strat = dep_key_strat();
        let replica_strat = (0u8..5).prop_map(replica);
        let dot_strat =
            (replica_strat, 1u64..10).prop_map(|(r, c)| Dot { replica: r, counter: c });
        let stamp_strat =
            (0u64..1000, 0u32..5, "[a-z]").prop_map(|(w, c, a)| Some(stamp(w, c, &a)));

        (
            proptest::collection::vec((dot_strat, key_strat), 0..5),
            stamp_strat,
        )
            .prop_map(|(ops, stamp)| {
                let mut set = OrSet::new();
                for (dot, key) in ops {
                    set.apply_add(dot, key);
                }
                DepStore { set, stamp }
            })
    }

    proptest! {
        #[test]
        fn dep_store_satisfies_laws(
            a in dep_store_strategy(),
            b in dep_store_strategy(),
            c in dep_store_strategy()
        ) {
            laws::check_crdt_laws(a, b, c);
        }
    }
}
