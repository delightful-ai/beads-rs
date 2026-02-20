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
#[derive(Clone, Debug, Serialize, Deserialize)]
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
