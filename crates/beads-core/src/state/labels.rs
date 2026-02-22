use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::collections::{Label, Labels};
use crate::crdt::Crdt;
use crate::identity::BeadId;
use crate::orset::{Dot, Dvv, OrSet};
use crate::time::Stamp;
use crate::wire_bead::WireLineageStamp;

use super::max_stamp;

/// Label membership for a single bead.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelState {
    pub(crate) set: OrSet<Label>,
    pub(crate) stamp: Option<Stamp>,
}

impl LabelState {
    pub fn new() -> Self {
        Self {
            set: OrSet::new(),
            stamp: None,
        }
    }

    pub fn from_parts(set: OrSet<Label>, stamp: Option<Stamp>) -> Self {
        Self { set, stamp }
    }

    pub fn stamp(&self) -> Option<&Stamp> {
        self.stamp.as_ref()
    }

    pub fn labels(&self) -> Labels {
        self.set.values().cloned().collect()
    }

    pub fn values(&self) -> impl Iterator<Item = &Label> {
        self.set.values()
    }

    pub fn dots_for(&self, label: &Label) -> Option<&BTreeSet<Dot>> {
        self.set.dots_for(label)
    }

    pub fn cc(&self) -> &Dvv {
        self.set.cc()
    }

    pub fn join(a: &Self, b: &Self) -> Self {
        Crdt::join(a, b)
    }
}

impl Crdt for LabelState {
    fn join(&self, other: &Self) -> Self {
        Self {
            set: self.set.join(&other.set),
            stamp: max_stamp(self.stamp.as_ref(), other.stamp.as_ref()),
        }
    }
}

impl Default for LabelState {
    fn default() -> Self {
        Self::new()
    }
}

/// Canonical label store keyed by bead id + lineage stamp.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LabelStore {
    by_bead: BTreeMap<BeadId, BTreeMap<Stamp, LabelState>>,
}

impl LabelStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self, id: &BeadId, lineage: &Stamp) -> Option<&LabelState> {
        self.by_bead.get(id).and_then(|states| states.get(lineage))
    }

    pub fn state_mut(&mut self, id: &BeadId, lineage: &Stamp) -> &mut LabelState {
        self.by_bead
            .entry(id.clone())
            .or_default()
            .entry(lineage.clone())
            .or_default()
    }

    pub fn insert_state(&mut self, id: BeadId, lineage: Stamp, state: LabelState) {
        self.by_bead.entry(id).or_default().insert(lineage, state);
    }

    pub(crate) fn take_state(&mut self, id: &BeadId, lineage: &Stamp) -> Option<LabelState> {
        let lineages = self.by_bead.get_mut(id)?;
        let state = lineages.remove(lineage)?;
        if lineages.is_empty() {
            self.by_bead.remove(id);
        }
        Some(state)
    }

    pub fn join(a: &Self, b: &Self) -> Self {
        Crdt::join(a, b)
    }
}

impl Crdt for LabelStore {
    fn join(&self, other: &Self) -> Self {
        let mut merged = LabelStore::new();
        let ids: BTreeSet<_> = self.by_bead.keys().chain(other.by_bead.keys()).cloned().collect();
        for id in ids {
            let mut merged_lineages: BTreeMap<Stamp, LabelState> = BTreeMap::new();
            if let Some(states) = self.by_bead.get(&id) {
                for (lineage, state) in states {
                    merged_lineages.insert(lineage.clone(), state.clone());
                }
            }
            if let Some(states) = other.by_bead.get(&id) {
                for (lineage, state) in states {
                    match merged_lineages.get(lineage) {
                        Some(existing) => {
                            merged_lineages
                                .insert(lineage.clone(), LabelState::join(existing, state));
                        }
                        None => {
                            merged_lineages.insert(lineage.clone(), state.clone());
                        }
                    }
                }
            }
            if !merged_lineages.is_empty() {
                merged.by_bead.insert(id.clone(), merged_lineages);
            }
        }
        merged
    }
}

#[derive(Serialize, Deserialize)]
struct LineageLabelState {
    lineage: WireLineageStamp,
    state: LabelState,
}

#[derive(Serialize, Deserialize)]
struct LabelStoreWire {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    by_bead: BTreeMap<BeadId, Vec<LineageLabelState>>,
}

impl Serialize for LabelStore {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut by_bead = BTreeMap::new();
        for (id, lineages) in &self.by_bead {
            let entries = lineages
                .iter()
                .map(|(lineage, state)| LineageLabelState {
                    lineage: WireLineageStamp::from(lineage.clone()),
                    state: state.clone(),
                })
                .collect::<Vec<_>>();
            if !entries.is_empty() {
                by_bead.insert(id.clone(), entries);
            }
        }
        let wire = LabelStoreWire { by_bead };
        wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for LabelStore {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LabelStoreWire::deserialize(deserializer)?;
        let mut by_bead: BTreeMap<BeadId, BTreeMap<Stamp, LabelState>> = BTreeMap::new();
        for (id, entries) in wire.by_bead {
            let mut lineages: BTreeMap<Stamp, LabelState> = BTreeMap::new();
            for entry in entries {
                let lineage = entry.lineage.stamp();
                match lineages.get(&lineage) {
                    Some(existing) => {
                        lineages.insert(lineage, LabelState::join(existing, &entry.state));
                    }
                    None => {
                        lineages.insert(lineage, entry.state);
                    }
                }
            }
            if !lineages.is_empty() {
                by_bead.insert(id, lineages);
            }
        }
        Ok(LabelStore { by_bead })
    }
}
