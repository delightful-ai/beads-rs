//! Layer 9: Canonical State
//!
//! The single source of truth for beads, tombstones, and deps.
//!
//! INVARIANT: each BeadId maps to either a live bead or a global tombstone.
//! This is structural via BeadEntry; lineage-scoped collision tombstones are separate.
//!
//! Collision tombstones are lineage-scoped and may coexist with a live bead of
//! the same ID when the live bead is a different lineage (SPEC §4.1.1).
//!
//! Resurrection rule: modification strictly newer than deletion can resurrect.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::OnceLock;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::bead::{Bead, BeadView, SameLineageBead};
use super::collections::{Label, Labels};
use super::composite::Note;
use super::dep::{AcyclicDepKey, DepAddKey, DepKey, FreeDepKey, NoCycleProof, ParentEdge};
use super::domain::DepKind;
use super::error::{CoreError, InvalidDependency};
use super::event::sha256_bytes;
use super::identity::{ActorId, BeadId, ContentHash, NoteId};
use super::orset::{Dot, Dvv, OrSet, OrSetChange};
use super::time::{Stamp, WallClock, WriteStamp};
use super::tombstone::{Tombstone, TombstoneKey};
use super::wire_bead::WireLineageStamp;

/// Label membership for a single bead.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabelState {
    set: OrSet<Label>,
    stamp: Option<Stamp>,
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
        Self {
            set: OrSet::join(&a.set, &b.set),
            stamp: max_stamp(a.stamp.as_ref(), b.stamp.as_ref()),
        }
    }
}

impl Default for LabelState {
    fn default() -> Self {
        Self::new()
    }
}

/// Deterministic fallback lineage for legacy data without lineage stamps.
///
/// This is a compatibility-only marker used during import and legacy op handling.
/// Any state stored under this lineage should be migrated to a real bead lineage
/// when possible; read paths never consult it directly.
pub fn legacy_fallback_lineage() -> Stamp {
    static STAMP: OnceLock<Stamp> = OnceLock::new();
    STAMP
        .get_or_init(|| {
            Stamp::new(
                WriteStamp::new(0, 0),
                ActorId::new("legacy").expect("legacy actor id"),
            )
        })
        .clone()
}

/// Canonical label store keyed by bead id + lineage stamp.
#[derive(Clone, Debug, Default)]
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

    fn take_state(&mut self, id: &BeadId, lineage: &Stamp) -> Option<LabelState> {
        let lineages = self.by_bead.get_mut(id)?;
        let state = lineages.remove(lineage)?;
        if lineages.is_empty() {
            self.by_bead.remove(id);
        }
        Some(state)
    }

    pub fn join(a: &Self, b: &Self) -> Self {
        let mut merged = LabelStore::new();
        let ids: BTreeSet<_> = a.by_bead.keys().chain(b.by_bead.keys()).cloned().collect();
        for id in ids {
            let mut merged_lineages: BTreeMap<Stamp, LabelState> = BTreeMap::new();
            if let Some(states) = a.by_bead.get(&id) {
                for (lineage, state) in states {
                    merged_lineages.insert(lineage.clone(), state.clone());
                }
            }
            if let Some(states) = b.by_bead.get(&id) {
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

/// Canonical dependency store (OR-Set membership only).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DepStore {
    set: OrSet<DepKey>,
    stamp: Option<Stamp>,
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
        Self {
            set: OrSet::join(&a.set, &b.set),
            stamp: max_stamp(a.stamp.as_ref(), b.stamp.as_ref()),
        }
    }
}

impl Default for DepStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Canonical note store keyed by bead id + lineage stamp.
#[derive(Clone, Debug, Default)]
pub struct NoteStore {
    by_bead: BTreeMap<BeadId, BTreeMap<Stamp, BTreeMap<NoteId, Note>>>,
}

impl NoteStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, id: BeadId, lineage: Stamp, note: Note) -> Option<Note> {
        let entry = self
            .by_bead
            .entry(id)
            .or_default()
            .entry(lineage)
            .or_default();
        if let Some(existing) = entry.get(&note.id) {
            return Some(existing.clone());
        }
        entry.insert(note.id.clone(), note);
        None
    }

    pub fn replace(&mut self, id: BeadId, lineage: Stamp, note: Note) -> Option<Note> {
        let entry = self
            .by_bead
            .entry(id)
            .or_default()
            .entry(lineage)
            .or_default();
        entry.insert(note.id.clone(), note)
    }

    pub fn get(&self, id: &BeadId, lineage: &Stamp, note_id: &NoteId) -> Option<&Note> {
        self.by_bead
            .get(id)
            .and_then(|notes| notes.get(lineage))
            .and_then(|notes| notes.get(note_id))
    }

    pub fn note_id_exists(&self, id: &BeadId, note_id: &NoteId) -> bool {
        if let Some(lineages) = self.by_bead.get(id) {
            for notes in lineages.values() {
                if notes.contains_key(note_id) {
                    return true;
                }
            }
        }
        false
    }

    pub fn notes_for(&self, id: &BeadId, lineage: &Stamp) -> Vec<&Note> {
        let Some(notes) = self.by_bead.get(id).and_then(|notes| notes.get(lineage)) else {
            return Vec::new();
        };
        let mut out: Vec<&Note> = notes.values().collect();
        out.sort_by(|a, b| a.at.cmp(&b.at).then_with(|| a.id.cmp(&b.id)));
        out
    }

    pub fn iter_lineages(
        &self,
    ) -> impl Iterator<Item = (&BeadId, &Stamp, &BTreeMap<NoteId, Note>)> {
        self.by_bead.iter().flat_map(|(id, lineages)| {
            lineages
                .iter()
                .map(move |(lineage, notes)| (id, lineage, notes))
        })
    }

    fn take_lineage_notes(
        &mut self,
        id: &BeadId,
        lineage: &Stamp,
    ) -> Option<BTreeMap<NoteId, Note>> {
        let lineages = self.by_bead.get_mut(id)?;
        let notes = lineages.remove(lineage)?;
        if lineages.is_empty() {
            self.by_bead.remove(id);
        }
        Some(notes)
    }

    pub fn join(a: &Self, b: &Self) -> Self {
        let mut merged = NoteStore::new();
        for (id, lineages) in a.by_bead.iter().chain(b.by_bead.iter()) {
            let entry = merged.by_bead.entry(id.clone()).or_default();
            for (lineage, notes) in lineages {
                let lineage_entry = entry.entry(lineage.clone()).or_default();
                for (note_id, note) in notes {
                    match lineage_entry.get(note_id) {
                        None => {
                            lineage_entry.insert(note_id.clone(), note.clone());
                        }
                        Some(existing) => {
                            if note_collision_cmp(existing, note) == Ordering::Less {
                                lineage_entry.insert(note_id.clone(), note.clone());
                            }
                        }
                    }
                }
            }
        }

        merged
    }
}

#[derive(Serialize, Deserialize)]
struct LineageNotes {
    lineage: WireLineageStamp,
    notes: BTreeMap<NoteId, Note>,
}

#[derive(Serialize, Deserialize)]
struct NoteStoreWire {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    by_bead: BTreeMap<BeadId, Vec<LineageNotes>>,
}

impl Serialize for NoteStore {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut by_bead = BTreeMap::new();
        for (id, lineages) in &self.by_bead {
            let entries = lineages
                .iter()
                .map(|(lineage, notes)| LineageNotes {
                    lineage: WireLineageStamp::from(lineage.clone()),
                    notes: notes.clone(),
                })
                .collect::<Vec<_>>();
            if !entries.is_empty() {
                by_bead.insert(id.clone(), entries);
            }
        }
        let wire = NoteStoreWire { by_bead };
        wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NoteStore {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = NoteStoreWire::deserialize(deserializer)?;
        let mut by_bead: BTreeMap<BeadId, BTreeMap<Stamp, BTreeMap<NoteId, Note>>> =
            BTreeMap::new();
        for (id, entries) in wire.by_bead {
            let mut lineages: BTreeMap<Stamp, BTreeMap<NoteId, Note>> = BTreeMap::new();
            for entry in entries {
                let lineage = entry.lineage.stamp();
                match lineages.get(&lineage) {
                    Some(existing) => {
                        let mut merged = existing.clone();
                        for (note_id, note) in entry.notes {
                            match merged.get(&note_id) {
                                None => {
                                    merged.insert(note_id, note);
                                }
                                Some(existing_note) => {
                                    if note_collision_cmp(existing_note, &note) == Ordering::Less {
                                        merged.insert(note_id, note);
                                    }
                                }
                            }
                        }
                        lineages.insert(lineage, merged);
                    }
                    None => {
                        lineages.insert(lineage, entry.notes);
                    }
                }
            }
            if !lineages.is_empty() {
                by_bead.insert(id, lineages);
            }
        }
        Ok(NoteStore { by_bead })
    }
}

fn max_stamp(a: Option<&Stamp>, b: Option<&Stamp>) -> Option<Stamp> {
    match (a, b) {
        (Some(left), Some(right)) => Some(if left >= right {
            left.clone()
        } else {
            right.clone()
        }),
        (Some(stamp), None) | (None, Some(stamp)) => Some(stamp.clone()),
        (None, None) => None,
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

/// Bead entry stored by ID.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BeadEntry {
    Live(Box<Bead>),
    Tombstone(Box<Tombstone>),
}

/// Canonical state - the CRDT for the entire bead store.
///
/// Invariant: An ID cannot be both live and globally deleted (tombstone lineage=None).
/// Collision tombstones (tombstone lineage=Some(created)) may coexist with a live bead
/// of the same ID, as long as the live bead has a different creation stamp.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CanonicalState {
    beads: BTreeMap<BeadId, BeadEntry>,
    collision_tombstones: BTreeMap<TombstoneKey, Tombstone>,
    #[serde(default)]
    labels: LabelStore,
    #[serde(default)]
    dep_store: DepStore,
    #[serde(default)]
    notes: NoteStore,
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
        match self.beads.get(id) {
            Some(BeadEntry::Live(bead)) => Some(bead.as_ref()),
            _ => None,
        }
    }

    pub fn get_mut(&mut self, id: &BeadId) -> Option<&mut Bead> {
        match self.beads.get_mut(id) {
            Some(BeadEntry::Live(bead)) => Some(bead.as_mut()),
            _ => None,
        }
    }

    pub fn is_deleted(&self, id: &BeadId) -> bool {
        matches!(self.beads.get(id), Some(BeadEntry::Tombstone(_)))
    }

    pub fn get_tombstone(&self, id: &BeadId) -> Option<&Tombstone> {
        match self.beads.get(id) {
            Some(BeadEntry::Tombstone(tomb)) => Some(tomb.as_ref()),
            _ => None,
        }
    }

    pub fn has_lineage_tombstone(&self, id: &BeadId, lineage: &Stamp) -> bool {
        self.collision_tombstones
            .contains_key(&TombstoneKey::lineage(id.clone(), lineage.clone()))
    }

    pub fn has_collision_tombstone(&self, id: &BeadId) -> bool {
        self.collision_tombstones.keys().any(|k| &k.id == id)
    }

    pub fn contains(&self, id: &BeadId) -> bool {
        self.beads.contains_key(id) || self.has_any_tombstone(id)
    }

    pub fn live_count(&self) -> usize {
        self.beads
            .values()
            .filter(|entry| matches!(entry, BeadEntry::Live(_)))
            .count()
    }

    pub fn tombstone_count(&self) -> usize {
        let globals = self
            .beads
            .values()
            .filter(|entry| matches!(entry, BeadEntry::Tombstone(_)))
            .count();
        globals + self.collision_tombstones.len()
    }

    pub fn dep_count(&self) -> usize {
        self.dep_store.len()
    }

    pub fn dep_contains(&self, key: &DepKey) -> bool {
        self.dep_store.contains(key)
    }

    pub fn iter_live(&self) -> impl Iterator<Item = (&BeadId, &Bead)> {
        self.beads.iter().filter_map(|(id, entry)| match entry {
            BeadEntry::Live(bead) => Some((id, bead.as_ref())),
            _ => None,
        })
    }

    pub fn iter_tombstones(&self) -> impl Iterator<Item = (TombstoneKey, &Tombstone)> {
        let globals = self.beads.iter().filter_map(|(id, entry)| match entry {
            BeadEntry::Tombstone(tomb) => Some((TombstoneKey::global(id.clone()), tomb.as_ref())),
            _ => None,
        });
        let collisions = self
            .collision_tombstones
            .iter()
            .map(|(key, tomb)| (key.clone(), tomb));
        globals.chain(collisions)
    }

    pub fn labels_for(&self, id: &BeadId) -> Labels {
        let Some(bead) = self.get_live(id) else {
            return Labels::new();
        };
        let lineage = bead.core.created();
        self.labels
            .state(id, lineage)
            .map(LabelState::labels)
            .unwrap_or_default()
    }

    pub fn labels_for_lineage(&self, id: &BeadId, lineage: &Stamp) -> Labels {
        self.labels
            .state(id, lineage)
            .map(LabelState::labels)
            .unwrap_or_default()
    }

    pub fn label_dvv(&self, id: &BeadId, label: &Label, lineage: &Stamp) -> Dvv {
        let mut dots = BTreeSet::new();
        let mut collect = |state: Option<&LabelState>| {
            if let Some(state) = state
                && let Some(entries) = state.dots_for(label)
            {
                dots.extend(entries.iter().copied());
            }
        };
        collect(self.labels.state(id, lineage));

        Dvv::from_dots(dots.iter().copied())
    }

    pub fn dep_dvv(&self, key: &DepKey) -> Dvv {
        let dots = self.dep_store.dots_for(key);
        Self::dvv_from_dots(dots)
    }

    pub fn label_stamp(&self, id: &BeadId) -> Option<Stamp> {
        let bead = self.get_live(id)?;
        let lineage = bead.core.created();
        self.labels
            .state(id, lineage)
            .and_then(|state| state.stamp())
            .cloned()
    }

    pub fn notes_for(&self, id: &BeadId) -> Vec<&Note> {
        let Some(bead) = self.get_live(id) else {
            return Vec::new();
        };
        let lineage = bead.core.created();
        self.notes.notes_for(id, lineage)
    }

    pub fn note_id_exists(&self, id: &BeadId, note_id: &NoteId) -> bool {
        self.notes.note_id_exists(id, note_id)
    }

    pub fn label_store(&self) -> &LabelStore {
        &self.labels
    }

    pub fn note_store(&self) -> &NoteStore {
        &self.notes
    }

    pub fn dep_store(&self) -> &DepStore {
        &self.dep_store
    }

    pub fn set_label_store(&mut self, labels: LabelStore) {
        self.labels = labels;
    }

    pub fn set_note_store(&mut self, notes: NoteStore) {
        self.notes = notes;
    }

    pub fn set_dep_store(&mut self, dep_store: DepStore) {
        self.dep_store = dep_store;
        self.rebuild_dep_indexes();
    }

    fn absorb_legacy_lineage(&mut self, id: &BeadId, lineage: &Stamp) {
        if self.has_collision_tombstone(id) {
            return;
        }
        let legacy_lineage = legacy_fallback_lineage();
        if &legacy_lineage == lineage {
            return;
        }

        if let Some(legacy_state) = self.labels.take_state(id, &legacy_lineage) {
            let state = self.labels.state_mut(id, lineage);
            *state = LabelState::join(state, &legacy_state);
        }

        if let Some(legacy_notes) = self.notes.take_lineage_notes(id, &legacy_lineage) {
            let entry = self
                .notes
                .by_bead
                .entry(id.clone())
                .or_default()
                .entry(lineage.clone())
                .or_default();
            for (note_id, note) in legacy_notes {
                match entry.get(&note_id) {
                    None => {
                        entry.insert(note_id, note);
                    }
                    Some(existing) => {
                        if note_collision_cmp(existing, &note) == Ordering::Less {
                            entry.insert(note_id, note);
                        }
                    }
                }
            }
        }
    }

    pub fn bead_view(&self, id: &BeadId) -> Option<BeadView> {
        let bead = self.get(id)?.clone();
        let labels = self.labels_for(id);
        let notes = self
            .notes
            .notes_for(id, bead.core.created())
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let label_stamp = self.label_stamp(id);
        Some(BeadView::new(bead, labels, notes, label_stamp))
    }

    pub fn updated_stamp_for(&self, id: &BeadId) -> Option<Stamp> {
        self.bead_view(id).map(|view| view.updated_stamp().clone())
    }

    pub fn content_hash_for(&self, id: &BeadId) -> Option<super::identity::ContentHash> {
        self.bead_view(id).map(|view| *view.content_hash())
    }

    fn updated_stamp_for_merge(&self, id: &BeadId, bead: &Bead) -> Stamp {
        let lineage = bead.core.created();
        let labels = self.labels_for_lineage(id, lineage);
        let notes = self
            .notes
            .notes_for(id, lineage)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let label_stamp = self
            .labels
            .state(id, lineage)
            .and_then(|state| state.stamp())
            .cloned();
        BeadView::new(bead.clone(), labels, notes, label_stamp)
            .updated_stamp()
            .clone()
    }

    pub fn apply_label_add(
        &mut self,
        id: BeadId,
        label: Label,
        dot: Dot,
        stamp: Stamp,
        lineage: Stamp,
    ) -> OrSetChange<Label> {
        let state = self.labels.state_mut(&id, &lineage);
        let change = state.set.apply_add(dot, label);
        if change.changed() {
            state.stamp = max_stamp(state.stamp.as_ref(), Some(&stamp));
        }
        change
    }

    pub fn apply_label_remove(
        &mut self,
        id: BeadId,
        label: &Label,
        ctx: &Dvv,
        stamp: Stamp,
        lineage: Stamp,
    ) -> OrSetChange<Label> {
        let state = self.labels.state_mut(&id, &lineage);
        let change = state.set.apply_remove(label, ctx);
        if change.changed() {
            state.stamp = max_stamp(state.stamp.as_ref(), Some(&stamp));
        }
        change
    }

    pub fn apply_dep_add(&mut self, key: DepAddKey, dot: Dot, stamp: Stamp) -> OrSetChange<DepKey> {
        let key = key.into_dep_key();
        let change = self.dep_store.set.apply_add(dot, key.clone());
        if change.changed() {
            self.dep_store.stamp = max_stamp(self.dep_store.stamp.as_ref(), Some(&stamp));
        }
        if !change.is_empty() {
            for added in &change.added {
                self.dep_indexes.add(added.from(), added.to(), added.kind());
            }
            for removed in &change.removed {
                self.dep_indexes
                    .remove(removed.from(), removed.to(), removed.kind());
            }
        }
        change
    }

    /// Check that adding a DAG-only edge would not introduce a cycle.
    pub fn check_no_cycle(
        &self,
        from: &BeadId,
        to: &BeadId,
        kind: DepKind,
    ) -> Result<NoCycleProof, InvalidDependency> {
        if !kind.requires_dag() {
            return Err(InvalidDependency {
                reason: format!(
                    "dependency kind {} does not require DAG proof",
                    kind.as_str()
                ),
            });
        }

        let mut visited = HashSet::new();
        let mut queue = vec![to.clone()];

        while let Some(current) = queue.pop() {
            if &current == from {
                return Err(InvalidDependency {
                    reason: format!(
                        "circular dependency: {} already depends on {} (directly or transitively)",
                        to, from
                    ),
                });
            }
            if !visited.insert(current.clone()) {
                continue;
            }
            for key in self.deps_from(&current) {
                if key.kind().requires_dag() && !visited.contains(key.to()) {
                    queue.push(key.to().clone());
                }
            }
        }

        Ok(NoCycleProof::new())
    }

    /// Convert a raw dep key into a typed key, enforcing DAG proofs for ordering deps.
    pub fn check_dep_add_key(&self, key: DepKey) -> Result<DepAddKey, InvalidDependency> {
        if key.kind().requires_dag() {
            let proof = self.check_no_cycle(key.from(), key.to(), key.kind())?;
            let key = AcyclicDepKey::from_dep_key(key, proof)?;
            Ok(DepAddKey::Acyclic(key))
        } else {
            let key = FreeDepKey::from_dep_key(key)?;
            Ok(DepAddKey::Free(key))
        }
    }

    pub fn apply_dep_remove(
        &mut self,
        key: &DepKey,
        ctx: &Dvv,
        stamp: Stamp,
    ) -> OrSetChange<DepKey> {
        let change = self.dep_store.set.apply_remove(key, ctx);
        if change.changed() {
            self.dep_store.stamp = max_stamp(self.dep_store.stamp.as_ref(), Some(&stamp));
        }
        if !change.is_empty() {
            for removed in &change.removed {
                self.dep_indexes
                    .remove(removed.from(), removed.to(), removed.kind());
            }
        }
        change
    }

    pub fn insert_note(&mut self, id: BeadId, lineage: Stamp, note: Note) -> Option<Note> {
        self.notes.insert(id, lineage, note)
    }

    pub fn replace_note(&mut self, id: BeadId, lineage: Stamp, note: Note) -> Option<Note> {
        self.notes.replace(id, lineage, note)
    }

    /// Get a live bead by ID (alias for get).
    pub fn get_live(&self, id: &BeadId) -> Option<&Bead> {
        self.get(id)
    }

    /// Get a mutable live bead by ID.
    pub fn get_live_mut(&mut self, id: &BeadId) -> Option<&mut Bead> {
        self.get_mut(id)
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
        match self.beads.get(id) {
            Some(BeadEntry::Live(bead)) => Ok(bead.as_ref()),
            Some(BeadEntry::Tombstone(_)) => Err(LiveLookupError::Deleted),
            None => Err(LiveLookupError::NotFound),
        }
    }

    /// Require a mutable live bead, returning appropriate error if not found or deleted.
    pub fn require_live_mut(&mut self, id: &BeadId) -> Result<&mut Bead, LiveLookupError> {
        match self.beads.get_mut(id) {
            Some(BeadEntry::Live(bead)) => Ok(bead.as_mut()),
            Some(BeadEntry::Tombstone(_)) => Err(LiveLookupError::Deleted),
            None => Err(LiveLookupError::NotFound),
        }
    }

    /// Get the maximum WriteStamp across all beads.
    ///
    /// Used for clock synchronization after sync.
    pub fn max_write_stamp(&self) -> Option<super::time::WriteStamp> {
        self.beads
            .iter()
            .filter_map(|(id, entry)| match entry {
                BeadEntry::Live(_) => self.updated_stamp_for(id).map(|s| s.at.clone()),
                _ => None,
            })
            .max()
    }

    // =========================================================================
    // Mutations (enforce invariant)
    // =========================================================================

    /// Insert a bead - removes any tombstone for this ID.
    ///
    /// If bead already exists, merges via SameLineageBead::join.
    /// Returns Err on ID collision (same ID, different creation stamp).
    pub fn insert(&mut self, bead: Bead) -> Result<(), CoreError> {
        let id = bead.core.id.clone();
        let merged = match self.beads.remove(&id) {
            Some(BeadEntry::Live(existing)) => {
                let (a, b) = Bead::same_lineage(existing.as_ref(), &bead)?;
                SameLineageBead::join(a, b)?
            }
            Some(BeadEntry::Tombstone(_)) | None => bead,
        };
        self.absorb_legacy_lineage(&id, merged.core.created());
        self.beads.insert(id, BeadEntry::Live(Box::new(merged)));

        Ok(())
    }

    /// Delete a bead - adds tombstone, removes from live.
    ///
    /// If tombstone already exists, merges (keeps later deletion).
    pub fn delete(&mut self, tombstone: Tombstone) {
        if tombstone.lineage.is_some() {
            self.insert_tombstone(tombstone);
            return;
        }

        let id = tombstone.id.clone();
        let merged = match self.beads.remove(&id) {
            Some(BeadEntry::Tombstone(existing)) => Tombstone::join(existing.as_ref(), &tombstone),
            _ => tombstone,
        };
        self.beads
            .insert(id, BeadEntry::Tombstone(Box::new(merged)));
    }

    /// Remove a global deletion tombstone.
    pub fn remove_global_tombstone(&mut self, id: &BeadId) -> Option<Tombstone> {
        match self.beads.get(id) {
            Some(BeadEntry::Tombstone(_)) => match self.beads.remove(id) {
                Some(BeadEntry::Tombstone(tomb)) => Some(*tomb),
                _ => None,
            },
            _ => None,
        }
    }

    /// Remove a live bead by ID, returning it if present.
    pub fn remove_live(&mut self, id: &BeadId) -> Option<Bead> {
        match self.beads.get(id) {
            Some(BeadEntry::Live(_)) => match self.beads.remove(id) {
                Some(BeadEntry::Live(bead)) => Some(*bead),
                _ => None,
            },
            _ => None,
        }
    }

    /// Insert a bead directly without CRDT merge.
    ///
    /// Used for collision resolution when we've already handled the logic.
    /// Removes any tombstone for this ID.
    pub fn insert_live(&mut self, bead: Bead) {
        self.absorb_legacy_lineage(&bead.core.id, bead.core.created());
        self.beads
            .insert(bead.core.id.clone(), BeadEntry::Live(Box::new(bead)));
    }

    /// Insert a tombstone directly.
    ///
    /// Used for collision resolution. Does not remove live beads.
    pub fn insert_tombstone(&mut self, tombstone: Tombstone) {
        if tombstone.lineage.is_none() {
            self.delete(tombstone);
            return;
        }

        let key = tombstone.key();
        self.collision_tombstones
            .entry(key)
            .and_modify(|t| *t = Tombstone::join(t, &tombstone))
            .or_insert(tombstone);
    }

    fn dvv_from_dots(dots: Option<&BTreeSet<Dot>>) -> Dvv {
        if let Some(dots) = dots {
            Dvv::from_dots(dots.iter().copied())
        } else {
            Dvv::default()
        }
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
        let errors = Vec::new();

        // Merge collision tombstones by key.
        for (key, tomb) in a
            .collision_tombstones
            .iter()
            .chain(b.collision_tombstones.iter())
        {
            result
                .collision_tombstones
                .entry(key.clone())
                .and_modify(|t| *t = Tombstone::join(t, tomb))
                .or_insert_with(|| tomb.clone());
        }

        result.labels = LabelStore::join(&a.labels, &b.labels);
        result.dep_store = DepStore::join(&a.dep_store, &b.dep_store);
        result.notes = NoteStore::join(&a.notes, &b.notes);

        // Collect all bead IDs from both sides (including collision tombstones).
        let all_ids: BTreeSet<_> = a
            .beads
            .keys()
            .chain(b.beads.keys())
            .chain(a.collision_tombstones.keys().map(|k| &k.id))
            .chain(b.collision_tombstones.keys().map(|k| &k.id))
            .cloned()
            .collect();

        for id in all_ids {
            let (a_bead, a_tomb) = match a.beads.get(&id) {
                Some(BeadEntry::Live(bead)) => (Some(bead.as_ref()), None),
                Some(BeadEntry::Tombstone(tomb)) => (None, Some(tomb.as_ref())),
                None => (None, None),
            };
            let (b_bead, b_tomb) = match b.beads.get(&id) {
                Some(BeadEntry::Live(bead)) => (Some(bead.as_ref()), None),
                Some(BeadEntry::Tombstone(tomb)) => (None, Some(tomb.as_ref())),
                None => (None, None),
            };

            // Merge beads if both exist (resolve collisions deterministically)
            let merged_bead = match (a_bead, b_bead) {
                (Some(ab), Some(bb)) => match Bead::same_lineage(ab, bb)
                    .and_then(|(a, b)| SameLineageBead::join(a, b))
                {
                    Ok(merged) => Some(merged),
                    Err(_) => {
                        let ordering = bead_collision_cmp(&result, ab, bb);
                        let (winner, loser_stamp) = if ordering == Ordering::Less {
                            (bb.clone(), ab.core.created().clone())
                        } else {
                            (ab.clone(), bb.core.created().clone())
                        };
                        let deleted =
                            std::cmp::max(ab.core.created().clone(), bb.core.created().clone());
                        result.insert_tombstone(Tombstone::new_collision(
                            id.clone(),
                            deleted,
                            loser_stamp,
                            None,
                        ));
                        Some(winner)
                    }
                },
                (Some(b), None) | (None, Some(b)) => Some(b.clone()),
                (None, None) => None,
            };

            let merged_tomb = match (a_tomb, b_tomb) {
                (Some(at), Some(bt)) => Some(Tombstone::join(at, bt)),
                (Some(t), None) | (None, Some(t)) => Some(t.clone()),
                (None, None) => None,
            };

            let mut final_bead = merged_bead;
            let mut final_tomb = merged_tomb;

            if let Some(bead) = final_bead.as_ref() {
                // Collision tombstone: if a lineage-scoped tombstone exists for this
                // bead's creation stamp, it always wins and permanently suppresses
                // that lineage at this ID.
                let collision_key = TombstoneKey::lineage(id.clone(), bead.core.created().clone());
                if result.collision_tombstones.contains_key(&collision_key) {
                    final_bead = None;
                }
            }

            if let (Some(bead), Some(tomb)) = (final_bead.as_ref(), final_tomb.as_ref()) {
                // RESURRECTION RULE: bead wins if strictly newer than deletion
                let updated = result.updated_stamp_for_merge(&id, bead);
                if updated > tomb.deleted {
                    final_tomb = None;
                } else {
                    final_bead = None;
                }
            }

            if let Some(bead) = final_bead {
                result.beads.insert(id, BeadEntry::Live(Box::new(bead)));
            } else if let Some(tomb) = final_tomb {
                result
                    .beads
                    .insert(id, BeadEntry::Tombstone(Box::new(tomb)));
            }
        }

        // Rebuild derived indexes from merged dep store
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
        let before = self.tombstone_count();
        self.beads.retain(|_, entry| match entry {
            BeadEntry::Tombstone(tomb) => tomb.deleted.at.wall_ms + ttl_ms > now.0,
            _ => true,
        });
        self.collision_tombstones
            .retain(|_, tomb| tomb.deleted.at.wall_ms + ttl_ms > now.0);
        before - self.tombstone_count()
    }

    /// Get all active deps for a bead (outgoing).
    ///
    /// Returns dep keys only; membership is tracked in the OR-Set.
    /// Uses the derived index for O(neighbors) lookup instead of O(all deps).
    pub fn deps_from(&self, id: &BeadId) -> Vec<DepKey> {
        if self.get_live(id).is_none() {
            return Vec::new();
        }
        self.dep_indexes
            .out_edges(id)
            .iter()
            .filter(|(to, _)| self.get_live(to).is_some())
            .filter_map(|(to, kind)| DepKey::new(id.clone(), to.clone(), *kind).ok())
            .collect()
    }

    /// Get all active deps to a bead (incoming).
    ///
    /// Returns dep keys only; membership is tracked in the OR-Set.
    /// Uses the derived index for O(neighbors) lookup instead of O(all deps).
    pub fn deps_to(&self, id: &BeadId) -> Vec<DepKey> {
        if self.get_live(id).is_none() {
            return Vec::new();
        }
        self.dep_indexes
            .in_edges(id)
            .iter()
            .filter(|(from, _)| self.get_live(from).is_some())
            .filter_map(|(from, kind)| DepKey::new(from.clone(), id.clone(), *kind).ok())
            .collect()
    }

    /// Get parent edges outgoing from a bead (child -> parent).
    pub fn parent_edges_from(&self, child: &BeadId) -> Vec<ParentEdge> {
        self.deps_from(child)
            .into_iter()
            .filter_map(|key| ParentEdge::try_from(key).ok())
            .collect()
    }

    /// Get parent edges incoming to a bead (child -> parent).
    pub fn parent_edges_to(&self, parent: &BeadId) -> Vec<ParentEdge> {
        self.dep_indexes
            .in_edges(parent)
            .iter()
            .filter(|(from, kind)| *kind == DepKind::Parent && self.get_live(from).is_some())
            .filter_map(|(from, _)| ParentEdge::new(from.clone(), parent.clone()).ok())
            .collect()
    }

    /// Get all parent edges in the state.
    pub fn parent_edges(&self) -> Vec<ParentEdge> {
        self.dep_store
            .values()
            .filter_map(|key| ParentEdge::try_from(key.clone()).ok())
            .collect()
    }

    /// Rebuild the derived dep indexes from scratch.
    ///
    /// Call this after deserializing state or after `join()`.
    pub fn rebuild_dep_indexes(&mut self) {
        self.dep_indexes = DepIndexes::new();
        for key in self.dep_store.values() {
            self.dep_indexes.add(key.from(), key.to(), key.kind());
        }
    }

    /// Access the dep indexes (for queries that need direct access).
    pub fn dep_indexes(&self) -> &DepIndexes {
        &self.dep_indexes
    }

    /// Detect dependency cycles among active deps.
    ///
    /// Returns cycles as paths that start and end at the same bead ID.
    /// The ordering is deterministic.
    pub fn dependency_cycles(&self) -> Vec<Vec<BeadId>> {
        use std::collections::{BTreeMap, BTreeSet};

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum VisitState {
            Visiting,
            Visited,
        }

        let mut adjacency: BTreeMap<BeadId, Vec<BeadId>> = BTreeMap::new();
        let mut nodes: BTreeSet<BeadId> = BTreeSet::new();
        for key in self.dep_store.values() {
            if self.get_live(key.from()).is_none() || self.get_live(key.to()).is_none() {
                continue;
            }
            nodes.insert(key.from().clone());
            nodes.insert(key.to().clone());
            adjacency
                .entry(key.from().clone())
                .or_default()
                .push(key.to().clone());
        }
        for targets in adjacency.values_mut() {
            targets.sort();
        }

        let mut state: BTreeMap<BeadId, VisitState> = BTreeMap::new();
        let mut stack: Vec<BeadId> = Vec::new();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut cycles: Vec<Vec<BeadId>> = Vec::new();

        fn normalize_cycle(cycle: &[BeadId]) -> Vec<BeadId> {
            let mut base: Vec<BeadId> = cycle.to_vec();
            if base.len() > 1 && base.first() == base.last() {
                base.pop();
            }
            if base.is_empty() {
                return base;
            }
            let n = base.len();
            let mut best = 0;
            for i in 1..n {
                let a = base[i].as_str();
                let b = base[best].as_str();
                if a < b {
                    best = i;
                    continue;
                }
                if a == b {
                    for offset in 1..n {
                        let lhs = base[(i + offset) % n].as_str();
                        let rhs = base[(best + offset) % n].as_str();
                        if lhs < rhs {
                            best = i;
                            break;
                        }
                        if lhs > rhs {
                            break;
                        }
                    }
                }
            }
            let mut out = Vec::with_capacity(n + 1);
            for offset in 0..n {
                out.push(base[(best + offset) % n].clone());
            }
            out.push(out[0].clone());
            out
        }

        fn cycle_key(cycle: &[BeadId]) -> String {
            cycle
                .iter()
                .map(|id| id.as_str())
                .collect::<Vec<_>>()
                .join(">")
        }

        fn dfs(
            node: &BeadId,
            adjacency: &BTreeMap<BeadId, Vec<BeadId>>,
            state: &mut BTreeMap<BeadId, VisitState>,
            stack: &mut Vec<BeadId>,
            seen: &mut BTreeSet<String>,
            cycles: &mut Vec<Vec<BeadId>>,
        ) {
            state.insert(node.clone(), VisitState::Visiting);
            stack.push(node.clone());

            if let Some(targets) = adjacency.get(node) {
                for target in targets {
                    match state.get(target) {
                        Some(VisitState::Visiting) => {
                            if let Some(pos) = stack.iter().position(|id| id == target) {
                                let mut cycle = stack[pos..].to_vec();
                                cycle.push(target.clone());
                                let normalized = normalize_cycle(&cycle);
                                let key = cycle_key(&normalized);
                                if seen.insert(key) {
                                    cycles.push(normalized);
                                }
                            }
                        }
                        Some(VisitState::Visited) => {}
                        None => dfs(target, adjacency, state, stack, seen, cycles),
                    }
                }
            }

            stack.pop();
            state.insert(node.clone(), VisitState::Visited);
        }

        for node in nodes {
            if !state.contains_key(&node) {
                dfs(
                    &node,
                    &adjacency,
                    &mut state,
                    &mut stack,
                    &mut seen,
                    &mut cycles,
                );
            }
        }

        cycles.sort_by_key(|cycle| cycle_key(cycle));
        cycles
    }

    fn has_any_tombstone(&self, id: &BeadId) -> bool {
        if matches!(self.beads.get(id), Some(BeadEntry::Tombstone(_))) {
            return true;
        }
        self.collision_tombstones.keys().any(|k| &k.id == id)
    }
}

fn bead_content_hash_for_collision(state: &CanonicalState, bead: &Bead) -> ContentHash {
    let lineage = bead.core.created();
    let labels = state.labels_for_lineage(bead.id(), lineage);
    let notes = state
        .note_store()
        .notes_for(bead.id(), lineage)
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let label_stamp = state
        .label_store()
        .state(bead.id(), lineage)
        .and_then(|state| state.stamp())
        .cloned();
    *BeadView::new(bead.clone(), labels, notes, label_stamp).content_hash()
}

pub fn bead_collision_cmp(state: &CanonicalState, existing: &Bead, incoming: &Bead) -> Ordering {
    existing
        .core
        .created()
        .cmp(incoming.core.created())
        .then_with(|| {
            bead_content_hash_for_collision(state, existing)
                .as_bytes()
                .cmp(bead_content_hash_for_collision(state, incoming).as_bytes())
        })
}

pub fn note_collision_cmp(existing: &Note, incoming: &Note) -> Ordering {
    existing
        .at
        .cmp(&incoming.at)
        .then_with(|| existing.author.cmp(&incoming.author))
        .then_with(|| {
            sha256_bytes(existing.content.as_bytes())
                .cmp(&sha256_bytes(incoming.content.as_bytes()))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::Label;
    use crate::composite::Note;
    use crate::dep::{AcyclicDepKey, DepAddKey, NoCycleProof, ParentEdge};
    use crate::identity::{ActorId, NoteId, ReplicaId};
    use crate::orset::Dot;
    use crate::time::{Stamp, WriteStamp};
    use proptest::prelude::*;
    use uuid::Uuid;

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

    fn apply_dep_add_checked(state: &mut CanonicalState, key: DepKey, dot: Dot, stamp: Stamp) {
        let key = state
            .check_dep_add_key(key)
            .unwrap_or_else(|err| panic!("dep key invalid: {}", err.reason));
        state.apply_dep_add(key, dot, stamp);
    }

    fn apply_dep_add_unchecked(state: &mut CanonicalState, key: DepKey, dot: Dot, stamp: Stamp) {
        let key = AcyclicDepKey::from_dep_key(key, NoCycleProof::new())
            .unwrap_or_else(|err| panic!("dep key invalid: {}", err.reason));
        state.apply_dep_add(DepAddKey::Acyclic(key), dot, stamp);
    }

    fn make_bead(id: &BeadId, stamp: &Stamp) -> Bead {
        use crate::bead::{BeadCore, BeadFields};
        use crate::composite::{Claim, Workflow};
        use crate::crdt::Lww;
        use crate::domain::{BeadType, Priority};

        let core = BeadCore::new(id.clone(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("title".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        Bead::new(core, fields)
    }

    fn add_dep(state: &mut CanonicalState, key: DepKey, stamp: &Stamp, counter: u64) {
        let dot = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([9u8; 16])),
            counter,
        };
        apply_dep_add_checked(state, key, dot, stamp.clone());
    }

    fn remove_dep(state: &mut CanonicalState, key: &DepKey, stamp: &Stamp) {
        let ctx = state.dep_dvv(key);
        state.apply_dep_remove(key, &ctx, stamp.clone());
    }

    fn stamp_strategy() -> impl Strategy<Value = Stamp> {
        let actor = prop_oneof![Just("alice"), Just("bob"), Just("carol")];
        (0u64..10_000, 0u32..5, actor).prop_map(|(wall_ms, counter, actor)| {
            Stamp::new(WriteStamp::new(wall_ms, counter), actor_id(actor))
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

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
        use crate::bead::{BeadCore, BeadFields};
        use crate::composite::{Claim, Workflow};
        use crate::crdt::Lww;
        use crate::domain::{BeadType, Priority};

        let mut state = CanonicalState::new();
        let id = BeadId::parse("bd-abc").unwrap();
        let stamp = make_stamp(1000, 0, "alice");

        // Add tombstone first
        let tomb = Tombstone::new(id.clone(), stamp.clone(), None);
        state.delete(tomb);
        assert!(state.is_deleted(&id));
        assert!(state.get_live(&id).is_none());

        // Now insert bead - should remove tombstone
        let core = BeadCore::new(id.clone(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("test".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        let bead = Bead::new(core, fields);
        state.insert(bead).unwrap();

        assert!(!state.is_deleted(&id));
        assert!(state.get_live(&id).is_some());
        assert!(state.get_tombstone(&id).is_none());
    }

    #[test]
    fn collision_hides_other_lineage_labels_and_notes() {
        let id = bead_id("bd-collision");
        let stamp_a = make_stamp(1000, 0, "alice");
        let stamp_b = make_stamp(2000, 0, "bob");

        let mut state_a = CanonicalState::new();
        state_a.insert_live(make_bead(&id, &stamp_a));
        let mut state_b = CanonicalState::new();
        state_b.insert_live(make_bead(&id, &stamp_b));

        let label_a = Label::parse("alpha").expect("label");
        let label_b = Label::parse("beta").expect("label");
        let dot_a = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([1u8; 16])),
            counter: 1,
        };
        let dot_b = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([2u8; 16])),
            counter: 1,
        };

        state_a.apply_label_add(
            id.clone(),
            label_a.clone(),
            dot_a,
            stamp_a.clone(),
            stamp_a.clone(),
        );
        state_b.apply_label_add(
            id.clone(),
            label_b.clone(),
            dot_b,
            stamp_b.clone(),
            stamp_b.clone(),
        );

        let note_a = Note::new(
            NoteId::new("note-a").unwrap(),
            "note-a".to_string(),
            actor_id("alice"),
            WriteStamp::new(10, 0),
        );
        let note_b = Note::new(
            NoteId::new("note-b").unwrap(),
            "note-b".to_string(),
            actor_id("bob"),
            WriteStamp::new(20, 0),
        );
        state_a.insert_note(id.clone(), stamp_a.clone(), note_a);
        state_b.insert_note(id.clone(), stamp_b.clone(), note_b);

        let merged = CanonicalState::join(&state_a, &state_b)
            .unwrap_or_else(|e| panic!("join failed: {e:?}"));

        assert!(merged.has_lineage_tombstone(&id, &stamp_a));

        let labels = merged.labels_for(&id);
        assert!(labels.contains(label_b.as_str()));
        assert!(!labels.contains(label_a.as_str()));

        let notes = merged.notes_for(&id);
        assert!(notes.iter().any(|note| note.content == "note-b"));
        assert!(!notes.iter().any(|note| note.content == "note-a"));
    }

    #[test]
    fn invariant_delete_removes_live() {
        use crate::bead::{BeadCore, BeadFields};
        use crate::composite::{Claim, Workflow};
        use crate::crdt::Lww;
        use crate::domain::{BeadType, Priority};

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
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        let bead = Bead::new(core, fields);
        state.insert(bead).unwrap();
        assert!(state.get_live(&id).is_some());

        // Delete - should remove from live, add tombstone
        let tomb = Tombstone::new(id.clone(), stamp.clone(), Some("test delete".to_string()));
        state.delete(tomb);

        assert!(state.get_live(&id).is_none());
        assert!(state.get_tombstone(&id).is_some());
        assert!(state.is_deleted(&id));
    }

    #[test]
    fn resurrection_newer_bead_wins() {
        use crate::bead::{BeadCore, BeadFields};
        use crate::composite::{Claim, Workflow};
        use crate::crdt::Lww;
        use crate::domain::{BeadType, Priority};

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
            external_ref: Lww::new(None, new_stamp.clone()),
            source_repo: Lww::new(None, new_stamp.clone()),
            estimated_minutes: Lww::new(None, new_stamp.clone()),
            workflow: Lww::new(Workflow::default(), new_stamp.clone()),
            claim: Lww::new(Claim::default(), new_stamp.clone()),
        };
        state_b.insert(Bead::new(core, fields)).unwrap();

        // Merge: bead should win (resurrection)
        let merged = CanonicalState::join(&state_a, &state_b).unwrap();
        assert!(merged.get_live(&id).is_some(), "bead should be resurrected");
        assert!(
            merged.get_tombstone(&id).is_none(),
            "tombstone should be gone"
        );
    }

    #[test]
    fn deletion_wins_when_newer() {
        use crate::bead::{BeadCore, BeadFields};
        use crate::composite::{Claim, Workflow};
        use crate::crdt::Lww;
        use crate::domain::{BeadType, Priority};

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
        assert!(merged.get_live(&id).is_none(), "bead should be deleted");
        assert!(
            merged.get_tombstone(&id).is_some(),
            "tombstone should exist"
        );
    }

    #[test]
    fn note_store_join_resolves_collisions_deterministically() {
        let bead = bead_id("bd-note-join");
        let note_id = NoteId::new("note-join").unwrap();
        let lineage = make_stamp(1000, 0, "alice");
        let note_a = Note::new(
            note_id.clone(),
            "alpha".to_string(),
            actor_id("alice"),
            WriteStamp::new(10, 0),
        );
        let note_b = Note::new(
            note_id.clone(),
            "beta".to_string(),
            actor_id("bob"),
            WriteStamp::new(20, 0),
        );

        let mut store_a = NoteStore::new();
        store_a.insert(bead.clone(), lineage.clone(), note_a);
        let mut store_b = NoteStore::new();
        store_b.insert(bead.clone(), lineage.clone(), note_b.clone());

        let joined_ab = NoteStore::join(&store_a, &store_b);
        let joined_ba = NoteStore::join(&store_b, &store_a);

        let stored_ab = joined_ab
            .get(&bead, &lineage, &note_id)
            .expect("note stored");
        let stored_ba = joined_ba
            .get(&bead, &lineage, &note_id)
            .expect("note stored");

        assert_eq!(stored_ab, stored_ba);
        assert_eq!(stored_ab.content, "beta");
    }

    #[test]
    fn require_live_returns_bead_when_exists() {
        use crate::bead::{BeadCore, BeadFields};
        use crate::composite::{Claim, Workflow};
        use crate::crdt::Lww;
        use crate::domain::{BeadType, Priority};

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
    fn dependency_cycles_detects_simple_cycle() {
        let mut state = CanonicalState::new();
        let stamp = make_stamp(1000, 0, "alice");
        let a = bead_id("bd-aaa");
        let b = bead_id("bd-bbb");
        let c = bead_id("bd-ccc");
        for id in [&a, &b, &c] {
            state.insert(make_bead(id, &stamp)).unwrap();
        }

        let ab = DepKey::new(a.clone(), b.clone(), DepKind::Blocks)
            .unwrap_or_else(|e| panic!("dep key invalid: {}", e.reason));
        let bc = DepKey::new(b.clone(), c.clone(), DepKind::Blocks)
            .unwrap_or_else(|e| panic!("dep key invalid: {}", e.reason));
        let ca = DepKey::new(c.clone(), a.clone(), DepKind::Blocks)
            .unwrap_or_else(|e| panic!("dep key invalid: {}", e.reason));

        let replica = ReplicaId::from(Uuid::from_bytes([2u8; 16]));
        apply_dep_add_unchecked(
            &mut state,
            ab,
            Dot {
                replica,
                counter: 1,
            },
            stamp.clone(),
        );
        apply_dep_add_unchecked(
            &mut state,
            bc,
            Dot {
                replica,
                counter: 2,
            },
            stamp.clone(),
        );
        apply_dep_add_unchecked(
            &mut state,
            ca,
            Dot {
                replica,
                counter: 3,
            },
            stamp,
        );

        let cycles = state.dependency_cycles();
        assert_eq!(cycles.len(), 1);
        assert_eq!(cycles[0], vec![a.clone(), b.clone(), c.clone(), a.clone()]);
    }

    #[test]
    fn updated_stamp_includes_labels_and_notes() {
        let mut state = CanonicalState::new();
        let base = make_stamp(1000, 0, "alice");
        let id = bead_id("bd-aaa");
        state.insert(make_bead(&id, &base)).unwrap();

        let label = Label::parse("urgent").unwrap();
        let label_stamp = make_stamp(2000, 0, "bob");
        let dot = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([1u8; 16])),
            counter: 1,
        };
        state.apply_label_add(id.clone(), label, dot, label_stamp.clone(), base.clone());

        let updated = state.updated_stamp_for(&id).expect("updated stamp");
        assert_eq!(updated.at.wall_ms, label_stamp.at.wall_ms);

        let note_id = NoteId::new("note-1").unwrap();
        let note_author = ActorId::new("carol").unwrap();
        let note_stamp = WriteStamp::new(3000, 0);
        let note = Note::new(note_id, "hi".to_string(), note_author.clone(), note_stamp);
        state.insert_note(id.clone(), base.clone(), note);

        let updated = state.updated_stamp_for(&id).expect("updated stamp");
        assert_eq!(updated.at.wall_ms, 3000);
        assert_eq!(updated.by, note_author);
    }

    #[test]
    fn label_stamp_is_monotonic_for_out_of_order_ops() {
        let mut state = CanonicalState::new();
        let base = make_stamp(1000, 0, "alice");
        let id = bead_id("bd-label-stamp");
        state.insert(make_bead(&id, &base)).unwrap();

        let label = Label::parse("urgent").unwrap();
        let stamp_new = make_stamp(3000, 0, "carol");
        let stamp_old = make_stamp(2000, 0, "bob");
        let dot_a = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([1u8; 16])),
            counter: 1,
        };
        let dot_b = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([2u8; 16])),
            counter: 2,
        };

        state.apply_label_add(
            id.clone(),
            label.clone(),
            dot_a,
            stamp_new.clone(),
            base.clone(),
        );
        state.apply_label_add(id.clone(), label.clone(), dot_b, stamp_old, base.clone());

        let stamp = state.label_stamp(&id).expect("label stamp");
        assert_eq!(stamp, stamp_new);
    }

    #[test]
    fn label_stamp_updates_on_new_dot_without_membership_change() {
        let mut state = CanonicalState::new();
        let base = make_stamp(1000, 0, "alice");
        let id = bead_id("bd-label-dot");
        state.insert(make_bead(&id, &base)).unwrap();

        let label = Label::parse("inbox").unwrap();
        let stamp_a = make_stamp(2000, 0, "bob");
        let stamp_b = make_stamp(3000, 0, "carol");
        let dot_a = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([3u8; 16])),
            counter: 1,
        };
        let dot_b = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([4u8; 16])),
            counter: 2,
        };

        state.apply_label_add(id.clone(), label.clone(), dot_a, stamp_a, base.clone());
        state.apply_label_add(id.clone(), label, dot_b, stamp_b.clone(), base.clone());

        assert_eq!(state.labels_for(&id).len(), 1);
        let stamp = state.label_stamp(&id).expect("label stamp");
        assert_eq!(stamp, stamp_b);
    }

    #[test]
    fn dep_stamp_is_monotonic_for_out_of_order_ops() {
        let mut state = CanonicalState::new();
        let base = make_stamp(1000, 0, "alice");
        let from = bead_id("bd-dep-from");
        let to = bead_id("bd-dep-to");
        state.insert(make_bead(&from, &base)).unwrap();

        let key = DepKey::new(from.clone(), to, DepKind::Blocks).unwrap();
        let stamp_new = make_stamp(3000, 0, "carol");
        let stamp_old = make_stamp(2000, 0, "bob");

        let dot_a = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([5u8; 16])),
            counter: 1,
        };
        let dot_b = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([6u8; 16])),
            counter: 2,
        };

        apply_dep_add_checked(&mut state, key.clone(), dot_a, stamp_new.clone());
        apply_dep_add_checked(&mut state, key, dot_b, stamp_old);

        let stamp = state.dep_store().stamp().expect("dep stamp");
        assert_eq!(stamp, &stamp_new);
    }

    #[test]
    fn dep_stamp_updates_on_new_dot_without_membership_change() {
        let mut state = CanonicalState::new();
        let base = make_stamp(1000, 0, "alice");
        let from = bead_id("bd-dep-dot-from");
        let to = bead_id("bd-dep-dot-to");
        state.insert(make_bead(&from, &base)).unwrap();

        let key = DepKey::new(from.clone(), to, DepKind::Blocks).unwrap();
        let stamp_a = make_stamp(2000, 0, "bob");
        let stamp_b = make_stamp(3000, 0, "carol");

        let dot_a = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([7u8; 16])),
            counter: 1,
        };
        let dot_b = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([8u8; 16])),
            counter: 2,
        };

        apply_dep_add_checked(&mut state, key.clone(), dot_a, stamp_a);
        apply_dep_add_checked(&mut state, key, dot_b, stamp_b.clone());

        let stamp = state.dep_store().stamp().expect("dep stamp");
        assert_eq!(stamp, &stamp_b);
    }

    #[test]
    fn dependency_cycles_empty_for_acyclic_graph() {
        let mut state = CanonicalState::new();
        let stamp = make_stamp(1000, 0, "alice");
        let a = bead_id("bd-aaa");
        let b = bead_id("bd-bbb");
        let c = bead_id("bd-ccc");

        let ab = DepKey::new(a, b.clone(), DepKind::Blocks)
            .unwrap_or_else(|e| panic!("dep key invalid: {}", e.reason));
        let bc = DepKey::new(b, c, DepKind::Blocks)
            .unwrap_or_else(|e| panic!("dep key invalid: {}", e.reason));

        let replica = ReplicaId::from(Uuid::from_bytes([3u8; 16]));
        apply_dep_add_checked(
            &mut state,
            ab,
            Dot {
                replica,
                counter: 1,
            },
            stamp.clone(),
        );
        apply_dep_add_checked(
            &mut state,
            bc,
            Dot {
                replica,
                counter: 2,
            },
            stamp,
        );

        let cycles = state.dependency_cycles();
        assert!(cycles.is_empty());
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
        use crate::bead::{BeadCore, BeadFields};
        use crate::composite::{Claim, Workflow};
        use crate::crdt::Lww;
        use crate::domain::{BeadType, Priority};

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
        add_dep(&mut state, key, &stamp, 1);

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
        add_dep(&mut state, key.clone(), &stamp, 1);
        remove_dep(&mut state, &key, &stamp);

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
        add_dep(&mut state, key.clone(), &stamp1, 1);

        // Should be in indexes
        assert_eq!(state.dep_indexes().out_edges(&from).len(), 1);

        // Now delete it
        remove_dep(&mut state, &key, &stamp2);

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

        // Insert then remove edge
        let key = DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap();
        add_dep(&mut state, key.clone(), &stamp1, 1);
        remove_dep(&mut state, &key, &stamp1);

        // Should NOT be in indexes
        assert!(state.dep_indexes().out_edges(&from).is_empty());

        // Now restore it (active edge with newer stamp)
        add_dep(&mut state, key, &stamp2, 2);

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
        for id in [&from, &to1, &to2] {
            state.insert(make_bead(id, &stamp)).unwrap();
        }

        // Add two deps from the same source
        let key1 = DepKey::new(from.clone(), to1.clone(), DepKind::Blocks).unwrap();
        let key2 = ParentEdge::new(from.clone(), to2.clone())
            .unwrap()
            .to_dep_key();
        add_dep(&mut state, key1.clone(), &stamp, 1);
        add_dep(&mut state, key2.clone(), &stamp, 2);

        let deps = state.deps_from(&from);
        assert_eq!(deps.len(), 2);

        // Verify the edges are correct
        let to_ids: std::collections::HashSet<_> =
            deps.iter().map(|key| key.to().clone()).collect();
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
        for id in [&from1, &from2, &to] {
            state.insert(make_bead(id, &stamp)).unwrap();
        }

        // Add two deps to the same target
        let key1 = DepKey::new(from1.clone(), to.clone(), DepKind::Blocks).unwrap();
        let key2 = DepKey::new(from2.clone(), to.clone(), DepKind::Blocks).unwrap();
        add_dep(&mut state, key1.clone(), &stamp, 1);
        add_dep(&mut state, key2.clone(), &stamp, 2);

        let deps = state.deps_to(&to);
        assert_eq!(deps.len(), 2);

        // Verify the edges are correct
        let from_ids: std::collections::HashSet<_> =
            deps.iter().map(|key| key.from().clone()).collect();
        assert!(from_ids.contains(&from1));
        assert!(from_ids.contains(&from2));
    }

    #[test]
    fn parent_edges_to_keeps_children_visible_when_parent_tombstoned() {
        let mut state = CanonicalState::new();
        let stamp = make_stamp(1000, 0, "alice");
        let child = BeadId::parse("bd-child").unwrap();
        let parent = BeadId::parse("bd-parent").unwrap();
        state.insert(make_bead(&child, &stamp)).unwrap();
        state.insert(make_bead(&parent, &stamp)).unwrap();

        let parent_edge = ParentEdge::new(child.clone(), parent.clone())
            .expect("valid parent edge")
            .to_dep_key();
        add_dep(&mut state, parent_edge, &stamp, 1);

        state.delete(Tombstone::new(
            parent.clone(),
            make_stamp(2000, 0, "bob"),
            None,
        ));

        let edges = state.parent_edges_to(&parent);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].child(), &child);
        assert_eq!(edges[0].parent(), &parent);
    }

    #[test]
    fn dep_index_rebuild_from_deps() {
        let mut state = CanonicalState::new();
        let stamp = make_stamp(1000, 0, "alice");
        let from = BeadId::parse("bd-aaa").unwrap();
        let to = BeadId::parse("bd-bbb").unwrap();

        // Manually insert into dep store (simulating deserialization)
        let key = DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap();
        let dot = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([7u8; 16])),
            counter: 1,
        };
        state.dep_store.set.apply_add(dot, key.clone());
        state.dep_store.stamp = Some(stamp);

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
        add_dep(&mut state_a, key, &stamp, 1);

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
        add_dep(&mut state, key1.clone(), &stamp, 1);
        add_dep(&mut state, key2.clone(), &stamp, 2);

        // Should have two edges in indexes
        let out = state.dep_indexes().out_edges(&from);
        assert_eq!(out.len(), 2);

        let kinds: std::collections::HashSet<_> = out.iter().map(|(_, k)| *k).collect();
        assert!(kinds.contains(&DepKind::Blocks));
        assert!(kinds.contains(&DepKind::Related));
    }
}
