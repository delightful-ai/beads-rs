use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::composite::Note;
use crate::crdt::Crdt;
use crate::event::sha256_bytes;
use crate::identity::{BeadId, NoteId};
use crate::time::Stamp;
use crate::wire_bead::WireLineageStamp;

/// Canonical note store keyed by bead id + lineage stamp.
#[derive(Clone, Debug, Default)]
pub struct NoteStore {
    pub(crate) by_bead: BTreeMap<BeadId, BTreeMap<Stamp, BTreeMap<NoteId, Note>>>,
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

    pub(crate) fn take_lineage_notes(
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
        Crdt::join(a, b)
    }
}

impl Crdt for NoteStore {
    fn join(&self, other: &Self) -> Self {
        let mut merged = NoteStore::new();
        for (id, lineages) in self.by_bead.iter().chain(other.by_bead.iter()) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{ActorId, BeadId};
    use crate::time::WriteStamp;

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
}
