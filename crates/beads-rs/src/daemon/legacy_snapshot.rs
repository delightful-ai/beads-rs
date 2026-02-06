//! Legacy snapshot persistence for migration/import.
//!
//! These snapshots are full-state records written atomically via rename.
//! On disk they remain in the legacy `wal/` directory and `.wal` extension
//! for compatibility with older runtimes.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::remote::RemoteUrl;
use crate::core::{
    Bead, BeadId, BeadSlug, CanonicalState, DepKey, DepStore, Dot, Dvv,
    LEGACY_SNAPSHOT_FORMAT_VERSION, LabelStore, Limits, NoteStore, OrSet, OrSetValue, Stamp,
    Tombstone, TombstoneKey,
};

/// A legacy snapshot entry containing a full state snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacySnapshotEntry {
    /// Format version for future compatibility.
    pub version: u32,
    /// Wall clock time when written (for debugging).
    pub written_at_ms: u64,
    /// Full state snapshot.
    #[serde(with = "snapshot_state")]
    pub state: CanonicalState,
    /// Root slug for bead IDs.
    pub root_slug: Option<BeadSlug>,
    /// Monotonic sequence number.
    pub sequence: u64,
}

mod snapshot_state {
    use super::*;
    use crate::core::state::{LabelState, legacy_fallback_lineage, note_collision_cmp};
    use crate::core::wire_bead::{
        SnapshotCodec, SnapshotWireV1, WireDepEntryV1, WireDepStoreV1, WireFieldStamp,
        WireLineageStamp,
    };
    use crate::core::{Note, NoteId};
    use serde::de::Error as DeError;
    use serde::{Deserializer, Serializer};
    use serde_json::Value;
    use std::cmp::Ordering;
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Serialize, Deserialize)]
    struct SnapshotStateV2 {
        live: Vec<Bead>,
        tombstones: Vec<Tombstone>,
        #[serde(default)]
        deps: SnapshotDepStore,
        #[serde(default)]
        labels: SnapshotLabelStore,
        #[serde(default)]
        notes: SnapshotNoteStore,
    }

    #[derive(Clone, Debug, Serialize)]
    #[serde(transparent)]
    struct SnapshotDepStore(WireDepStoreV1);

    impl Default for SnapshotDepStore {
        fn default() -> Self {
            Self(WireDepStoreV1 {
                cc: Dvv::default(),
                entries: Vec::new(),
                stamp: None,
            })
        }
    }

    impl SnapshotDepStore {
        fn into_dep_store(self) -> DepStore {
            let mut map: BTreeMap<DepKey, BTreeSet<Dot>> = BTreeMap::new();
            for entry in self.0.entries {
                let dots: BTreeSet<Dot> = entry.dots.into_iter().collect();
                if !dots.is_empty() {
                    map.insert(entry.key, dots);
                }
            }
            let (set, normalization) = OrSet::normalize_for_import(map, self.0.cc);
            if normalization.changed() {
                tracing::warn!(
                    normalized_cc = normalization.normalized_cc,
                    pruned_dots = normalization.pruned_dots,
                    removed_empty_entries = normalization.removed_empty_entries,
                    resolved_collisions = normalization.resolved_collisions,
                    "normalized dep OR-Set from legacy snapshot"
                );
            }
            let stamp = self.0.stamp.map(stamp_from_wire_field_stamp);
            DepStore::from_parts(set, stamp)
        }
    }

    fn stamp_from_wire_field_stamp(stamp: WireFieldStamp) -> Stamp {
        let (wire, actor) = stamp;
        Stamp::new(crate::core::WriteStamp::from(wire), actor)
    }

    impl<'de> Deserialize<'de> for SnapshotDepStore {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            #[derive(Deserialize)]
            #[serde(untagged)]
            enum SnapshotDepStoreRepr {
                V2 {
                    #[serde(default)]
                    cc: Dvv,
                    #[serde(default)]
                    entries: Vec<WireDepEntryV1>,
                    #[serde(default)]
                    stamp: Option<WireFieldStamp>,
                },
                LegacyEntries(Vec<WireDepEntryV1>),
            }

            match SnapshotDepStoreRepr::deserialize(deserializer)? {
                SnapshotDepStoreRepr::V2 { cc, entries, stamp } => {
                    Ok(SnapshotDepStore(WireDepStoreV1 { cc, entries, stamp }))
                }
                SnapshotDepStoreRepr::LegacyEntries(entries) => {
                    Ok(SnapshotDepStore(WireDepStoreV1 {
                        cc: Dvv::default(),
                        entries,
                        stamp: None,
                    }))
                }
            }
        }
    }

    #[derive(Clone, Debug, Default)]
    struct SnapshotLabelStore {
        by_bead: LabelStore,
        legacy: BTreeMap<BeadId, LabelState>,
    }

    impl From<LabelStore> for SnapshotLabelStore {
        fn from(store: LabelStore) -> Self {
            Self {
                by_bead: store,
                legacy: BTreeMap::new(),
            }
        }
    }

    impl Serialize for SnapshotLabelStore {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.by_bead.serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for SnapshotLabelStore {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            #[derive(Deserialize)]
            struct SnapshotLineageLabelState {
                lineage: WireLineageStamp,
                state: LabelState,
            }

            #[derive(Deserialize)]
            struct SnapshotLabelStoreWire {
                #[serde(default)]
                by_bead: BTreeMap<BeadId, Vec<SnapshotLineageLabelState>>,
                #[serde(default)]
                legacy: BTreeMap<BeadId, LabelState>,
            }

            #[derive(Deserialize)]
            #[serde(untagged)]
            enum SnapshotLabelStoreRepr {
                Legacy(BTreeMap<BeadId, LabelState>),
                Wire(SnapshotLabelStoreWire),
            }

            match SnapshotLabelStoreRepr::deserialize(deserializer)? {
                SnapshotLabelStoreRepr::Legacy(map) => Ok(SnapshotLabelStore {
                    by_bead: LabelStore::new(),
                    legacy: map,
                }),
                SnapshotLabelStoreRepr::Wire(wire) => {
                    let mut store = LabelStore::new();
                    for (id, entries) in wire.by_bead {
                        for entry in entries {
                            let lineage = entry.lineage.stamp();
                            let state = store.state_mut(&id, &lineage);
                            *state = LabelState::join(state, &entry.state);
                        }
                    }
                    Ok(SnapshotLabelStore {
                        by_bead: store,
                        legacy: wire.legacy,
                    })
                }
            }
        }
    }

    impl SnapshotLabelStore {
        fn into_label_store(self, state: &CanonicalState) -> LabelStore {
            let mut store = self.by_bead;
            for (id, legacy_state) in self.legacy {
                let lineage = resolve_legacy_lineage(state, &id);
                let entry = store.state_mut(&id, &lineage);
                *entry = LabelState::join(entry, &legacy_state);
            }
            store
        }
    }

    #[derive(Clone, Debug, Default)]
    struct SnapshotNoteStore {
        by_bead: NoteStore,
        legacy: BTreeMap<BeadId, BTreeMap<NoteId, Note>>,
    }

    impl From<NoteStore> for SnapshotNoteStore {
        fn from(store: NoteStore) -> Self {
            Self {
                by_bead: store,
                legacy: BTreeMap::new(),
            }
        }
    }

    impl Serialize for SnapshotNoteStore {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.by_bead.serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for SnapshotNoteStore {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            #[derive(Deserialize)]
            struct SnapshotLineageNotes {
                lineage: WireLineageStamp,
                notes: BTreeMap<NoteId, Note>,
            }

            #[derive(Deserialize)]
            struct SnapshotNoteStoreWire {
                #[serde(default)]
                by_bead: BTreeMap<BeadId, Vec<SnapshotLineageNotes>>,
                #[serde(default)]
                legacy: BTreeMap<BeadId, BTreeMap<NoteId, Note>>,
            }

            #[derive(Deserialize)]
            #[serde(untagged)]
            enum SnapshotNoteStoreRepr {
                Legacy(BTreeMap<BeadId, BTreeMap<NoteId, Note>>),
                Wire(SnapshotNoteStoreWire),
            }

            match SnapshotNoteStoreRepr::deserialize(deserializer)? {
                SnapshotNoteStoreRepr::Legacy(map) => Ok(SnapshotNoteStore {
                    by_bead: NoteStore::new(),
                    legacy: map,
                }),
                SnapshotNoteStoreRepr::Wire(wire) => {
                    let mut store = NoteStore::new();
                    for (id, entries) in wire.by_bead {
                        for entry in entries {
                            let lineage = entry.lineage.stamp();
                            for (_note_id, note) in entry.notes {
                                if let Some(existing) =
                                    store.insert(id.clone(), lineage.clone(), note.clone())
                                    && note_collision_cmp(&existing, &note) == Ordering::Less
                                {
                                    store.replace(id.clone(), lineage.clone(), note);
                                }
                            }
                        }
                    }
                    Ok(SnapshotNoteStore {
                        by_bead: store,
                        legacy: wire.legacy,
                    })
                }
            }
        }
    }

    impl SnapshotNoteStore {
        fn into_note_store(self, state: &CanonicalState) -> NoteStore {
            let mut store = self.by_bead;
            for (id, legacy_notes) in self.legacy {
                let lineage = resolve_legacy_lineage(state, &id);
                for (_note_id, note) in legacy_notes {
                    if let Some(existing) = store.insert(id.clone(), lineage.clone(), note.clone())
                        && note_collision_cmp(&existing, &note) == Ordering::Less
                    {
                        store.replace(id.clone(), lineage.clone(), note);
                    }
                }
            }
            store
        }
    }

    fn resolve_legacy_lineage(state: &CanonicalState, bead_id: &BeadId) -> Stamp {
        if state.has_collision_tombstone(bead_id) {
            legacy_fallback_lineage()
        } else if let Some(bead) = state.get_live(bead_id) {
            bead.core.created().clone()
        } else {
            legacy_fallback_lineage()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::core::collections::Label;
        use crate::core::crdt::Lww;
        use crate::core::domain::{BeadType, Priority};
        use crate::core::{ActorId, BeadCore, BeadFields, Claim, ReplicaId, Workflow, WriteStamp};
        use uuid::Uuid;

        fn dep_key(from: &str, to: &str) -> DepKey {
            DepKey::new(
                BeadId::parse(from).unwrap(),
                BeadId::parse(to).unwrap(),
                crate::core::DepKind::Blocks,
            )
            .unwrap()
        }

        fn dot(seed: u8) -> Dot {
            Dot {
                replica: crate::core::ReplicaId::new(Uuid::from_bytes([seed; 16])),
                counter: 1,
            }
        }

        #[test]
        fn snapshot_dep_store_empty_dots_pruned() {
            let key = dep_key("bd-a", "bd-b");
            let store = SnapshotDepStore(WireDepStoreV1 {
                cc: Dvv::default(),
                entries: vec![WireDepEntryV1 {
                    key: key.clone(),
                    dots: Vec::new(),
                }],
                stamp: None,
            });

            let dep_store = store.into_dep_store();
            assert!(!dep_store.contains(&key));
            assert!(dep_store.is_empty());
        }

        #[test]
        fn snapshot_dep_store_mixed_dots_preserves_non_empty() {
            let empty_key = dep_key("bd-a", "bd-b");
            let filled_key = dep_key("bd-a", "bd-c");
            let store = SnapshotDepStore(WireDepStoreV1 {
                cc: Dvv::default(),
                entries: vec![
                    WireDepEntryV1 {
                        key: empty_key.clone(),
                        dots: Vec::new(),
                    },
                    WireDepEntryV1 {
                        key: filled_key.clone(),
                        dots: vec![dot(1)],
                    },
                ],
                stamp: None,
            });

            let dep_store = store.into_dep_store();
            assert!(!dep_store.contains(&empty_key));
            assert!(dep_store.contains(&filled_key));
            assert_eq!(dep_store.len(), 1);
        }

        #[test]
        fn snapshot_dep_store_serializes_transparently() {
            let key = dep_key("bd-a", "bd-b");
            let store = SnapshotDepStore(WireDepStoreV1 {
                cc: Dvv::default(),
                entries: vec![WireDepEntryV1 {
                    key,
                    dots: vec![dot(1)],
                }],
                stamp: None,
            });

            let value = serde_json::to_value(&store).unwrap();
            let obj = value.as_object().expect("deps should serialize as object");
            assert!(obj.contains_key("cc"));
            assert!(obj.contains_key("entries"));
            assert!(!obj.contains_key("stamp"));
        }

        fn actor_id(actor: &str) -> ActorId {
            ActorId::new(actor).unwrap_or_else(|e| panic!("invalid actor id {actor}: {e}"))
        }

        fn make_stamp(wall_ms: u64, counter: u32, actor: &str) -> Stamp {
            Stamp::new(WriteStamp::new(wall_ms, counter), actor_id(actor))
        }

        fn make_bead(id: &BeadId, stamp: &Stamp) -> Bead {
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

        #[test]
        fn snapshot_label_store_migrates_legacy_labels() {
            let bead_id = BeadId::parse("bd-legacy-label").unwrap();
            let lineage = make_stamp(10, 0, "alice");
            let mut state = CanonicalState::new();
            state.insert_live(make_bead(&bead_id, &lineage));

            let label = Label::parse("urgent").unwrap();
            let mut set = OrSet::new();
            set.apply_add(
                Dot {
                    replica: ReplicaId::new(Uuid::from_bytes([7u8; 16])),
                    counter: 1,
                },
                label.clone(),
            );
            let legacy_state = LabelState::from_parts(set, Some(lineage.clone()));
            let mut legacy = BTreeMap::new();
            legacy.insert(bead_id.clone(), legacy_state);

            let value = serde_json::to_value(&legacy).unwrap();
            let snapshot_labels: SnapshotLabelStore = serde_json::from_value(value).unwrap();
            let migrated = snapshot_labels.into_label_store(&state);
            let labels = migrated
                .state(&bead_id, &lineage)
                .expect("label state missing")
                .labels();
            assert!(labels.contains(label.as_str()));
        }

        #[test]
        fn snapshot_note_store_migrates_legacy_notes() {
            let bead_id = BeadId::parse("bd-legacy-note").unwrap();
            let lineage = make_stamp(20, 0, "alice");
            let mut state = CanonicalState::new();
            state.insert_live(make_bead(&bead_id, &lineage));

            let note = Note::new(
                NoteId::new("note-1").unwrap(),
                "hello".to_string(),
                actor_id("bob"),
                WriteStamp::new(30, 0),
            );
            let mut notes = BTreeMap::new();
            notes.insert(note.id.clone(), note.clone());
            let mut legacy = BTreeMap::new();
            legacy.insert(bead_id.clone(), notes);

            let value = serde_json::to_value(&legacy).unwrap();
            let snapshot_notes: SnapshotNoteStore = serde_json::from_value(value).unwrap();
            let migrated = snapshot_notes.into_note_store(&state);
            let migrated_notes = migrated.notes_for(&bead_id, &lineage);
            assert_eq!(migrated_notes.len(), 1);
            assert_eq!(migrated_notes[0], &note);
        }
    }

    #[derive(Deserialize)]
    struct LegacySnapshotStateVec {
        live: Vec<Bead>,
        tombstones: Vec<Tombstone>,
        deps: Vec<LegacySnapshotDep>,
        #[serde(default)]
        labels: SnapshotLabelStore,
        #[serde(default)]
        notes: SnapshotNoteStore,
    }

    #[derive(Deserialize)]
    struct LegacySnapshotDep {
        key: DepKey,
        #[serde(flatten)]
        _edge: BTreeMap<String, serde_json::Value>,
    }

    #[derive(Deserialize)]
    struct LegacySnapshotStateMap {
        live: BTreeMap<BeadId, Bead>,
        tombstones: BTreeMap<TombstoneKey, Tombstone>,
        deps: BTreeMap<DepKey, serde_json::Value>,
        #[serde(default)]
        labels: SnapshotLabelStore,
        #[serde(default)]
        notes: SnapshotNoteStore,
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum SnapshotStateRepr {
        V2(SnapshotStateV2),
        LegacyVecs(LegacySnapshotStateVec),
        LegacyMaps(LegacySnapshotStateMap),
    }

    pub fn serialize<S>(state: &CanonicalState, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let snapshot = SnapshotCodec::from_state(state);
        snapshot.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<CanonicalState, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let is_object = value.is_object();
        if !is_object {
            return Err(DeError::custom(
                "legacy snapshot state must be a JSON object",
            ));
        }

        let is_legacy = value.get("live").is_some();
        if !is_legacy {
            let snapshot: SnapshotWireV1 =
                serde_json::from_value(value).map_err(DeError::custom)?;
            return SnapshotCodec::into_state(snapshot).map_err(DeError::custom);
        }

        let (live, tombstones, deps, labels, notes) =
            match serde_json::from_value::<SnapshotStateRepr>(value).map_err(DeError::custom)? {
                SnapshotStateRepr::V2(snapshot) => (
                    snapshot.live,
                    snapshot.tombstones,
                    snapshot.deps.into_dep_store(),
                    snapshot.labels,
                    snapshot.notes,
                ),
                SnapshotStateRepr::LegacyVecs(snapshot) => (
                    snapshot.live,
                    snapshot.tombstones,
                    dep_store_from_legacy(snapshot.deps.into_iter().map(|dep| dep.key)),
                    snapshot.labels,
                    snapshot.notes,
                ),
                SnapshotStateRepr::LegacyMaps(snapshot) => (
                    snapshot.live.into_values().collect(),
                    snapshot.tombstones.into_values().collect(),
                    dep_store_from_legacy(snapshot.deps.into_keys()),
                    snapshot.labels,
                    snapshot.notes,
                ),
            };
        let mut state = CanonicalState::new();
        for bead in live {
            state.insert(bead).map_err(DeError::custom)?;
        }
        for tombstone in tombstones {
            state.insert_tombstone(tombstone);
        }
        let labels = labels.into_label_store(&state);
        let notes = notes.into_note_store(&state);
        state.set_label_store(labels);
        state.set_note_store(notes);
        state.set_dep_store(deps);
        Ok(state)
    }

    fn dep_store_from_legacy<I>(deps: I) -> DepStore
    where
        I: IntoIterator<Item = DepKey>,
    {
        let mut entries: BTreeMap<DepKey, BTreeSet<Dot>> = BTreeMap::new();
        for key in deps {
            let dot = legacy_dot_from_bytes(&key.collision_bytes());
            entries.insert(key, BTreeSet::from([dot]));
        }
        let (set, normalization) = OrSet::normalize_for_import(entries, Dvv::default());
        if normalization.changed() {
            tracing::warn!(
                normalized_cc = normalization.normalized_cc,
                pruned_dots = normalization.pruned_dots,
                removed_empty_entries = normalization.removed_empty_entries,
                resolved_collisions = normalization.resolved_collisions,
                "normalized legacy dep OR-Set during legacy snapshot decode"
            );
        }
        DepStore::from_parts(set, None)
    }

    fn legacy_dot_from_bytes(bytes: &[u8]) -> Dot {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let digest = hasher.finalize();

        let mut uuid_bytes = [0u8; 16];
        uuid_bytes.copy_from_slice(&digest[..16]);
        let mut counter_bytes = [0u8; 8];
        counter_bytes.copy_from_slice(&digest[16..24]);

        Dot {
            replica: crate::core::ReplicaId::from(uuid::Uuid::from_bytes(uuid_bytes)),
            counter: u64::from_le_bytes(counter_bytes),
        }
    }
}

impl LegacySnapshotEntry {
    /// Create a new legacy snapshot entry.
    pub fn new(
        state: CanonicalState,
        root_slug: Option<BeadSlug>,
        sequence: u64,
        wall_ms: u64,
    ) -> Self {
        LegacySnapshotEntry {
            version: LEGACY_SNAPSHOT_FORMAT_VERSION,
            written_at_ms: wall_ms,
            state,
            root_slug,
            sequence,
        }
    }
}

/// Legacy snapshot errors.
#[derive(Debug, Error)]
pub enum LegacySnapshotError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("legacy snapshot version mismatch: expected {expected}, got {got}")]
    VersionMismatch { expected: u32, got: u32 },

    #[error("legacy snapshot record too large: max {max_bytes} bytes, got {got_bytes} bytes")]
    TooLarge { max_bytes: usize, got_bytes: usize },
}

/// Legacy snapshot manager.
///
/// Stores per-remote snapshot files in a subdirectory of a persistent base dir.
pub struct LegacySnapshotStore {
    dir: PathBuf,
}

impl LegacySnapshotStore {
    /// Create a new legacy snapshot store.
    ///
    /// Creates the legacy snapshot directory if it doesn't exist.
    pub fn new(base_dir: &Path) -> Result<Self, LegacySnapshotError> {
        let dir = base_dir.join("wal");
        fs::create_dir_all(&dir)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o700));
        }

        Ok(LegacySnapshotStore { dir })
    }

    /// Best-effort migration from a legacy runtime snapshot directory.
    ///
    /// The legacy path is `<runtime_dir>/wal`. Any snapshot files found there are
    /// copied into the persistent snapshot dir. If both exist, the newer entry wins.
    pub fn migrate_from_runtime_dir(&self, runtime_dir: &Path) {
        let legacy_dir = runtime_dir.join("wal");
        if legacy_dir == self.dir || !legacy_dir.exists() {
            return;
        }

        let entries = match fs::read_dir(&legacy_dir) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(
                    "legacy snapshot migration: failed to read {:?}: {}",
                    legacy_dir,
                    e
                );
                return;
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_none_or(|e| e != "wal") {
                continue;
            }
            let file_name = match path.file_name() {
                Some(name) => name.to_os_string(),
                None => continue,
            };
            let dest = self.dir.join(&file_name);
            if dest == path {
                continue;
            }

            if !dest.exists() {
                if let Err(e) = copy_then_remove(&path, &dest) {
                    tracing::warn!(
                        "legacy snapshot migration: failed to move {:?}: {}",
                        path,
                        e
                    );
                }
                continue;
            }

            let src_entry = read_entry_at(&path);
            let dest_entry = read_entry_at(&dest);

            match (src_entry, dest_entry) {
                (Ok(src), Ok(dest_entry)) => {
                    if is_newer(&src, &dest_entry) {
                        if let Err(e) = copy_then_remove(&path, &dest) {
                            tracing::warn!(
                                "legacy snapshot migration: failed to update {:?}: {}",
                                dest,
                                e
                            );
                        }
                    } else if let Err(e) = fs::remove_file(&path) {
                        tracing::warn!(
                            "legacy snapshot migration: failed to remove {:?}: {}",
                            path,
                            e
                        );
                    }
                }
                (Ok(_), Err(_)) => {
                    if let Err(e) = copy_then_remove(&path, &dest) {
                        tracing::warn!(
                            "legacy snapshot migration: failed to update {:?}: {}",
                            dest,
                            e
                        );
                    }
                }
                (Err(e), Ok(_)) => {
                    tracing::warn!(
                        "legacy snapshot migration: keeping legacy snapshot {:?} (unreadable): {}",
                        path,
                        e
                    );
                }
                (Err(e1), Err(e2)) => {
                    tracing::warn!(
                        "legacy snapshot migration: keeping legacy snapshot {:?} (unreadable): {}, {}",
                        path,
                        e1,
                        e2
                    );
                }
            }
        }
    }

    /// Get the legacy snapshot file path for a remote.
    fn snapshot_path(&self, remote: &RemoteUrl) -> PathBuf {
        // Hash the remote URL to get a stable filename
        let mut hasher = Sha256::new();
        hasher.update(remote.0.as_bytes());
        let hash = hasher.finalize();
        let hash_hex = hex::encode(&hash[..8]); // First 16 hex chars
        self.dir.join(format!("{}.wal", hash_hex))
    }

    /// Get the temporary file path for atomic writes.
    fn snapshot_tmp_path(&self, remote: &RemoteUrl) -> PathBuf {
        let snapshot_path = self.snapshot_path(remote);
        snapshot_path.with_extension("wal.tmp")
    }

    /// Write state to a legacy snapshot atomically.
    ///
    /// Uses write-to-temp + fsync + rename for crash safety.
    pub fn write(
        &self,
        remote: &RemoteUrl,
        entry: &LegacySnapshotEntry,
    ) -> Result<(), LegacySnapshotError> {
        self.write_with_limits(remote, entry, &Limits::default())
    }

    /// Write state to a legacy snapshot atomically with limit enforcement.
    pub fn write_with_limits(
        &self,
        remote: &RemoteUrl,
        entry: &LegacySnapshotEntry,
        limits: &Limits,
    ) -> Result<(), LegacySnapshotError> {
        let tmp_path = self.snapshot_tmp_path(remote);
        let snapshot_path = self.snapshot_path(remote);

        // Serialize to JSON
        let data = serde_json::to_vec(entry)?;
        if data.len() > limits.policy().max_wal_record_bytes() {
            return Err(LegacySnapshotError::TooLarge {
                max_bytes: limits.policy().max_wal_record_bytes(),
                got_bytes: data.len(),
            });
        }

        // Write to temp file
        let mut file = File::create(&tmp_path)?;
        file.write_all(&data)?;
        file.sync_all()?; // fsync for durability

        // Atomic rename
        fs::rename(&tmp_path, &snapshot_path)?;

        // fsync the directory to ensure rename is durable
        #[cfg(unix)]
        {
            if let Ok(dir) = File::open(&self.dir) {
                let _ = dir.sync_all();
            }
        }

        Ok(())
    }

    /// Read state from a legacy snapshot if it exists.
    ///
    /// Returns None if no snapshot file exists.
    /// Returns error if file exists but is corrupted.
    pub fn read(
        &self,
        remote: &RemoteUrl,
    ) -> Result<Option<LegacySnapshotEntry>, LegacySnapshotError> {
        let snapshot_path = self.snapshot_path(remote);

        if !snapshot_path.exists() {
            return Ok(None);
        }

        let data = fs::read(&snapshot_path)?;
        let entry: LegacySnapshotEntry = serde_json::from_slice(&data)?;

        // Version check
        if entry.version != LEGACY_SNAPSHOT_FORMAT_VERSION {
            return Err(LegacySnapshotError::VersionMismatch {
                expected: LEGACY_SNAPSHOT_FORMAT_VERSION,
                got: entry.version,
            });
        }

        Ok(Some(entry))
    }

    /// Delete legacy snapshots for a remote.
    ///
    /// Called after successful remote sync.
    pub fn delete(&self, remote: &RemoteUrl) -> Result<(), LegacySnapshotError> {
        let snapshot_path = self.snapshot_path(remote);
        let tmp_path = self.snapshot_tmp_path(remote);

        // Remove both snapshots and any stale temp file
        let _ = fs::remove_file(&snapshot_path);
        let _ = fs::remove_file(&tmp_path);

        Ok(())
    }

    /// Check if a legacy snapshot exists for a remote.
    pub fn exists(&self, remote: &RemoteUrl) -> bool {
        self.snapshot_path(remote).exists()
    }

    /// Clean up any stale temp files (from crashes during write).
    ///
    /// Called on startup.
    pub fn cleanup_stale(&self) -> Result<(), LegacySnapshotError> {
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "tmp") {
                    let _ = fs::remove_file(&path);
                }
            }
        }
        Ok(())
    }
}

fn read_entry_at(path: &Path) -> Result<LegacySnapshotEntry, LegacySnapshotError> {
    let data = fs::read(path)?;
    let entry: LegacySnapshotEntry = serde_json::from_slice(&data)?;
    if entry.version != LEGACY_SNAPSHOT_FORMAT_VERSION {
        return Err(LegacySnapshotError::VersionMismatch {
            expected: LEGACY_SNAPSHOT_FORMAT_VERSION,
            got: entry.version,
        });
    }
    Ok(entry)
}

fn is_newer(a: &LegacySnapshotEntry, b: &LegacySnapshotEntry) -> bool {
    a.sequence > b.sequence || (a.sequence == b.sequence && a.written_at_ms > b.written_at_ms)
}

fn copy_then_remove(src: &Path, dest: &Path) -> Result<(), LegacySnapshotError> {
    fs::copy(src, dest)?;
    fs::remove_file(src)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        ActorId, BeadCore, BeadFields, BeadId, BeadType, Claim, DepKey, DepKind, Dot, Limits, Lww,
        Priority, ReplicaId, Stamp, Workflow, WriteStamp,
    };
    use serde::Serialize;
    use std::collections::BTreeMap;
    use tempfile::TempDir;
    use uuid::Uuid;

    fn test_remote() -> RemoteUrl {
        RemoteUrl("git@github.com:test/repo.git".into())
    }

    #[test]
    fn write_read_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let snapshot_store = LegacySnapshotStore::new(tmp.path()).unwrap();
        let remote = test_remote();

        let entry = LegacySnapshotEntry::new(
            CanonicalState::new(),
            Some(BeadSlug::parse("test-slug").unwrap()),
            42,
            1234567890,
        );

        snapshot_store.write(&remote, &entry).unwrap();

        let loaded = snapshot_store.read(&remote).unwrap().unwrap();
        assert_eq!(loaded.version, LEGACY_SNAPSHOT_FORMAT_VERSION);
        assert_eq!(
            loaded.root_slug,
            Some(BeadSlug::parse("test-slug").unwrap())
        );
        assert_eq!(loaded.sequence, 42);
        assert_eq!(loaded.written_at_ms, 1234567890);
    }

    #[test]
    fn write_rejects_oversize_entry() {
        let tmp = TempDir::new().unwrap();
        let snapshot_store = LegacySnapshotStore::new(tmp.path()).unwrap();
        let remote = test_remote();

        let entry = LegacySnapshotEntry::new(CanonicalState::new(), None, 1, 0);
        let limits = Limits {
            max_wal_record_bytes: 1,
            ..Limits::default()
        };

        let err = snapshot_store
            .write_with_limits(&remote, &entry, &limits)
            .unwrap_err();
        assert!(matches!(err, LegacySnapshotError::TooLarge { .. }));
    }

    fn make_bead(id: &str, stamp: &Stamp) -> Bead {
        let core = BeadCore::new(BeadId::parse(id).unwrap(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("test".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::new(2).unwrap(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::Open, stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        Bead::new(core, fields)
    }

    #[test]
    fn write_read_roundtrip_with_tombstones_and_deps() {
        let tmp = TempDir::new().unwrap();
        let snapshot_store = LegacySnapshotStore::new(tmp.path()).unwrap();
        let remote = test_remote();

        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(1234, 0), actor);

        let mut state = CanonicalState::new();
        state.insert(make_bead("bd-abc", &stamp)).unwrap();
        state.insert_tombstone(Tombstone::new(
            BeadId::parse("bd-del").unwrap(),
            stamp.clone(),
            None,
        ));
        let dep_key = DepKey::new(
            BeadId::parse("bd-abc").unwrap(),
            BeadId::parse("bd-def").unwrap(),
            DepKind::Blocks,
        )
        .unwrap();
        let dep_dot = Dot {
            replica: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            counter: 1,
        };
        let dep_key = state.check_dep_add_key(dep_key).unwrap();
        state.apply_dep_add(dep_key, dep_dot, stamp.clone());

        let entry = LegacySnapshotEntry::new(state, None, 7, 42);
        snapshot_store.write(&remote, &entry).unwrap();

        let loaded = snapshot_store.read(&remote).unwrap().unwrap();
        assert_eq!(loaded.state.live_count(), 1);
        assert_eq!(loaded.state.tombstone_count(), 1);
        assert_eq!(loaded.state.dep_count(), 1);
    }

    #[test]
    fn read_legacy_map_state_format() {
        #[derive(Serialize)]
        struct LegacySnapshotState {
            live: BTreeMap<BeadId, Bead>,
            tombstones: BTreeMap<TombstoneKey, Tombstone>,
            deps: BTreeMap<DepKey, serde_json::Value>,
        }

        #[derive(Serialize)]
        struct LegacySnapshotWireEntry {
            version: u32,
            written_at_ms: u64,
            state: LegacySnapshotState,
            root_slug: Option<String>,
            sequence: u64,
        }

        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor);
        let bead = make_bead("bd-abc", &stamp);

        let mut live = BTreeMap::new();
        live.insert(bead.core.id.clone(), bead);

        let legacy = LegacySnapshotWireEntry {
            version: LEGACY_SNAPSHOT_FORMAT_VERSION,
            written_at_ms: 1,
            state: LegacySnapshotState {
                live,
                tombstones: BTreeMap::new(),
                deps: BTreeMap::new(),
            },
            root_slug: None,
            sequence: 1,
        };

        let data = serde_json::to_vec(&legacy).unwrap();
        let loaded: LegacySnapshotEntry = serde_json::from_slice(&data).unwrap();
        assert_eq!(loaded.state.live_count(), 1);
        assert_eq!(loaded.state.tombstone_count(), 0);
        assert_eq!(loaded.state.dep_count(), 0);
    }

    #[test]
    fn read_legacy_dep_store_entries_format() {
        #[derive(Serialize)]
        struct LegacySnapshotDepEntry {
            key: DepKey,
            dots: Vec<Dot>,
        }

        #[derive(Serialize)]
        struct LegacySnapshotState {
            live: Vec<Bead>,
            tombstones: Vec<Tombstone>,
            deps: Vec<LegacySnapshotDepEntry>,
            #[serde(default)]
            labels: LabelStore,
            #[serde(default)]
            notes: NoteStore,
        }

        #[derive(Serialize)]
        struct LegacySnapshotWireEntry {
            version: u32,
            written_at_ms: u64,
            state: LegacySnapshotState,
            root_slug: Option<String>,
            sequence: u64,
        }

        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor);
        let bead = make_bead("bd-abc", &stamp);

        let dep_key = DepKey::new(
            BeadId::parse("bd-abc").unwrap(),
            BeadId::parse("bd-def").unwrap(),
            DepKind::Blocks,
        )
        .unwrap();
        let dep_dot = Dot {
            replica: ReplicaId::new(Uuid::from_bytes([2u8; 16])),
            counter: 7,
        };

        let legacy = LegacySnapshotWireEntry {
            version: LEGACY_SNAPSHOT_FORMAT_VERSION,
            written_at_ms: 1,
            state: LegacySnapshotState {
                live: vec![bead],
                tombstones: Vec::new(),
                deps: vec![LegacySnapshotDepEntry {
                    key: dep_key.clone(),
                    dots: vec![dep_dot],
                }],
                labels: LabelStore::default(),
                notes: NoteStore::default(),
            },
            root_slug: None,
            sequence: 1,
        };

        let data = serde_json::to_vec(&legacy).unwrap();
        let loaded: LegacySnapshotEntry = serde_json::from_slice(&data).unwrap();
        assert_eq!(loaded.state.live_count(), 1);
        assert_eq!(loaded.state.dep_count(), 1);
        assert!(loaded.state.dep_contains(&dep_key));
    }

    #[test]
    fn read_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let snapshot_store = LegacySnapshotStore::new(tmp.path()).unwrap();
        let remote = test_remote();

        assert!(snapshot_store.read(&remote).unwrap().is_none());
    }

    #[test]
    fn delete_removes_file() {
        let tmp = TempDir::new().unwrap();
        let snapshot_store = LegacySnapshotStore::new(tmp.path()).unwrap();
        let remote = test_remote();

        let entry = LegacySnapshotEntry::new(CanonicalState::new(), None, 1, 0);
        snapshot_store.write(&remote, &entry).unwrap();
        assert!(snapshot_store.exists(&remote));

        snapshot_store.delete(&remote).unwrap();
        assert!(!snapshot_store.exists(&remote));
    }

    #[test]
    fn cleanup_stale_removes_tmp() {
        let tmp = TempDir::new().unwrap();
        let snapshot_store = LegacySnapshotStore::new(tmp.path()).unwrap();

        // Create a stale .tmp file
        let stale = snapshot_store.dir.join("stale.wal.tmp");
        fs::write(&stale, b"garbage").unwrap();
        assert!(stale.exists());

        snapshot_store.cleanup_stale().unwrap();
        assert!(!stale.exists());
    }

    #[test]
    fn different_remotes_different_files() {
        let tmp = TempDir::new().unwrap();
        let snapshot_store = LegacySnapshotStore::new(tmp.path()).unwrap();

        let remote1 = RemoteUrl("git@github.com:user/repo1.git".into());
        let remote2 = RemoteUrl("git@github.com:user/repo2.git".into());

        let entry1 = LegacySnapshotEntry::new(
            CanonicalState::new(),
            Some(BeadSlug::parse("slug1").unwrap()),
            1,
            0,
        );
        let entry2 = LegacySnapshotEntry::new(
            CanonicalState::new(),
            Some(BeadSlug::parse("slug2").unwrap()),
            2,
            0,
        );

        snapshot_store.write(&remote1, &entry1).unwrap();
        snapshot_store.write(&remote2, &entry2).unwrap();

        let loaded1 = snapshot_store.read(&remote1).unwrap().unwrap();
        let loaded2 = snapshot_store.read(&remote2).unwrap().unwrap();

        assert_eq!(loaded1.root_slug, Some(BeadSlug::parse("slug1").unwrap()));
        assert_eq!(loaded2.root_slug, Some(BeadSlug::parse("slug2").unwrap()));
    }

    #[test]
    fn migrate_from_runtime_dir_moves_snapshot() {
        let legacy_base = TempDir::new().unwrap();
        let new_base = TempDir::new().unwrap();
        let legacy = LegacySnapshotStore::new(legacy_base.path()).unwrap();
        let current = LegacySnapshotStore::new(new_base.path()).unwrap();
        let remote = test_remote();

        let entry = LegacySnapshotEntry::new(CanonicalState::new(), None, 1, 123);
        legacy.write(&remote, &entry).unwrap();
        assert!(legacy.exists(&remote));

        current.migrate_from_runtime_dir(legacy_base.path());
        assert!(current.exists(&remote));
        assert!(!legacy.exists(&remote));
    }

    #[test]
    fn migrate_prefers_newer_sequence() {
        let legacy_base = TempDir::new().unwrap();
        let new_base = TempDir::new().unwrap();
        let legacy = LegacySnapshotStore::new(legacy_base.path()).unwrap();
        let current = LegacySnapshotStore::new(new_base.path()).unwrap();
        let remote = test_remote();

        let older = LegacySnapshotEntry::new(CanonicalState::new(), None, 1, 100);
        let newer = LegacySnapshotEntry::new(CanonicalState::new(), None, 2, 200);

        current.write(&remote, &older).unwrap();
        legacy.write(&remote, &newer).unwrap();

        current.migrate_from_runtime_dir(legacy_base.path());
        let loaded = current.read(&remote).unwrap().unwrap();
        assert_eq!(loaded.sequence, 2);
    }
}
