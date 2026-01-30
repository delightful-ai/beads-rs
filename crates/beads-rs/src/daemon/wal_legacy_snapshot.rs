//! Write-Ahead Log for mutation durability.
//!
//! Provides crash-safe persistence without flooding git history with commits.
//! WAL entries are state snapshots (not operation replay) written atomically
//! via rename. Cleared after successful remote sync.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::remote::RemoteUrl;
use crate::core::{
    Bead, BeadId, BeadSlug, CanonicalState, DepKey, DepStore, Dot, Dvv, LabelStore, Limits,
    NoteStore, OrSet, OrSetValue, Stamp, Tombstone, TombstoneKey,
};

/// WAL format version.
const WAL_VERSION: u32 = 1;

/// A WAL entry containing a full state snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Format version for future compatibility.
    pub version: u32,
    /// Wall clock time when written (for debugging).
    pub written_at_ms: u64,
    /// Full state snapshot.
    #[serde(with = "wal_state")]
    pub state: CanonicalState,
    /// Root slug for bead IDs.
    pub root_slug: Option<BeadSlug>,
    /// Monotonic sequence number.
    pub sequence: u64,
}

mod wal_state {
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
    struct WalStateV2 {
        live: Vec<Bead>,
        tombstones: Vec<Tombstone>,
        #[serde(default)]
        deps: WalDepStore,
        #[serde(default)]
        labels: WalLabelStore,
        #[serde(default)]
        notes: WalNoteStore,
    }

    #[derive(Clone, Debug, Serialize)]
    #[serde(transparent)]
    struct WalDepStore(WireDepStoreV1);

    impl Default for WalDepStore {
        fn default() -> Self {
            Self(WireDepStoreV1 {
                cc: Dvv::default(),
                entries: Vec::new(),
                stamp: None,
            })
        }
    }

    impl WalDepStore {
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
                    "normalized dep OR-Set from WAL snapshot"
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

    impl<'de> Deserialize<'de> for WalDepStore {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            #[derive(Deserialize)]
            #[serde(untagged)]
            enum WalDepStoreRepr {
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

            match WalDepStoreRepr::deserialize(deserializer)? {
                WalDepStoreRepr::V2 { cc, entries, stamp } => {
                    Ok(WalDepStore(WireDepStoreV1 { cc, entries, stamp }))
                }
                WalDepStoreRepr::LegacyEntries(entries) => Ok(WalDepStore(WireDepStoreV1 {
                    cc: Dvv::default(),
                    entries,
                    stamp: None,
                })),
            }
        }
    }

    #[derive(Clone, Debug, Default)]
    struct WalLabelStore {
        by_bead: LabelStore,
        legacy: BTreeMap<BeadId, LabelState>,
    }

    impl From<LabelStore> for WalLabelStore {
        fn from(store: LabelStore) -> Self {
            Self {
                by_bead: store,
                legacy: BTreeMap::new(),
            }
        }
    }

    impl Serialize for WalLabelStore {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.by_bead.serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for WalLabelStore {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            #[derive(Deserialize)]
            struct WalLineageLabelState {
                lineage: WireLineageStamp,
                state: LabelState,
            }

            #[derive(Deserialize)]
            struct WalLabelStoreWire {
                #[serde(default)]
                by_bead: BTreeMap<BeadId, Vec<WalLineageLabelState>>,
                #[serde(default)]
                legacy: BTreeMap<BeadId, LabelState>,
            }

            #[derive(Deserialize)]
            #[serde(untagged)]
            enum WalLabelStoreRepr {
                Legacy(BTreeMap<BeadId, LabelState>),
                Wire(WalLabelStoreWire),
            }

            match WalLabelStoreRepr::deserialize(deserializer)? {
                WalLabelStoreRepr::Legacy(map) => Ok(WalLabelStore {
                    by_bead: LabelStore::new(),
                    legacy: map,
                }),
                WalLabelStoreRepr::Wire(wire) => {
                    let mut store = LabelStore::new();
                    for (id, entries) in wire.by_bead {
                        for entry in entries {
                            let lineage = entry.lineage.stamp();
                            let state = store.state_mut(&id, &lineage);
                            *state = LabelState::join(state, &entry.state);
                        }
                    }
                    Ok(WalLabelStore {
                        by_bead: store,
                        legacy: wire.legacy,
                    })
                }
            }
        }
    }

    impl WalLabelStore {
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
    struct WalNoteStore {
        by_bead: NoteStore,
        legacy: BTreeMap<BeadId, BTreeMap<NoteId, Note>>,
    }

    impl From<NoteStore> for WalNoteStore {
        fn from(store: NoteStore) -> Self {
            Self {
                by_bead: store,
                legacy: BTreeMap::new(),
            }
        }
    }

    impl Serialize for WalNoteStore {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.by_bead.serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for WalNoteStore {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            #[derive(Deserialize)]
            struct WalLineageNotes {
                lineage: WireLineageStamp,
                notes: BTreeMap<NoteId, Note>,
            }

            #[derive(Deserialize)]
            struct WalNoteStoreWire {
                #[serde(default)]
                by_bead: BTreeMap<BeadId, Vec<WalLineageNotes>>,
                #[serde(default)]
                legacy: BTreeMap<BeadId, BTreeMap<NoteId, Note>>,
            }

            #[derive(Deserialize)]
            #[serde(untagged)]
            enum WalNoteStoreRepr {
                Legacy(BTreeMap<BeadId, BTreeMap<NoteId, Note>>),
                Wire(WalNoteStoreWire),
            }

            match WalNoteStoreRepr::deserialize(deserializer)? {
                WalNoteStoreRepr::Legacy(map) => Ok(WalNoteStore {
                    by_bead: NoteStore::new(),
                    legacy: map,
                }),
                WalNoteStoreRepr::Wire(wire) => {
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
                    Ok(WalNoteStore {
                        by_bead: store,
                        legacy: wire.legacy,
                    })
                }
            }
        }
    }

    impl WalNoteStore {
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
        fn wal_dep_store_empty_dots_pruned() {
            let key = dep_key("bd-a", "bd-b");
            let store = WalDepStore(WireDepStoreV1 {
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
        fn wal_dep_store_mixed_dots_preserves_non_empty() {
            let empty_key = dep_key("bd-a", "bd-b");
            let filled_key = dep_key("bd-a", "bd-c");
            let store = WalDepStore(WireDepStoreV1 {
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
        fn wal_dep_store_serializes_transparently() {
            let key = dep_key("bd-a", "bd-b");
            let store = WalDepStore(WireDepStoreV1 {
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
        fn wal_label_store_migrates_legacy_labels() {
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
            let wal_labels: WalLabelStore = serde_json::from_value(value).unwrap();
            let migrated = wal_labels.into_label_store(&state);
            let labels = migrated
                .state(&bead_id, &lineage)
                .expect("label state missing")
                .labels();
            assert!(labels.contains(label.as_str()));
        }

        #[test]
        fn wal_note_store_migrates_legacy_notes() {
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
            let wal_notes: WalNoteStore = serde_json::from_value(value).unwrap();
            let migrated = wal_notes.into_note_store(&state);
            let migrated_notes = migrated.notes_for(&bead_id, &lineage);
            assert_eq!(migrated_notes.len(), 1);
            assert_eq!(migrated_notes[0], &note);
        }
    }

    #[derive(Deserialize)]
    struct LegacyWalStateVec {
        live: Vec<Bead>,
        tombstones: Vec<Tombstone>,
        deps: Vec<LegacyWalDep>,
        #[serde(default)]
        labels: WalLabelStore,
        #[serde(default)]
        notes: WalNoteStore,
    }

    #[derive(Deserialize)]
    struct LegacyWalDep {
        key: DepKey,
        #[serde(flatten)]
        _edge: BTreeMap<String, serde_json::Value>,
    }

    #[derive(Deserialize)]
    struct LegacyWalStateMap {
        live: BTreeMap<BeadId, Bead>,
        tombstones: BTreeMap<TombstoneKey, Tombstone>,
        deps: BTreeMap<DepKey, serde_json::Value>,
        #[serde(default)]
        labels: WalLabelStore,
        #[serde(default)]
        notes: WalNoteStore,
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum WalStateRepr {
        V2(WalStateV2),
        LegacyVecs(LegacyWalStateVec),
        LegacyMaps(LegacyWalStateMap),
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
            return Err(DeError::custom("wal state must be a JSON object"));
        }

        let is_legacy = value.get("live").is_some();
        if !is_legacy {
            let snapshot: SnapshotWireV1 =
                serde_json::from_value(value).map_err(DeError::custom)?;
            return SnapshotCodec::into_state(snapshot).map_err(DeError::custom);
        }

        let (live, tombstones, deps, labels, notes) =
            match serde_json::from_value::<WalStateRepr>(value).map_err(DeError::custom)? {
                WalStateRepr::V2(snapshot) => (
                    snapshot.live,
                    snapshot.tombstones,
                    snapshot.deps.into_dep_store(),
                    snapshot.labels,
                    snapshot.notes,
                ),
                WalStateRepr::LegacyVecs(snapshot) => (
                    snapshot.live,
                    snapshot.tombstones,
                    dep_store_from_legacy(snapshot.deps.into_iter().map(|dep| dep.key)),
                    snapshot.labels,
                    snapshot.notes,
                ),
                WalStateRepr::LegacyMaps(snapshot) => (
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
                "normalized legacy dep OR-Set during WAL decode"
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

impl WalEntry {
    /// Create a new WAL entry.
    pub fn new(
        state: CanonicalState,
        root_slug: Option<BeadSlug>,
        sequence: u64,
        wall_ms: u64,
    ) -> Self {
        WalEntry {
            version: WAL_VERSION,
            written_at_ms: wall_ms,
            state,
            root_slug,
            sequence,
        }
    }
}

/// WAL errors.
#[derive(Debug, Error)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("WAL version mismatch: expected {expected}, got {got}")]
    VersionMismatch { expected: u32, got: u32 },

    #[error("WAL record too large: max {max_bytes} bytes, got {got_bytes} bytes")]
    TooLarge { max_bytes: usize, got_bytes: usize },
}

/// Write-Ahead Log manager.
///
/// Stores per-remote WAL files in a subdirectory of a persistent base dir.
pub struct Wal {
    dir: PathBuf,
}

impl Wal {
    /// Create a new WAL manager.
    ///
    /// Creates the WAL directory if it doesn't exist.
    pub fn new(base_dir: &Path) -> Result<Self, WalError> {
        let dir = base_dir.join("wal");
        fs::create_dir_all(&dir)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o700));
        }

        Ok(Wal { dir })
    }

    /// Best-effort migration from a legacy runtime WAL directory.
    ///
    /// The legacy path is `<runtime_dir>/wal`. Any WAL files found there are
    /// copied into the persistent WAL dir. If both exist, the newer entry wins.
    pub fn migrate_from_runtime_dir(&self, runtime_dir: &Path) {
        let legacy_dir = runtime_dir.join("wal");
        if legacy_dir == self.dir || !legacy_dir.exists() {
            return;
        }

        let entries = match fs::read_dir(&legacy_dir) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("wal migration: failed to read {:?}: {}", legacy_dir, e);
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
                    tracing::warn!("wal migration: failed to move {:?}: {}", path, e);
                }
                continue;
            }

            let src_entry = read_entry_at(&path);
            let dest_entry = read_entry_at(&dest);

            match (src_entry, dest_entry) {
                (Ok(src), Ok(dest_entry)) => {
                    if is_newer(&src, &dest_entry) {
                        if let Err(e) = copy_then_remove(&path, &dest) {
                            tracing::warn!("wal migration: failed to update {:?}: {}", dest, e);
                        }
                    } else if let Err(e) = fs::remove_file(&path) {
                        tracing::warn!("wal migration: failed to remove {:?}: {}", path, e);
                    }
                }
                (Ok(_), Err(_)) => {
                    if let Err(e) = copy_then_remove(&path, &dest) {
                        tracing::warn!("wal migration: failed to update {:?}: {}", dest, e);
                    }
                }
                (Err(e), Ok(_)) => {
                    tracing::warn!(
                        "wal migration: keeping legacy WAL {:?} (unreadable): {}",
                        path,
                        e
                    );
                }
                (Err(e1), Err(e2)) => {
                    tracing::warn!(
                        "wal migration: keeping legacy WAL {:?} (unreadable): {}, {}",
                        path,
                        e1,
                        e2
                    );
                }
            }
        }
    }

    /// Get the WAL file path for a remote.
    fn wal_path(&self, remote: &RemoteUrl) -> PathBuf {
        // Hash the remote URL to get a stable filename
        let mut hasher = Sha256::new();
        hasher.update(remote.0.as_bytes());
        let hash = hasher.finalize();
        let hash_hex = hex::encode(&hash[..8]); // First 16 hex chars
        self.dir.join(format!("{}.wal", hash_hex))
    }

    /// Get the temporary file path for atomic writes.
    fn tmp_path(&self, remote: &RemoteUrl) -> PathBuf {
        let wal_path = self.wal_path(remote);
        wal_path.with_extension("wal.tmp")
    }

    /// Write state to WAL atomically.
    ///
    /// Uses write-to-temp + fsync + rename for crash safety.
    pub fn write(&self, remote: &RemoteUrl, entry: &WalEntry) -> Result<(), WalError> {
        self.write_with_limits(remote, entry, &Limits::default())
    }

    /// Write state to WAL atomically with limit enforcement.
    pub fn write_with_limits(
        &self,
        remote: &RemoteUrl,
        entry: &WalEntry,
        limits: &Limits,
    ) -> Result<(), WalError> {
        let tmp_path = self.tmp_path(remote);
        let wal_path = self.wal_path(remote);

        // Serialize to JSON
        let data = serde_json::to_vec(entry)?;
        if data.len() > limits.policy().max_wal_record_bytes() {
            return Err(WalError::TooLarge {
                max_bytes: limits.policy().max_wal_record_bytes(),
                got_bytes: data.len(),
            });
        }

        // Write to temp file
        let mut file = File::create(&tmp_path)?;
        file.write_all(&data)?;
        file.sync_all()?; // fsync for durability

        // Atomic rename
        fs::rename(&tmp_path, &wal_path)?;

        // fsync the directory to ensure rename is durable
        #[cfg(unix)]
        {
            if let Ok(dir) = File::open(&self.dir) {
                let _ = dir.sync_all();
            }
        }

        Ok(())
    }

    /// Read state from WAL if it exists.
    ///
    /// Returns None if no WAL file exists.
    /// Returns error if file exists but is corrupted.
    pub fn read(&self, remote: &RemoteUrl) -> Result<Option<WalEntry>, WalError> {
        let wal_path = self.wal_path(remote);

        if !wal_path.exists() {
            return Ok(None);
        }

        let data = fs::read(&wal_path)?;
        let entry: WalEntry = serde_json::from_slice(&data)?;

        // Version check
        if entry.version != WAL_VERSION {
            return Err(WalError::VersionMismatch {
                expected: WAL_VERSION,
                got: entry.version,
            });
        }

        Ok(Some(entry))
    }

    /// Delete WAL for a remote.
    ///
    /// Called after successful remote sync.
    pub fn delete(&self, remote: &RemoteUrl) -> Result<(), WalError> {
        let wal_path = self.wal_path(remote);
        let tmp_path = self.tmp_path(remote);

        // Remove both WAL and any stale temp file
        let _ = fs::remove_file(&wal_path);
        let _ = fs::remove_file(&tmp_path);

        Ok(())
    }

    /// Check if a WAL exists for a remote.
    pub fn exists(&self, remote: &RemoteUrl) -> bool {
        self.wal_path(remote).exists()
    }

    /// Clean up any stale temp files (from crashes during write).
    ///
    /// Called on startup.
    pub fn cleanup_stale(&self) -> Result<(), WalError> {
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

fn read_entry_at(path: &Path) -> Result<WalEntry, WalError> {
    let data = fs::read(path)?;
    let entry: WalEntry = serde_json::from_slice(&data)?;
    if entry.version != WAL_VERSION {
        return Err(WalError::VersionMismatch {
            expected: WAL_VERSION,
            got: entry.version,
        });
    }
    Ok(entry)
}

fn is_newer(a: &WalEntry, b: &WalEntry) -> bool {
    a.sequence > b.sequence || (a.sequence == b.sequence && a.written_at_ms > b.written_at_ms)
}

fn copy_then_remove(src: &Path, dest: &Path) -> Result<(), WalError> {
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
        let wal = Wal::new(tmp.path()).unwrap();
        let remote = test_remote();

        let entry = WalEntry::new(
            CanonicalState::new(),
            Some(BeadSlug::parse("test-slug").unwrap()),
            42,
            1234567890,
        );

        wal.write(&remote, &entry).unwrap();

        let loaded = wal.read(&remote).unwrap().unwrap();
        assert_eq!(loaded.version, WAL_VERSION);
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
        let wal = Wal::new(tmp.path()).unwrap();
        let remote = test_remote();

        let entry = WalEntry::new(CanonicalState::new(), None, 1, 0);
        let limits = Limits {
            max_wal_record_bytes: 1,
            ..Limits::default()
        };

        let err = wal.write_with_limits(&remote, &entry, &limits).unwrap_err();
        assert!(matches!(err, WalError::TooLarge { .. }));
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
        let wal = Wal::new(tmp.path()).unwrap();
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

        let entry = WalEntry::new(state, None, 7, 42);
        wal.write(&remote, &entry).unwrap();

        let loaded = wal.read(&remote).unwrap().unwrap();
        assert_eq!(loaded.state.live_count(), 1);
        assert_eq!(loaded.state.tombstone_count(), 1);
        assert_eq!(loaded.state.dep_count(), 1);
    }

    #[test]
    fn read_legacy_map_state_format() {
        #[derive(Serialize)]
        struct LegacyWalState {
            live: BTreeMap<BeadId, Bead>,
            tombstones: BTreeMap<TombstoneKey, Tombstone>,
            deps: BTreeMap<DepKey, serde_json::Value>,
        }

        #[derive(Serialize)]
        struct LegacyWalEntry {
            version: u32,
            written_at_ms: u64,
            state: LegacyWalState,
            root_slug: Option<String>,
            sequence: u64,
        }

        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor);
        let bead = make_bead("bd-abc", &stamp);

        let mut live = BTreeMap::new();
        live.insert(bead.core.id.clone(), bead);

        let legacy = LegacyWalEntry {
            version: WAL_VERSION,
            written_at_ms: 1,
            state: LegacyWalState {
                live,
                tombstones: BTreeMap::new(),
                deps: BTreeMap::new(),
            },
            root_slug: None,
            sequence: 1,
        };

        let data = serde_json::to_vec(&legacy).unwrap();
        let loaded: WalEntry = serde_json::from_slice(&data).unwrap();
        assert_eq!(loaded.state.live_count(), 1);
        assert_eq!(loaded.state.tombstone_count(), 0);
        assert_eq!(loaded.state.dep_count(), 0);
    }

    #[test]
    fn read_legacy_dep_store_entries_format() {
        #[derive(Serialize)]
        struct LegacyWalDepEntry {
            key: DepKey,
            dots: Vec<Dot>,
        }

        #[derive(Serialize)]
        struct LegacyWalState {
            live: Vec<Bead>,
            tombstones: Vec<Tombstone>,
            deps: Vec<LegacyWalDepEntry>,
            #[serde(default)]
            labels: LabelStore,
            #[serde(default)]
            notes: NoteStore,
        }

        #[derive(Serialize)]
        struct LegacyWalEntry {
            version: u32,
            written_at_ms: u64,
            state: LegacyWalState,
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

        let legacy = LegacyWalEntry {
            version: WAL_VERSION,
            written_at_ms: 1,
            state: LegacyWalState {
                live: vec![bead],
                tombstones: Vec::new(),
                deps: vec![LegacyWalDepEntry {
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
        let loaded: WalEntry = serde_json::from_slice(&data).unwrap();
        assert_eq!(loaded.state.live_count(), 1);
        assert_eq!(loaded.state.dep_count(), 1);
        assert!(loaded.state.dep_contains(&dep_key));
    }

    #[test]
    fn read_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::new(tmp.path()).unwrap();
        let remote = test_remote();

        assert!(wal.read(&remote).unwrap().is_none());
    }

    #[test]
    fn delete_removes_file() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::new(tmp.path()).unwrap();
        let remote = test_remote();

        let entry = WalEntry::new(CanonicalState::new(), None, 1, 0);
        wal.write(&remote, &entry).unwrap();
        assert!(wal.exists(&remote));

        wal.delete(&remote).unwrap();
        assert!(!wal.exists(&remote));
    }

    #[test]
    fn cleanup_stale_removes_tmp() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::new(tmp.path()).unwrap();

        // Create a stale .tmp file
        let stale = wal.dir.join("stale.wal.tmp");
        fs::write(&stale, b"garbage").unwrap();
        assert!(stale.exists());

        wal.cleanup_stale().unwrap();
        assert!(!stale.exists());
    }

    #[test]
    fn different_remotes_different_files() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::new(tmp.path()).unwrap();

        let remote1 = RemoteUrl("git@github.com:user/repo1.git".into());
        let remote2 = RemoteUrl("git@github.com:user/repo2.git".into());

        let entry1 = WalEntry::new(
            CanonicalState::new(),
            Some(BeadSlug::parse("slug1").unwrap()),
            1,
            0,
        );
        let entry2 = WalEntry::new(
            CanonicalState::new(),
            Some(BeadSlug::parse("slug2").unwrap()),
            2,
            0,
        );

        wal.write(&remote1, &entry1).unwrap();
        wal.write(&remote2, &entry2).unwrap();

        let loaded1 = wal.read(&remote1).unwrap().unwrap();
        let loaded2 = wal.read(&remote2).unwrap().unwrap();

        assert_eq!(loaded1.root_slug, Some(BeadSlug::parse("slug1").unwrap()));
        assert_eq!(loaded2.root_slug, Some(BeadSlug::parse("slug2").unwrap()));
    }

    #[test]
    fn migrate_from_runtime_dir_moves_wal() {
        let legacy_base = TempDir::new().unwrap();
        let new_base = TempDir::new().unwrap();
        let legacy = Wal::new(legacy_base.path()).unwrap();
        let current = Wal::new(new_base.path()).unwrap();
        let remote = test_remote();

        let entry = WalEntry::new(CanonicalState::new(), None, 1, 123);
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
        let legacy = Wal::new(legacy_base.path()).unwrap();
        let current = Wal::new(new_base.path()).unwrap();
        let remote = test_remote();

        let older = WalEntry::new(CanonicalState::new(), None, 1, 100);
        let newer = WalEntry::new(CanonicalState::new(), None, 2, 200);

        current.write(&remote, &older).unwrap();
        legacy.write(&remote, &newer).unwrap();

        current.migrate_from_runtime_dir(legacy_base.path());
        let loaded = current.read(&remote).unwrap().unwrap();
        assert_eq!(loaded.sequence, 2);
    }
}
