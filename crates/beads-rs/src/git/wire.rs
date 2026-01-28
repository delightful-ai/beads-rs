//! Wire format serialization for git storage.
//!
//! Per SPEC §5.2.1, uses sparse _v representation:
//! - Each bead has top-level `_at` and `_by` (bead-level stamp)
//! - `_v` object maps field names to stamps only when they differ from bead-level
//! - If all fields share bead-level stamp, `_v` is omitted

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::error::WireError;
use crate::core::{
    ActorId, Bead, BeadCore, BeadFields, BeadId, BeadSlug, BeadType, BeadView, CanonicalState,
    Claim, Closure, ContentHash, DepKey, DepStore, Dot, Dvv, Label, LabelStore, Lww, Note, NoteId,
    NoteStore, OrSet, Priority, Stamp, Tombstone, WallClock, Workflow, WriteStamp, sha256_bytes,
};

use crate::core::state::LabelState;

// =============================================================================
// Wire format types (intermediate representation for JSON)
// =============================================================================

/// Write stamp as array: [wall_ms, counter]
type WireStamp = (u64, u32);

/// Field stamp as array: [(wall_ms, counter), actor]
type WireFieldStamp = (WireStamp, String);

#[derive(Serialize, Deserialize)]
struct WireLabelState {
    entries: BTreeMap<Label, BTreeSet<Dot>>,
    cc: Dvv,
}

/// Bead wire format with sparse _v
#[derive(Serialize, Deserialize)]
struct WireBead {
    // Core (immutable)
    id: String,
    created_at: WireStamp,
    created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_on_branch: Option<String>,

    // Fields (mutable)
    title: String,
    description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    design: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    acceptance_criteria: Option<String>,
    priority: u8,
    #[serde(rename = "type")]
    bead_type: String,
    labels: WireLabelState,
    #[serde(skip_serializing_if = "Option::is_none")]
    external_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_repo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_minutes: Option<u32>,

    // Workflow state
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    closed_at: Option<WireStamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    closed_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    closed_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    closed_on_branch: Option<String>,

    // Claim
    #[serde(skip_serializing_if = "Option::is_none")]
    assignee: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    assignee_at: Option<WireStamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    assignee_expires: Option<u64>,

    // Version metadata (sparse)
    #[serde(rename = "_at")]
    at: WireStamp,
    #[serde(rename = "_by")]
    by: String,
    #[serde(rename = "_v", skip_serializing_if = "Option::is_none")]
    v: Option<BTreeMap<String, (WireStamp, String)>>,
}

#[derive(Serialize, Deserialize)]
struct WireNote {
    id: String,
    content: String,
    author: String,
    at: WireStamp,
}

#[derive(Serialize, Deserialize)]
struct WireNoteEntry {
    bead_id: String,
    note: WireNote,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lineage_created_at: Option<WireStamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lineage_created_by: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct WireTombstone {
    id: String,
    deleted_at: WireStamp,
    deleted_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lineage_created_at: Option<WireStamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lineage_created_by: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct WireDepStore {
    cc: Dvv,
    entries: Vec<WireDepEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stamp: Option<WireFieldStamp>,
}

#[derive(Serialize, Deserialize)]
struct WireDepEntry {
    key: DepKey,
    dots: Vec<Dot>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ParsedWireBead {
    bead: Bead,
    label_state: LabelState,
}

#[derive(Serialize, Deserialize)]
struct WireMeta {
    format_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    root_slug: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_write_stamp: Option<WireStamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state_sha256: Option<ContentHash>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tombstones_sha256: Option<ContentHash>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deps_sha256: Option<ContentHash>,
    #[serde(skip_serializing_if = "Option::is_none")]
    notes_sha256: Option<ContentHash>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreChecksums {
    pub state: ContentHash,
    pub tombstones: ContentHash,
    pub deps: ContentHash,
    pub notes: Option<ContentHash>,
}

impl StoreChecksums {
    pub fn from_bytes(
        state_bytes: &[u8],
        tombs_bytes: &[u8],
        deps_bytes: &[u8],
        notes_bytes: Option<&[u8]>,
    ) -> Self {
        Self {
            state: ContentHash::from_bytes(sha256_bytes(state_bytes).0),
            tombstones: ContentHash::from_bytes(sha256_bytes(tombs_bytes).0),
            deps: ContentHash::from_bytes(sha256_bytes(deps_bytes).0),
            notes: notes_bytes.map(|bytes| ContentHash::from_bytes(sha256_bytes(bytes).0)),
        }
    }
}

// =============================================================================
// Serialization
// =============================================================================

/// Serialize state to state.jsonl bytes.
///
/// Sorted by bead ID, one JSON object per line.
pub fn serialize_state(state: &CanonicalState) -> Result<Vec<u8>, WireError> {
    let mut lines = Vec::new();

    // Sort by ID
    let mut ids: Vec<_> = state.iter_live().map(|(id, _)| id.clone()).collect();
    ids.sort();

    for id in ids {
        let Some(view) = state.bead_view(&id) else {
            continue;
        };
        let lineage_state = state.label_store().state(&id, view.bead.core.created());
        let label_state = if state.has_collision_tombstone(&id) {
            lineage_state.cloned()
        } else {
            match (lineage_state, state.label_store().legacy_state(&id)) {
                (Some(lineage), Some(legacy)) => Some(LabelState::join(lineage, legacy)),
                (Some(lineage), None) => Some(lineage.clone()),
                (None, Some(legacy)) => Some(legacy.clone()),
                (None, None) => None,
            }
        };
        let wire = bead_to_wire(&view, label_state.as_ref());
        let json = serde_json::to_string(&wire)?;
        lines.push(json);
    }

    let mut output = lines.join("\n");
    if !output.is_empty() {
        output.push('\n');
    }
    Ok(output.into_bytes())
}

/// Serialize tombstones to tombstones.jsonl bytes.
pub fn serialize_tombstones(state: &CanonicalState) -> Result<Vec<u8>, WireError> {
    let mut lines = Vec::new();

    let mut tombs: Vec<_> = state.iter_tombstones().collect();
    tombs.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (_, tomb) in tombs {
        let (lineage_created_at, lineage_created_by) = tomb
            .lineage
            .as_ref()
            .map(|s| (Some(stamp_to_wire(&s.at)), Some(s.by.as_str().to_string())))
            .unwrap_or((None, None));
        let wire = WireTombstone {
            id: tomb.id.as_str().to_string(),
            deleted_at: stamp_to_wire(&tomb.deleted.at),
            deleted_by: tomb.deleted.by.as_str().to_string(),
            reason: tomb.reason.clone(),
            lineage_created_at,
            lineage_created_by,
        };
        let json = serde_json::to_string(&wire)?;
        lines.push(json);
    }

    let mut output = lines.join("\n");
    if !output.is_empty() {
        output.push('\n');
    }
    Ok(output.into_bytes())
}

/// Serialize deps to deps.jsonl bytes.
pub fn serialize_deps(state: &CanonicalState) -> Result<Vec<u8>, WireError> {
    let dep_store = state.dep_store();
    let mut entries = Vec::new();

    for key in dep_store.values() {
        let mut dots: Vec<Dot> = dep_store
            .dots_for(key)
            .map(|dots| dots.iter().copied().collect())
            .unwrap_or_default();
        dots.sort();
        entries.push(WireDepEntry {
            key: key.clone(),
            dots,
        });
    }

    entries.sort_by(|a, b| a.key.cmp(&b.key));

    let wire = WireDepStore {
        cc: dep_store.cc().clone(),
        entries,
        stamp: dep_store.stamp().map(stamp_to_field),
    };

    let json = serde_json::to_string(&wire)?;
    let mut output = json;
    if !output.is_empty() {
        output.push('\n');
    }
    Ok(output.into_bytes())
}

/// Serialize notes to notes.jsonl bytes.
pub fn serialize_notes(state: &CanonicalState) -> Result<Vec<u8>, WireError> {
    let mut entries: Vec<(BeadId, Option<Stamp>, Note)> = Vec::new();

    for (bead_id, lineage, notes) in state.note_store().iter_lineages() {
        for note in notes.values() {
            entries.push((bead_id.clone(), Some(lineage.clone()), note.clone()));
        }
    }

    for (bead_id, notes) in state.note_store().iter_legacy() {
        if state.has_collision_tombstone(bead_id) {
            continue;
        }
        for note in notes.values() {
            entries.push((bead_id.clone(), None, note.clone()));
        }
    }

    entries.sort_by(|(a_id, a_lineage, a_note), (b_id, b_lineage, b_note)| {
        a_id.cmp(b_id)
            .then_with(|| a_lineage.cmp(b_lineage))
            .then_with(|| a_note.at.cmp(&b_note.at))
            .then_with(|| a_note.id.cmp(&b_note.id))
    });

    let mut lines = Vec::new();
    for (bead_id, lineage, note) in entries {
        let (lineage_created_at, lineage_created_by) = lineage
            .as_ref()
            .map(|stamp| {
                (
                    Some(stamp_to_wire(&stamp.at)),
                    Some(stamp.by.as_str().to_string()),
                )
            })
            .unwrap_or((None, None));
        let wire = WireNoteEntry {
            bead_id: bead_id.as_str().to_string(),
            note: WireNote {
                id: note.id.as_str().to_string(),
                content: note.content,
                author: note.author.as_str().to_string(),
                at: (note.at.wall_ms, note.at.counter),
            },
            lineage_created_at,
            lineage_created_by,
        };
        let json = serde_json::to_string(&wire)?;
        lines.push(json);
    }

    let mut output = lines.join("\n");
    if !output.is_empty() {
        output.push('\n');
    }
    Ok(output.into_bytes())
}

/// Serialize meta.json bytes.
pub fn serialize_meta(
    root_slug: Option<&str>,
    last_write_stamp: Option<&WriteStamp>,
    checksums: &StoreChecksums,
) -> Result<Vec<u8>, WireError> {
    let notes = checksums
        .notes
        .ok_or_else(|| WireError::InvalidValue("meta.json missing checksum fields".into()))?;
    let meta = WireMeta {
        format_version: 1,
        root_slug: root_slug.map(|s| s.to_string()),
        last_write_stamp: last_write_stamp.map(stamp_to_wire),
        state_sha256: Some(checksums.state),
        tombstones_sha256: Some(checksums.tombstones),
        deps_sha256: Some(checksums.deps),
        notes_sha256: Some(notes),
    };
    let json = serde_json::to_string_pretty(&meta)?;
    Ok(json.into_bytes())
}

// =============================================================================
// Deserialization
// =============================================================================

/// Parse state.jsonl bytes into beads.
pub fn parse_state(bytes: &[u8]) -> Result<Vec<Bead>, WireError> {
    Ok(parse_state_full(bytes)?
        .into_iter()
        .map(|parsed| parsed.bead)
        .collect())
}

fn parse_state_full(bytes: &[u8]) -> Result<Vec<ParsedWireBead>, WireError> {
    let content = parse_utf8(bytes)?;
    let mut beads = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let wire: WireBead = serde_json::from_str(line)?;
        let parsed = wire_to_parts(wire)?;
        beads.push(parsed);
    }

    Ok(beads)
}

/// Parse tombstones.jsonl bytes.
pub fn parse_tombstones(bytes: &[u8]) -> Result<Vec<Tombstone>, WireError> {
    let content = parse_utf8(bytes)?;
    let mut tombs = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let wire: WireTombstone = serde_json::from_str(line)?;
        let deleted = Stamp::new(
            wire_to_stamp(wire.deleted_at),
            ActorId::new(wire.deleted_by).map_err(|e| WireError::InvalidValue(e.to_string()))?,
        );
        let lineage = match (wire.lineage_created_at, wire.lineage_created_by) {
            (Some(at), Some(by)) => Some(Stamp::new(
                wire_to_stamp(at),
                ActorId::new(by).map_err(|e| WireError::InvalidValue(e.to_string()))?,
            )),
            (None, None) => None,
            _ => {
                return Err(WireError::InvalidValue(
                    "tombstone lineage requires both created_at and created_by".to_string(),
                ));
            }
        };
        let tomb = if let Some(lineage) = lineage {
            Tombstone::new_collision(
                BeadId::parse(&wire.id).map_err(|e| WireError::InvalidValue(e.to_string()))?,
                deleted,
                lineage,
                wire.reason,
            )
        } else {
            Tombstone::new(
                BeadId::parse(&wire.id).map_err(|e| WireError::InvalidValue(e.to_string()))?,
                deleted,
                wire.reason,
            )
        };
        tombs.push(tomb);
    }

    Ok(tombs)
}

/// Parse deps.jsonl bytes.
fn parse_deps(bytes: &[u8]) -> Result<DepStore, WireError> {
    let content = parse_utf8(bytes)?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Ok(DepStore::new());
    }

    let wire: WireDepStore = serde_json::from_str(trimmed)?;
    wire_dep_store_to_state(wire)
}

/// Parse notes.jsonl bytes.
pub fn parse_notes(bytes: &[u8]) -> Result<Vec<(BeadId, Option<Stamp>, Note)>, WireError> {
    let content = parse_utf8(bytes)?;
    let mut notes = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let wire: WireNoteEntry = serde_json::from_str(line)?;
        let bead_id =
            BeadId::parse(&wire.bead_id).map_err(|e| WireError::InvalidValue(e.to_string()))?;
        let note_id =
            NoteId::new(wire.note.id).map_err(|e| WireError::InvalidValue(e.to_string()))?;
        let author =
            ActorId::new(wire.note.author).map_err(|e| WireError::InvalidValue(e.to_string()))?;
        let note = Note::new(
            note_id,
            wire.note.content,
            author,
            wire_to_stamp(wire.note.at),
        );
        let lineage = match (wire.lineage_created_at, wire.lineage_created_by) {
            (None, None) => None,
            (Some(at), Some(by)) => Some(Stamp::new(
                wire_to_stamp(at),
                ActorId::new(by).map_err(|e| WireError::InvalidValue(e.to_string()))?,
            )),
            _ => {
                return Err(WireError::InvalidValue(
                    "lineage_created_at and lineage_created_by must be set together".into(),
                ));
            }
        };
        notes.push((bead_id, lineage, note));
    }

    Ok(notes)
}

/// Parse git JSONL files into a canonical state.
pub fn parse_legacy_state(
    state_bytes: &[u8],
    tombstones_bytes: &[u8],
    deps_bytes: &[u8],
    notes_bytes: &[u8],
) -> Result<CanonicalState, WireError> {
    let beads = parse_state_full(state_bytes)?;
    let tombstones = parse_tombstones(tombstones_bytes)?;
    let dep_store = parse_deps(deps_bytes)?;
    let notes = parse_notes(notes_bytes)?;

    let mut state = CanonicalState::new();
    let mut label_store = LabelStore::new();
    let mut note_store = NoteStore::new();
    for parsed in beads {
        let bead = parsed.bead;
        let bead_id = bead.core.id.clone();
        let lineage = bead.core.created().clone();
        state.insert_live(bead);
        label_store.insert_state(bead_id.clone(), lineage, parsed.label_state);
    }
    for (bead_id, lineage, note) in notes {
        note_store.insert(bead_id, lineage, note);
    }
    state.set_label_store(label_store);
    state.set_note_store(note_store);
    for tombstone in tombstones {
        state.insert_tombstone(tombstone);
    }
    state.set_dep_store(dep_store);
    state.rebuild_dep_indexes();
    Ok(state)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreMeta {
    Legacy,
    V1 {
        root_slug: Option<BeadSlug>,
        last_write_stamp: Option<WriteStamp>,
        checksums: StoreChecksums,
    },
}

impl StoreMeta {
    pub fn root_slug(&self) -> Option<&BeadSlug> {
        match self {
            StoreMeta::Legacy => None,
            StoreMeta::V1 { root_slug, .. } => root_slug.as_ref(),
        }
    }

    pub fn last_write_stamp(&self) -> Option<&WriteStamp> {
        match self {
            StoreMeta::Legacy => None,
            StoreMeta::V1 {
                last_write_stamp, ..
            } => last_write_stamp.as_ref(),
        }
    }

    pub fn checksums(&self) -> Option<&StoreChecksums> {
        match self {
            StoreMeta::Legacy => None,
            StoreMeta::V1 { checksums, .. } => Some(checksums),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SupportedStoreMeta {
    meta: StoreMeta,
}

impl SupportedStoreMeta {
    pub fn legacy() -> Self {
        Self {
            meta: StoreMeta::Legacy,
        }
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, WireError> {
        let content = parse_utf8(bytes)?;
        let meta: WireMeta = serde_json::from_str(content)?;
        if meta.format_version != 1 {
            return Err(WireError::InvalidValue(format!(
                "unsupported meta format_version {}",
                meta.format_version
            )));
        }
        let checksums = match (
            meta.state_sha256,
            meta.tombstones_sha256,
            meta.deps_sha256,
            meta.notes_sha256,
        ) {
            (Some(state), Some(tombstones), Some(deps), Some(notes)) => StoreChecksums {
                state,
                tombstones,
                deps,
                notes: Some(notes),
            },
            _ => {
                return Err(WireError::InvalidValue(
                    "meta.json missing checksum fields".into(),
                ));
            }
        };
        let root_slug = match meta.root_slug {
            Some(raw) => {
                Some(BeadSlug::parse(&raw).map_err(|e| WireError::InvalidValue(e.to_string()))?)
            }
            None => None,
        };
        Ok(Self {
            meta: StoreMeta::V1 {
                root_slug,
                last_write_stamp: meta.last_write_stamp.map(wire_to_stamp),
                checksums,
            },
        })
    }

    pub fn meta(&self) -> &StoreMeta {
        &self.meta
    }

    pub fn root_slug(&self) -> Option<&BeadSlug> {
        self.meta.root_slug()
    }

    pub fn last_write_stamp(&self) -> Option<&WriteStamp> {
        self.meta.last_write_stamp()
    }

    pub fn checksums(&self) -> Option<&StoreChecksums> {
        self.meta.checksums()
    }

    pub fn is_legacy(&self) -> bool {
        matches!(self.meta, StoreMeta::Legacy)
    }
}

/// Parse meta.json bytes.
pub fn parse_meta(bytes: &[u8]) -> Result<SupportedStoreMeta, WireError> {
    SupportedStoreMeta::parse(bytes)
}

pub fn verify_store_checksums(
    expected: &StoreChecksums,
    state_bytes: &[u8],
    tombs_bytes: &[u8],
    deps_bytes: &[u8],
    notes_bytes: &[u8],
) -> Result<(), WireError> {
    let actual =
        StoreChecksums::from_bytes(state_bytes, tombs_bytes, deps_bytes, Some(notes_bytes));
    if actual.state != expected.state {
        return Err(WireError::ChecksumMismatch {
            blob: "state.jsonl",
            expected: expected.state,
            actual: actual.state,
        });
    }
    if actual.tombstones != expected.tombstones {
        return Err(WireError::ChecksumMismatch {
            blob: "tombstones.jsonl",
            expected: expected.tombstones,
            actual: actual.tombstones,
        });
    }
    if actual.deps != expected.deps {
        return Err(WireError::ChecksumMismatch {
            blob: "deps.jsonl",
            expected: expected.deps,
            actual: actual.deps,
        });
    }
    if let Some(expected_notes) = expected.notes {
        let actual_notes = actual.notes.expect("notes checksum missing");
        if actual_notes != expected_notes {
            return Err(WireError::ChecksumMismatch {
                blob: "notes.jsonl",
                expected: expected_notes,
                actual: actual_notes,
            });
        }
    }
    Ok(())
}

fn parse_utf8(bytes: &[u8]) -> Result<&str, WireError> {
    std::str::from_utf8(bytes).map_err(|e| WireError::InvalidValue(format!("utf-8 error: {e}")))
}

// =============================================================================
// Conversion helpers
// =============================================================================

fn stamp_to_wire(stamp: &WriteStamp) -> WireStamp {
    (stamp.wall_ms, stamp.counter)
}

fn wire_to_stamp(wire: WireStamp) -> WriteStamp {
    WriteStamp::new(wire.0, wire.1)
}

fn stamp_to_field(stamp: &Stamp) -> WireFieldStamp {
    (stamp_to_wire(&stamp.at), stamp.by.as_str().to_string())
}

fn wire_field_to_stamp(field: WireFieldStamp) -> Result<Stamp, WireError> {
    let (at, by) = field;
    Ok(Stamp::new(
        wire_to_stamp(at),
        ActorId::new(by).map_err(|e| WireError::InvalidValue(e.to_string()))?,
    ))
}

fn label_state_to_wire(state: Option<&LabelState>) -> WireLabelState {
    let mut entries: BTreeMap<Label, BTreeSet<Dot>> = BTreeMap::new();
    let cc = state.map(|state| state.cc().clone()).unwrap_or_default();

    if let Some(state) = state {
        for label in state.values() {
            if let Some(dots) = state.dots_for(label) {
                entries.insert(label.clone(), dots.clone());
            }
        }
    }

    WireLabelState { entries, cc }
}

fn wire_dep_store_to_state(wire: WireDepStore) -> Result<DepStore, WireError> {
    let mut entries: BTreeMap<DepKey, BTreeSet<Dot>> = BTreeMap::new();
    for entry in wire.entries {
        let mut dots = BTreeSet::new();
        for dot in entry.dots {
            dots.insert(dot);
        }
        entries.insert(entry.key, dots);
    }

    let stamp = match wire.stamp {
        Some(stamp) => Some(wire_field_to_stamp(stamp)?),
        None => None,
    };
    let set = OrSet::try_from_parts(entries, wire.cc)
        .map_err(|err| WireError::InvalidValue(format!("dep orset invalid: {err}")))?;
    Ok(DepStore::from_parts(set, stamp))
}

fn str_to_bead_type(s: &str) -> Result<BeadType, WireError> {
    match s {
        "bug" => Ok(BeadType::Bug),
        "feature" => Ok(BeadType::Feature),
        "task" => Ok(BeadType::Task),
        "epic" => Ok(BeadType::Epic),
        "chore" => Ok(BeadType::Chore),
        _ => Err(WireError::InvalidValue(format!("unknown bead type: {}", s))),
    }
}

/// Convert Bead view to wire format with sparse _v.
fn bead_to_wire(view: &BeadView, label_state: Option<&LabelState>) -> WireBead {
    let bead = &view.bead;
    // Find bead-level stamp (max of all field stamps)
    let bead_stamp = view.updated_stamp().clone();

    // Build sparse _v: only include fields with different stamps
    let mut v_map: BTreeMap<String, (WireStamp, String)> = BTreeMap::new();

    macro_rules! check_field {
        ($field:expr, $name:expr) => {
            if $field.stamp != bead_stamp {
                v_map.insert(
                    $name.to_string(),
                    (
                        stamp_to_wire(&$field.stamp.at),
                        $field.stamp.by.as_str().to_string(),
                    ),
                );
            }
        };
    }

    check_field!(bead.fields.title, "title");
    check_field!(bead.fields.description, "description");
    check_field!(bead.fields.design, "design");
    check_field!(bead.fields.acceptance_criteria, "acceptance_criteria");
    check_field!(bead.fields.priority, "priority");
    check_field!(bead.fields.bead_type, "type");
    if let Some(label_stamp) = view.label_stamp.as_ref()
        && label_stamp != &bead_stamp
    {
        v_map.insert(
            "labels".to_string(),
            (
                stamp_to_wire(&label_stamp.at),
                label_stamp.by.as_str().to_string(),
            ),
        );
    }
    check_field!(bead.fields.external_ref, "external_ref");
    check_field!(bead.fields.source_repo, "source_repo");
    check_field!(bead.fields.estimated_minutes, "estimated_minutes");
    check_field!(bead.fields.workflow, "workflow");
    check_field!(bead.fields.claim, "claim");

    // Extract closure info from workflow
    let (closed_at, closed_by, closed_reason, closed_on_branch) =
        if let Workflow::Closed(ref closure) = bead.fields.workflow.value {
            // Derive closed_at/closed_by from workflow stamp
            (
                Some(stamp_to_wire(&bead.fields.workflow.stamp.at)),
                Some(bead.fields.workflow.stamp.by.as_str().to_string()),
                closure.reason.clone(),
                closure.on_branch.clone(),
            )
        } else {
            (None, None, None, None)
        };

    // Extract claim info
    let (assignee, assignee_at, assignee_expires) =
        if let Claim::Claimed { assignee, expires } = &bead.fields.claim.value {
            (
                Some(assignee.as_str().to_string()),
                Some(stamp_to_wire(&bead.fields.claim.stamp.at)),
                expires.map(|w| w.0),
            )
        } else {
            (None, None, None)
        };

    let labels = label_state_to_wire(label_state);

    WireBead {
        id: bead.core.id.as_str().to_string(),
        created_at: stamp_to_wire(&bead.core.created().at),
        created_by: bead.core.created().by.as_str().to_string(),
        created_on_branch: bead.core.created_on_branch().map(|s| s.to_string()),
        title: bead.fields.title.value.clone(),
        description: bead.fields.description.value.clone(),
        design: bead.fields.design.value.clone(),
        acceptance_criteria: bead.fields.acceptance_criteria.value.clone(),
        priority: bead.fields.priority.value.value(),
        bead_type: bead.fields.bead_type.value.as_str().to_string(),
        labels,
        external_ref: bead.fields.external_ref.value.clone(),
        source_repo: bead.fields.source_repo.value.clone(),
        estimated_minutes: bead.fields.estimated_minutes.value,
        status: bead.fields.workflow.value.status().to_string(),
        closed_at,
        closed_by,
        closed_reason,
        closed_on_branch,
        assignee,
        assignee_at,
        assignee_expires,
        at: stamp_to_wire(&bead_stamp.at),
        by: bead_stamp.by.as_str().to_string(),
        v: if v_map.is_empty() { None } else { Some(v_map) },
    }
}

fn wire_to_parts(wire: WireBead) -> Result<ParsedWireBead, WireError> {
    // Default stamp is bead-level
    let default_stamp = Stamp::new(
        wire_to_stamp(wire.at),
        ActorId::new(&wire.by).map_err(|e| WireError::InvalidValue(e.to_string()))?,
    );

    // Helper to get field stamp from _v or default
    let get_stamp = |field: &str| -> Result<Stamp, WireError> {
        if let Some(ref v_map) = wire.v
            && let Some((at, by)) = v_map.get(field)
        {
            return Ok(Stamp::new(
                wire_to_stamp(*at),
                ActorId::new(by).map_err(|e| WireError::InvalidValue(e.to_string()))?,
            ));
        }
        Ok(default_stamp.clone())
    };

    let label_stamp = get_stamp("labels")?;

    // Parse core
    let core = BeadCore::new(
        BeadId::parse(&wire.id).map_err(|e| WireError::InvalidValue(e.to_string()))?,
        Stamp::new(
            wire_to_stamp(wire.created_at),
            ActorId::new(&wire.created_by).map_err(|e| WireError::InvalidValue(e.to_string()))?,
        ),
        wire.created_on_branch,
    );

    // Parse claim
    let claim_value = match wire.assignee {
        Some(assignee) => Claim::claimed(
            ActorId::new(assignee).map_err(|e| WireError::InvalidValue(e.to_string()))?,
            wire.assignee_expires.map(WallClock),
        ),
        None => Claim::Unclaimed,
    };

    // Parse workflow
    let workflow_value = match wire.status.as_str() {
        "open" => Workflow::Open,
        "in_progress" => Workflow::InProgress,
        "closed" => Workflow::Closed(Closure::new(wire.closed_reason, wire.closed_on_branch)),
        _ => {
            return Err(WireError::InvalidValue(format!(
                "unknown status: {}",
                wire.status
            )));
        }
    };

    let label_state = {
        let set = OrSet::try_from_parts(wire.labels.entries, wire.labels.cc)
            .map_err(|err| WireError::InvalidValue(format!("label orset invalid: {err}")))?;
        LabelState::from_parts(set, Some(label_stamp.clone()))
    };

    // Build fields
    let fields = BeadFields {
        title: Lww::new(wire.title, get_stamp("title")?),
        description: Lww::new(wire.description, get_stamp("description")?),
        design: Lww::new(wire.design, get_stamp("design")?),
        acceptance_criteria: Lww::new(wire.acceptance_criteria, get_stamp("acceptance_criteria")?),
        priority: Lww::new(
            Priority::new(wire.priority).map_err(|e| WireError::InvalidValue(e.to_string()))?,
            get_stamp("priority")?,
        ),
        bead_type: Lww::new(str_to_bead_type(&wire.bead_type)?, get_stamp("type")?),
        external_ref: Lww::new(wire.external_ref, get_stamp("external_ref")?),
        source_repo: Lww::new(wire.source_repo, get_stamp("source_repo")?),
        estimated_minutes: Lww::new(wire.estimated_minutes, get_stamp("estimated_minutes")?),
        workflow: Lww::new(workflow_value, get_stamp("workflow")?),
        claim: Lww::new(claim_value, get_stamp("claim")?),
    };

    let bead = Bead::new(core, fields);

    Ok(ParsedWireBead { bead, label_state })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{DepKind, ReplicaId};
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet};
    use uuid::Uuid;

    fn actor_id(actor: &str) -> ActorId {
        ActorId::new(actor).unwrap_or_else(|e| panic!("invalid actor id {actor}: {e}"))
    }

    fn bead_id(id: &str) -> BeadId {
        BeadId::parse(id).unwrap_or_else(|e| panic!("invalid bead id {id}: {e}"))
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

    fn dot(replica_byte: u8, counter: u64) -> Dot {
        Dot {
            replica: ReplicaId::from(Uuid::from_bytes([replica_byte; 16])),
            counter,
        }
    }

    fn apply_dep_add_checked(state: &mut CanonicalState, key: DepKey, dot: Dot, stamp: Stamp) {
        let key = state
            .check_dep_add_key(key)
            .unwrap_or_else(|err| panic!("dep key invalid: {}", err.reason));
        state.apply_dep_add(key, dot, stamp);
    }

    fn base58_id_strategy() -> impl Strategy<Value = String> {
        proptest::string::string_regex("[1-9A-HJ-NP-Za-km-z]{5,8}")
            .unwrap_or_else(|e| panic!("regex failed: {e}"))
            .prop_map(|suffix| format!("bd-{suffix}"))
    }

    fn stamp_strategy() -> impl Strategy<Value = Stamp> {
        let actor = prop_oneof![Just("alice"), Just("bob"), Just("carol")];
        (0u64..10_000, 0u32..5, actor).prop_map(|(wall_ms, counter, actor)| {
            Stamp::new(WriteStamp::new(wall_ms, counter), actor_id(actor))
        })
    }

    fn tombstone_strategy() -> impl Strategy<Value = Tombstone> {
        (base58_id_strategy(), stamp_strategy())
            .prop_map(|(id, stamp)| Tombstone::new(bead_id(&id), stamp, None))
    }

    fn dep_strategy() -> impl Strategy<Value = (DepKey, Dot, Stamp)> {
        let kind = prop_oneof![
            Just(DepKind::Blocks),
            Just(DepKind::Parent),
            Just(DepKind::Related),
            Just(DepKind::DiscoveredFrom),
        ];
        let dot_strategy =
            (0u8..=255, 0u64..10_000).prop_map(|(replica, counter)| dot(replica, counter));
        (
            base58_id_strategy(),
            base58_id_strategy(),
            kind,
            dot_strategy,
            stamp_strategy(),
        )
            .prop_filter("deps cannot be self-referential", |(from, to, _, _, _)| {
                from != to
            })
            .prop_map(|(from, to, kind, dot, stamp)| {
                let key = DepKey::new(bead_id(&from), bead_id(&to), kind)
                    .unwrap_or_else(|e| panic!("dep key invalid: {}", e.reason));
                (key, dot, stamp)
            })
    }

    #[test]
    fn roundtrip_empty_state() {
        let state = CanonicalState::new();
        let bytes = serialize_state(&state).unwrap();
        let beads = parse_state(&bytes).unwrap();
        assert!(beads.is_empty());
        let notes = serialize_notes(&state).unwrap();
        assert!(notes.is_empty());
    }

    #[test]
    fn parse_state_roundtrip() {
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        let bead = make_bead(&bead_id("bd-legacy"), &stamp);
        state.insert(bead).unwrap();
        state.insert_tombstone(Tombstone::new(
            bead_id("bd-legacy-tomb"),
            stamp.clone(),
            None,
        ));
        let dep_key = DepKey::new(
            bead_id("bd-legacy"),
            bead_id("bd-legacy-target"),
            DepKind::Blocks,
        )
        .unwrap();
        apply_dep_add_checked(&mut state, dep_key, dot(2, 1), stamp.clone());

        let note_id = NoteId::new("note-legacy").unwrap();
        let note = Note::new(
            note_id,
            "legacy note".to_string(),
            actor_id("alice"),
            WriteStamp::new(2, 0),
        );
        state.insert_note(bead_id("bd-legacy"), Some(stamp.clone()), note);

        let state_bytes = serialize_state(&state).unwrap();
        let tomb_bytes = serialize_tombstones(&state).unwrap();
        let deps_bytes = serialize_deps(&state).unwrap();
        let notes_bytes = serialize_notes(&state).unwrap();

        let parsed =
            parse_legacy_state(&state_bytes, &tomb_bytes, &deps_bytes, &notes_bytes).unwrap();
        assert_eq!(serialize_state(&parsed).unwrap(), state_bytes);
        assert_eq!(serialize_tombstones(&parsed).unwrap(), tomb_bytes);
        assert_eq!(serialize_deps(&parsed).unwrap(), deps_bytes);
        assert_eq!(serialize_notes(&parsed).unwrap(), notes_bytes);
    }

    #[test]
    fn roundtrip_orset_metadata_and_notes() {
        let stamp = Stamp::new(WriteStamp::new(5, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        let id = bead_id("bd-orset");
        state.insert(make_bead(&id, &stamp)).unwrap();

        let label = Label::parse("urgent").unwrap();
        let dot = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([1u8; 16])),
            counter: 1,
        };
        state.apply_label_add(
            id.clone(),
            label.clone(),
            dot,
            stamp.clone(),
            Some(stamp.clone()),
        );
        let ctx = state.label_dvv(&id, &label, Some(&stamp));
        state.apply_label_remove(id.clone(), &label, &ctx, stamp.clone(), Some(stamp.clone()));

        let dep_key = DepKey::new(id.clone(), bead_id("bd-target"), DepKind::Blocks).unwrap();
        let dep_dot = Dot {
            replica: ReplicaId::from(Uuid::from_bytes([2u8; 16])),
            counter: 2,
        };
        apply_dep_add_checked(&mut state, dep_key.clone(), dep_dot, stamp.clone());
        let dep_ctx = state.dep_dvv(&dep_key);
        state.apply_dep_remove(&dep_key, &dep_ctx, stamp.clone());

        let note_id = NoteId::new("note-orset").unwrap();
        let note = Note::new(
            note_id,
            "orset note".to_string(),
            actor_id("bob"),
            WriteStamp::new(6, 0),
        );
        state.insert_note(id.clone(), Some(stamp.clone()), note);

        let state_bytes = serialize_state(&state).unwrap();
        let tomb_bytes = serialize_tombstones(&state).unwrap();
        let deps_bytes = serialize_deps(&state).unwrap();
        let notes_bytes = serialize_notes(&state).unwrap();

        let parsed =
            parse_legacy_state(&state_bytes, &tomb_bytes, &deps_bytes, &notes_bytes).unwrap();
        assert_eq!(serialize_state(&parsed).unwrap(), state_bytes);
        assert_eq!(serialize_deps(&parsed).unwrap(), deps_bytes);
        assert_eq!(serialize_notes(&parsed).unwrap(), notes_bytes);
    }

    #[test]
    fn roundtrip_orset_metadata_preserves_dots_and_context() {
        let stamp = Stamp::new(WriteStamp::new(10, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        let id = bead_id("bd-orset-meta");
        state.insert(make_bead(&id, &stamp)).unwrap();

        let label_a = Label::parse("urgent").unwrap();
        let label_b = Label::parse("backend").unwrap();
        state.apply_label_add(
            id.clone(),
            label_a.clone(),
            dot(1, 1),
            stamp.clone(),
            Some(stamp.clone()),
        );
        state.apply_label_add(
            id.clone(),
            label_b.clone(),
            dot(2, 2),
            stamp.clone(),
            Some(stamp.clone()),
        );
        let label_ctx = state.label_dvv(&id, &label_a, Some(&stamp));
        state.apply_label_remove(
            id.clone(),
            &label_a,
            &label_ctx,
            stamp.clone(),
            Some(stamp.clone()),
        );

        let dep_key_a = DepKey::new(id.clone(), bead_id("bd-target-a"), DepKind::Blocks).unwrap();
        let dep_key_b = DepKey::new(id.clone(), bead_id("bd-target-b"), DepKind::Related).unwrap();
        apply_dep_add_checked(&mut state, dep_key_a.clone(), dot(3, 3), stamp.clone());
        apply_dep_add_checked(&mut state, dep_key_b.clone(), dot(4, 4), stamp.clone());
        let dep_ctx = state.dep_dvv(&dep_key_b);
        state.apply_dep_remove(&dep_key_b, &dep_ctx, stamp.clone());

        let note_id = NoteId::new("note-meta").unwrap();
        let note = Note::new(
            note_id,
            "metadata note".to_string(),
            actor_id("bob"),
            WriteStamp::new(11, 0),
        );
        state.insert_note(id.clone(), Some(stamp.clone()), note);

        let state_bytes = serialize_state(&state).unwrap();
        let tomb_bytes = serialize_tombstones(&state).unwrap();
        let deps_bytes = serialize_deps(&state).unwrap();
        let notes_bytes = serialize_notes(&state).unwrap();

        let parsed =
            parse_legacy_state(&state_bytes, &tomb_bytes, &deps_bytes, &notes_bytes).unwrap();

        let original_labels = state
            .label_store()
            .state(&id, &stamp)
            .expect("label state missing");
        let parsed_labels = parsed
            .label_store()
            .state(&id, &stamp)
            .expect("label state missing");
        assert_eq!(original_labels.cc(), parsed_labels.cc());
        assert_eq!(
            original_labels.dots_for(&label_a),
            parsed_labels.dots_for(&label_a)
        );
        assert_eq!(
            original_labels.dots_for(&label_b),
            parsed_labels.dots_for(&label_b)
        );
        assert_eq!(original_labels.stamp(), parsed_labels.stamp());

        let original_deps = state.dep_store();
        let parsed_deps = parsed.dep_store();
        assert_eq!(original_deps.cc(), parsed_deps.cc());
        assert_eq!(
            original_deps.dots_for(&dep_key_a),
            parsed_deps.dots_for(&dep_key_a)
        );
        assert_eq!(
            original_deps.dots_for(&dep_key_b),
            parsed_deps.dots_for(&dep_key_b)
        );
        assert_eq!(original_deps.stamp(), parsed_deps.stamp());

        let original_notes = state.notes_for(&id);
        let parsed_notes = parsed.notes_for(&id);
        assert_eq!(original_notes, parsed_notes);
    }

    #[test]
    fn serialize_orset_is_deterministic_across_insertion_order() {
        let stamp = Stamp::new(WriteStamp::new(12, 0), actor_id("alice"));
        let label_stamp_a = Stamp::new(WriteStamp::new(20, 0), actor_id("bob"));
        let label_stamp_b = Stamp::new(WriteStamp::new(21, 0), actor_id("carol"));
        let dep_stamp_a = Stamp::new(WriteStamp::new(22, 0), actor_id("dave"));
        let dep_stamp_b = Stamp::new(WriteStamp::new(23, 0), actor_id("erin"));
        let id = bead_id("bd-orset-order");

        let mut state_a = CanonicalState::new();
        state_a.insert(make_bead(&id, &stamp)).unwrap();
        let label_a = Label::parse("alpha").unwrap();
        let label_b = Label::parse("beta").unwrap();
        state_a.apply_label_add(
            id.clone(),
            label_a.clone(),
            dot(5, 1),
            label_stamp_a.clone(),
            Some(stamp.clone()),
        );
        state_a.apply_label_add(
            id.clone(),
            label_b.clone(),
            dot(6, 2),
            label_stamp_b.clone(),
            Some(stamp.clone()),
        );
        let dep_key_a = DepKey::new(id.clone(), bead_id("bd-order-a"), DepKind::Blocks).unwrap();
        let dep_key_b = DepKey::new(id.clone(), bead_id("bd-order-b"), DepKind::Related).unwrap();
        apply_dep_add_checked(
            &mut state_a,
            dep_key_a.clone(),
            dot(7, 3),
            dep_stamp_a.clone(),
        );
        apply_dep_add_checked(
            &mut state_a,
            dep_key_b.clone(),
            dot(8, 4),
            dep_stamp_b.clone(),
        );
        let note_a = Note::new(
            NoteId::new("note-a").unwrap(),
            "first".to_string(),
            actor_id("bob"),
            WriteStamp::new(10, 0),
        );
        let note_b = Note::new(
            NoteId::new("note-b").unwrap(),
            "second".to_string(),
            actor_id("carol"),
            WriteStamp::new(11, 0),
        );
        state_a.insert_note(id.clone(), Some(stamp.clone()), note_a);
        state_a.insert_note(id.clone(), Some(stamp.clone()), note_b);

        let mut state_b = CanonicalState::new();
        state_b.insert(make_bead(&id, &stamp)).unwrap();
        state_b.apply_label_add(
            id.clone(),
            label_b,
            dot(6, 2),
            label_stamp_b,
            Some(stamp.clone()),
        );
        state_b.apply_label_add(
            id.clone(),
            label_a,
            dot(5, 1),
            label_stamp_a,
            Some(stamp.clone()),
        );
        apply_dep_add_checked(&mut state_b, dep_key_b, dot(8, 4), dep_stamp_b);
        apply_dep_add_checked(&mut state_b, dep_key_a, dot(7, 3), dep_stamp_a);
        state_b.insert_note(
            id.clone(),
            Some(stamp.clone()),
            Note::new(
                NoteId::new("note-b").unwrap(),
                "second".to_string(),
                actor_id("carol"),
                WriteStamp::new(11, 0),
            ),
        );
        state_b.insert_note(
            id.clone(),
            Some(stamp.clone()),
            Note::new(
                NoteId::new("note-a").unwrap(),
                "first".to_string(),
                actor_id("bob"),
                WriteStamp::new(10, 0),
            ),
        );

        assert_eq!(
            serialize_state(&state_a).unwrap(),
            serialize_state(&state_b).unwrap()
        );
        assert_eq!(
            serialize_deps(&state_a).unwrap(),
            serialize_deps(&state_b).unwrap()
        );
        assert_eq!(
            serialize_notes(&state_a).unwrap(),
            serialize_notes(&state_b).unwrap()
        );
    }

    #[test]
    fn label_ops_with_distinct_stamps_are_deterministic() {
        let base = Stamp::new(WriteStamp::new(5, 0), actor_id("alice"));
        let stamp_a = Stamp::new(WriteStamp::new(10, 0), actor_id("bob"));
        let stamp_b = Stamp::new(WriteStamp::new(11, 0), actor_id("carol"));
        let id = bead_id("bd-label-order");
        let label_a = Label::parse("alpha").unwrap();
        let label_b = Label::parse("beta").unwrap();

        let mut state_a = CanonicalState::new();
        state_a.insert(make_bead(&id, &base)).unwrap();
        state_a.apply_label_add(
            id.clone(),
            label_a.clone(),
            dot(1, 1),
            stamp_a.clone(),
            Some(base.clone()),
        );
        state_a.apply_label_add(
            id.clone(),
            label_b.clone(),
            dot(2, 1),
            stamp_b.clone(),
            Some(base.clone()),
        );

        let mut state_b = CanonicalState::new();
        state_b.insert(make_bead(&id, &base)).unwrap();
        state_b.apply_label_add(id.clone(), label_b, dot(2, 1), stamp_b, Some(base.clone()));
        state_b.apply_label_add(id.clone(), label_a, dot(1, 1), stamp_a, Some(base.clone()));

        assert_eq!(
            serialize_state(&state_a).unwrap(),
            serialize_state(&state_b).unwrap()
        );
        assert_eq!(state_a.label_stamp(&id), state_b.label_stamp(&id));
    }

    #[test]
    fn dep_ops_with_distinct_stamps_are_deterministic() {
        let stamp_a = Stamp::new(WriteStamp::new(15, 0), actor_id("dan"));
        let stamp_b = Stamp::new(WriteStamp::new(16, 0), actor_id("erin"));
        let key_a = DepKey::new(bead_id("bd-dep-a"), bead_id("bd-dep-b"), DepKind::Blocks).unwrap();
        let key_b =
            DepKey::new(bead_id("bd-dep-a"), bead_id("bd-dep-c"), DepKind::Related).unwrap();

        let mut state_a = CanonicalState::new();
        apply_dep_add_checked(&mut state_a, key_a.clone(), dot(3, 1), stamp_a.clone());
        apply_dep_add_checked(&mut state_a, key_b.clone(), dot(4, 1), stamp_b.clone());

        let mut state_b = CanonicalState::new();
        apply_dep_add_checked(&mut state_b, key_b, dot(4, 1), stamp_b);
        apply_dep_add_checked(&mut state_b, key_a, dot(3, 1), stamp_a);

        assert_eq!(
            serialize_deps(&state_a).unwrap(),
            serialize_deps(&state_b).unwrap()
        );
        assert_eq!(state_a.dep_store().stamp(), state_b.dep_store().stamp());
    }

    #[test]
    fn serialize_deps_sorts_by_dep_kind_canonical() {
        let stamp = Stamp::new(WriteStamp::new(12, 0), actor_id("alice"));
        let from = bead_id("bd-kind-from");
        let to = bead_id("bd-kind-to");

        let mut state = CanonicalState::new();
        state.insert(make_bead(&from, &stamp)).unwrap();

        let key_blocks = DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap();
        let key_discovered =
            DepKey::new(from.clone(), to.clone(), DepKind::DiscoveredFrom).unwrap();
        let key_parent = DepKey::new(from.clone(), to.clone(), DepKind::Parent).unwrap();
        let key_related = DepKey::new(from.clone(), to.clone(), DepKind::Related).unwrap();

        apply_dep_add_checked(&mut state, key_related, dot(1, 4), stamp.clone());
        apply_dep_add_checked(&mut state, key_parent, dot(1, 3), stamp.clone());
        apply_dep_add_checked(&mut state, key_discovered, dot(1, 2), stamp.clone());
        apply_dep_add_checked(&mut state, key_blocks, dot(1, 1), stamp.clone());

        let bytes = serialize_deps(&state).unwrap();
        let wire: WireDepStore =
            serde_json::from_slice(&bytes).expect("deps.jsonl should deserialize");
        let kinds: Vec<DepKind> = wire.entries.iter().map(|entry| entry.key.kind()).collect();
        assert_eq!(
            kinds,
            vec![
                DepKind::Blocks,
                DepKind::DiscoveredFrom,
                DepKind::Parent,
                DepKind::Related
            ]
        );
    }

    #[test]
    fn legacy_deps_jsonl_parses_expected_entries() {
        let stamp = Stamp::new(WriteStamp::new(42, 7), actor_id("alice"));
        let replica_a = ReplicaId::from(Uuid::from_bytes([4u8; 16]));
        let replica_b = ReplicaId::from(Uuid::from_bytes([7u8; 16]));
        let key_blocks = DepKey::new(
            bead_id("bd-legacy-from"),
            bead_id("bd-legacy-to"),
            DepKind::Blocks,
        )
        .unwrap();
        let key_related = DepKey::new(
            bead_id("bd-legacy-from"),
            bead_id("bd-legacy-other"),
            DepKind::Related,
        )
        .unwrap();

        let wire = WireDepStore {
            cc: Dvv {
                max: BTreeMap::from([(replica_a, 1), (replica_b, 2)]),
                dots: BTreeSet::new(),
            },
            entries: vec![
                WireDepEntry {
                    key: key_related.clone(),
                    dots: vec![dot(7, 5), dot(7, 3)],
                },
                WireDepEntry {
                    key: key_blocks.clone(),
                    dots: vec![dot(4, 2)],
                },
            ],
            stamp: Some(stamp_to_field(&stamp)),
        };

        let mut bytes = serde_json::to_vec(&wire).expect("serialize wire deps");
        bytes.push(b'\n');
        let parsed = parse_deps(&bytes).expect("parse_deps");

        assert!(parsed.contains(&key_blocks));
        assert!(parsed.contains(&key_related));
        assert_eq!(parsed.stamp(), Some(&stamp));
        assert_eq!(parsed.cc().max.get(&replica_a), Some(&1));
        assert_eq!(parsed.cc().max.get(&replica_b), Some(&2));
        let related_dots = parsed.dots_for(&key_related).expect("related dots");
        assert_eq!(related_dots, &BTreeSet::from([dot(7, 5), dot(7, 3)]));
    }

    #[test]
    fn legacy_deps_parse_then_serialize_is_deterministic() {
        let key_parent = DepKey::new(
            bead_id("bd-legacy-a"),
            bead_id("bd-legacy-b"),
            DepKind::Parent,
        )
        .unwrap();
        let key_blocks = DepKey::new(
            bead_id("bd-legacy-a"),
            bead_id("bd-legacy-c"),
            DepKind::Blocks,
        )
        .unwrap();

        let wire = WireDepStore {
            cc: Dvv::default(),
            entries: vec![
                WireDepEntry {
                    key: key_parent.clone(),
                    dots: vec![dot(9, 2), dot(9, 1)],
                },
                WireDepEntry {
                    key: key_blocks.clone(),
                    dots: vec![dot(8, 3)],
                },
            ],
            stamp: None,
        };
        let mut bytes = serde_json::to_vec(&wire).expect("serialize wire deps");
        bytes.push(b'\n');

        let parsed = parse_deps(&bytes).expect("parse legacy deps");
        let mut state = CanonicalState::new();
        state.set_dep_store(parsed);

        let serialized = serialize_deps(&state).expect("serialize deps");
        let serialized_again = serialize_deps(&state).expect("serialize deps");
        assert_eq!(serialized, serialized_again);
        assert!(
            serialized.ends_with(b"\n"),
            "deps.jsonl should end with newline"
        );

        let wire_out: WireDepStore =
            serde_json::from_slice(&serialized).expect("deps.jsonl should deserialize");
        let keys: Vec<DepKey> = wire_out
            .entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect();
        let mut expected_keys = vec![key_parent.clone(), key_blocks.clone()];
        expected_keys.sort();
        assert_eq!(keys, expected_keys);
        let blocks_entry = wire_out
            .entries
            .iter()
            .find(|entry| entry.key == key_blocks)
            .expect("blocks entry present");
        let parent_entry = wire_out
            .entries
            .iter()
            .find(|entry| entry.key == key_parent)
            .expect("parent entry present");
        assert_eq!(blocks_entry.dots, vec![dot(8, 3)]);
        assert_eq!(parent_entry.dots, vec![dot(9, 1), dot(9, 2)]);
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

        #[test]
        fn roundtrip_state(beads in prop::collection::vec((base58_id_strategy(), stamp_strategy()), 0..12)) {
            let mut state = CanonicalState::new();
            for (id, stamp) in beads {
                let bead = make_bead(&bead_id(&id), &stamp);
                if let Err(err) = state.insert(bead) {
                    panic!("insert bead failed: {err:?}");
                }
            }

            let bytes = serialize_state(&state).unwrap_or_else(|e| panic!("serialize_state failed: {e}"));
            let parsed = parse_state(&bytes).unwrap_or_else(|e| panic!("parse_state failed: {e}"));
            let mut rebuilt = CanonicalState::new();
            for bead in parsed {
                rebuilt.insert_live(bead);
            }
            let bytes2 = serialize_state(&rebuilt).unwrap_or_else(|e| panic!("serialize_state failed: {e}"));
            prop_assert_eq!(bytes, bytes2);
        }

        #[test]
        fn roundtrip_tombstones(tombs in prop::collection::vec(tombstone_strategy(), 0..12)) {
            let mut state = CanonicalState::new();
            for tomb in tombs {
                state.delete(tomb);
            }
            let bytes = serialize_tombstones(&state)
                .unwrap_or_else(|e| panic!("serialize_tombstones failed: {e}"));
            let parsed = parse_tombstones(&bytes)
                .unwrap_or_else(|e| panic!("parse_tombstones failed: {e}"));
            let mut rebuilt = CanonicalState::new();
            for tomb in parsed {
                rebuilt.delete(tomb);
            }
            let bytes2 = serialize_tombstones(&rebuilt)
                .unwrap_or_else(|e| panic!("serialize_tombstones failed: {e}"));
            prop_assert_eq!(bytes, bytes2);
        }

        #[test]
        fn roundtrip_deps(deps in prop::collection::vec(dep_strategy(), 0..12)) {
            let mut state = CanonicalState::new();
            for (key, dot, stamp) in deps {
                apply_dep_add_checked(&mut state, key, dot, stamp);
            }
            let bytes = serialize_deps(&state).unwrap_or_else(|e| panic!("serialize_deps failed: {e}"));
            let parsed = parse_deps(&bytes).unwrap_or_else(|e| panic!("parse_deps failed: {e}"));
            let mut rebuilt = CanonicalState::new();
            rebuilt.set_dep_store(parsed);
            let bytes2 = serialize_deps(&rebuilt).unwrap_or_else(|e| panic!("serialize_deps failed: {e}"));
            prop_assert_eq!(bytes, bytes2);
        }

        #[test]
        fn roundtrip_meta(root in proptest::option::of(base58_id_strategy()), stamp in proptest::option::of((0u64..10_000, 0u32..5))) {
            let write_stamp = stamp.map(|(wall_ms, counter)| WriteStamp::new(wall_ms, counter));
            let checksums = StoreChecksums::from_bytes(&[], &[], &[], Some(&[]));
            let bytes = serialize_meta(root.as_deref(), write_stamp.as_ref(), &checksums)
                .unwrap_or_else(|e| panic!("serialize_meta failed: {e}"));
            let parsed = parse_meta(&bytes).unwrap_or_else(|e| panic!("parse_meta failed: {e}"));
            let expected_root = root
                .as_ref()
                .map(|raw| BeadSlug::parse(raw).expect("slug parse"));
            match parsed.meta() {
                StoreMeta::V1 {
                    root_slug,
                    last_write_stamp,
                    checksums: parsed_checksums,
                } => {
                    prop_assert_eq!(root_slug, &expected_root);
                    prop_assert_eq!(last_write_stamp, &write_stamp);
                    prop_assert_eq!(parsed_checksums, &checksums);
                }
                StoreMeta::Legacy => {
                    prop_assert!(false, "expected v1 meta");
                }
            }
        }
    }

    #[test]
    fn verify_store_checksums_detects_mismatch() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        verify_store_checksums(&checksums, b"state", b"tombs", b"deps", b"notes").unwrap();

        let err = verify_store_checksums(&checksums, b"state-x", b"tombs", b"deps", b"notes")
            .unwrap_err();
        match err {
            WireError::ChecksumMismatch { blob, .. } => {
                assert_eq!(blob, "state.jsonl");
            }
            other => panic!("expected checksum mismatch, got {other:?}"),
        }
    }

    #[test]
    fn parse_meta_rejects_unsupported_version() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        value["format_version"] = serde_json::Value::from(2);
        let mutated = serde_json::to_vec(&value).expect("json");

        let err = parse_meta(&mutated).expect_err("unsupported version should fail");
        assert!(matches!(err, WireError::InvalidValue(_)));
    }

    #[test]
    fn parse_meta_rejects_missing_checksums() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        value
            .as_object_mut()
            .expect("object")
            .remove("notes_sha256");
        let mutated = serde_json::to_vec(&value).expect("json");

        let err = parse_meta(&mutated).expect_err("missing checksums should fail");
        assert!(matches!(err, WireError::InvalidValue(_)));
    }

    #[test]
    fn parse_meta_rejects_invalid_root_slug() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        value["root_slug"] = serde_json::Value::from("bad slug");
        let mutated = serde_json::to_vec(&value).expect("json");

        let err = parse_meta(&mutated).expect_err("invalid slug should fail");
        assert!(matches!(err, WireError::InvalidValue(_)));
    }

    #[test]
    fn parse_meta_accepts_v1_with_checksums() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let parsed = parse_meta(&bytes).expect("parse meta");
        match parsed.meta() {
            StoreMeta::V1 {
                root_slug,
                last_write_stamp,
                checksums: parsed_checksums,
            } => {
                assert_eq!(
                    root_slug,
                    &Some(BeadSlug::parse("valid-slug").expect("slug"))
                );
                assert_eq!(last_write_stamp, &None);
                assert_eq!(parsed_checksums, &checksums);
            }
            StoreMeta::Legacy => panic!("expected v1 meta"),
        }
    }
}
