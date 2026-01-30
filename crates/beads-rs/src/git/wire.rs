//! Wire format serialization for git storage.
//!
//! Per SPEC §5.2.1, uses sparse _v representation:
//! - Each bead has top-level `_at` and `_by` (bead-level stamp)
//! - `_v` object maps field names to stamps only when they differ from bead-level
//! - If all fields share bead-level stamp, `_v` is omitted

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::error::WireError;
use crate::core::state::{LabelState, legacy_fallback_lineage};
use crate::core::{
    ActorId, Bead, BeadId, BeadSlug, BeadSnapshotWireV1, CanonicalState, ContentHash, DepKey,
    DepStore, Dot, LabelStore, Note, NoteAppendV1, NoteId, NoteStore, OrSet, Stamp, Tombstone,
    WireDepEntryV1, WireDepStoreV1, WireFieldStamp, WireLineageStamp, WireNoteV1, WireStamp,
    WireTombstoneV1, WriteStamp, sha256_bytes,
};

// =============================================================================
// Wire format types (intermediate representation for JSON)
// =============================================================================

#[derive(Clone, Debug)]
struct ParsedWireBead {
    bead: Bead,
    label_state: LabelState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WireBeadFullCompat {
    #[serde(flatten)]
    wire: BeadSnapshotWireV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    closed_at: Option<WireStamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    closed_by: Option<ActorId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    assignee_at: Option<WireStamp>,
}

impl WireBeadFullCompat {
    fn from_wire(wire: BeadSnapshotWireV1) -> Self {
        let workflow_stamp = wire_field_stamp(&wire, "workflow");
        let claim_stamp = wire_field_stamp(&wire, "claim");
        let (closed_at, closed_by) = match &wire.workflow {
            crate::core::WireWorkflowSnapshot::Closed { .. } => (
                Some(WireStamp::from(&workflow_stamp.at)),
                Some(workflow_stamp.by.clone()),
            ),
            _ => (None, None),
        };
        let assignee_at = match &wire.claim {
            crate::core::WireClaimSnapshot::Claimed(_) => Some(WireStamp::from(&claim_stamp.at)),
            crate::core::WireClaimSnapshot::Unclaimed(_) => None,
        };
        Self {
            wire,
            closed_at,
            closed_by,
            assignee_at,
        }
    }

    fn validate_redundant_fields(&self, raw: &serde_json::Value) -> Result<(), WireError> {
        let obj = raw.as_object().ok_or_else(|| {
            WireError::InvalidValue("state.jsonl bead must be a JSON object".to_string())
        })?;

        let workflow_closed = matches!(
            &self.wire.workflow,
            crate::core::WireWorkflowSnapshot::Closed { .. }
        );
        if workflow_closed {
            match (self.closed_at.as_ref(), self.closed_by.as_ref()) {
                (Some(closed_at), Some(closed_by)) => {
                    let workflow_stamp = wire_field_stamp(&self.wire, "workflow");
                    let closed_stamp = Stamp::new(wire_to_stamp(*closed_at), closed_by.clone());
                    if closed_stamp != workflow_stamp {
                        return Err(WireError::InvalidValue(
                            "closed_at/closed_by does not match workflow stamp".to_string(),
                        ));
                    }
                }
                (None, None) => {}
                _ => {
                    return Err(WireError::InvalidValue(
                        "closed_at/closed_by must be provided together".to_string(),
                    ));
                }
            }
        } else if self.closed_at.is_some() || self.closed_by.is_some() {
            return Err(WireError::InvalidValue(
                "closed_at/closed_by present for non-closed status".to_string(),
            ));
        }

        let claim_claimed = matches!(&self.wire.claim, crate::core::WireClaimSnapshot::Claimed(_));
        let assignee_present = obj.get("assignee").is_some();
        let assignee_expires_present = obj.get("assignee_expires").is_some();
        if claim_claimed {
            if let Some(assignee_at) = self.assignee_at.as_ref() {
                let claim_stamp = wire_field_stamp(&self.wire, "claim");
                if wire_to_stamp(*assignee_at) != claim_stamp.at {
                    return Err(WireError::InvalidValue(
                        "assignee_at does not match claim stamp".to_string(),
                    ));
                }
            }
        } else {
            if assignee_present || assignee_expires_present || self.assignee_at.is_some() {
                return Err(WireError::InvalidValue(
                    "assignee_at/expires present without assignee".to_string(),
                ));
            }
        }

        Ok(())
    }
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
        let label_state = state
            .label_store()
            .state(&id, view.bead.core.created())
            .cloned();
        let wire = BeadSnapshotWireV1::from_view(&view, label_state.as_ref());
        let compat = WireBeadFullCompat::from_wire(wire);
        let json = serde_json::to_string(&compat)?;
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
        let wire = WireTombstoneV1 {
            id: tomb.id.clone(),
            deleted_at: WireStamp::from(&tomb.deleted.at),
            deleted_by: tomb.deleted.by.clone(),
            reason: tomb.reason.clone(),
            lineage: tomb.lineage.as_ref().map(WireLineageStamp::from),
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
        entries.push(WireDepEntryV1 {
            key: key.clone(),
            dots,
        });
    }

    entries.sort_by(|a, b| a.key.cmp(&b.key));

    let wire = WireDepStoreV1 {
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

    entries.sort_by(|(a_id, a_lineage, a_note), (b_id, b_lineage, b_note)| {
        a_id.cmp(b_id)
            .then_with(|| a_lineage.cmp(b_lineage))
            .then_with(|| a_note.at.cmp(&b_note.at))
            .then_with(|| a_note.id.cmp(&b_note.id))
    });

    let mut lines = Vec::new();
    for (bead_id, lineage, note) in entries {
        let wire = NoteAppendV1 {
            bead_id,
            note: WireNoteV1::from(note),
            lineage: lineage.as_ref().map(WireLineageStamp::from),
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

fn ensure_strictly_increasing<T: Ord + std::fmt::Debug>(
    prev: Option<&T>,
    current: &T,
    file: &str,
    line: usize,
    label: &str,
) -> Result<(), WireError> {
    if let Some(prev) = prev {
        match current.cmp(prev) {
            Ordering::Greater => Ok(()),
            Ordering::Equal => Err(WireError::InvalidValue(format!(
                "{file} line {line}: duplicate {label} {current:?}"
            ))),
            Ordering::Less => Err(WireError::InvalidValue(format!(
                "{file} line {line}: {label} out of order (prev={prev:?}, got={current:?})"
            ))),
        }
    } else {
        Ok(())
    }
}

fn parse_state_full(bytes: &[u8]) -> Result<Vec<ParsedWireBead>, WireError> {
    let content = parse_utf8(bytes)?;
    let mut beads = Vec::new();
    let mut prev_id: Option<BeadId> = None;

    for (idx, line) in content.lines().enumerate() {
        let line_no = idx + 1;
        if line.trim().is_empty() {
            continue;
        }
        let raw: serde_json::Value = serde_json::from_str(line)?;
        let compat: WireBeadFullCompat = serde_json::from_value(raw.clone())?;
        compat.validate_redundant_fields(&raw)?;
        let parsed = wire_to_parts(compat.wire)?;
        let bead_id = parsed.bead.id().clone();
        ensure_strictly_increasing(
            prev_id.as_ref(),
            &bead_id,
            "state.jsonl",
            line_no,
            "bead id",
        )?;
        prev_id = Some(bead_id);
        beads.push(parsed);
    }

    Ok(beads)
}

/// Parse tombstones.jsonl bytes.
pub fn parse_tombstones(bytes: &[u8]) -> Result<Vec<Tombstone>, WireError> {
    let content = parse_utf8(bytes)?;
    let mut tombs = Vec::new();
    let mut prev_key = None;

    for (idx, line) in content.lines().enumerate() {
        let line_no = idx + 1;
        if line.trim().is_empty() {
            continue;
        }
        let wire: WireTombstoneV1 = serde_json::from_str(line)?;
        let deleted = wire.deleted_stamp();
        let lineage = wire.lineage_stamp();
        let tomb = if let Some(lineage) = lineage {
            Tombstone::new_collision(wire.id.clone(), deleted, lineage, wire.reason)
        } else {
            Tombstone::new(wire.id.clone(), deleted, wire.reason)
        };
        let key = tomb.key();
        ensure_strictly_increasing(
            prev_key.as_ref(),
            &key,
            "tombstones.jsonl",
            line_no,
            "tombstone key",
        )?;
        prev_key = Some(key);
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

    let wire: WireDepStoreV1 = serde_json::from_str(trimmed)?;
    let mut prev_key: Option<DepKey> = None;
    for (idx, entry) in wire.entries.iter().enumerate() {
        let entry_no = idx + 1;
        if let Some(prev) = prev_key.as_ref() {
            match entry.key.cmp(prev) {
                Ordering::Greater => {}
                Ordering::Equal => {
                    return Err(WireError::InvalidValue(format!(
                        "deps.jsonl entry {entry_no}: duplicate dep key {key:?}",
                        key = entry.key
                    )));
                }
                Ordering::Less => {
                    return Err(WireError::InvalidValue(format!(
                        "deps.jsonl entry {entry_no}: dep key out of order (prev={prev:?}, got={key:?})",
                        key = entry.key
                    )));
                }
            }
        }
        prev_key = Some(entry.key.clone());
    }
    wire_dep_store_to_state(wire)
}

/// Parse notes.jsonl bytes.
pub fn parse_notes(bytes: &[u8]) -> Result<Vec<(BeadId, Option<Stamp>, Note)>, WireError> {
    let content = parse_utf8(bytes)?;
    let mut notes = Vec::new();
    let mut prev_key: Option<(BeadId, Option<Stamp>, WriteStamp, NoteId)> = None;
    let mut seen_note_ids: BTreeSet<(BeadId, Option<Stamp>, NoteId)> = BTreeSet::new();

    for (idx, line) in content.lines().enumerate() {
        let line_no = idx + 1;
        if line.trim().is_empty() {
            continue;
        }
        let wire: NoteAppendV1 = serde_json::from_str(line)?;
        let lineage = wire.lineage_stamp();
        let bead_id = wire.bead_id;
        let note = Note::from(wire.note);
        let order_key = (
            bead_id.clone(),
            lineage.clone(),
            note.at.clone(),
            note.id.clone(),
        );
        ensure_strictly_increasing(
            prev_key.as_ref(),
            &order_key,
            "notes.jsonl",
            line_no,
            "note entry",
        )?;
        let note_id = note.id.clone();
        let id_key = (bead_id.clone(), lineage.clone(), note_id.clone());
        if !seen_note_ids.insert(id_key) {
            return Err(WireError::InvalidValue(format!(
                "notes.jsonl line {line_no}: duplicate note id {note_id} for bead {bead_id} lineage {lineage:?}"
            )));
        }
        prev_key = Some(order_key);
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
        insert_label_state(&mut label_store, bead_id, lineage, parsed.label_state);
    }
    for tombstone in tombstones {
        state.insert_tombstone(tombstone);
    }
    state.set_label_store(label_store);
    for (bead_id, lineage, note) in notes {
        let lineage = lineage.unwrap_or_else(|| {
            if state.has_collision_tombstone(&bead_id) {
                legacy_fallback_lineage()
            } else if let Some(bead) = state.get_live(&bead_id) {
                bead.core.created().clone()
            } else {
                legacy_fallback_lineage()
            }
        });
        note_store.insert(bead_id, lineage, note);
    }
    state.set_note_store(note_store);
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
            (Some(state), Some(tombstones), Some(deps), notes) => StoreChecksums {
                state,
                tombstones,
                deps,
                notes,
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

/// Parse meta.json bytes into a supported meta type.
pub fn parse_supported_meta(bytes: &[u8]) -> Result<SupportedStoreMeta, WireError> {
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
    WireStamp::from(stamp)
}

fn wire_to_stamp(wire: WireStamp) -> WriteStamp {
    WriteStamp::from(wire)
}

fn stamp_to_field(stamp: &Stamp) -> WireFieldStamp {
    (WireStamp::from(&stamp.at), stamp.by.clone())
}

fn wire_field_to_stamp(field: WireFieldStamp) -> Stamp {
    let (at, by) = field;
    Stamp::new(wire_to_stamp(at), by)
}

fn wire_field_stamp(wire: &BeadSnapshotWireV1, field: &str) -> Stamp {
    let bead_stamp = Stamp::new(wire_to_stamp(wire.at), wire.by.clone());
    if let Some(v_map) = &wire.v
        && let Some((at, by)) = v_map.get(field)
    {
        return Stamp::new(wire_to_stamp(*at), by.clone());
    }
    bead_stamp
}

fn wire_dep_store_to_state(wire: WireDepStoreV1) -> Result<DepStore, WireError> {
    let mut entries: BTreeMap<DepKey, BTreeSet<Dot>> = BTreeMap::new();
    for entry in wire.entries {
        let mut dots = BTreeSet::new();
        for dot in entry.dots {
            dots.insert(dot);
        }
        entries.insert(entry.key, dots);
    }

    let stamp = wire.stamp.map(wire_field_to_stamp);
    let set = OrSet::try_from_parts(entries, wire.cc)
        .map_err(|err| WireError::InvalidValue(format!("dep orset invalid: {err}")))?;
    Ok(DepStore::from_parts(set, stamp))
}

fn insert_label_state(
    label_store: &mut LabelStore,
    bead_id: BeadId,
    lineage: Stamp,
    state: LabelState,
) {
    let entry = label_store.state_mut(&bead_id, &lineage);
    *entry = LabelState::join(entry, &state);
}

fn wire_to_parts(wire: BeadSnapshotWireV1) -> Result<ParsedWireBead, WireError> {
    let label_stamp = wire.label_stamp();
    let labels = wire.labels.clone();
    let label_state = {
        let set = OrSet::try_from_parts(labels.entries, labels.cc)
            .map_err(|err| WireError::InvalidValue(format!("label orset invalid: {err}")))?;
        LabelState::from_parts(set, Some(label_stamp))
    };

    let bead = Bead::from(wire);
    Ok(ParsedWireBead { bead, label_state })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        ActorId, BeadCore, BeadFields, BeadType, Claim, DepKind, Dvv, Label, Lww, NoteId, OrSet,
        ParentEdge, Priority, ReplicaId, Workflow,
    };
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

    fn make_bead_with(id: &BeadId, stamp: &Stamp, workflow: Workflow, claim: Claim) -> Bead {
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
            workflow: Lww::new(workflow, stamp.clone()),
            claim: Lww::new(claim, stamp.clone()),
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
        let non_parent_kind = prop_oneof![
            Just(DepKind::Blocks),
            Just(DepKind::Related),
            Just(DepKind::DiscoveredFrom),
        ];
        let non_parent = (
            base58_id_strategy(),
            base58_id_strategy(),
            non_parent_kind,
            (0u8..=255, 0u64..10_000).prop_map(|(replica, counter)| dot(replica, counter)),
            stamp_strategy(),
        )
            .prop_filter("deps cannot be self-referential", |(from, to, _, _, _)| {
                from != to
            })
            .prop_map(|(from, to, kind, dot, stamp)| {
                let key = DepKey::new(bead_id(&from), bead_id(&to), kind)
                    .unwrap_or_else(|e| panic!("dep key invalid: {}", e.reason));
                (key, dot, stamp)
            });

        let parent = (
            base58_id_strategy(),
            base58_id_strategy(),
            (0u8..=255, 0u64..10_000).prop_map(|(replica, counter)| dot(replica, counter)),
            stamp_strategy(),
        )
            .prop_filter(
                "deps cannot be self-referential",
                |(child, parent, _, _)| child != parent,
            )
            .prop_map(|(child, parent, dot, stamp)| {
                let edge = ParentEdge::new(bead_id(&child), bead_id(&parent))
                    .unwrap_or_else(|e| panic!("parent edge invalid: {}", e.reason));
                (edge.to_dep_key(), dot, stamp)
            });

        prop_oneof![non_parent, parent]
    }

    fn jsonl_lines(bytes: &[u8]) -> Vec<String> {
        String::from_utf8(bytes.to_vec())
            .expect("utf-8")
            .lines()
            .map(|line| line.to_string())
            .collect()
    }

    fn join_jsonl(lines: &[String]) -> Vec<u8> {
        let mut output = lines.join("\n");
        if !output.is_empty() {
            output.push('\n');
        }
        output.into_bytes()
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
        state.insert_note(bead_id("bd-legacy"), stamp.clone(), note);

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
    fn parse_legacy_state_migrates_unscoped_notes() {
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        let id = bead_id("bd-unscoped");
        state.insert(make_bead(&id, &stamp)).unwrap();

        let state_bytes = serialize_state(&state).unwrap();
        let tomb_bytes = serialize_tombstones(&state).unwrap();
        let deps_bytes = serialize_deps(&state).unwrap();

        let note = Note::new(
            NoteId::new("note-legacy").unwrap(),
            "legacy note".to_string(),
            actor_id("alice"),
            WriteStamp::new(2, 0),
        );
        let note_wire = NoteAppendV1 {
            bead_id: id.clone(),
            note: WireNoteV1::from(note.clone()),
            lineage: None,
        };
        let notes_bytes = format!("{}\n", serde_json::to_string(&note_wire).unwrap()).into_bytes();

        let parsed =
            parse_legacy_state(&state_bytes, &tomb_bytes, &deps_bytes, &notes_bytes).unwrap();
        let notes = parsed.notes_for(&id);
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0], &note);
    }

    #[test]
    fn insert_label_state_merges_existing_entry() {
        let mut label_store = LabelStore::new();
        let bead_id = bead_id("bd-label-merge");
        let lineage = Stamp::new(WriteStamp::new(1, 0), actor_id("alice"));
        let label_a = Label::parse("urgent").unwrap();
        let label_b = Label::parse("backend").unwrap();
        let stamp_a = Stamp::new(WriteStamp::new(2, 0), actor_id("bob"));
        let stamp_b = Stamp::new(WriteStamp::new(3, 0), actor_id("carol"));

        let mut set_a = OrSet::new();
        set_a.apply_add(dot(1, 1), label_a.clone());
        let state_a = LabelState::from_parts(set_a, Some(stamp_a.clone()));

        let mut set_b = OrSet::new();
        set_b.apply_add(dot(2, 2), label_b.clone());
        let state_b = LabelState::from_parts(set_b, Some(stamp_b.clone()));

        insert_label_state(&mut label_store, bead_id.clone(), lineage.clone(), state_a);
        insert_label_state(&mut label_store, bead_id.clone(), lineage.clone(), state_b);

        let merged = label_store
            .state(&bead_id, &lineage)
            .expect("label state missing");
        assert!(merged.dots_for(&label_a).is_some());
        assert!(merged.dots_for(&label_b).is_some());
        assert_eq!(merged.stamp(), Some(&stamp_b));
    }

    #[test]
    fn serialize_state_emits_workflow_and_claim_timestamps() {
        let stamp = Stamp::new(WriteStamp::new(5, 1), actor_id("alice"));
        let workflow = Workflow::Closed(crate::core::Closure::new(None, None));
        let claim = Claim::claimed(actor_id("bob"), None);
        let bead = make_bead_with(&bead_id("bd-closed"), &stamp, workflow, claim);
        let mut state = CanonicalState::new();
        state.insert(bead).unwrap();

        let bytes = serialize_state(&state).expect("serialize_state");
        let lines = jsonl_lines(&bytes);
        assert_eq!(lines.len(), 1);
        let value: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        let obj = value.as_object().expect("object");
        assert!(obj.contains_key("closed_at"));
        assert!(obj.contains_key("closed_by"));
        assert!(obj.contains_key("assignee_at"));
    }

    #[test]
    fn parse_state_accepts_legacy_closed_without_closed_at_by() {
        let stamp = Stamp::new(WriteStamp::new(5, 1), actor_id("alice"));
        let workflow = Workflow::Closed(crate::core::Closure::new(None, None));
        let bead = make_bead_with(&bead_id("bd-missing"), &stamp, workflow, Claim::default());
        let mut state = CanonicalState::new();
        state.insert(bead).unwrap();

        let bytes = serialize_state(&state).expect("serialize_state");
        let mut lines = jsonl_lines(&bytes);
        let mut value: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        let obj = value.as_object_mut().expect("object");
        obj.remove("closed_at");
        obj.remove("closed_by");
        lines[0] = serde_json::to_string(&value).unwrap();

        let parsed = parse_state(&join_jsonl(&lines)).expect("legacy closed fields remain readable");
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].fields.workflow.value.is_closed());
    }

    #[test]
    fn parse_state_accepts_legacy_assignee_without_assignee_at() {
        let stamp = Stamp::new(WriteStamp::new(5, 1), actor_id("alice"));
        let claim = Claim::claimed(actor_id("bob"), None);
        let bead = make_bead_with(&bead_id("bd-claimed"), &stamp, Workflow::Open, claim);
        let mut state = CanonicalState::new();
        state.insert(bead).unwrap();

        let bytes = serialize_state(&state).expect("serialize_state");
        let mut lines = jsonl_lines(&bytes);
        let mut value: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        let obj = value.as_object_mut().expect("object");
        obj.remove("assignee_at");
        lines[0] = serde_json::to_string(&value).unwrap();

        let parsed = parse_state(&join_jsonl(&lines)).expect("legacy claim fields remain readable");
        assert_eq!(parsed.len(), 1);
        assert!(matches!(
            parsed[0].fields.claim.value,
            Claim::Claimed { .. }
        ));
    }

    #[test]
    fn parse_state_rejects_partial_closed_redundant_fields() {
        let stamp = Stamp::new(WriteStamp::new(5, 1), actor_id("alice"));
        let workflow = Workflow::Closed(crate::core::Closure::new(None, None));
        let bead = make_bead_with(&bead_id("bd-partial-closed"), &stamp, workflow, Claim::default());
        let mut state = CanonicalState::new();
        state.insert(bead).unwrap();

        let bytes = serialize_state(&state).expect("serialize_state");
        let mut lines = jsonl_lines(&bytes);
        let mut value: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        let obj = value.as_object_mut().expect("object");
        obj.remove("closed_by");
        lines[0] = serde_json::to_string(&value).unwrap();

        let err = parse_state(&join_jsonl(&lines))
            .expect_err("partial closed_at/closed_by should remain invalid");
        assert!(matches!(err, WireError::InvalidValue(_)));
    }

    #[test]
    fn parse_state_rejects_mismatched_closed_stamp() {
        let stamp = Stamp::new(WriteStamp::new(5, 1), actor_id("alice"));
        let workflow = Workflow::Closed(crate::core::Closure::new(None, None));
        let bead = make_bead_with(&bead_id("bd-closed2"), &stamp, workflow, Claim::default());
        let mut state = CanonicalState::new();
        state.insert(bead).unwrap();

        let bytes = serialize_state(&state).expect("serialize_state");
        let mut lines = jsonl_lines(&bytes);
        let mut value: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        let obj = value.as_object_mut().expect("object");
        obj.insert("closed_at".to_string(), serde_json::json!([999, 0]));
        lines[0] = serde_json::to_string(&value).unwrap();

        let err = parse_state(&join_jsonl(&lines)).expect_err("expected mismatched stamp");
        assert!(matches!(err, WireError::InvalidValue(_)));
    }

    #[test]
    fn parse_state_rejects_assignee_expires_without_assignee() {
        let stamp = Stamp::new(WriteStamp::new(5, 1), actor_id("alice"));
        let bead = make_bead(&bead_id("bd-unclaimed"), &stamp);
        let mut state = CanonicalState::new();
        state.insert(bead).unwrap();

        let bytes = serialize_state(&state).expect("serialize_state");
        let mut lines = jsonl_lines(&bytes);
        let mut value: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        let obj = value.as_object_mut().expect("object");
        obj.insert("assignee_expires".to_string(), serde_json::json!(12345));
        lines[0] = serde_json::to_string(&value).unwrap();

        let err = parse_state(&join_jsonl(&lines)).expect_err("expected invalid claim data");
        assert!(matches!(err, WireError::InvalidValue(_)));
    }

    #[test]
    fn serialize_state_omits_sparse_v_when_stamps_match() {
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        let bead = make_bead(&bead_id("bd-sparse"), &stamp);
        state.insert(bead).unwrap();

        let bytes = serialize_state(&state).expect("serialize_state");
        let lines = jsonl_lines(&bytes);
        assert_eq!(lines.len(), 1);

        let value: serde_json::Value =
            serde_json::from_str(&lines[0]).expect("state.jsonl should deserialize");
        let obj = value.as_object().expect("state json object");
        assert!(!obj.contains_key("_v"), "expected sparse _v to be omitted");
    }

    #[test]
    fn serialize_state_includes_sparse_v_for_overrides() {
        let base = Stamp::new(WriteStamp::new(1, 0), actor_id("alice"));
        let newer = Stamp::new(WriteStamp::new(2, 0), actor_id("bob"));
        let core = BeadCore::new(bead_id("bd-sparse-v"), base.clone(), None);
        let fields = BeadFields {
            title: Lww::new("title".to_string(), base.clone()),
            description: Lww::new(String::new(), newer.clone()),
            design: Lww::new(None, base.clone()),
            acceptance_criteria: Lww::new(None, base.clone()),
            priority: Lww::new(Priority::default(), base.clone()),
            bead_type: Lww::new(BeadType::Task, base.clone()),
            external_ref: Lww::new(None, base.clone()),
            source_repo: Lww::new(None, base.clone()),
            estimated_minutes: Lww::new(None, base.clone()),
            workflow: Lww::new(Workflow::default(), base.clone()),
            claim: Lww::new(Claim::default(), base.clone()),
        };
        let mut state = CanonicalState::new();
        state.insert(Bead::new(core, fields)).unwrap();

        let bytes = serialize_state(&state).expect("serialize_state");
        let lines = jsonl_lines(&bytes);
        assert_eq!(lines.len(), 1);

        let value: serde_json::Value =
            serde_json::from_str(&lines[0]).expect("state.jsonl should deserialize");
        let obj = value.as_object().expect("state json object");
        let v_map = obj
            .get("_v")
            .and_then(|value| value.as_object())
            .expect("expected sparse _v map");
        assert!(v_map.contains_key("title"));
        assert!(!v_map.contains_key("description"));
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
        state.apply_label_add(id.clone(), label.clone(), dot, stamp.clone(), stamp.clone());
        let ctx = state.label_dvv(&id, &label, &stamp);
        state.apply_label_remove(id.clone(), &label, &ctx, stamp.clone(), stamp.clone());

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
        state.insert_note(id.clone(), stamp.clone(), note);

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
            stamp.clone(),
        );
        state.apply_label_add(
            id.clone(),
            label_b.clone(),
            dot(2, 2),
            stamp.clone(),
            stamp.clone(),
        );
        let label_ctx = state.label_dvv(&id, &label_a, &stamp);
        state.apply_label_remove(
            id.clone(),
            &label_a,
            &label_ctx,
            stamp.clone(),
            stamp.clone(),
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
        state.insert_note(id.clone(), stamp.clone(), note);

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
            stamp.clone(),
        );
        state_a.apply_label_add(
            id.clone(),
            label_b.clone(),
            dot(6, 2),
            label_stamp_b.clone(),
            stamp.clone(),
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
        state_a.insert_note(id.clone(), stamp.clone(), note_a);
        state_a.insert_note(id.clone(), stamp.clone(), note_b);

        let mut state_b = CanonicalState::new();
        state_b.insert(make_bead(&id, &stamp)).unwrap();
        state_b.apply_label_add(id.clone(), label_b, dot(6, 2), label_stamp_b, stamp.clone());
        state_b.apply_label_add(id.clone(), label_a, dot(5, 1), label_stamp_a, stamp.clone());
        apply_dep_add_checked(&mut state_b, dep_key_b, dot(8, 4), dep_stamp_b);
        apply_dep_add_checked(&mut state_b, dep_key_a, dot(7, 3), dep_stamp_a);
        state_b.insert_note(
            id.clone(),
            stamp.clone(),
            Note::new(
                NoteId::new("note-b").unwrap(),
                "second".to_string(),
                actor_id("carol"),
                WriteStamp::new(11, 0),
            ),
        );
        state_b.insert_note(
            id.clone(),
            stamp.clone(),
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
            base.clone(),
        );
        state_a.apply_label_add(
            id.clone(),
            label_b.clone(),
            dot(2, 1),
            stamp_b.clone(),
            base.clone(),
        );

        let mut state_b = CanonicalState::new();
        state_b.insert(make_bead(&id, &base)).unwrap();
        state_b.apply_label_add(id.clone(), label_b, dot(2, 1), stamp_b, base.clone());
        state_b.apply_label_add(id.clone(), label_a, dot(1, 1), stamp_a, base.clone());

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
        let key_parent = ParentEdge::new(from.clone(), to.clone())
            .unwrap_or_else(|e| panic!("parent edge invalid: {}", e.reason))
            .to_dep_key();
        let key_related = DepKey::new(from.clone(), to.clone(), DepKind::Related).unwrap();

        apply_dep_add_checked(&mut state, key_related, dot(1, 4), stamp.clone());
        apply_dep_add_checked(&mut state, key_parent, dot(1, 3), stamp.clone());
        apply_dep_add_checked(&mut state, key_discovered, dot(1, 2), stamp.clone());
        apply_dep_add_checked(&mut state, key_blocks, dot(1, 1), stamp.clone());

        let bytes = serialize_deps(&state).unwrap();
        let wire: WireDepStoreV1 =
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

        let wire = WireDepStoreV1 {
            cc: Dvv {
                max: BTreeMap::from([(replica_a, 1), (replica_b, 2)]),
                dots: BTreeSet::new(),
            },
            entries: vec![
                WireDepEntryV1 {
                    key: key_related.clone(),
                    dots: vec![dot(7, 5), dot(7, 3)],
                },
                WireDepEntryV1 {
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
        let key_parent = ParentEdge::new(bead_id("bd-legacy-a"), bead_id("bd-legacy-b"))
            .unwrap_or_else(|e| panic!("parent edge invalid: {}", e.reason))
            .to_dep_key();
        let key_blocks = DepKey::new(
            bead_id("bd-legacy-a"),
            bead_id("bd-legacy-c"),
            DepKind::Blocks,
        )
        .unwrap();

        let wire = WireDepStoreV1 {
            cc: Dvv::default(),
            entries: vec![
                WireDepEntryV1 {
                    key: key_parent.clone(),
                    dots: vec![dot(9, 2), dot(9, 1)],
                },
                WireDepEntryV1 {
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

        let wire_out: WireDepStoreV1 =
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
            let parsed =
                parse_supported_meta(&bytes).unwrap_or_else(|e| panic!("parse_meta failed: {e}"));
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
    fn parse_state_rejects_out_of_order() {
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        state.insert_live(make_bead(&bead_id("bd-a"), &stamp));
        state.insert_live(make_bead(&bead_id("bd-b"), &stamp));
        let bytes = serialize_state(&state).expect("serialize_state");

        let mut lines = jsonl_lines(&bytes);
        lines.swap(0, 1);
        let bytes = join_jsonl(&lines);

        let err = parse_state(&bytes).expect_err("out-of-order state should fail");
        match err {
            WireError::InvalidValue(msg) => {
                assert!(msg.contains("state.jsonl line 2"), "{msg}");
            }
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn parse_state_rejects_duplicate() {
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        state.insert_live(make_bead(&bead_id("bd-a"), &stamp));
        state.insert_live(make_bead(&bead_id("bd-b"), &stamp));
        let bytes = serialize_state(&state).expect("serialize_state");

        let mut lines = jsonl_lines(&bytes);
        lines.insert(1, lines[0].clone());
        let bytes = join_jsonl(&lines);

        let err = parse_state(&bytes).expect_err("duplicate state should fail");
        match err {
            WireError::InvalidValue(msg) => {
                assert!(msg.contains("state.jsonl line 2"), "{msg}");
            }
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn parse_tombstones_rejects_out_of_order() {
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        state.delete(Tombstone::new(bead_id("bd-a"), stamp.clone(), None));
        state.delete(Tombstone::new(bead_id("bd-b"), stamp.clone(), None));
        let bytes = serialize_tombstones(&state).expect("serialize_tombstones");

        let mut lines = jsonl_lines(&bytes);
        lines.swap(0, 1);
        let bytes = join_jsonl(&lines);

        let err = parse_tombstones(&bytes).expect_err("out-of-order tombstones should fail");
        match err {
            WireError::InvalidValue(msg) => {
                assert!(msg.contains("tombstones.jsonl line 2"), "{msg}");
            }
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn parse_tombstones_rejects_duplicate() {
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        state.delete(Tombstone::new(bead_id("bd-a"), stamp.clone(), None));
        state.delete(Tombstone::new(bead_id("bd-b"), stamp.clone(), None));
        let bytes = serialize_tombstones(&state).expect("serialize_tombstones");

        let mut lines = jsonl_lines(&bytes);
        lines.insert(1, lines[0].clone());
        let bytes = join_jsonl(&lines);

        let err = parse_tombstones(&bytes).expect_err("duplicate tombstones should fail");
        match err {
            WireError::InvalidValue(msg) => {
                assert!(msg.contains("tombstones.jsonl line 2"), "{msg}");
            }
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn parse_deps_rejects_out_of_order() {
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        let key_a =
            DepKey::new(bead_id("bd-a"), bead_id("bd-b"), DepKind::Blocks).expect("dep key a");
        let key_b =
            DepKey::new(bead_id("bd-a"), bead_id("bd-c"), DepKind::Blocks).expect("dep key b");
        apply_dep_add_checked(&mut state, key_a, dot(1, 1), stamp.clone());
        apply_dep_add_checked(&mut state, key_b, dot(2, 2), stamp.clone());
        let bytes = serialize_deps(&state).expect("serialize_deps");

        let mut wire: WireDepStoreV1 = serde_json::from_slice(&bytes).expect("deps json");
        wire.entries.swap(0, 1);
        let mut bytes = serde_json::to_vec(&wire).expect("deps json");
        bytes.push(b'\n');

        let err = parse_deps(&bytes).expect_err("out-of-order deps should fail");
        match err {
            WireError::InvalidValue(msg) => {
                assert!(msg.contains("deps.jsonl entry 2"), "{msg}");
            }
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn parse_deps_rejects_duplicate() {
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        let key_a =
            DepKey::new(bead_id("bd-a"), bead_id("bd-b"), DepKind::Blocks).expect("dep key a");
        let key_b =
            DepKey::new(bead_id("bd-a"), bead_id("bd-c"), DepKind::Blocks).expect("dep key b");
        apply_dep_add_checked(&mut state, key_a, dot(1, 1), stamp.clone());
        apply_dep_add_checked(&mut state, key_b, dot(2, 2), stamp.clone());
        let bytes = serialize_deps(&state).expect("serialize_deps");

        let mut wire: WireDepStoreV1 = serde_json::from_slice(&bytes).expect("deps json");
        let entry = wire.entries[0].clone();
        wire.entries.insert(1, entry);
        let mut bytes = serde_json::to_vec(&wire).expect("deps json");
        bytes.push(b'\n');

        let err = parse_deps(&bytes).expect_err("duplicate deps should fail");
        match err {
            WireError::InvalidValue(msg) => {
                assert!(msg.contains("deps.jsonl entry 2"), "{msg}");
            }
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn parse_notes_rejects_out_of_order() {
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        let note_a = Note::new(
            NoteId::new("note-a").unwrap(),
            "note a".to_string(),
            actor_id("alice"),
            WriteStamp::new(1, 0),
        );
        let note_b = Note::new(
            NoteId::new("note-b").unwrap(),
            "note b".to_string(),
            actor_id("alice"),
            WriteStamp::new(2, 0),
        );
        state.insert_note(bead_id("bd-a"), stamp.clone(), note_a);
        state.insert_note(bead_id("bd-a"), stamp.clone(), note_b);
        let bytes = serialize_notes(&state).expect("serialize_notes");

        let mut lines = jsonl_lines(&bytes);
        lines.swap(0, 1);
        let bytes = join_jsonl(&lines);

        let err = parse_notes(&bytes).expect_err("out-of-order notes should fail");
        match err {
            WireError::InvalidValue(msg) => {
                assert!(msg.contains("notes.jsonl line 2"), "{msg}");
            }
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn parse_notes_rejects_duplicate_note_id() {
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor_id("alice"));
        let mut state = CanonicalState::new();
        let note = Note::new(
            NoteId::new("note-a").unwrap(),
            "note a".to_string(),
            actor_id("alice"),
            WriteStamp::new(1, 0),
        );
        state.insert_note(bead_id("bd-a"), stamp.clone(), note);
        let bytes = serialize_notes(&state).expect("serialize_notes");

        let lines = jsonl_lines(&bytes);
        let mut wire: NoteAppendV1 = serde_json::from_str(&lines[0]).expect("note json");
        wire.note.at = WireStamp(9, 0);
        let dup_line = serde_json::to_string(&wire).expect("note json");
        let bytes = join_jsonl(&vec![lines[0].clone(), dup_line]);

        let err = parse_notes(&bytes).expect_err("duplicate note id should fail");
        match err {
            WireError::InvalidValue(msg) => {
                assert!(msg.contains("notes.jsonl line 2"), "{msg}");
                assert!(msg.contains("duplicate note id"), "{msg}");
            }
            other => panic!("expected InvalidValue, got {other:?}"),
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
    fn parse_supported_meta_rejects_unsupported_version() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        value["format_version"] = serde_json::Value::from(2);
        let mutated = serde_json::to_vec(&value).expect("json");

        let err = parse_supported_meta(&mutated).expect_err("unsupported version should fail");
        assert!(matches!(err, WireError::InvalidValue(_)));
    }


    #[test]
    fn parse_supported_meta_accepts_missing_notes_checksum_for_legacy_v1() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        value
            .as_object_mut()
            .expect("object")
            .remove("notes_sha256");
        let mutated = serde_json::to_vec(&value).expect("json");

        let parsed = parse_supported_meta(&mutated).expect("legacy v1 notes checksum is optional");
        let parsed_checksums = parsed.checksums().expect("checksums");
        assert_eq!(parsed_checksums.notes, None);
    }

    #[test]
    fn parse_supported_meta_rejects_missing_required_checksums() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        value
            .as_object_mut()
            .expect("object")
            .remove("state_sha256");
        let mutated = serde_json::to_vec(&value).expect("json");

        let err =
            parse_supported_meta(&mutated).expect_err("missing required checksums should fail");
        assert!(matches!(err, WireError::InvalidValue(_)));
    }
    #[test]
    fn parse_supported_meta_rejects_invalid_root_slug() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        value["root_slug"] = serde_json::Value::from("bad slug");
        let mutated = serde_json::to_vec(&value).expect("json");

        let err = parse_supported_meta(&mutated).expect_err("invalid slug should fail");
        assert!(matches!(err, WireError::InvalidValue(_)));
    }

    #[test]
    fn parse_supported_meta_accepts_v1_with_checksums() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let parsed = parse_supported_meta(&bytes).expect("parse meta");
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
