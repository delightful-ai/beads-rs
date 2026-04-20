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
    ActorId, Bead, BeadId, BeadSlug, BeadSnapshotWireV1, CanonicalState, DepKey, DepKind, Dot, Dvv,
    IssueStatus, NoteAppendV1, OrSet, OrSetValue, SnapshotCodec, SnapshotWireV1, Stamp,
    StateJsonlSha256, WireDepEntryV1, WireDepStoreV1, WireStamp, WireTombstoneV1, WriteStamp,
};

// =============================================================================
// Wire format types (intermediate representation for JSON)
// =============================================================================

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
        let status_stamp = wire_field_stamp(&wire, "status");
        let claim_stamp = wire_field_stamp(&wire, "claim");
        let (closed_at, closed_by) = if wire.status.is_terminal() {
            (
                Some(WireStamp::from(&status_stamp.at)),
                Some(status_stamp.by.clone()),
            )
        } else {
            (None, None)
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

        let workflow_closed = self.wire.status.is_terminal();
        if workflow_closed {
            match (self.closed_at.as_ref(), self.closed_by.as_ref()) {
                (Some(closed_at), Some(closed_by)) => {
                    let workflow_stamp = wire_field_stamp(&self.wire, "status");
                    let closed_stamp = Stamp::new(wire_to_stamp(*closed_at), closed_by.clone());
                    if closed_stamp != workflow_stamp {
                        return Err(WireError::InvalidValue(
                            "closed_at/closed_by does not match status stamp".to_string(),
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
        } else if assignee_present || assignee_expires_present || self.assignee_at.is_some() {
            return Err(WireError::InvalidValue(
                "assignee_at/expires present without assignee".to_string(),
            ));
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
    state_sha256: Option<StateJsonlSha256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tombstones_sha256: Option<StateJsonlSha256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deps_sha256: Option<StateJsonlSha256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    notes_sha256: Option<StateJsonlSha256>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreChecksums {
    pub state: StateJsonlSha256,
    pub tombstones: StateJsonlSha256,
    pub deps: StateJsonlSha256,
    pub notes: Option<StateJsonlSha256>,
}

impl StoreChecksums {
    pub fn from_bytes(
        state_bytes: &[u8],
        tombs_bytes: &[u8],
        deps_bytes: &[u8],
        notes_bytes: Option<&[u8]>,
    ) -> Self {
        Self {
            state: StateJsonlSha256::from_jsonl_bytes(state_bytes),
            tombstones: StateJsonlSha256::from_jsonl_bytes(tombs_bytes),
            deps: StateJsonlSha256::from_jsonl_bytes(deps_bytes),
            notes: notes_bytes.map(StateJsonlSha256::from_jsonl_bytes),
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

    let snapshot = SnapshotCodec::from_state(state);
    for wire in snapshot.beads {
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

    let snapshot = SnapshotCodec::from_state(state);
    for wire in snapshot.tombstones {
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
    let snapshot = SnapshotCodec::from_state(state);
    let wire = snapshot.deps;

    let json = serde_json::to_string(&wire)?;
    let mut output = json;
    if !output.is_empty() {
        output.push('\n');
    }
    Ok(output.into_bytes())
}

/// Serialize notes to notes.jsonl bytes.
pub fn serialize_notes(state: &CanonicalState) -> Result<Vec<u8>, WireError> {
    let mut lines = Vec::new();
    let snapshot = SnapshotCodec::from_state(state);
    for wire in snapshot.notes {
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
        format_version: crate::core::FormatVersion::CURRENT.get(),
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
    Ok(parse_state_current_full(bytes)?
        .into_iter()
        .map(Bead::from)
        .collect())
}

fn parse_state_current_full(bytes: &[u8]) -> Result<Vec<BeadSnapshotWireV1>, WireError> {
    let content = parse_utf8(bytes)?;
    let mut beads = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let raw: serde_json::Value = serde_json::from_str(line)?;
        let compat: WireBeadFullCompat = serde_json::from_value(raw.clone())?;
        compat.validate_redundant_fields(&raw)?;
        beads.push(compat.wire);
    }

    SnapshotCodec::validate_beads(&beads).map_err(|err| map_snapshot_error("state.jsonl", err))?;
    Ok(beads)
}

fn parse_state_migration_full(bytes: &[u8]) -> Result<Vec<BeadSnapshotWireV1>, WireError> {
    let content = parse_utf8(bytes)?;
    let mut beads = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let mut raw: serde_json::Value = serde_json::from_str(line)?;
        inject_legacy_status(&mut raw)?;
        let compat: WireBeadFullCompat = serde_json::from_value(raw.clone())?;
        compat.validate_redundant_fields(&raw)?;
        beads.push(compat.wire);
    }

    SnapshotCodec::validate_beads(&beads).map_err(|err| map_snapshot_error("state.jsonl", err))?;
    Ok(beads)
}

fn inject_legacy_status(raw: &mut serde_json::Value) -> Result<(), WireError> {
    let Some(obj) = raw.as_object_mut() else {
        return Err(WireError::InvalidValue(
            "state.jsonl bead must be a JSON object".to_string(),
        ));
    };
    // Legacy v1 migration path: accept the old rich `tracker_state` field name
    // and rewrite it onto canonical `status` before snapshot decode.
    if let Some(status_value) = obj.get("tracker_state") {
        let Some(status_raw) = status_value.as_str() else {
            return Err(WireError::InvalidValue(
                "state.jsonl tracker_state must be a string".to_string(),
            ));
        };
        let status = IssueStatus::parse(status_raw).ok_or_else(|| {
            WireError::InvalidValue(format!("unknown legacy tracker_state {status_raw:?}"))
        })?;
        obj.insert("status".to_string(), serde_json::json!(status.as_str()));
        return Ok(());
    }

    let Some(status_value) = obj.get("status") else {
        return Ok(());
    };
    let Some(status_raw) = status_value.as_str() else {
        return Err(WireError::InvalidValue(
            "state.jsonl status must be a string".to_string(),
        ));
    };
    if let Some(status) = IssueStatus::parse(status_raw) {
        obj.insert("status".to_string(), serde_json::json!(status.as_str()));
        return Ok(());
    }
    let status = match status_raw.trim() {
        "open" => Some(IssueStatus::Todo),
        "in_progress" => Some(IssueStatus::InProgress),
        "closed" => IssueStatus::from_close_reason(
            obj.get("closed_reason").and_then(|value| value.as_str()),
        )
        .or(Some(IssueStatus::Done)),
        _ => None,
    }
    .ok_or_else(|| WireError::InvalidValue(format!("unknown legacy status {status_raw:?}")))?;

    obj.insert(
        "status".to_string(),
        serde_json::to_value(status).map_err(WireError::from)?,
    );
    Ok(())
}

/// Parse tombstones.jsonl bytes into wire tombstones.
pub fn parse_tombstones(bytes: &[u8]) -> Result<Vec<WireTombstoneV1>, WireError> {
    let content = parse_utf8(bytes)?;
    let mut tombs = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let wire: WireTombstoneV1 = serde_json::from_str(line)?;
        tombs.push(wire);
    }

    SnapshotCodec::validate_tombstones(&tombs)
        .map_err(|err| map_snapshot_error("tombstones.jsonl", err))?;
    Ok(tombs)
}

/// Parse deps.jsonl bytes into a wire dep store.
fn parse_deps(bytes: &[u8]) -> Result<WireDepStoreV1, WireError> {
    let content = parse_utf8(bytes)?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Ok(WireDepStoreV1::default());
    }

    let wire: WireDepStoreV1 =
        serde_json::from_str(trimmed).map_err(|err| map_json_error("deps.jsonl", err))?;
    SnapshotCodec::validate_dep_store(&wire)
        .map_err(|err| map_snapshot_error("deps.jsonl", err))?;
    Ok(wire)
}

/// Parsed deps format for migration-aware loading.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DepsFormat {
    OrSetV1,
    LegacyEdges,
}

/// Parse deps.jsonl bytes as the strict OR-Set wire shape.
///
/// This is the canonical runtime parser used by strict store loading.
pub fn parse_deps_wire(bytes: &[u8]) -> Result<WireDepStoreV1, WireError> {
    parse_deps(bytes)
}

/// Parse legacy line-per-edge deps JSONL and convert it to OR-Set deps wire.
///
/// This parser is migration-only. Runtime strict loads should continue using
/// `parse_deps_wire`.
pub fn parse_legacy_deps_edges(bytes: &[u8]) -> Result<(WireDepStoreV1, Vec<String>), WireError> {
    let content = parse_utf8(bytes)?;
    let mut warnings = Vec::new();
    let mut edge_status: BTreeMap<DepKey, LegacyEdgeStatus> = BTreeMap::new();
    let mut saw_line = false;
    let mut parsed_edge = false;

    for (idx, line) in content.lines().enumerate() {
        let line_no = idx + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        saw_line = true;
        let raw: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(err) => {
                warnings.push(legacy_deps_warning(
                    "JSON",
                    line_no,
                    format!("invalid json ({err})"),
                ));
                continue;
            }
        };
        let Some((key, deleted)) = parse_legacy_edge_line(&raw, line_no, &mut warnings) else {
            continue;
        };
        parsed_edge = true;
        let next_status = if deleted {
            LegacyEdgeStatus::Deleted
        } else {
            LegacyEdgeStatus::Active
        };
        edge_status
            .entry(key)
            .and_modify(|status| {
                if *status != next_status {
                    warnings.push(legacy_deps_warning(
                        "SHAPE",
                        line_no,
                        "conflicting active/deleted records; using last record",
                    ));
                }
                *status = next_status;
            })
            .or_insert(next_status);
    }

    if saw_line && !parsed_edge {
        return Err(WireError::InvalidValue(
            "deps.jsonl does not contain parseable legacy dependency edges".into(),
        ));
    }

    let mut active_entries: BTreeMap<DepKey, BTreeSet<Dot>> = BTreeMap::new();
    let mut deleted_dots: BTreeSet<Dot> = BTreeSet::new();
    for (key, status) in edge_status {
        let dot = legacy_dep_dot_for_key(&key);
        match status {
            LegacyEdgeStatus::Active => {
                active_entries.entry(key).or_default().insert(dot);
            }
            LegacyEdgeStatus::Deleted => {
                deleted_dots.insert(dot);
            }
        }
    }

    let (active_set, _) = OrSet::normalize_for_import(active_entries, Dvv::default());
    let mut cc = active_set.cc().clone();
    for dot in deleted_dots {
        cc.observe(dot);
    }
    cc.normalize();

    let mut entries = Vec::new();
    for key in active_set.values() {
        let mut dots: Vec<Dot> = active_set
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
        cc,
        entries,
        stamp: None,
    };

    SnapshotCodec::validate_dep_store(&wire)
        .map_err(|err| map_snapshot_error("deps.jsonl", err))?;
    Ok((wire, warnings))
}

/// Parse notes.jsonl bytes into note append entries.
pub fn parse_notes(bytes: &[u8]) -> Result<Vec<NoteAppendV1>, WireError> {
    let content = parse_utf8(bytes)?;
    let mut notes = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let wire: NoteAppendV1 = serde_json::from_str(line)?;
        notes.push(wire);
    }

    SnapshotCodec::validate_notes(&notes).map_err(|err| map_snapshot_error("notes.jsonl", err))?;
    Ok(notes)
}

/// Parse git JSONL files into a canonical state.
pub fn parse_legacy_state(
    state_bytes: &[u8],
    tombstones_bytes: &[u8],
    deps_bytes: &[u8],
    notes_bytes: &[u8],
) -> Result<CanonicalState, WireError> {
    let beads = parse_state_migration_full(state_bytes)?;
    let tombstones = parse_tombstones(tombstones_bytes)?;
    let deps = parse_deps(deps_bytes)?;
    let notes = parse_notes(notes_bytes)?;

    let snapshot = SnapshotWireV1 {
        beads,
        tombstones,
        deps,
        notes,
    };
    SnapshotCodec::into_state(snapshot)
        .map_err(|err| WireError::InvalidValue(format!("snapshot decode failed: {err}")))
}

/// Parse current git JSONL files into a canonical state.
pub fn parse_current_state(
    state_bytes: &[u8],
    tombstones_bytes: &[u8],
    deps_bytes: &[u8],
    notes_bytes: &[u8],
) -> Result<CanonicalState, WireError> {
    let beads = parse_state_current_full(state_bytes)?;
    let tombstones = parse_tombstones(tombstones_bytes)?;
    let deps = parse_deps(deps_bytes)?;
    let notes = parse_notes(notes_bytes)?;

    let snapshot = SnapshotWireV1 {
        beads,
        tombstones,
        deps,
        notes,
    };
    SnapshotCodec::into_state(snapshot)
        .map_err(|err| WireError::InvalidValue(format!("snapshot decode failed: {err}")))
}

/// Parse git JSONL files into canonical state, allowing legacy deps conversion.
///
/// This is migration-only. Runtime strict loads should continue using
/// `parse_legacy_state` (which is strict for deps).
pub fn parse_state_allow_legacy_deps(
    state_bytes: &[u8],
    tombstones_bytes: &[u8],
    deps_bytes: &[u8],
    notes_bytes: &[u8],
) -> Result<(CanonicalState, DepsFormat, Vec<String>), WireError> {
    let beads = parse_state_migration_full(state_bytes)?;
    let tombstones = parse_tombstones(tombstones_bytes)?;
    let notes = parse_notes(notes_bytes)?;
    let (deps, deps_format, warnings) = match parse_deps_wire(deps_bytes) {
        Ok(wire) => (wire, DepsFormat::OrSetV1, Vec::new()),
        Err(strict_err) => match parse_legacy_deps_edges(deps_bytes) {
            Ok((wire, warnings)) => (wire, DepsFormat::LegacyEdges, warnings),
            Err(legacy_err) => {
                return Err(WireError::InvalidValue(format!(
                    "deps.jsonl decode failed (strict: {strict_err}; legacy: {legacy_err})"
                )));
            }
        },
    };

    let snapshot = SnapshotWireV1 {
        beads,
        tombstones,
        deps,
        notes,
    };
    let state = SnapshotCodec::into_state(snapshot)
        .map_err(|err| WireError::InvalidValue(format!("snapshot decode failed: {err}")))?;
    Ok((state, deps_format, warnings))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LegacyEdgeStatus {
    Active,
    Deleted,
}

fn parse_legacy_edge_line(
    raw: &serde_json::Value,
    line_no: usize,
    warnings: &mut Vec<String>,
) -> Option<(DepKey, bool)> {
    let Some(obj) = raw.as_object() else {
        warnings.push(legacy_deps_warning(
            "SHAPE",
            line_no,
            format!("expected object, got {}", value_kind(raw)),
        ));
        return None;
    };

    let Some(raw_from) = read_string_field(obj, &["from", "issue_id"]) else {
        warnings.push(legacy_deps_warning(
            "SHAPE",
            line_no,
            "missing required fields (expected from/to/kind)",
        ));
        return None;
    };
    let Some(raw_to) = read_string_field(obj, &["to", "depends_on_id"]) else {
        warnings.push(legacy_deps_warning(
            "SHAPE",
            line_no,
            "missing required fields (expected from/to/kind)",
        ));
        return None;
    };
    let Some(raw_kind) = read_string_field(obj, &["kind", "type", "dep_type"]) else {
        warnings.push(legacy_deps_warning(
            "SHAPE",
            line_no,
            "missing required fields (expected from/to/kind)",
        ));
        return None;
    };

    let from = BeadId::parse(raw_from)
        .map_err(|err| {
            warnings.push(legacy_deps_warning(
                "KEY",
                line_no,
                format!("invalid from id `{raw_from}` ({err})"),
            ));
        })
        .ok();
    let to = BeadId::parse(raw_to)
        .map_err(|err| {
            warnings.push(legacy_deps_warning(
                "KEY",
                line_no,
                format!("invalid to id `{raw_to}` ({err})"),
            ));
        })
        .ok();
    let kind = DepKind::parse(raw_kind)
        .map_err(|err| {
            warnings.push(legacy_deps_warning(
                "KIND",
                line_no,
                format!("invalid kind `{raw_kind}` ({err})"),
            ));
        })
        .ok();

    let (Some(from), Some(to), Some(kind)) = (from, to, kind) else {
        return None;
    };

    let key = match DepKey::new(from, to, kind) {
        Ok(key) => key,
        Err(err) => {
            warnings.push(legacy_deps_warning(
                "KEY",
                line_no,
                format!("invalid dep key ({err})"),
            ));
            return None;
        }
    };
    let deleted = read_deleted_flag(obj);
    Some((key, deleted))
}

fn legacy_deps_warning(code: &str, line_no: usize, message: impl std::fmt::Display) -> String {
    format!("LEGACY_DEPS_{code}(line={line_no}): {message}")
}

fn read_string_field<'a>(
    obj: &'a serde_json::Map<String, serde_json::Value>,
    names: &[&str],
) -> Option<&'a str> {
    for name in names {
        let Some(value) = obj.get(*name) else {
            continue;
        };
        if let Some(raw) = value.as_str() {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                return Some(trimmed);
            }
        }
    }
    None
}

fn read_deleted_flag(obj: &serde_json::Map<String, serde_json::Value>) -> bool {
    if obj
        .get("deleted")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return true;
    }
    if obj
        .get("is_deleted")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return true;
    }
    obj.get("deleted_at").is_some_and(is_present_marker)
}

fn is_present_marker(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::String(raw) => !raw.trim().is_empty(),
        serde_json::Value::Array(values) => !values.is_empty(),
        serde_json::Value::Object(values) => !values.is_empty(),
        _ => true,
    }
}

fn value_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn legacy_dep_dot_for_key(key: &DepKey) -> Dot {
    crate::core::wire_bead::legacy_hash_dot(b"legacy-deps-v0", &key.collision_bytes())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreMeta {
    Legacy,
    V1 {
        root_slug: Option<BeadSlug>,
        last_write_stamp: Option<WriteStamp>,
        checksums: Option<StoreChecksums>,
    },
    V2 {
        root_slug: Option<BeadSlug>,
        last_write_stamp: Option<WriteStamp>,
        checksums: Option<StoreChecksums>,
    },
}

impl StoreMeta {
    pub fn root_slug(&self) -> Option<&BeadSlug> {
        match self {
            StoreMeta::Legacy => None,
            StoreMeta::V1 { root_slug, .. } => root_slug.as_ref(),
            StoreMeta::V2 { root_slug, .. } => root_slug.as_ref(),
        }
    }

    pub fn last_write_stamp(&self) -> Option<&WriteStamp> {
        match self {
            StoreMeta::Legacy => None,
            StoreMeta::V1 {
                last_write_stamp, ..
            } => last_write_stamp.as_ref(),
            StoreMeta::V2 {
                last_write_stamp, ..
            } => last_write_stamp.as_ref(),
        }
    }

    pub fn checksums(&self) -> Option<&StoreChecksums> {
        match self {
            StoreMeta::Legacy => None,
            StoreMeta::V1 { checksums, .. } => checksums.as_ref(),
            StoreMeta::V2 { checksums, .. } => checksums.as_ref(),
        }
    }

    pub fn format_version(&self) -> Option<u32> {
        match self {
            StoreMeta::Legacy => None,
            StoreMeta::V1 { .. } => Some(1),
            StoreMeta::V2 { .. } => Some(crate::core::FormatVersion::CURRENT.get()),
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
        let checksums = match meta.format_version {
            1 => match (
                meta.state_sha256,
                meta.tombstones_sha256,
                meta.deps_sha256,
                meta.notes_sha256,
            ) {
                (Some(state), Some(tombstones), Some(deps), notes) => Some(StoreChecksums {
                    state,
                    tombstones,
                    deps,
                    notes,
                }),
                (None, None, None, None) => None,
                _ => {
                    return Err(WireError::InvalidValue(
                        "meta.json has partial checksum fields".into(),
                    ));
                }
            },
            version if version == crate::core::FormatVersion::CURRENT.get() => {
                match (
                    meta.state_sha256,
                    meta.tombstones_sha256,
                    meta.deps_sha256,
                    meta.notes_sha256,
                ) {
                    (Some(state), Some(tombstones), Some(deps), Some(notes)) => {
                        Some(StoreChecksums {
                            state,
                            tombstones,
                            deps,
                            notes: Some(notes),
                        })
                    }
                    _ => {
                        return Err(WireError::InvalidValue(
                            "meta.json missing required checksum fields".into(),
                        ));
                    }
                }
            }
            _ => {
                return Err(WireError::InvalidValue(format!(
                    "unsupported meta format_version {}",
                    meta.format_version
                )));
            }
        };
        let root_slug = match meta.root_slug {
            Some(raw) => {
                Some(BeadSlug::parse(&raw).map_err(|e| WireError::InvalidValue(e.to_string()))?)
            }
            None => None,
        };
        let last_write_stamp = meta.last_write_stamp.map(wire_to_stamp);
        Ok(Self {
            meta: match meta.format_version {
                1 => StoreMeta::V1 {
                    root_slug,
                    last_write_stamp,
                    checksums,
                },
                version if version == crate::core::FormatVersion::CURRENT.get() => StoreMeta::V2 {
                    root_slug,
                    last_write_stamp,
                    checksums,
                },
                _ => unreachable!("unsupported format version handled above"),
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

pub fn parse_current_meta(bytes: &[u8]) -> Result<SupportedStoreMeta, WireError> {
    let meta = SupportedStoreMeta::parse(bytes)?;
    if meta.meta().format_version() == Some(crate::core::FormatVersion::CURRENT.get()) {
        Ok(meta)
    } else {
        Err(WireError::InvalidValue(format!(
            "unsupported meta format_version {}",
            meta.meta().format_version().unwrap_or(0)
        )))
    }
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

fn map_snapshot_error(file: &str, err: crate::core::SnapshotCodecError) -> WireError {
    WireError::InvalidValue(format!("{file}: {err}"))
}

fn map_json_error(file: &str, err: serde_json::Error) -> WireError {
    WireError::InvalidValue(format!("{file}: {err}"))
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

fn wire_field_stamp(wire: &BeadSnapshotWireV1, field: &str) -> Stamp {
    let bead_stamp = Stamp::new(wire_to_stamp(wire.at), wire.by.clone());
    if let Some(v_map) = &wire.v
        && let Some((at, by)) = v_map.get(field)
    {
        return Stamp::new(wire_to_stamp(*at), by.clone());
    }
    bead_stamp
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::crdt::Crdt;
    use crate::core::state::LabelState;
    use crate::core::{
        ActorId, BeadCore, BeadFields, BeadId, BeadType, Claim, DepKey, DepKind, Dot, Dvv,
        IssueStatus, Label, LabelStore, Lww, Note, NoteId, OrSet, ParentEdge, Priority, ReplicaId,
        Tombstone, WireDepEntryV1, WireFieldStamp, WireNoteV1,
    };
    use proptest::prelude::*;
    use sha2::{Digest, Sha256 as Sha2};
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
            status: Lww::new(IssueStatus::Todo, stamp.clone()),
            closed_on_branch: Lww::new(None, stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        Bead::new(core, fields)
    }

    fn make_bead_with(id: &BeadId, stamp: &Stamp, status: IssueStatus, claim: Claim) -> Bead {
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
            status: Lww::new(status, stamp.clone()),
            closed_on_branch: Lww::new(None, stamp.clone()),
            claim: Lww::new(claim, stamp.clone()),
        };
        Bead::new(core, fields)
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

    fn dot(replica_byte: u8, counter: u64) -> Dot {
        Dot {
            replica: ReplicaId::from(Uuid::from_bytes([replica_byte; 16])),
            counter,
        }
    }

    fn apply_dep_add_checked(state: &mut CanonicalState, key: DepKey, dot: Dot, stamp: Stamp) {
        let key = state
            .check_dep_add_key(key)
            .unwrap_or_else(|err| panic!("dep key invalid: {}", err));
        state.apply_dep_add(key, dot, stamp);
    }

    fn tombstone_from_wire(wire: WireTombstoneV1) -> Tombstone {
        let deleted = wire.deleted_stamp();
        let lineage = wire.lineage_stamp();
        match lineage {
            Some(stamp) => Tombstone::new_collision(wire.id.clone(), deleted, stamp, wire.reason),
            None => Tombstone::new(wire.id.clone(), deleted, wire.reason),
        }
    }

    fn stamp_to_field(stamp: &Stamp) -> WireFieldStamp {
        (WireStamp::from(&stamp.at), stamp.by.clone())
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
                    .unwrap_or_else(|err| panic!("dep key invalid: {}", err));
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
                    .unwrap_or_else(|e| panic!("parent edge invalid: {}", e));
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
        let workflow = IssueStatus::Done;
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
        let workflow = IssueStatus::Done;
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

        let parsed =
            parse_state(&join_jsonl(&lines)).expect("legacy closed fields remain readable");
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].fields.status.value.is_terminal());
    }

    #[test]
    fn parse_state_rejects_legacy_status_without_rich_status() {
        let stamp = Stamp::new(WriteStamp::new(5, 1), actor_id("alice"));
        let bead = make_bead_with(
            &bead_id("bd-legacy-status"),
            &stamp,
            IssueStatus::Done,
            Claim::default(),
        );
        let mut state = CanonicalState::new();
        state.insert(bead).unwrap();

        let bytes = serialize_state(&state).expect("serialize_state");
        let mut lines = jsonl_lines(&bytes);
        let mut value: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        let obj = value.as_object_mut().expect("object");
        obj.remove("status");
        obj.insert("status".to_string(), serde_json::json!("closed"));
        obj.insert(
            "closed_reason".to_string(),
            serde_json::json!("duplicate cleanup"),
        );
        lines[0] = serde_json::to_string(&value).unwrap();

        let err = parse_state(&join_jsonl(&lines))
            .expect_err("strict state parser should reject legacy status rows");
        assert!(matches!(
            err,
            WireError::Json(_) | WireError::InvalidValue(_)
        ));
    }

    #[test]
    fn parse_legacy_state_accepts_legacy_status_without_rich_status() {
        let stamp = Stamp::new(WriteStamp::new(5, 1), actor_id("alice"));
        let bead = make_bead_with(
            &bead_id("bd-legacy-status"),
            &stamp,
            IssueStatus::Done,
            Claim::default(),
        );
        let mut state = CanonicalState::new();
        state.insert(bead).unwrap();

        let state_bytes = serialize_state(&state).expect("serialize_state");
        let tomb_bytes = serialize_tombstones(&state).expect("serialize_tombstones");
        let deps_bytes = serialize_deps(&state).expect("serialize_deps");
        let notes_bytes = serialize_notes(&state).expect("serialize_notes");

        let mut lines = jsonl_lines(&state_bytes);
        let mut value: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        let obj = value.as_object_mut().expect("object");
        obj.remove("status");
        obj.insert("status".to_string(), serde_json::json!("closed"));
        obj.insert(
            "closed_reason".to_string(),
            serde_json::json!("duplicate cleanup"),
        );
        lines[0] = serde_json::to_string(&value).unwrap();

        let parsed =
            parse_legacy_state(&join_jsonl(&lines), &tomb_bytes, &deps_bytes, &notes_bytes)
                .expect("migration parser should accept legacy status rows");
        let bead = parsed
            .bead_view(&bead_id("bd-legacy-status"))
            .expect("bead view");
        assert!(bead.bead.fields.status.value.is_terminal());
    }

    #[test]
    fn parse_state_accepts_legacy_assignee_without_assignee_at() {
        let stamp = Stamp::new(WriteStamp::new(5, 1), actor_id("alice"));
        let claim = Claim::claimed(actor_id("bob"), None);
        let bead = make_bead_with(&bead_id("bd-claimed"), &stamp, IssueStatus::Todo, claim);
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
    fn parse_state_accepts_legacy_labels_array() {
        let stamp = Stamp::new(WriteStamp::new(5, 1), actor_id("alice"));
        let id = bead_id("bd-legacy-labels");
        let mut state = CanonicalState::new();
        state.insert(make_bead(&id, &stamp)).unwrap();
        let label_value = "legacy".to_string();
        let label = Label::parse(&label_value).unwrap();
        state.apply_label_add(id.clone(), label, dot(1, 1), stamp.clone(), stamp.clone());

        let state_bytes = serialize_state(&state).expect("serialize_state");
        let tomb_bytes = serialize_tombstones(&state).expect("serialize_tombstones");
        let deps_bytes = serialize_deps(&state).expect("serialize_deps");
        let notes_bytes = serialize_notes(&state).expect("serialize_notes");

        let mut lines = jsonl_lines(&state_bytes);
        let mut value: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        let obj = value.as_object_mut().unwrap();
        obj.insert(
            "labels".to_string(),
            serde_json::json!([label_value.clone()]),
        );
        lines[0] = serde_json::to_string(&value).unwrap();

        let parsed =
            parse_legacy_state(&join_jsonl(&lines), &tomb_bytes, &deps_bytes, &notes_bytes)
                .expect("legacy labels array should parse");

        assert!(parsed.labels_for(&id).contains(label_value.as_str()));
    }

    #[test]
    fn parse_state_rejects_partial_closed_redundant_fields() {
        let stamp = Stamp::new(WriteStamp::new(5, 1), actor_id("alice"));
        let workflow = IssueStatus::Done;
        let bead = make_bead_with(
            &bead_id("bd-partial-closed"),
            &stamp,
            workflow,
            Claim::default(),
        );
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
        let workflow = IssueStatus::Done;
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

        // Rejected at the serde level (WireClaimSnapshot deserializer) before
        // validate_redundant_fields gets a chance, so the error is Json not InvalidValue.
        let err = parse_state(&join_jsonl(&lines)).expect_err("expected invalid claim data");
        assert!(
            err.to_string()
                .contains("assignee_expires requires assignee"),
            "unexpected error: {err}"
        );
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
            status: Lww::new(IssueStatus::Todo, base.clone()),
            closed_on_branch: Lww::new(None, base.clone()),
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
            .unwrap_or_else(|e| panic!("parent edge invalid: {}", e))
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
        let parsed_wire = parse_deps(&bytes).expect("parse_deps");
        let parsed =
            SnapshotCodec::dep_store_from_wire(parsed_wire).expect("parse dep store from wire");

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
            .unwrap_or_else(|e| panic!("parent edge invalid: {}", e))
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

        let parsed_wire = parse_deps(&bytes).expect("parse legacy deps");
        let parsed =
            SnapshotCodec::dep_store_from_wire(parsed_wire).expect("parse dep store from wire");
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

    #[test]
    fn parse_legacy_deps_edges_supports_known_shapes_and_deletes() {
        let bytes = br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
{"issue_id":"bd-c","depends_on_id":"bd-d","type":"related","deleted_at":"2026-01-02T00:00:00Z"}
"#;
        let (wire, warnings) = parse_legacy_deps_edges(bytes).expect("legacy deps parse");
        assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
        assert_eq!(wire.entries.len(), 1, "deleted edge should not be active");

        let active_key = DepKey::new(bead_id("bd-a"), bead_id("bd-b"), DepKind::Blocks).unwrap();
        assert_eq!(wire.entries[0].key, active_key);
        assert_eq!(wire.entries[0].dots.len(), 1);

        let deleted_key = DepKey::new(bead_id("bd-c"), bead_id("bd-d"), DepKind::Related).unwrap();
        let deleted_dot = legacy_dep_dot_for_key(&deleted_key);
        assert!(
            wire.cc.dominates(&deleted_dot),
            "deleted edge dot should be represented in cc"
        );
    }

    #[test]
    fn parse_legacy_deps_edges_conflict_uses_last_record() {
        let bytes = br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
{"from":"bd-a","to":"bd-b","kind":"blocks","deleted_at":"2026-01-02T00:00:00Z"}
"#;
        let (wire, warnings) = parse_legacy_deps_edges(bytes).expect("legacy deps parse");
        assert_eq!(wire.entries.len(), 0, "last deleted record should win");
        assert_eq!(warnings.len(), 1, "expected one conflict warning");
        assert_eq!(
            warnings[0],
            "LEGACY_DEPS_SHAPE(line=2): conflicting active/deleted records; using last record"
        );

        let key = DepKey::new(bead_id("bd-a"), bead_id("bd-b"), DepKind::Blocks).unwrap();
        let deleted_dot = legacy_dep_dot_for_key(&key);
        assert!(
            wire.cc.dominates(&deleted_dot),
            "deleted edge dot should be represented in cc when last record is deleted"
        );
    }

    #[test]
    fn migrated_legacy_active_and_deleted_join_preserves_deletion() {
        let active_bytes = br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
"#;
        let deleted_bytes =
            br#"{"from":"bd-a","to":"bd-b","kind":"blocks","deleted_at":"2026-01-02T00:00:00Z"}
"#;

        let (active_wire, active_warnings) =
            parse_legacy_deps_edges(active_bytes).expect("active legacy parse");
        assert!(
            active_warnings.is_empty(),
            "unexpected warnings: {active_warnings:?}"
        );
        let (deleted_wire, deleted_warnings) =
            parse_legacy_deps_edges(deleted_bytes).expect("deleted legacy parse");
        assert!(
            deleted_warnings.is_empty(),
            "unexpected warnings: {deleted_warnings:?}"
        );

        let active_store = SnapshotCodec::dep_store_from_wire(active_wire).expect("active store");
        let deleted_store =
            SnapshotCodec::dep_store_from_wire(deleted_wire).expect("deleted store");

        let mut active_state = CanonicalState::new();
        active_state.set_dep_store(active_store);
        let mut deleted_state = CanonicalState::new();
        deleted_state.set_dep_store(deleted_store);

        let key = DepKey::new(bead_id("bd-a"), bead_id("bd-b"), DepKind::Blocks).unwrap();
        let merged = active_state.join(&deleted_state);
        assert!(
            !merged.dep_store().contains(&key),
            "deleted side should win when add dot is dominated by merged context"
        );
        let reverse = deleted_state.join(&active_state);
        assert!(
            !reverse.dep_store().contains(&key),
            "deleted side should also win in reverse join order"
        );
    }

    #[test]
    fn parse_state_allow_legacy_deps_migrates_legacy_edges_while_strict_rejects() {
        let state_bytes = b"";
        let tomb_bytes = b"";
        let notes_bytes = b"";
        let deps_bytes = br#"{"from":"bd-a","to":"bd-b","kind":"block"}
"#;

        let strict_err = parse_legacy_state(state_bytes, tomb_bytes, deps_bytes, notes_bytes)
            .expect_err("strict parse must reject legacy");
        match strict_err {
            WireError::Json(_) => {}
            WireError::InvalidValue(msg) => {
                assert!(
                    msg.contains("deps.jsonl"),
                    "strict parse should fail on deps decoding: {msg}"
                );
            }
            other => panic!("unexpected strict error: {other:?}"),
        }

        let (state, deps_format, warnings) =
            parse_state_allow_legacy_deps(state_bytes, tomb_bytes, deps_bytes, notes_bytes)
                .expect("migration parse should accept legacy deps");
        assert_eq!(deps_format, DepsFormat::LegacyEdges);
        assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
        let deps = state.dep_store();
        assert_eq!(deps.values().count(), 1);
        let key = DepKey::new(bead_id("bd-a"), bead_id("bd-b"), DepKind::Blocks).unwrap();
        assert!(deps.contains(&key));
    }

    #[test]
    fn parse_legacy_deps_edges_errors_when_no_valid_lines() {
        let bytes = br#"{"garbage":true}
"#;
        let err = parse_legacy_deps_edges(bytes).expect_err("should reject unusable legacy deps");
        match err {
            WireError::InvalidValue(msg) => {
                assert!(msg.contains("parseable legacy"), "{msg}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_legacy_deps_edges_emits_stable_warning_prefixes() {
        let bytes = br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
{"from":
[]
{"from":"bd-a","to":"bd-b","kind":"blocked-by"}
{"from":"bd-a","to":"bd-a","kind":"blocks"}
{"garbage":true}
"#;

        let (_wire, warnings) = parse_legacy_deps_edges(bytes).expect("legacy deps parse");
        assert_eq!(warnings.len(), 5, "unexpected warnings: {warnings:?}");
        assert!(
            warnings[0].starts_with("LEGACY_DEPS_JSON(line=2):"),
            "unexpected warning text: {:?}",
            warnings
        );
        assert!(
            warnings[1].starts_with("LEGACY_DEPS_SHAPE(line=3):"),
            "unexpected warning text: {:?}",
            warnings
        );
        assert!(
            warnings[2].starts_with("LEGACY_DEPS_KIND(line=4):"),
            "unexpected warning text: {:?}",
            warnings
        );
        assert!(
            warnings[3].starts_with("LEGACY_DEPS_KEY(line=5):"),
            "unexpected warning text: {:?}",
            warnings
        );
        assert!(
            warnings[4].starts_with("LEGACY_DEPS_SHAPE(line=6):"),
            "unexpected warning text: {:?}",
            warnings
        );
    }

    #[test]
    fn legacy_dep_dot_for_key_matches_legacy_namespace_seed_order() {
        let key = DepKey::new(bead_id("bd-a"), bead_id("bd-b"), DepKind::Blocks).unwrap();

        let dot = legacy_dep_dot_for_key(&key);

        let mut hasher = Sha2::new();
        hasher.update(b"legacy-deps-v0");
        hasher.update(key.collision_bytes());
        let digest = hasher.finalize();

        let mut replica_bytes = [0u8; 16];
        replica_bytes.copy_from_slice(&digest[..16]);
        let mut counter_bytes = [0u8; 8];
        counter_bytes.copy_from_slice(&digest[16..24]);

        let expected = Dot {
            replica: ReplicaId::from(Uuid::from_bytes(replica_bytes)),
            counter: u64::from_le_bytes(counter_bytes),
        };
        assert_eq!(dot, expected);
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
                rebuilt.delete(tombstone_from_wire(tomb));
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
            let parsed_wire = parse_deps(&bytes).unwrap_or_else(|e| panic!("parse_deps failed: {e}"));
            let parsed =
                SnapshotCodec::dep_store_from_wire(parsed_wire).unwrap_or_else(|e| panic!("dep store decode failed: {e}"));
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
                StoreMeta::V2 {
                    root_slug,
                    last_write_stamp,
                    checksums: parsed_checksums,
                } => {
                    prop_assert_eq!(root_slug, &expected_root);
                    prop_assert_eq!(last_write_stamp, &write_stamp);
                    prop_assert_eq!(parsed_checksums.as_ref(), Some(&checksums));
                }
                StoreMeta::Legacy | StoreMeta::V1 { .. } => prop_assert!(false, "expected v2 meta"),
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
                assert!(msg.contains("state.jsonl"), "{msg}");
                assert!(msg.contains("line 2"), "{msg}");
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
                assert!(msg.contains("state.jsonl"), "{msg}");
                assert!(msg.contains("line 2"), "{msg}");
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
                assert!(msg.contains("tombstones.jsonl"), "{msg}");
                assert!(msg.contains("line 2"), "{msg}");
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
                assert!(msg.contains("tombstones.jsonl"), "{msg}");
                assert!(msg.contains("line 2"), "{msg}");
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
                assert!(msg.contains("deps.jsonl"), "{msg}");
                assert!(msg.contains("line 2"), "{msg}");
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
                assert!(msg.contains("deps.jsonl"), "{msg}");
                assert!(msg.contains("line 2"), "{msg}");
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
                assert!(msg.contains("notes.jsonl"), "{msg}");
                assert!(msg.contains("line 2"), "{msg}");
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
                assert!(msg.contains("notes.jsonl"), "{msg}");
                assert!(msg.contains("line 2"), "{msg}");
                assert!(msg.contains("duplicate"), "{msg}");
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
        value["format_version"] = serde_json::Value::from(3);
        let mutated = serde_json::to_vec(&value).expect("json");

        let err = parse_supported_meta(&mutated).expect_err("unsupported version should fail");
        assert!(matches!(err, WireError::InvalidValue(_)));
    }

    #[test]
    fn parse_supported_meta_accepts_missing_notes_checksum_for_legacy_v1() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        value["format_version"] = serde_json::Value::from(1);
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
    fn parse_supported_meta_accepts_v1_without_checksums() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        value["format_version"] = serde_json::Value::from(1);
        let obj = value.as_object_mut().expect("object");
        obj.remove("state_sha256");
        obj.remove("tombstones_sha256");
        obj.remove("deps_sha256");
        obj.remove("notes_sha256");
        let mutated = serde_json::to_vec(&value).expect("json");

        let parsed = parse_supported_meta(&mutated).expect("v1 without checksums should parse");
        assert!(parsed.checksums().is_none());
    }

    #[test]
    fn parse_supported_meta_rejects_partial_required_checksums() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        value
            .as_object_mut()
            .expect("object")
            .remove("state_sha256");
        let mutated = serde_json::to_vec(&value).expect("json");

        let err =
            parse_supported_meta(&mutated).expect_err("partial required checksums should fail");
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
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        value["format_version"] = serde_json::Value::from(1);
        let mutated = serde_json::to_vec(&value).expect("json");
        let parsed = parse_supported_meta(&mutated).expect("parse meta");
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
                assert_eq!(parsed_checksums.as_ref(), Some(&checksums));
            }
            StoreMeta::Legacy | StoreMeta::V2 { .. } => panic!("expected v1 meta"),
        }
    }

    #[test]
    fn parse_supported_meta_accepts_v2_with_checksums() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let parsed = parse_supported_meta(&bytes).expect("parse meta");
        match parsed.meta() {
            StoreMeta::V2 {
                root_slug,
                last_write_stamp,
                checksums: parsed_checksums,
            } => {
                assert_eq!(
                    root_slug,
                    &Some(BeadSlug::parse("valid-slug").expect("slug"))
                );
                assert_eq!(last_write_stamp, &None);
                assert_eq!(parsed_checksums.as_ref(), Some(&checksums));
            }
            StoreMeta::Legacy | StoreMeta::V1 { .. } => panic!("expected v2 meta"),
        }
    }

    #[test]
    fn parse_current_meta_rejects_legacy_v1() {
        let checksums = StoreChecksums::from_bytes(b"state", b"tombs", b"deps", Some(b"notes"));
        let bytes = serialize_meta(Some("valid-slug"), None, &checksums).expect("meta bytes");
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        value["format_version"] = serde_json::Value::from(1);
        let mutated = serde_json::to_vec(&value).expect("json");

        let err = parse_current_meta(&mutated).expect_err("strict current meta should reject v1");
        assert!(matches!(err, WireError::InvalidValue(_)));
    }
}
