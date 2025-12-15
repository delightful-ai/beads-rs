//! Wire format serialization for git storage.
//!
//! Per SPEC §5.2.1, uses sparse _v representation:
//! - Each bead has top-level `_at` and `_by` (bead-level stamp)
//! - `_v` object maps field names to stamps only when they differ from bead-level
//! - If all fields share bead-level stamp, `_v` is omitted

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::error::WireError;
use crate::core::{
    ActorId, Bead, BeadCore, BeadFields, BeadId, BeadType, CanonicalState, Claim, Closure, DepEdge,
    DepKey, DepKind, Label, Labels, Lww, Priority, Stamp, Tombstone, WallClock, Workflow,
    WriteStamp,
};

// =============================================================================
// Wire format types (intermediate representation for JSON)
// =============================================================================

/// Write stamp as array: [wall_ms, counter]
type WireStamp = (u64, u32);

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
    labels: Vec<String>,
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

    // Notes
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    notes: Vec<WireNote>,

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
struct WireDep {
    from: String,
    to: String,
    kind: String,
    created_at: WireStamp,
    created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_at: Option<WireStamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_by: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct WireMeta {
    format_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    root_slug: Option<String>,
}

// =============================================================================
// Serialization
// =============================================================================

/// Serialize state to state.jsonl bytes.
///
/// Sorted by bead ID, one JSON object per line.
pub fn serialize_state(state: &CanonicalState) -> Vec<u8> {
    let mut lines = Vec::new();

    // Sort by ID
    let mut beads: Vec<_> = state.iter_live().collect();
    beads.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (_, bead) in beads {
        let wire = bead_to_wire(bead);
        let json = serde_json::to_string(&wire).expect("bead serialization failed");
        lines.push(json);
    }

    let mut output = lines.join("\n");
    if !output.is_empty() {
        output.push('\n');
    }
    output.into_bytes()
}

/// Serialize tombstones to tombstones.jsonl bytes.
pub fn serialize_tombstones(state: &CanonicalState) -> Vec<u8> {
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
        let json = serde_json::to_string(&wire).expect("tombstone serialization failed");
        lines.push(json);
    }

    let mut output = lines.join("\n");
    if !output.is_empty() {
        output.push('\n');
    }
    output.into_bytes()
}

/// Serialize deps to deps.jsonl bytes.
pub fn serialize_deps(state: &CanonicalState) -> Vec<u8> {
    let mut lines = Vec::new();

    let mut deps: Vec<_> = state.iter_deps().collect();
    deps.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (_, edge) in deps {
        let wire = WireDep {
            from: edge.key.from().as_str().to_string(),
            to: edge.key.to().as_str().to_string(),
            kind: dep_kind_to_str(edge.key.kind()),
            created_at: stamp_to_wire(&edge.created.at),
            created_by: edge.created.by.as_str().to_string(),
            deleted_at: edge.deleted_stamp().map(|s| stamp_to_wire(&s.at)),
            deleted_by: edge.deleted_stamp().map(|s| s.by.as_str().to_string()),
        };
        let json = serde_json::to_string(&wire).expect("dep serialization failed");
        lines.push(json);
    }

    let mut output = lines.join("\n");
    if !output.is_empty() {
        output.push('\n');
    }
    output.into_bytes()
}

/// Serialize meta.json bytes.
pub fn serialize_meta(root_slug: Option<&str>) -> Vec<u8> {
    let meta = WireMeta {
        format_version: 1,
        root_slug: root_slug.map(|s| s.to_string()),
    };
    let json = serde_json::to_string_pretty(&meta).expect("meta serialization failed");
    json.into_bytes()
}

// =============================================================================
// Deserialization
// =============================================================================

/// Parse state.jsonl bytes into beads.
pub fn parse_state(bytes: &[u8]) -> Result<Vec<Bead>, WireError> {
    let content = String::from_utf8(bytes.to_vec())?;
    let mut beads = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let wire: WireBead = serde_json::from_str(line)?;
        let bead = wire_to_bead(wire)?;
        beads.push(bead);
    }

    Ok(beads)
}

/// Parse tombstones.jsonl bytes.
pub fn parse_tombstones(bytes: &[u8]) -> Result<Vec<Tombstone>, WireError> {
    let content = String::from_utf8(bytes.to_vec())?;
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
pub fn parse_deps(bytes: &[u8]) -> Result<Vec<DepEdge>, WireError> {
    let content = String::from_utf8(bytes.to_vec())?;
    let mut deps = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let wire: WireDep = serde_json::from_str(line)?;
        let key = DepKey::new(
            BeadId::parse(&wire.from).map_err(|e| WireError::InvalidValue(e.to_string()))?,
            BeadId::parse(&wire.to).map_err(|e| WireError::InvalidValue(e.to_string()))?,
            str_to_dep_kind(&wire.kind)?,
        )
        .map_err(|e| WireError::InvalidValue(e.reason))?;
        let created = Stamp::new(
            wire_to_stamp(wire.created_at),
            ActorId::new(wire.created_by).map_err(|e| WireError::InvalidValue(e.to_string()))?,
        );
        let deleted = match (wire.deleted_at, wire.deleted_by) {
            (Some(at), Some(by)) => Some(Stamp::new(
                wire_to_stamp(at),
                ActorId::new(by).map_err(|e| WireError::InvalidValue(e.to_string()))?,
            )),
            _ => None,
        };

        let mut edge = DepEdge::new(key, created);
        if let Some(del) = deleted {
            edge.delete(del);
        }
        deps.push(edge);
    }

    Ok(deps)
}

/// Parsed metadata from meta.json.
pub struct ParsedMeta {
    pub format_version: u32,
    pub root_slug: Option<String>,
}

/// Parse meta.json bytes.
pub fn parse_meta(bytes: &[u8]) -> Result<ParsedMeta, WireError> {
    let content = String::from_utf8(bytes.to_vec())?;
    let meta: WireMeta = serde_json::from_str(&content)?;
    Ok(ParsedMeta {
        format_version: meta.format_version,
        root_slug: meta.root_slug,
    })
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

fn dep_kind_to_str(kind: DepKind) -> String {
    kind.as_str().to_string()
}

fn str_to_dep_kind(s: &str) -> Result<DepKind, WireError> {
    match s {
        "blocks" => Ok(DepKind::Blocks),
        "parent" => Ok(DepKind::Parent),
        "related" => Ok(DepKind::Related),
        "discovered_from" => Ok(DepKind::DiscoveredFrom),
        _ => Err(WireError::InvalidValue(format!("unknown dep kind: {}", s))),
    }
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

/// Convert Bead to wire format with sparse _v.
fn bead_to_wire(bead: &Bead) -> WireBead {
    // Find bead-level stamp (max of all field stamps)
    let bead_stamp = bead.updated_stamp();

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
    check_field!(bead.fields.labels, "labels");
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

    // Convert notes
    let notes: Vec<WireNote> = bead
        .notes
        .sorted()
        .iter()
        .map(|n| WireNote {
            id: n.id.as_str().to_string(),
            content: n.content.clone(),
            author: n.author.as_str().to_string(),
            at: (n.at.wall_ms, n.at.counter),
        })
        .collect();

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
        labels: bead
            .fields
            .labels
            .value
            .iter()
            .map(|l| l.as_str().to_string())
            .collect(),
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
        notes,
        at: stamp_to_wire(&bead_stamp.at),
        by: bead_stamp.by.as_str().to_string(),
        v: if v_map.is_empty() { None } else { Some(v_map) },
    }
}

/// Convert wire format to Bead, handling sparse _v.
fn wire_to_bead(wire: WireBead) -> Result<Bead, WireError> {
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
        labels: Lww::new(
            wire.labels
                .into_iter()
                .map(Label::parse)
                .collect::<std::result::Result<Labels, _>>()
                .map_err(|e| WireError::InvalidValue(e.to_string()))?,
            get_stamp("labels")?,
        ),
        external_ref: Lww::new(wire.external_ref, get_stamp("external_ref")?),
        source_repo: Lww::new(wire.source_repo, get_stamp("source_repo")?),
        estimated_minutes: Lww::new(wire.estimated_minutes, get_stamp("estimated_minutes")?),
        workflow: Lww::new(workflow_value, get_stamp("workflow")?),
        claim: Lww::new(claim_value, get_stamp("claim")?),
    };

    let mut bead = Bead::new(core, fields);

    // Parse notes
    for wire_note in wire.notes {
        use crate::core::Note;
        let note = Note::new(
            crate::core::NoteId::new(wire_note.id)
                .map_err(|e| WireError::InvalidValue(e.to_string()))?,
            wire_note.content,
            ActorId::new(wire_note.author).map_err(|e| WireError::InvalidValue(e.to_string()))?,
            wire_to_stamp(wire_note.at),
        );
        bead.notes.insert(note);
    }

    Ok(bead)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty_state() {
        let state = CanonicalState::new();
        let bytes = serialize_state(&state);
        let beads = parse_state(&bytes).unwrap();
        assert!(beads.is_empty());
    }
}
