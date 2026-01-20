//! Wire types for realtime deltas and checkpoint snapshots.
//!
//! Notes rule: bead_upsert deltas should omit notes; if notes are present they
//! mean set-union only (never truncation).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::de;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use super::bead::{BeadCore, BeadFields};
use super::collections::Label;
use super::composite::{Claim, Closure, Note, Workflow};
use super::crdt::Lww;
use super::dep::DepKey;
use super::domain::{BeadType, DepKind, Priority};
use super::identity::{ActorId, BeadId, NoteId, ReplicaId};
use super::orset::{Dot, Dvv};
use super::state::LabelState;
use super::time::{Stamp, WallClock, WriteStamp};
use super::{Bead, BeadView};

/// Wire stamp encoded as [wall_ms, counter].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireStamp(pub u64, pub u32);

impl From<WriteStamp> for WireStamp {
    fn from(stamp: WriteStamp) -> Self {
        Self(stamp.wall_ms, stamp.counter)
    }
}

impl From<&WriteStamp> for WireStamp {
    fn from(stamp: &WriteStamp) -> Self {
        Self(stamp.wall_ms, stamp.counter)
    }
}

impl From<WireStamp> for WriteStamp {
    fn from(stamp: WireStamp) -> Self {
        WriteStamp::new(stamp.0, stamp.1)
    }
}

impl From<&WireStamp> for WriteStamp {
    fn from(stamp: &WireStamp) -> Self {
        WriteStamp::new(stamp.0, stamp.1)
    }
}

/// Note wire representation (used in note_append).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireNoteV1 {
    pub id: NoteId,
    pub content: String,
    pub author: ActorId,
    pub at: WireStamp,
}

impl From<&Note> for WireNoteV1 {
    fn from(note: &Note) -> Self {
        Self {
            id: note.id.clone(),
            content: note.content.clone(),
            author: note.author.clone(),
            at: WireStamp::from(&note.at),
        }
    }
}

impl From<Note> for WireNoteV1 {
    fn from(note: Note) -> Self {
        Self {
            id: note.id,
            content: note.content,
            author: note.author,
            at: WireStamp::from(note.at),
        }
    }
}

impl From<WireNoteV1> for Note {
    fn from(note: WireNoteV1) -> Self {
        Note::new(
            note.id,
            note.content,
            note.author,
            WriteStamp::from(note.at),
        )
    }
}

/// Three-way patch for nullable fields: keep, clear, set.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum WirePatch<T> {
    #[default]
    Keep,
    Clear,
    Set(T),
}

impl<T> WirePatch<T> {
    pub fn is_keep(&self) -> bool {
        matches!(self, WirePatch::Keep)
    }
}

impl<T: Serialize> Serialize for WirePatch<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            WirePatch::Keep => serializer.serialize_none(),
            WirePatch::Clear => serializer.serialize_none(),
            WirePatch::Set(value) => value.serialize(serializer),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for WirePatch<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<T>::deserialize(deserializer)?;
        Ok(match opt {
            None => WirePatch::Clear,
            Some(value) => WirePatch::Set(value),
        })
    }
}

/// Wire Dot for OR-Set ops.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireDotV1 {
    pub replica: ReplicaId,
    pub counter: u64,
}

impl From<Dot> for WireDotV1 {
    fn from(dot: Dot) -> Self {
        Self {
            replica: dot.replica,
            counter: dot.counter,
        }
    }
}

impl From<&Dot> for WireDotV1 {
    fn from(dot: &Dot) -> Self {
        Self {
            replica: dot.replica,
            counter: dot.counter,
        }
    }
}

impl From<WireDotV1> for Dot {
    fn from(dot: WireDotV1) -> Self {
        Self {
            replica: dot.replica,
            counter: dot.counter,
        }
    }
}

impl From<&WireDotV1> for Dot {
    fn from(dot: &WireDotV1) -> Self {
        Self {
            replica: dot.replica,
            counter: dot.counter,
        }
    }
}

/// Wire DVV for OR-Set ops.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireDvvV1 {
    pub max: BTreeMap<ReplicaId, u64>,
}

impl From<&Dvv> for WireDvvV1 {
    fn from(dvv: &Dvv) -> Self {
        Self {
            max: dvv.max.clone(),
        }
    }
}

impl From<Dvv> for WireDvvV1 {
    fn from(dvv: Dvv) -> Self {
        Self { max: dvv.max }
    }
}

impl From<WireDvvV1> for Dvv {
    fn from(dvv: WireDvvV1) -> Self {
        Self { max: dvv.max }
    }
}

impl From<&WireDvvV1> for Dvv {
    fn from(dvv: &WireDvvV1) -> Self {
        Self {
            max: dvv.max.clone(),
        }
    }
}

/// Wire workflow status (matches legacy wire values).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Open,
    InProgress,
    Closed,
}

crate::enum_str! {
    impl WorkflowStatus {
        pub fn as_str(&self) -> &'static str;
        fn parse_str(raw: &str) -> Option<Self>;
        variants {
            Open => ["open"],
            InProgress => ["in_progress"],
            Closed => ["closed"],
        }
    }
}

impl WorkflowStatus {
    pub fn from_workflow(workflow: &Workflow) -> Self {
        match workflow {
            Workflow::Open => WorkflowStatus::Open,
            Workflow::InProgress => WorkflowStatus::InProgress,
            Workflow::Closed(_) => WorkflowStatus::Closed,
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        Self::parse_str(raw)
    }

    pub fn into_workflow(
        self,
        closed_reason: Option<String>,
        closed_on_branch: Option<String>,
    ) -> Workflow {
        match self {
            WorkflowStatus::Open => Workflow::Open,
            WorkflowStatus::InProgress => Workflow::InProgress,
            WorkflowStatus::Closed => {
                Workflow::Closed(Closure::new(closed_reason, closed_on_branch))
            }
        }
    }
}

/// Field-level stamp map entry: (at, by).
pub type WireFieldStamp = (WireStamp, ActorId);

/// OR-Set label state snapshot for checkpoints.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireLabelStateV1 {
    pub entries: BTreeMap<Label, BTreeSet<Dot>>,
    pub cc: Dvv,
}

/// OR-Set dep entry snapshot for checkpoints.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireDepEntryV1 {
    pub key: DepKey,
    pub dots: Vec<Dot>,
}

/// OR-Set dep store snapshot for checkpoints.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireDepStoreV1 {
    pub cc: Dvv,
    pub entries: Vec<WireDepEntryV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stamp: Option<WireFieldStamp>,
}

/// Full bead wire representation (checkpoint snapshots).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireBeadFull {
    // Core (immutable)
    pub id: BeadId,
    pub created_at: WireStamp,
    pub created_by: ActorId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_on_branch: Option<String>,

    // Fields (mutable)
    pub title: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub design: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acceptance_criteria: Option<String>,
    pub priority: Priority,
    #[serde(rename = "type")]
    pub bead_type: BeadType,
    pub labels: WireLabelStateV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_repo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_minutes: Option<u32>,

    // Workflow state
    pub status: WorkflowStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_at: Option<WireStamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_by: Option<ActorId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_on_branch: Option<String>,

    // Claim
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignee: Option<ActorId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignee_at: Option<WireStamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignee_expires: Option<WallClock>,

    // Notes
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<WireNoteV1>,

    // Version metadata (sparse)
    #[serde(rename = "_at")]
    pub at: WireStamp,
    #[serde(rename = "_by")]
    pub by: ActorId,
    #[serde(rename = "_v", skip_serializing_if = "Option::is_none")]
    pub v: Option<BTreeMap<String, WireFieldStamp>>,
}

fn label_state_to_wire(state: Option<&LabelState>) -> WireLabelStateV1 {
    let mut entries: BTreeMap<Label, BTreeSet<Dot>> = BTreeMap::new();
    let cc = state.map(|state| state.cc().clone()).unwrap_or_default();

    if let Some(state) = state {
        for label in state.values() {
            if let Some(dots) = state.dots_for(label) {
                entries.insert(label.clone(), dots.clone());
            }
        }
    }

    WireLabelStateV1 { entries, cc }
}

impl WireBeadFull {
    pub fn from_view(view: &BeadView, label_state: Option<&LabelState>) -> Self {
        let bead = &view.bead;
        let bead_stamp = view.updated_stamp().clone();

        let mut v_map: BTreeMap<String, WireFieldStamp> = BTreeMap::new();
        macro_rules! check_field {
            ($field:expr, $name:expr) => {
                if $field.stamp != bead_stamp {
                    v_map.insert(
                        $name.to_string(),
                        (WireStamp::from(&$field.stamp.at), $field.stamp.by.clone()),
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
                (WireStamp::from(&label_stamp.at), label_stamp.by.clone()),
            );
        }
        check_field!(bead.fields.external_ref, "external_ref");
        check_field!(bead.fields.source_repo, "source_repo");
        check_field!(bead.fields.estimated_minutes, "estimated_minutes");
        check_field!(bead.fields.workflow, "workflow");
        check_field!(bead.fields.claim, "claim");

        let (closed_at, closed_by, closed_reason, closed_on_branch) =
            if let Workflow::Closed(ref closure) = bead.fields.workflow.value {
                (
                    Some(WireStamp::from(&bead.fields.workflow.stamp.at)),
                    Some(bead.fields.workflow.stamp.by.clone()),
                    closure.reason.clone(),
                    closure.on_branch.clone(),
                )
            } else {
                (None, None, None, None)
            };

        let (assignee, assignee_at, assignee_expires) =
            if let Claim::Claimed { assignee, expires } = &bead.fields.claim.value {
                (
                    Some(assignee.clone()),
                    Some(WireStamp::from(&bead.fields.claim.stamp.at)),
                    *expires,
                )
            } else {
                (None, None, None)
            };

        let mut notes = view.notes.clone();
        notes.sort_by(|a, b| a.at.cmp(&b.at).then_with(|| a.id.cmp(&b.id)));
        let notes = notes.into_iter().map(WireNoteV1::from).collect();

        let labels = label_state_to_wire(label_state);

        WireBeadFull {
            id: bead.core.id.clone(),
            created_at: WireStamp::from(&bead.core.created().at),
            created_by: bead.core.created().by.clone(),
            created_on_branch: bead.core.created_on_branch().map(|s| s.to_string()),
            title: bead.fields.title.value.clone(),
            description: bead.fields.description.value.clone(),
            design: bead.fields.design.value.clone(),
            acceptance_criteria: bead.fields.acceptance_criteria.value.clone(),
            priority: bead.fields.priority.value,
            bead_type: bead.fields.bead_type.value,
            labels,
            external_ref: bead.fields.external_ref.value.clone(),
            source_repo: bead.fields.source_repo.value.clone(),
            estimated_minutes: bead.fields.estimated_minutes.value,
            status: WorkflowStatus::from_workflow(&bead.fields.workflow.value),
            closed_at,
            closed_by,
            closed_reason,
            closed_on_branch,
            assignee,
            assignee_at,
            assignee_expires,
            notes,
            at: WireStamp::from(&bead_stamp.at),
            by: bead_stamp.by.clone(),
            v: if v_map.is_empty() { None } else { Some(v_map) },
        }
    }
}

impl WireBeadFull {
    pub fn label_stamp(&self) -> Stamp {
        let bead_stamp = Stamp::new(WriteStamp::from(self.at), self.by.clone());
        if let Some(v_map) = &self.v
            && let Some((at, by)) = v_map.get("labels")
        {
            return Stamp::new(WriteStamp::from(at), by.clone());
        }
        bead_stamp
    }
}

impl From<WireBeadFull> for Bead {
    fn from(wire: WireBeadFull) -> Self {
        let bead_stamp = Stamp::new(WriteStamp::from(wire.at), wire.by.clone());
        let field_stamp = |field: &str| -> Stamp {
            if let Some(ref v_map) = wire.v
                && let Some((at, by)) = v_map.get(field)
            {
                return Stamp::new(WriteStamp::from(at), by.clone());
            }
            bead_stamp.clone()
        };

        let core = BeadCore::new(
            wire.id,
            Stamp::new(WriteStamp::from(wire.created_at), wire.created_by),
            wire.created_on_branch,
        );

        let workflow_value = wire
            .status
            .into_workflow(wire.closed_reason, wire.closed_on_branch);

        let claim_value = match wire.assignee {
            Some(assignee) => Claim::claimed(assignee, wire.assignee_expires),
            None => Claim::Unclaimed,
        };

        let fields = BeadFields {
            title: Lww::new(wire.title, field_stamp("title")),
            description: Lww::new(wire.description, field_stamp("description")),
            design: Lww::new(wire.design, field_stamp("design")),
            acceptance_criteria: Lww::new(
                wire.acceptance_criteria,
                field_stamp("acceptance_criteria"),
            ),
            priority: Lww::new(wire.priority, field_stamp("priority")),
            bead_type: Lww::new(wire.bead_type, field_stamp("type")),
            external_ref: Lww::new(wire.external_ref, field_stamp("external_ref")),
            source_repo: Lww::new(wire.source_repo, field_stamp("source_repo")),
            estimated_minutes: Lww::new(wire.estimated_minutes, field_stamp("estimated_minutes")),
            workflow: Lww::new(workflow_value, field_stamp("workflow")),
            claim: Lww::new(claim_value, field_stamp("claim")),
        };
        Bead::new(core, fields)
    }
}

/// Bead patch for deltas (mutable fields only).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireBeadPatch {
    pub id: BeadId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<WireStamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<ActorId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_on_branch: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "WirePatch::is_keep")]
    pub design: WirePatch<String>,
    #[serde(default, skip_serializing_if = "WirePatch::is_keep")]
    pub acceptance_criteria: WirePatch<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<Priority>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub bead_type: Option<BeadType>,
    #[serde(default, skip_serializing_if = "WirePatch::is_keep")]
    pub external_ref: WirePatch<String>,
    #[serde(default, skip_serializing_if = "WirePatch::is_keep")]
    pub source_repo: WirePatch<String>,
    #[serde(default, skip_serializing_if = "WirePatch::is_keep")]
    pub estimated_minutes: WirePatch<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<WorkflowStatus>,
    #[serde(default, skip_serializing_if = "WirePatch::is_keep")]
    pub closed_reason: WirePatch<String>,
    #[serde(default, skip_serializing_if = "WirePatch::is_keep")]
    pub closed_on_branch: WirePatch<String>,

    #[serde(default, skip_serializing_if = "WirePatch::is_keep")]
    pub assignee: WirePatch<ActorId>,
    #[serde(default, skip_serializing_if = "WirePatch::is_keep")]
    pub assignee_expires: WirePatch<WallClock>,
}

impl WireBeadPatch {
    pub fn new(id: BeadId) -> Self {
        Self {
            id,
            created_at: None,
            created_by: None,
            created_on_branch: None,
            title: None,
            description: None,
            design: WirePatch::Keep,
            acceptance_criteria: WirePatch::Keep,
            priority: None,
            bead_type: None,
            external_ref: WirePatch::Keep,
            source_repo: WirePatch::Keep,
            estimated_minutes: WirePatch::Keep,
            status: None,
            closed_reason: WirePatch::Keep,
            closed_on_branch: WirePatch::Keep,
            assignee: WirePatch::Keep,
            assignee_expires: WirePatch::Keep,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireTombstoneV1 {
    pub id: BeadId,
    pub deleted_at: WireStamp,
    pub deleted_by: ActorId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_created_at: Option<WireStamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_created_by: Option<ActorId>,
}

impl WireTombstoneV1 {
    pub fn deleted_stamp(&self) -> Stamp {
        Stamp::new(WriteStamp::from(self.deleted_at), self.deleted_by.clone())
    }

    pub fn lineage_stamp(&self) -> Option<Stamp> {
        let at = self.lineage_created_at?;
        let by = self.lineage_created_by.clone()?;
        Some(Stamp::new(WriteStamp::from(at), by))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireLabelAddV1 {
    pub bead_id: BeadId,
    pub label: Label,
    pub dot: WireDotV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireLabelRemoveV1 {
    pub bead_id: BeadId,
    pub label: Label,
    pub ctx: WireDvvV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireDepAddV1 {
    pub from: BeadId,
    pub to: BeadId,
    pub kind: DepKind,
    pub dot: WireDotV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireDepRemoveV1 {
    pub from: BeadId,
    pub to: BeadId,
    pub kind: DepKind,
    pub ctx: WireDvvV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteAppendV1 {
    pub bead_id: BeadId,
    pub note: WireNoteV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", content = "data", rename_all = "snake_case")]
pub enum TxnOpV1 {
    BeadUpsert(Box<WireBeadPatch>),
    BeadDelete(WireTombstoneV1),
    LabelAdd(WireLabelAddV1),
    LabelRemove(WireLabelRemoveV1),
    DepAdd(WireDepAddV1),
    DepRemove(WireDepRemoveV1),
    NoteAppend(NoteAppendV1),
}

impl TxnOpV1 {
    pub fn key(&self) -> TxnOpKey {
        match self {
            TxnOpV1::BeadUpsert(upsert) => TxnOpKey::BeadUpsert {
                id: upsert.id.clone(),
            },
            TxnOpV1::BeadDelete(delete) => TxnOpKey::BeadDelete {
                id: delete.id.clone(),
                lineage: delete.lineage_stamp(),
            },
            TxnOpV1::LabelAdd(op) => TxnOpKey::LabelAdd {
                bead_id: op.bead_id.clone(),
                label: op.label.clone(),
                dot: op.dot.into(),
            },
            TxnOpV1::LabelRemove(op) => TxnOpKey::LabelRemove {
                bead_id: op.bead_id.clone(),
                label: op.label.clone(),
            },
            TxnOpV1::DepAdd(dep) => TxnOpKey::DepAdd {
                from: dep.from.clone(),
                to: dep.to.clone(),
                kind: dep.kind,
                dot: dep.dot.into(),
            },
            TxnOpV1::DepRemove(dep) => TxnOpKey::DepRemove {
                from: dep.from.clone(),
                to: dep.to.clone(),
                kind: dep.kind,
            },
            TxnOpV1::NoteAppend(append) => TxnOpKey::NoteAppend {
                bead_id: append.bead_id.clone(),
                note_id: append.note.id.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TxnOpKey {
    BeadUpsert {
        id: BeadId,
    },
    BeadDelete {
        id: BeadId,
        lineage: Option<Stamp>,
    },
    LabelAdd {
        bead_id: BeadId,
        label: Label,
        dot: Dot,
    },
    LabelRemove {
        bead_id: BeadId,
        label: Label,
    },
    DepAdd {
        from: BeadId,
        to: BeadId,
        kind: DepKind,
        dot: Dot,
    },
    DepRemove {
        from: BeadId,
        to: BeadId,
        kind: DepKind,
    },
    NoteAppend {
        bead_id: BeadId,
        note_id: NoteId,
    },
}

impl TxnOpKey {
    pub fn kind(&self) -> &'static str {
        match self {
            TxnOpKey::BeadUpsert { .. } => "bead_upsert",
            TxnOpKey::BeadDelete { .. } => "bead_delete",
            TxnOpKey::LabelAdd { .. } => "label_add",
            TxnOpKey::LabelRemove { .. } => "label_remove",
            TxnOpKey::DepAdd { .. } => "dep_add",
            TxnOpKey::DepRemove { .. } => "dep_remove",
            TxnOpKey::NoteAppend { .. } => "note_append",
        }
    }

    pub fn describe(&self) -> String {
        match self {
            TxnOpKey::BeadUpsert { id } => format!("bead_upsert:{}", id.as_str()),
            TxnOpKey::BeadDelete { id, lineage } => match lineage {
                Some(stamp) => format!(
                    "bead_delete:{}:{}:{}:{}",
                    id.as_str(),
                    stamp.at.wall_ms,
                    stamp.at.counter,
                    stamp.by.as_str()
                ),
                None => format!("bead_delete:{}", id.as_str()),
            },
            TxnOpKey::LabelAdd {
                bead_id,
                label,
                dot,
            } => format!(
                "label_add:{}:{}:{}:{}",
                bead_id.as_str(),
                label.as_str(),
                dot.replica,
                dot.counter
            ),
            TxnOpKey::LabelRemove { bead_id, label } => {
                format!("label_remove:{}:{}", bead_id.as_str(), label.as_str())
            }
            TxnOpKey::DepAdd {
                from,
                to,
                kind,
                dot,
            } => format!(
                "dep_add:{}:{}:{}:{}:{}",
                from.as_str(),
                to.as_str(),
                kind.as_str(),
                dot.replica,
                dot.counter
            ),
            TxnOpKey::DepRemove { from, to, kind } => format!(
                "dep_remove:{}:{}:{}",
                from.as_str(),
                to.as_str(),
                kind.as_str()
            ),
            TxnOpKey::NoteAppend { bead_id, note_id } => {
                format!("note_append:{}:{}", bead_id.as_str(), note_id.as_str())
            }
        }
    }
}

impl fmt::Display for TxnOpKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.describe())
    }
}

#[derive(Debug, Error)]
pub enum TxnDeltaError {
    #[error("duplicate op {kind} for key {key}")]
    DuplicateOp { kind: &'static str, key: String },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TxnDeltaV1 {
    ops: BTreeMap<TxnOpKey, TxnOpV1>,
}

impl TxnDeltaV1 {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, op: TxnOpV1) -> Result<(), TxnDeltaError> {
        let key = op.key();
        if self.ops.contains_key(&key) {
            return Err(TxnDeltaError::DuplicateOp {
                kind: key.kind(),
                key: key.describe(),
            });
        }
        self.ops.insert(key, op);
        Ok(())
    }

    pub fn from_parts(
        bead_upserts: Vec<WireBeadPatch>,
        bead_deletes: Vec<WireTombstoneV1>,
        label_adds: Vec<WireLabelAddV1>,
        label_removes: Vec<WireLabelRemoveV1>,
        dep_adds: Vec<WireDepAddV1>,
        dep_removes: Vec<WireDepRemoveV1>,
        note_appends: Vec<NoteAppendV1>,
    ) -> Result<Self, TxnDeltaError> {
        let mut delta = TxnDeltaV1::new();
        for up in bead_upserts {
            delta.insert(TxnOpV1::BeadUpsert(Box::new(up)))?;
        }
        for delete in bead_deletes {
            delta.insert(TxnOpV1::BeadDelete(delete))?;
        }
        for op in label_adds {
            delta.insert(TxnOpV1::LabelAdd(op))?;
        }
        for op in label_removes {
            delta.insert(TxnOpV1::LabelRemove(op))?;
        }
        for dep in dep_adds {
            delta.insert(TxnOpV1::DepAdd(dep))?;
        }
        for dep in dep_removes {
            delta.insert(TxnOpV1::DepRemove(dep))?;
        }
        for na in note_appends {
            delta.insert(TxnOpV1::NoteAppend(na))?;
        }
        Ok(delta)
    }

    pub fn total_ops(&self) -> usize {
        self.ops.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &TxnOpV1> {
        self.ops.values()
    }
}

impl Serialize for TxnDeltaV1 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let ops: Vec<&TxnOpV1> = self.ops.values().collect();
        ops.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TxnDeltaV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ops = Vec::<TxnOpV1>::deserialize(deserializer)?;
        let mut delta = TxnDeltaV1::new();
        for op in ops {
            delta.insert(op).map_err(de::Error::custom)?;
        }
        Ok(delta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::BeadView;
    use crate::core::collections::Labels;
    use crate::core::composite::Note;
    use crate::core::identity::{ActorId, ReplicaId};
    use crate::core::time::Stamp;

    fn actor_id(actor: &str) -> ActorId {
        ActorId::new(actor).unwrap_or_else(|e| panic!("invalid actor id {actor}: {e}"))
    }

    fn bead_id(id: &str) -> BeadId {
        BeadId::parse(id).unwrap_or_else(|e| panic!("invalid bead id {id}: {e}"))
    }

    fn note_id(id: &str) -> NoteId {
        NoteId::new(id).unwrap_or_else(|e| panic!("invalid note id {id}: {e}"))
    }

    #[test]
    fn wire_note_roundtrip() {
        let note = WireNoteV1 {
            id: note_id("note-1"),
            content: "hello".to_string(),
            author: actor_id("alice"),
            at: WireStamp(10, 2),
        };
        let json = serde_json::to_string(&note).unwrap();
        let back: WireNoteV1 = serde_json::from_str(&json).unwrap();
        assert_eq!(note, back);
    }

    #[test]
    fn wire_note_conversion_roundtrip() {
        let note = Note::new(
            note_id("note-2"),
            "content".to_string(),
            actor_id("bob"),
            WriteStamp::new(25, 3),
        );
        let wire = WireNoteV1::from(&note);
        let back = Note::from(wire);
        assert_eq!(note, back);
    }

    #[test]
    fn wire_bead_full_preserves_stamps() {
        let base = Stamp::new(WriteStamp::new(10, 0), actor_id("alice"));
        let newer = Stamp::new(WriteStamp::new(20, 0), actor_id("bob"));

        let core = BeadCore::new(bead_id("bd-abc123"), base.clone(), Some("main".to_string()));
        let fields = BeadFields {
            title: Lww::new("t".to_string(), newer.clone()),
            description: Lww::new("d".to_string(), base.clone()),
            design: Lww::new(None, base.clone()),
            acceptance_criteria: Lww::new(None, base.clone()),
            priority: Lww::new(Priority::default(), base.clone()),
            bead_type: Lww::new(BeadType::Task, base.clone()),
            external_ref: Lww::new(None, base.clone()),
            source_repo: Lww::new(None, base.clone()),
            estimated_minutes: Lww::new(None, base.clone()),
            workflow: Lww::new(Workflow::Open, base.clone()),
            claim: Lww::new(Claim::Unclaimed, base.clone()),
        };
        let bead = Bead::new(core, fields);
        let labels = Labels::new();
        let notes = vec![Note::new(
            note_id("note-3"),
            "n".to_string(),
            actor_id("carol"),
            WriteStamp::new(5, 1),
        )];

        let view = BeadView::new(bead.clone(), labels, notes.clone(), Some(base.clone()));
        let wire = WireBeadFull::from_view(&view, None);
        let rebuilt = Bead::from(wire.clone());

        assert_eq!(bead.core.id, rebuilt.core.id);
        assert_eq!(bead.core.created(), rebuilt.core.created());
        assert_eq!(bead.fields.title.stamp, rebuilt.fields.title.stamp);
        assert_eq!(
            bead.fields.description.stamp,
            rebuilt.fields.description.stamp
        );
        assert_eq!(wire.label_stamp(), base);
        assert_eq!(
            wire.notes,
            notes.iter().map(WireNoteV1::from).collect::<Vec<_>>()
        );
    }

    #[test]
    fn wire_bead_patch_roundtrip() {
        let mut patch = WireBeadPatch::new(bead_id("bd-xyz987"));
        patch.created_at = Some(WireStamp(10, 1));
        patch.created_by = Some(actor_id("alice"));
        patch.title = Some("title".to_string());
        patch.design = WirePatch::Clear;

        let json = serde_json::to_string(&patch).unwrap();
        let back: WireBeadPatch = serde_json::from_str(&json).unwrap();
        assert_eq!(patch, back);
    }

    #[test]
    fn txn_delta_rejects_duplicate_keys() {
        let mut delta = TxnDeltaV1::new();
        let patch = WireBeadPatch::new(bead_id("bd-dupe"));
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch.clone())))
            .unwrap();
        let err = delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
            .unwrap_err();
        assert!(matches!(err, TxnDeltaError::DuplicateOp { .. }));
    }

    #[test]
    fn txn_delta_orders_ops_canonically() {
        let mut delta = TxnDeltaV1::new();
        let delete = WireTombstoneV1 {
            id: bead_id("bd-order"),
            deleted_at: WireStamp(5, 1),
            deleted_by: actor_id("alice"),
            reason: None,
            lineage_created_at: None,
            lineage_created_by: None,
        };
        let dep_add = WireDepAddV1 {
            from: bead_id("bd-order"),
            to: bead_id("bd-up"),
            kind: DepKind::Blocks,
            dot: WireDotV1 {
                replica: ReplicaId::from(uuid::Uuid::from_bytes([1u8; 16])),
                counter: 1,
            },
        };
        let dep_remove = WireDepRemoveV1 {
            from: bead_id("bd-order"),
            to: bead_id("bd-down"),
            kind: DepKind::Related,
            ctx: WireDvvV1 {
                max: BTreeMap::new(),
            },
        };
        let label_add = WireLabelAddV1 {
            bead_id: bead_id("bd-order"),
            label: Label::parse("triage".to_string()).unwrap(),
            dot: WireDotV1 {
                replica: ReplicaId::from(uuid::Uuid::from_bytes([4u8; 16])),
                counter: 2,
            },
        };
        let label_remove = WireLabelRemoveV1 {
            bead_id: bead_id("bd-order"),
            label: Label::parse("triage".to_string()).unwrap(),
            ctx: WireDvvV1 {
                max: BTreeMap::from([(ReplicaId::from(uuid::Uuid::from_bytes([4u8; 16])), 2)]),
            },
        };
        let append = NoteAppendV1 {
            bead_id: bead_id("bd-order"),
            note: WireNoteV1 {
                id: note_id("note-5"),
                content: "c".to_string(),
                author: actor_id("alice"),
                at: WireStamp(1, 1),
            },
        };
        delta.insert(TxnOpV1::NoteAppend(append)).unwrap();
        delta.insert(TxnOpV1::DepRemove(dep_remove)).unwrap();
        delta.insert(TxnOpV1::BeadDelete(delete)).unwrap();
        delta.insert(TxnOpV1::LabelRemove(label_remove)).unwrap();
        delta.insert(TxnOpV1::DepAdd(dep_add)).unwrap();
        delta.insert(TxnOpV1::LabelAdd(label_add)).unwrap();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(WireBeadPatch::new(bead_id(
                "bd-order",
            )))))
            .unwrap();

        let mut iter = delta.iter();
        assert!(matches!(iter.next(), Some(TxnOpV1::BeadUpsert(_))));
        assert!(matches!(iter.next(), Some(TxnOpV1::BeadDelete(_))));
        assert!(matches!(iter.next(), Some(TxnOpV1::LabelAdd(_))));
        assert!(matches!(iter.next(), Some(TxnOpV1::LabelRemove(_))));
        assert!(matches!(iter.next(), Some(TxnOpV1::DepAdd(_))));
        assert!(matches!(iter.next(), Some(TxnOpV1::DepRemove(_))));
        assert!(matches!(iter.next(), Some(TxnOpV1::NoteAppend(_))));
    }

    #[test]
    fn txn_delta_roundtrip() {
        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(WireBeadPatch::new(bead_id(
                "bd-rt",
            )))))
            .unwrap();
        delta
            .insert(TxnOpV1::BeadDelete(WireTombstoneV1 {
                id: bead_id("bd-rt-delete"),
                deleted_at: WireStamp(3, 0),
                deleted_by: actor_id("alice"),
                reason: Some("cleanup".to_string()),
                lineage_created_at: None,
                lineage_created_by: None,
            }))
            .unwrap();
        delta
            .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                from: bead_id("bd-rt"),
                to: bead_id("bd-rt-dep"),
                kind: DepKind::Blocks,
                dot: WireDotV1 {
                    replica: ReplicaId::from(uuid::Uuid::from_bytes([2u8; 16])),
                    counter: 7,
                },
            }))
            .unwrap();
        delta
            .insert(TxnOpV1::DepRemove(WireDepRemoveV1 {
                from: bead_id("bd-rt"),
                to: bead_id("bd-rt-dep2"),
                kind: DepKind::Related,
                ctx: WireDvvV1 {
                    max: BTreeMap::from([
                        (ReplicaId::from(uuid::Uuid::from_bytes([1u8; 16])), 5),
                        (ReplicaId::from(uuid::Uuid::from_bytes([3u8; 16])), 2),
                    ]),
                },
            }))
            .unwrap();
        delta
            .insert(TxnOpV1::LabelAdd(WireLabelAddV1 {
                bead_id: bead_id("bd-rt"),
                label: Label::parse("triage".to_string()).unwrap(),
                dot: WireDotV1 {
                    replica: ReplicaId::from(uuid::Uuid::from_bytes([4u8; 16])),
                    counter: 9,
                },
            }))
            .unwrap();
        delta
            .insert(TxnOpV1::LabelRemove(WireLabelRemoveV1 {
                bead_id: bead_id("bd-rt"),
                label: Label::parse("triage".to_string()).unwrap(),
                ctx: WireDvvV1 {
                    max: BTreeMap::from([(ReplicaId::from(uuid::Uuid::from_bytes([4u8; 16])), 9)]),
                },
            }))
            .unwrap();
        delta
            .insert(TxnOpV1::NoteAppend(NoteAppendV1 {
                bead_id: bead_id("bd-rt"),
                note: WireNoteV1 {
                    id: note_id("note-6"),
                    content: "c".to_string(),
                    author: actor_id("bob"),
                    at: WireStamp(2, 2),
                },
            }))
            .unwrap();

        let json = serde_json::to_string(&delta).unwrap();
        let back: TxnDeltaV1 = serde_json::from_str(&json).unwrap();
        assert_eq!(delta, back);
    }
}
