//! Deterministic EventBody application into CanonicalState.

use std::collections::BTreeSet;

use thiserror::Error;

use super::bead::{BeadCore, BeadFields};
use super::collections::{Label, Labels};
use super::composite::{Claim, Closure, Note, Workflow};
use super::crdt::Lww;
use super::dep::{DepEdge, DepKey, DepLife};
use super::domain::{BeadType, Priority};
use super::event::{EventBody, EventKindV1, Sha256, TxnV1};
use super::identity::{ActorId, BeadId, NoteId, ReplicaId};
use super::orset::{Dot, OrSetValue};
use super::state::CanonicalState;
use super::time::{Stamp, WriteStamp};
use super::tombstone::Tombstone;
use super::wire_bead::{
    NotesPatch, TxnOpV1, WireBeadPatch, WireDepDeleteV1, WireDepV1, WireNoteV1, WirePatch,
    WireTombstoneV1,
};
use sha2::{Digest, Sha256 as Sha256Hasher};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NoteKey {
    pub bead_id: BeadId,
    pub note_id: NoteId,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ApplyOutcome {
    pub changed_beads: BTreeSet<BeadId>,
    pub changed_deps: BTreeSet<super::dep::DepKey>,
    pub changed_notes: BTreeSet<NoteKey>,
}

#[derive(Debug, Error)]
pub enum ApplyError {
    #[error("unsupported event kind: {0}")]
    UnsupportedKind(String),
    #[error("bead {id} missing creation stamp")]
    MissingCreationStamp { id: BeadId },
    #[error("bead {id} creation collision")]
    BeadCollision { id: BeadId },
    #[error("note collision for {bead_id}:{note_id}")]
    NoteCollision { bead_id: BeadId, note_id: NoteId },
    #[error("invalid claim patch for {id}: {reason}")]
    InvalidClaimPatch { id: BeadId, reason: String },
    #[error("invalid dependency: {reason}")]
    InvalidDependency { reason: String },
}

pub fn apply_event(
    state: &mut CanonicalState,
    body: &EventBody,
) -> Result<ApplyOutcome, ApplyError> {
    let EventKindV1::TxnV1(txn) = &body.kind;

    let stamp = event_stamp(body, txn);
    let mut outcome = ApplyOutcome::default();

    for op in txn.delta.iter() {
        match op {
            TxnOpV1::BeadUpsert(patch) => {
                apply_bead_upsert(state, patch, &stamp, &mut outcome)?;
            }
            TxnOpV1::BeadDelete(delete) => {
                apply_bead_delete(state, delete, &mut outcome)?;
            }
            TxnOpV1::DepUpsert(dep) => {
                apply_dep_upsert(state, dep, &mut outcome)?;
            }
            TxnOpV1::DepDelete(dep) => {
                apply_dep_delete(state, dep, &mut outcome)?;
            }
            TxnOpV1::NoteAppend(append) => {
                apply_note_append(state, append.bead_id.clone(), &append.note, &mut outcome)?;
            }
        }
    }

    Ok(outcome)
}

fn event_stamp(body: &EventBody, txn: &TxnV1) -> Stamp {
    let hlc = &txn.hlc_max;
    Stamp::new(
        WriteStamp::new(body.event_time_ms, hlc.logical),
        hlc.actor_id.clone(),
    )
}

fn creation_stamp(patch: &WireBeadPatch, event_stamp: &Stamp) -> Result<Stamp, ApplyError> {
    match (patch.created_at, patch.created_by.as_ref()) {
        (Some(at), Some(by)) => Ok(Stamp::new(WriteStamp::new(at.0, at.1), by.clone())),
        (None, None) => Ok(event_stamp.clone()),
        _ => Err(ApplyError::MissingCreationStamp {
            id: patch.id.clone(),
        }),
    }
}

fn apply_bead_upsert(
    state: &mut CanonicalState,
    patch: &WireBeadPatch,
    event_stamp: &Stamp,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let id = patch.id.clone();
    if patch.created_at.is_some() ^ patch.created_by.is_some() {
        return Err(ApplyError::MissingCreationStamp { id: id.clone() });
    }

    if state.get_live(&id).is_some() {
        let field_changed = {
            let bead = state.get_live_mut(&id).expect("live bead checked above");
            if let (Some(at), Some(by)) = (patch.created_at, patch.created_by.as_ref()) {
                let incoming = Stamp::new(WriteStamp::new(at.0, at.1), by.clone());
                if bead.core.created() != &incoming {
                    return Err(ApplyError::BeadCollision { id: id.clone() });
                }
            }
            apply_patch_to_bead(bead, patch, event_stamp, outcome)?
        };

        let mut changed = field_changed;
        if let Some(labels) = &patch.labels {
            if apply_label_patch(state, &id, labels, event_stamp) {
                changed = true;
            }
        }
        if let NotesPatch::AtLeast(notes) = &patch.notes {
            for note in notes {
                let note = note_to_core(note);
                if apply_note(state, id.clone(), note, outcome)? {
                    changed = true;
                }
            }
        }
        if changed {
            outcome.changed_beads.insert(id);
        }
        return Ok(());
    }

    let created = creation_stamp(patch, event_stamp)?;
    if state.has_lineage_tombstone(&id, &created) {
        return Ok(());
    }

    if let Some(tomb) = state.get_tombstone(&id) {
        if event_stamp <= &tomb.deleted {
            return Ok(());
        }
        state.remove_global_tombstone(&id);
    }

    let core = BeadCore::new(id.clone(), created, patch.created_on_branch.clone());
    let bead = super::Bead::new(core, fields_from_patch(patch, event_stamp)?);
    state
        .insert(bead)
        .map_err(|_| ApplyError::BeadCollision { id: id.clone() })?;

    if let Some(labels) = &patch.labels {
        apply_label_patch(state, &id, labels, event_stamp);
    }
    if let NotesPatch::AtLeast(notes) = &patch.notes {
        for note in notes {
            let note = note_to_core(note);
            apply_note(state, id.clone(), note, outcome)?;
        }
    }

    outcome.changed_beads.insert(id);
    Ok(())
}

fn apply_bead_delete(
    state: &mut CanonicalState,
    delete: &WireTombstoneV1,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let id = delete.id.clone();
    let tombstone = Tombstone {
        id: id.clone(),
        deleted: delete.deleted_stamp(),
        reason: delete.reason.clone(),
        lineage: delete.lineage_stamp(),
    };

    if let Some(lineage) = tombstone.lineage.clone() {
        let mut removed = false;
        if let Some(bead) = state.get_live(&id)
            && bead.core.created() == &lineage
        {
            state.remove_live(&id);
            removed = true;
        }
        let had_tombstone = state.has_lineage_tombstone(&id, &lineage);
        state.insert_tombstone(tombstone);
        if removed || !had_tombstone {
            outcome.changed_beads.insert(id);
        }
        return Ok(());
    }

    if let Some(updated) = state.updated_stamp_for(&id)
        && updated > tombstone.deleted
    {
        return Ok(());
    }

    let tombstone_deleted = tombstone.deleted.clone();
    let should_mark = state
        .get_tombstone(&id)
        .is_none_or(|existing| existing.deleted < tombstone_deleted);
    state.delete(tombstone);
    if should_mark {
        outcome.changed_beads.insert(id);
    }
    Ok(())
}

fn apply_dep_upsert(
    state: &mut CanonicalState,
    dep: &WireDepV1,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let key = DepKey::new(dep.from.clone(), dep.to.clone(), dep.kind)
        .map_err(|e| ApplyError::InvalidDependency { reason: e.reason })?;
    let created = Stamp::new(WriteStamp::from(dep.created_at), dep.created_by.clone());
    let edge = match (dep.deleted_at, dep.deleted_by.as_ref()) {
        (Some(at), Some(by)) => {
            let deleted = Stamp::new(WriteStamp::from(at), by.clone());
            let life = Lww::new(DepLife::Deleted, deleted);
            DepEdge::with_life(created, life)
        }
        (None, None) => DepEdge::new(created),
        _ => {
            return Err(ApplyError::InvalidDependency {
                reason: "deleted_at and deleted_by must be set together".into(),
            });
        }
    };

    state.insert_dep(key.clone(), edge);
    outcome.changed_deps.insert(key);
    Ok(())
}

fn apply_dep_delete(
    state: &mut CanonicalState,
    dep: &WireDepDeleteV1,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let key = DepKey::new(dep.from.clone(), dep.to.clone(), dep.kind)
        .map_err(|e| ApplyError::InvalidDependency { reason: e.reason })?;
    let deleted = Stamp::new(WriteStamp::from(dep.deleted_at), dep.deleted_by.clone());
    let life = Lww::new(DepLife::Deleted, deleted.clone());
    let edge = DepEdge::with_life(deleted, life);
    state.insert_dep(key.clone(), edge);
    outcome.changed_deps.insert(key);
    Ok(())
}

fn fields_from_patch(patch: &WireBeadPatch, event_stamp: &Stamp) -> Result<BeadFields, ApplyError> {
    let mut fields = default_fields(event_stamp.clone());

    if let Some(title) = &patch.title {
        fields.title = Lww::new(title.clone(), event_stamp.clone());
    }
    if let Some(description) = &patch.description {
        fields.description = Lww::new(description.clone(), event_stamp.clone());
    }
    if !patch.design.is_keep() {
        let existing = fields.design.value.clone();
        fields.design = Lww::new(
            apply_patch_option(&patch.design, existing),
            event_stamp.clone(),
        );
    }
    if !patch.acceptance_criteria.is_keep() {
        let existing = fields.acceptance_criteria.value.clone();
        fields.acceptance_criteria = Lww::new(
            apply_patch_option(&patch.acceptance_criteria, existing),
            event_stamp.clone(),
        );
    }
    if let Some(priority) = patch.priority {
        fields.priority = Lww::new(priority, event_stamp.clone());
    }
    if let Some(bead_type) = patch.bead_type {
        fields.bead_type = Lww::new(bead_type, event_stamp.clone());
    }
    if !patch.external_ref.is_keep() {
        let existing = fields.external_ref.value.clone();
        fields.external_ref = Lww::new(
            apply_patch_option(&patch.external_ref, existing),
            event_stamp.clone(),
        );
    }
    if !patch.source_repo.is_keep() {
        let existing = fields.source_repo.value.clone();
        fields.source_repo = Lww::new(
            apply_patch_option(&patch.source_repo, existing),
            event_stamp.clone(),
        );
    }
    if !patch.estimated_minutes.is_keep() {
        let existing = fields.estimated_minutes.value;
        fields.estimated_minutes = Lww::new(
            apply_patch_option(&patch.estimated_minutes, existing),
            event_stamp.clone(),
        );
    }

    if let Some(status) = patch.status {
        let workflow_value = build_workflow(
            status,
            &patch.closed_reason,
            &patch.closed_on_branch,
            &fields.workflow.value,
        );
        fields.workflow = Lww::new(workflow_value, event_stamp.clone());
    }

    if !patch.assignee.is_keep() || !patch.assignee_expires.is_keep() {
        let claim_value = build_claim(
            &patch.id,
            &fields.claim.value,
            &patch.assignee,
            &patch.assignee_expires,
        )?;
        fields.claim = Lww::new(claim_value, event_stamp.clone());
    }

    Ok(fields)
}

fn apply_patch_to_bead(
    bead: &mut super::Bead,
    patch: &WireBeadPatch,
    event_stamp: &Stamp,
    outcome: &mut ApplyOutcome,
) -> Result<bool, ApplyError> {
    let mut changed = false;

    if let Some(title) = &patch.title {
        changed |= update_lww(&mut bead.fields.title, title.clone(), event_stamp);
    }
    if let Some(description) = &patch.description {
        changed |= update_lww(
            &mut bead.fields.description,
            description.clone(),
            event_stamp,
        );
    }
    if !patch.design.is_keep() {
        let existing = bead.fields.design.value.clone();
        changed |= update_lww(
            &mut bead.fields.design,
            apply_patch_option(&patch.design, existing),
            event_stamp,
        );
    }
    if !patch.acceptance_criteria.is_keep() {
        let existing = bead.fields.acceptance_criteria.value.clone();
        changed |= update_lww(
            &mut bead.fields.acceptance_criteria,
            apply_patch_option(&patch.acceptance_criteria, existing),
            event_stamp,
        );
    }
    if let Some(priority) = patch.priority {
        changed |= update_lww(&mut bead.fields.priority, priority, event_stamp);
    }
    if let Some(bead_type) = patch.bead_type {
        changed |= update_lww(&mut bead.fields.bead_type, bead_type, event_stamp);
    }
    if !patch.external_ref.is_keep() {
        let existing = bead.fields.external_ref.value.clone();
        changed |= update_lww(
            &mut bead.fields.external_ref,
            apply_patch_option(&patch.external_ref, existing),
            event_stamp,
        );
    }
    if !patch.source_repo.is_keep() {
        let existing = bead.fields.source_repo.value.clone();
        changed |= update_lww(
            &mut bead.fields.source_repo,
            apply_patch_option(&patch.source_repo, existing),
            event_stamp,
        );
    }
    if !patch.estimated_minutes.is_keep() {
        let existing = bead.fields.estimated_minutes.value;
        changed |= update_lww(
            &mut bead.fields.estimated_minutes,
            apply_patch_option(&patch.estimated_minutes, existing),
            event_stamp,
        );
    }

    if let Some(status) = patch.status {
        let workflow_value = build_workflow(
            status,
            &patch.closed_reason,
            &patch.closed_on_branch,
            &bead.fields.workflow.value,
        );
        changed |= update_lww(&mut bead.fields.workflow, workflow_value, event_stamp);
    }

    if !patch.assignee.is_keep() || !patch.assignee_expires.is_keep() {
        let claim_value = build_claim(
            bead.id(),
            &bead.fields.claim.value,
            &patch.assignee,
            &patch.assignee_expires,
        )?;
        changed |= update_lww(&mut bead.fields.claim, claim_value, event_stamp);
    }

    Ok(changed)
}

fn apply_label_patch(
    state: &mut CanonicalState,
    bead_id: &BeadId,
    labels: &Labels,
    event_stamp: &Stamp,
) -> bool {
    let existing = state.labels_for_any(bead_id);
    let mut changed = false;

    for label in labels.iter() {
        if existing.contains(label.as_str()) {
            continue;
        }
        let dot = legacy_dot_from_bytes(&label.collision_bytes(), event_stamp);
        let change = state.apply_label_add(
            bead_id.clone(),
            label.clone(),
            dot,
            Sha256([0; 32]),
            event_stamp.clone(),
        );
        if !change.is_empty() {
            changed = true;
        }
    }

    for label in existing.iter() {
        if labels.contains(label.as_str()) {
            continue;
        }
        let ctx = state.label_dvv(bead_id, label);
        let change = state.apply_label_remove(bead_id.clone(), label, &ctx, event_stamp.clone());
        if !change.is_empty() {
            changed = true;
        }
    }

    changed
}

fn build_workflow(
    status: super::wire_bead::WorkflowStatus,
    closed_reason: &WirePatch<String>,
    closed_on_branch: &WirePatch<String>,
    existing: &Workflow,
) -> Workflow {
    match status {
        super::wire_bead::WorkflowStatus::Open => Workflow::Open,
        super::wire_bead::WorkflowStatus::InProgress => Workflow::InProgress,
        super::wire_bead::WorkflowStatus::Closed => {
            let (existing_reason, existing_branch) = match existing {
                Workflow::Closed(c) => (c.reason.clone(), c.on_branch.clone()),
                _ => (None, None),
            };
            let reason = apply_patch_option(closed_reason, existing_reason);
            let branch = apply_patch_option(closed_on_branch, existing_branch);
            Workflow::Closed(Closure::new(reason, branch))
        }
    }
}

fn build_claim(
    bead_id: &BeadId,
    existing: &Claim,
    assignee: &WirePatch<ActorId>,
    assignee_expires: &WirePatch<super::time::WallClock>,
) -> Result<Claim, ApplyError> {
    match assignee {
        WirePatch::Clear => Ok(Claim::Unclaimed),
        WirePatch::Set(new_assignee) => {
            let expires = match assignee_expires {
                WirePatch::Keep => match existing {
                    Claim::Claimed { expires, .. } => *expires,
                    Claim::Unclaimed => None,
                },
                WirePatch::Clear => None,
                WirePatch::Set(v) => Some(*v),
            };
            Ok(Claim::claimed(new_assignee.clone(), expires))
        }
        WirePatch::Keep => match existing {
            Claim::Claimed { assignee, expires } => {
                let new_expires = match assignee_expires {
                    WirePatch::Keep => *expires,
                    WirePatch::Clear => None,
                    WirePatch::Set(v) => Some(*v),
                };
                Ok(Claim::claimed(assignee.clone(), new_expires))
            }
            Claim::Unclaimed => {
                if !assignee_expires.is_keep() {
                    return Err(ApplyError::InvalidClaimPatch {
                        id: bead_id.clone(),
                        reason: "expires without assignee".into(),
                    });
                }
                Ok(Claim::Unclaimed)
            }
        },
    }
}

fn apply_note_append(
    state: &mut CanonicalState,
    bead_id: BeadId,
    note: &WireNoteV1,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let note = note_to_core(note);
    if apply_note(state, bead_id.clone(), note, outcome)? {
        if state.get_live(&bead_id).is_some() {
            outcome.changed_beads.insert(bead_id);
        }
    }
    Ok(())
}

fn apply_note(
    state: &mut CanonicalState,
    bead_id: BeadId,
    note: Note,
    outcome: &mut ApplyOutcome,
) -> Result<bool, ApplyError> {
    if let Some(existing) = state.insert_note(bead_id.clone(), note.clone()) {
        if existing != note {
            return Err(ApplyError::NoteCollision {
                bead_id,
                note_id: note.id.clone(),
            });
        }
        return Ok(false);
    }

    outcome.changed_notes.insert(NoteKey {
        bead_id,
        note_id: note.id,
    });
    Ok(true)
}

fn note_to_core(note: &WireNoteV1) -> Note {
    Note::new(
        note.id.clone(),
        note.content.clone(),
        note.author.clone(),
        WriteStamp::new(note.at.0, note.at.1),
    )
}

fn legacy_dot_from_bytes(value_bytes: &[u8], stamp: &Stamp) -> Dot {
    let mut hasher = Sha256Hasher::new();
    hasher.update(value_bytes);
    hasher.update(stamp.at.wall_ms.to_le_bytes());
    hasher.update(stamp.at.counter.to_le_bytes());
    hasher.update(stamp.by.as_str().as_bytes());
    let digest = hasher.finalize();

    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&digest[..16]);
    let mut counter_bytes = [0u8; 8];
    counter_bytes.copy_from_slice(&digest[16..24]);

    Dot {
        replica: ReplicaId::from(Uuid::from_bytes(uuid_bytes)),
        counter: u64::from_le_bytes(counter_bytes),
    }
}

fn update_lww<T: Clone + PartialEq>(field: &mut Lww<T>, value: T, stamp: &Stamp) -> bool {
    let candidate = Lww::new(value, stamp.clone());
    let merged = Lww::join(field, &candidate);
    if *field == merged {
        false
    } else {
        *field = merged;
        true
    }
}

fn apply_patch_option<T: Clone>(patch: &WirePatch<T>, existing: Option<T>) -> Option<T> {
    match patch {
        WirePatch::Keep => existing,
        WirePatch::Clear => None,
        WirePatch::Set(value) => Some(value.clone()),
    }
}

fn default_fields(stamp: Stamp) -> BeadFields {
    BeadFields {
        title: Lww::new(String::new(), stamp.clone()),
        description: Lww::new(String::new(), stamp.clone()),
        design: Lww::new(None, stamp.clone()),
        acceptance_criteria: Lww::new(None, stamp.clone()),
        priority: Lww::new(Priority::default(), stamp.clone()),
        bead_type: Lww::new(BeadType::Task, stamp.clone()),
        external_ref: Lww::new(None, stamp.clone()),
        source_repo: Lww::new(None, stamp.clone()),
        estimated_minutes: Lww::new(None, stamp.clone()),
        workflow: Lww::new(Workflow::Open, stamp.clone()),
        claim: Lww::new(Claim::Unclaimed, stamp),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::DepKind;
    use crate::core::event::{EventKindV1, HlcMax};
    use crate::core::identity::{
        ClientRequestId, ReplicaId, StoreEpoch, StoreId, StoreIdentity, TraceId, TxnId,
    };
    use crate::core::namespace::NamespaceId;
    use crate::core::wire_bead::WireStamp;
    use crate::core::{Seq1, TxnDeltaV1};
    use uuid::Uuid;

    fn actor_id(actor: &str) -> ActorId {
        ActorId::new(actor).unwrap()
    }

    fn sample_event(note_content: &str) -> EventBody {
        let store = StoreIdentity::new(
            StoreId::new(Uuid::from_bytes([1u8; 16])),
            StoreEpoch::new(1),
        );
        let origin = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let txn_id = TxnId::new(Uuid::from_bytes([3u8; 16]));
        let client_request_id = ClientRequestId::new(Uuid::from_bytes([4u8; 16]));
        let trace_id = TraceId::from(client_request_id);

        let mut patch = WireBeadPatch::new(BeadId::parse("bd-apply1").unwrap());
        patch.created_at = Some(WireStamp(10, 1));
        patch.created_by = Some(actor_id("alice"));
        patch.title = Some("title".to_string());

        let note = WireNoteV1 {
            id: NoteId::new("note-apply").unwrap(),
            content: note_content.to_string(),
            author: actor_id("alice"),
            at: WireStamp(11, 1),
        };

        let mut delta = TxnDeltaV1::new();
        delta.insert(TxnOpV1::BeadUpsert(Box::new(patch))).unwrap();
        delta
            .insert(TxnOpV1::NoteAppend(super::super::wire_bead::NoteAppendV1 {
                bead_id: BeadId::parse("bd-apply1").unwrap(),
                note,
            }))
            .unwrap();

        EventBody {
            envelope_v: 1,
            store,
            namespace: NamespaceId::core(),
            origin_replica_id: origin,
            origin_seq: Seq1::from_u64(1).unwrap(),
            event_time_ms: 100,
            txn_id,
            client_request_id: Some(client_request_id),
            trace_id: Some(trace_id),
            kind: EventKindV1::TxnV1(TxnV1 {
                delta,
                hlc_max: HlcMax {
                    actor_id: actor_id("alice"),
                    physical_ms: 100,
                    logical: 5,
                },
            }),
        }
    }

    fn event_with_delta(delta: TxnDeltaV1, wall_ms: u64) -> EventBody {
        let mut event = sample_event("note");
        let EventKindV1::TxnV1(txn) = &mut event.kind;
        txn.delta = delta;
        txn.hlc_max.physical_ms = wall_ms;
        txn.hlc_max.logical = 0;
        event.event_time_ms = wall_ms;
        event
    }

    #[test]
    fn apply_event_is_idempotent() {
        let mut state = CanonicalState::new();
        let event = sample_event("note");
        let outcome1 = apply_event(&mut state, &event).unwrap();
        assert!(
            outcome1
                .changed_beads
                .contains(&BeadId::parse("bd-apply1").unwrap())
        );

        let outcome2 = apply_event(&mut state, &event).unwrap();
        assert!(outcome2.changed_beads.is_empty());
        assert!(outcome2.changed_notes.is_empty());
    }

    #[test]
    fn note_collision_is_detected() {
        let mut state = CanonicalState::new();
        let event = sample_event("note-a");
        apply_event(&mut state, &event).unwrap();

        let event_b = sample_event("note-b");
        let err = apply_event(&mut state, &event_b).unwrap_err();
        assert!(matches!(err, ApplyError::NoteCollision { .. }));
    }

    #[test]
    fn outcome_tracks_notes_and_beads() {
        let mut state = CanonicalState::new();
        let event = sample_event("note");
        let outcome = apply_event(&mut state, &event).unwrap();
        assert!(
            outcome
                .changed_beads
                .contains(&BeadId::parse("bd-apply1").unwrap())
        );
        assert_eq!(outcome.changed_notes.len(), 1);
    }

    #[test]
    fn dep_delete_before_create_converges() {
        let mut state = CanonicalState::new();
        let from = BeadId::parse("bd-from").unwrap();
        let to = BeadId::parse("bd-to").unwrap();
        let key = DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap();

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepDelete(WireDepDeleteV1 {
                from: from.clone(),
                to: to.clone(),
                kind: DepKind::Blocks,
                deleted_at: WireStamp(10, 0),
                deleted_by: actor_id("alice"),
            }))
            .unwrap();
        apply_event(&mut state, &event_with_delta(delta, 10)).unwrap();

        let edge = state.get_dep(&key).unwrap();
        assert!(edge.is_deleted());

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepUpsert(WireDepV1 {
                from: from.clone(),
                to: to.clone(),
                kind: DepKind::Blocks,
                created_at: WireStamp(5, 0),
                created_by: actor_id("bob"),
                deleted_at: None,
                deleted_by: None,
            }))
            .unwrap();
        apply_event(&mut state, &event_with_delta(delta, 11)).unwrap();

        let edge = state.get_dep(&key).unwrap();
        assert!(edge.is_deleted());

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepUpsert(WireDepV1 {
                from: from.clone(),
                to: to.clone(),
                kind: DepKind::Blocks,
                created_at: WireStamp(20, 0),
                created_by: actor_id("carol"),
                deleted_at: None,
                deleted_by: None,
            }))
            .unwrap();
        apply_event(&mut state, &event_with_delta(delta, 12)).unwrap();

        let edge = state.get_dep(&key).unwrap();
        assert!(edge.is_active());
    }

    #[test]
    fn bead_delete_ignores_older_than_live() {
        let mut state = CanonicalState::new();
        let mut patch = WireBeadPatch::new(BeadId::parse("bd-delete").unwrap());
        patch.created_at = Some(WireStamp(20, 0));
        patch.created_by = Some(actor_id("alice"));
        patch.title = Some("title".to_string());

        let mut delta = TxnDeltaV1::new();
        delta.insert(TxnOpV1::BeadUpsert(Box::new(patch))).unwrap();
        apply_event(&mut state, &event_with_delta(delta, 20)).unwrap();

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadDelete(WireTombstoneV1 {
                id: BeadId::parse("bd-delete").unwrap(),
                deleted_at: WireStamp(10, 0),
                deleted_by: actor_id("alice"),
                reason: None,
                lineage_created_at: None,
                lineage_created_by: None,
            }))
            .unwrap();
        apply_event(&mut state, &event_with_delta(delta, 21)).unwrap();

        assert!(
            state
                .get_live(&BeadId::parse("bd-delete").unwrap())
                .is_some()
        );
        assert!(
            state
                .get_tombstone(&BeadId::parse("bd-delete").unwrap())
                .is_none()
        );
    }
}
