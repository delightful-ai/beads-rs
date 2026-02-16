//! Deterministic EventBody application into CanonicalState.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use thiserror::Error;

use super::bead::{Bead, BeadCore, BeadFields};
use super::composite::{Claim, Closure, Note, Workflow};
use super::crdt::Lww;
use super::domain::{BeadType, Priority};
use super::event::{
    ValidatedBeadPatch, ValidatedDepAdd, ValidatedDepRemove, ValidatedEventBody,
    ValidatedEventKindV1, ValidatedParentAdd, ValidatedParentRemove, ValidatedTombstone,
    ValidatedTxnOpV1, ValidatedTxnV1,
};
use super::identity::{ActorId, BeadId, BranchName, NoteId};
use super::state::{
    CanonicalState, bead_collision_cmp, legacy_fallback_lineage, note_collision_cmp,
};
use super::time::{Stamp, WriteStamp};
use super::tombstone::Tombstone;
use super::wire_bead::{WireLabelAddV1, WireLabelRemoveV1, WireNoteV1, WirePatch};

#[cfg(test)]
use super::event::sha256_bytes;

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
    #[error("invalid claim patch for {id}: {reason}")]
    InvalidClaimPatch { id: BeadId, reason: String },
    #[error(transparent)]
    InvalidDependency(#[from] super::error::InvalidDependency),
}

/// ```compile_fail
/// use beads_core::{apply_event, CanonicalState, EventBody};
///
/// let mut state = CanonicalState::new();
/// let body: EventBody = todo!();
/// apply_event(&mut state, &body);
/// ```
pub fn apply_event(
    state: &mut CanonicalState,
    body: &ValidatedEventBody,
) -> Result<ApplyOutcome, ApplyError> {
    let ValidatedEventKindV1::TxnV1(txn) = body.kind();

    let stamp = event_stamp(body, txn);
    let mut outcome = ApplyOutcome::default();

    for op in txn.delta.iter() {
        match op {
            ValidatedTxnOpV1::BeadUpsert(patch) => {
                apply_bead_upsert(state, patch, &stamp, &mut outcome)?;
            }
            ValidatedTxnOpV1::BeadDelete(delete) => {
                apply_bead_delete(state, delete, &mut outcome)?;
            }
            ValidatedTxnOpV1::LabelAdd(op) => {
                apply_label_add(state, op, &stamp, &mut outcome)?;
            }
            ValidatedTxnOpV1::LabelRemove(op) => {
                apply_label_remove(state, op, &stamp, &mut outcome)?;
            }
            ValidatedTxnOpV1::DepAdd(dep) => {
                apply_dep_add(state, dep, &stamp, &mut outcome)?;
            }
            ValidatedTxnOpV1::DepRemove(dep) => {
                apply_dep_remove(state, dep, &stamp, &mut outcome)?;
            }
            ValidatedTxnOpV1::ParentAdd(op) => {
                apply_parent_add(state, op, &stamp, &mut outcome)?;
            }
            ValidatedTxnOpV1::ParentRemove(op) => {
                apply_parent_remove(state, op, &stamp, &mut outcome)?;
            }
            ValidatedTxnOpV1::NoteAppend(append) => {
                apply_note_append(
                    state,
                    append.bead_id.clone(),
                    &append.note,
                    append.lineage_stamp(),
                    &mut outcome,
                )?;
            }
        }
    }

    Ok(outcome)
}

fn event_stamp(body: &ValidatedEventBody, txn: &ValidatedTxnV1) -> Stamp {
    let hlc = &txn.hlc_max;
    Stamp::new(
        WriteStamp::new(body.event_time_ms, hlc.logical),
        hlc.actor_id.clone(),
    )
}

fn creation_stamp(patch: &ValidatedBeadPatch, event_stamp: &Stamp) -> Result<Stamp, ApplyError> {
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
    patch: &ValidatedBeadPatch,
    event_stamp: &Stamp,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let id = patch.id.clone();

    if let Some(existing) = state.get_live(&id).cloned() {
        if let (Some(at), Some(by)) = (patch.created_at, patch.created_by.as_ref()) {
            let incoming_created = Stamp::new(WriteStamp::new(at.0, at.1), by.clone());
            if state.has_lineage_tombstone(&id, &incoming_created) {
                return Ok(());
            }
            if existing.core.created() != &incoming_created {
                let core = BeadCore::new(
                    id.clone(),
                    incoming_created.clone(),
                    patch.created_on_branch.clone(),
                );
                let incoming = Bead::new(core, fields_from_patch(patch, event_stamp)?);
                // Compute deleted stamp before move for deterministic tombstone regardless of apply order
                let deleted = std::cmp::max(
                    existing.core.created().clone(),
                    incoming.core.created().clone(),
                );
                let ordering = bead_collision_cmp(state, &existing, &incoming);
                let (winner, loser_stamp, incoming_won) = if ordering == Ordering::Less {
                    (incoming, existing.core.created().clone(), true)
                } else {
                    (existing, incoming.core.created().clone(), false)
                };

                state.insert_tombstone(Tombstone::new_collision(
                    id.clone(),
                    deleted,
                    loser_stamp,
                    None,
                ));
                if incoming_won {
                    state.insert_live(winner);
                }
                outcome.changed_beads.insert(id);
                return Ok(());
            }
        }

        let field_changed = {
            let bead = state.get_live_mut(&id).expect("live bead checked above");
            apply_patch_to_bead(bead, patch, event_stamp, outcome)?
        };

        if field_changed {
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
        .expect("bead collision should be handled before insert");

    outcome.changed_beads.insert(id);
    Ok(())
}

fn apply_bead_delete(
    state: &mut CanonicalState,
    delete: &ValidatedTombstone,
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

fn apply_label_add(
    state: &mut CanonicalState,
    op: &WireLabelAddV1,
    event_stamp: &Stamp,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let dot = op.dot.into();
    let lineage = resolve_lineage(state, &op.bead_id, op.lineage_stamp());
    let change = state.apply_label_add(
        op.bead_id.clone(),
        op.label.clone(),
        dot,
        event_stamp.clone(),
        lineage.clone(),
    );
    if change.changed() && matches_live_lineage(state, &op.bead_id, &lineage) {
        outcome.changed_beads.insert(op.bead_id.clone());
    }
    Ok(())
}

fn apply_label_remove(
    state: &mut CanonicalState,
    op: &WireLabelRemoveV1,
    event_stamp: &Stamp,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let ctx = (&op.ctx).into();
    let lineage = resolve_lineage(state, &op.bead_id, op.lineage_stamp());
    let change = state.apply_label_remove(
        op.bead_id.clone(),
        &op.label,
        &ctx,
        event_stamp.clone(),
        lineage.clone(),
    );
    if change.changed() && matches_live_lineage(state, &op.bead_id, &lineage) {
        outcome.changed_beads.insert(op.bead_id.clone());
    }
    Ok(())
}

fn apply_dep_add(
    state: &mut CanonicalState,
    dep: &ValidatedDepAdd,
    event_stamp: &Stamp,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let key = state.check_dep_add_key(dep.key.clone())?;
    let dot = dep.dot.into();
    let change = state.apply_dep_add(key, dot, event_stamp.clone());
    for changed in change.added.iter().chain(change.removed.iter()) {
        outcome.changed_deps.insert(changed.clone());
    }
    Ok(())
}

fn apply_dep_remove(
    state: &mut CanonicalState,
    dep: &ValidatedDepRemove,
    event_stamp: &Stamp,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let key = dep.key.clone();
    let ctx = (&dep.ctx).into();
    let change = state.apply_dep_remove(&key, &ctx, event_stamp.clone());
    for changed in change.added.iter().chain(change.removed.iter()) {
        outcome.changed_deps.insert(changed.clone());
    }
    Ok(())
}

fn apply_parent_add(
    state: &mut CanonicalState,
    op: &ValidatedParentAdd,
    event_stamp: &Stamp,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let key = state.check_dep_add_key(op.edge().to_dep_key())?;
    let dot = op.dot.into();
    let change = state.apply_dep_add(key, dot, event_stamp.clone());
    for changed in change.added.iter().chain(change.removed.iter()) {
        outcome.changed_deps.insert(changed.clone());
    }
    Ok(())
}

fn apply_parent_remove(
    state: &mut CanonicalState,
    op: &ValidatedParentRemove,
    event_stamp: &Stamp,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let key = op.edge().to_dep_key();
    let ctx = (&op.ctx).into();
    let change = state.apply_dep_remove(&key, &ctx, event_stamp.clone());
    for changed in change.added.iter().chain(change.removed.iter()) {
        outcome.changed_deps.insert(changed.clone());
    }
    Ok(())
}

fn fields_from_patch(
    patch: &ValidatedBeadPatch,
    event_stamp: &Stamp,
) -> Result<BeadFields, ApplyError> {
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
    patch: &ValidatedBeadPatch,
    event_stamp: &Stamp,
    _outcome: &mut ApplyOutcome,
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

fn build_workflow(
    status: super::wire_bead::WorkflowStatus,
    closed_reason: &WirePatch<String>,
    closed_on_branch: &WirePatch<BranchName>,
    existing: &Workflow,
) -> Workflow {
    match status {
        super::wire_bead::WorkflowStatus::Open => Workflow::Open,
        super::wire_bead::WorkflowStatus::InProgress => Workflow::InProgress,
        super::wire_bead::WorkflowStatus::Closed => {
            let (existing_reason, existing_branch) = match existing {
                Workflow::Closed(c) => (c.reason.clone(), c.on_branch.clone()),
                Workflow::Open | Workflow::InProgress => (None, None),
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
    lineage: Option<Stamp>,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let note = note_to_core(note);
    if apply_note(state, bead_id.clone(), lineage.clone(), note, outcome)?
        && matches_live_lineage(state, &bead_id, &resolve_lineage(state, &bead_id, lineage))
    {
        outcome.changed_beads.insert(bead_id);
    }
    Ok(())
}

fn apply_note(
    state: &mut CanonicalState,
    bead_id: BeadId,
    lineage: Option<Stamp>,
    note: Note,
    outcome: &mut ApplyOutcome,
) -> Result<bool, ApplyError> {
    let lineage = resolve_lineage(state, &bead_id, lineage);
    if let Some(existing) = state.insert_note(bead_id.clone(), lineage.clone(), note.clone()) {
        if existing == note {
            return Ok(false);
        }
        if note_collision_cmp(&existing, &note) == Ordering::Less {
            state.replace_note(bead_id.clone(), lineage.clone(), note.clone());
            outcome.changed_notes.insert(NoteKey {
                bead_id,
                note_id: note.id.clone(),
            });
            return Ok(true);
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

fn resolve_lineage(state: &CanonicalState, bead_id: &BeadId, lineage: Option<Stamp>) -> Stamp {
    match lineage {
        Some(lineage) => lineage,
        None => {
            if state.has_collision_tombstone(bead_id) {
                legacy_fallback_lineage()
            } else if let Some(bead) = state.get_live(bead_id) {
                bead.core.created().clone()
            } else {
                legacy_fallback_lineage()
            }
        }
    }
}

fn matches_live_lineage(state: &CanonicalState, bead_id: &BeadId, lineage: &Stamp) -> bool {
    let Some(bead) = state.get_live(bead_id) else {
        return false;
    };
    bead.core.created() == lineage
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
    use crate::collections::Label;
    use crate::dep::DepKey;
    use crate::domain::DepKind;
    use crate::event::{EventKindV1, HlcMax, ValidatedEventBody};
    use crate::identity::{
        ClientRequestId, ReplicaId, StoreEpoch, StoreId, StoreIdentity, TraceId, TxnId,
    };
    use crate::namespace::NamespaceId;
    use crate::wire_bead::{
        WireBeadPatch, WireDepAddV1, WireDepRemoveV1, WireDotV1, WireDvvV1, WireLabelAddV1,
        WireLabelRemoveV1, WireLineageStamp, WireNoteV1, WireStamp, WireTombstoneV1,
    };
    use crate::{EventBody, Limits, Seq1, TxnDeltaV1, TxnOpV1, TxnV1};
    use std::collections::BTreeMap;
    use uuid::Uuid;

    fn actor_id(actor: &str) -> ActorId {
        ActorId::new(actor).unwrap()
    }

    fn sample_event_raw(note_content: &str) -> EventBody {
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

        let lineage = WireLineageStamp {
            at: patch.created_at.expect("created_at"),
            by: patch.created_by.clone().expect("created_by"),
        };
        let mut delta = TxnDeltaV1::new();
        delta.insert(TxnOpV1::BeadUpsert(Box::new(patch))).unwrap();
        delta
            .insert(TxnOpV1::NoteAppend(super::super::wire_bead::NoteAppendV1 {
                bead_id: BeadId::parse("bd-apply1").unwrap(),
                note,
                lineage: Some(lineage),
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

    fn sample_event(note_content: &str) -> ValidatedEventBody {
        sample_event_raw(note_content)
            .into_validated(&Limits::default())
            .expect("valid event fixture")
    }

    fn event_with_delta(delta: TxnDeltaV1, wall_ms: u64) -> ValidatedEventBody {
        let mut event = sample_event_raw("note");
        let EventKindV1::TxnV1(txn) = &mut event.kind;
        txn.delta = delta;
        txn.hlc_max.physical_ms = wall_ms;
        txn.hlc_max.logical = 0;
        event.event_time_ms = wall_ms;
        event
            .into_validated(&Limits::default())
            .expect("valid delta event")
    }

    fn bead_upsert_event(
        bead_id: BeadId,
        created_at: WireStamp,
        created_by: ActorId,
        title: &str,
        wall_ms: u64,
    ) -> ValidatedEventBody {
        let mut patch = WireBeadPatch::new(bead_id);
        patch.created_at = Some(created_at);
        patch.created_by = Some(created_by);
        patch.title = Some(title.to_string());

        let mut delta = TxnDeltaV1::new();
        delta.insert(TxnOpV1::BeadUpsert(Box::new(patch))).unwrap();
        event_with_delta(delta, wall_ms)
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
    fn note_collision_is_deterministic() {
        let mut state = CanonicalState::new();
        let event = sample_event("note-a");
        apply_event(&mut state, &event).unwrap();

        let event_b = sample_event("note-b");
        apply_event(&mut state, &event_b).unwrap();

        let bead_id = BeadId::parse("bd-apply1").unwrap();
        let note_id = NoteId::new("note-apply").unwrap();
        let lineage = state
            .get_live(&bead_id)
            .expect("bead should be live")
            .core
            .created()
            .clone();
        let stored = state
            .note_store()
            .get(&bead_id, &lineage, &note_id)
            .expect("note should be stored");
        let hash_a = sha256_bytes("note-a".as_bytes());
        let hash_b = sha256_bytes("note-b".as_bytes());
        let expected = if hash_a >= hash_b { "note-a" } else { "note-b" };
        assert_eq!(stored.content, expected);
    }

    #[test]
    fn apply_claim_patch_set_with_clear_expiry_is_total() {
        let mut state = CanonicalState::new();
        let bead_id = BeadId::parse("bd-claim").unwrap();
        let create = bead_upsert_event(
            bead_id.clone(),
            WireStamp(10, 1),
            actor_id("alice"),
            "title",
            10,
        );
        apply_event(&mut state, &create).unwrap();

        let mut patch = WireBeadPatch::new(bead_id.clone());
        patch.assignee = WirePatch::Set(actor_id("bob"));
        patch.assignee_expires = WirePatch::Clear;
        let mut delta = TxnDeltaV1::new();
        delta.insert(TxnOpV1::BeadUpsert(Box::new(patch))).unwrap();
        let event = event_with_delta(delta, 20);
        apply_event(&mut state, &event).unwrap();

        let bead = state.get_live(&bead_id).expect("bead");
        match &bead.fields.claim.value {
            Claim::Claimed { assignee, expires } => {
                assert_eq!(assignee, &actor_id("bob"));
                assert_eq!(*expires, None);
            }
            Claim::Unclaimed => panic!("expected claimed"),
        }
    }

    #[test]
    fn note_append_before_bead_exists_is_stored() {
        let mut state = CanonicalState::new();
        let bead_id = BeadId::parse("bd-orphan-note").unwrap();
        let note_id = NoteId::new("note-orphan").unwrap();
        let note = WireNoteV1 {
            id: note_id.clone(),
            content: "orphan".to_string(),
            author: actor_id("alice"),
            at: WireStamp(10, 1),
        };

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::NoteAppend(super::super::wire_bead::NoteAppendV1 {
                bead_id: bead_id.clone(),
                note,
                lineage: None,
            }))
            .unwrap();
        let outcome = apply_event(&mut state, &event_with_delta(delta, 10)).unwrap();

        assert!(state.get_live(&bead_id).is_none());
        assert!(state.note_id_exists(&bead_id, &note_id));
        assert!(outcome.changed_notes.contains(&NoteKey {
            bead_id: bead_id.clone(),
            note_id: note_id.clone(),
        }));
        assert!(!outcome.changed_beads.contains(&bead_id));
    }

    #[test]
    fn label_ops_on_missing_bead_are_total() {
        let mut state = CanonicalState::new();
        let bead_id = BeadId::parse("bd-orphan-label").unwrap();
        let label = Label::parse("orphan").unwrap();
        let replica_id = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
        let lineage = Stamp::new(WriteStamp::new(5, 0), actor_id("alice"));

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::LabelRemove(WireLabelRemoveV1 {
                bead_id: bead_id.clone(),
                label: label.clone(),
                ctx: WireDvvV1 {
                    max: BTreeMap::new(),
                    dots: Vec::new(),
                },
                lineage: Some(lineage.clone().into()),
            }))
            .unwrap();
        apply_event(&mut state, &event_with_delta(delta, 10)).unwrap();

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::LabelAdd(WireLabelAddV1 {
                bead_id: bead_id.clone(),
                label: label.clone(),
                dot: WireDotV1 {
                    replica: replica_id,
                    counter: 1,
                },
                lineage: Some(lineage.clone().into()),
            }))
            .unwrap();
        apply_event(&mut state, &event_with_delta(delta, 11)).unwrap();

        let labels = state.labels_for_lineage(&bead_id, &lineage);
        assert!(labels.contains(label.as_str()));
    }

    #[test]
    fn bead_creation_collision_is_deterministic() {
        let bead_id = BeadId::parse("bd-collision").unwrap();
        let low_stamp = Stamp::new(WriteStamp::new(10, 1), actor_id("alice"));
        let high_stamp = Stamp::new(WriteStamp::new(20, 1), actor_id("bob"));

        let event_low = bead_upsert_event(
            bead_id.clone(),
            WireStamp(10, 1),
            actor_id("alice"),
            "low",
            10,
        );
        let event_high = bead_upsert_event(
            bead_id.clone(),
            WireStamp(20, 1),
            actor_id("bob"),
            "high",
            11,
        );

        let mut state_a = CanonicalState::new();
        apply_event(&mut state_a, &event_low).unwrap();
        apply_event(&mut state_a, &event_high).unwrap();

        let mut state_b = CanonicalState::new();
        apply_event(&mut state_b, &event_high).unwrap();
        apply_event(&mut state_b, &event_low).unwrap();

        assert_eq!(
            state_a.get_live(&bead_id).unwrap().core.created(),
            &high_stamp
        );
        assert_eq!(
            state_b.get_live(&bead_id).unwrap().core.created(),
            &high_stamp
        );
        assert!(state_a.has_lineage_tombstone(&bead_id, &low_stamp));
        assert!(state_b.has_lineage_tombstone(&bead_id, &low_stamp));
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
        let replica_id = ReplicaId::new(Uuid::from_bytes([9u8; 16]));

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepRemove(WireDepRemoveV1 {
                key: DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap(),
                ctx: WireDvvV1 {
                    max: std::collections::BTreeMap::from([(replica_id, 10)]),
                    dots: Vec::new(),
                },
            }))
            .unwrap();
        apply_event(&mut state, &event_with_delta(delta, 10)).unwrap();

        assert!(!state.dep_contains(&key));

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                key: DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap(),
                dot: WireDotV1 {
                    replica: replica_id,
                    counter: 5,
                },
            }))
            .unwrap();
        apply_event(&mut state, &event_with_delta(delta, 11)).unwrap();

        assert!(!state.dep_contains(&key));

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                key: DepKey::new(from, to, DepKind::Blocks).unwrap(),
                dot: WireDotV1 {
                    replica: replica_id,
                    counter: 20,
                },
            }))
            .unwrap();
        apply_event(&mut state, &event_with_delta(delta, 12)).unwrap();

        assert!(state.dep_contains(&key));
    }

    #[test]
    fn dep_add_accepts_acyclic_dag_edge() {
        let mut state = CanonicalState::new();
        let from = BeadId::parse("bd-acyclic-from").unwrap();
        let to = BeadId::parse("bd-acyclic-to").unwrap();

        apply_event(
            &mut state,
            &bead_upsert_event(from.clone(), WireStamp(1, 0), actor_id("alice"), "from", 10),
        )
        .unwrap();
        apply_event(
            &mut state,
            &bead_upsert_event(to.clone(), WireStamp(2, 0), actor_id("bob"), "to", 11),
        )
        .unwrap();

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                key: DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap(),
                dot: WireDotV1 {
                    replica: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
                    counter: 1,
                },
            }))
            .unwrap();

        apply_event(&mut state, &event_with_delta(delta, 12)).unwrap();
        assert!(state.dep_contains(&DepKey::new(from, to, DepKind::Blocks).unwrap()));
    }

    #[test]
    fn dep_add_rejects_cycle_from_apply_event() {
        let mut state = CanonicalState::new();
        let a = BeadId::parse("bd-cycle-a").unwrap();
        let b = BeadId::parse("bd-cycle-b").unwrap();

        apply_event(
            &mut state,
            &bead_upsert_event(a.clone(), WireStamp(1, 0), actor_id("alice"), "a", 20),
        )
        .unwrap();
        apply_event(
            &mut state,
            &bead_upsert_event(b.clone(), WireStamp(2, 0), actor_id("bob"), "b", 21),
        )
        .unwrap();

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                key: DepKey::new(a.clone(), b.clone(), DepKind::Blocks).unwrap(),
                dot: WireDotV1 {
                    replica: ReplicaId::new(Uuid::from_bytes([3u8; 16])),
                    counter: 1,
                },
            }))
            .unwrap();
        apply_event(&mut state, &event_with_delta(delta, 22)).unwrap();

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                key: DepKey::new(b.clone(), a.clone(), DepKind::Blocks).unwrap(),
                dot: WireDotV1 {
                    replica: ReplicaId::new(Uuid::from_bytes([4u8; 16])),
                    counter: 1,
                },
            }))
            .unwrap();

        let err = apply_event(&mut state, &event_with_delta(delta, 23)).unwrap_err();
        match err {
            ApplyError::InvalidDependency(crate::error::InvalidDependency::CycleDetected {
                ..
            }) => {}
            other => panic!("expected InvalidDependency::CycleDetected, got {other:?}"),
        }
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
                lineage: None,
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

    #[test]
    fn label_add_same_value_different_dot_tracks_change() {
        let mut state = CanonicalState::new();
        let bead_id = BeadId::parse("bd-label-change").unwrap();
        let label = Label::parse("status").unwrap();
        let replica_a = ReplicaId::new(Uuid::from_bytes([10u8; 16]));
        let replica_b = ReplicaId::new(Uuid::from_bytes([11u8; 16]));

        // Create the bead first
        let mut patch = WireBeadPatch::new(bead_id.clone());
        patch.created_at = Some(WireStamp(10, 0));
        patch.created_by = Some(actor_id("alice"));
        patch.title = Some("title".to_string());
        let mut delta = TxnDeltaV1::new();
        delta.insert(TxnOpV1::BeadUpsert(Box::new(patch))).unwrap();
        apply_event(&mut state, &event_with_delta(delta, 10)).unwrap();

        // First label add
        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::LabelAdd(WireLabelAddV1 {
                bead_id: bead_id.clone(),
                label: label.clone(),
                dot: WireDotV1 {
                    replica: replica_a,
                    counter: 1,
                },
                lineage: None,
            }))
            .unwrap();
        let outcome1 = apply_event(&mut state, &event_with_delta(delta, 11)).unwrap();
        assert!(outcome1.changed_beads.contains(&bead_id));

        // Second label add with different dot but same label value
        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::LabelAdd(WireLabelAddV1 {
                bead_id: bead_id.clone(),
                label: label.clone(),
                dot: WireDotV1 {
                    replica: replica_b,
                    counter: 1,
                },
                lineage: None,
            }))
            .unwrap();
        let outcome2 = apply_event(&mut state, &event_with_delta(delta, 12)).unwrap();
        // Changed should be tracked even though label value didn't change (new dot added)
        assert!(outcome2.changed_beads.contains(&bead_id));
    }

    #[test]
    fn collision_tombstone_is_order_independent() {
        let bead_id = BeadId::parse("bd-coll-tomb").unwrap();
        let low_stamp = Stamp::new(WriteStamp::new(10, 1), actor_id("alice"));

        let event_low = bead_upsert_event(
            bead_id.clone(),
            WireStamp(10, 1),
            actor_id("alice"),
            "low",
            10,
        );
        let event_high = bead_upsert_event(
            bead_id.clone(),
            WireStamp(20, 1),
            actor_id("bob"),
            "high",
            11,
        );

        // Apply in order: low then high
        let mut state_a = CanonicalState::new();
        apply_event(&mut state_a, &event_low).unwrap();
        apply_event(&mut state_a, &event_high).unwrap();

        // Apply in order: high then low
        let mut state_b = CanonicalState::new();
        apply_event(&mut state_b, &event_high).unwrap();
        apply_event(&mut state_b, &event_low).unwrap();

        // Both should have collision tombstone for the loser (low_stamp)
        assert!(state_a.has_lineage_tombstone(&bead_id, &low_stamp));
        assert!(state_b.has_lineage_tombstone(&bead_id, &low_stamp));

        // Get the collision tombstones via iter_tombstones iterator
        let tomb_a = state_a
            .iter_tombstones()
            .find(|(_, t)| t.lineage.as_ref() == Some(&low_stamp))
            .map(|(_, t)| t.clone())
            .expect("collision tombstone exists");
        let tomb_b = state_b
            .iter_tombstones()
            .find(|(_, t)| t.lineage.as_ref() == Some(&low_stamp))
            .map(|(_, t)| t.clone())
            .expect("collision tombstone exists");

        // Tombstones must be identical (deleted stamp, lineage stamp)
        assert_eq!(tomb_a.deleted, tomb_b.deleted);
        assert_eq!(tomb_a.lineage, tomb_b.lineage);
    }
}
