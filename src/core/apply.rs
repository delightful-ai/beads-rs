//! Deterministic EventBody application into CanonicalState.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use thiserror::Error;

use super::bead::{Bead, BeadCore, BeadFields, BeadView};
use super::composite::{Claim, Closure, Note, Workflow};
use super::crdt::Lww;
use super::dep::DepKey;
use super::domain::{BeadType, Priority};
use super::event::{EventBody, EventKindV1, Sha256, TxnV1, sha256_bytes};
use super::identity::{ActorId, BeadId, ContentHash, NoteId};
use super::state::CanonicalState;
use super::time::{Stamp, WriteStamp};
use super::tombstone::Tombstone;
use super::wire_bead::{
    TxnOpV1, WireBeadPatch, WireDepAddV1, WireDepRemoveV1, WireLabelAddV1, WireLabelRemoveV1,
    WireNoteV1, WirePatch, WireTombstoneV1,
};

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
            TxnOpV1::LabelAdd(op) => {
                apply_label_add(state, op, &stamp, &mut outcome)?;
            }
            TxnOpV1::LabelRemove(op) => {
                apply_label_remove(state, op, &stamp, &mut outcome)?;
            }
            TxnOpV1::DepAdd(dep) => {
                apply_dep_add(state, dep, &stamp, &mut outcome)?;
            }
            TxnOpV1::DepRemove(dep) => {
                apply_dep_remove(state, dep, &stamp, &mut outcome)?;
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

fn bead_content_hash_for_collision(state: &CanonicalState, bead: &Bead) -> ContentHash {
    let id = bead.id();
    let labels = state.labels_for_any(id);
    let notes = state
        .note_store()
        .notes_for(id)
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let label_stamp = state.label_stamp(id).cloned();
    *BeadView::new(bead.clone(), labels, notes, label_stamp).content_hash()
}

fn bead_collision_cmp(state: &CanonicalState, existing: &Bead, incoming: &Bead) -> Ordering {
    existing
        .core
        .created()
        .cmp(incoming.core.created())
        .then_with(|| {
            bead_content_hash_for_collision(state, existing)
                .as_bytes()
                .cmp(bead_content_hash_for_collision(state, incoming).as_bytes())
        })
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
                let ordering = bead_collision_cmp(state, &existing, &incoming);
                let (winner, loser_stamp, incoming_won) = if ordering == Ordering::Less {
                    (incoming, existing.core.created().clone(), true)
                } else {
                    (existing, incoming.core.created().clone(), false)
                };

                state.insert_tombstone(Tombstone::new_collision(
                    id.clone(),
                    event_stamp.clone(),
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
        .map_err(|_| ApplyError::BeadCollision { id: id.clone() })?;

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

fn apply_label_add(
    state: &mut CanonicalState,
    op: &WireLabelAddV1,
    event_stamp: &Stamp,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let dot = op.dot.into();
    let change = state.apply_label_add(
        op.bead_id.clone(),
        op.label.clone(),
        dot,
        Sha256([0; 32]),
        event_stamp.clone(),
    );
    if !change.is_empty() {
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
    let change = state.apply_label_remove(op.bead_id.clone(), &op.label, &ctx, event_stamp.clone());
    if !change.is_empty() {
        outcome.changed_beads.insert(op.bead_id.clone());
    }
    Ok(())
}

fn apply_dep_add(
    state: &mut CanonicalState,
    dep: &WireDepAddV1,
    event_stamp: &Stamp,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let key = DepKey::new(dep.from.clone(), dep.to.clone(), dep.kind)
        .map_err(|e| ApplyError::InvalidDependency { reason: e.reason })?;
    let dot = dep.dot.into();
    let change = state.apply_dep_add(key.clone(), dot, Sha256([0; 32]), event_stamp.clone());
    for changed in change.added.iter().chain(change.removed.iter()) {
        outcome.changed_deps.insert(changed.clone());
    }
    Ok(())
}

fn apply_dep_remove(
    state: &mut CanonicalState,
    dep: &WireDepRemoveV1,
    event_stamp: &Stamp,
    outcome: &mut ApplyOutcome,
) -> Result<(), ApplyError> {
    let key = DepKey::new(dep.from.clone(), dep.to.clone(), dep.kind)
        .map_err(|e| ApplyError::InvalidDependency { reason: e.reason })?;
    let ctx = (&dep.ctx).into();
    let change = state.apply_dep_remove(&key, &ctx, event_stamp.clone());
    for changed in change.added.iter().chain(change.removed.iter()) {
        outcome.changed_deps.insert(changed.clone());
    }
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
    if apply_note(state, bead_id.clone(), note, outcome)? && state.get_live(&bead_id).is_some() {
        outcome.changed_beads.insert(bead_id);
    }
    Ok(())
}

fn note_collision_cmp(existing: &Note, incoming: &Note) -> Ordering {
    existing
        .at
        .cmp(&incoming.at)
        .then_with(|| existing.author.cmp(&incoming.author))
        .then_with(|| {
            sha256_bytes(existing.content.as_bytes())
                .cmp(&sha256_bytes(incoming.content.as_bytes()))
        })
}

fn apply_note(
    state: &mut CanonicalState,
    bead_id: BeadId,
    note: Note,
    outcome: &mut ApplyOutcome,
) -> Result<bool, ApplyError> {
    if let Some(existing) = state.insert_note(bead_id.clone(), note.clone()) {
        if existing == note {
            return Ok(false);
        }
        if note_collision_cmp(&existing, &note) == Ordering::Less {
            state.replace_note(bead_id.clone(), note.clone());
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
    use crate::core::collections::Label;
    use crate::core::domain::DepKind;
    use crate::core::event::{EventKindV1, HlcMax};
    use crate::core::identity::{
        ClientRequestId, ReplicaId, StoreEpoch, StoreId, StoreIdentity, TraceId, TxnId,
    };
    use crate::core::namespace::NamespaceId;
    use crate::core::wire_bead::{WireDotV1, WireDvvV1, WireStamp};
    use crate::core::{Seq1, TxnDeltaV1};
    use std::collections::BTreeMap;
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

    fn bead_upsert_event(
        bead_id: BeadId,
        created_at: WireStamp,
        created_by: ActorId,
        title: &str,
        wall_ms: u64,
    ) -> EventBody {
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
        let stored = state
            .note_store()
            .get(&bead_id, &note_id)
            .expect("note should be stored");
        let hash_a = sha256_bytes("note-a".as_bytes());
        let hash_b = sha256_bytes("note-b".as_bytes());
        let expected = if hash_a >= hash_b { "note-a" } else { "note-b" };
        assert_eq!(stored.content, expected);
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

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::LabelRemove(WireLabelRemoveV1 {
                bead_id: bead_id.clone(),
                label: label.clone(),
                ctx: WireDvvV1 {
                    max: BTreeMap::new(),
                    dots: Vec::new(),
                },
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
            }))
            .unwrap();
        apply_event(&mut state, &event_with_delta(delta, 11)).unwrap();

        let labels = state.labels_for_any(&bead_id);
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
                from: from.clone(),
                to: to.clone(),
                kind: DepKind::Blocks,
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
                from: from.clone(),
                to: to.clone(),
                kind: DepKind::Blocks,
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
                from: from.clone(),
                to: to.clone(),
                kind: DepKind::Blocks,
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
}
