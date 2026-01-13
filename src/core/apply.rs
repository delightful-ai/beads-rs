//! Deterministic EventBody application into CanonicalState.

use std::collections::BTreeSet;

use thiserror::Error;

use super::bead::{BeadCore, BeadFields};
use super::collections::Labels;
use super::composite::{Claim, Closure, Note, Workflow};
use super::crdt::Lww;
use super::domain::{BeadType, Priority};
use super::event::{EventBody, EventKindV1};
use super::identity::{ActorId, BeadId, NoteId};
use super::state::CanonicalState;
use super::time::{Stamp, WriteStamp};
use super::wire_bead::{NotesPatch, TxnOpV1, WireBeadPatch, WireNoteV1, WirePatch};

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
    #[error("event missing hlc_max for stamp derivation")]
    MissingHlcMax,
    #[error("unsupported event kind: {0}")]
    UnsupportedKind(String),
    #[error("bead {id} missing creation stamp")]
    MissingCreationStamp { id: BeadId },
    #[error("bead {id} creation collision")]
    BeadCollision { id: BeadId },
    #[error("note collision for {bead_id}:{note_id}")]
    NoteCollision { bead_id: BeadId, note_id: NoteId },
    #[error("note append missing bead {id}")]
    MissingBead { id: BeadId },
    #[error("invalid claim patch for {id}: {reason}")]
    InvalidClaimPatch { id: BeadId, reason: String },
}

pub fn apply_event(
    state: &mut CanonicalState,
    body: &EventBody,
) -> Result<ApplyOutcome, ApplyError> {
    if !matches!(body.kind, EventKindV1::TxnV1) {
        return Err(ApplyError::UnsupportedKind(body.kind.as_str().to_string()));
    }

    let stamp = event_stamp(body)?;
    let mut outcome = ApplyOutcome::default();

    for op in body.delta.iter() {
        match op {
            TxnOpV1::BeadUpsert(patch) => {
                apply_bead_upsert(state, patch, &stamp, &mut outcome)?;
            }
            TxnOpV1::NoteAppend(append) => {
                apply_note_append(state, append.bead_id.clone(), &append.note, &mut outcome)?;
            }
        }
    }

    Ok(outcome)
}

fn event_stamp(body: &EventBody) -> Result<Stamp, ApplyError> {
    let hlc = body.hlc_max.as_ref().ok_or(ApplyError::MissingHlcMax)?;
    Ok(Stamp::new(
        WriteStamp::new(body.event_time_ms, hlc.logical),
        hlc.actor_id.clone(),
    ))
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

    if let Some(bead) = state.get_live_mut(&id) {
        if let (Some(at), Some(by)) = (patch.created_at, patch.created_by.as_ref()) {
            let incoming = Stamp::new(WriteStamp::new(at.0, at.1), by.clone());
            if bead.core.created() != &incoming {
                return Err(ApplyError::BeadCollision { id: id.clone() });
            }
        }
        let changed = apply_patch_to_bead(bead, patch, event_stamp, outcome)?;
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
    let mut bead = super::Bead::new(core, default_fields(event_stamp.clone()));
    apply_patch_to_bead(&mut bead, patch, event_stamp, outcome)?;
    state
        .insert(bead)
        .map_err(|_| ApplyError::BeadCollision { id: id.clone() })?;
    outcome.changed_beads.insert(id);
    Ok(())
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
    if let Some(labels) = &patch.labels {
        changed |= update_lww(&mut bead.fields.labels, labels.clone(), event_stamp);
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

    if let NotesPatch::AtLeast(notes) = &patch.notes {
        for note in notes {
            let note = note_to_core(note);
            if apply_note(bead, note, outcome)? {
                changed = true;
            }
        }
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
    let Some(bead) = state.get_live_mut(&bead_id) else {
        return Err(ApplyError::MissingBead { id: bead_id });
    };
    let note = note_to_core(note);
    if apply_note(bead, note, outcome)? {
        outcome.changed_beads.insert(bead_id);
    }
    Ok(())
}

fn apply_note(
    bead: &mut super::Bead,
    note: Note,
    outcome: &mut ApplyOutcome,
) -> Result<bool, ApplyError> {
    if let Some(existing) = bead.notes.get(&note.id) {
        if existing == &note {
            return Ok(false);
        }
        return Err(ApplyError::NoteCollision {
            bead_id: bead.id().clone(),
            note_id: note.id.clone(),
        });
    }

    bead.notes.insert(note.clone());
    outcome.changed_notes.insert(NoteKey {
        bead_id: bead.id().clone(),
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
        labels: Lww::new(Labels::new(), stamp.clone()),
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
    use crate::core::event::{EventKindV1, HlcMax};
    use crate::core::identity::{
        ClientRequestId, ReplicaId, StoreEpoch, StoreId, StoreIdentity, TxnId,
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
            kind: EventKindV1::TxnV1,
            delta,
            hlc_max: Some(HlcMax {
                actor_id: actor_id("alice"),
                physical_ms: 100,
                logical: 5,
            }),
        }
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
}
