#![allow(dead_code)]

use beads_rs::core::BranchName;
use beads_rs::core::NoteAppendV1;
use beads_rs::{
    ActorId, BeadId, BeadType, ClientRequestId, EventBody, EventKindV1, HlcMax, NamespaceId,
    NoteId, Priority, ReplicaId, Seq1, StoreEpoch, StoreId, StoreIdentity, TraceId, TxnDeltaV1,
    TxnId, TxnOpV1, TxnV1, WallClock, WireBeadPatch, WireNoteV1, WirePatch, WireStamp,
    WorkflowStatus,
};
use uuid::Uuid;

pub fn actor_id(seed: u8) -> ActorId {
    ActorId::new(format!("actor-{seed:02x}")).expect("valid actor id fixture")
}

pub fn bead_id(seed: u8) -> BeadId {
    BeadId::parse(&format!("bd-{seed:02x}")).expect("valid bead id fixture")
}

pub fn note_id(seed: u8) -> NoteId {
    NoteId::new(format!("note-{seed:02x}")).expect("valid note id fixture")
}

pub fn sample_bead_patch(seed: u8) -> WireBeadPatch {
    let id = bead_id(seed);
    let mut patch = WireBeadPatch::new(id);
    patch.created_at = Some(WireStamp(10_000 + seed as u64, 1));
    patch.created_by = Some(actor_id(seed));
    patch.created_on_branch =
        Some(BranchName::parse(format!("branch-{seed:02x}")).expect("valid branch name fixture"));
    patch.title = Some(format!("title-{seed:02x}"));
    patch.description = Some(format!("description-{seed:02x}"));
    patch.design = WirePatch::Set(format!("design-{seed:02x}"));
    patch.acceptance_criteria = WirePatch::Clear;
    patch.priority = Some(Priority::HIGH);
    patch.bead_type = Some(BeadType::Task);
    patch.external_ref = WirePatch::Set(format!("ref-{seed:02x}"));
    patch.estimated_minutes = WirePatch::Set(45);
    patch.status = Some(WorkflowStatus::Open);
    patch.closed_reason = WirePatch::Clear;
    patch.closed_on_branch = WirePatch::Clear;
    patch.assignee = WirePatch::Set(actor_id(seed.wrapping_add(1)));
    patch.assignee_expires = WirePatch::Set(WallClock(20_000 + seed as u64));
    patch
}

pub fn sample_note(seed: u8, author_seed: u8) -> WireNoteV1 {
    WireNoteV1 {
        id: note_id(seed),
        content: format!("note-{seed:02x}"),
        author: actor_id(author_seed),
        at: WireStamp(12_000 + seed as u64, 3),
    }
}

pub fn sample_delta(seed: u8) -> TxnDeltaV1 {
    let bead = sample_bead_patch(seed);
    let note = sample_note(seed.wrapping_add(1), seed);
    let append = NoteAppendV1 {
        bead_id: bead.id.clone(),
        note,
        lineage: None,
    };

    let mut delta = TxnDeltaV1::new();
    delta
        .insert(TxnOpV1::BeadUpsert(Box::new(bead)))
        .expect("unique bead upsert");
    delta
        .insert(TxnOpV1::NoteAppend(append))
        .expect("unique note append");
    delta
}

pub fn event_body_with_delta(seed: u8, delta: TxnDeltaV1) -> EventBody {
    let store = StoreIdentity::new(
        StoreId::new(Uuid::from_bytes([seed; 16])),
        StoreEpoch::new(1),
    );
    let origin_replica_id = ReplicaId::new(Uuid::from_bytes([seed.wrapping_add(1); 16]));
    let txn_id = TxnId::new(Uuid::from_bytes([seed.wrapping_add(2); 16]));
    let client_request_id = ClientRequestId::new(Uuid::from_bytes([seed.wrapping_add(3); 16]));
    let trace_id = TraceId::from(client_request_id);
    let event_time_ms = 1_700_000_000_000u64 + seed as u64;

    EventBody {
        envelope_v: 1,
        store,
        namespace: NamespaceId::core(),
        origin_replica_id,
        origin_seq: Seq1::from_u64(seed as u64 + 1).expect("valid seq1"),
        event_time_ms,
        txn_id,
        client_request_id: Some(client_request_id),
        trace_id: Some(trace_id),
        kind: EventKindV1::TxnV1(TxnV1 {
            delta,
            hlc_max: HlcMax {
                actor_id: actor_id(seed),
                physical_ms: event_time_ms,
                logical: 7 + seed as u32,
            },
        }),
    }
}

pub fn sample_event_body(seed: u8) -> EventBody {
    event_body_with_delta(seed, sample_delta(seed))
}
