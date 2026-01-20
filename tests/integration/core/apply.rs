//! Core apply semantics + LWW determinism.

use beads_rs::core::NoteAppendV1;
use beads_rs::{
    ActorId, CanonicalState, EventBody, EventKindV1, HlcMax, TxnDeltaV1, TxnOpV1, WireBeadPatch,
    WireNoteV1, WireStamp, apply_event, sha256_bytes,
};

use crate::fixtures::apply_harness::{
    ApplyHarness, assert_note_present, assert_outcome_contains_bead, assert_outcome_contains_note,
};
use crate::fixtures::event_body::{
    actor_id, bead_id, event_body_with_delta, note_id, sample_event_body,
};

fn update_title_event(bead_id: &beads_rs::BeadId, title: &str, actor: ActorId) -> EventBody {
    let mut patch = WireBeadPatch::new(bead_id.clone());
    patch.title = Some(title.to_string());

    let mut delta = TxnDeltaV1::new();
    delta
        .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
        .expect("unique upsert");

    let mut event = event_body_with_delta(9, delta);
    event.event_time_ms = 1_700_000_000_500;
    let EventKindV1::TxnV1(txn) = &mut event.kind;
    txn.hlc_max = HlcMax {
        actor_id: actor,
        physical_ms: event.event_time_ms,
        logical: 42,
    };
    event
}

#[test]
fn apply_event_is_idempotent() {
    let mut harness = ApplyHarness::new();
    let event = sample_event_body(1);
    let bead_id = bead_id(1);
    let note_id = note_id(2);

    let (first, second) = harness.apply_twice(&event);
    assert_outcome_contains_bead(&first, &bead_id);
    assert_outcome_contains_note(&first, &bead_id, &note_id);

    assert!(second.changed_beads.is_empty());
    assert!(second.changed_notes.is_empty());

    assert_note_present(harness.state(), &bead_id, &note_id);
}

#[test]
fn note_collision_is_deterministic() {
    let mut state = CanonicalState::new();
    let base = sample_event_body(1);
    apply_event(&mut state, &base).expect("apply base");

    let bead_id = bead_id(1);
    let note_id = note_id(2);

    let mut delta = TxnDeltaV1::new();
    let note = WireNoteV1 {
        id: note_id.clone(),
        content: "different-content".to_string(),
        author: actor_id(9),
        at: WireStamp(12_345, 1),
    };
    delta
        .insert(TxnOpV1::NoteAppend(NoteAppendV1 { bead_id, note }))
        .expect("unique note append");

    let event = event_body_with_delta(2, delta);
    apply_event(&mut state, &event).expect("apply collision");

    let stored = state
        .notes_for(&bead_id)
        .into_iter()
        .find(|note| note.id == note_id)
        .expect("note stored");
    let hash_base = sha256_bytes("note-02".as_bytes());
    let hash_incoming = sha256_bytes("different-content".as_bytes());
    let expected = if hash_base >= hash_incoming {
        "note-02"
    } else {
        "different-content"
    };
    assert_eq!(stored.content, expected);
}

#[test]
fn lww_merge_ordering_is_deterministic() {
    let base = sample_event_body(1);
    let bead_id = bead_id(1);

    let mut state_a = CanonicalState::new();
    apply_event(&mut state_a, &base).expect("apply base");
    let mut state_b = state_a.clone();

    let event_a = update_title_event(&bead_id, "title-a", actor_id(1));
    let event_b = update_title_event(&bead_id, "title-b", actor_id(2));

    apply_event(&mut state_a, &event_a).expect("apply event a");
    apply_event(&mut state_a, &event_b).expect("apply event b");

    apply_event(&mut state_b, &event_b).expect("apply event b");
    apply_event(&mut state_b, &event_a).expect("apply event a");

    let title_a = state_a
        .get_live(&bead_id)
        .expect("bead")
        .title()
        .to_string();
    let title_b = state_b
        .get_live(&bead_id)
        .expect("bead")
        .title()
        .to_string();
    assert_eq!(title_a, title_b);
    assert_eq!(title_a, "title-b");
}
