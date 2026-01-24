//! Core apply semantics + LWW determinism.

use beads_rs::core::NoteAppendV1;
use beads_rs::{
    ActorId, CanonicalState, DepKey, DepKind, EventBody, EventKindV1, HlcMax, ReplicaId,
    TxnDeltaV1, TxnOpV1, WireBeadPatch, WireDepAddV1, WireDepRemoveV1, WireDotV1, WireDvvV1,
    apply_event, sha256_bytes,
};

use crate::fixtures::apply_harness::{
    ApplyHarness, assert_note_present, assert_outcome_contains_bead, assert_outcome_contains_note,
};
use crate::fixtures::event_body::{
    actor_id, bead_id, event_body_with_delta, note_id, sample_event_body, sample_note,
};
use std::collections::BTreeMap;
use uuid::Uuid;

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
    let mut note = sample_note(2, 1);
    note.content = "different-content".to_string();
    delta
        .insert(TxnOpV1::NoteAppend(NoteAppendV1 {
            bead_id: bead_id.clone(),
            note,
        }))
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

#[test]
fn dep_delete_then_readd_restores_indexes() {
    let mut state = CanonicalState::new();
    let from = bead_id(1);
    let to = bead_id(2);
    let kind = DepKind::Blocks;
    let key = DepKey::new(from.clone(), to.clone(), kind).expect("valid dep key");

    apply_event(&mut state, &sample_event_body(1)).expect("apply base 1");
    apply_event(&mut state, &sample_event_body(2)).expect("apply base 2");

    let replica = ReplicaId::new(Uuid::from_bytes([9u8; 16]));

    let mut add_delta = TxnDeltaV1::new();
    add_delta
        .insert(TxnOpV1::DepAdd(WireDepAddV1 {
            from: from.clone(),
            to: to.clone(),
            kind,
            dot: WireDotV1 {
                replica,
                counter: 1,
            },
        }))
        .expect("unique dep add");
    let add_event = event_body_with_delta(3, add_delta);
    apply_event(&mut state, &add_event).expect("apply dep add");

    assert!(state.deps_from(&from).contains(&key));
    assert!(state.deps_to(&to).contains(&key));
    assert!(state
        .dep_indexes()
        .out_edges(&from)
        .iter()
        .any(|(t, k)| t == &to && *k == kind));
    assert!(state
        .dep_indexes()
        .in_edges(&to)
        .iter()
        .any(|(f, k)| f == &from && *k == kind));

    let mut remove_delta = TxnDeltaV1::new();
    remove_delta
        .insert(TxnOpV1::DepRemove(WireDepRemoveV1 {
            from: from.clone(),
            to: to.clone(),
            kind,
            ctx: WireDvvV1 {
                max: BTreeMap::from([(replica, 1)]),
            },
        }))
        .expect("unique dep remove");
    let remove_event = event_body_with_delta(4, remove_delta);
    apply_event(&mut state, &remove_event).expect("apply dep remove");

    assert!(state.deps_from(&from).is_empty());
    assert!(state.deps_to(&to).is_empty());
    assert!(state.dep_indexes().out_edges(&from).is_empty());
    assert!(state.dep_indexes().in_edges(&to).is_empty());

    let mut readd_delta = TxnDeltaV1::new();
    readd_delta
        .insert(TxnOpV1::DepAdd(WireDepAddV1 {
            from: from.clone(),
            to: to.clone(),
            kind,
            dot: WireDotV1 {
                replica,
                counter: 2,
            },
        }))
        .expect("unique dep add");
    let readd_event = event_body_with_delta(5, readd_delta);
    apply_event(&mut state, &readd_event).expect("apply dep re-add");

    assert_eq!(state.deps_from(&from), vec![key.clone()]);
    assert_eq!(state.deps_to(&to), vec![key.clone()]);
    assert_eq!(state.dep_indexes().out_edges(&from).len(), 1);
    assert_eq!(state.dep_indexes().in_edges(&to).len(), 1);
    assert!(state
        .dep_indexes()
        .out_edges(&from)
        .iter()
        .any(|(t, k)| t == &to && *k == kind));
    assert!(state
        .dep_indexes()
        .in_edges(&to)
        .iter()
        .any(|(f, k)| f == &from && *k == kind));
}
