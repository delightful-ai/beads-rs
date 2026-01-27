//! Core apply semantics + LWW determinism.

use beads_rs::core::Label;
use beads_rs::core::NoteAppendV1;
use beads_rs::{
    ActorId, CanonicalState, DepKey, DepKind, EventBody, EventKindV1, HlcMax, ReplicaId, Stamp,
    TxnDeltaV1, TxnOpV1, WireBeadPatch, WireDepAddV1, WireDepRemoveV1, WireDotV1, WireDvvV1,
    WireLabelAddV1, WireLabelRemoveV1, WireStamp, WriteStamp, apply_event, sha256_bytes,
};

use crate::fixtures::apply_harness::{
    ApplyHarness, assert_note_present, assert_outcome_contains_bead, assert_outcome_contains_note,
};
use crate::fixtures::event_body::{
    actor_id, bead_id, event_body_with_delta, note_id, sample_bead_patch, sample_event_body,
    sample_note,
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

fn bead_create_event(seed: u8) -> EventBody {
    let mut delta = TxnDeltaV1::new();
    delta
        .insert(TxnOpV1::BeadUpsert(Box::new(sample_bead_patch(seed))))
        .expect("unique bead upsert");
    event_body_with_delta(seed, delta)
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
            lineage: None,
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
fn join_bead_collision_is_deterministic_and_inserts_lineage_tombstone() {
    let bead = bead_id(50);

    let mut patch_a = sample_bead_patch(50);
    patch_a.id = bead.clone();
    patch_a.created_at = Some(WireStamp(1000, 0));
    patch_a.created_by = Some(actor_id(1));
    patch_a.title = Some("title-a".to_string());

    let mut patch_b = sample_bead_patch(51);
    patch_b.id = bead.clone();
    patch_b.created_at = Some(WireStamp(2000, 0));
    patch_b.created_by = Some(actor_id(2));
    patch_b.title = Some("title-b".to_string());

    let mut state_a = CanonicalState::new();
    let mut delta_a = TxnDeltaV1::new();
    delta_a
        .insert(TxnOpV1::BeadUpsert(Box::new(patch_a)))
        .expect("unique bead upsert");
    let event_a = event_body_with_delta(50, delta_a);
    apply_event(&mut state_a, &event_a).expect("apply a");

    let mut state_b = CanonicalState::new();
    let mut delta_b = TxnDeltaV1::new();
    delta_b
        .insert(TxnOpV1::BeadUpsert(Box::new(patch_b)))
        .expect("unique bead upsert");
    let event_b = event_body_with_delta(51, delta_b);
    apply_event(&mut state_b, &event_b).expect("apply b");

    let merged_ab = CanonicalState::join(&state_a, &state_b).expect("merge ab");
    let merged_ba = CanonicalState::join(&state_b, &state_a).expect("merge ba");

    let winner_ab = merged_ab.get_live(&bead).expect("winner");
    let winner_ba = merged_ba.get_live(&bead).expect("winner");
    assert_eq!(winner_ab.title(), "title-b");
    assert_eq!(winner_ba.title(), "title-b");

    let loser_stamp = Stamp::new(WriteStamp::new(1000, 0), actor_id(1));
    assert!(merged_ab.has_lineage_tombstone(&bead, &loser_stamp));
    assert!(merged_ba.has_lineage_tombstone(&bead, &loser_stamp));
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
fn orphan_label_and_note_ops_become_visible_after_create() {
    let mut state = CanonicalState::new();
    let bead = bead_id(40);

    let keep_label = Label::parse("alpha").expect("label");
    let remove_label = Label::parse("beta").expect("label");
    let replica = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
    let remove_replica = ReplicaId::new(Uuid::from_bytes([2u8; 16]));

    let note = sample_note(3, 1);
    let note_id = note.id.clone();

    let mut delta = TxnDeltaV1::new();
    delta
        .insert(TxnOpV1::LabelAdd(WireLabelAddV1 {
            bead_id: bead.clone(),
            label: keep_label.clone(),
            dot: WireDotV1 {
                replica: replica.clone(),
                counter: 1,
            },
            lineage: None,
        }))
        .expect("unique label add");
    delta
        .insert(TxnOpV1::LabelRemove(WireLabelRemoveV1 {
            bead_id: bead.clone(),
            label: remove_label.clone(),
            ctx: WireDvvV1 {
                max: BTreeMap::from([(remove_replica.clone(), 1)]),
                dots: Vec::new(),
            },
            lineage: None,
        }))
        .expect("unique label remove");
    delta
        .insert(TxnOpV1::NoteAppend(NoteAppendV1 {
            bead_id: bead.clone(),
            note,
            lineage: None,
        }))
        .expect("unique note append");

    let orphan_event = event_body_with_delta(10, delta);
    apply_event(&mut state, &orphan_event).expect("apply orphan ops");

    assert!(state.labels_for(&bead).is_empty());
    assert!(state.notes_for(&bead).is_empty());

    let create_event = bead_create_event(40);
    apply_event(&mut state, &create_event).expect("apply bead create");

    let labels = state.labels_for(&bead);
    assert!(labels.contains(keep_label.as_str()));
    assert!(!labels.contains(remove_label.as_str()));
    let notes = state.notes_for(&bead);
    assert!(notes.iter().any(|note| note.id == note_id));
}

#[test]
fn orphan_dep_ops_become_visible_after_create() {
    let mut state = CanonicalState::new();
    let from = bead_id(41);
    let to = bead_id(42);
    let to_removed = bead_id(43);
    let replica = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
    let remove_replica = ReplicaId::new(Uuid::from_bytes([3u8; 16]));

    let mut delta = TxnDeltaV1::new();
    delta
        .insert(TxnOpV1::DepAdd(WireDepAddV1 {
            key: DepKey::new(from.clone(), to.clone(), DepKind::Blocks).unwrap(),
            dot: WireDotV1 {
                replica: replica.clone(),
                counter: 1,
            },
        }))
        .expect("unique dep add");
    delta
        .insert(TxnOpV1::DepRemove(WireDepRemoveV1 {
            key: DepKey::new(from.clone(), to_removed.clone(), DepKind::Related).unwrap(),
            ctx: WireDvvV1 {
                max: BTreeMap::from([(remove_replica.clone(), 5)]),
                dots: Vec::new(),
            },
        }))
        .expect("unique dep remove");

    let orphan_event = event_body_with_delta(12, delta);
    apply_event(&mut state, &orphan_event).expect("apply orphan deps");

    assert!(state.deps_from(&from).is_empty());
    assert!(state.deps_to(&to).is_empty());

    apply_event(&mut state, &bead_create_event(41)).expect("apply from");
    apply_event(&mut state, &bead_create_event(42)).expect("apply to");
    apply_event(&mut state, &bead_create_event(43)).expect("apply to removed");

    let expected = DepKey::new(from.clone(), to.clone(), DepKind::Blocks).expect("dep key");
    assert_eq!(state.deps_from(&from), vec![expected.clone()]);
    assert!(state.deps_to(&to).contains(&expected));
    assert!(state.deps_to(&to_removed).is_empty());
}

#[test]
fn label_dot_collision_is_deterministic() {
    let bead = bead_id(1);
    let mut state_a = CanonicalState::new();
    apply_event(&mut state_a, &sample_event_body(1)).expect("apply base");

    let mut state_b = CanonicalState::new();
    apply_event(&mut state_b, &sample_event_body(1)).expect("apply base");

    let label_low = Label::parse("alpha").expect("label");
    let label_high = Label::parse("beta").expect("label");
    let dot = WireDotV1 {
        replica: ReplicaId::new(Uuid::from_bytes([4u8; 16])),
        counter: 1,
    };

    let mut delta_low = TxnDeltaV1::new();
    delta_low
        .insert(TxnOpV1::LabelAdd(WireLabelAddV1 {
            bead_id: bead.clone(),
            label: label_low.clone(),
            dot,
            lineage: None,
        }))
        .expect("unique label add");
    let event_low = event_body_with_delta(10, delta_low);

    let mut delta_high = TxnDeltaV1::new();
    delta_high
        .insert(TxnOpV1::LabelAdd(WireLabelAddV1 {
            bead_id: bead.clone(),
            label: label_high.clone(),
            dot,
            lineage: None,
        }))
        .expect("unique label add");
    let event_high = event_body_with_delta(11, delta_high);

    apply_event(&mut state_a, &event_low).expect("apply low");
    apply_event(&mut state_a, &event_high).expect("apply high");

    apply_event(&mut state_b, &event_high).expect("apply high");
    apply_event(&mut state_b, &event_low).expect("apply low");

    let labels_a = state_a.labels_for(&bead);
    let labels_b = state_b.labels_for(&bead);
    assert_eq!(labels_a, labels_b);
    assert_eq!(labels_a.len(), 1);
    assert!(labels_a.contains(label_high.as_str()));
    assert_eq!(state_a.label_stamp(&bead), state_b.label_stamp(&bead));
}

#[test]
fn dep_dot_collision_is_deterministic() {
    let from = bead_id(1);
    let to_low = bead_id(2);
    let to_high = bead_id(3);

    let mut state_a = CanonicalState::new();
    apply_event(&mut state_a, &sample_event_body(1)).expect("apply base from");
    apply_event(&mut state_a, &sample_event_body(2)).expect("apply base to");
    apply_event(&mut state_a, &sample_event_body(3)).expect("apply base to");

    let mut state_b = CanonicalState::new();
    apply_event(&mut state_b, &sample_event_body(1)).expect("apply base from");
    apply_event(&mut state_b, &sample_event_body(2)).expect("apply base to");
    apply_event(&mut state_b, &sample_event_body(3)).expect("apply base to");

    let key_low = DepKey::new(from.clone(), to_low.clone(), DepKind::Blocks).expect("dep key");
    let key_high = DepKey::new(from.clone(), to_high.clone(), DepKind::Related).expect("dep key");
    let dot = WireDotV1 {
        replica: ReplicaId::new(Uuid::from_bytes([9u8; 16])),
        counter: 1,
    };

    let mut delta_low = TxnDeltaV1::new();
    delta_low
        .insert(TxnOpV1::DepAdd(WireDepAddV1 {
            key: DepKey::new(from.clone(), to_low.clone(), DepKind::Blocks).unwrap(),
            dot,
        }))
        .expect("unique dep add");
    let event_low = event_body_with_delta(12, delta_low);

    let mut delta_high = TxnDeltaV1::new();
    delta_high
        .insert(TxnOpV1::DepAdd(WireDepAddV1 {
            key: DepKey::new(from.clone(), to_high.clone(), DepKind::Related).unwrap(),
            dot,
        }))
        .expect("unique dep add");
    let event_high = event_body_with_delta(13, delta_high);

    apply_event(&mut state_a, &event_low).expect("apply low");
    apply_event(&mut state_a, &event_high).expect("apply high");

    apply_event(&mut state_b, &event_high).expect("apply high");
    apply_event(&mut state_b, &event_low).expect("apply low");

    let deps_a = state_a.deps_from(&from);
    let deps_b = state_b.deps_from(&from);
    assert_eq!(deps_a, deps_b);
    assert_eq!(deps_a.len(), 1);
    let expected = if key_low > key_high {
        key_low.clone()
    } else {
        key_high.clone()
    };
    assert_eq!(deps_a[0], expected);
    assert_eq!(state_a.deps_to(&to_low).is_empty(), expected == key_high);
    assert_eq!(state_a.deps_to(&to_high).is_empty(), expected == key_low);
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
            key: DepKey::new(from.clone(), to.clone(), kind).unwrap(),
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
    assert!(
        state
            .dep_indexes()
            .out_edges(&from)
            .iter()
            .any(|(t, k)| t == &to && *k == kind)
    );
    assert!(
        state
            .dep_indexes()
            .in_edges(&to)
            .iter()
            .any(|(f, k)| f == &from && *k == kind)
    );

    let mut remove_delta = TxnDeltaV1::new();
    remove_delta
        .insert(TxnOpV1::DepRemove(WireDepRemoveV1 {
            key: DepKey::new(from.clone(), to.clone(), kind).unwrap(),
            ctx: WireDvvV1 {
                max: BTreeMap::from([(replica, 1)]),
                dots: Vec::new(),
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
            key: DepKey::new(from.clone(), to.clone(), kind).unwrap(),
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
    assert!(
        state
            .dep_indexes()
            .out_edges(&from)
            .iter()
            .any(|(t, k)| t == &to && *k == kind)
    );
    assert!(
        state
            .dep_indexes()
            .in_edges(&to)
            .iter()
            .any(|(f, k)| f == &from && *k == kind)
    );
}
