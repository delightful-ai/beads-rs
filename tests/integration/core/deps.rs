use std::collections::{BTreeMap, BTreeSet};

use beads_rs::git::wire::serialize_deps;
use beads_rs::{
    BeadId, CanonicalState, DepKey, DepKind, ReplicaId, TxnDeltaV1, TxnOpV1, WireDepAddV1,
    WireDepRemoveV1, WireDotV1, WireDvvV1, apply_event,
};
use serde_json::Value;
use uuid::Uuid;

use crate::fixtures::event_body::{bead_id, event_body_with_delta, sample_bead_patch};

fn apply_bead(state: &mut CanonicalState, seed: u8) {
    let mut delta = TxnDeltaV1::new();
    delta
        .insert(TxnOpV1::BeadUpsert(Box::new(sample_bead_patch(seed))))
        .expect("unique bead upsert");
    let event = event_body_with_delta(seed, delta);
    apply_event(state, &event).expect("apply bead");
}

fn dep_keys_from_serialize(state: &CanonicalState) -> BTreeSet<DepKey> {
    let bytes = serialize_deps(state).expect("serialize deps");
    let value: Value = serde_json::from_slice(&bytes).expect("dep store json");
    let entries = value
        .get("entries")
        .and_then(|v| v.as_array())
        .expect("dep entries");

    let mut keys = BTreeSet::new();
    for entry in entries {
        let key_value = entry.get("key").expect("dep key");
        let from = key_value
            .get("from")
            .and_then(|v| v.as_str())
            .expect("dep from");
        let to = key_value
            .get("to")
            .and_then(|v| v.as_str())
            .expect("dep to");
        let kind = key_value
            .get("kind")
            .and_then(|v| v.as_str())
            .expect("dep kind");
        let key = DepKey::new(
            BeadId::parse(from).expect("from id"),
            BeadId::parse(to).expect("to id"),
            DepKind::parse(kind).expect("dep kind"),
        )
        .expect("dep key");
        keys.insert(key);
    }
    keys
}

#[test]
fn dep_add_remove_is_idempotent() {
    let mut state = CanonicalState::new();
    apply_bead(&mut state, 50);
    apply_bead(&mut state, 51);

    let from = bead_id(50);
    let to = bead_id(51);
    let replica = ReplicaId::new(Uuid::from_bytes([4u8; 16]));
    let key = DepKey::new(from.clone(), to.clone(), DepKind::Blocks).expect("dep key");

    let mut add_delta = TxnDeltaV1::new();
    add_delta
        .insert(TxnOpV1::DepAdd(WireDepAddV1 {
            from: from.clone(),
            to: to.clone(),
            kind: DepKind::Blocks,
            dot: WireDotV1 {
                replica: replica.clone(),
                counter: 1,
            },
        }))
        .expect("unique dep add");
    let add_event = event_body_with_delta(10, add_delta);

    let first = apply_event(&mut state, &add_event).expect("apply add");
    let second = apply_event(&mut state, &add_event).expect("apply add again");
    assert!(first.changed_deps.contains(&key));
    assert!(second.changed_deps.is_empty());
    assert_eq!(state.deps_from(&from), vec![key.clone()]);

    let mut remove_delta = TxnDeltaV1::new();
    remove_delta
        .insert(TxnOpV1::DepRemove(WireDepRemoveV1 {
            from: from.clone(),
            to: to.clone(),
            kind: DepKind::Blocks,
            ctx: WireDvvV1 {
                max: BTreeMap::from([(replica.clone(), 1)]),
                dots: Vec::new(),
            },
        }))
        .expect("unique dep remove");
    let remove_event = event_body_with_delta(11, remove_delta);

    let first_remove = apply_event(&mut state, &remove_event).expect("apply remove");
    let second_remove = apply_event(&mut state, &remove_event).expect("apply remove again");
    assert!(first_remove.changed_deps.contains(&key));
    assert!(second_remove.changed_deps.is_empty());
    assert!(state.deps_from(&from).is_empty());
    assert!(state.deps_to(&to).is_empty());
}

#[test]
fn dep_indexes_match_dep_store() {
    let mut state = CanonicalState::new();
    apply_bead(&mut state, 60);
    apply_bead(&mut state, 61);
    apply_bead(&mut state, 62);

    let from = bead_id(60);
    let to_a = bead_id(61);
    let to_b = bead_id(62);

    let mut delta = TxnDeltaV1::new();
    delta
        .insert(TxnOpV1::DepAdd(WireDepAddV1 {
            from: from.clone(),
            to: to_a.clone(),
            kind: DepKind::Blocks,
            dot: WireDotV1 {
                replica: ReplicaId::new(Uuid::from_bytes([9u8; 16])),
                counter: 1,
            },
        }))
        .expect("unique dep add");
    delta
        .insert(TxnOpV1::DepAdd(WireDepAddV1 {
            from: from.clone(),
            to: to_b.clone(),
            kind: DepKind::Related,
            dot: WireDotV1 {
                replica: ReplicaId::new(Uuid::from_bytes([10u8; 16])),
                counter: 1,
            },
        }))
        .expect("unique dep add");
    let event = event_body_with_delta(12, delta);
    apply_event(&mut state, &event).expect("apply deps");

    let store_keys = dep_keys_from_serialize(&state);
    let mut index_keys = BTreeSet::new();
    let mut froms = BTreeSet::new();
    for key in &store_keys {
        froms.insert(key.from().clone());
    }
    for from in froms {
        for (to, kind) in state.dep_indexes().out_edges(&from) {
            let key = DepKey::new(from.clone(), to.clone(), *kind).expect("dep key");
            index_keys.insert(key);
        }
    }
    assert_eq!(index_keys, store_keys);
}

#[test]
fn dep_kind_ordering_in_serialize_is_canonical() {
    let mut state = CanonicalState::new();
    apply_bead(&mut state, 70);
    apply_bead(&mut state, 71);

    let from = bead_id(70);
    let to = bead_id(71);

    let mut delta = TxnDeltaV1::new();
    for (idx, kind) in [
        DepKind::Blocks,
        DepKind::DiscoveredFrom,
        DepKind::Parent,
        DepKind::Related,
    ]
    .iter()
    .enumerate()
    {
        delta
            .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                from: from.clone(),
                to: to.clone(),
                kind: *kind,
                dot: WireDotV1 {
                    replica: ReplicaId::new(Uuid::from_bytes([11u8; 16])),
                    counter: idx as u64 + 1,
                },
            }))
            .expect("unique dep add");
    }
    let event = event_body_with_delta(13, delta);
    apply_event(&mut state, &event).expect("apply deps");

    let bytes = serialize_deps(&state).expect("serialize deps");
    let value: Value = serde_json::from_slice(&bytes).expect("dep store json");
    let entries = value
        .get("entries")
        .and_then(|v| v.as_array())
        .expect("dep entries");

    let mut kinds = Vec::new();
    for entry in entries {
        let key_value = entry.get("key").expect("dep key");
        kinds.push(
            key_value
                .get("kind")
                .and_then(|v| v.as_str())
                .expect("dep kind")
                .to_string(),
        );
    }

    let expected = vec!["blocks", "discovered_from", "parent", "related"];
    assert_eq!(kinds, expected);
}

#[test]
fn dep_delete_then_readd_across_kinds() {
    let mut state = CanonicalState::new();
    apply_bead(&mut state, 80);
    apply_bead(&mut state, 81);

    let from = bead_id(80);
    let to = bead_id(81);

    let blocks_replica = ReplicaId::new(Uuid::from_bytes([12u8; 16]));
    let related_replica = ReplicaId::new(Uuid::from_bytes([13u8; 16]));

    let mut add_delta = TxnDeltaV1::new();
    add_delta
        .insert(TxnOpV1::DepAdd(WireDepAddV1 {
            from: from.clone(),
            to: to.clone(),
            kind: DepKind::Blocks,
            dot: WireDotV1 {
                replica: blocks_replica.clone(),
                counter: 1,
            },
        }))
        .expect("unique dep add");
    add_delta
        .insert(TxnOpV1::DepAdd(WireDepAddV1 {
            from: from.clone(),
            to: to.clone(),
            kind: DepKind::Related,
            dot: WireDotV1 {
                replica: related_replica.clone(),
                counter: 1,
            },
        }))
        .expect("unique dep add");
    let add_event = event_body_with_delta(14, add_delta);
    apply_event(&mut state, &add_event).expect("apply add");

    let mut remove_delta = TxnDeltaV1::new();
    remove_delta
        .insert(TxnOpV1::DepRemove(WireDepRemoveV1 {
            from: from.clone(),
            to: to.clone(),
            kind: DepKind::Blocks,
            ctx: WireDvvV1 {
                max: BTreeMap::from([(blocks_replica.clone(), 1)]),
                dots: Vec::new(),
            },
        }))
        .expect("unique dep remove");
    remove_delta
        .insert(TxnOpV1::DepRemove(WireDepRemoveV1 {
            from: from.clone(),
            to: to.clone(),
            kind: DepKind::Related,
            ctx: WireDvvV1 {
                max: BTreeMap::from([(related_replica.clone(), 1)]),
                dots: Vec::new(),
            },
        }))
        .expect("unique dep remove");
    let remove_event = event_body_with_delta(15, remove_delta);
    apply_event(&mut state, &remove_event).expect("apply remove");
    assert!(state.deps_from(&from).is_empty());

    let mut readd_delta = TxnDeltaV1::new();
    readd_delta
        .insert(TxnOpV1::DepAdd(WireDepAddV1 {
            from: from.clone(),
            to: to.clone(),
            kind: DepKind::Blocks,
            dot: WireDotV1 {
                replica: blocks_replica.clone(),
                counter: 2,
            },
        }))
        .expect("unique dep add");
    readd_delta
        .insert(TxnOpV1::DepAdd(WireDepAddV1 {
            from: from.clone(),
            to: to.clone(),
            kind: DepKind::Related,
            dot: WireDotV1 {
                replica: related_replica.clone(),
                counter: 2,
            },
        }))
        .expect("unique dep add");
    let readd_event = event_body_with_delta(16, readd_delta);
    apply_event(&mut state, &readd_event).expect("apply readd");

    let expected = BTreeSet::from([
        DepKey::new(from.clone(), to.clone(), DepKind::Blocks).expect("dep key"),
        DepKey::new(from.clone(), to.clone(), DepKind::Related).expect("dep key"),
    ]);
    let actual = state.deps_from(&from).into_iter().collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
}
