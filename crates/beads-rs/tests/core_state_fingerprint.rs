//! Property tests for CRDT state fingerprint equivalence.
//!
//! These tests verify that CanonicalState's CRDT properties hold by comparing
//! serialized representations. They depend on git::wire serialization and thus
//! live in beads-rs rather than beads-core.

use beads_rs::core::bead::{Bead, BeadCore, BeadFields};
use beads_rs::core::crdt::Crdt;
use beads_rs::core::composite::{Claim, Workflow};
use beads_rs::core::crdt::Lww;
use beads_rs::core::domain::{BeadType, DepKind, Priority};
use beads_rs::core::identity::{ActorId, BeadId, ReplicaId};
use beads_rs::core::orset::Dot;
use beads_rs::core::state::CanonicalState;
use beads_rs::core::time::{Stamp, WriteStamp};
use beads_rs::core::tombstone::Tombstone;
use beads_rs::core::{DepKey, ParentEdge};
use beads_rs::git::wire;
use proptest::prelude::*;
use uuid::Uuid;

fn actor_id(actor: &str) -> ActorId {
    ActorId::new(actor).unwrap_or_else(|e| panic!("invalid actor id {actor}: {e}"))
}

fn bead_id(id: &str) -> BeadId {
    BeadId::parse(id).unwrap_or_else(|e| panic!("invalid bead id {id}: {e}"))
}

fn make_bead(id: &BeadId, stamp: &Stamp) -> Bead {
    let core = BeadCore::new(id.clone(), stamp.clone(), None);
    let fields = BeadFields {
        title: Lww::new("title".to_string(), stamp.clone()),
        description: Lww::new(String::new(), stamp.clone()),
        design: Lww::new(None, stamp.clone()),
        acceptance_criteria: Lww::new(None, stamp.clone()),
        priority: Lww::new(Priority::default(), stamp.clone()),
        bead_type: Lww::new(BeadType::Task, stamp.clone()),
        external_ref: Lww::new(None, stamp.clone()),
        source_repo: Lww::new(None, stamp.clone()),
        estimated_minutes: Lww::new(None, stamp.clone()),
        workflow: Lww::new(Workflow::default(), stamp.clone()),
        claim: Lww::new(Claim::default(), stamp.clone()),
    };
    Bead::new(core, fields)
}

#[derive(Clone, Debug)]
enum Entry {
    Live { id: String, stamp: Stamp },
    Tombstone { id: String, stamp: Stamp },
}

fn state_fingerprint(state: &CanonicalState) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let state_bytes =
        wire::serialize_state(state).unwrap_or_else(|e| panic!("serialize state failed: {e}"));
    let tomb_bytes = wire::serialize_tombstones(state)
        .unwrap_or_else(|e| panic!("serialize tombstones failed: {e}"));
    let deps_bytes =
        wire::serialize_deps(state).unwrap_or_else(|e| panic!("serialize deps failed: {e}"));
    (state_bytes, tomb_bytes, deps_bytes)
}

fn base58_id_strategy() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[1-9A-HJ-NP-Za-km-z]{5,8}")
        .unwrap_or_else(|e| panic!("regex failed: {e}"))
        .prop_map(|suffix| format!("bd-{suffix}"))
}

fn stamp_strategy() -> impl Strategy<Value = Stamp> {
    let actor = prop_oneof![Just("alice"), Just("bob"), Just("carol")];
    (0u64..10_000, 0u32..5, actor).prop_map(|(wall_ms, counter, actor)| {
        Stamp::new(WriteStamp::new(wall_ms, counter), actor_id(actor))
    })
}

fn entry_strategy() -> impl Strategy<Value = Entry> {
    (base58_id_strategy(), stamp_strategy(), any::<bool>()).prop_map(|(id, stamp, is_live)| {
        if is_live {
            Entry::Live { id, stamp }
        } else {
            Entry::Tombstone { id, stamp }
        }
    })
}

fn dep_strategy() -> impl Strategy<Value = (DepKey, Dot, Stamp)> {
    let non_parent_kind = prop_oneof![
        Just(DepKind::Blocks),
        Just(DepKind::Related),
        Just(DepKind::DiscoveredFrom),
    ];
    let replica = any::<u128>().prop_map(|raw| ReplicaId::new(Uuid::from_u128(raw)));
    let dot_non_parent =
        (replica.clone(), 0u64..10_000).prop_map(|(replica, counter)| Dot { replica, counter });
    let dot_parent =
        (replica, 0u64..10_000).prop_map(|(replica, counter)| Dot { replica, counter });

    let non_parent = (
        base58_id_strategy(),
        base58_id_strategy(),
        non_parent_kind,
        dot_non_parent,
        stamp_strategy(),
    )
        .prop_filter("deps cannot be self-referential", |(from, to, _, _, _)| {
            from != to
        })
        .prop_map(|(from, to, kind, dot, stamp)| {
            let key = DepKey::new(bead_id(&from), bead_id(&to), kind)
                .unwrap_or_else(|e| panic!("dep key invalid: {}", e));
            (key, dot, stamp)
        });

    let parent = (
        base58_id_strategy(),
        base58_id_strategy(),
        dot_parent,
        stamp_strategy(),
    )
        .prop_filter(
            "deps cannot be self-referential",
            |(child, parent, _, _)| child != parent,
        )
        .prop_map(|(child, parent, dot, stamp)| {
            let edge = ParentEdge::new(bead_id(&child), bead_id(&parent))
                .unwrap_or_else(|e| panic!("parent edge invalid: {}", e));
            (edge.to_dep_key(), dot, stamp)
        });

    prop_oneof![non_parent, parent]
}

fn state_strategy() -> impl Strategy<Value = CanonicalState> {
    (
        prop::collection::vec(entry_strategy(), 0..12),
        prop::collection::vec(dep_strategy(), 0..12),
    )
        .prop_map(|(entries, deps)| {
            let mut state = CanonicalState::new();
            for entry in entries {
                match entry {
                    Entry::Live { id, stamp } => {
                        let bead = make_bead(&bead_id(&id), &stamp);
                        if let Err(err) = state.insert(bead) {
                            panic!("insert bead failed: {err:?}");
                        }
                    }
                    Entry::Tombstone { id, stamp } => {
                        state.delete(Tombstone::new(bead_id(&id), stamp, None));
                    }
                }
            }
            for (key, dot, stamp) in deps {
                if let Ok(key) = state.check_dep_add_key(key) {
                    state.apply_dep_add(key, dot, stamp);
                }
            }
            state
        })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

    #[test]
    fn join_commutative(a in state_strategy(), b in state_strategy()) {
        let ab = a.join(&b);
        let ba = b.join(&a);
        prop_assert_eq!(state_fingerprint(&ab), state_fingerprint(&ba));
    }

    #[test]
    fn join_commutative_with_collision(
        stamp_a in stamp_strategy(),
        stamp_b in stamp_strategy(),
    ) {
        prop_assume!(stamp_a != stamp_b);
        let id = bead_id("bd-collision");
        let mut state_a = CanonicalState::new();
        state_a.insert_live(make_bead(&id, &stamp_a));
        let mut state_b = CanonicalState::new();
        state_b.insert_live(make_bead(&id, &stamp_b));

        let ab = state_a.join(&state_b);
        let ba = state_b.join(&state_a);
        prop_assert_eq!(state_fingerprint(&ab), state_fingerprint(&ba));
    }

    #[test]
    fn join_idempotent(a in state_strategy()) {
        let aa = a.join(&a);
        prop_assert_eq!(state_fingerprint(&aa), state_fingerprint(&a));
    }

    #[test]
    fn join_associative(a in state_strategy(), b in state_strategy(), c in state_strategy()) {
        let ab = a.join(&b);
        let left = ab.join(&c);
        let bc = b.join(&c);
        let right = a.join(&bc);
        prop_assert_eq!(state_fingerprint(&left), state_fingerprint(&right));
    }
}
