//! Model 6: Hash(preimage) + prev-link continuity verification.
//!
//! This is a *toy* stand-in for the "canonical CBOR + sha256 + prev_sha" rules.
//! It models these checks:
//! - store identity must match
//! - declared sha must equal sha(preimage)
//! - seq=1 must have prev=None
//! - seq>1 must have prev==expected_prev_head
//!
//! **Note:** your current types file verifies prev *before* gap-buffering.
//! That means an out-of-order delivery (seq=2 arriving before seq=1) is forced to fail.
//! The spec text you pasted earlier implied buffering out-of-order should be allowed.
//! This model is a nice place to decide which behavior you actually want.

use beads_stateright_models::spec::{NamespaceId, ReplicaId, Sha256, StoreEpoch, StoreIdentity, StoreId};
use beads_stateright_models::toy_codec::{make_event, verify_bytes, VerifyError, WireEnvelope};
use stateright::{Model, Property};
use std::time::Duration;
use uuid::Uuid;

const MAX_SEQ: u8 = 4;

#[derive(Clone, Debug)]
struct HashPrevModel {
    store: StoreIdentity,
    wrong_store: StoreIdentity,
    ns: NamespaceId,
    origin: ReplicaId,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum CorruptKind {
    Valid,
    WrongSha,
    WrongPrev,
    WrongStore,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Obs {
    kind: CorruptKind,
    seq: u8,
    expected_next: bool,
    ok: bool,
    err: Option<VerifyError>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    durable: u8,
    head: Option<Sha256>,
    closed: bool,
    last: Option<Obs>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Action {
    seq: u8,
    kind: CorruptKind,
}

impl HashPrevModel {
    /// Compute the *true* sha for event `seq` under the toy rules,
    /// assuming an honest sender that constructs the hash-chain prefix.
    fn chain_sha(&self, seq: u8) -> Sha256 {
        assert!(seq >= 1);
        let mut prev: Option<Sha256> = None;
        for s in 1..=seq {
            let ev = make_event(self.store, self.ns.clone(), self.origin, s, prev);
            prev = Some(ev.wire.sha);
        }
        prev.unwrap()
    }

    fn true_prev_for(&self, seq: u8) -> Option<Sha256> {
        if seq == 1 {
            None
        } else {
            Some(self.chain_sha(seq - 1))
        }
    }

    fn build_bytes(&self, seq: u8, kind: CorruptKind) -> Vec<u8> {
        let store = match kind {
            CorruptKind::WrongStore => self.wrong_store,
            _ => self.store,
        };

        // Start from an internally-consistent envelope (honest sender).
        let prev = self.true_prev_for(seq);
        let mut ev = make_event(store, self.ns.clone(), self.origin, seq, prev);

        match kind {
            CorruptKind::Valid | CorruptKind::WrongStore => {
                // leave as-is
            }
            CorruptKind::WrongPrev => {
                // Flip prev without changing sha (sha excludes prev in this spec).
                ev.wire.prev = Some(Sha256([0xAA; 32]));
            }
            CorruptKind::WrongSha => {
                // Flip sha without changing preimage.
                let mut b = ev.wire.sha.0;
                b[0] ^= 0x01;
                ev.wire.sha = Sha256(b);
            }
        }

        serde_json::to_vec(&ev.wire).expect("json encode")
    }
}

impl Model for HashPrevModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            durable: 0,
            head: None,
            closed: false,
            last: None,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.closed {
            return;
        }
        for seq in 1..=MAX_SEQ {
            for kind in [
                CorruptKind::Valid,
                CorruptKind::WrongSha,
                CorruptKind::WrongPrev,
                CorruptKind::WrongStore,
            ] {
                actions.push(Action { seq, kind });
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        if next.closed {
            return Some(next);
        }

        let expected_next = action.seq == next.durable + 1;
        let bytes = self.build_bytes(action.seq, action.kind.clone());

        let res = verify_bytes(self.store, next.head, &bytes);

        match res {
            Ok(verified) => {
                next.durable = verified.seq;
                next.head = Some(verified.sha);
                next.last = Some(Obs {
                    kind: action.kind,
                    seq: action.seq,
                    expected_next,
                    ok: true,
                    err: None,
                });
            }
            Err(e) => {
                next.closed = true;
                next.last = Some(Obs {
                    kind: action.kind,
                    seq: action.seq,
                    expected_next,
                    ok: false,
                    err: Some(e),
                });
            }
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // If the sender constructs a valid event and it is exactly the expected next seq,
            // verification must succeed.
            Property::always("valid expected-next events verify", |_, s| {
                match &s.last {
                    Some(obs) if obs.kind == CorruptKind::Valid && obs.expected_next => obs.ok,
                    _ => true,
                }
            }),

            // Any corruption kind should force failure.
            Property::always("corrupt events fail verification", |_, s| {
                match &s.last {
                    Some(obs) if obs.kind != CorruptKind::Valid => !obs.ok,
                    _ => true,
                }
            }),

            // Reachability: it's possible to verify a full prefix.
            Property::sometimes("can verify full prefix", |_, s| s.durable == MAX_SEQ),
        ]
    }
}

fn main() -> Result<(), pico_args::Error> {
    env_logger::init();

    let store = StoreIdentity {
        store_id: StoreId(Uuid::new_v4()),
        store_epoch: StoreEpoch(1),
    };
    let wrong_store = StoreIdentity {
        store_id: StoreId(Uuid::new_v4()),
        store_epoch: StoreEpoch(1),
    };

    let model = HashPrevModel {
        store,
        wrong_store,
        ns: NamespaceId("alpha".into()),
        origin: ReplicaId(Uuid::new_v4()),
    };

    model
        .checker()
        .threads(num_cpus::get())
        .timeout_duration(Duration::from_secs(60))
        .command(pico_args::Arguments::from_env().finish())?
        .run()
}
