//! Model: Durability coordination + ReplicatedFsync(k) quorum semantics.
//!
//! Plan alignment:
//! - Durability coordination + classes: REALTIME_PLAN.md §10.1-§10.5
//! - Applied vs durable watermarks: REALTIME_PLAN.md §0.12
//! - Timeout + receipt semantics: REALTIME_PLAN.md §0.11
//!
//! This model keeps a single in-flight client write and explicitly tracks
//! per-event persistence (fsync) so "durable" is derived from contiguous
//! persisted events, not just a counter.

use stateright::{report::WriteReporter, Checker, Model, Property};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const MAX_SEQ: u8 = 3;
const TIMEOUT_TICKS: u8 = 3;
const MAX_K: u8 = 3;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Replica {
    R1,
    R2,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum DurabilityClass {
    LocalFsync,
    ReplicatedFsync(u8),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Pending {
    seq: u8,
    class: DurabilityClass,
    started_at: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PeerState {
    eligible: bool,
    persisted: BTreeSet<u8>,
    durable: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum ResultKind {
    Success { seq: u8, class: DurabilityClass },
    Timeout { seq: u8 },
    Unavailable { k: u8 },
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    now: u8,
    appended: u8,
    local_persisted: BTreeSet<u8>,
    local_durable: u8,
    local_applied: u8,
    peers: BTreeMap<Replica, PeerState>,
    pending: Option<Pending>,
    last_result: Option<ResultKind>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    Tick,
    ClientWrite(DurabilityClass),
    LocalPersist(u8),
    LocalApply,
    RemotePersist { replica: Replica, seq: u8 },
    RemoteAck(Replica),
}

#[derive(Clone, Debug)]
struct DurabilityQuorum;

fn contiguous_prefix(persisted: &BTreeSet<u8>) -> u8 {
    let mut max = 0;
    while persisted.contains(&(max + 1)) {
        max += 1;
        if max == MAX_SEQ {
            break;
        }
    }
    max
}

fn eligible_total(state: &State) -> u8 {
    let mut count = 1; // local replica is always eligible
    for peer in state.peers.values() {
        if peer.eligible {
            count += 1;
        }
    }
    count
}

fn eligible_durable_count(state: &State, seq: u8) -> u8 {
    let mut count = 0;
    if state.local_durable >= seq {
        count += 1;
    }
    for peer in state.peers.values() {
        if peer.eligible && peer.durable >= seq {
            count += 1;
        }
    }
    count
}

fn pending_satisfied(state: &State, pending: &Pending) -> bool {
    if state.local_durable < pending.seq || state.local_applied < pending.seq {
        return false;
    }
    match pending.class {
        DurabilityClass::LocalFsync => true,
        DurabilityClass::ReplicatedFsync(k) => eligible_durable_count(state, pending.seq) >= k,
    }
}

fn maybe_finalize(state: &mut State) {
    let pending = match state.pending {
        Some(ref pending) => pending,
        None => return,
    };

    if pending_satisfied(state, pending) {
        state.last_result = Some(ResultKind::Success {
            seq: pending.seq,
            class: pending.class,
        });
        state.pending = None;
        return;
    }

    if state.now.saturating_sub(pending.started_at) >= TIMEOUT_TICKS
        && state.local_durable >= pending.seq
        && state.local_applied >= pending.seq
    {
        state.last_result = Some(ResultKind::Timeout { seq: pending.seq });
        state.pending = None;
    }
}

impl Model for DurabilityQuorum {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut peers = BTreeMap::new();
        peers.insert(
            Replica::R1,
            PeerState {
                eligible: true,
                persisted: BTreeSet::new(),
                durable: 0,
            },
        );
        peers.insert(
            Replica::R2,
            PeerState {
                eligible: false,
                persisted: BTreeSet::new(),
                durable: 0,
            },
        );

        vec![State {
            now: 0,
            appended: 0,
            local_persisted: BTreeSet::new(),
            local_durable: 0,
            local_applied: 0,
            peers,
            pending: None,
            last_result: None,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        actions.push(Action::Tick);

        if state.pending.is_none() && state.appended < MAX_SEQ {
            actions.push(Action::ClientWrite(DurabilityClass::LocalFsync));
            for k in 1..=MAX_K {
                actions.push(Action::ClientWrite(DurabilityClass::ReplicatedFsync(k)));
            }
        }

        if state.local_applied < state.appended {
            actions.push(Action::LocalApply);
        }

        for seq in 1..=state.appended {
            if !state.local_persisted.contains(&seq) {
                actions.push(Action::LocalPersist(seq));
            }
        }

        for (replica, peer) in state.peers.iter() {
            for seq in 1..=state.appended {
                if !peer.persisted.contains(&seq) {
                    actions.push(Action::RemotePersist {
                        replica: *replica,
                        seq,
                    });
                }
            }

            if peer.durable < contiguous_prefix(&peer.persisted) {
                actions.push(Action::RemoteAck(*replica));
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        next.last_result = None;

        match action {
            Action::Tick => {
                next.now = next.now.saturating_add(1);
            }
            Action::ClientWrite(class) => {
                if next.pending.is_some() || next.appended >= MAX_SEQ {
                    return Some(next);
                }
                if let DurabilityClass::ReplicatedFsync(k) = class {
                    if eligible_total(&next) < k {
                        next.last_result = Some(ResultKind::Unavailable { k });
                        return Some(next);
                    }
                }
                next.appended = next.appended.saturating_add(1);
                next.pending = Some(Pending {
                    seq: next.appended,
                    class,
                    started_at: next.now,
                });
            }
            Action::LocalPersist(seq) => {
                if seq <= next.appended && next.local_persisted.insert(seq) {
                    next.local_durable = contiguous_prefix(&next.local_persisted);
                }
            }
            Action::LocalApply => {
                if next.local_applied < next.appended {
                    next.local_applied = next.local_applied.saturating_add(1);
                }
            }
            Action::RemotePersist { replica, seq } => {
                if let Some(peer) = next.peers.get_mut(&replica) {
                    if seq <= next.appended {
                        peer.persisted.insert(seq);
                    }
                }
            }
            Action::RemoteAck(replica) => {
                if let Some(peer) = next.peers.get_mut(&replica) {
                    let max = contiguous_prefix(&peer.persisted);
                    if peer.durable < max {
                        peer.durable += 1;
                    }
                }
            }
        }

        maybe_finalize(&mut next);
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no false durability", |_, s: &State| {
                if s.local_durable != contiguous_prefix(&s.local_persisted) {
                    return false;
                }
                if s.local_durable > s.appended || s.local_applied > s.appended {
                    return false;
                }
                for peer in s.peers.values() {
                    let max = contiguous_prefix(&peer.persisted);
                    if peer.durable > max || max > s.appended {
                        return false;
                    }
                }
                true
            }),
            Property::always("quorum correctness for k", |_, s: &State| {
                match s.last_result {
                    Some(ResultKind::Success {
                        seq,
                        class: DurabilityClass::ReplicatedFsync(k),
                    }) => eligible_durable_count(s, seq) >= k,
                    _ => true,
                }
            }),
            Property::always("ineligible replicas never satisfy quorum", |_, s: &State| {
                match s.last_result {
                    Some(ResultKind::Success {
                        seq,
                        class: DurabilityClass::ReplicatedFsync(k),
                    }) => {
                        let eligible = eligible_durable_count(s, seq);
                        eligible >= k
                    }
                    _ => true,
                }
            }),
            Property::always("honest timeout receipts", |_, s: &State| {
                match s.last_result {
                    Some(ResultKind::Timeout { seq }) => {
                        s.local_durable >= seq && s.local_applied >= seq && s.pending.is_none()
                    }
                    _ => true,
                }
            }),
            Property::always("durability unavailable is fail-fast", |_, s: &State| {
                match s.last_result {
                    Some(ResultKind::Unavailable { k }) => eligible_total(s) < k,
                    _ => true,
                }
            }),
        ]
    }
}

fn main() -> Result<(), pico_args::Error> {
    env_logger::init();

    let mut args = pico_args::Arguments::from_env();
    match args.subcommand()?.as_deref() {
        Some("explore") => {
            let address = args
                .opt_free_from_str()?
                .unwrap_or("localhost:3000".to_string());
            println!("Exploring durability coordinator state space on {address}.");
            DurabilityQuorum
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking durability coordinator.");
            DurabilityQuorum
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  durability_quorum_machine check");
            println!("  durability_quorum_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
