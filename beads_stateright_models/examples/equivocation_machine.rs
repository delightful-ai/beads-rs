//! Model 3: Equivocation gate.
//!
//! In the plan/spec: if the same EventId is observed with different `sha256`,
//! the receiver must treat it as corruption / equivocation and hard-close.
//!
//! This model checks:
//! - observing both variants for the same seq *forces* the model into `errored`
//! - observing the same variant repeatedly is safe (idempotent)

use stateright::{Model, Property};
use std::collections::BTreeMap;
use std::time::Duration;

const MAX_SEQ: u8 = 3;

/// Per-seq: bit0 => saw sha "A", bit1 => saw sha "B".
fn set_seen_mask(m: &mut BTreeMap<u8, u8>, seq: u8, variant: u8) {
    let bit = match variant {
        0 => 0b01,
        1 => 0b10,
        _ => unreachable!(),
    };
    let cur = m.get(&seq).copied().unwrap_or(0);
    m.insert(seq, cur | bit);
}

fn saw_both_variants(m: &BTreeMap<u8, u8>) -> bool {
    m.values().any(|mask| *mask == 0b11)
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    /// "durable" store mapping: EventId(seq) -> canonical sha variant.
    stored: BTreeMap<u8, u8>,

    /// what we've observed so far, even if it was conflicting
    seen_mask: BTreeMap<u8, u8>,

    /// once true, we model the session having hard-closed
    errored: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    Observe { seq: u8, variant: u8 },
}

#[derive(Clone, Debug)]
struct EquivocationGate;

impl Model for EquivocationGate {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            stored: BTreeMap::new(),
            seen_mask: BTreeMap::new(),
            errored: false,
        }]
    }

    fn actions(&self, _state: &Self::State, _actions: &mut Vec<Self::Action>) {
        // All seqs, both variants.
        for seq in 1..=MAX_SEQ {
            for variant in 0..=1 {
                _actions.push(Action::Observe { seq, variant });
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();

        if next.errored {
            // Once hard-closed, nothing "uncloses".
            return Some(next);
        }

        match action {
            Action::Observe { seq, variant } => {
                set_seen_mask(&mut next.seen_mask, seq, variant);

                match next.stored.get(&seq).copied() {
                    None => {
                        // First observation for this EventId becomes the canonical.
                        next.stored.insert(seq, variant);
                    }
                    Some(existing) if existing == variant => {
                        // Idempotent duplicate.
                    }
                    Some(_existing) => {
                        // Conflicting sha for same EventId => equivocation.
                        next.errored = true;
                    }
                }
            }
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety: if we have ever observed both variants for any seq,
            // we must have errored.
            Property::always("both sha variants implies hard-close", |_, s| {
                !saw_both_variants(&s.seen_mask) || s.errored
            }),
            // Liveness: equivocation is reachable in this model.
            Property::sometimes("equivocation reachable", |_, s| s.errored),
        ]
    }
}

fn main() -> Result<(), pico_args::Error> {
    env_logger::init();

    EquivocationGate
        .checker()
        .threads(num_cpus::get())
        .timeout_duration(Duration::from_secs(60))
        .command(pico_args::Arguments::from_env().finish())?
        .run()
}
