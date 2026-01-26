//! Model 4: Gap-buffering + WANT logic (Plan §9.4 / §9.8 style).
//!
//! This is the "never skip gaps" machine.
//!
//! We model a single (namespace, origin) stream and only care about seq numbers:
//! - if seq == durable+1 => forward contiguous batch (including any buffered suffix)
//! - if seq  > durable+1 => buffer (bounded) and emit WANT(durable)
//! - if seq <= durable => duplicate noop

use stateright::{Checker, Model, Property, report::WriteReporter};
use std::collections::BTreeSet;
use std::time::Duration;

const MAX_SEQ: u8 = 4;
const MAX_BUFFER_EVENTS: usize = 3;
const GAP_TIMEOUT_TICKS: u8 = 2;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    now: u8,
    durable: u8,
    buffered: BTreeSet<u8>,
    gap_started_at: Option<u8>,

    // For checking properties.
    last_want_from: Option<u8>,
    last_forwarded: Vec<u8>,

    rejected: bool,
    reject_code: Option<&'static str>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    Tick,
    Deliver(u8),
}

#[derive(Clone, Debug)]
struct GapWant;

impl Model for GapWant {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            now: 0,
            durable: 0,
            buffered: BTreeSet::new(),
            gap_started_at: None,
            last_want_from: None,
            last_forwarded: vec![],
            rejected: false,
            reject_code: None,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.rejected {
            return;
        }
        actions.push(Action::Tick);
        for seq in 1..=MAX_SEQ {
            actions.push(Action::Deliver(seq));
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        next.last_want_from = None;
        next.last_forwarded.clear();

        if next.rejected {
            return Some(next);
        }

        match action {
            Action::Tick => {
                next.now = next.now.saturating_add(1);
            }
            Action::Deliver(seq) => {
                // Duplicate noop.
                if seq <= next.durable {
                    return Some(next);
                }

                // Gap handling.
                if seq != next.durable + 1 {
                    // Start or continue the gap timer.
                    match next.gap_started_at {
                        None => next.gap_started_at = Some(next.now),
                        Some(start) => {
                            if next.now.saturating_sub(start) > GAP_TIMEOUT_TICKS {
                                next.rejected = true;
                                next.reject_code = Some("gap_timeout");
                                return Some(next);
                            }
                        }
                    }

                    // Bounded buffering.
                    if next.buffered.len() >= MAX_BUFFER_EVENTS {
                        next.rejected = true;
                        next.reject_code = Some("gap_buffer_overflow");
                        return Some(next);
                    }

                    next.buffered.insert(seq);
                    next.last_want_from = Some(next.durable);
                    return Some(next);
                }

                // Contiguous: forward (seq plus any now-contiguous buffered suffix).
                let mut batch = vec![seq];
                let mut cursor = seq.saturating_add(1);
                while next.buffered.remove(&cursor) {
                    batch.push(cursor);
                    cursor = cursor.saturating_add(1);
                }

                next.durable = *batch.last().unwrap();
                next.last_forwarded = batch;

                if next.buffered.is_empty() {
                    next.gap_started_at = None;
                }
            }
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety: forwarded batches are always contiguous, and end exactly at `durable`.
            Property::always("forwarded batches are contiguous", |_, s: &State| {
                if s.last_forwarded.is_empty() {
                    return true;
                }
                let batch = &s.last_forwarded;
                // End of batch == durable.
                if *batch.last().unwrap() != s.durable {
                    return false;
                }
                // Strictly increasing by 1.
                for w in batch.windows(2) {
                    if w[1] != w[0] + 1 {
                        return false;
                    }
                }
                true
            }),
            // Safety: nothing buffered is <= durable+1; if it were, we should have forwarded it.
            Property::always(
                "buffered items are strictly beyond the next expected",
                |_, s: &State| s.buffered.iter().all(|b| *b > s.durable + 1),
            ),
            // Liveness-ish: it's possible to reach durable==MAX_SEQ.
            Property::sometimes("can fully catch up", |_, s: &State| s.durable == MAX_SEQ),
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
            println!("Exploring gap/WANT state space on {address}.");
            GapWant
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking gap/WANT semantics.");
            GapWant
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  gap_want_machine check");
            println!("  gap_want_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
