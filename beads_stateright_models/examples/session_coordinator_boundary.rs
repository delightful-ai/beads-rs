//! Model 5: Session + coordinator boundary (Plan §0.13) with gap buffering.
//!
//! This ties together:
//! - the gap/WANT machine
//! - the "session does not mutate durable itself" rule
//! - a coordinator that ingests only contiguous batches
//!
//! **Intentionally abstract**:
//! - no hashing, no bytes, no namespaces. We model one (ns, origin) stream.

use stateright::{report::WriteReporter, Checker, Model, Property};
use std::collections::BTreeSet;
use std::time::Duration;

const MAX_SEQ: u8 = 4;
const MAX_BUFFER_EVENTS: usize = 3;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct State {
    // Session view.
    pub session_durable: u8,
    pub buffered: BTreeSet<u8>,
    pub pending_ingest: Option<Vec<u8>>,
    pub pending_ack: Option<u8>,
    pub last_want_from: Option<u8>,

    // Coordinator truth.
    pub coord_durable: u8,
    pub coord_log: BTreeSet<u8>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum Action {
    DeliverEvent(u8),
    CoordinatorIngest,
    DeliverAck,
}

#[derive(Clone, Debug)]
pub struct SessionCoordinator;

impl Model for SessionCoordinator {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            session_durable: 0,
            buffered: BTreeSet::new(),
            pending_ingest: None,
            pending_ack: None,
            last_want_from: None,
            coord_durable: 0,
            coord_log: BTreeSet::new(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        for s in 1..=MAX_SEQ {
            actions.push(Action::DeliverEvent(s));
        }
        if state.pending_ingest.is_some() {
            actions.push(Action::CoordinatorIngest);
        }
        if state.pending_ack.is_some() {
            actions.push(Action::DeliverAck);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        next.last_want_from = None;

        match action {
            Action::DeliverEvent(seq) => {
                // If an ingest is in-flight, we only buffer and/or ignore.
                if next.pending_ingest.is_some() {
                    if seq <= next.session_durable {
                        return Some(next);
                    }
                    let expected = next.session_durable + 1;
                    if seq == expected {
                        // This is the same head we're already sending; treat as dup.
                        return Some(next);
                    }
                    if next.buffered.len() >= MAX_BUFFER_EVENTS {
                        // In the real system we'd close. Here we just drop to keep the model small.
                        return Some(next);
                    }
                    next.buffered.insert(seq);
                    next.last_want_from = Some(next.session_durable);
                    return Some(next);
                }

                // Normal ingest path.
                if seq <= next.session_durable {
                    return Some(next);
                }

                let expected = next.session_durable + 1;
                if seq != expected {
                    if next.buffered.len() < MAX_BUFFER_EVENTS {
                        next.buffered.insert(seq);
                    }
                    next.last_want_from = Some(next.session_durable);
                    return Some(next);
                }

                // Contiguous head arrived: build batch from buffered suffix.
                let mut batch = vec![seq];
                let mut cursor = seq.saturating_add(1);
                while next.buffered.remove(&cursor) {
                    batch.push(cursor);
                    cursor = cursor.saturating_add(1);
                }

                next.pending_ingest = Some(batch);
            }

            Action::CoordinatorIngest => {
                let batch = next.pending_ingest.take().unwrap();

                // Coordinator only ingests if batch is contiguous from its durable+1.
                // (In the real system, this is guaranteed by the session and enforced again
                // at the coordinator boundary.)
                let expected = next.coord_durable + 1;
                if batch.first().copied() != Some(expected) {
                    // Drop in toy model.
                    return Some(next);
                }

                for &s in &batch {
                    next.coord_log.insert(s);
                }
                next.coord_durable = *batch.last().unwrap();
                next.pending_ack = Some(next.coord_durable);
            }

            Action::DeliverAck => {
                let ack = next.pending_ack.take().unwrap();
                // Session updates its durable watermark only on coordinator ACK.
                next.session_durable = next.session_durable.max(ack);
                next.buffered.retain(|&seq| seq > next.session_durable);

                // Drop any stale in-flight batch that the ACK already covered.
                if next
                    .pending_ingest
                    .as_ref()
                    .and_then(|batch| batch.first().copied())
                    .is_some_and(|first| first <= next.session_durable)
                {
                    next.pending_ingest = None;
                }

                // Opportunistic flush: if the next expected is already buffered, schedule
                // another ingest immediately.
                let expected = next.session_durable + 1;
                if next.pending_ingest.is_none() && next.buffered.contains(&expected) {
                    next.buffered.remove(&expected);
                    let mut batch = vec![expected];
                    let mut cursor = expected.saturating_add(1);
                    while next.buffered.remove(&cursor) {
                        batch.push(cursor);
                        cursor = cursor.saturating_add(1);
                    }
                    next.pending_ingest = Some(batch);
                }
            }
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety: session never "gets ahead" of coordinator truth.
            Property::always("session durable never exceeds coordinator durable", |_, s: &State| {
                s.session_durable <= s.coord_durable
            }),

            // Safety: any pending ingest must start at session_durable+1.
            Property::always("pending ingest starts at next expected", |_, s: &State| {
                if let Some(batch) = &s.pending_ingest {
                    let expected = s.session_durable + 1;
                    batch.first().copied() == Some(expected)
                        && batch
                            .windows(2)
                            .all(|w| w[1] == w[0] + 1)
                } else {
                    true
                }
            }),

            // Safety: coordinator durable is exactly the max element in the log (contiguous ingest).
            Property::always("coordinator durable matches its log max", |_, s: &State| {
                let max = s.coord_log.iter().copied().max().unwrap_or(0);
                max == s.coord_durable
            }),

            // Liveness-ish: system can reach full catch-up.
            Property::sometimes("can fully replicate", |_, s: &State| {
                s.coord_durable == MAX_SEQ && s.session_durable == MAX_SEQ
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
            println!("Exploring session/coordinator boundary on {address}.");
            SessionCoordinator
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking session/coordinator boundary.");
            SessionCoordinator
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  session_coordinator_boundary check");
            println!("  session_coordinator_boundary explore [ADDRESS]");
        }
    }

    Ok(())
}
