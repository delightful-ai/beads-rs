//! Model: Bounded buffers + fairness across namespaces.
//!
//! Plan alignment:
//! - Resource bounds: REALTIME_PLAN.md §0.19
//! - Fair scheduling across namespaces: REALTIME_PLAN.md §9.8
//!
//! This model has two namespaces (core, wf) and a round-robin scheduler.
//! It enforces per-namespace buffer caps and in-flight limits, and checks
//! that core cannot be starved under sustained wf churn.

use stateright::{report::WriteReporter, Checker, Model, Property};
use std::time::Duration;

const MAX_BUFFER: u8 = 3;
const MAX_IN_FLIGHT: u8 = 2;
const MAX_STARVE: u8 = 2;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Namespace {
    Core,
    Wf,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    core_queue: u8,
    wf_queue: u8,
    core_in_flight: u8,
    wf_in_flight: u8,
    next_ns: Namespace,
    core_wait: u8,
    closed: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    Enqueue(Namespace),
    Schedule,
    Ack(Namespace),
}

#[derive(Clone, Debug)]
struct FairnessBounds;

fn can_send(queue: u8, in_flight: u8) -> bool {
    queue > 0 && in_flight < MAX_IN_FLIGHT
}

impl Model for FairnessBounds {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            core_queue: 0,
            wf_queue: 0,
            core_in_flight: 0,
            wf_in_flight: 0,
            next_ns: Namespace::Core,
            core_wait: 0,
            closed: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.closed {
            return;
        }

        actions.push(Action::Enqueue(Namespace::Core));
        actions.push(Action::Enqueue(Namespace::Wf));

        if can_send(state.core_queue, state.core_in_flight)
            || can_send(state.wf_queue, state.wf_in_flight)
        {
            actions.push(Action::Schedule);
        }

        if state.core_in_flight > 0 {
            actions.push(Action::Ack(Namespace::Core));
        }
        if state.wf_in_flight > 0 {
            actions.push(Action::Ack(Namespace::Wf));
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();

        match action {
            Action::Enqueue(ns) => {
                if next.closed {
                    return Some(next);
                }
                let queue = match ns {
                    Namespace::Core => &mut next.core_queue,
                    Namespace::Wf => &mut next.wf_queue,
                };
                if *queue >= MAX_BUFFER {
                    next.closed = true;
                } else {
                    *queue = queue.saturating_add(1);
                }
            }
            Action::Schedule => {
                if next.closed {
                    return Some(next);
                }
                let core_can = can_send(next.core_queue, next.core_in_flight);
                let wf_can = can_send(next.wf_queue, next.wf_in_flight);

                let choose_core = match next.next_ns {
                    Namespace::Core if core_can => true,
                    Namespace::Wf if wf_can => false,
                    Namespace::Core if !core_can && wf_can => false,
                    Namespace::Wf if !wf_can && core_can => true,
                    _ => return Some(next),
                };

                if choose_core {
                    next.core_queue = next.core_queue.saturating_sub(1);
                    next.core_in_flight = next.core_in_flight.saturating_add(1);
                    next.next_ns = Namespace::Wf;
                } else {
                    next.wf_queue = next.wf_queue.saturating_sub(1);
                    next.wf_in_flight = next.wf_in_flight.saturating_add(1);
                    next.next_ns = Namespace::Core;
                }

                if core_can {
                    if choose_core {
                        next.core_wait = 0;
                    } else {
                        next.core_wait = next.core_wait.saturating_add(1);
                    }
                } else {
                    next.core_wait = 0;
                }
            }
            Action::Ack(ns) => match ns {
                Namespace::Core => {
                    if next.core_in_flight > 0 {
                        next.core_in_flight -= 1;
                    }
                }
                Namespace::Wf => {
                    if next.wf_in_flight > 0 {
                        next.wf_in_flight -= 1;
                    }
                }
            },
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("buffers never exceed bounds", |_, s: &State| {
                s.closed
                    || (s.core_queue <= MAX_BUFFER
                        && s.wf_queue <= MAX_BUFFER
                        && s.core_in_flight <= MAX_IN_FLIGHT
                        && s.wf_in_flight <= MAX_IN_FLIGHT)
            }),
            Property::always("core not starved under wf churn", |_, s: &State| {
                s.core_wait <= MAX_STARVE
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
            println!("Exploring fairness/bounds state space on {address}.");
            FairnessBounds
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking fairness and bounds.");
            FairnessBounds
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  fairness_bounds_machine check");
            println!("  fairness_bounds_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
