//! Model: require_min_seen read gating + timeout semantics.
//!
//! Plan alignment:
//! - Read gating via applied watermarks: REALTIME_PLAN.md §16.1
//!
//! Reads either return success once applied >= require_min_seen, or wait up to
//! wait_timeout_ms and return a retryable error with the current watermark.

use stateright::{report::WriteReporter, Checker, Model, Property};
use std::time::Duration;

const MAX_APPLIED: u8 = 3;
const MAX_TIMEOUT: u8 = 2;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct ReadRequest {
    required: u8,
    deadline: u8,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Response {
    Success { applied_at: u8, required: u8 },
    Timeout {
        applied_at: u8,
        required: u8,
        retryable: bool,
    },
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    now: u8,
    applied: u8,
    pending: Option<ReadRequest>,
    last_response: Option<Response>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    Apply,
    StartRead { required: u8, timeout: u8 },
    Tick,
}

#[derive(Clone, Debug)]
struct ReadGating;

impl Model for ReadGating {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            now: 0,
            applied: 0,
            pending: None,
            last_response: None,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        actions.push(Action::Tick);
        if state.applied < MAX_APPLIED {
            actions.push(Action::Apply);
        }
        if state.pending.is_none() {
            for required in 0..=MAX_APPLIED {
                for timeout in 0..=MAX_TIMEOUT {
                    actions.push(Action::StartRead { required, timeout });
                }
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        next.last_response = None;

        match action {
            Action::Apply => {
                if next.applied < MAX_APPLIED {
                    next.applied = next.applied.saturating_add(1);
                }
            }
            Action::StartRead { required, timeout } => {
                if next.pending.is_none() {
                    let deadline = next.now.saturating_add(timeout);
                    next.pending = Some(ReadRequest { required, deadline });
                }
            }
            Action::Tick => {
                next.now = next.now.saturating_add(1);
            }
        }

        if let Some(req) = next.pending {
            if next.applied >= req.required {
                next.last_response = Some(Response::Success {
                    applied_at: next.applied,
                    required: req.required,
                });
                next.pending = None;
            } else if next.now >= req.deadline {
                next.last_response = Some(Response::Timeout {
                    applied_at: next.applied,
                    required: req.required,
                    retryable: true,
                });
                next.pending = None;
            }
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no stale reads", |_, s: &State| {
                match s.last_response {
                    Some(Response::Success { applied_at, required }) => {
                        applied_at >= required && applied_at == s.applied
                    }
                    _ => true,
                }
            }),
            Property::always("timeouts are retryable and include watermark", |_, s: &State| {
                match s.last_response {
                    Some(Response::Timeout {
                        applied_at,
                        required,
                        retryable,
                    }) => retryable && applied_at == s.applied && applied_at < required,
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
            println!("Exploring read-gating state space on {address}.");
            ReadGating
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking read gating.");
            ReadGating
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  read_gating_machine check");
            println!("  read_gating_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
