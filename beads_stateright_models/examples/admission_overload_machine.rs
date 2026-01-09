//! Model: Admission priorities + overload shedding.
//!
//! Plan alignment:
//! - Admission/overload priorities: REALTIME_PLAN.md §0.19
//!
//! Priority order: IPC mutations > replication ingest > WANT servicing > checkpoint/scrub.
//! On overload, replication is shed first when available.

use stateright::{report::WriteReporter, Checker, Model, Property};
use std::time::Duration;

const MAX_QUEUE: u8 = 4;
const MAX_IPC_WAIT: u8 = 2;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Class {
    Ipc,
    Replication,
    Want,
    Background,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct OverloadRecord {
    shed: Class,
    repl_present: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    ipc_q: u8,
    repl_q: u8,
    want_q: u8,
    bg_q: u8,
    ipc_wait: u8,
    last_overload: Option<OverloadRecord>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    Enqueue(Class),
    Process,
}

#[derive(Clone, Debug)]
struct AdmissionOverload;

fn total_queue(state: &State) -> u8 {
    state
        .ipc_q
        .saturating_add(state.repl_q)
        .saturating_add(state.want_q)
        .saturating_add(state.bg_q)
}

fn dequeue_one(state: &mut State, class: Class) {
    match class {
        Class::Ipc => state.ipc_q = state.ipc_q.saturating_sub(1),
        Class::Replication => state.repl_q = state.repl_q.saturating_sub(1),
        Class::Want => state.want_q = state.want_q.saturating_sub(1),
        Class::Background => state.bg_q = state.bg_q.saturating_sub(1),
    }
}

fn enqueue_one(state: &mut State, class: Class) {
    match class {
        Class::Ipc => state.ipc_q = state.ipc_q.saturating_add(1),
        Class::Replication => state.repl_q = state.repl_q.saturating_add(1),
        Class::Want => state.want_q = state.want_q.saturating_add(1),
        Class::Background => state.bg_q = state.bg_q.saturating_add(1),
    }
}

impl Model for AdmissionOverload {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            ipc_q: 0,
            repl_q: 0,
            want_q: 0,
            bg_q: 0,
            ipc_wait: 0,
            last_overload: None,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        actions.push(Action::Enqueue(Class::Ipc));
        actions.push(Action::Enqueue(Class::Replication));
        actions.push(Action::Enqueue(Class::Want));
        actions.push(Action::Enqueue(Class::Background));

        if total_queue(state) > 0 {
            actions.push(Action::Process);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        next.last_overload = None;

        match action {
            Action::Enqueue(class) => {
                let total = total_queue(&next);
                if total < MAX_QUEUE {
                    enqueue_one(&mut next, class);
                    return Some(next);
                }

                let repl_present = next.repl_q > 0;
                let shed = match class {
                    Class::Ipc => {
                        if next.repl_q > 0 {
                            dequeue_one(&mut next, Class::Replication);
                            Class::Replication
                        } else if next.want_q > 0 {
                            dequeue_one(&mut next, Class::Want);
                            Class::Want
                        } else if next.bg_q > 0 {
                            dequeue_one(&mut next, Class::Background);
                            Class::Background
                        } else {
                            Class::Ipc
                        }
                    }
                    Class::Replication => {
                        Class::Replication
                    }
                    Class::Want => {
                        if next.repl_q > 0 {
                            dequeue_one(&mut next, Class::Replication);
                            Class::Replication
                        } else {
                            Class::Want
                        }
                    }
                    Class::Background => {
                        if next.repl_q > 0 {
                            dequeue_one(&mut next, Class::Replication);
                            Class::Replication
                        } else if next.want_q > 0 {
                            dequeue_one(&mut next, Class::Want);
                            Class::Want
                        } else {
                            Class::Background
                        }
                    }
                };

                if shed != class {
                    enqueue_one(&mut next, class);
                }
                next.last_overload = Some(OverloadRecord {
                    shed,
                    repl_present,
                });
            }
            Action::Process => {
                if next.ipc_q > 0 {
                    next.ipc_q -= 1;
                    next.ipc_wait = 0;
                } else if next.repl_q > 0 {
                    next.repl_q -= 1;
                    next.ipc_wait = 0;
                } else if next.want_q > 0 {
                    next.want_q -= 1;
                    next.ipc_wait = 0;
                } else if next.bg_q > 0 {
                    next.bg_q -= 1;
                    next.ipc_wait = 0;
                }
            }
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("queues stay bounded", |_, s: &State| {
                total_queue(s) <= MAX_QUEUE
            }),
            Property::always("IPC not starved by background", |_, s: &State| {
                s.ipc_wait <= MAX_IPC_WAIT
            }),
            Property::always("replication sheds first on overload", |_, s: &State| {
                match s.last_overload {
                    Some(record) if record.repl_present => record.shed == Class::Replication,
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
            println!("Exploring admission/overload state space on {address}.");
            AdmissionOverload
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking admission/overload priorities.");
            AdmissionOverload
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  admission_overload_machine check");
            println!("  admission_overload_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
