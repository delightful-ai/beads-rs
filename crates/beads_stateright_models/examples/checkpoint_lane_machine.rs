//! Model: Checkpoint lane correctness + multi-writer convergence.
//!
//! Plan alignment:
//! - Checkpoint export/import semantics: REALTIME_PLAN.md §13
//! - Included_heads continuity: REALTIME_PLAN.md §2.3
//!
//! This model abstracts checkpoint content to (included, included_head, epoch)
//! and focuses on truthful inclusion, import advancement, multi-writer retries,
//! and no cross-epoch merges.

use stateright::{Checker, Model, Property, report::WriteReporter};
use std::collections::BTreeMap;
use std::time::Duration;

const MAX_WATERMARK: u8 = 3;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Replica {
    A,
    B,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct Checkpoint {
    id: u8,
    epoch: u8,
    included: u8,
    included_head: u8,
    writer: Replica,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ReplicaState {
    durable: u8,
    head: u8,
    known_ref: Option<Checkpoint>,
    staged: Option<Checkpoint>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    epoch: u8,
    ref_head: Option<Checkpoint>,
    replicas: BTreeMap<Replica, ReplicaState>,
    next_checkpoint_id: u8,
    foreign_injected: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    AdvanceLocal(Replica),
    Export(Replica),
    Push(Replica),
    Fetch(Replica),
    InjectForeign,
}

#[derive(Clone, Debug)]
struct CheckpointLane;

fn max_local_durable(state: &State) -> u8 {
    state
        .replicas
        .values()
        .map(|replica| replica.durable)
        .max()
        .unwrap_or(0)
}

fn truthful_checkpoint(state: &State, checkpoint: &Checkpoint) -> bool {
    let Some(writer) = state.replicas.get(&checkpoint.writer) else {
        return true;
    };
    checkpoint.included <= writer.durable
        && checkpoint.included_head <= writer.head
        && checkpoint.included_head == checkpoint.included
}

impl Model for CheckpointLane {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut replicas = BTreeMap::new();
        replicas.insert(
            Replica::A,
            ReplicaState {
                durable: 0,
                head: 0,
                known_ref: None,
                staged: None,
            },
        );
        replicas.insert(
            Replica::B,
            ReplicaState {
                durable: 0,
                head: 0,
                known_ref: None,
                staged: None,
            },
        );

        vec![State {
            epoch: 1,
            ref_head: None,
            replicas,
            next_checkpoint_id: 1,
            foreign_injected: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        for replica in [Replica::A, Replica::B] {
            let replica_state = state.replicas.get(&replica).expect("replica state");

            if replica_state.durable < MAX_WATERMARK {
                actions.push(Action::AdvanceLocal(replica));
            }
            if replica_state.staged.is_none() {
                actions.push(Action::Export(replica));
            }
            if replica_state.staged.is_some() {
                actions.push(Action::Push(replica));
            }
            actions.push(Action::Fetch(replica));
        }

        if !state.foreign_injected {
            actions.push(Action::InjectForeign);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();

        match action {
            Action::AdvanceLocal(replica) => {
                if let Some(replica_state) = next.replicas.get_mut(&replica) {
                    if replica_state.durable < MAX_WATERMARK {
                        replica_state.durable += 1;
                        replica_state.head = replica_state.durable;
                    }
                }
            }
            Action::Export(replica) => {
                if let Some(replica_state) = next.replicas.get_mut(&replica) {
                    if replica_state.staged.is_none() {
                        let checkpoint = Checkpoint {
                            id: next.next_checkpoint_id,
                            epoch: next.epoch,
                            included: replica_state.durable,
                            included_head: replica_state.head,
                            writer: replica,
                        };
                        next.next_checkpoint_id = next.next_checkpoint_id.saturating_add(1);
                        replica_state.staged = Some(checkpoint);
                    }
                }
            }
            Action::Push(replica) => {
                if let Some(replica_state) = next.replicas.get_mut(&replica) {
                    let staged = match replica_state.staged {
                        Some(checkpoint) => checkpoint,
                        None => return Some(next),
                    };

                    let epoch_ok = next
                        .ref_head
                        .map(|head| head.epoch == next.epoch)
                        .unwrap_or(true);
                    let base_ok = replica_state.known_ref == next.ref_head;

                    if epoch_ok && base_ok {
                        next.ref_head = Some(staged);
                        replica_state.known_ref = next.ref_head;
                    }
                    replica_state.staged = None;
                }
            }
            Action::Fetch(replica) => {
                if let Some(replica_state) = next.replicas.get_mut(&replica) {
                    let Some(head) = next.ref_head else {
                        replica_state.known_ref = None;
                        return Some(next);
                    };

                    if head.epoch != next.epoch {
                        return Some(next);
                    }

                    replica_state.known_ref = Some(head);
                    replica_state.durable = replica_state.durable.max(head.included);
                    replica_state.head = replica_state.head.max(head.included_head);
                }
            }
            Action::InjectForeign => {
                if next.foreign_injected {
                    return Some(next);
                }
                let foreign = Checkpoint {
                    id: next.next_checkpoint_id,
                    epoch: next.epoch.saturating_add(1),
                    included: 0,
                    included_head: 0,
                    writer: Replica::A,
                };
                next.next_checkpoint_id = next.next_checkpoint_id.saturating_add(1);
                next.ref_head = Some(foreign);
                next.foreign_injected = true;
            }
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("truthful inclusion", |_, s: &State| {
                if let Some(head) = s.ref_head {
                    if !truthful_checkpoint(s, &head) {
                        return false;
                    }
                }
                for replica_state in s.replicas.values() {
                    if let Some(staged) = replica_state.staged {
                        if !truthful_checkpoint(s, &staged) {
                            return false;
                        }
                    }
                }
                true
            }),
            Property::always("import advances durable >= included", |_, s: &State| {
                s.replicas.values().all(|replica| {
                    replica
                        .known_ref
                        .map(|checkpoint| {
                            checkpoint.epoch == s.epoch
                                && replica.durable >= checkpoint.included
                                && replica.head >= checkpoint.included_head
                        })
                        .unwrap_or(true)
                })
            }),
            Property::always("no cross-epoch merge", |_, s: &State| {
                s.replicas.values().all(|replica| {
                    replica
                        .known_ref
                        .map(|checkpoint| checkpoint.epoch == s.epoch)
                        .unwrap_or(true)
                })
            }),
            Property::sometimes("multi-writer convergence", |_, s: &State| {
                match s.ref_head {
                    Some(head) => head.epoch == s.epoch && head.included == max_local_durable(s),
                    None => false,
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
            println!("Exploring checkpoint lane state space on {address}.");
            CheckpointLane
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking checkpoint lane.");
            CheckpointLane
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  checkpoint_lane_machine check");
            println!("  checkpoint_lane_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
