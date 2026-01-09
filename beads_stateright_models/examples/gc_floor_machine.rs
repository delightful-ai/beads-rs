//! Model: GC floor enforcement (NamespaceGcMarker) and contiguity safety.
//!
//! Plan alignment:
//! - GC markers + floor rule: REALTIME_PLAN.md §2.4
//! - Contiguity / ACK semantics: REALTIME_PLAN.md §9.4
//!
//! Events with event_time_ms <= gc_floor are ignored as state mutations but
//! must still advance contiguity and ACK.

use stateright::{report::WriteReporter, Checker, Model, Property};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const MAX_SEQ: u8 = 3;
const MAX_TIME: u8 = 3;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Namespace {
    Core,
    Wf,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Replica {
    A,
    B,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct EventId {
    ns: Namespace,
    seq: u8,
    time: u8,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct Delivery {
    replica: Replica,
    ns: Namespace,
    seq: u8,
    time: u8,
    old: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ReplicaState {
    floor: BTreeMap<Namespace, u8>,
    floor_max: BTreeMap<Namespace, u8>,
    seen: BTreeMap<Namespace, u8>,
    applied: BTreeSet<EventId>,
    received: BTreeSet<EventId>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    replicas: BTreeMap<Replica, ReplicaState>,
    last_delivery: Option<Delivery>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    Deliver {
        replica: Replica,
        ns: Namespace,
        seq: u8,
        time: u8,
    },
    EmitGc {
        replica: Replica,
        ns: Namespace,
        floor: u8,
    },
}

#[derive(Clone, Debug)]
struct GcFloorModel;

fn get_map_value(map: &BTreeMap<Namespace, u8>, ns: Namespace) -> u8 {
    map.get(&ns).copied().unwrap_or(0)
}

impl Model for GcFloorModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut replicas = BTreeMap::new();
        for replica in [Replica::A, Replica::B] {
            let mut floor = BTreeMap::new();
            let mut floor_max = BTreeMap::new();
            let mut seen = BTreeMap::new();
            for ns in [Namespace::Core, Namespace::Wf] {
                floor.insert(ns, 0);
                floor_max.insert(ns, 0);
                seen.insert(ns, 0);
            }
            replicas.insert(
                replica,
                ReplicaState {
                    floor,
                    floor_max,
                    seen,
                    applied: BTreeSet::new(),
                    received: BTreeSet::new(),
                },
            );
        }

        vec![State {
            replicas,
            last_delivery: None,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        for &replica in [Replica::A, Replica::B].iter() {
            let replica_state = state.replicas.get(&replica).expect("replica");
            for &ns in [Namespace::Core, Namespace::Wf].iter() {
                let next_seq = get_map_value(&replica_state.seen, ns) + 1;
                if next_seq <= MAX_SEQ {
                    for time in 1..=MAX_TIME {
                        actions.push(Action::Deliver {
                            replica,
                            ns,
                            seq: next_seq,
                            time,
                        });
                    }
                }

                let current_floor = get_map_value(&replica_state.floor, ns);
                for floor in (current_floor + 1)..=MAX_TIME {
                    actions.push(Action::EmitGc { replica, ns, floor });
                }
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        next.last_delivery = None;

        match action {
            Action::Deliver {
                replica,
                ns,
                seq,
                time,
            } => {
                let replica_state = next.replicas.get_mut(&replica).expect("replica");
                let expected = get_map_value(&replica_state.seen, ns) + 1;
                if seq != expected {
                    return Some(next);
                }

                let floor = get_map_value(&replica_state.floor, ns);
                let old = time <= floor;

                replica_state.seen.insert(ns, seq);
                replica_state.received.insert(EventId { ns, seq, time });

                if !old {
                    replica_state.applied.insert(EventId { ns, seq, time });
                }

                next.last_delivery = Some(Delivery {
                    replica,
                    ns,
                    seq,
                    time,
                    old,
                });
            }
            Action::EmitGc { replica, ns, floor } => {
                let replica_state = next.replicas.get_mut(&replica).expect("replica");
                let current = get_map_value(&replica_state.floor, ns);
                if floor <= current {
                    return Some(next);
                }
                replica_state.floor.insert(ns, floor);
                let max_seen = get_map_value(&replica_state.floor_max, ns).max(floor);
                replica_state.floor_max.insert(ns, max_seen);
                replica_state
                    .applied
                    .retain(|event| event.ns != ns || event.time > floor);
            }
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("floor monotonicity", |_, s: &State| {
                s.replicas.values().all(|replica| {
                    replica.floor.iter().all(|(ns, floor)| {
                        let max_seen = get_map_value(&replica.floor_max, *ns);
                        *floor == max_seen
                    })
                })
            }),
            Property::always("old events do not mutate state", |_, s: &State| {
                match s.last_delivery {
                    Some(delivery) if delivery.old => {
                        let replica_state =
                            s.replicas.get(&delivery.replica).expect("replica");
                        !replica_state
                            .applied
                            .contains(&EventId {
                                ns: delivery.ns,
                                seq: delivery.seq,
                                time: delivery.time,
                            })
                    }
                    _ => true,
                }
            }),
            Property::always("old events still advance contiguity/ACK", |_, s: &State| {
                match s.last_delivery {
                    Some(delivery) if delivery.old => {
                        let replica_state =
                            s.replicas.get(&delivery.replica).expect("replica");
                        get_map_value(&replica_state.seen, delivery.ns) >= delivery.seq
                    }
                    _ => true,
                }
            }),
            Property::always("deterministic convergence", |_, s: &State| {
                let replica_a = s.replicas.get(&Replica::A).expect("replica A");
                let replica_b = s.replicas.get(&Replica::B).expect("replica B");
                if replica_a.received == replica_b.received && replica_a.floor == replica_b.floor {
                    replica_a.applied == replica_b.applied && replica_a.seen == replica_b.seen
                } else {
                    true
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
            println!("Exploring GC floor state space on {address}.");
            GcFloorModel
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking GC floor semantics.");
            GcFloorModel
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  gc_floor_machine check");
            println!("  gc_floor_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
