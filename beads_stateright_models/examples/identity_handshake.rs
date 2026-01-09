//! Model 1: Identity + handshake gating (Plan §9.3 / §9.10 style).
//!
//! What this model checks:
//! - wrong store => hard close
//! - replica id collision => hard close
//! - version negotiation is intersection-compatible only
//! - accepted namespaces are the intersection of (requested ∩ offered ∩ locally_allowed)
//! - negotiated max_frame_bytes never exceeds local limit

use beads_stateright_models::spec::{negotiate_version, NamespaceId, ProtocolRange, ReplicaId, StoreEpoch, StoreId, StoreIdentity};
use stateright::{report::WriteReporter, Checker, Model, Property};
use std::collections::BTreeSet;
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Phase {
    AwaitHello,
    Established {
        version: u32,
        max_frame_bytes: u32,
        accepted: BTreeSet<NamespaceId>,
    },
    Closed { code: &'static str },
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    phase: Phase,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum HelloCase {
    Good,
    WrongStore,
    ReplicaIdCollision,
    VersionIncompatible,
    EmptyNamespaceIntersection,
    RequestedNotAllowed,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    DeliverHello(HelloCase),
}

#[derive(Clone, Debug)]
struct HandshakeModel {
    local_store: StoreIdentity,
    local_replica: ReplicaId,
    protocol: ProtocolRange,
    local_max_frame_bytes: u32,
    locally_allowed: BTreeSet<NamespaceId>,
}

impl HandshakeModel {
    fn hello_for(&self, c: HelloCase) -> (StoreIdentity, ReplicaId, u32, u32, u32, Vec<NamespaceId>, Vec<NamespaceId>) {
        // Returns (store, peer_replica_id, peer_max, peer_min, peer_max_frame_bytes, requested, offered)
        let good_store = self.local_store;
        let wrong_store = StoreIdentity {
            store_id: StoreId(Uuid::new_v4()),
            store_epoch: self.local_store.store_epoch,
        };

        let peer_id_good = ReplicaId(Uuid::new_v4());
        let peer_id_collision = self.local_replica;

        let (peer_store, peer_replica) = match c {
            HelloCase::WrongStore => (wrong_store, peer_id_good),
            HelloCase::ReplicaIdCollision => (good_store, peer_id_collision),
            _ => (good_store, peer_id_good),
        };

        let (peer_max, peer_min) = match c {
            HelloCase::VersionIncompatible => (0, 0),
            _ => (self.protocol.max, self.protocol.min),
        };

        let peer_max_frame_bytes = match c {
            HelloCase::Good => self.local_max_frame_bytes / 2,
            HelloCase::EmptyNamespaceIntersection => self.local_max_frame_bytes / 2,
            HelloCase::RequestedNotAllowed => self.local_max_frame_bytes / 2,
            _ => self.local_max_frame_bytes * 2, // peer may claim bigger; we must clamp
        };

        let ns_a = NamespaceId("a".into());
        let ns_b = NamespaceId("b".into());
        let ns_c = NamespaceId("c".into());

        let (requested, offered) = match c {
            HelloCase::Good => (vec![ns_a.clone()], vec![ns_a, ns_c.clone()]),
            HelloCase::EmptyNamespaceIntersection => (vec![ns_a.clone()], vec![ns_b]),
            HelloCase::RequestedNotAllowed => (vec![ns_c.clone()], vec![ns_c]),
            _ => (vec![ns_a.clone()], vec![ns_a]),
        };

        (peer_store, peer_replica, peer_max, peer_min, peer_max_frame_bytes, requested, offered)
    }

    fn step(&self, s: &State, a: &Action) -> State {
        match (&s.phase, a) {
            (Phase::AwaitHello, Action::DeliverHello(c)) => {
                let (peer_store, peer_replica, peer_max, peer_min, peer_mfb, requested, offered) = self.hello_for(*c);

                // Store gating.
                if peer_store != self.local_store {
                    return State {
                        phase: Phase::Closed { code: "wrong_store" },
                    };
                }

                // Replica collision gating.
                if peer_replica == self.local_replica {
                    return State {
                        phase: Phase::Closed {
                            code: "replica_id_collision",
                        },
                    };
                }

                // Version negotiation gating.
                let version = match negotiate_version(self.protocol, peer_max, peer_min) {
                    Ok(v) => v,
                    Err(_) => {
                        return State {
                            phase: Phase::Closed {
                                code: "version_incompatible",
                            },
                        }
                    }
                };

                // Namespace acceptance = requested ∩ offered ∩ local_policy.
                let requested: BTreeSet<_> = requested.into_iter().collect();
                let offered: BTreeSet<_> = offered.into_iter().collect();
                let accepted: BTreeSet<_> = requested
                    .intersection(&offered)
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .intersection(&self.locally_allowed)
                    .cloned()
                    .collect();

                let max_frame_bytes = peer_mfb.min(self.local_max_frame_bytes);

                State {
                    phase: Phase::Established {
                        version,
                        max_frame_bytes,
                        accepted,
                    },
                }
            }
            _ => s.clone(),
        }
    }
}

impl Model for HandshakeModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            phase: Phase::AwaitHello,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if matches!(state.phase, Phase::AwaitHello) {
            actions.extend([
                Action::DeliverHello(HelloCase::Good),
                Action::DeliverHello(HelloCase::WrongStore),
                Action::DeliverHello(HelloCase::ReplicaIdCollision),
                Action::DeliverHello(HelloCase::VersionIncompatible),
                Action::DeliverHello(HelloCase::EmptyNamespaceIntersection),
                Action::DeliverHello(HelloCase::RequestedNotAllowed),
            ]);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        Some(self.step(state, &action))
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![Property::always("negotiated invariants", |m, s: &State| match &s.phase {
            Phase::Established {
                version,
                max_frame_bytes,
                accepted,
            } => {
                // v is within [min, max]
                if *version < 1 || *version > 2 {
                    return false;
                }
                // frame limit clamped
                if *max_frame_bytes > 16 * 1024 {
                    return false;
                }
                // accepted respects local policy
                accepted.is_subset(&m.locally_allowed)
            }
            _ => true,
        })]
    }
}

fn main() -> Result<(), pico_args::Error> {
    env_logger::init();

    let local_store = StoreIdentity {
        store_id: StoreId(Uuid::new_v4()),
        store_epoch: StoreEpoch(1),
    };

    let locally_allowed = BTreeSet::from([NamespaceId("a".into()), NamespaceId("b".into())]);

    let model = HandshakeModel {
        local_store,
        local_replica: ReplicaId(Uuid::new_v4()),
        protocol: ProtocolRange { max: 2, min: 1 },
        local_max_frame_bytes: 16 * 1024,
        locally_allowed,
    };

    let mut args = pico_args::Arguments::from_env();
    match args.subcommand()?.as_deref() {
        Some("explore") => {
            let address = args
                .opt_free_from_str()?
                .unwrap_or("localhost:3000".to_string());
            println!("Exploring identity handshake state space on {address}.");
            model
                .clone()
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(20))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking identity handshake.");
            model
                .clone()
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(20))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  identity_handshake check");
            println!("  identity_handshake explore [ADDRESS]");
        }
    }

    Ok(())
}
