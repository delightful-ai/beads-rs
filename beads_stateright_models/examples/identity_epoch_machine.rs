//! Model: Store identity + epoch guardrails across replication, checkpoint, and WAL.
//!
//! Plan alignment:
//! - Store identity/epoch basics: REALTIME_PLAN.md §0.1, §2.1
//! - Replication handshake + errors: REALTIME_PLAN.md §9.3, §9.10
//!
//! The same identity validation should gate:
//! - replication HELLO/WELCOME
//! - checkpoint import
//! - WAL header validation

use beads_stateright_models::spec::{StoreEpoch, StoreId, StoreIdentity};
use stateright::{report::WriteReporter, Checker, Model, Property};
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum IdentityCase {
    Good,
    WrongStore,
    WrongEpoch,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Outcome {
    Accepted,
    RejectStore,
    RejectEpoch,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct LaneResult {
    input: StoreIdentity,
    outcome: Outcome,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    repl: Option<LaneResult>,
    checkpoint: Option<LaneResult>,
    wal: Option<LaneResult>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    Replication(IdentityCase),
    CheckpointImport(IdentityCase),
    WalHeader(IdentityCase),
}

#[derive(Clone, Debug)]
struct IdentityEpochModel {
    local: StoreIdentity,
    wrong_store: StoreIdentity,
    wrong_epoch: StoreIdentity,
}

impl IdentityEpochModel {
    fn new() -> Self {
        let local = StoreIdentity {
            store_id: StoreId(Uuid::nil()),
            store_epoch: StoreEpoch(1),
        };
        let wrong_store = StoreIdentity {
            store_id: StoreId(Uuid::from_u128(1)),
            store_epoch: local.store_epoch,
        };
        let wrong_epoch = StoreIdentity {
            store_id: local.store_id,
            store_epoch: StoreEpoch(2),
        };
        Self {
            local,
            wrong_store,
            wrong_epoch,
        }
    }

    fn identity_for(&self, case: IdentityCase) -> StoreIdentity {
        match case {
            IdentityCase::Good => self.local,
            IdentityCase::WrongStore => self.wrong_store,
            IdentityCase::WrongEpoch => self.wrong_epoch,
        }
    }

    fn validate(&self, input: StoreIdentity) -> Outcome {
        if input.store_id != self.local.store_id {
            return Outcome::RejectStore;
        }
        if input.store_epoch != self.local.store_epoch {
            return Outcome::RejectEpoch;
        }
        Outcome::Accepted
    }
}

impl Model for IdentityEpochModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            repl: None,
            checkpoint: None,
            wal: None,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.repl.is_none() {
            actions.extend([
                Action::Replication(IdentityCase::Good),
                Action::Replication(IdentityCase::WrongStore),
                Action::Replication(IdentityCase::WrongEpoch),
            ]);
        }
        if state.checkpoint.is_none() {
            actions.extend([
                Action::CheckpointImport(IdentityCase::Good),
                Action::CheckpointImport(IdentityCase::WrongStore),
                Action::CheckpointImport(IdentityCase::WrongEpoch),
            ]);
        }
        if state.wal.is_none() {
            actions.extend([
                Action::WalHeader(IdentityCase::Good),
                Action::WalHeader(IdentityCase::WrongStore),
                Action::WalHeader(IdentityCase::WrongEpoch),
            ]);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        match action {
            Action::Replication(case) => {
                let input = self.identity_for(case);
                next.repl = Some(LaneResult {
                    input,
                    outcome: self.validate(input),
                });
            }
            Action::CheckpointImport(case) => {
                let input = self.identity_for(case);
                next.checkpoint = Some(LaneResult {
                    input,
                    outcome: self.validate(input),
                });
            }
            Action::WalHeader(case) => {
                let input = self.identity_for(case);
                next.wal = Some(LaneResult {
                    input,
                    outcome: self.validate(input),
                });
            }
        }
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no cross-store merge", |m, s: &State| {
                for lane in [&s.repl, &s.checkpoint, &s.wal] {
                    if let Some(result) = lane {
                        if matches!(result.outcome, Outcome::Accepted)
                            && result.input.store_id != m.local.store_id
                        {
                            return false;
                        }
                    }
                }
                true
            }),
            Property::always("no cross-epoch merge", |m, s: &State| {
                for lane in [&s.repl, &s.checkpoint, &s.wal] {
                    if let Some(result) = lane {
                        if matches!(result.outcome, Outcome::Accepted)
                            && result.input.store_epoch != m.local.store_epoch
                        {
                            return false;
                        }
                    }
                }
                true
            }),
            Property::always("consistent rejection across lanes", |m, s: &State| {
                for lane in [&s.repl, &s.checkpoint, &s.wal] {
                    if let Some(result) = lane {
                        let expected = if result.input.store_id != m.local.store_id {
                            Outcome::RejectStore
                        } else if result.input.store_epoch != m.local.store_epoch {
                            Outcome::RejectEpoch
                        } else {
                            Outcome::Accepted
                        };
                        if result.outcome != expected {
                            return false;
                        }
                    }
                }
                true
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
            println!("Exploring identity/epoch guardrails on {address}.");
            IdentityEpochModel::new()
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking identity/epoch guardrails.");
            IdentityEpochModel::new()
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  identity_epoch_machine check");
            println!("  identity_epoch_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
