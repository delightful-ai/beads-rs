//! Model: WAL crash recovery cut points.
//!
//! Plan alignment:
//! - Crash ordering contract: REALTIME_PLAN.md §6.5
//! - Receipt semantics: REALTIME_PLAN.md §2.6
//!
//! This model encodes the append -> fsync -> index -> apply -> receipt -> reply
//! pipeline and allows a crash at each cut point, followed by recovery.

use stateright::{report::WriteReporter, Checker, Model, Property};
use std::collections::BTreeSet;
use std::time::Duration;

const MAX_SEQ: u8 = 4;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum FlightStage {
    Written(u8),
    Fsynced(u8),
    Indexed(u8),
    Applied(u8),
    ReceiptFinalized(u8),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    next_seq: u8,
    in_flight: Option<FlightStage>,
    wal_written: BTreeSet<u8>,
    wal_fsynced: BTreeSet<u8>,
    indexed: BTreeSet<u8>,
    applied: BTreeSet<u8>,
    receipts: BTreeSet<u8>,
    crashed: bool,
    just_recovered: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    BeginAppend,
    Fsync,
    IndexCommit,
    Apply,
    FinalizeReceipt,
    Reply,
    Crash,
    Recover,
}

#[derive(Clone, Debug)]
struct WalCrashRecovery;

fn max_seq(set: &BTreeSet<u8>) -> u8 {
    set.iter().copied().max().unwrap_or(0)
}

impl Model for WalCrashRecovery {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            next_seq: 1,
            in_flight: None,
            wal_written: BTreeSet::new(),
            wal_fsynced: BTreeSet::new(),
            indexed: BTreeSet::new(),
            applied: BTreeSet::new(),
            receipts: BTreeSet::new(),
            crashed: false,
            just_recovered: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.crashed {
            actions.push(Action::Recover);
            return;
        }

        actions.push(Action::Crash);

        match state.in_flight {
            None => {
                if state.next_seq <= MAX_SEQ {
                    actions.push(Action::BeginAppend);
                }
            }
            Some(FlightStage::Written(_)) => actions.push(Action::Fsync),
            Some(FlightStage::Fsynced(_)) => actions.push(Action::IndexCommit),
            Some(FlightStage::Indexed(_)) => actions.push(Action::Apply),
            Some(FlightStage::Applied(_)) => actions.push(Action::FinalizeReceipt),
            Some(FlightStage::ReceiptFinalized(_)) => actions.push(Action::Reply),
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        next.just_recovered = false;

        match action {
            Action::BeginAppend => {
                if next.in_flight.is_some() {
                    return Some(next);
                }
                if next.next_seq > MAX_SEQ {
                    return Some(next);
                }
                let seq = next.next_seq;
                next.next_seq = next.next_seq.saturating_add(1);
                next.wal_written.insert(seq);
                next.in_flight = Some(FlightStage::Written(seq));
            }
            Action::Fsync => {
                if let Some(FlightStage::Written(seq)) = next.in_flight {
                    next.wal_fsynced.insert(seq);
                    next.in_flight = Some(FlightStage::Fsynced(seq));
                }
            }
            Action::IndexCommit => {
                if let Some(FlightStage::Fsynced(seq)) = next.in_flight {
                    next.indexed.insert(seq);
                    next.in_flight = Some(FlightStage::Indexed(seq));
                }
            }
            Action::Apply => {
                if let Some(FlightStage::Indexed(seq)) = next.in_flight {
                    next.applied.insert(seq);
                    next.in_flight = Some(FlightStage::Applied(seq));
                }
            }
            Action::FinalizeReceipt => {
                if let Some(FlightStage::Applied(seq)) = next.in_flight {
                    next.receipts.insert(seq);
                    next.in_flight = Some(FlightStage::ReceiptFinalized(seq));
                }
            }
            Action::Reply => {
                if matches!(next.in_flight, Some(FlightStage::ReceiptFinalized(_))) {
                    next.in_flight = None;
                }
            }
            Action::Crash => {
                next.crashed = true;
                next.just_recovered = false;

                if let Some(stage) = next.in_flight {
                    let seq = match stage {
                        FlightStage::Written(s)
                        | FlightStage::Fsynced(s)
                        | FlightStage::Indexed(s)
                        | FlightStage::Applied(s)
                        | FlightStage::ReceiptFinalized(s) => s,
                    };
                    if !next.wal_fsynced.contains(&seq) {
                        next.wal_written.remove(&seq);
                    }
                }

                next.in_flight = None;
                next.applied.clear(); // in-memory state lost on crash
            }
            Action::Recover => {
                if next.crashed {
                    next.crashed = false;
                    next.just_recovered = true;
                    for seq in next.wal_fsynced.iter().copied() {
                        next.indexed.insert(seq);
                    }
                    let max_durable = max_seq(&next.wal_fsynced);
                    if next.next_seq <= max_durable {
                        next.next_seq = max_durable.saturating_add(1);
                    }
                }
            }
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no origin_seq reuse after recovery", |_, s: &State| {
                s.next_seq > max_seq(&s.wal_fsynced)
            }),
            Property::always("LocalFsync receipts are durable", |_, s: &State| {
                s.receipts.iter().all(|seq| s.wal_fsynced.contains(seq))
            }),
            Property::always(
                "recovery catches up durable-but-unindexed",
                |_, s: &State| {
                    if s.just_recovered {
                        s.wal_fsynced.iter().all(|seq| s.indexed.contains(seq))
                    } else {
                        true
                    }
                },
            ),
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
            println!("Exploring WAL crash recovery state space on {address}.");
            WalCrashRecovery
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking WAL crash recovery.");
            WalCrashRecovery
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  wal_crash_recovery_machine check");
            println!("  wal_crash_recovery_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
