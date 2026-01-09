//! Model: Idempotency + receipts with crash cut points.
//!
//! Plan alignment:
//! - Receipt + retry semantics: REALTIME_PLAN.md §0.11
//! - Event sequencing + idempotency: REALTIME_PLAN.md §8.2
//! - Crash-consistency ordering: REALTIME_PLAN.md §6.5
//!
//! This model keeps a single client_request_id and exercises retry behavior
//! across crash cut points. It tracks WAL persistence, receipt state, and
//! ensures retries never mint new txn_id/stamps once a durable record exists.

use stateright::{report::WriteReporter, Checker, Model, Property};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Digest {
    A,
    B,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct Record {
    txn_id: u8,
    stamp: u8,
    digest: Digest,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct RecordId {
    txn_id: u8,
    stamp: u8,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum ReceiptState {
    None,
    Pending(Record),
    Committed(Record),
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Response {
    Ok { txn_id: u8, stamp: u8 },
    Retryable { txn_id: u8, stamp: u8 },
    Mismatch,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    wal: Option<Record>,
    wal_fsynced: bool,
    receipt: ReceiptState,
    applied: bool,
    crashed: bool,
    last_request: Option<Digest>,
    last_response: Option<Response>,
    next_txn_id: u8,
    next_stamp: u8,
    durable_record: Option<RecordId>,
    remint_violation: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    ClientRequest(Digest),
    Fsync,
    IndexCommit,
    Apply,
    FinalizeReceipt,
    Reply,
    Crash,
    Restart,
}

#[derive(Clone, Debug)]
struct IdempotencyModel;

fn record_id(record: Record) -> RecordId {
    RecordId {
        txn_id: record.txn_id,
        stamp: record.stamp,
    }
}

fn current_record(state: &State) -> Option<Record> {
    match state.receipt {
        ReceiptState::Pending(record) | ReceiptState::Committed(record) => Some(record),
        ReceiptState::None => state.wal,
    }
}

fn durable_record(state: &State) -> Option<RecordId> {
    let record = current_record(state)?;
    if state.wal_fsynced || !matches!(state.receipt, ReceiptState::None) {
        Some(record_id(record))
    } else {
        None
    }
}

fn track_durable_record(state: &mut State) {
    if let Some(record) = durable_record(state) {
        match state.durable_record {
            None => state.durable_record = Some(record),
            Some(existing) => {
                if existing != record {
                    state.remint_violation = true;
                }
            }
        }
    }
}

impl Model for IdempotencyModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            wal: None,
            wal_fsynced: false,
            receipt: ReceiptState::None,
            applied: false,
            crashed: false,
            last_request: None,
            last_response: None,
            next_txn_id: 1,
            next_stamp: 1,
            durable_record: None,
            remint_violation: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.crashed {
            actions.push(Action::Restart);
            return;
        }

        actions.push(Action::Crash);
        actions.push(Action::ClientRequest(Digest::A));
        actions.push(Action::ClientRequest(Digest::B));

        if state.wal.is_some() && !state.wal_fsynced {
            actions.push(Action::Fsync);
        }
        if state.wal_fsynced && matches!(state.receipt, ReceiptState::None) {
            actions.push(Action::IndexCommit);
        }
        if matches!(state.receipt, ReceiptState::Pending(_)) && !state.applied {
            actions.push(Action::Apply);
        }
        if matches!(state.receipt, ReceiptState::Pending(_)) && state.applied {
            actions.push(Action::FinalizeReceipt);
        }
        if matches!(state.receipt, ReceiptState::Committed(_)) {
            actions.push(Action::Reply);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        next.last_request = None;
        next.last_response = None;

        match action {
            Action::ClientRequest(digest) => {
                next.last_request = Some(digest);

                if let Some(record) = current_record(&next) {
                    if record.digest != digest {
                        next.last_response = Some(Response::Mismatch);
                        track_durable_record(&mut next);
                        return Some(next);
                    }

                    match next.receipt {
                        ReceiptState::Committed(_) => {
                            next.last_response = Some(Response::Ok {
                                txn_id: record.txn_id,
                                stamp: record.stamp,
                            });
                        }
                        ReceiptState::Pending(_) | ReceiptState::None => {
                            next.last_response = Some(Response::Retryable {
                                txn_id: record.txn_id,
                                stamp: record.stamp,
                            });
                        }
                    }

                    track_durable_record(&mut next);
                    return Some(next);
                }

                let record = Record {
                    txn_id: next.next_txn_id,
                    stamp: next.next_stamp,
                    digest,
                };
                next.next_txn_id = next.next_txn_id.saturating_add(1);
                next.next_stamp = next.next_stamp.saturating_add(1);
                next.wal = Some(record);
                next.wal_fsynced = false;
            }
            Action::Fsync => {
                if next.wal.is_some() {
                    next.wal_fsynced = true;
                }
            }
            Action::IndexCommit => {
                if next.wal_fsynced {
                    if let Some(record) = next.wal {
                        next.receipt = ReceiptState::Pending(record);
                    }
                }
            }
            Action::Apply => {
                if matches!(next.receipt, ReceiptState::Pending(_)) {
                    next.applied = true;
                }
            }
            Action::FinalizeReceipt => {
                if let ReceiptState::Pending(record) = next.receipt {
                    if next.applied {
                        next.receipt = ReceiptState::Committed(record);
                    }
                }
            }
            Action::Reply => {
                if let ReceiptState::Committed(record) = next.receipt {
                    next.last_response = Some(Response::Ok {
                        txn_id: record.txn_id,
                        stamp: record.stamp,
                    });
                }
            }
            Action::Crash => {
                next.crashed = true;
                next.last_request = None;
                next.last_response = None;
                next.applied = false;

                if !next.wal_fsynced {
                    next.wal = None;
                    next.receipt = ReceiptState::None;
                }
                if next.wal.is_none() {
                    next.wal_fsynced = false;
                }
            }
            Action::Restart => {
                if next.crashed {
                    next.crashed = false;
                    next.last_request = None;
                    next.last_response = None;
                    next.applied = false;

                    if next.wal_fsynced && matches!(next.receipt, ReceiptState::None) {
                        if let Some(record) = next.wal {
                            next.receipt = ReceiptState::Pending(record);
                        }
                    }
                }
            }
        }

        track_durable_record(&mut next);
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("at-most-once per client_request_id", |_, s: &State| {
                !s.remint_violation
            }),
            Property::always("retry returns original receipt", |_, s: &State| {
                match s.last_response {
                    Some(Response::Ok { txn_id, stamp })
                    | Some(Response::Retryable { txn_id, stamp }) => {
                        if let Some(record) = current_record(s) {
                            let digest_match = s
                                .last_request
                                .map(|digest| digest == record.digest)
                                .unwrap_or(true);
                            txn_id == record.txn_id && stamp == record.stamp && digest_match
                        } else {
                            false
                        }
                    }
                    _ => true,
                }
            }),
            Property::always("no stamp remint", |_, s: &State| {
                if let Some(record) = s.durable_record {
                    match s.last_response {
                        Some(Response::Ok { txn_id, stamp })
                        | Some(Response::Retryable { txn_id, stamp }) => {
                            txn_id == record.txn_id && stamp == record.stamp
                        }
                        _ => true,
                    }
                } else {
                    true
                }
            }),
            Property::always("request digest mismatch rejected", |_, s: &State| {
                match s.last_response {
                    Some(Response::Mismatch) => {
                        if let (Some(digest), Some(record)) =
                            (s.last_request, current_record(s))
                        {
                            digest != record.digest
                        } else {
                            false
                        }
                    }
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
            println!("Exploring idempotency/receipt state space on {address}.");
            IdempotencyModel
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking idempotency + receipts.");
            IdempotencyModel
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  idempotency_receipt_machine check");
            println!("  idempotency_receipt_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
