//! Model: Canonical hash/prev gating with out-of-order buffering.
//!
//! Plan alignment:
//! - Canonical CBOR + unknown key preservation: REALTIME_PLAN.md §0.6, §0.6.1
//! - Hash-chain continuity: REALTIME_PLAN.md §0.12.1
//! - Out-of-order buffering / contiguity: REALTIME_PLAN.md §9.4
//!
//! This model rejects non-canonical or unknown-key-dropping events, enforces
//! prev continuity when applying, and buffers out-of-order events until gaps fill.

use stateright::{report::WriteReporter, Checker, Model, Property};
use std::collections::BTreeMap;
use std::time::Duration;

const MAX_SEQ: u8 = 3;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum EventCase {
    Good,
    NonCanonical,
    UnknownKeysDropped,
    PrevMismatch,
    WrongStore,
    Equivocation,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct BufferedEvent {
    hash: u8,
    prev_hash: u8,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum LastEffectKind {
    Applied,
    Buffered,
    Rejected,
    Duplicate,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct LastEvent {
    kind: LastEffectKind,
    seq: u8,
    seen_before: u8,
    canonical: bool,
    unknown_keys_ok: bool,
    store_ok: bool,
    prev_ok: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    seen: u8,
    head_hash: u8,
    applied_hashes: BTreeMap<u8, u8>,
    buffer: BTreeMap<u8, BufferedEvent>,
    errored: bool,
    last_event: Option<LastEvent>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    Deliver { seq: u8, case: EventCase },
}

#[derive(Clone, Debug)]
struct CanonicalHashModel;

fn event_hash_for(seq: u8, case: EventCase) -> u8 {
    match case {
        EventCase::Equivocation => seq.saturating_add(10),
        _ => seq,
    }
}

impl Model for CanonicalHashModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            seen: 0,
            head_hash: 0,
            applied_hashes: BTreeMap::new(),
            buffer: BTreeMap::new(),
            errored: false,
            last_event: None,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.errored {
            return;
        }
        for seq in 1..=MAX_SEQ {
            actions.push(Action::Deliver {
                seq,
                case: EventCase::Good,
            });
            actions.push(Action::Deliver {
                seq,
                case: EventCase::NonCanonical,
            });
            actions.push(Action::Deliver {
                seq,
                case: EventCase::UnknownKeysDropped,
            });
            actions.push(Action::Deliver {
                seq,
                case: EventCase::PrevMismatch,
            });
            actions.push(Action::Deliver {
                seq,
                case: EventCase::WrongStore,
            });
            actions.push(Action::Deliver {
                seq,
                case: EventCase::Equivocation,
            });
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        next.last_event = None;

        if next.errored {
            return Some(next);
        }

        match action {
            Action::Deliver { seq, case } => {
                let seen_before = next.seen;
                let canonical = !matches!(case, EventCase::NonCanonical);
                let unknown_keys_ok = !matches!(case, EventCase::UnknownKeysDropped);
                let store_ok = !matches!(case, EventCase::WrongStore);
                let hash = event_hash_for(seq, case);
                let prev_hash = if matches!(case, EventCase::PrevMismatch) {
                    next.head_hash.saturating_add(1)
                } else {
                    next.head_hash
                };

                let mut record_last = |kind: LastEffectKind, prev_ok: bool| {
                    next.last_event = Some(LastEvent {
                        kind,
                        seq,
                        seen_before,
                        canonical,
                        unknown_keys_ok,
                        store_ok,
                        prev_ok,
                    });
                };

                if !store_ok || !canonical || !unknown_keys_ok {
                    next.errored = true;
                    record_last(LastEffectKind::Rejected, false);
                    return Some(next);
                }

                if let Some(existing) = next.applied_hashes.get(&seq) {
                    if *existing != hash {
                        next.errored = true;
                        record_last(LastEffectKind::Rejected, false);
                        return Some(next);
                    }
                    record_last(LastEffectKind::Duplicate, true);
                    return Some(next);
                }

                if let Some(existing) = next.buffer.get(&seq) {
                    if existing.hash != hash {
                        next.errored = true;
                        record_last(LastEffectKind::Rejected, false);
                        return Some(next);
                    }
                    record_last(LastEffectKind::Duplicate, true);
                    return Some(next);
                }

                if seq == next.seen + 1 {
                    let prev_ok = prev_hash == next.head_hash;
                    if !prev_ok {
                        next.errored = true;
                        record_last(LastEffectKind::Rejected, false);
                        return Some(next);
                    }

                    next.seen = seq;
                    next.head_hash = hash;
                    next.applied_hashes.insert(seq, hash);
                    record_last(LastEffectKind::Applied, true);

                    // Flush any contiguous buffered suffix.
                    loop {
                        let next_seq = next.seen + 1;
                        let Some(buffered) = next.buffer.remove(&next_seq) else {
                            break;
                        };
                        if buffered.prev_hash != next.head_hash {
                            next.errored = true;
                            break;
                        }
                        next.seen = next_seq;
                        next.head_hash = buffered.hash;
                        next.applied_hashes.insert(next_seq, buffered.hash);
                    }
                } else if seq > next.seen + 1 {
                    next.buffer.insert(seq, BufferedEvent { hash, prev_hash });
                    record_last(LastEffectKind::Buffered, true);
                } else {
                    record_last(LastEffectKind::Duplicate, true);
                }
            }
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("canonical-only acceptance", |_, s: &State| {
                match s.last_event.as_ref() {
                    Some(last) if matches!(last.kind, LastEffectKind::Applied | LastEffectKind::Buffered) => {
                        last.canonical && last.unknown_keys_ok && last.store_ok
                    }
                    _ => true,
                }
            }),
            Property::always("unknown keys preserved for hash validity", |_, s: &State| {
                match s.last_event.as_ref() {
                    Some(last) if matches!(last.kind, LastEffectKind::Applied | LastEffectKind::Buffered) => {
                        last.unknown_keys_ok
                    }
                    _ => true,
                }
            }),
            Property::always("prev continuity on apply", |_, s: &State| {
                match s.last_event.as_ref() {
                    Some(last) if matches!(last.kind, LastEffectKind::Applied) => last.prev_ok,
                    _ => true,
                }
            }),
            Property::always("out-of-order events are buffered", |_, s: &State| {
                match s.last_event.as_ref() {
                    Some(last) if matches!(last.kind, LastEffectKind::Buffered) => {
                        last.seq > last.seen_before + 1 && s.seen == last.seen_before
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
            println!("Exploring canonical hash/prev state space on {address}.");
            CanonicalHashModel
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking canonical hash/prev rules.");
            CanonicalHashModel
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  canonical_hash_machine check");
            println!("  canonical_hash_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
