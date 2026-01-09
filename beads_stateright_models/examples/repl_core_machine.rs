//! Model: Realtime replication core (events, gaps, WANT, ACK).
//!
//! Plan alignment:
//! - EVENTS ordering + contiguity rules: REALTIME_PLAN.md §9.4
//! - ACK monotonicity + applied vs durable: REALTIME_PLAN.md §9.5, §0.12
//! - WANT semantics + gap buffering: REALTIME_PLAN.md §9.6, §9.8
//! - Equivocation detection: REALTIME_PLAN.md §9.4
//!
//! This model intentionally abstracts payloads to a sha variant token and focuses on:
//! - contiguity-only advancement (no gaps)
//! - bounded gap buffering + timeout
//! - idempotent apply
//! - equivocation hard-close
//! - ACK never exceeds contiguous seen

use stateright::{report::WriteReporter, Checker, Model, Property};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const MAX_SEQ: u8 = 4;
const MAX_BUFFER_EVENTS: usize = 3;
const GAP_TIMEOUT_TICKS: u8 = 2;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Ns {
    Core,
    Wf,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Origin {
    A,
    B,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum ShaVariant {
    A,
    B,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct StreamKey {
    ns: Ns,
    origin: Origin,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct StreamState {
    seen: u8,
    gap: BTreeSet<u8>,
    gap_started_at: Option<u8>,
}

impl StreamState {
    fn new() -> Self {
        Self {
            seen: 0,
            gap: BTreeSet::new(),
            gap_started_at: None,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct EventKey {
    key: StreamKey,
    seq: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum LastEffectKind {
    Applied { key: StreamKey, seq: u8, prev_seen: u8 },
    Buffered { key: StreamKey, seq: u8 },
    Duplicate { key: StreamKey, seq: u8 },
    AckSent,
    Rejected { code: &'static str },
    Tick,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct LastEffect {
    kind: LastEffectKind,
    changed: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    now: u8,
    streams: BTreeMap<StreamKey, StreamState>,
    seen_max: BTreeMap<StreamKey, u8>,
    applied: BTreeMap<StreamKey, BTreeSet<u8>>,
    known_sha: BTreeMap<EventKey, ShaVariant>,
    ack_durable: BTreeMap<StreamKey, u8>,
    ack_max: BTreeMap<StreamKey, u8>,
    errored: bool,
    equivocation_seen: bool,
    last_want: Option<(StreamKey, u8)>,
    last_effect: Option<LastEffect>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    Tick,
    Deliver {
        ns: Ns,
        origin: Origin,
        seq: u8,
        sha: ShaVariant,
    },
    SendAck,
}

#[derive(Clone, Debug)]
struct ReplCore;

fn stream_keys() -> Vec<StreamKey> {
    let mut keys = Vec::new();
    for ns in [Ns::Core, Ns::Wf] {
        for origin in [Origin::A, Origin::B] {
            keys.push(StreamKey { ns, origin });
        }
    }
    keys
}

impl Model for ReplCore {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut streams = BTreeMap::new();
        let mut seen_max = BTreeMap::new();
        let mut applied = BTreeMap::new();
        let mut ack_durable = BTreeMap::new();
        let mut ack_max = BTreeMap::new();
        for key in stream_keys() {
            streams.insert(key, StreamState::new());
            seen_max.insert(key, 0);
            applied.insert(key, BTreeSet::new());
            ack_durable.insert(key, 0);
            ack_max.insert(key, 0);
        }
        vec![State {
            now: 0,
            streams,
            seen_max,
            applied,
            known_sha: BTreeMap::new(),
            ack_durable,
            ack_max,
            errored: false,
            equivocation_seen: false,
            last_want: None,
            last_effect: None,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.errored {
            return;
        }
        actions.push(Action::Tick);
        actions.push(Action::SendAck);
        for ns in [Ns::Core, Ns::Wf] {
            for origin in [Origin::A, Origin::B] {
                for seq in 1..=MAX_SEQ {
                    actions.push(Action::Deliver {
                        ns,
                        origin,
                        seq,
                        sha: ShaVariant::A,
                    });
                    actions.push(Action::Deliver {
                        ns,
                        origin,
                        seq,
                        sha: ShaVariant::B,
                    });
                }
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        next.last_want = None;
        next.last_effect = None;

        if next.errored {
            return Some(next);
        }

        let mut effect_kind: Option<LastEffectKind> = None;

        match action {
            Action::Tick => {
                next.now = next.now.saturating_add(1);
                for stream in next.streams.values_mut() {
                    if let Some(start) = stream.gap_started_at {
                        if next.now.saturating_sub(start) > GAP_TIMEOUT_TICKS {
                            next.errored = true;
                            effect_kind = Some(LastEffectKind::Rejected { code: "gap_timeout" });
                            break;
                        }
                    }
                }
                if effect_kind.is_none() {
                    effect_kind = Some(LastEffectKind::Tick);
                }
            }
            Action::SendAck => {
                for (key, stream) in next.streams.iter() {
                    let entry = next.ack_durable.entry(*key).or_insert(0);
                    if stream.seen > *entry {
                        *entry = stream.seen;
                    }
                }
                effect_kind = Some(LastEffectKind::AckSent);
            }
            Action::Deliver { ns, origin, seq, sha } => {
                let key = StreamKey { ns, origin };
                let stream = next.streams.get_mut(&key).expect("stream exists");

                let event_key = EventKey { key, seq };
                match next.known_sha.get(&event_key).copied() {
                    Some(existing) if existing != sha => {
                        next.equivocation_seen = true;
                        next.errored = true;
                        effect_kind = Some(LastEffectKind::Rejected { code: "equivocation" });
                    }
                    Some(_) => {
                        effect_kind = Some(LastEffectKind::Duplicate { key, seq });
                    }
                    None => {
                        next.known_sha.insert(event_key, sha);

                        if seq <= stream.seen {
                            effect_kind = Some(LastEffectKind::Duplicate { key, seq });
                        } else if seq == stream.seen.saturating_add(1) {
                            let prev_seen = stream.seen;
                            let mut batch = vec![seq];
                            let mut cursor = seq.saturating_add(1);
                            while stream.gap.remove(&cursor) {
                                batch.push(cursor);
                                cursor = cursor.saturating_add(1);
                            }
                            for s in &batch {
                                next.applied
                                    .get_mut(&key)
                                    .expect("applied exists")
                                    .insert(*s);
                            }
                            stream.seen = *batch.last().expect("batch non-empty");
                            if stream.gap.is_empty() {
                                stream.gap_started_at = None;
                            }
                            effect_kind = Some(LastEffectKind::Applied {
                                key,
                                seq,
                                prev_seen,
                            });
                        } else {
                            if stream.gap_started_at.is_none() {
                                stream.gap_started_at = Some(next.now);
                            }
                            if stream.gap.len() >= MAX_BUFFER_EVENTS {
                                next.errored = true;
                                effect_kind = Some(LastEffectKind::Rejected {
                                    code: "gap_buffer_overflow",
                                });
                            } else if let Some(start) = stream.gap_started_at {
                                if next.now.saturating_sub(start) > GAP_TIMEOUT_TICKS {
                                    next.errored = true;
                                    effect_kind = Some(LastEffectKind::Rejected { code: "gap_timeout" });
                                } else {
                                    stream.gap.insert(seq);
                                    next.last_want = Some((key, stream.seen));
                                    effect_kind = Some(LastEffectKind::Buffered { key, seq });
                                }
                            }
                        }
                    }
                }
            }
        }

        for key in stream_keys() {
            let prev_seen = *state.seen_max.get(&key).unwrap_or(&0);
            let seen = next.streams.get(&key).map(|st| st.seen).unwrap_or(0);
            next.seen_max.insert(key, prev_seen.max(seen));

            let prev_ack = *state.ack_max.get(&key).unwrap_or(&0);
            let ack = *next.ack_durable.get(&key).unwrap_or(&0);
            next.ack_max.insert(key, prev_ack.max(ack));
        }

        let changed = next.streams != state.streams
            || next.applied != state.applied
            || next.ack_durable != state.ack_durable
            || next.known_sha != state.known_sha
            || next.errored != state.errored
            || next.equivocation_seen != state.equivocation_seen
            || next.seen_max != state.seen_max
            || next.ack_max != state.ack_max;

        if let Some(kind) = effect_kind {
            next.last_effect = Some(LastEffect { kind, changed });
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("ack never exceeds contiguous seen", |_, s: &State| {
                s.ack_durable.iter().all(|(key, ack)| {
                    let seen = s.streams.get(key).map(|st| st.seen).unwrap_or(0);
                    *ack <= seen
                })
            }),
            Property::always("seen map is monotonic", |_, s: &State| {
                s.streams.iter().all(|(key, stream)| {
                    s.seen_max.get(key).copied().unwrap_or(0) == stream.seen
                })
            }),
            Property::always("ack map is monotonic", |_, s: &State| {
                s.ack_durable.iter().all(|(key, ack)| {
                    s.ack_max.get(key).copied().unwrap_or(0) == *ack
                })
            }),
            Property::always("contiguous seen implies applied prefix", |_, s: &State| {
                s.streams.iter().all(|(key, stream)| {
                    let applied = s.applied.get(key).expect("applied exists");
                    (1..=stream.seen).all(|seq| applied.contains(&seq))
                })
            }),
            Property::always("applies only the next contiguous seq", |_, s: &State| {
                match s.last_effect.as_ref() {
                    Some(LastEffect {
                        kind: LastEffectKind::Applied { seq, prev_seen, .. },
                        ..
                    }) => *seq == prev_seen.saturating_add(1),
                    _ => true,
                }
            }),
            Property::always("buffered items are beyond next expected", |_, s: &State| {
                s.streams.iter().all(|(_, stream)| {
                    stream.gap.iter().all(|seq| *seq > stream.seen.saturating_add(1))
                })
            }),
            Property::always("buffer stays within bounds unless closed", |_, s: &State| {
                s.errored
                    || s.streams
                        .values()
                        .all(|stream| stream.gap.len() <= MAX_BUFFER_EVENTS)
            }),
            Property::always("equivocation implies hard close", |_, s: &State| {
                !s.equivocation_seen || s.errored
            }),
            Property::always("duplicate deliveries are idempotent", |_, s: &State| {
                match s.last_effect.as_ref() {
                    Some(LastEffect {
                        kind: LastEffectKind::Duplicate { .. },
                        changed,
                    }) => !changed,
                    _ => true,
                }
            }),
            Property::always("want from equals seen when emitted", |_, s: &State| {
                match s.last_want {
                    None => true,
                    Some((key, from)) => {
                        s.streams.get(&key).map(|st| st.seen) == Some(from)
                            && s.streams.get(&key).map(|st| !st.gap.is_empty()).unwrap_or(false)
                    }
                }
            }),
            Property::sometimes("can fully catch up", |_, s: &State| {
                s.streams.values().all(|stream| stream.seen == MAX_SEQ)
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
            println!("Exploring replication core state space on {address}.");
            ReplCore
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking replication core.");
            ReplCore
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  repl_core_machine check");
            println!("  repl_core_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
