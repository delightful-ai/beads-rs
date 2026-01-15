//! WANT frame assembly helpers.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::core::{EventBytes, EventFrameV1, Limits, NamespaceId, Opaque, ReplicaId};
use crate::daemon::broadcast::BroadcastEvent;
use crate::daemon::repl::proto::Want;
use crate::daemon::repl::runtime::{WalRangeError, WalRangeReader};

type WantKey = (NamespaceId, ReplicaId);

pub(crate) enum WantFramesOutcome {
    Frames(Vec<EventFrameV1>),
    BootstrapRequired { namespaces: BTreeSet<NamespaceId> },
}

struct WantState {
    next_seq: u64,
    frames: VecDeque<EventFrameV1>,
    stopped: bool,
}

impl WantState {
    fn new(next_seq: u64) -> Self {
        Self {
            next_seq,
            frames: VecDeque::new(),
            stopped: false,
        }
    }
}

pub(crate) fn broadcast_to_frame(event: BroadcastEvent) -> EventFrameV1 {
    EventFrameV1 {
        eid: event.event_id,
        sha256: event.sha256,
        prev_sha256: event.prev_sha256,
        bytes: EventBytes::<Opaque>::from(event.bytes),
    }
}

pub(crate) fn build_want_frames(
    want: &Want,
    cache: Vec<BroadcastEvent>,
    wal_reader: Option<&WalRangeReader>,
    limits: &Limits,
    allowed_set: Option<&BTreeSet<NamespaceId>>,
) -> Result<WantFramesOutcome, WalRangeError> {
    let mut needed: BTreeMap<WantKey, u64> = BTreeMap::new();
    for (namespace, origins) in &want.want {
        if let Some(allowed) = allowed_set
            && !allowed.contains(namespace)
        {
            continue;
        }
        for (origin, seq) in origins {
            needed.insert((namespace.clone(), *origin), *seq);
        }
    }

    if needed.is_empty() {
        return Ok(WantFramesOutcome::Frames(Vec::new()));
    }

    let mut states: BTreeMap<WantKey, WantState> = needed
        .iter()
        .map(|(key, seq)| (key.clone(), WantState::new(seq.saturating_add(1))))
        .collect();

    for event in cache {
        let key = (event.namespace.clone(), event.event_id.origin_replica_id);
        let Some(state) = states.get_mut(&key) else {
            continue;
        };
        if state.stopped {
            continue;
        }
        let seq = event.event_id.origin_seq.get();
        if seq < state.next_seq {
            continue;
        }
        if seq == state.next_seq {
            state.frames.push_back(broadcast_to_frame(event));
            state.next_seq = state.next_seq.saturating_add(1);
        } else if !state.frames.is_empty() {
            state.stopped = true;
        }
    }

    if let Some(wal_reader) = wal_reader {
        for (key, want_seq) in &needed {
            let state = states.get_mut(key).expect("state entry");
            if !state.frames.is_empty() {
                continue;
            }
            let (namespace, origin) = key;
            match wal_reader.read_range(namespace, origin, *want_seq, limits.max_event_batch_bytes)
            {
                Ok(wal_frames) => {
                    state.frames = VecDeque::from(wal_frames);
                }
                Err(err @ WalRangeError::MissingRange { .. }) => {
                    let _ = err;
                }
                Err(err) => return Err(err),
            }
        }
    }

    if states.values().any(|state| state.frames.is_empty()) {
        let namespaces: BTreeSet<NamespaceId> = needed.keys().map(|(ns, _)| ns.clone()).collect();
        return Ok(WantFramesOutcome::BootstrapRequired { namespaces });
    }

    Ok(WantFramesOutcome::Frames(round_robin_frames(states)))
}

fn round_robin_frames(states: BTreeMap<WantKey, WantState>) -> Vec<EventFrameV1> {
    let mut queues: VecDeque<VecDeque<EventFrameV1>> =
        states.into_values().map(|state| state.frames).collect();
    let mut frames = Vec::new();

    loop {
        let mut progressed = false;
        for queue in queues.iter_mut() {
            if let Some(frame) = queue.pop_front() {
                frames.push(frame);
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
    }

    frames
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use uuid::Uuid;

    use crate::core::{Canonical, EventBytes, EventId, Seq1, Sha256};
    use crate::daemon::repl::proto::WatermarkMap;

    fn make_event(namespace: NamespaceId, origin: ReplicaId, seq: u64) -> BroadcastEvent {
        let seq1 = Seq1::from_u64(seq).expect("seq1");
        let event_id = EventId::new(origin, namespace, seq1);
        let sha = Sha256([seq as u8; 32]);
        BroadcastEvent::new(
            event_id,
            sha,
            None,
            EventBytes::<Canonical>::new(Bytes::from_static(b"x")),
        )
    }

    fn make_want(entries: Vec<(NamespaceId, ReplicaId, u64)>) -> Want {
        let mut want_map = WatermarkMap::new();
        for (namespace, origin, seq) in entries {
            want_map.entry(namespace).or_default().insert(origin, seq);
        }
        Want { want: want_map }
    }

    #[test]
    fn want_frames_round_robin_interleaves_keys() {
        let ns_alpha = NamespaceId::parse("alpha").unwrap();
        let ns_beta = NamespaceId::parse("beta").unwrap();
        let origin_a = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
        let origin_b = ReplicaId::new(Uuid::from_bytes([2u8; 16]));

        let want = make_want(vec![
            (ns_alpha.clone(), origin_a, 0),
            (ns_beta.clone(), origin_b, 0),
        ]);

        let cache = vec![
            make_event(ns_alpha.clone(), origin_a, 1),
            make_event(ns_alpha.clone(), origin_a, 2),
            make_event(ns_beta.clone(), origin_b, 1),
            make_event(ns_beta.clone(), origin_b, 2),
        ];

        let outcome = build_want_frames(&want, cache, None, &Limits::default(), None).unwrap();
        let WantFramesOutcome::Frames(frames) = outcome else {
            panic!("expected frames");
        };

        let actual = frames
            .iter()
            .map(|frame| {
                (
                    frame.eid.namespace.clone(),
                    frame.eid.origin_replica_id,
                    frame.eid.origin_seq.get(),
                )
            })
            .collect::<Vec<_>>();
        let expected = vec![
            (ns_alpha.clone(), origin_a, 1),
            (ns_beta.clone(), origin_b, 1),
            (ns_alpha.clone(), origin_a, 2),
            (ns_beta.clone(), origin_b, 2),
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn want_frames_stop_at_gap() {
        let ns_alpha = NamespaceId::parse("alpha").unwrap();
        let ns_beta = NamespaceId::parse("beta").unwrap();
        let origin_a = ReplicaId::new(Uuid::from_bytes([3u8; 16]));
        let origin_b = ReplicaId::new(Uuid::from_bytes([4u8; 16]));

        let want = make_want(vec![
            (ns_alpha.clone(), origin_a, 0),
            (ns_beta.clone(), origin_b, 0),
        ]);

        let cache = vec![
            make_event(ns_alpha.clone(), origin_a, 1),
            make_event(ns_alpha.clone(), origin_a, 3),
            make_event(ns_beta.clone(), origin_b, 1),
        ];

        let outcome = build_want_frames(&want, cache, None, &Limits::default(), None).unwrap();
        let WantFramesOutcome::Frames(frames) = outcome else {
            panic!("expected frames");
        };

        let actual = frames
            .iter()
            .map(|frame| {
                (
                    frame.eid.namespace.clone(),
                    frame.eid.origin_replica_id,
                    frame.eid.origin_seq.get(),
                )
            })
            .collect::<Vec<_>>();
        let expected = vec![
            (ns_alpha.clone(), origin_a, 1),
            (ns_beta.clone(), origin_b, 1),
        ];
        assert_eq!(actual, expected);
    }
}
