use std::collections::VecDeque;

use beads_daemon::broadcast::BroadcastEvent;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PendingDrop {
    pub dropped_events: usize,
    pub dropped_bytes: usize,
    pub dropped_new_event_bytes: Option<usize>,
}

impl PendingDrop {
    pub fn total_events(self) -> usize {
        self.dropped_events
            + if self.dropped_new_event_bytes.is_some() {
                1
            } else {
                0
            }
    }

    pub fn total_bytes(self) -> usize {
        self.dropped_bytes + self.dropped_new_event_bytes.unwrap_or(0)
    }

    pub fn dropped_new_event(self) -> bool {
        self.dropped_new_event_bytes.is_some()
    }

    pub fn dropped_any(self) -> bool {
        self.total_events() > 0
    }
}

#[derive(Debug)]
pub(super) struct PendingEvents {
    events: VecDeque<BroadcastEvent>,
    max_events: usize,
    max_bytes: usize,
    total_bytes: usize,
}

impl PendingEvents {
    pub(super) fn new(max_events: usize, max_bytes: usize) -> Self {
        let max_events = max_events.max(1);
        let max_bytes = max_bytes.max(1);
        Self {
            events: VecDeque::new(),
            max_events,
            max_bytes,
            total_bytes: 0,
        }
    }

    pub(super) fn push(&mut self, event: BroadcastEvent) -> PendingDrop {
        let event_bytes = event.bytes.len();
        if event_bytes > self.max_bytes {
            return PendingDrop {
                dropped_events: 0,
                dropped_bytes: 0,
                dropped_new_event_bytes: Some(event_bytes),
            };
        }

        let mut dropped_events = 0;
        let mut dropped_bytes = 0;
        while self.events.len() >= self.max_events
            || self.total_bytes + event_bytes > self.max_bytes
        {
            let Some(dropped) = self.events.pop_front() else {
                break;
            };
            let dropped_len = dropped.bytes.len();
            debug_assert!(self.total_bytes >= dropped_len, "pending bytes underflow");
            self.total_bytes -= dropped_len;
            dropped_events += 1;
            dropped_bytes += dropped_len;
        }

        if self.events.len() >= self.max_events || self.total_bytes + event_bytes > self.max_bytes {
            return PendingDrop {
                dropped_events,
                dropped_bytes,
                dropped_new_event_bytes: Some(event_bytes),
            };
        }

        self.total_bytes += event_bytes;
        self.events.push_back(event);
        PendingDrop {
            dropped_events,
            dropped_bytes,
            dropped_new_event_bytes: None,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.events.len()
    }

    pub(super) fn bytes(&self) -> usize {
        self.total_bytes
    }

    pub(super) fn max_events(&self) -> usize {
        self.max_events
    }

    pub(super) fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub(super) fn drain(&mut self) -> std::collections::vec_deque::Drain<'_, BroadcastEvent> {
        self.total_bytes = 0;
        self.events.drain(..)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use uuid::Uuid;

    use crate::core::{EventBytes, EventId, NamespaceId, Opaque, ReplicaId, Seq1, Sha256};

    fn event(seq: u64, bytes: usize) -> BroadcastEvent {
        let replica = ReplicaId::new(Uuid::from_bytes([7u8; 16]));
        BroadcastEvent::new(
            EventId::new(replica, NamespaceId::core(), Seq1::from_u64(seq).unwrap()),
            Sha256([seq as u8; 32]),
            None,
            EventBytes::<Opaque>::new(Bytes::from(vec![0u8; bytes])),
        )
    }

    #[test]
    fn pending_events_drops_oldest_on_event_limit() {
        let mut pending = PendingEvents::new(2, 1024);

        assert!(!pending.push(event(1, 10)).dropped_any());
        assert!(!pending.push(event(2, 10)).dropped_any());
        let drop = pending.push(event(3, 10));

        assert_eq!(drop.total_events(), 1);
        assert_eq!(drop.total_bytes(), 10);
        assert_eq!(pending.len(), 2);
        assert_eq!(pending.bytes(), 20);

        let seqs = pending
            .drain()
            .map(|event| event.event_id.origin_seq.get())
            .collect::<Vec<_>>();
        assert_eq!(seqs, vec![2, 3]);
    }

    #[test]
    fn pending_events_drops_oldest_on_byte_limit() {
        let mut pending = PendingEvents::new(10, 5);

        assert!(!pending.push(event(1, 3)).dropped_any());
        let drop = pending.push(event(2, 3));

        assert_eq!(drop.total_events(), 1);
        assert_eq!(drop.total_bytes(), 3);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending.bytes(), 3);

        let seqs = pending
            .drain()
            .map(|event| event.event_id.origin_seq.get())
            .collect::<Vec<_>>();
        assert_eq!(seqs, vec![2]);
    }

    #[test]
    fn pending_events_drops_new_event_too_large() {
        let mut pending = PendingEvents::new(10, 4);

        let drop = pending.push(event(1, 5));
        assert!(drop.dropped_new_event());
        assert_eq!(drop.total_events(), 1);
        assert_eq!(drop.total_bytes(), 5);
        assert_eq!(pending.len(), 0);
        assert_eq!(pending.bytes(), 0);
    }
}
