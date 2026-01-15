//! Event broadcaster and hot cache for realtime replication.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam::channel::{Receiver, Sender, TryRecvError, TrySendError};
use thiserror::Error;

use crate::core::{EventBytes, EventId, Limits, NamespaceId, Opaque, Sha256};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BroadcastEvent {
    pub event_id: EventId,
    pub namespace: NamespaceId,
    pub sha256: Sha256,
    pub prev_sha256: Option<Sha256>,
    pub bytes: EventBytes<Opaque>,
}

impl BroadcastEvent {
    pub fn new(
        event_id: EventId,
        sha256: Sha256,
        prev_sha256: Option<Sha256>,
        bytes: EventBytes<Opaque>,
    ) -> Self {
        let namespace = event_id.namespace.clone();
        Self {
            event_id,
            namespace,
            sha256,
            prev_sha256,
            bytes,
        }
    }

    fn byte_len(&self) -> usize {
        self.bytes.len()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BroadcasterLimits {
    pub max_subscribers: usize,
    pub hot_cache_max_events: usize,
    pub hot_cache_max_bytes: usize,
}

impl BroadcasterLimits {
    pub fn from_limits(limits: &Limits) -> Self {
        Self {
            max_subscribers: limits.max_broadcast_subscribers,
            hot_cache_max_events: limits.event_hot_cache_max_events,
            hot_cache_max_bytes: limits.event_hot_cache_max_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriberLimits {
    pub max_events: usize,
    pub max_bytes: usize,
}

impl SubscriberLimits {
    pub fn new(max_events: usize, max_bytes: usize) -> Result<Self, BroadcastError> {
        if max_events == 0 {
            return Err(BroadcastError::InvalidSubscriberLimits {
                reason: "max_events must be > 0".to_string(),
            });
        }
        if max_bytes == 0 {
            return Err(BroadcastError::InvalidSubscriberLimits {
                reason: "max_bytes must be > 0".to_string(),
            });
        }
        Ok(Self {
            max_events,
            max_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    SubscriberLagged,
}

pub struct EventSubscription {
    receiver: Receiver<BroadcastEvent>,
    queued_bytes: Arc<AtomicUsize>,
    drop_reason: Arc<Mutex<Option<DropReason>>>,
}

impl EventSubscription {
    pub fn recv(&self) -> Result<BroadcastEvent, crossbeam::channel::RecvError> {
        let event = self.receiver.recv()?;
        self.decrement_bytes(event.byte_len());
        Ok(event)
    }

    pub fn try_recv(&self) -> Result<BroadcastEvent, TryRecvError> {
        let event = self.receiver.try_recv()?;
        self.decrement_bytes(event.byte_len());
        Ok(event)
    }

    pub fn drop_reason(&self) -> Option<DropReason> {
        self.drop_reason.lock().ok().and_then(|guard| *guard)
    }

    fn decrement_bytes(&self, amount: usize) {
        let prev = self.queued_bytes.fetch_sub(amount, Ordering::AcqRel);
        debug_assert!(prev >= amount, "queued bytes underflow");
    }
}

#[derive(Clone)]
pub struct EventBroadcaster {
    inner: Arc<Mutex<BroadcasterState>>,
}

impl EventBroadcaster {
    pub fn new(limits: BroadcasterLimits) -> Self {
        Self {
            inner: Arc::new(Mutex::new(BroadcasterState::new(limits))),
        }
    }

    pub fn subscribe(&self, limits: SubscriberLimits) -> Result<EventSubscription, BroadcastError> {
        let mut state = self.lock_state()?;
        if state.subscribers.len() >= state.limits.max_subscribers {
            return Err(BroadcastError::SubscriberLimitReached {
                max_subscribers: state.limits.max_subscribers,
            });
        }

        let (sender, receiver) = crossbeam::channel::bounded(limits.max_events);
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        let drop_reason = Arc::new(Mutex::new(None));
        let id = state.next_subscriber_id;
        state.next_subscriber_id = state.next_subscriber_id.saturating_add(1);
        state.subscribers.insert(
            id,
            SubscriberState {
                sender,
                max_bytes: limits.max_bytes,
                queued_bytes: Arc::clone(&queued_bytes),
                drop_reason: Arc::clone(&drop_reason),
            },
        );

        Ok(EventSubscription {
            receiver,
            queued_bytes,
            drop_reason,
        })
    }

    pub fn publish(&self, event: BroadcastEvent) -> Result<(), BroadcastError> {
        let mut state = self.lock_state()?;
        state.push_hot_cache(event.clone());

        let mut dropped = Vec::new();
        for (id, subscriber) in &state.subscribers {
            let queued = subscriber.queued_bytes.load(Ordering::Acquire);
            if queued.saturating_add(event.byte_len()) > subscriber.max_bytes {
                subscriber.set_drop_reason(DropReason::SubscriberLagged);
                dropped.push(*id);
                continue;
            }

            match subscriber.sender.try_send(event.clone()) {
                Ok(()) => {
                    subscriber
                        .queued_bytes
                        .fetch_add(event.byte_len(), Ordering::AcqRel);
                }
                Err(TrySendError::Full(_)) => {
                    subscriber.set_drop_reason(DropReason::SubscriberLagged);
                    dropped.push(*id);
                }
                Err(TrySendError::Disconnected(_)) => {
                    dropped.push(*id);
                }
            }
        }

        for id in dropped {
            state.subscribers.remove(&id);
        }

        Ok(())
    }

    pub fn hot_cache(&self) -> Result<Vec<BroadcastEvent>, BroadcastError> {
        let state = self.lock_state()?;
        Ok(state.hot_cache.iter().cloned().collect())
    }

    pub fn subscriber_count(&self) -> Result<usize, BroadcastError> {
        let state = self.lock_state()?;
        Ok(state.subscribers.len())
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, BroadcasterState>, BroadcastError> {
        self.inner.lock().map_err(|_| BroadcastError::LockPoisoned)
    }
}

struct BroadcasterState {
    limits: BroadcasterLimits,
    hot_cache: VecDeque<BroadcastEvent>,
    hot_cache_bytes: usize,
    next_subscriber_id: u64,
    subscribers: BTreeMap<u64, SubscriberState>,
}

impl BroadcasterState {
    fn new(limits: BroadcasterLimits) -> Self {
        Self {
            limits,
            hot_cache: VecDeque::new(),
            hot_cache_bytes: 0,
            next_subscriber_id: 1,
            subscribers: BTreeMap::new(),
        }
    }

    fn push_hot_cache(&mut self, event: BroadcastEvent) {
        self.hot_cache_bytes = self.hot_cache_bytes.saturating_add(event.byte_len());
        self.hot_cache.push_back(event);

        while self.hot_cache.len() > self.limits.hot_cache_max_events
            || self.hot_cache_bytes > self.limits.hot_cache_max_bytes
        {
            if let Some(evicted) = self.hot_cache.pop_front() {
                self.hot_cache_bytes = self.hot_cache_bytes.saturating_sub(evicted.byte_len());
            } else {
                break;
            }
        }
    }
}

struct SubscriberState {
    sender: Sender<BroadcastEvent>,
    max_bytes: usize,
    queued_bytes: Arc<AtomicUsize>,
    drop_reason: Arc<Mutex<Option<DropReason>>>,
}

impl SubscriberState {
    fn set_drop_reason(&self, reason: DropReason) {
        if let Ok(mut guard) = self.drop_reason.lock()
            && guard.is_none()
        {
            *guard = Some(reason);
        }
    }
}

#[derive(Debug, Error)]
pub enum BroadcastError {
    #[error("subscriber limit reached ({max_subscribers})")]
    SubscriberLimitReached { max_subscribers: usize },
    #[error("subscriber limits invalid: {reason}")]
    InvalidSubscriberLimits { reason: String },
    #[error("broadcaster lock poisoned")]
    LockPoisoned,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use uuid::Uuid;

    use crate::core::{NamespaceId, Opaque, ReplicaId, Seq1};

    fn event(seq: u64, bytes: usize) -> BroadcastEvent {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
        let event_id = EventId::new(origin, namespace, Seq1::from_u64(seq).unwrap());
        let payload = vec![42u8; bytes.max(1)];
        let bytes = EventBytes::<Opaque>::new(Bytes::from(payload));
        let sha256 = Sha256([seq as u8; 32]);
        BroadcastEvent::new(event_id, sha256, None, bytes)
    }

    #[test]
    fn delivers_events_in_order() {
        let broadcaster = EventBroadcaster::new(BroadcasterLimits {
            max_subscribers: 1,
            hot_cache_max_events: 8,
            hot_cache_max_bytes: 1024,
        });
        let sub = broadcaster
            .subscribe(SubscriberLimits::new(8, 1024).unwrap())
            .unwrap();

        broadcaster.publish(event(1, 4)).unwrap();
        broadcaster.publish(event(2, 4)).unwrap();

        let first = sub.recv().unwrap();
        let second = sub.recv().unwrap();
        assert_eq!(first.event_id.origin_seq, Seq1::from_u64(1).unwrap());
        assert_eq!(second.event_id.origin_seq, Seq1::from_u64(2).unwrap());
    }

    #[test]
    fn subscriber_drops_on_full_queue() {
        let broadcaster = EventBroadcaster::new(BroadcasterLimits {
            max_subscribers: 1,
            hot_cache_max_events: 8,
            hot_cache_max_bytes: 1024,
        });
        let sub = broadcaster
            .subscribe(SubscriberLimits::new(1, 1024).unwrap())
            .unwrap();

        broadcaster.publish(event(1, 8)).unwrap();
        broadcaster.publish(event(2, 8)).unwrap();

        assert_eq!(sub.drop_reason(), Some(DropReason::SubscriberLagged));
    }

    #[test]
    fn subscriber_drops_on_byte_limit() {
        let broadcaster = EventBroadcaster::new(BroadcasterLimits {
            max_subscribers: 1,
            hot_cache_max_events: 8,
            hot_cache_max_bytes: 1024,
        });
        let sub = broadcaster
            .subscribe(SubscriberLimits::new(8, 10).unwrap())
            .unwrap();

        broadcaster.publish(event(1, 6)).unwrap();
        broadcaster.publish(event(2, 6)).unwrap();

        assert_eq!(sub.drop_reason(), Some(DropReason::SubscriberLagged));
    }

    #[test]
    fn hot_cache_evicts_by_event_count() {
        let broadcaster = EventBroadcaster::new(BroadcasterLimits {
            max_subscribers: 1,
            hot_cache_max_events: 2,
            hot_cache_max_bytes: 1024,
        });

        broadcaster.publish(event(1, 1)).unwrap();
        broadcaster.publish(event(2, 1)).unwrap();
        broadcaster.publish(event(3, 1)).unwrap();

        let cache = broadcaster.hot_cache().unwrap();
        assert_eq!(cache.len(), 2);
        assert_eq!(cache[0].event_id.origin_seq, Seq1::from_u64(2).unwrap());
        assert_eq!(cache[1].event_id.origin_seq, Seq1::from_u64(3).unwrap());
    }

    #[test]
    fn hot_cache_evicts_by_bytes() {
        let broadcaster = EventBroadcaster::new(BroadcasterLimits {
            max_subscribers: 1,
            hot_cache_max_events: 8,
            hot_cache_max_bytes: 5,
        });

        broadcaster.publish(event(1, 4)).unwrap();
        broadcaster.publish(event(2, 4)).unwrap();

        let cache = broadcaster.hot_cache().unwrap();
        assert_eq!(cache.len(), 1);
        assert_eq!(cache[0].event_id.origin_seq, Seq1::from_u64(2).unwrap());
    }
}
