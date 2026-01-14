//! Checkpoint scheduling with debounce, max interval, and max events.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::time::{Duration, Instant};

use crate::core::{NamespaceId, ReplicaId, StoreId};

const DEFAULT_DEBOUNCE_MS: u64 = 200;
const DEFAULT_MAX_INTERVAL_MS: u64 = 1000;
const DEFAULT_MAX_EVENTS: u64 = 2000;

#[derive(Clone, Debug)]
pub struct CheckpointGroupConfig {
    pub store_id: StoreId,
    pub group: String,
    pub namespaces: Vec<NamespaceId>,
    pub git_ref: String,
    pub checkpoint_writers: Vec<ReplicaId>,
    pub primary_writer: Option<ReplicaId>,
    pub local_replica_id: ReplicaId,
    pub debounce: Duration,
    pub max_interval: Duration,
    pub max_events: u64,
    pub durable_copy_via_git: bool,
}

impl CheckpointGroupConfig {
    pub fn core_default(store_id: StoreId, local_replica_id: ReplicaId) -> Self {
        let group = "core".to_string();
        Self {
            store_id,
            group: group.clone(),
            namespaces: vec![NamespaceId::core()],
            git_ref: format!("refs/beads/{store_id}/{group}"),
            checkpoint_writers: vec![local_replica_id],
            primary_writer: Some(local_replica_id),
            local_replica_id,
            debounce: Duration::from_millis(DEFAULT_DEBOUNCE_MS),
            max_interval: Duration::from_millis(DEFAULT_MAX_INTERVAL_MS),
            max_events: DEFAULT_MAX_EVENTS,
            durable_copy_via_git: false,
        }
    }

    pub fn includes_namespace(&self, namespace: &NamespaceId) -> bool {
        self.namespaces.iter().any(|ns| ns == namespace)
    }

    pub fn auto_push(&self) -> bool {
        match self.primary_writer {
            Some(primary) => primary == self.local_replica_id,
            None => self
                .checkpoint_writers
                .iter()
                .any(|replica| *replica == self.local_replica_id),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct CheckpointGroupKey {
    pub store_id: StoreId,
    pub group: String,
}

pub struct CheckpointScheduler {
    groups: HashMap<CheckpointGroupKey, GroupState>,
    pending: HashMap<CheckpointGroupKey, Instant>,
    heap: BinaryHeap<Reverse<(Instant, CheckpointGroupKey)>>,
}

impl CheckpointScheduler {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            pending: HashMap::new(),
            heap: BinaryHeap::new(),
        }
    }

    pub fn register_group(&mut self, config: CheckpointGroupConfig) -> CheckpointGroupKey {
        let key = CheckpointGroupKey {
            store_id: config.store_id,
            group: config.group.clone(),
        };
        let now = Instant::now();
        self.groups
            .entry(key.clone())
            .and_modify(|state| state.config = config.clone())
            .or_insert_with(|| GroupState::new(config));
        self.schedule_if_needed(&key, now);
        key
    }

    pub fn group_config(&self, key: &CheckpointGroupKey) -> Option<&CheckpointGroupConfig> {
        self.groups.get(key).map(|state| &state.config)
    }

    pub fn checkpoint_groups_for_store(&self, store_id: StoreId) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        for (key, state) in &self.groups {
            if key.store_id == store_id {
                map.insert(state.config.group.clone(), state.config.git_ref.clone());
            }
        }
        map
    }

    pub fn mark_dirty_for_namespace(
        &mut self,
        store_id: StoreId,
        namespace: &NamespaceId,
        events: u64,
    ) {
        self.mark_dirty_for_namespace_at(store_id, namespace, events, Instant::now());
    }

    pub fn mark_dirty_for_namespace_at(
        &mut self,
        store_id: StoreId,
        namespace: &NamespaceId,
        events: u64,
        now: Instant,
    ) {
        let keys: Vec<CheckpointGroupKey> = self
            .groups
            .iter()
            .filter(|(key, state)| {
                key.store_id == store_id && state.config.includes_namespace(namespace)
            })
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys {
            if let Some(state) = self.groups.get_mut(&key) {
                state.record_event(now, events);
            }
            self.schedule_if_needed(&key, now);
        }
    }

    pub fn next_deadline(&mut self) -> Option<Instant> {
        self.pop_stale();
        self.heap.peek().map(|Reverse((t, _))| *t)
    }

    pub fn drain_due(&mut self, now: Instant) -> Vec<CheckpointGroupKey> {
        let mut due = Vec::new();
        loop {
            self.pop_stale();
            let Some(Reverse((fire_at, key))) = self.heap.peek().cloned() else {
                break;
            };
            if fire_at > now {
                break;
            }
            let _ = self.heap.pop();
            if self.pending.get(&key).copied() == Some(fire_at) {
                self.pending.remove(&key);
                due.push(key);
            }
        }
        due
    }

    pub fn start_in_flight(&mut self, key: &CheckpointGroupKey, now: Instant) {
        if let Some(state) = self.groups.get_mut(key) {
            state.start_in_flight(now);
        }
        self.pending.remove(key);
    }

    pub fn complete_success(&mut self, key: &CheckpointGroupKey, now: Instant) {
        if let Some(state) = self.groups.get_mut(key) {
            state.complete_in_flight(now);
        }
        self.schedule_if_needed(key, now);
    }

    pub fn complete_failure(&mut self, key: &CheckpointGroupKey, now: Instant) {
        if let Some(state) = self.groups.get_mut(key) {
            state.fail_in_flight(now);
        }
        self.schedule_if_needed(key, now);
    }

    fn schedule_if_needed(&mut self, key: &CheckpointGroupKey, now: Instant) {
        let Some(state) = self.groups.get(key) else {
            return;
        };
        let Some(deadline) = state.deadline(now) else {
            self.pending.remove(key);
            return;
        };

        self.pending.insert(key.clone(), deadline);
        self.heap.push(Reverse((deadline, key.clone())));
    }

    fn pop_stale(&mut self) {
        while let Some(Reverse((fire_at, key))) = self.heap.peek() {
            match self.pending.get(key).copied() {
                Some(current) if current == *fire_at => break,
                _ => {
                    let _ = self.heap.pop();
                }
            }
        }
    }
}

struct GroupState {
    config: CheckpointGroupConfig,
    dirty: bool,
    dirty_since: Option<Instant>,
    last_event_at: Option<Instant>,
    pending_events: u64,
    in_flight: bool,
    #[allow(dead_code)]
    last_checkpoint_at: Option<Instant>,
}

impl GroupState {
    fn new(config: CheckpointGroupConfig) -> Self {
        Self {
            config,
            dirty: false,
            dirty_since: None,
            last_event_at: None,
            pending_events: 0,
            in_flight: false,
            last_checkpoint_at: None,
        }
    }

    fn record_event(&mut self, now: Instant, events: u64) {
        self.pending_events = self.pending_events.saturating_add(events);
        if !self.dirty {
            self.dirty = true;
            self.dirty_since = Some(now);
        }
        self.last_event_at = Some(now);
    }

    fn start_in_flight(&mut self, _now: Instant) {
        self.in_flight = true;
        self.dirty = false;
        self.dirty_since = None;
        self.last_event_at = None;
        self.pending_events = 0;
    }

    fn complete_in_flight(&mut self, now: Instant) {
        self.in_flight = false;
        self.last_checkpoint_at = Some(now);
    }

    fn fail_in_flight(&mut self, now: Instant) {
        self.in_flight = false;
        if !self.dirty {
            self.dirty = true;
            self.dirty_since = Some(now);
        }
        self.last_event_at = Some(now);
    }

    fn deadline(&self, now: Instant) -> Option<Instant> {
        if !self.dirty || self.in_flight || !self.config.auto_push() {
            return None;
        }

        let last_event = self.last_event_at?;
        let dirty_since = self.dirty_since.unwrap_or(last_event);
        let debounce_deadline = last_event + self.config.debounce;
        let max_deadline = dirty_since + self.config.max_interval;
        let mut deadline = debounce_deadline.min(max_deadline);
        if self.pending_events >= self.config.max_events {
            deadline = now;
        }
        Some(deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn config_with_limits(store_id: StoreId, replica_id: ReplicaId) -> CheckpointGroupConfig {
        let mut config = CheckpointGroupConfig::core_default(store_id, replica_id);
        config.debounce = Duration::from_millis(10);
        config.max_interval = Duration::from_millis(40);
        config.max_events = 3;
        config
    }

    #[test]
    fn debounce_reschedules_later() {
        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let mut scheduler = CheckpointScheduler::new();
        let key = scheduler.register_group(config_with_limits(store_id, replica_id));

        let base = Instant::now();
        scheduler.mark_dirty_for_namespace_at(store_id, &NamespaceId::core(), 1, base);
        assert_eq!(
            scheduler.next_deadline(),
            Some(base + Duration::from_millis(10))
        );

        scheduler.mark_dirty_for_namespace_at(
            store_id,
            &NamespaceId::core(),
            1,
            base + Duration::from_millis(5),
        );
        assert_eq!(
            scheduler.next_deadline(),
            Some(base + Duration::from_millis(15))
        );

        let due = scheduler.drain_due(base + Duration::from_millis(15));
        assert_eq!(due, vec![key]);
    }

    #[test]
    fn max_interval_caps_deadline() {
        let store_id = StoreId::new(Uuid::from_bytes([3u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([4u8; 16]));
        let mut scheduler = CheckpointScheduler::new();
        scheduler.register_group(config_with_limits(store_id, replica_id));

        let base = Instant::now();
        scheduler.mark_dirty_for_namespace_at(store_id, &NamespaceId::core(), 1, base);
        scheduler.mark_dirty_for_namespace_at(
            store_id,
            &NamespaceId::core(),
            1,
            base + Duration::from_millis(35),
        );
        // max_interval is 40ms, so deadline should cap at base+40ms.
        assert_eq!(
            scheduler.next_deadline(),
            Some(base + Duration::from_millis(40))
        );
    }

    #[test]
    fn max_events_triggers_immediate() {
        let store_id = StoreId::new(Uuid::from_bytes([5u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([6u8; 16]));
        let mut scheduler = CheckpointScheduler::new();
        let key = scheduler.register_group(config_with_limits(store_id, replica_id));

        let base = Instant::now();
        scheduler.mark_dirty_for_namespace_at(store_id, &NamespaceId::core(), 3, base);
        assert_eq!(scheduler.next_deadline(), Some(base));

        let due = scheduler.drain_due(base);
        assert_eq!(due, vec![key]);
    }

    #[test]
    fn dirty_during_in_flight_schedules_follow_up() {
        let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([8u8; 16]));
        let mut scheduler = CheckpointScheduler::new();
        let key = scheduler.register_group(config_with_limits(store_id, replica_id));

        let base = Instant::now();
        scheduler.mark_dirty_for_namespace_at(store_id, &NamespaceId::core(), 1, base);
        let due = scheduler.drain_due(base + Duration::from_millis(10));
        assert_eq!(due, vec![key.clone()]);

        scheduler.start_in_flight(&key, base + Duration::from_millis(10));
        scheduler.mark_dirty_for_namespace_at(
            store_id,
            &NamespaceId::core(),
            1,
            base + Duration::from_millis(12),
        );

        assert!(scheduler.next_deadline().is_none());

        scheduler.complete_success(&key, base + Duration::from_millis(20));
        assert_eq!(
            scheduler.next_deadline(),
            Some(base + Duration::from_millis(22))
        );
    }
}
