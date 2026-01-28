//! Replication gap buffering and contiguity enforcement.

use std::collections::BTreeMap;

use crate::core::error::details::ReplRejectReason;
use crate::core::{
    Durable, HeadStatus, Limits, NamespaceId, PrevVerified, ReplicaId, Seq0, Seq1, Sha256,
    VerifiedEvent, VerifiedEventAny, Watermark, WatermarkError,
};
use crate::daemon::repl::{ContiguousBatch, ContiguousBatchError};

#[derive(Clone, Debug)]
pub struct GapBuffer {
    buffered: BTreeMap<Seq1, VerifiedEventAny>,
    buffered_bytes: usize,
    started_at_ms: Option<u64>,
    max_events: usize,
    max_bytes: usize,
    timeout_ms: u64,
}

impl GapBuffer {
    pub fn new(limits: &Limits) -> Self {
        Self {
            buffered: BTreeMap::new(),
            buffered_bytes: 0,
            started_at_ms: None,
            max_events: limits.max_repl_gap_events,
            max_bytes: limits.max_repl_gap_bytes,
            timeout_ms: limits.repl_gap_timeout_ms,
        }
    }
}

#[cfg(feature = "model-testing")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum HeadSnapshot {
    Genesis,
    Known(Sha256),
}

#[cfg(feature = "model-testing")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WatermarkSnapshot {
    pub seq: Seq0,
    pub head: HeadSnapshot,
}

#[cfg(feature = "model-testing")]
impl WatermarkSnapshot {
    fn from_watermark<K>(wm: Watermark<K>) -> Self {
        let head = match wm.head() {
            HeadStatus::Genesis => HeadSnapshot::Genesis,
            HeadStatus::Known(bytes) => HeadSnapshot::Known(Sha256(bytes)),
        };
        Self {
            seq: wm.seq(),
            head,
        }
    }
}

#[cfg(feature = "model-testing")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum BufferedPrevSnapshot {
    Contiguous {
        prev: Option<Sha256>,
    },
    Deferred {
        prev: Sha256,
        expected_prev_seq: Seq1,
    },
}

#[cfg(feature = "model-testing")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BufferedEventSnapshot {
    pub seq: Seq1,
    pub sha256: Sha256,
    pub prev: BufferedPrevSnapshot,
    pub bytes_len: usize,
}

#[cfg(feature = "model-testing")]
impl BufferedEventSnapshot {
    fn from_event(seq: Seq1, event: &VerifiedEventAny) -> Self {
        let (sha256, prev) = match event {
            VerifiedEventAny::Contiguous(ev) => (
                ev.sha256,
                BufferedPrevSnapshot::Contiguous { prev: ev.prev.prev },
            ),
            VerifiedEventAny::Deferred(ev) => (
                ev.sha256,
                BufferedPrevSnapshot::Deferred {
                    prev: ev.prev.prev,
                    expected_prev_seq: ev.prev.expected_prev_seq,
                },
            ),
        };
        Self {
            seq,
            sha256,
            prev,
            bytes_len: event.bytes_len(),
        }
    }
}

#[cfg(feature = "model-testing")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GapBufferSnapshot {
    pub buffered: Vec<BufferedEventSnapshot>,
    pub buffered_bytes: usize,
    pub started_at_ms: Option<u64>,
    pub max_events: usize,
    pub max_bytes: usize,
    pub timeout_ms: u64,
}

#[cfg(feature = "model-testing")]
impl GapBuffer {
    pub fn model_snapshot(&self) -> GapBufferSnapshot {
        let buffered = self
            .buffered
            .iter()
            .map(|(seq, ev)| BufferedEventSnapshot::from_event(*seq, ev))
            .collect();
        GapBufferSnapshot {
            buffered,
            buffered_bytes: self.buffered_bytes,
            started_at_ms: self.started_at_ms,
            max_events: self.max_events,
            max_bytes: self.max_bytes,
            timeout_ms: self.timeout_ms,
        }
    }
}

#[cfg(feature = "model-testing")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OriginStreamSnapshot {
    pub namespace: NamespaceId,
    pub origin: ReplicaId,
    pub durable: WatermarkSnapshot,
    pub gap: GapBufferSnapshot,
}

#[cfg(feature = "model-testing")]
impl OriginStreamState {
    pub fn model_snapshot(&self) -> OriginStreamSnapshot {
        OriginStreamSnapshot {
            namespace: self.namespace.clone(),
            origin: self.origin,
            durable: WatermarkSnapshot::from_watermark(self.durable),
            gap: self.gap.model_snapshot(),
        }
    }
}

#[cfg(feature = "model-testing")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GapBufferByNsOriginSnapshot {
    pub origins: Vec<OriginStreamSnapshot>,
}

#[derive(Clone, Debug)]
pub struct OriginStreamState {
    pub namespace: NamespaceId,
    pub origin: ReplicaId,
    pub durable: Watermark<Durable>,
    pub gap: GapBuffer,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngestDecision {
    ForwardContiguousBatch(ContiguousBatch),
    BufferedNeedWant { want_from: Seq0 },
    DuplicateNoop,
    Reject { reason: ReplRejectReason },
    InvalidBatch(ContiguousBatchError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DrainError {
    PrevMismatch {
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: Seq1,
        expected: Option<Sha256>,
        got: Option<Sha256>,
        head_seq: u64,
    },
    InvalidBatch(ContiguousBatchError),
}

impl OriginStreamState {
    pub fn new(
        namespace: NamespaceId,
        origin: ReplicaId,
        durable: Watermark<Durable>,
        limits: &Limits,
    ) -> Self {
        Self {
            namespace,
            origin,
            durable,
            gap: GapBuffer::new(limits),
        }
    }

    pub fn expected_next(&self) -> Seq1 {
        self.durable.seq().next()
    }

    pub fn ingest_one(&mut self, ev: VerifiedEventAny, now_ms: u64) -> IngestDecision {
        let seq = ev.seq();
        if seq.get() <= self.durable.seq().get() {
            return IngestDecision::DuplicateNoop;
        }

        if self.gap.buffered.contains_key(&seq) {
            return IngestDecision::DuplicateNoop;
        }

        if ev.is_deferred() {
            return self.buffer_gap(ev, now_ms);
        }

        if seq.get() != self.durable.seq().get() + 1 {
            return self.buffer_gap(ev, now_ms);
        }

        let VerifiedEventAny::Contiguous(ev) = ev else {
            return IngestDecision::Reject {
                reason: ReplRejectReason::PrevUnknown,
            };
        };

        let mut batch = vec![ev];
        let mut next = seq.get() + 1;
        while let Some(nz) = std::num::NonZeroU64::new(next) {
            let s = Seq1::new(nz);
            let Some(buffered) = self.gap.buffered.remove(&s) else {
                break;
            };
            let bytes_len = buffered.bytes_len();
            self.gap.buffered_bytes = self.gap.buffered_bytes.saturating_sub(bytes_len);
            match buffered {
                VerifiedEventAny::Contiguous(ev2) => {
                    batch.push(ev2);
                    next += 1;
                }
                other => {
                    self.gap.buffered_bytes += bytes_len;
                    self.gap.buffered.insert(s, other);
                    break;
                }
            }
        }

        if self.gap.buffered.is_empty() {
            self.gap.started_at_ms = None;
        }

        match ContiguousBatch::try_new(batch) {
            Ok(batch) => IngestDecision::ForwardContiguousBatch(batch),
            Err(err) => IngestDecision::InvalidBatch(err),
        }
    }

    pub fn advance_durable_batch(&mut self, batch: &ContiguousBatch) -> Result<(), WatermarkError> {
        for event in batch.events() {
            let expected = self.durable.seq().next();
            if event.seq() != expected {
                return Err(WatermarkError::NonContiguous {
                    expected,
                    got: event.seq(),
                });
            }
            self.durable = Watermark::new(
                Seq0::new(event.seq().get()),
                HeadStatus::Known(event.sha256.0),
            )?;
        }
        Ok(())
    }

    pub fn drain_ready(&mut self) -> Result<Option<ContiguousBatch>, DrainError> {
        let mut head = match self.durable.head() {
            HeadStatus::Genesis => None,
            HeadStatus::Known(head) => Some(Sha256(head)),
        };

        let mut batch = Vec::new();
        let mut next = self.durable.seq().next();

        while let Some(buffered) = self.gap.buffered.remove(&next) {
            let bytes_len = buffered.bytes_len();
            self.gap.buffered_bytes = self.gap.buffered_bytes.saturating_sub(bytes_len);
            let verified = match buffered {
                VerifiedEventAny::Contiguous(ev) => {
                    if head != ev.prev.prev {
                        return Err(DrainError::PrevMismatch {
                            namespace: self.namespace.clone(),
                            origin: self.origin,
                            seq: next,
                            expected: head,
                            got: ev.prev.prev,
                            head_seq: next.get().saturating_sub(1),
                        });
                    }
                    ev
                }
                VerifiedEventAny::Deferred(ev) => {
                    if head != Some(ev.prev.prev) {
                        return Err(DrainError::PrevMismatch {
                            namespace: self.namespace.clone(),
                            origin: self.origin,
                            seq: next,
                            expected: head,
                            got: Some(ev.prev.prev),
                            head_seq: next.get().saturating_sub(1),
                        });
                    }
                    VerifiedEvent {
                        body: ev.body,
                        bytes: ev.bytes,
                        sha256: ev.sha256,
                        prev: PrevVerified { prev: head },
                    }
                }
            };

            head = Some(verified.sha256);
            batch.push(verified);
            next = next.next();
        }

        if self.gap.buffered.is_empty() {
            self.gap.started_at_ms = None;
        }

        if batch.is_empty() {
            return Ok(None);
        }

        ContiguousBatch::try_new(batch)
            .map(Some)
            .map_err(DrainError::InvalidBatch)
    }

    fn buffer_gap(&mut self, ev: VerifiedEventAny, now_ms: u64) -> IngestDecision {
        if let Some(start) = self.gap.started_at_ms {
            if now_ms.saturating_sub(start) > self.gap.timeout_ms {
                return IngestDecision::Reject {
                    reason: ReplRejectReason::GapTimeout,
                };
            }
        } else {
            self.gap.started_at_ms = Some(now_ms);
        }

        if self.gap.buffered.len() >= self.gap.max_events {
            return IngestDecision::Reject {
                reason: ReplRejectReason::GapBufferOverflow,
            };
        }

        let bytes = ev.bytes_len();
        if self.gap.buffered_bytes + bytes > self.gap.max_bytes {
            return IngestDecision::Reject {
                reason: ReplRejectReason::GapBufferBytesOverflow,
            };
        }

        self.gap.buffered_bytes += bytes;
        self.gap.buffered.insert(ev.seq(), ev);
        IngestDecision::BufferedNeedWant {
            want_from: self.durable.seq(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct GapBufferByNsOrigin {
    limits: Limits,
    by_ns: BTreeMap<NamespaceId, BTreeMap<ReplicaId, OriginStreamState>>,
}

impl GapBufferByNsOrigin {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            by_ns: BTreeMap::new(),
        }
    }

    pub fn ingest(
        &mut self,
        namespace: NamespaceId,
        origin: ReplicaId,
        durable: Watermark<Durable>,
        event: VerifiedEventAny,
        now_ms: u64,
    ) -> IngestDecision {
        let state = self.ensure_origin(namespace, origin, durable);
        state.ingest_one(event, now_ms)
    }

    pub fn advance_durable_batch(&mut self, batch: &ContiguousBatch) -> Result<(), WatermarkError> {
        let namespace = batch.namespace();
        let origin = batch.origin();
        let Some(origins) = self.by_ns.get_mut(namespace) else {
            return Ok(());
        };
        let Some(state) = origins.get_mut(&origin) else {
            return Ok(());
        };
        state.advance_durable_batch(batch)
    }

    pub fn drain_ready(
        &mut self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
    ) -> Result<Option<ContiguousBatch>, DrainError> {
        let Some(origins) = self.by_ns.get_mut(namespace) else {
            return Ok(None);
        };
        let Some(state) = origins.get_mut(origin) else {
            return Ok(None);
        };
        state.drain_ready()
    }

    pub fn want_from(&self, namespace: &NamespaceId, origin: &ReplicaId) -> Option<Seq0> {
        let origins = self.by_ns.get(namespace)?;
        let state = origins.get(origin)?;
        if state.gap.buffered.is_empty() {
            None
        } else {
            Some(state.durable.seq())
        }
    }

    fn ensure_origin(
        &mut self,
        namespace: NamespaceId,
        origin: ReplicaId,
        durable: Watermark<Durable>,
    ) -> &mut OriginStreamState {
        let origins = self.by_ns.entry(namespace.clone()).or_default();
        let state = origins.entry(origin).or_insert_with(|| {
            OriginStreamState::new(namespace.clone(), origin, durable, &self.limits)
        });
        if durable.seq().get() > state.durable.seq().get() {
            state.durable = durable;
        }
        state
    }
}

#[cfg(feature = "model-testing")]
impl GapBufferByNsOrigin {
    pub fn model_snapshot(&self) -> GapBufferByNsOriginSnapshot {
        let mut origins = Vec::new();
        for origins_map in self.by_ns.values() {
            for state in origins_map.values() {
                origins.push(state.model_snapshot());
            }
        }
        GapBufferByNsOriginSnapshot { origins }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    use crate::core::{
        ActorId, EventBody, EventBytes, EventKindV1, HlcMax, Limits, NamespaceId, ReplicaId, Seq1,
        StoreEpoch, StoreId, StoreIdentity, TxnDeltaV1, TxnId, TxnV1, ValidatedEventBody,
        encode_event_body_canonical,
    };

    fn sample_body(seq: u64) -> ValidatedEventBody {
        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::new(2));
        let origin = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let txn_id = TxnId::new(Uuid::from_bytes([3u8; 16]));

        EventBody {
            envelope_v: 1,
            store,
            namespace: NamespaceId::core(),
            origin_replica_id: origin,
            origin_seq: Seq1::from_u64(seq).unwrap(),
            event_time_ms: 123,
            txn_id,
            client_request_id: None,
            trace_id: None,
            kind: EventKindV1::TxnV1(TxnV1 {
                delta: TxnDeltaV1::new(),
                hlc_max: HlcMax {
                    actor_id: ActorId::new("alice").unwrap(),
                    physical_ms: 123,
                    logical: 0,
                },
            }),
        }
        .into_validated(&Limits::default())
        .expect("valid event fixture")
    }

    fn event_bytes(body: &ValidatedEventBody) -> EventBytes<crate::core::Canonical> {
        encode_event_body_canonical(body.as_ref()).expect("encode body")
    }

    fn contiguous_event(seq: u64) -> VerifiedEventAny {
        let body = sample_body(seq);
        let bytes = event_bytes(&body);
        VerifiedEventAny::Contiguous(VerifiedEvent {
            body,
            bytes,
            sha256: crate::core::Sha256([seq as u8; 32]),
            prev: crate::core::PrevVerified { prev: None },
        })
    }

    fn deferred_event(seq: u64) -> VerifiedEventAny {
        let body = sample_body(seq);
        let bytes = event_bytes(&body);
        VerifiedEventAny::Deferred(VerifiedEvent {
            body,
            bytes,
            sha256: crate::core::Sha256([seq as u8; 32]),
            prev: crate::core::PrevDeferred {
                prev: crate::core::Sha256([0u8; 32]),
                expected_prev_seq: Seq1::from_u64(seq.saturating_sub(1)).unwrap(),
            },
        })
    }

    fn state_with_limits(limits: &Limits) -> OriginStreamState {
        OriginStreamState::new(
            NamespaceId::core(),
            ReplicaId::new(Uuid::from_bytes([2u8; 16])),
            Watermark::<Durable>::genesis(),
            limits,
        )
    }

    #[test]
    fn single_gap_buffers_then_drains() {
        let limits = Limits::default();
        let mut state = state_with_limits(&limits);

        let decision = state.ingest_one(contiguous_event(2), 100);
        assert!(matches!(decision, IngestDecision::BufferedNeedWant { .. }));
        assert_eq!(state.gap.buffered.len(), 1);

        let decision = state.ingest_one(contiguous_event(1), 101);
        let IngestDecision::ForwardContiguousBatch(batch) = decision else {
            panic!("expected contiguous batch");
        };
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.events()[0].seq().get(), 1);
        assert_eq!(batch.events()[1].seq().get(), 2);
        assert!(state.gap.buffered.is_empty());
    }

    #[test]
    fn multiple_gaps_drain_in_order() {
        let limits = Limits::default();
        let mut state = state_with_limits(&limits);

        let _ = state.ingest_one(contiguous_event(3), 100);
        let _ = state.ingest_one(contiguous_event(5), 100);

        let IngestDecision::ForwardContiguousBatch(batch) =
            state.ingest_one(contiguous_event(1), 101)
        else {
            panic!("expected contiguous batch");
        };
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.events()[0].seq().get(), 1);
        state.advance_durable_batch(&batch).unwrap();

        let IngestDecision::ForwardContiguousBatch(batch) =
            state.ingest_one(contiguous_event(2), 102)
        else {
            panic!("expected contiguous batch");
        };
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.events()[0].seq().get(), 2);
        assert_eq!(batch.events()[1].seq().get(), 3);
        assert_eq!(state.gap.buffered.len(), 1);
    }

    #[test]
    fn deferred_event_blocks_drain() {
        let limits = Limits::default();
        let mut state = state_with_limits(&limits);

        let decision = state.ingest_one(deferred_event(2), 100);
        assert!(matches!(decision, IngestDecision::BufferedNeedWant { .. }));

        let decision = state.ingest_one(contiguous_event(1), 101);
        let IngestDecision::ForwardContiguousBatch(batch) = decision else {
            panic!("expected contiguous batch");
        };
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.events()[0].seq().get(), 1);
        assert_eq!(state.gap.buffered.len(), 1);
    }

    #[test]
    fn duplicate_events_are_noops() {
        let limits = Limits::default();
        let mut state = state_with_limits(&limits);
        let _ = state.ingest_one(contiguous_event(2), 100);
        let decision = state.ingest_one(contiguous_event(2), 100);
        assert!(matches!(decision, IngestDecision::DuplicateNoop));
        assert_eq!(state.gap.buffered.len(), 1);
    }

    #[test]
    fn gap_overflow_rejects() {
        let limits = Limits {
            max_repl_gap_events: 1,
            ..Default::default()
        };
        let mut state = state_with_limits(&limits);

        let _ = state.ingest_one(contiguous_event(2), 100);
        let decision = state.ingest_one(contiguous_event(3), 100);
        assert!(matches!(
            decision,
            IngestDecision::Reject {
                reason: ReplRejectReason::GapBufferOverflow
            }
        ));
    }

    #[test]
    fn gap_timeout_rejects() {
        let limits = Limits {
            repl_gap_timeout_ms: 10,
            ..Default::default()
        };
        let mut state = state_with_limits(&limits);

        let _ = state.ingest_one(contiguous_event(2), 100);
        let decision = state.ingest_one(contiguous_event(3), 120);
        assert!(matches!(
            decision,
            IngestDecision::Reject {
                reason: ReplRejectReason::GapTimeout
            }
        ));
    }

    #[test]
    fn gap_bytes_overflow_rejects() {
        let limits = Limits {
            max_repl_gap_bytes: 4,
            ..Default::default()
        };
        let mut state = state_with_limits(&limits);

        let decision = state.ingest_one(contiguous_event(2), 100);
        assert!(matches!(
            decision,
            IngestDecision::Reject {
                reason: ReplRejectReason::GapBufferBytesOverflow
            }
        ));
    }
}
