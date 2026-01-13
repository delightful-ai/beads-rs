//! Replication gap buffering and contiguity enforcement.

use std::collections::BTreeMap;

use crate::core::{
    Durable, HeadStatus, Limits, NamespaceId, ReplicaId, Seq0, Seq1, Watermark, WatermarkError,
    VerifiedEvent, VerifiedEventAny,
};

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

#[derive(Clone, Debug)]
pub struct OriginStreamState {
    pub namespace: NamespaceId,
    pub origin: ReplicaId,
    pub durable: Watermark<Durable>,
    pub gap: GapBuffer,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngestDecision {
    ForwardContiguousBatch(Vec<VerifiedEvent<crate::core::PrevVerified>>),
    BufferedNeedWant { want_from: Seq0 },
    DuplicateNoop,
    Reject { code: String },
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
                code: "prev_unknown".to_string(),
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

        IngestDecision::ForwardContiguousBatch(batch)
    }

    pub fn advance_durable_batch(
        &mut self,
        batch: &[VerifiedEvent<crate::core::PrevVerified>],
    ) -> Result<(), WatermarkError> {
        for event in batch {
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

    fn buffer_gap(&mut self, ev: VerifiedEventAny, now_ms: u64) -> IngestDecision {
        if let Some(start) = self.gap.started_at_ms {
            if now_ms.saturating_sub(start) > self.gap.timeout_ms {
                return IngestDecision::Reject {
                    code: "gap_timeout".to_string(),
                };
            }
        } else {
            self.gap.started_at_ms = Some(now_ms);
        }

        if self.gap.buffered.len() >= self.gap.max_events {
            return IngestDecision::Reject {
                code: "gap_buffer_overflow".to_string(),
            };
        }

        let bytes = ev.bytes_len();
        if self.gap.buffered_bytes + bytes > self.gap.max_bytes {
            return IngestDecision::Reject {
                code: "gap_buffer_bytes_overflow".to_string(),
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

    pub fn advance_durable_batch(
        &mut self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        batch: &[VerifiedEvent<crate::core::PrevVerified>],
    ) -> Result<(), WatermarkError> {
        let Some(origins) = self.by_ns.get_mut(namespace) else {
            return Ok(());
        };
        let Some(state) = origins.get_mut(origin) else {
            return Ok(());
        };
        state.advance_durable_batch(batch)
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use uuid::Uuid;

    use crate::core::{
        ActorId, EventBody, EventBytes, EventKindV1, HlcMax, NamespaceId, ReplicaId, Seq1,
        StoreEpoch, StoreId, StoreIdentity, TxnDeltaV1, TxnId,
    };

    fn sample_body(seq: u64) -> EventBody {
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
            kind: EventKindV1::TxnV1,
            delta: TxnDeltaV1::new(),
            hlc_max: Some(HlcMax {
                actor_id: ActorId::new("alice").unwrap(),
                physical_ms: 123,
                logical: 0,
            }),
        }
    }

    fn event_bytes(len: usize) -> EventBytes<crate::core::Opaque> {
        let payload = vec![1u8; len.max(1)];
        EventBytes::new(Bytes::from(payload))
    }

    fn contiguous_event(seq: u64) -> VerifiedEventAny {
        VerifiedEventAny::Contiguous(VerifiedEvent {
            body: sample_body(seq),
            bytes: event_bytes(8),
            sha256: crate::core::Sha256([seq as u8; 32]),
            prev: crate::core::PrevVerified { prev: None },
        })
    }

    fn deferred_event(seq: u64) -> VerifiedEventAny {
        VerifiedEventAny::Deferred(VerifiedEvent {
            body: sample_body(seq),
            bytes: event_bytes(8),
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
        assert_eq!(batch[0].seq().get(), 1);
        assert_eq!(batch[1].seq().get(), 2);
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
        assert_eq!(batch[0].seq().get(), 1);
        state.advance_durable_batch(&batch).unwrap();

        let IngestDecision::ForwardContiguousBatch(batch) =
            state.ingest_one(contiguous_event(2), 102)
        else {
            panic!("expected contiguous batch");
        };
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].seq().get(), 2);
        assert_eq!(batch[1].seq().get(), 3);
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
        assert_eq!(batch[0].seq().get(), 1);
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
        let mut limits = Limits::default();
        limits.max_repl_gap_events = 1;
        let mut state = state_with_limits(&limits);

        let _ = state.ingest_one(contiguous_event(2), 100);
        let decision = state.ingest_one(contiguous_event(3), 100);
        assert!(matches!(
            decision,
            IngestDecision::Reject { code } if code == "gap_buffer_overflow"
        ));
    }

    #[test]
    fn gap_timeout_rejects() {
        let mut limits = Limits::default();
        limits.repl_gap_timeout_ms = 10;
        let mut state = state_with_limits(&limits);

        let _ = state.ingest_one(contiguous_event(2), 100);
        let decision = state.ingest_one(contiguous_event(3), 120);
        assert!(matches!(
            decision,
            IngestDecision::Reject { code } if code == "gap_timeout"
        ));
    }

    #[test]
    fn gap_bytes_overflow_rejects() {
        let mut limits = Limits::default();
        limits.max_repl_gap_bytes = 4;
        let mut state = state_with_limits(&limits);

        let decision = state.ingest_one(contiguous_event(2), 100);
        assert!(matches!(decision, IngestDecision::Reject { code } if code == "gap_buffer_bytes_overflow"));
    }
}
