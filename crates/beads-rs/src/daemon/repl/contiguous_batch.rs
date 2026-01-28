//! Contiguous replication batch with verified ordering invariants.

use crate::core::{NamespaceId, PrevVerified, ReplicaId, Seq1, Sha256, VerifiedEvent};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContiguousBatch {
    namespace: NamespaceId,
    origin: ReplicaId,
    first: Seq1,
    last: Seq1,
    events: Vec<VerifiedEvent<PrevVerified>>,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ContiguousBatchError {
    #[error("batch is empty")]
    Empty,
    #[error("batch has mixed namespace (expected {expected}, got {got})")]
    MixedNamespace {
        expected: NamespaceId,
        got: NamespaceId,
    },
    #[error("batch has mixed origin (expected {expected}, got {got})")]
    MixedOrigin { expected: ReplicaId, got: ReplicaId },
    #[error("batch has non-contiguous sequence (expected {expected}, got {got})")]
    NonContiguousSeq { expected: Seq1, got: Seq1 },
    #[error("batch has prev sha mismatch at seq {seq} (expected {expected:?}, got {got:?})")]
    PrevMismatch {
        seq: Seq1,
        expected: Option<Sha256>,
        got: Option<Sha256>,
    },
}

impl ContiguousBatch {
    pub fn try_new(events: Vec<VerifiedEvent<PrevVerified>>) -> Result<Self, ContiguousBatchError> {
        let first_event = events.first().ok_or(ContiguousBatchError::Empty)?;
        let namespace = first_event.body.namespace.clone();
        let origin = first_event.body.origin_replica_id;
        let first = first_event.seq();
        let mut last = first;
        let mut prev_sha = first_event.sha256;

        for (idx, event) in events.iter().enumerate() {
            if event.body.namespace != namespace {
                return Err(ContiguousBatchError::MixedNamespace {
                    expected: namespace,
                    got: event.body.namespace.clone(),
                });
            }
            if event.body.origin_replica_id != origin {
                return Err(ContiguousBatchError::MixedOrigin {
                    expected: origin,
                    got: event.body.origin_replica_id,
                });
            }
            if idx == 0 {
                continue;
            }
            let expected_seq = last.next();
            if event.seq() != expected_seq {
                return Err(ContiguousBatchError::NonContiguousSeq {
                    expected: expected_seq,
                    got: event.seq(),
                });
            }
            let expected_prev = Some(prev_sha);
            if event.prev.prev != expected_prev {
                return Err(ContiguousBatchError::PrevMismatch {
                    seq: event.seq(),
                    expected: expected_prev,
                    got: event.prev.prev,
                });
            }
            last = event.seq();
            prev_sha = event.sha256;
        }

        Ok(Self {
            namespace,
            origin,
            first,
            last,
            events,
        })
    }

    pub fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    pub fn origin(&self) -> ReplicaId {
        self.origin
    }

    pub fn first(&self) -> Seq1 {
        self.first
    }

    pub fn last(&self) -> Seq1 {
        self.last
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn events(&self) -> &[VerifiedEvent<PrevVerified>] {
        &self.events
    }

    pub fn first_event(&self) -> &VerifiedEvent<PrevVerified> {
        &self.events[0]
    }

    pub fn last_event(&self) -> &VerifiedEvent<PrevVerified> {
        self.events
            .last()
            .expect("contiguous batch always non-empty")
    }

    pub fn into_events(self) -> Vec<VerifiedEvent<PrevVerified>> {
        self.events
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

    fn sample_body(namespace: NamespaceId, origin: ReplicaId, seq: u64) -> ValidatedEventBody {
        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::new(2));
        let txn_id = TxnId::new(Uuid::from_bytes([3u8; 16]));

        EventBody {
            envelope_v: 1,
            store,
            namespace,
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

    fn verified_event(
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: u64,
        prev: Option<Sha256>,
    ) -> VerifiedEvent<PrevVerified> {
        let body = sample_body(namespace, origin, seq);
        let bytes = event_bytes(&body);
        VerifiedEvent {
            body,
            bytes,
            sha256: Sha256([seq as u8; 32]),
            prev: PrevVerified { prev },
        }
    }

    #[test]
    fn contiguous_batch_accepts_valid_sequence() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let first = verified_event(namespace.clone(), origin, 1, None);
        let second = verified_event(namespace.clone(), origin, 2, Some(first.sha256));
        let batch = ContiguousBatch::try_new(vec![first.clone(), second.clone()]).expect("batch");
        assert_eq!(batch.namespace(), &namespace);
        assert_eq!(batch.origin(), origin);
        assert_eq!(batch.first(), first.seq());
        assert_eq!(batch.last(), second.seq());
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.last_event().seq(), second.seq());
    }

    #[test]
    fn contiguous_batch_rejects_gap() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let first = verified_event(namespace.clone(), origin, 1, None);
        let third = verified_event(namespace, origin, 3, Some(first.sha256));
        let err = ContiguousBatch::try_new(vec![first, third]).expect_err("gap");
        assert!(matches!(err, ContiguousBatchError::NonContiguousSeq { .. }));
    }

    #[test]
    fn contiguous_batch_rejects_duplicate() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let first = verified_event(namespace.clone(), origin, 1, None);
        let dup = verified_event(namespace, origin, 1, None);
        let err = ContiguousBatch::try_new(vec![first, dup]).expect_err("dup");
        assert!(matches!(err, ContiguousBatchError::NonContiguousSeq { .. }));
    }

    #[test]
    fn contiguous_batch_rejects_mixed_origin() {
        let namespace = NamespaceId::core();
        let origin_a = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let origin_b = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
        let first = verified_event(namespace.clone(), origin_a, 1, None);
        let second = verified_event(namespace, origin_b, 2, Some(first.sha256));
        let err = ContiguousBatch::try_new(vec![first, second]).expect_err("origin");
        assert!(matches!(err, ContiguousBatchError::MixedOrigin { .. }));
    }
}
