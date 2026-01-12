//! Durability classes and proofs for mutation receipts.

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::watermark::{Applied, Durable, WatermarkError, Watermarks};
use super::{EventId, ReplicaId, StoreIdentity, TxnId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurabilityClass {
    LocalFsync,
    ReplicatedFsync { k: NonZeroU32 },
}

impl fmt::Display for DurabilityClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DurabilityClass::LocalFsync => write!(f, "local_fsync"),
            DurabilityClass::ReplicatedFsync { k } => write!(f, "replicated_fsync({})", k),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFsyncProof {
    pub at_ms: u64,
    pub durable_seq: Watermarks<Durable>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedProof {
    pub k: NonZeroU32,
    pub acked_by: Vec<ReplicaId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityProofV1 {
    pub local_fsync: LocalFsyncProof,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicated: Option<ReplicatedProof>,
}

impl DurabilityProofV1 {
    pub fn merge(&self, other: &Self) -> Result<Self, WatermarkError> {
        let mut durable_seq = self.local_fsync.durable_seq.clone();
        durable_seq.merge_at_least(&other.local_fsync.durable_seq)?;
        let local_fsync = LocalFsyncProof {
            at_ms: self.local_fsync.at_ms.max(other.local_fsync.at_ms),
            durable_seq,
        };

        let replicated = match (&self.replicated, &other.replicated) {
            (None, None) => None,
            (Some(rep), None) | (None, Some(rep)) => Some(rep.clone()),
            (Some(a), Some(b)) => {
                let k = std::cmp::max(a.k, b.k);
                let mut acked_by: BTreeSet<ReplicaId> = a.acked_by.iter().cloned().collect();
                acked_by.extend(b.acked_by.iter().cloned());
                Some(ReplicatedProof {
                    k,
                    acked_by: acked_by.into_iter().collect(),
                })
            }
        };

        Ok(Self {
            local_fsync,
            replicated,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurabilityOutcome {
    Achieved {
        requested: DurabilityClass,
        achieved: DurabilityClass,
    },
    Pending {
        requested: DurabilityClass,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityReceipt {
    pub store: StoreIdentity,
    pub txn_id: TxnId,
    pub event_ids: Vec<EventId>,
    pub durability_proof: DurabilityProofV1,
    pub outcome: DurabilityOutcome,
    pub min_seen: Watermarks<Applied>,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ReceiptMergeError {
    #[error("store identity mismatch: expected {expected:?}, got {actual:?}")]
    StoreIdentityMismatch {
        expected: StoreIdentity,
        actual: StoreIdentity,
    },

    #[error("txn id mismatch: expected {expected}, got {actual}")]
    TxnIdMismatch { expected: TxnId, actual: TxnId },

    #[error("event ids mismatch")]
    EventIdsMismatch {
        expected: Vec<EventId>,
        actual: Vec<EventId>,
    },

    #[error("requested durability mismatch: expected {expected:?}, got {actual:?}")]
    RequestedMismatch {
        expected: DurabilityClass,
        actual: DurabilityClass,
    },

    #[error(transparent)]
    Watermark(#[from] WatermarkError),
}

impl DurabilityReceipt {
    pub fn local_fsync(
        store: StoreIdentity,
        txn_id: TxnId,
        event_ids: Vec<EventId>,
        at_ms: u64,
        durable_seq: Watermarks<Durable>,
        min_seen: Watermarks<Applied>,
    ) -> Self {
        let local_fsync = LocalFsyncProof { at_ms, durable_seq };
        let durability_proof = DurabilityProofV1 {
            local_fsync,
            replicated: None,
        };
        let outcome = DurabilityOutcome::Achieved {
            requested: DurabilityClass::LocalFsync,
            achieved: DurabilityClass::LocalFsync,
        };

        Self {
            store,
            txn_id,
            event_ids,
            durability_proof,
            outcome,
            min_seen,
        }
    }

    pub fn local_fsync_defaults(
        store: StoreIdentity,
        txn_id: TxnId,
        event_ids: Vec<EventId>,
        at_ms: u64,
    ) -> Self {
        Self::local_fsync(
            store,
            txn_id,
            event_ids,
            at_ms,
            Watermarks::new(),
            Watermarks::new(),
        )
    }

    pub fn merge(&self, other: &Self) -> Result<Self, ReceiptMergeError> {
        if self.store != other.store {
            return Err(ReceiptMergeError::StoreIdentityMismatch {
                expected: self.store,
                actual: other.store,
            });
        }
        if self.txn_id != other.txn_id {
            return Err(ReceiptMergeError::TxnIdMismatch {
                expected: self.txn_id,
                actual: other.txn_id,
            });
        }
        if self.event_ids != other.event_ids {
            return Err(ReceiptMergeError::EventIdsMismatch {
                expected: self.event_ids.clone(),
                actual: other.event_ids.clone(),
            });
        }

        let durability_proof = self.durability_proof.merge(&other.durability_proof)?;
        let outcome = merge_outcome(&self.outcome, &other.outcome)?;

        let mut min_seen = self.min_seen.clone();
        min_seen.merge_at_least(&other.min_seen)?;

        Ok(Self {
            store: self.store,
            txn_id: self.txn_id,
            event_ids: self.event_ids.clone(),
            durability_proof,
            outcome,
            min_seen,
        })
    }
}

fn merge_outcome(
    left: &DurabilityOutcome,
    right: &DurabilityOutcome,
) -> Result<DurabilityOutcome, ReceiptMergeError> {
    let requested_left = requested_durability(left);
    let requested_right = requested_durability(right);
    if requested_left != requested_right {
        return Err(ReceiptMergeError::RequestedMismatch {
            expected: requested_left,
            actual: requested_right,
        });
    }

    let requested = requested_left;
    let outcome = match (left, right) {
        (
            DurabilityOutcome::Achieved { achieved: a, .. },
            DurabilityOutcome::Achieved { achieved: b, .. },
        ) => DurabilityOutcome::Achieved {
            requested,
            achieved: max_durability_class(*a, *b),
        },
        (DurabilityOutcome::Achieved { achieved, .. }, _)
        | (_, DurabilityOutcome::Achieved { achieved, .. }) => DurabilityOutcome::Achieved {
            requested,
            achieved: *achieved,
        },
        (DurabilityOutcome::Pending { .. }, DurabilityOutcome::Pending { .. }) => {
            DurabilityOutcome::Pending { requested }
        }
    };

    Ok(outcome)
}

fn requested_durability(outcome: &DurabilityOutcome) -> DurabilityClass {
    match outcome {
        DurabilityOutcome::Achieved { requested, .. } => *requested,
        DurabilityOutcome::Pending { requested } => *requested,
    }
}

fn max_durability_class(a: DurabilityClass, b: DurabilityClass) -> DurabilityClass {
    if durability_strength(a) >= durability_strength(b) {
        a
    } else {
        b
    }
}

fn durability_strength(class: DurabilityClass) -> (u8, u32) {
    match class {
        DurabilityClass::LocalFsync => (0, 0),
        DurabilityClass::ReplicatedFsync { k } => (1, k.get()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{HeadStatus, NamespaceId, Seq0, Seq1, StoreEpoch, StoreId};
    use uuid::Uuid;

    #[test]
    fn durability_proof_serde_roundtrip() {
        let ns = NamespaceId::parse("core").unwrap();
        let origin = ReplicaId::new(Uuid::from_bytes([1u8; 16]));

        let mut watermarks = Watermarks::<Durable>::new();
        watermarks
            .advance_contiguous(&ns, &origin, Seq1::from_u64(1).unwrap(), [2u8; 32])
            .unwrap();

        let proof = DurabilityProofV1 {
            local_fsync: LocalFsyncProof {
                at_ms: 1_726_000_000_000,
                durable_seq: watermarks,
            },
            replicated: Some(ReplicatedProof {
                k: NonZeroU32::new(2).unwrap(),
                acked_by: vec![origin],
            }),
        };

        let json = serde_json::to_string(&proof).unwrap();
        let parsed: DurabilityProofV1 = serde_json::from_str(&json).unwrap();
        assert_eq!(proof, parsed);
    }

    #[test]
    fn durability_outcome_serde_roundtrip() {
        let outcome = DurabilityOutcome::Pending {
            requested: DurabilityClass::LocalFsync,
        };
        let json = serde_json::to_string(&outcome).unwrap();
        let parsed: DurabilityOutcome = serde_json::from_str(&json).unwrap();
        assert_eq!(outcome, parsed);
    }

    #[test]
    fn durability_receipt_serde_roundtrip() {
        let ns = NamespaceId::parse("core").unwrap();
        let origin = ReplicaId::new(Uuid::from_bytes([3u8; 16]));
        let event_id = EventId::new(origin, ns.clone(), Seq1::from_u64(1).unwrap());
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([4u8; 16])), StoreEpoch::ZERO);
        let txn_id = TxnId::new(Uuid::from_bytes([5u8; 16]));

        let mut min_seen = Watermarks::<Applied>::new();
        min_seen
            .observe_at_least(&ns, &origin, Seq0::new(1), HeadStatus::Known([7u8; 32]))
            .unwrap();

        let receipt = DurabilityReceipt {
            store,
            txn_id,
            event_ids: vec![event_id],
            durability_proof: DurabilityProofV1 {
                local_fsync: LocalFsyncProof {
                    at_ms: 1_726_000_000_000,
                    durable_seq: Watermarks::<Durable>::new(),
                },
                replicated: None,
            },
            outcome: DurabilityOutcome::Achieved {
                requested: DurabilityClass::LocalFsync,
                achieved: DurabilityClass::LocalFsync,
            },
            min_seen,
        };

        let json = serde_json::to_string(&receipt).unwrap();
        let parsed: DurabilityReceipt = serde_json::from_str(&json).unwrap();
        assert_eq!(receipt, parsed);
    }

    #[test]
    fn durability_receipt_merge_upgrades() {
        let ns = NamespaceId::parse("core").unwrap();
        let origin = ReplicaId::new(Uuid::from_bytes([8u8; 16]));
        let other_replica = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
        let event_id = EventId::new(origin, ns.clone(), Seq1::from_u64(1).unwrap());
        let store =
            StoreIdentity::new(StoreId::new(Uuid::from_bytes([10u8; 16])), StoreEpoch::ZERO);
        let txn_id = TxnId::new(Uuid::from_bytes([11u8; 16]));

        let pending = DurabilityReceipt {
            store,
            txn_id,
            event_ids: vec![event_id.clone()],
            durability_proof: DurabilityProofV1 {
                local_fsync: LocalFsyncProof {
                    at_ms: 10,
                    durable_seq: Watermarks::<Durable>::new(),
                },
                replicated: Some(ReplicatedProof {
                    k: NonZeroU32::new(2).unwrap(),
                    acked_by: vec![origin],
                }),
            },
            outcome: DurabilityOutcome::Pending {
                requested: DurabilityClass::ReplicatedFsync {
                    k: NonZeroU32::new(2).unwrap(),
                },
            },
            min_seen: Watermarks::<Applied>::new(),
        };

        let mut min_seen = Watermarks::<Applied>::new();
        min_seen
            .observe_at_least(&ns, &origin, Seq0::new(1), HeadStatus::Known([1u8; 32]))
            .unwrap();

        let achieved = DurabilityReceipt {
            store,
            txn_id,
            event_ids: vec![event_id],
            durability_proof: DurabilityProofV1 {
                local_fsync: LocalFsyncProof {
                    at_ms: 20,
                    durable_seq: Watermarks::<Durable>::new(),
                },
                replicated: Some(ReplicatedProof {
                    k: NonZeroU32::new(2).unwrap(),
                    acked_by: vec![origin, other_replica],
                }),
            },
            outcome: DurabilityOutcome::Achieved {
                requested: DurabilityClass::ReplicatedFsync {
                    k: NonZeroU32::new(2).unwrap(),
                },
                achieved: DurabilityClass::ReplicatedFsync {
                    k: NonZeroU32::new(2).unwrap(),
                },
            },
            min_seen,
        };

        let merged = pending.merge(&achieved).unwrap();
        assert!(matches!(merged.outcome, DurabilityOutcome::Achieved { .. }));
        let replicated = merged
            .durability_proof
            .replicated
            .expect("replicated proof");
        assert_eq!(replicated.k.get(), 2);
        assert!(replicated.acked_by.contains(&origin));
        assert!(replicated.acked_by.contains(&other_replica));

        let watermark = merged.min_seen.get(&ns, &origin).expect("min_seen");
        assert_eq!(watermark.seq().get(), 1);
    }
}
