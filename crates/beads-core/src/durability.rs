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

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum DurabilityParseError {
    #[error("durability cannot be empty")]
    Empty,

    #[error("unsupported durability class: {raw}")]
    Unsupported { raw: String },
}

impl DurabilityClass {
    pub fn parse(raw: &str) -> Result<Self, DurabilityParseError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(DurabilityParseError::Empty);
        }
        let value = trimmed.to_lowercase();
        if value == "local_fsync" || value == "local-fsync" {
            return Ok(DurabilityClass::LocalFsync);
        }
        if let Some(rest) = value.strip_prefix("replicated_fsync") {
            let rest = rest.trim();
            let rest = rest
                .strip_prefix('(')
                .and_then(|s| s.strip_suffix(')'))
                .or_else(|| rest.strip_prefix(':'))
                .or_else(|| rest.strip_prefix('='))
                .map(str::trim);
            if let Some(k_raw) = rest {
                let k = k_raw.parse::<u32>().ok().and_then(NonZeroU32::new);
                if let Some(k) = k {
                    return Ok(DurabilityClass::ReplicatedFsync { k });
                }
            }
        }

        Err(DurabilityParseError::Unsupported {
            raw: trimmed.to_string(),
        })
    }

    pub fn parse_optional(raw: Option<&str>) -> Result<Self, DurabilityParseError> {
        match raw {
            None => Ok(DurabilityClass::LocalFsync),
            Some(raw) => DurabilityClass::parse(raw),
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
    Achieved(AchievedOutcome),
    Pending(PendingOutcome),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AchievedOutcome {
    requested: DurabilityClass,
    achieved: DurabilityClass,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingOutcome {
    requested: DurabilityClass,
}

impl DurabilityOutcome {
    pub fn requested(&self) -> DurabilityClass {
        match self {
            DurabilityOutcome::Achieved(inner) => inner.requested,
            DurabilityOutcome::Pending(inner) => inner.requested,
        }
    }

    pub fn achieved(&self) -> Option<DurabilityClass> {
        match self {
            DurabilityOutcome::Achieved(inner) => Some(inner.achieved),
            DurabilityOutcome::Pending(_) => None,
        }
    }

    pub fn is_pending(&self) -> bool {
        matches!(self, DurabilityOutcome::Pending(_))
    }

    pub fn is_achieved(&self) -> bool {
        matches!(self, DurabilityOutcome::Achieved(_))
    }

    pub(crate) fn pending(requested: DurabilityClass) -> Self {
        DurabilityOutcome::Pending(PendingOutcome { requested })
    }

    pub(crate) fn build_achieved(requested: DurabilityClass, achieved: DurabilityClass) -> Self {
        DurabilityOutcome::Achieved(AchievedOutcome {
            requested,
            achieved,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "DurabilityReceiptWire", into = "DurabilityReceiptWire")]
pub struct DurabilityReceipt {
    store: StoreIdentity,
    txn_id: TxnId,
    event_ids: Vec<EventId>,
    durability_proof: DurabilityProofV1,
    outcome: DurabilityOutcome,
    min_seen: Watermarks<Applied>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DurabilityReceiptWire {
    store: StoreIdentity,
    txn_id: TxnId,
    event_ids: Vec<EventId>,
    durability_proof: DurabilityProofV1,
    outcome: DurabilityOutcome,
    min_seen: Watermarks<Applied>,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ReceiptBuildError {
    #[error("pending outcome requires replicated durability, got {requested:?}")]
    PendingRequiresReplicated { requested: DurabilityClass },

    #[error("achieved durability {achieved:?} weaker than requested {requested:?}")]
    AchievedWeaker {
        requested: DurabilityClass,
        achieved: DurabilityClass,
    },

    #[error("replicated proof missing for requested {requested:?}")]
    MissingReplicatedProof { requested: DurabilityClass },

    #[error("replicated proof present for local durability")]
    UnexpectedReplicatedProof,

    #[error("replicated achievement {achieved:?} for local request")]
    UnexpectedReplicatedAchievement { achieved: DurabilityClass },

    #[error("replicated proof k {proof_k} does not match expected {expected_k}")]
    ReplicatedProofMismatch {
        expected_k: NonZeroU32,
        proof_k: NonZeroU32,
    },

    #[error("replicated acks {acked} below required {required}")]
    ReplicatedAckedByTooSmall { required: NonZeroU32, acked: usize },
    // Pending receipts may carry partial or even complete ack sets; outcome is authoritative.
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

    #[error(transparent)]
    InvalidReceipt(#[from] ReceiptBuildError),
}

impl TryFrom<DurabilityReceiptWire> for DurabilityReceipt {
    type Error = ReceiptBuildError;

    fn try_from(value: DurabilityReceiptWire) -> Result<Self, Self::Error> {
        Self::new_checked(
            value.store,
            value.txn_id,
            value.event_ids,
            value.durability_proof,
            value.outcome,
            value.min_seen,
        )
    }
}

impl From<DurabilityReceipt> for DurabilityReceiptWire {
    fn from(value: DurabilityReceipt) -> Self {
        Self {
            store: value.store,
            txn_id: value.txn_id,
            event_ids: value.event_ids,
            durability_proof: value.durability_proof,
            outcome: value.outcome,
            min_seen: value.min_seen,
        }
    }
}

impl From<&DurabilityReceipt> for DurabilityReceiptWire {
    fn from(value: &DurabilityReceipt) -> Self {
        Self {
            store: value.store,
            txn_id: value.txn_id,
            event_ids: value.event_ids.clone(),
            durability_proof: value.durability_proof.clone(),
            outcome: value.outcome.clone(),
            min_seen: value.min_seen.clone(),
        }
    }
}

impl DurabilityReceipt {
    pub fn store(&self) -> StoreIdentity {
        self.store
    }

    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    pub fn event_ids(&self) -> &[EventId] {
        &self.event_ids
    }

    pub fn durability_proof(&self) -> &DurabilityProofV1 {
        &self.durability_proof
    }

    pub fn outcome(&self) -> &DurabilityOutcome {
        &self.outcome
    }

    pub fn min_seen(&self) -> &Watermarks<Applied> {
        &self.min_seen
    }

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
        let outcome = DurabilityOutcome::build_achieved(
            DurabilityClass::LocalFsync,
            DurabilityClass::LocalFsync,
        );

        Self::new_checked(
            store,
            txn_id,
            event_ids,
            durability_proof,
            outcome,
            min_seen,
        )
        .expect("local fsync receipt is valid")
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

    pub fn with_replicated_pending(
        self,
        requested: NonZeroU32,
        acked_by: Vec<ReplicaId>,
    ) -> Result<Self, ReceiptBuildError> {
        let durability_proof = DurabilityProofV1 {
            local_fsync: self.durability_proof.local_fsync,
            replicated: Some(ReplicatedProof {
                k: requested,
                acked_by,
            }),
        };
        let outcome = DurabilityOutcome::pending(DurabilityClass::ReplicatedFsync { k: requested });
        Self::new_checked(
            self.store,
            self.txn_id,
            self.event_ids,
            durability_proof,
            outcome,
            self.min_seen,
        )
    }

    pub fn with_replicated_achieved(
        self,
        requested: NonZeroU32,
        achieved: NonZeroU32,
        acked_by: Vec<ReplicaId>,
    ) -> Result<Self, ReceiptBuildError> {
        let durability_proof = DurabilityProofV1 {
            local_fsync: self.durability_proof.local_fsync,
            replicated: Some(ReplicatedProof {
                k: achieved,
                acked_by,
            }),
        };
        let outcome = DurabilityOutcome::build_achieved(
            DurabilityClass::ReplicatedFsync { k: requested },
            DurabilityClass::ReplicatedFsync { k: achieved },
        );
        Self::new_checked(
            self.store,
            self.txn_id,
            self.event_ids,
            durability_proof,
            outcome,
            self.min_seen,
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

        Ok(Self::new_checked(
            self.store,
            self.txn_id,
            self.event_ids.clone(),
            durability_proof,
            outcome,
            min_seen,
        )?)
    }

    fn new_checked(
        store: StoreIdentity,
        txn_id: TxnId,
        event_ids: Vec<EventId>,
        durability_proof: DurabilityProofV1,
        outcome: DurabilityOutcome,
        min_seen: Watermarks<Applied>,
    ) -> Result<Self, ReceiptBuildError> {
        validate_receipt(&durability_proof, &outcome)?;
        Ok(Self {
            store,
            txn_id,
            event_ids,
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
    let requested_left = left.requested();
    let requested_right = right.requested();
    if requested_left != requested_right {
        return Err(ReceiptMergeError::RequestedMismatch {
            expected: requested_left,
            actual: requested_right,
        });
    }

    let requested = requested_left;
    let outcome = match (left.achieved(), right.achieved()) {
        (Some(left), Some(right)) => {
            DurabilityOutcome::build_achieved(requested, max_durability_class(left, right))
        }
        (Some(achieved), None) | (None, Some(achieved)) => {
            DurabilityOutcome::build_achieved(requested, achieved)
        }
        (None, None) => DurabilityOutcome::pending(requested),
    };

    Ok(outcome)
}

fn validate_receipt(
    proof: &DurabilityProofV1,
    outcome: &DurabilityOutcome,
) -> Result<(), ReceiptBuildError> {
    let requested = outcome.requested();

    match outcome.achieved() {
        Some(achieved) => {
            if durability_strength(achieved) < durability_strength(requested) {
                return Err(ReceiptBuildError::AchievedWeaker {
                    requested,
                    achieved,
                });
            }
            match (requested, achieved) {
                (DurabilityClass::LocalFsync, DurabilityClass::LocalFsync) => {
                    if proof.replicated.is_some() {
                        return Err(ReceiptBuildError::UnexpectedReplicatedProof);
                    }
                    Ok(())
                }
                (DurabilityClass::LocalFsync, _) => {
                    if proof.replicated.is_some() {
                        return Err(ReceiptBuildError::UnexpectedReplicatedProof);
                    }
                    Err(ReceiptBuildError::UnexpectedReplicatedAchievement { achieved })
                }
                (
                    DurabilityClass::ReplicatedFsync { k: requested_k },
                    DurabilityClass::ReplicatedFsync { k: achieved_k },
                ) => {
                    let replicated = proof.replicated.as_ref().ok_or(
                        ReceiptBuildError::MissingReplicatedProof {
                            requested: DurabilityClass::ReplicatedFsync { k: requested_k },
                        },
                    )?;
                    if replicated.k != achieved_k {
                        return Err(ReceiptBuildError::ReplicatedProofMismatch {
                            expected_k: achieved_k,
                            proof_k: replicated.k,
                        });
                    }
                    let acked = replicated.acked_by.len();
                    if acked < achieved_k.get() as usize {
                        return Err(ReceiptBuildError::ReplicatedAckedByTooSmall {
                            required: achieved_k,
                            acked,
                        });
                    }
                    Ok(())
                }
                (DurabilityClass::ReplicatedFsync { .. }, DurabilityClass::LocalFsync) => {
                    Err(ReceiptBuildError::AchievedWeaker {
                        requested,
                        achieved,
                    })
                }
            }
        }
        None => {
            let DurabilityClass::ReplicatedFsync { k } = requested else {
                return Err(ReceiptBuildError::PendingRequiresReplicated { requested });
            };
            let replicated = proof
                .replicated
                .as_ref()
                .ok_or(ReceiptBuildError::MissingReplicatedProof { requested })?;
            if replicated.k != k {
                return Err(ReceiptBuildError::ReplicatedProofMismatch {
                    expected_k: k,
                    proof_k: replicated.k,
                });
            }
            Ok(())
        }
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
    use crate::{HeadStatus, NamespaceId, Seq0, Seq1, StoreEpoch, StoreId};
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
        let outcome = DurabilityOutcome::pending(DurabilityClass::ReplicatedFsync {
            k: NonZeroU32::new(1).unwrap(),
        });
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

        let receipt = DurabilityReceipt::local_fsync(
            store,
            txn_id,
            vec![event_id],
            1_726_000_000_000,
            Watermarks::<Durable>::new(),
            min_seen,
        );

        let json = serde_json::to_string(&receipt).unwrap();
        let parsed: DurabilityReceipt = serde_json::from_str(&json).unwrap();
        assert_eq!(receipt, parsed);
    }

    #[test]
    fn durability_receipt_rejects_missing_replicated_proof() {
        let ns = NamespaceId::parse("core").unwrap();
        let origin = ReplicaId::new(Uuid::from_bytes([12u8; 16]));
        let event_id = EventId::new(origin, ns, Seq1::from_u64(1).unwrap());
        let store =
            StoreIdentity::new(StoreId::new(Uuid::from_bytes([13u8; 16])), StoreEpoch::ZERO);
        let txn_id = TxnId::new(Uuid::from_bytes([14u8; 16]));

        let wire = DurabilityReceiptWire {
            store,
            txn_id,
            event_ids: vec![event_id],
            durability_proof: DurabilityProofV1 {
                local_fsync: LocalFsyncProof {
                    at_ms: 100,
                    durable_seq: Watermarks::<Durable>::new(),
                },
                replicated: None,
            },
            outcome: DurabilityOutcome::pending(DurabilityClass::ReplicatedFsync {
                k: NonZeroU32::new(2).unwrap(),
            }),
            min_seen: Watermarks::<Applied>::new(),
        };

        let err = DurabilityReceipt::try_from(wire).unwrap_err();
        assert!(matches!(
            err,
            ReceiptBuildError::MissingReplicatedProof { .. }
        ));
    }

    #[test]
    fn durability_receipt_rejects_achieved_without_quorum() {
        let ns = NamespaceId::parse("core").unwrap();
        let origin = ReplicaId::new(Uuid::from_bytes([15u8; 16]));
        let event_id = EventId::new(origin, ns, Seq1::from_u64(1).unwrap());
        let store =
            StoreIdentity::new(StoreId::new(Uuid::from_bytes([16u8; 16])), StoreEpoch::ZERO);
        let txn_id = TxnId::new(Uuid::from_bytes([17u8; 16]));

        let k = NonZeroU32::new(2).unwrap();
        let wire = DurabilityReceiptWire {
            store,
            txn_id,
            event_ids: vec![event_id],
            durability_proof: DurabilityProofV1 {
                local_fsync: LocalFsyncProof {
                    at_ms: 200,
                    durable_seq: Watermarks::<Durable>::new(),
                },
                replicated: Some(ReplicatedProof {
                    k,
                    acked_by: vec![origin],
                }),
            },
            outcome: DurabilityOutcome::build_achieved(
                DurabilityClass::ReplicatedFsync { k },
                DurabilityClass::ReplicatedFsync { k },
            ),
            min_seen: Watermarks::<Applied>::new(),
        };

        let err = DurabilityReceipt::try_from(wire).unwrap_err();
        assert!(matches!(
            err,
            ReceiptBuildError::ReplicatedAckedByTooSmall { .. }
        ));
    }

    #[test]
    fn durability_parse_accepts_variants() {
        assert_eq!(
            DurabilityClass::parse("local_fsync").unwrap(),
            DurabilityClass::LocalFsync
        );
        assert_eq!(
            DurabilityClass::parse("local-fsync").unwrap(),
            DurabilityClass::LocalFsync
        );
        assert_eq!(
            DurabilityClass::parse("replicated_fsync(2)").unwrap(),
            DurabilityClass::ReplicatedFsync {
                k: NonZeroU32::new(2).unwrap()
            }
        );
        assert_eq!(
            DurabilityClass::parse("replicated_fsync:3").unwrap(),
            DurabilityClass::ReplicatedFsync {
                k: NonZeroU32::new(3).unwrap()
            }
        );
        assert_eq!(
            DurabilityClass::parse("replicated_fsync=4").unwrap(),
            DurabilityClass::ReplicatedFsync {
                k: NonZeroU32::new(4).unwrap()
            }
        );
    }

    #[test]
    fn durability_parse_rejects_invalid() {
        assert!(matches!(
            DurabilityClass::parse(""),
            Err(DurabilityParseError::Empty)
        ));
        assert!(matches!(
            DurabilityClass::parse("replicated_fsync(0)"),
            Err(DurabilityParseError::Unsupported { .. })
        ));
        assert!(matches!(
            DurabilityClass::parse("weird"),
            Err(DurabilityParseError::Unsupported { .. })
        ));
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

        let pending = DurabilityReceipt::local_fsync(
            store,
            txn_id,
            vec![event_id.clone()],
            10,
            Watermarks::<Durable>::new(),
            Watermarks::<Applied>::new(),
        )
        .with_replicated_pending(NonZeroU32::new(2).unwrap(), vec![origin])
        .expect("pending receipt");

        let mut min_seen = Watermarks::<Applied>::new();
        min_seen
            .observe_at_least(&ns, &origin, Seq0::new(1), HeadStatus::Known([1u8; 32]))
            .unwrap();

        let achieved = DurabilityReceipt::local_fsync(
            store,
            txn_id,
            vec![event_id],
            20,
            Watermarks::<Durable>::new(),
            min_seen,
        )
        .with_replicated_achieved(
            NonZeroU32::new(2).unwrap(),
            NonZeroU32::new(2).unwrap(),
            vec![origin, other_replica],
        )
        .expect("achieved receipt");

        let merged = pending.merge(&achieved).unwrap();
        assert!(merged.outcome().is_achieved());
        let replicated = merged
            .durability_proof()
            .replicated
            .expect("replicated proof");
        assert_eq!(replicated.k.get(), 2);
        assert!(replicated.acked_by.contains(&origin));
        assert!(replicated.acked_by.contains(&other_replica));

        let watermark = merged.min_seen.get(&ns, &origin).expect("min_seen");
        assert_eq!(watermark.seq().get(), 1);
    }
}
