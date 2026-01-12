//! Durability classes and proofs for mutation receipts.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use super::watermark::{Durable, Watermarks};
use super::ReplicaId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurabilityClass {
    LocalFsync,
    ReplicatedFsync { k: NonZeroU32 },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{NamespaceId, Seq1};
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
}
