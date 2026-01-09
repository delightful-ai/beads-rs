//! Toy "canonical" codec + hashing for model checking.
//!
//! This is **not** CBOR and it does **not** implement unknown-key preservation.
//! The purpose is to give the Stateright models a deterministic:
//! - preimage (hashed bytes)
//! - envelope (wire bytes)
//! while keeping the state space tiny.

use crate::spec::{sha256_bytes, NamespaceId, ReplicaId, Seq0, Seq1, Sha256, StoreIdentity};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ToyPreimage {
    pub store: StoreIdentity,
    pub namespace: NamespaceId,
    pub origin: ReplicaId,
    /// 1-based sequence number.
    pub seq: u64,
    /// Some tiny payload so different events don't collapse.
    pub payload_tag: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ToyEnvelope {
    pub pre: ToyPreimage,
    /// Prev link rule:
    /// - seq==1 => prev MUST be None
    /// - seq>1  => prev MUST be Some
    pub prev: Option<Sha256>,
    /// Declared hash of canonical preimage.
    pub sha: Sha256,
}

impl ToyEnvelope {
    pub fn compute_sha(pre: &ToyPreimage) -> Sha256 {
        let pre_bytes = serde_json::to_vec(pre).expect("toy preimage must serialize");
        sha256_bytes(&pre_bytes)
    }

    pub fn new(store: StoreIdentity, namespace: NamespaceId, origin: ReplicaId, seq: Seq1, payload_tag: u8, prev: Option<Sha256>) -> Self {
        let pre = ToyPreimage {
            store,
            namespace,
            origin,
            seq: seq.as_u64(),
            payload_tag,
        };
        let sha = Self::compute_sha(&pre);
        Self { pre, prev, sha }
    }

    pub fn encode_envelope_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("toy envelope must serialize")
    }

    pub fn decode_envelope_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        serde_json::from_slice(bytes).map_err(|_| "decode")
    }

    pub fn verify(
        bytes: &[u8],
        expected_store: StoreIdentity,
        expected_prev_head: Option<Sha256>,
    ) -> Result<Self, &'static str> {
        let env = Self::decode_envelope_bytes(bytes)?;

        // Store gating.
        if env.pre.store != expected_store {
            return Err("wrong_store");
        }

        // SHA gating.
        let computed = Self::compute_sha(&env.pre);
        if computed != env.sha {
            return Err("sha_mismatch");
        }

        // Prev-link gating.
        match (env.pre.seq, env.prev, expected_prev_head) {
            (1, None, _) => {}
            (1, Some(_), _) => return Err("prev_mismatch"),
            (s, Some(p), Some(head)) if s > 1 && p == head => {}
            (s, Some(_), None) if s > 1 => return Err("prev_unknown"),
            (s, _, _) if s > 1 => return Err("prev_mismatch"),
            _ => {}
        }

        Ok(env)
    }
}

/// Helper: compute the Seq0 watermark after accepting a contiguous batch.
pub fn advance_seq0_by(mut cur: Seq0, batch_len: usize) -> Seq0 {
    cur.0 += batch_len as u64;
    cur
}
