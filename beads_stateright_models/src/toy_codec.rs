//! Toy "canonical" codec + hashing for model checking.
//!
//! This models the v0.5 rule set:
//! - hash over raw EventBody bytes
//! - sha/prev are frame headers (not inside the hashed payload)
//!
//! This is **not** CBOR. We use JSON to keep the state space tiny.

use crate::spec::{sha256_bytes, NamespaceId, ReplicaId, Seq0, Seq1, Sha256, StoreIdentity};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ToyEventId {
    pub namespace: NamespaceId,
    pub origin: ReplicaId,
    pub seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ToyEventBody {
    pub store: StoreIdentity,
    pub namespace: NamespaceId,
    pub origin: ReplicaId,
    /// 1-based sequence number.
    pub seq: u64,
    /// Some tiny payload so different events don't collapse.
    pub payload_tag: u8,
}

impl ToyEventBody {
    pub fn encode_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("toy body must serialize")
    }

    pub fn decode_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        serde_json::from_slice(bytes).map_err(|_| "decode_body")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ToyFrame {
    pub eid: ToyEventId,
    /// Declared hash of the EventBody bytes.
    pub sha: Sha256,
    /// Prev link rule:
    /// - seq==1 => prev MUST be None
    /// - seq>1  => prev MUST be Some
    pub prev: Option<Sha256>,
    /// EventBody bytes (toy JSON; locally-authored bytes are canonical).
    pub body_bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedFrame {
    pub body: ToyEventBody,
    pub sha: Sha256,
    pub prev: Option<Sha256>,
    pub body_bytes: Vec<u8>,
}

impl ToyFrame {
    pub fn compute_sha(body_bytes: &[u8]) -> Sha256 {
        sha256_bytes(body_bytes)
    }

    pub fn new(
        store: StoreIdentity,
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: Seq1,
        payload_tag: u8,
        prev: Option<Sha256>,
    ) -> Self {
        let body = ToyEventBody {
            store,
            namespace: namespace.clone(),
            origin,
            seq: seq.as_u64(),
            payload_tag,
        };
        let body_bytes = body.encode_bytes();
        let sha = Self::compute_sha(&body_bytes);
        let eid = ToyEventId {
            namespace,
            origin,
            seq: seq.as_u64(),
        };
        Self {
            eid,
            sha,
            prev,
            body_bytes,
        }
    }

    pub fn encode_frame_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("toy frame must serialize")
    }

    pub fn decode_frame_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        serde_json::from_slice(bytes).map_err(|_| "decode_frame")
    }

    pub fn verify(
        bytes: &[u8],
        expected_store: StoreIdentity,
        expected_prev_head: Option<Sha256>,
    ) -> Result<VerifiedFrame, &'static str> {
        let frame = Self::decode_frame_bytes(bytes)?;
        let body = ToyEventBody::decode_bytes(&frame.body_bytes)?;

        // Store gating.
        if body.store != expected_store {
            return Err("wrong_store");
        }

        // Cross-check event id fields.
        let expected_eid = ToyEventId {
            namespace: body.namespace.clone(),
            origin: body.origin,
            seq: body.seq,
        };
        if frame.eid != expected_eid {
            return Err("eid_mismatch");
        }

        // SHA gating.
        let computed = Self::compute_sha(&frame.body_bytes);
        if computed != frame.sha {
            return Err("sha_mismatch");
        }

        // Prev-link gating.
        match (body.seq, frame.prev, expected_prev_head) {
            (1, None, _) => {}
            (1, Some(_), _) => return Err("prev_mismatch"),
            (s, Some(p), Some(head)) if s > 1 && p == head => {}
            (s, Some(_), None) if s > 1 => return Err("prev_unknown"),
            (s, _, _) if s > 1 => return Err("prev_mismatch"),
            _ => {}
        }

        Ok(VerifiedFrame {
            body,
            sha: frame.sha,
            prev: frame.prev,
            body_bytes: frame.body_bytes,
        })
    }
}

/// Helper: compute the Seq0 watermark after accepting a contiguous batch.
pub fn advance_seq0_by(mut cur: Seq0, batch_len: usize) -> Seq0 {
    cur.0 += batch_len as u64;
    cur
}
