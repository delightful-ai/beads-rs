use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256 as Sha2};
use std::collections::BTreeMap;
use std::num::NonZeroU64;
use uuid::Uuid;

// -------------------------------------------------------------------------------------------------
// Identity + sequencing
// -------------------------------------------------------------------------------------------------

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StoreId(pub Uuid);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StoreEpoch(pub u64);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReplicaId(pub Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StoreIdentity {
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
}

/// Seq0 is used for "highest contiguous seen" values.
///
/// Seq0(0) means "nothing yet".
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Seq0(pub u64);

/// Seq1 is used for actual event origin_seq, which is 1-based.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Seq1(pub NonZeroU64);

impl Seq0 {
    pub fn next(self) -> Seq1 {
        Seq1(NonZeroU64::new(self.0.saturating_add(1)).unwrap())
    }
}

impl Seq1 {
    pub fn as_u64(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NamespaceId(pub String);

// -------------------------------------------------------------------------------------------------
// Hashes (toy)
// -------------------------------------------------------------------------------------------------

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Sha256(pub [u8; 32]);

pub fn sha256_bytes(data: &[u8]) -> Sha256 {
    let mut h = Sha2::new();
    h.update(data);
    let out = h.finalize();
    let mut buf = [0u8; 32];
    buf.copy_from_slice(&out);
    Sha256(buf)
}

// -------------------------------------------------------------------------------------------------
// Watermarks (toy)
// -------------------------------------------------------------------------------------------------

/// Watermarks are per (namespace, origin).
///
/// We keep this intentionally minimal for model checking.
pub type Watermarks = BTreeMap<(NamespaceId, ReplicaId), Seq0>;

pub fn wm_get(map: &Watermarks, ns: &NamespaceId, origin: &ReplicaId) -> Seq0 {
    map.get(&(ns.clone(), *origin)).copied().unwrap_or(Seq0(0))
}

pub fn wm_advance_one_contiguous(map: &mut Watermarks, ns: &NamespaceId, origin: &ReplicaId, seq: Seq1) -> bool {
    let cur = wm_get(map, ns, origin).0;
    let got = seq.as_u64();
    if got != cur + 1 {
        return false;
    }
    map.insert((ns.clone(), *origin), Seq0(got));
    true
}

pub fn wm_observe_at_least(map: &mut Watermarks, ns: NamespaceId, origin: ReplicaId, seq: Seq0) {
    let key = (ns, origin);
    let cur = map.get(&key).copied().unwrap_or(Seq0(0));
    if seq.0 > cur.0 {
        map.insert(key, seq);
    }
}

// -------------------------------------------------------------------------------------------------
// Handshake version negotiation (Plan-like)
// -------------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProtocolRange {
    pub max: u32,
    pub min: u32,
}

pub fn negotiate_version(local: ProtocolRange, peer_max: u32, peer_min: u32) -> Result<u32, &'static str> {
    let v = local.max.min(peer_max);
    let min_ok = local.min.max(peer_min);
    if v >= min_ok {
        Ok(v)
    } else {
        Err("version_incompatible")
    }
}
