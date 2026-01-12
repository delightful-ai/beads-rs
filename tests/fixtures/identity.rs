#![allow(dead_code)]

use beads_rs::{
    ClientRequestId, NamespaceId, ReplicaId, SegmentId, StoreEpoch, StoreId, StoreIdentity,
    StoreMeta, StoreMetaVersions, TxnId,
};
use uuid::Uuid;

pub fn store_id(seed: u8) -> StoreId {
    StoreId::new(Uuid::from_bytes([seed; 16]))
}

pub fn replica_id(seed: u8) -> ReplicaId {
    ReplicaId::new(Uuid::from_bytes([seed; 16]))
}

pub fn txn_id(seed: u8) -> TxnId {
    TxnId::new(Uuid::from_bytes([seed; 16]))
}

pub fn client_request_id(seed: u8) -> ClientRequestId {
    ClientRequestId::new(Uuid::from_bytes([seed; 16]))
}

pub fn segment_id(seed: u8) -> SegmentId {
    SegmentId::new(Uuid::from_bytes([seed; 16]))
}

pub fn store_identity(seed: u8) -> StoreIdentity {
    StoreIdentity::new(store_id(seed), StoreEpoch::new(0))
}

pub fn store_meta(seed: u8, created_at_ms: u64) -> StoreMeta {
    StoreMeta::new(
        store_identity(seed),
        replica_id(seed.wrapping_add(1)),
        StoreMetaVersions::new(1, 1, 1, 1, 1),
        created_at_ms,
    )
}

pub fn namespace_id(name: &str) -> NamespaceId {
    NamespaceId::parse(name).expect("valid namespace fixture")
}

pub fn valid_namespaces() -> Vec<&'static str> {
    vec!["core", "a", "abc123", "a_b", "a0_b1"]
}

pub fn invalid_namespaces() -> Vec<&'static str> {
    vec![
        "",
        "Core",
        "1core",
        "_core",
        "core-1",
        "core name",
        "core/name",
    ]
}
