//! Core CBOR hashing + decode bounds.

use beads_rs::{DecodeError, Limits, decode_event_body, encode_event_body_canonical};

use crate::fixtures::cbor::{
    GOLDEN_EVENT_BODY_CBOR, GOLDEN_EVENT_BODY_SHA256_HEX, golden_event_body, sha256_hex,
};

#[test]
fn canonical_cbor_bytes_match_fixture() {
    let body = golden_event_body();
    let bytes = encode_event_body_canonical(&body).expect("encode canonical");
    assert_eq!(bytes.as_bytes(), GOLDEN_EVENT_BODY_CBOR);
}

#[test]
fn canonical_cbor_hash_matches_fixture() {
    let body = golden_event_body();
    let bytes = encode_event_body_canonical(&body).expect("encode canonical");
    let digest = sha256_hex(bytes.as_bytes());
    assert_eq!(digest, GOLDEN_EVENT_BODY_SHA256_HEX);
}

#[test]
fn decode_rejects_indefinite_length_map() {
    let limits = Limits::default();
    let bytes = [0xbf, 0xff];
    let err = decode_event_body(&bytes, &limits).unwrap_err();
    assert!(matches!(err, DecodeError::IndefiniteLength));
}

#[test]
fn decode_rejects_oversized_payload() {
    let limits = Limits::default();
    let bytes = vec![0u8; limits.max_wal_record_bytes + 1];
    let err = decode_event_body(&bytes, &limits).unwrap_err();
    assert!(matches!(err, DecodeError::DecodeLimit(_)));
}
