#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::Cursor;

use bytes::Bytes;
use uuid::Uuid;

use beads_rs::Limits;
use beads_rs::core::{
    ActorId, Durable, EventBody, EventBytes, EventFrameV1, EventId, EventKindV1, HlcMax,
    NamespaceId, Opaque, ReplicaId, Seq1, Sha256, StoreIdentity, TxnDeltaV1, TxnId, TxnV1,
    encode_event_body_canonical, hash_event_body,
};
use beads_rs::daemon::repl::frame::{FrameReader, encode_frame};
use beads_rs::daemon::repl::proto::{
    Ack, Capabilities, Events, Hello, PROTOCOL_VERSION_V1, ReplEnvelope, ReplMessage, Want,
    WatermarkMap, WatermarkState, Welcome, decode_envelope, encode_envelope,
};

use super::identity;

pub fn default_capabilities() -> Capabilities {
    Capabilities {
        supports_snapshots: false,
        supports_live_stream: true,
        supports_compression: false,
    }
}

pub fn hello(store: StoreIdentity, sender_replica_id: ReplicaId) -> Hello {
    hello_with_namespaces(store, sender_replica_id, vec![NamespaceId::core()])
}

pub fn hello_with_namespaces(
    store: StoreIdentity,
    sender_replica_id: ReplicaId,
    namespaces: Vec<NamespaceId>,
) -> Hello {
    let limits = Limits::default();
    Hello {
        protocol_version: PROTOCOL_VERSION_V1,
        min_protocol_version: PROTOCOL_VERSION_V1,
        store_id: store.store_id,
        store_epoch: store.store_epoch,
        sender_replica_id,
        hello_nonce: 1,
        max_frame_bytes: limits.max_frame_bytes.min(u32::MAX as usize) as u32,
        requested_namespaces: namespaces.clone(),
        offered_namespaces: namespaces,
        seen_durable: BTreeMap::new(),
        seen_applied: None,
        capabilities: default_capabilities(),
    }
}

pub fn welcome(store: StoreIdentity, receiver_replica_id: ReplicaId) -> Welcome {
    welcome_with_namespaces(store, receiver_replica_id, vec![NamespaceId::core()])
}

pub fn welcome_with_namespaces(
    store: StoreIdentity,
    receiver_replica_id: ReplicaId,
    namespaces: Vec<NamespaceId>,
) -> Welcome {
    let limits = Limits::default();
    Welcome {
        protocol_version: PROTOCOL_VERSION_V1,
        store_id: store.store_id,
        store_epoch: store.store_epoch,
        receiver_replica_id,
        welcome_nonce: 2,
        accepted_namespaces: namespaces,
        receiver_seen_durable: BTreeMap::new(),
        receiver_seen_applied: None,
        live_stream_enabled: true,
        max_frame_bytes: limits.max_frame_bytes.min(u32::MAX as usize) as u32,
    }
}

pub fn events(frames: Vec<EventFrameV1>) -> Events {
    Events { events: frames }
}

pub fn ack(durable: WatermarkState<Durable>) -> Ack {
    Ack {
        durable,
        applied: None,
    }
}

pub fn want(want: WatermarkMap) -> Want {
    Want { want }
}

pub fn envelope(message: ReplMessage) -> ReplEnvelope {
    ReplEnvelope {
        version: PROTOCOL_VERSION_V1,
        message,
    }
}

pub fn encode_message(message: ReplMessage) -> Vec<u8> {
    encode_envelope(&envelope(message)).expect("encode envelope")
}

pub fn encode_message_frame(message: ReplMessage, max_frame_bytes: usize) -> Vec<u8> {
    let payload = encode_message(message);
    encode_frame(&payload, max_frame_bytes).expect("encode frame")
}

pub fn decode_message_frame(frame: &[u8], max_frame_bytes: usize) -> ReplEnvelope {
    let mut reader = FrameReader::new(Cursor::new(frame), max_frame_bytes);
    let payload = reader
        .read_next()
        .expect("frame read")
        .expect("frame payload");
    decode_envelope(&payload, &Limits::default()).expect("decode envelope")
}

pub fn event_frame(
    store: StoreIdentity,
    namespace: NamespaceId,
    origin: ReplicaId,
    seq: u64,
    prev: Option<Sha256>,
) -> EventFrameV1 {
    let body = EventBody {
        envelope_v: 1,
        store,
        namespace: namespace.clone(),
        origin_replica_id: origin,
        origin_seq: Seq1::from_u64(seq).expect("valid seq"),
        event_time_ms: 1_700_000_000_000 + seq,
        txn_id: TxnId::new(Uuid::from_bytes([seq as u8; 16])),
        client_request_id: None,
        trace_id: None,
        kind: EventKindV1::TxnV1(TxnV1 {
            delta: TxnDeltaV1::new(),
            hlc_max: HlcMax {
                actor_id: ActorId::new("fixture").expect("actor"),
                physical_ms: 1_700_000_000_000 + seq,
                logical: 0,
            },
        }),
    };

    let canonical = encode_event_body_canonical(&body).expect("encode event body");
    let sha = hash_event_body(&canonical);
    let bytes = EventBytes::<Opaque>::new(Bytes::copy_from_slice(canonical.as_ref()));
    EventFrameV1 {
        eid: EventId::new(origin, namespace, body.origin_seq),
        sha256: sha,
        prev_sha256: prev,
        bytes,
    }
}

pub fn single_event(store: StoreIdentity, origin: ReplicaId) -> EventFrameV1 {
    event_frame(store, NamespaceId::core(), origin, 1, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixtures_repl_frames_roundtrip() {
        let store = identity::store_identity_with_epoch(1, 1);
        let replica = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let hello = ReplMessage::Hello(hello(store, replica));
        let frame = encode_message_frame(hello.clone(), Limits::default().max_frame_bytes);
        let decoded = decode_message_frame(&frame, Limits::default().max_frame_bytes);
        assert_eq!(decoded.message, hello);
    }
}
