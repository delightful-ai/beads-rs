//! Replication ACK/WANT semantics.

use std::sync::Arc;

use uuid::Uuid;

use beads_rs::core::error::details as error_details;
use beads_rs::daemon::admission::AdmissionController;
use beads_rs::daemon::repl::{
    Events, ReplMessage, Session, SessionAction, SessionConfig, SessionPhase, SessionRole,
    WalRangeReader,
};
use beads_rs::daemon::wal::{
    IndexDurabilityMode, SegmentConfig, SegmentWriter, SqliteWalIndex, rebuild_index,
};
use beads_rs::paths;
use beads_rs::{
    ActorId, EventBody, EventBytes, EventFrameV1, EventId, EventKindV1, HlcMax, Limits,
    NamespaceId, Opaque, ProtocolErrorCode, ReplicaId, Seq0, Seq1, Sha256, StoreEpoch, StoreId,
    StoreIdentity, StoreMeta, StoreMetaVersions, TxnDeltaV1, TxnId, TxnV1,
    encode_event_body_canonical, hash_event_body,
};

use crate::fixtures::identity;
use crate::fixtures::repl_frames;
use crate::fixtures::repl_peer::MockStore;
use crate::fixtures::store_dir::TempStoreDir;
use crate::fixtures::wal::record_for_seq;

fn inbound_session() -> (Session, MockStore, StoreIdentity) {
    let limits = Limits::default();
    let identity = identity::store_identity_with_epoch(1, 1);
    let local_replica = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
    let mut config = SessionConfig::new(identity, local_replica, &limits);
    config.requested_namespaces = vec![NamespaceId::core()];
    config.offered_namespaces = vec![NamespaceId::core()];
    let admission = AdmissionController::new(&limits);
    let mut session = Session::new(SessionRole::Inbound, config, limits, admission);
    let mut store = MockStore::default();

    let peer_replica = ReplicaId::new(Uuid::from_bytes([8u8; 16]));
    let hello = repl_frames::hello(identity, peer_replica);
    session.handle_message(ReplMessage::Hello(hello), &mut store, 0);
    assert!(matches!(session.phase(), SessionPhase::Streaming));

    (session, store, identity)
}

fn event_frame_with_txn(
    store: StoreIdentity,
    namespace: NamespaceId,
    origin: ReplicaId,
    seq: u64,
    prev: Option<Sha256>,
    txn_seed: u8,
) -> EventFrameV1 {
    let txn_id = TxnId::new(Uuid::from_bytes([txn_seed; 16]));
    let event_time_ms = 1_700_000_000_000 + seq;
    let body = EventBody {
        envelope_v: 1,
        store,
        namespace: namespace.clone(),
        origin_replica_id: origin,
        origin_seq: Seq1::from_u64(seq).expect("seq1"),
        event_time_ms,
        txn_id,
        client_request_id: None,
        trace_id: None,
        kind: EventKindV1::TxnV1(TxnV1 {
            delta: TxnDeltaV1::new(),
            hlc_max: HlcMax {
                actor_id: ActorId::new("fixture").expect("actor"),
                physical_ms: event_time_ms,
                logical: 0,
            },
        }),
    };
    let canonical = encode_event_body_canonical(&body).expect("encode event body");
    let sha = hash_event_body(&canonical);
    let bytes = EventBytes::<Opaque>::new(bytes::Bytes::copy_from_slice(canonical.as_ref()));

    EventFrameV1 {
        eid: EventId::new(origin, namespace, body.origin_seq),
        sha256: sha,
        prev_sha256: prev,
        bytes,
    }
}

#[test]
fn repl_ack_advances_watermarks() {
    let (mut session, mut store, identity) = inbound_session();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([3u8; 16]));

    let e1 = repl_frames::event_frame(identity, namespace.clone(), origin, 1, None);
    let e2 = repl_frames::event_frame(identity, namespace.clone(), origin, 2, Some(e1.sha256));

    let actions = session.handle_message(
        ReplMessage::Events(Events {
            events: vec![e1, e2.clone()],
        }),
        &mut store,
        10,
    );

    let ack = actions
        .iter()
        .find_map(|action| match action {
            SessionAction::Send(ReplMessage::Ack(ack)) => Some(ack),
            _ => None,
        })
        .expect("ack");

    let seq = ack
        .durable
        .get(&namespace)
        .and_then(|m| m.get(&origin))
        .copied()
        .unwrap_or(Seq0::ZERO);
    assert_eq!(seq, Seq0::new(2));

    let head = ack
        .durable_heads
        .as_ref()
        .and_then(|heads| heads.get(&namespace))
        .and_then(|m| m.get(&origin))
        .copied()
        .expect("head");
    assert_eq!(head, e2.sha256);

    let event_id = EventId::new(origin, namespace.clone(), Seq1::from_u64(2).expect("seq1"));
    assert!(store.has_event(&event_id));
}

#[test]
fn repl_gap_triggers_want() {
    let (mut session, mut store, identity) = inbound_session();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([4u8; 16]));

    let e1 = repl_frames::event_frame(identity, namespace.clone(), origin, 1, None);
    let e3 = repl_frames::event_frame(identity, namespace.clone(), origin, 3, Some(e1.sha256));

    let actions = session.handle_message(
        ReplMessage::Events(Events { events: vec![e3] }),
        &mut store,
        10,
    );

    let want = actions
        .iter()
        .find_map(|action| match action {
            SessionAction::Send(ReplMessage::Want(want)) => Some(want),
            _ => None,
        })
        .expect("want");

    let seq = want
        .want
        .get(&namespace)
        .and_then(|m| m.get(&origin))
        .copied()
        .unwrap_or(Seq0::ZERO);
    assert_eq!(seq, Seq0::ZERO);
}

#[test]
fn repl_equivocation_errors() {
    let (mut session, mut store, identity) = inbound_session();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([5u8; 16]));

    let e1 = repl_frames::event_frame(identity, namespace.clone(), origin, 1, None);
    session.handle_message(
        ReplMessage::Events(Events { events: vec![e1] }),
        &mut store,
        10,
    );

    let e1_alt = event_frame_with_txn(identity, namespace.clone(), origin, 1, None, 7);
    let actions = session.handle_message(
        ReplMessage::Events(Events {
            events: vec![e1_alt],
        }),
        &mut store,
        20,
    );

    let error = actions
        .iter()
        .find_map(|action| match action {
            SessionAction::Send(ReplMessage::Error(payload)) => Some(payload),
            _ => None,
        })
        .expect("error");

    assert_eq!(error.code, ProtocolErrorCode::Equivocation.into());
}

#[test]
fn repl_prev_sha_mismatch_rejects() {
    let (mut session, mut store, identity) = inbound_session();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([6u8; 16]));

    let e1 = repl_frames::event_frame(identity, namespace.clone(), origin, 1, None);
    let expected_prev = e1.sha256;
    session.handle_message(
        ReplMessage::Events(Events { events: vec![e1] }),
        &mut store,
        10,
    );

    let bad_prev = Sha256([9u8; 32]);
    let e2_bad = repl_frames::event_frame(identity, namespace.clone(), origin, 2, Some(bad_prev));
    let actions = session.handle_message(
        ReplMessage::Events(Events {
            events: vec![e2_bad],
        }),
        &mut store,
        20,
    );

    let error = actions
        .iter()
        .find_map(|action| match action {
            SessionAction::Send(ReplMessage::Error(payload)) => Some(payload),
            _ => None,
        })
        .expect("error");

    assert_eq!(error.code, ProtocolErrorCode::PrevShaMismatch.into());
    let details = error
        .details_as::<error_details::PrevShaMismatchDetails>()
        .unwrap()
        .expect("details");
    assert_eq!(details.eid.namespace, namespace);
    assert_eq!(details.eid.origin_replica_id, origin);
    assert_eq!(details.eid.origin_seq, 2);
    assert_eq!(
        details.expected_prev_sha256,
        hex::encode(expected_prev.as_bytes())
    );
    assert_eq!(details.got_prev_sha256, hex::encode(bad_prev.as_bytes()));
    assert_eq!(details.head_seq, 1);
}

#[test]
fn repl_want_reads_from_wal() {
    let _temp_store = TempStoreDir::new().expect("temp store dir");
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([7u8; 16]));

    let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
    let store_dir = paths::store_dir(store_id);
    std::fs::create_dir_all(&store_dir).expect("create store dir");
    let identity = StoreIdentity::new(store_id, StoreEpoch::new(0));
    let replica_id = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
    let versions = StoreMetaVersions::new(1, 2, 1, 1, 1);
    let meta = StoreMeta::new(identity, replica_id, versions, 1_700_000_000_000);
    let limits = Limits::default();

    let record1 = record_for_seq(&meta, &namespace, origin, 1, None);
    let record2 = record_for_seq(&meta, &namespace, origin, 2, Some(record1.header().sha256));

    let mut writer = SegmentWriter::open(
        &store_dir,
        &meta,
        &namespace,
        1_700_000_000_000,
        SegmentConfig::from_limits(&limits),
    )
    .expect("open segment writer");
    writer
        .append(&record1, 1_700_000_000_000)
        .expect("append record1");
    writer
        .append(&record2, 1_700_000_000_000)
        .expect("append record2");

    let index = SqliteWalIndex::open(&store_dir, &meta, IndexDurabilityMode::Cache)
        .expect("open wal index");
    rebuild_index(&store_dir, &meta, &index, &limits).expect("rebuild index");

    let reader = WalRangeReader::new(store_id, Arc::new(index), limits.clone());
    let frames = reader
        .read_range(
            &namespace,
            &origin,
            Seq0::ZERO,
            limits.max_event_batch_bytes,
        )
        .expect("read wal range");

    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].eid.origin_seq.get(), 1);
    assert_eq!(frames[1].eid.origin_seq.get(), 2);
}
