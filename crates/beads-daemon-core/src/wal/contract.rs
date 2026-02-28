#[cfg(any(feature = "test-harness", test))]
use super::{
    ClientRequestEventIds, HlcRow, ReplicaDurabilityRole, ReplicaLivenessRow, SegmentRow,
    WalCursorOffset, WalIndex, WalIndexError,
};
#[cfg(any(feature = "test-harness", test))]
use crate::core::{
    ActorId, Applied, ClientRequestId, Durable, EventId, HeadStatus, NamespaceId, ReplicaId,
    ReplicaRole, SegmentId, Seq0, Seq1, TxnId, Watermark, WatermarkPair,
};
#[cfg(any(feature = "test-harness", test))]
use uuid::Uuid;

#[cfg(any(feature = "test-harness", test))]
pub fn wal_index_laws<I, F>(factory: F)
where
    I: WalIndex,
    F: Fn() -> I,
{
    test_event_recording(&factory);
    test_sequence_generation(&factory);
    test_watermark_updates(&factory);
    test_client_requests(&factory);
    test_segment_storage(&factory);
    test_segment_replacement_is_total_set_with_factory(&factory);
    test_hlc_storage(&factory);
    test_replica_liveness(&factory);
}

#[cfg(any(feature = "test-harness", test))]
fn test_segment_replacement_is_total_set_with_factory<I, F>(factory: &F)
where
    I: WalIndex,
    F: Fn() -> I,
{
    let index = factory();
    let namespace = NamespaceId::core();
    let stale_one = SegmentRow::open(
        namespace.clone(),
        SegmentId::new(Uuid::from_bytes([31u8; 16])),
        std::path::PathBuf::from("seg-stale-1"),
        10,
        WalCursorOffset::new(1),
    );
    let stale_two = SegmentRow::sealed(
        namespace.clone(),
        SegmentId::new(Uuid::from_bytes([32u8; 16])),
        std::path::PathBuf::from("seg-stale-2"),
        20,
        WalCursorOffset::new(2),
        2,
    );
    let replacement = SegmentRow::open(
        namespace.clone(),
        SegmentId::new(Uuid::from_bytes([33u8; 16])),
        std::path::PathBuf::from("seg-new"),
        30,
        WalCursorOffset::new(3),
    );

    let mut txn = index.writer().begin_txn().expect("begin txn");
    txn.upsert_segment(&stale_one).expect("seed stale_one");
    txn.upsert_segment(&stale_two).expect("seed stale_two");
    txn.commit().expect("commit seed");

    let reader = index.reader();
    assert_eq!(
        reader
            .list_segment_namespaces()
            .expect("list namespaces after seed"),
        vec![namespace.clone()]
    );

    let mut txn = index.writer().begin_txn().expect("begin replacement txn");
    txn.replace_namespace_segments(&namespace, std::slice::from_ref(&replacement))
        .expect("replace namespace rows");
    txn.commit().expect("commit replacement");

    let rows = reader
        .list_segments(&namespace)
        .expect("list segments after replacement");
    assert_eq!(rows, vec![replacement.clone()]);
    assert_eq!(
        reader
            .list_segment_namespaces()
            .expect("list namespaces after replacement"),
        vec![namespace.clone()]
    );

    let other_namespace = NamespaceId::parse("other").expect("valid namespace");
    let wrong_namespace_row = SegmentRow::open(
        other_namespace,
        SegmentId::new(Uuid::from_bytes([34u8; 16])),
        std::path::PathBuf::from("seg-wrong-ns"),
        40,
        WalCursorOffset::new(4),
    );
    let mut txn = index.writer().begin_txn().expect("begin mismatch txn");
    let err = txn
        .replace_namespace_segments(&namespace, &[wrong_namespace_row])
        .expect_err("namespace mismatch must error");
    assert!(matches!(err, WalIndexError::SegmentRowDecode(_)));
    txn.rollback().expect("rollback mismatch txn");

    let mut txn = index.writer().begin_txn().expect("begin clear txn");
    txn.replace_namespace_segments(&namespace, &[])
        .expect("clear namespace rows");
    txn.commit().expect("commit clear");

    assert!(
        reader
            .list_segments(&namespace)
            .expect("list segments after clear")
            .is_empty()
    );
    assert!(
        !reader
            .list_segment_namespaces()
            .expect("list namespaces after clear")
            .contains(&namespace)
    );
}

#[cfg(test)]
#[test]
fn test_segment_replacement_is_total_set() {
    test_segment_replacement_is_total_set_with_factory(&super::MemoryWalIndex::new);
}

#[cfg(any(feature = "test-harness", test))]
fn test_event_recording<I, F>(factory: &F)
where
    I: WalIndex,
    F: Fn() -> I,
{
    let index = factory();
    let ns = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::new_v4());

    let mut txn = index.writer().begin_txn().expect("begin txn");
    let seq = txn.next_origin_seq(&ns, &origin).expect("next seq");
    let event_id = EventId::new(origin, ns.clone(), seq);
    let sha = [1u8; 32];
    let txn_id = TxnId::new(Uuid::new_v4());
    let segment_id = SegmentId::new(Uuid::new_v4());

    // Record new event
    txn.record_event(
        &ns, &event_id, sha, None, segment_id, 0, 100, 1000, txn_id, None,
    )
    .expect("record event");
    txn.commit().expect("commit");

    // Verify lookup
    let reader = index.reader();
    let stored = reader.lookup_event_sha(&ns, &event_id).expect("lookup");
    assert_eq!(stored, Some(sha));

    // Idempotency (same SHA)
    let mut txn = index.writer().begin_txn().expect("begin txn");
    txn.record_event(
        &ns, &event_id, sha, None, segment_id, 0, 100, 1000, txn_id, None,
    )
    .expect("record event idempotent");
    txn.commit().expect("commit");

    // Metadata conflict (same SHA, different metadata)
    let mut txn = index.writer().begin_txn().expect("begin txn");
    let err = txn
        .record_event(
            &ns,
            &event_id,
            sha,
            Some([9u8; 32]),
            segment_id,
            0,
            100,
            1000,
            txn_id,
            None,
        )
        .expect_err("event conflict");
    assert!(matches!(err, WalIndexError::EventConflict { .. }));
    drop(txn);

    // Equivocation (different SHA)
    let mut txn = index.writer().begin_txn().expect("begin txn");
    let other_sha = [2u8; 32];
    let err = txn
        .record_event(
            &ns, &event_id, other_sha, None, segment_id, 0, 100, 1000, txn_id, None,
        )
        .expect_err("equivocation");

    match err {
        WalIndexError::Equivocation {
            existing_sha256,
            new_sha256,
            ..
        } => {
            assert_eq!(existing_sha256, sha);
            assert_eq!(new_sha256, other_sha);
        }
        _ => panic!("expected Equivocation error, got {:?}", err),
    }
}

#[cfg(any(feature = "test-harness", test))]
fn test_sequence_generation<I, F>(factory: &F)
where
    I: WalIndex,
    F: Fn() -> I,
{
    let index = factory();
    let ns = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::new_v4());

    let mut txn = index.writer().begin_txn().expect("begin txn");
    let seq1 = txn.next_origin_seq(&ns, &origin).expect("seq1");
    let seq2 = txn.next_origin_seq(&ns, &origin).expect("seq2");
    txn.commit().expect("commit");

    assert!(seq1 < seq2);
    assert_eq!(seq2.get(), seq1.get() + 1);

    // Persistence across txns
    let mut txn = index.writer().begin_txn().expect("begin txn");
    let seq3 = txn.next_origin_seq(&ns, &origin).expect("seq3");
    assert_eq!(seq3.get(), seq2.get() + 1);
}

#[cfg(any(feature = "test-harness", test))]
fn test_watermark_updates<I, F>(factory: &F)
where
    I: WalIndex,
    F: Fn() -> I,
{
    let index = factory();
    let ns = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::new_v4());

    let applied = Watermark::<Applied>::new(Seq0::new(10), HeadStatus::Known([10u8; 32])).unwrap();
    let durable = Watermark::<Durable>::new(Seq0::new(5), HeadStatus::Known([5u8; 32])).unwrap();
    let watermarks = WatermarkPair::new(applied, durable).unwrap();

    let mut txn = index.writer().begin_txn().expect("begin txn");
    txn.update_watermark(&ns, &origin, watermarks)
        .expect("update");
    txn.commit().expect("commit");

    let reader = index.reader();
    let rows = reader.load_watermarks().expect("load");
    let row = rows
        .iter()
        .find(|r| r.namespace == ns && r.origin == origin)
        .expect("found row");

    assert_eq!(row.applied_seq(), applied.seq().get());
    assert_eq!(row.durable_seq(), durable.seq().get());
}

#[cfg(any(feature = "test-harness", test))]
fn test_client_requests<I, F>(factory: &F)
where
    I: WalIndex,
    F: Fn() -> I,
{
    let index = factory();
    let ns = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::new_v4());
    let req_id = ClientRequestId::new(Uuid::new_v4());
    let txn_id = TxnId::new(Uuid::new_v4());
    let event_id = EventId::new(origin, ns.clone(), Seq1::from_u64(1).unwrap());
    let event_ids = ClientRequestEventIds::single(event_id.clone());
    let sha = [3u8; 32];

    let mut txn = index.writer().begin_txn().expect("begin txn");
    txn.upsert_client_request(&ns, &origin, req_id, sha, txn_id, &event_ids, 1000)
        .expect("upsert");
    txn.commit().expect("commit");

    let reader = index.reader();
    let row = reader
        .lookup_client_request(&ns, &origin, req_id)
        .expect("lookup")
        .expect("found");
    assert_eq!(row.request_sha256, sha);
    assert_eq!(row.txn_id, txn_id);

    // Mismatch detection
    let mut txn = index.writer().begin_txn().expect("begin txn");
    let other_sha = [4u8; 32];
    let err = txn
        .upsert_client_request(&ns, &origin, req_id, other_sha, txn_id, &event_ids, 2000)
        .expect_err("mismatch");

    match err {
        WalIndexError::ClientRequestIdReuseMismatch {
            expected_request_sha256,
            got_request_sha256,
            ..
        } => {
            assert_eq!(expected_request_sha256, sha);
            assert_eq!(got_request_sha256, other_sha);
        }
        _ => panic!("expected ClientRequestIdReuseMismatch, got {:?}", err),
    }
}

#[cfg(any(feature = "test-harness", test))]
fn test_segment_storage<I, F>(factory: &F)
where
    I: WalIndex,
    F: Fn() -> I,
{
    let index = factory();
    let ns = NamespaceId::core();
    let seg_id = SegmentId::new(Uuid::new_v4());
    let path = std::path::PathBuf::from("seg-1");

    let row = SegmentRow::open(
        ns.clone(),
        seg_id,
        path.clone(),
        1000,
        WalCursorOffset::new(0),
    );

    let mut txn = index.writer().begin_txn().expect("begin txn");
    txn.upsert_segment(&row).expect("upsert");
    txn.commit().expect("commit");

    let reader = index.reader();
    let rows = reader.list_segments(&ns).expect("list");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].segment_id(), seg_id);
    assert_eq!(rows[0].segment_path(), path.as_path());
    assert!(!rows[0].is_sealed());

    // Update to sealed
    let sealed = SegmentRow::sealed(
        ns.clone(),
        seg_id,
        path.clone(),
        1000,
        WalCursorOffset::new(100),
        100,
    );
    let mut txn = index.writer().begin_txn().expect("begin txn");
    txn.upsert_segment(&sealed).expect("upsert sealed");
    txn.commit().expect("commit");

    let rows = reader.list_segments(&ns).expect("list");
    assert_eq!(rows.len(), 1);
    assert!(rows[0].is_sealed());
    assert_eq!(rows[0].final_len(), Some(100));
}

#[cfg(any(feature = "test-harness", test))]
fn test_hlc_storage<I, F>(factory: &F)
where
    I: WalIndex,
    F: Fn() -> I,
{
    let index = factory();
    let actor = ActorId::new("test-actor").unwrap();

    let row = HlcRow {
        actor_id: actor.clone(),
        last_physical_ms: 1000,
        last_logical: 1,
    };

    let mut txn = index.writer().begin_txn().expect("begin txn");
    txn.update_hlc(&row).expect("update");
    txn.commit().expect("commit");

    let reader = index.reader();
    let rows = reader.load_hlc().expect("load");
    let loaded = rows.iter().find(|r| r.actor_id == actor).expect("found");
    assert_eq!(loaded.last_physical_ms, 1000);
}

#[cfg(any(feature = "test-harness", test))]
fn test_replica_liveness<I, F>(factory: &F)
where
    I: WalIndex,
    F: Fn() -> I,
{
    let index = factory();
    let replica = ReplicaId::new(Uuid::new_v4());

    let row = ReplicaLivenessRow {
        replica_id: replica,
        last_seen_ms: 1000,
        last_handshake_ms: 500,
        role: ReplicaDurabilityRole::peer(false),
    };

    let mut txn = index.writer().begin_txn().expect("begin txn");
    txn.upsert_replica_liveness(&row).expect("upsert");
    txn.commit().expect("commit");

    let reader = index.reader();
    let rows = reader.load_replica_liveness().expect("load");
    let loaded = rows
        .iter()
        .find(|r| r.replica_id == replica)
        .expect("found");
    assert_eq!(loaded.last_seen_ms, 1000);
    assert_eq!(loaded.role.role(), ReplicaRole::Peer);
}
