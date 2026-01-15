//! Phase 4 tests: receipt semantics + origin_seq persistence.

mod fixtures;

use uuid::Uuid;

use beads_rs::daemon::wal::{WalIndex, rebuild_index};
use beads_rs::{
    DurabilityReceipt, EventId, Limits, NamespaceId, ReplicaId, Seq1, StoreEpoch, StoreId,
    StoreIdentity, TxnId, Watermarks, sha256_bytes,
};

use crate::fixtures::identity;
use crate::fixtures::receipt;
use crate::fixtures::wal::{TempWalDir, record_for_seq};

#[test]
fn phase4_receipt_min_seen_is_monotonic() {
    let store = StoreIdentity::new(
        StoreId::new(Uuid::from_bytes([1u8; 16])),
        StoreEpoch::new(1),
    );
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
    let event_id = EventId::new(origin, namespace.clone(), Seq1::from_u64(1).expect("seq1"));
    let txn_id = TxnId::new(Uuid::from_bytes([3u8; 16]));

    let head1 = sha256_bytes(b"head1").0;
    let head2 = sha256_bytes(b"head2").0;

    let mut durable_a = Watermarks::new();
    durable_a
        .advance_contiguous(&namespace, &origin, Seq1::from_u64(1).expect("seq1"), head1)
        .expect("advance durable");
    let mut min_seen_a = Watermarks::new();
    min_seen_a
        .advance_contiguous(&namespace, &origin, Seq1::from_u64(1).expect("seq1"), head1)
        .expect("advance min_seen");

    let receipt_a = DurabilityReceipt::local_fsync(
        store,
        txn_id,
        vec![event_id.clone()],
        100,
        durable_a.clone(),
        min_seen_a.clone(),
    );

    let mut durable_b = durable_a.clone();
    durable_b
        .advance_contiguous(&namespace, &origin, Seq1::from_u64(2).expect("seq1"), head2)
        .expect("advance durable");
    let mut min_seen_b = min_seen_a.clone();
    min_seen_b
        .advance_contiguous(&namespace, &origin, Seq1::from_u64(2).expect("seq1"), head2)
        .expect("advance min_seen");

    let receipt_b =
        DurabilityReceipt::local_fsync(store, txn_id, vec![event_id], 200, durable_b, min_seen_b);

    receipt::assert_receipt_at_least(&receipt_a, &receipt_b);
}

#[test]
fn phase4_receipt_survives_restart() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = temp.meta().replica_id;

    let client_request_id = identity::client_request_id(22);
    let request_sha = [9u8; 32];
    let event_id = EventId::new(origin, namespace.clone(), Seq1::from_u64(1).expect("seq1"));
    let txn_id = TxnId::new(Uuid::from_bytes([8u8; 16]));
    let created_at_ms = 1_700_000_000_222;

    let index = temp.open_index().expect("open wal index");
    let mut txn = index.writer().begin_txn().expect("begin txn");
    txn.upsert_client_request(
        &namespace,
        &origin,
        client_request_id,
        request_sha,
        txn_id,
        &[event_id],
        created_at_ms,
    )
    .expect("upsert client request");
    txn.commit().expect("commit");

    let row_before = index
        .reader()
        .lookup_client_request(&namespace, &origin, client_request_id)
        .expect("lookup client request")
        .expect("client request row");
    drop(index);

    let index = temp.open_index().expect("reopen wal index");
    let row_after = index
        .reader()
        .lookup_client_request(&namespace, &origin, client_request_id)
        .expect("lookup client request")
        .expect("client request row");

    assert_eq!(row_before, row_after);

    let store = temp.meta().identity;
    let receipt_before = DurabilityReceipt::local_fsync_defaults(
        store,
        row_before.txn_id,
        row_before.event_ids.clone(),
        row_before.created_at_ms,
    );
    let receipt_after = DurabilityReceipt::local_fsync_defaults(
        store,
        row_after.txn_id,
        row_after.event_ids.clone(),
        row_after.created_at_ms + 1,
    );

    receipt::assert_receipt_identity(&receipt_before, &receipt_after);
    assert_ne!(
        receipt_before.durability_proof.local_fsync.at_ms,
        receipt_after.durability_proof.local_fsync.at_ms
    );
}

#[test]
fn phase4_origin_seq_after_restart_is_max_plus_one() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([33u8; 16]));

    let record1 = record_for_seq(temp.meta(), &namespace, origin, 1, None);
    let record2 = record_for_seq(
        temp.meta(),
        &namespace,
        origin,
        2,
        Some(record1.header.sha256),
    );

    temp.write_segment(&namespace, 1_700_000_000_000, &[record1, record2])
        .expect("write segment");

    let index = temp.open_index().expect("open wal index");
    let limits = Limits::default();
    rebuild_index(temp.store_dir(), temp.meta(), &index, &limits).expect("rebuild index");
    drop(index);

    let index = temp.open_index().expect("reopen wal index");
    let mut txn = index.writer().begin_txn().expect("begin txn");
    let next = txn
        .next_origin_seq(&namespace, &origin)
        .expect("next origin seq");
    txn.rollback().expect("rollback");

    assert_eq!(next, 3);
}
