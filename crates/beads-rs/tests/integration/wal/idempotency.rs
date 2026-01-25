//! WAL idempotency mapping + request hash reuse.

use uuid::Uuid;

use beads_rs::daemon::wal::{WalIndex, WalIndexError};
use beads_rs::{EventId, NamespaceId, Seq1, TxnId};

use crate::fixtures::identity;
use crate::fixtures::mutation;
use crate::fixtures::wal::TempWalDir;

#[test]
fn idempotency_mapping_reuses_txn_and_event_ids() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = temp.meta().replica_id;

    let client_request_id = identity::client_request_id(10);
    let ctx = mutation::context_with_client_request_id(10);
    let request = mutation::add_labels_request("bd-1", vec!["alpha".to_string()]);
    let request_sha = mutation::request_sha256(&ctx, &request).expect("request sha");

    let event_id = EventId::new(origin, namespace.clone(), Seq1::from_u64(1).expect("seq1"));
    let txn_id = TxnId::new(Uuid::from_bytes([5u8; 16]));
    let created_at_ms = 1_700_000_000_000;

    let index = temp.open_index().expect("open wal index");
    let mut txn = index.writer().begin_txn().expect("begin txn");
    txn.upsert_client_request(
        &namespace,
        &origin,
        client_request_id,
        request_sha,
        txn_id,
        std::slice::from_ref(&event_id),
        created_at_ms,
    )
    .expect("upsert client request");
    txn.commit().expect("commit");

    let row = index
        .reader()
        .lookup_client_request(&namespace, &origin, client_request_id)
        .expect("lookup client request")
        .expect("client request row");
    assert_eq!(row.txn_id, txn_id);
    assert_eq!(row.event_ids, vec![event_id.clone()]);

    let row_again = index
        .reader()
        .lookup_client_request(&namespace, &origin, client_request_id)
        .expect("lookup client request")
        .expect("client request row");
    assert_eq!(row, row_again);

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
    .expect("idempotent upsert");
    txn.commit().expect("commit");
}

#[test]
fn request_sha_mismatch_returns_error() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = temp.meta().replica_id;

    let client_request_id = identity::client_request_id(11);
    let ctx = mutation::context_with_client_request_id(11);
    let request = mutation::add_labels_request("bd-1", vec!["alpha".to_string()]);
    let request_sha = mutation::request_sha256(&ctx, &request).expect("request sha");

    let event_id = EventId::new(origin, namespace.clone(), Seq1::from_u64(1).expect("seq1"));
    let txn_id = TxnId::new(Uuid::from_bytes([6u8; 16]));
    let created_at_ms = 1_700_000_000_100;

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

    let different_request = mutation::add_labels_request("bd-1", vec!["beta".to_string()]);
    let mismatched_sha = mutation::request_sha256(&ctx, &different_request).expect("request sha");
    assert_ne!(request_sha, mismatched_sha);

    let mut txn = index.writer().begin_txn().expect("begin txn");
    let err = txn
        .upsert_client_request(
            &namespace,
            &origin,
            client_request_id,
            mismatched_sha,
            txn_id,
            &[],
            created_at_ms + 1,
        )
        .expect_err("expected mismatch error");
    txn.rollback().expect("rollback");

    assert!(matches!(
        err,
        WalIndexError::ClientRequestIdReuseMismatch { .. }
    ));
}
