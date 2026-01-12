//! Phase 3 tests: origin_seq allocation.

mod fixtures;

use bytes::Bytes;
use uuid::Uuid;

use beads_rs::daemon::wal::{Record, RecordHeader, WalIndex, rebuild_index};
use beads_rs::{Limits, NamespaceId, ReplicaId, TxnId};

use fixtures::wal::TempWalDir;

#[test]
fn phase3_seq_allocation_is_monotonic() {
    let temp = TempWalDir::new();
    let index = temp.open_index().expect("open wal index");
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([11u8; 16]));

    let mut txn = index.writer().begin_txn().expect("begin txn");
    let first = txn
        .next_origin_seq(&namespace, &origin)
        .expect("next origin seq");
    let second = txn
        .next_origin_seq(&namespace, &origin)
        .expect("next origin seq");
    txn.commit().expect("commit");

    assert_eq!(first, 1);
    assert_eq!(second, 2);

    let mut txn = index.writer().begin_txn().expect("begin txn");
    let third = txn
        .next_origin_seq(&namespace, &origin)
        .expect("next origin seq");
    txn.commit().expect("commit");
    assert_eq!(third, 3);

    let other_origin = ReplicaId::new(Uuid::from_bytes([12u8; 16]));
    let mut txn = index.writer().begin_txn().expect("begin txn");
    let other_first = txn
        .next_origin_seq(&namespace, &other_origin)
        .expect("next origin seq");
    txn.commit().expect("commit");
    assert_eq!(other_first, 1);
}

#[test]
fn phase3_seq_allocation_resumes_after_replay() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([13u8; 16]));
    let records = record_chain(origin, 1, 2);
    temp.write_segment(&namespace, 1_700_000_000_000, &records)
        .expect("write segment");
    let index = temp.open_index().expect("open wal index");
    let limits = Limits::default();

    rebuild_index(temp.store_dir(), temp.meta(), &index, &limits).expect("rebuild index");

    let mut txn = index.writer().begin_txn().expect("begin txn");
    let next = txn
        .next_origin_seq(&namespace, &origin)
        .expect("next origin seq");
    txn.commit().expect("commit");
    assert_eq!(next, 3);
}

fn record_chain(origin: ReplicaId, start_seq: u64, count: usize) -> Vec<Record> {
    let mut records = Vec::with_capacity(count);
    let mut prev_sha = None;
    for i in 0..count {
        let seq = start_seq + i as u64;
        let record = record_for_seq(origin, seq, prev_sha);
        prev_sha = Some(record.header.sha256);
        records.push(record);
    }
    records
}

fn record_for_seq(origin: ReplicaId, seq: u64, prev_sha: Option<[u8; 32]>) -> Record {
    let payload = Bytes::from(format!("payload-{seq}"));
    let sha = beads_rs::sha256_bytes(payload.as_ref()).0;
    let seed = seq as u8;
    let header = RecordHeader {
        origin_replica_id: origin,
        origin_seq: seq,
        event_time_ms: 1_700_000_000_000 + seq,
        txn_id: TxnId::new(Uuid::from_bytes([seed; 16])),
        client_request_id: None,
        request_sha256: None,
        sha256: sha,
        prev_sha256: prev_sha,
    };
    Record { header, payload }
}
