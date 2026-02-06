//! WAL fsck verification + repair.

use std::fs;

use uuid::Uuid;

use beads_rs::daemon::wal::fsck::{
    FsckCheckId, FsckEvidenceCode, FsckOptions, FsckRepairKind, FsckStatus, fsck_store_dir,
};
use beads_rs::daemon::wal::{SegmentRow, VerifiedRecord, WalIndex, rebuild_index};
use beads_rs::{Limits, NamespaceId, ReplicaId, StoreMeta};

use crate::fixtures::wal::{TempWalDir, record_for_seq};
use crate::fixtures::wal_corrupt::{
    corrupt_frame_body, corrupt_record_header_event_time, corrupt_record_header_sha,
    truncate_frame_mid_body,
};

#[test]
fn fsck_clean_segment_passes() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
    let records = record_chain(temp.meta(), &namespace, origin, 1, 2);
    temp.write_segment(&namespace, 1_700_000_000_000, &records)
        .expect("write segment");

    let report = fsck_store_dir(
        temp.store_dir(),
        temp.meta(),
        FsckOptions::new(false, Limits::default()),
    )
    .expect("fsck store dir");

    assert!(report.summary.safe_to_accept_writes);
    assert!(report.repairs.is_empty());
    assert!(
        report
            .checks
            .iter()
            .all(|check| check.status == FsckStatus::Pass)
    );
}

#[test]
fn fsck_repair_truncates_tail() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
    let records = record_chain(temp.meta(), &namespace, origin, 1, 1);
    let segment = temp
        .write_segment(&namespace, 1_700_000_000_000, &records)
        .expect("write segment");

    truncate_frame_mid_body(&segment, 0).expect("truncate frame");

    let report = fsck_store_dir(
        temp.store_dir(),
        temp.meta(),
        FsckOptions::new(true, Limits::default()),
    )
    .expect("fsck store dir");

    let repaired = report
        .repairs
        .iter()
        .any(|repair| repair.kind == FsckRepairKind::TruncateTail);
    assert!(repaired);

    let meta = fs::metadata(&segment.path).expect("segment metadata");
    assert_eq!(meta.len(), segment.header_len);
}

#[test]
fn fsck_repair_quarantines_mid_file_corruption() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([3u8; 16]));
    let records = record_chain(temp.meta(), &namespace, origin, 1, 2);
    let segment = temp
        .write_segment(&namespace, 1_700_000_000_000, &records)
        .expect("write segment");

    corrupt_frame_body(&segment, 0).expect("corrupt frame");

    let report = fsck_store_dir(
        temp.store_dir(),
        temp.meta(),
        FsckOptions::new(true, Limits::default()),
    )
    .expect("fsck store dir");

    assert!(!report.summary.safe_to_accept_writes);
    let quarantine = report
        .repairs
        .iter()
        .find(|repair| repair.kind == FsckRepairKind::QuarantineSegment)
        .expect("quarantine repair");
    let new_path = quarantine.path.as_ref().expect("quarantine path");
    assert!(!segment.path.exists());
    assert!(new_path.exists());

    let frames_check = report
        .checks
        .iter()
        .find(|check| check.id == FsckCheckId::SegmentFrames)
        .expect("segment frames check");
    assert_eq!(frames_check.status, FsckStatus::Fail);
}

#[test]
fn fsck_reports_header_mismatch() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([4u8; 16]));
    let record = record_for_seq(temp.meta(), &namespace, origin, 1, None);
    let segment = temp
        .write_segment(&namespace, 1_700_000_000_000, &[record])
        .expect("write segment");
    corrupt_record_header_event_time(&segment, 0).expect("corrupt header");

    let report = fsck_store_dir(
        temp.store_dir(),
        temp.meta(),
        FsckOptions::new(false, Limits::default()),
    )
    .expect("fsck store dir");

    let hashes_check = report
        .checks
        .iter()
        .find(|check| check.id == FsckCheckId::RecordHashes)
        .expect("record hashes check");
    assert!(
        hashes_check
            .evidence
            .iter()
            .any(|e| e.code == FsckEvidenceCode::RecordHeaderMismatch)
    );
}

#[test]
fn fsck_stops_after_record_sha_mismatch() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([12u8; 16]));
    let records = record_chain(temp.meta(), &namespace, origin, 1, 2);
    let segment = temp
        .write_segment(&namespace, 1_700_000_000_000, &records)
        .expect("write segment");
    corrupt_record_header_sha(&segment, 0).expect("corrupt header sha");

    let report = fsck_store_dir(
        temp.store_dir(),
        temp.meta(),
        FsckOptions::new(false, Limits::default()),
    )
    .expect("fsck store dir");

    let hashes_check = report
        .checks
        .iter()
        .find(|check| check.id == FsckCheckId::RecordHashes)
        .expect("record hashes check");
    assert!(
        hashes_check
            .evidence
            .iter()
            .any(|e| e.code == FsckEvidenceCode::RecordShaMismatch)
    );

    let contiguity_check = report
        .checks
        .iter()
        .find(|check| check.id == FsckCheckId::OriginContiguity)
        .expect("origin contiguity check");
    assert_eq!(contiguity_check.status, FsckStatus::Pass);
}

#[test]
fn fsck_reports_sealed_len_mismatch() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([5u8; 16]));
    let record1 = record_for_seq(temp.meta(), &namespace, origin, 1, None);
    let record2 = record_for_seq(
        temp.meta(),
        &namespace,
        origin,
        2,
        Some(record1.header().sha256),
    );
    let segment1 = temp
        .write_segment(&namespace, 1_700_000_000_000, &[record1])
        .expect("write segment");
    temp.write_segment(&namespace, 1_700_000_000_001, &[record2])
        .expect("write segment");
    let index = temp.open_index().expect("open wal index");
    let limits = Limits::default();

    rebuild_index(temp.store_dir(), temp.meta(), &index, &limits).expect("rebuild index");

    let rows = index.reader().list_segments(&namespace).expect("segments");
    let sealed = rows
        .iter()
        .find(|row| row.segment_id() == segment1.header.segment_id)
        .expect("segment1 row");
    let (namespace, segment_id, segment_path, created_at_ms, last_indexed_offset, final_len) =
        match sealed {
            SegmentRow::Sealed {
                namespace,
                segment_id,
                segment_path,
                created_at_ms,
                last_indexed_offset,
                final_len,
            } => (
                namespace.clone(),
                *segment_id,
                segment_path.clone(),
                *created_at_ms,
                *last_indexed_offset,
                *final_len,
            ),
            SegmentRow::Open { .. } => panic!("expected sealed segment row"),
        };
    let sealed = SegmentRow::sealed(
        namespace,
        segment_id,
        segment_path,
        created_at_ms,
        last_indexed_offset,
        final_len.saturating_add(1),
    );
    let mut txn = index.writer().begin_txn().expect("begin txn");
    txn.upsert_segment(&sealed).expect("upsert segment");
    txn.commit().expect("commit");

    let report = fsck_store_dir(
        temp.store_dir(),
        temp.meta(),
        FsckOptions::new(false, Limits::default()),
    )
    .expect("fsck store dir");

    let offsets_check = report
        .checks
        .iter()
        .find(|check| check.id == FsckCheckId::IndexOffsets)
        .expect("index offsets check");
    assert!(
        offsets_check
            .evidence
            .iter()
            .any(|e| e.code == FsckEvidenceCode::SealedSegmentLenMismatch)
    );
}

fn record_chain(
    meta: &StoreMeta,
    namespace: &NamespaceId,
    origin: ReplicaId,
    start_seq: u64,
    count: usize,
) -> Vec<VerifiedRecord> {
    let mut records = Vec::with_capacity(count);
    let mut prev_sha = None;
    for i in 0..count {
        let seq = start_seq + i as u64;
        let record = record_for_seq(meta, namespace, origin, seq, prev_sha);
        prev_sha = Some(record.header().sha256);
        records.push(record);
    }
    records
}
