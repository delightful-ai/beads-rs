//! Phase 3 tests: offline fsck verification + repair.

mod fixtures;

use std::fs;

use uuid::Uuid;

use beads_rs::daemon::wal::Record;
use beads_rs::daemon::wal::fsck::{
    FsckCheckId, FsckEvidenceCode, FsckOptions, FsckRepairKind, FsckStatus, fsck_store_dir,
};
use beads_rs::{Limits, NamespaceId, ReplicaId, StoreMeta};

use fixtures::wal::{TempWalDir, record_for_seq};
use fixtures::wal_corrupt::{corrupt_frame_body, truncate_frame_mid_body};

#[test]
fn phase3_fsck_clean_segment_passes() {
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
fn phase3_fsck_repair_truncates_tail() {
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
fn phase3_fsck_repair_quarantines_mid_file_corruption() {
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
fn phase3_fsck_reports_header_mismatch() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([4u8; 16]));
    let mut record = record_for_seq(temp.meta(), &namespace, origin, 1, None);
    record.header.event_time_ms = record.header.event_time_ms.saturating_add(1);
    temp.write_segment(&namespace, 1_700_000_000_000, &[record])
        .expect("write segment");

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

fn record_chain(
    meta: &StoreMeta,
    namespace: &NamespaceId,
    origin: ReplicaId,
    start_seq: u64,
    count: usize,
) -> Vec<Record> {
    let mut records = Vec::with_capacity(count);
    let mut prev_sha = None;
    for i in 0..count {
        let seq = start_seq + i as u64;
        let record = record_for_seq(meta, namespace, origin, seq, prev_sha);
        prev_sha = Some(record.header.sha256);
        records.push(record);
    }
    records
}
