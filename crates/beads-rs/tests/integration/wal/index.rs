//! WAL index rebuild + catch-up.

use std::fs::{self, OpenOptions};

use uuid::Uuid;

use beads_rs::daemon::wal::{
    FrameWriter, SegmentRow, VerifiedRecord, WalIndex, WalReplayError, catch_up_index,
    rebuild_index,
};
use beads_rs::{Limits, NamespaceId, ReplicaId, Seq0, Seq1, StoreMeta};

use crate::fixtures::wal::{SegmentFixture, TempWalDir, record_for_seq};

const MAX_RECORD_BYTES: usize = 1024 * 1024;

#[test]
fn index_rebuild_populates_watermarks_and_segments() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
    let records = record_chain(temp.meta(), &namespace, origin, 1, 2);
    let segment = temp
        .write_segment(&namespace, 1_700_000_000_000, &records)
        .expect("write segment");
    let index = temp.open_index().expect("open wal index");
    let limits = Limits::default();

    let stats =
        rebuild_index(temp.store_dir(), temp.meta(), &index, &limits).expect("rebuild index");
    assert_eq!(stats.records_indexed, 2);

    let watermarks = index.reader().load_watermarks().expect("load watermarks");
    let row = watermarks
        .iter()
        .find(|row| row.origin == origin && row.namespace == namespace)
        .expect("watermark row");
    assert_eq!(row.applied_seq, 2);
    assert_eq!(row.durable_seq, 2);
    assert_eq!(row.applied_head_sha, Some(records[1].header().sha256));
    assert_eq!(row.durable_head_sha, Some(records[1].header().sha256));

    let segments = index.reader().list_segments(&namespace).expect("segments");
    assert_eq!(segments.len(), 1);
    let expected_offset = segment
        .frame_offset(1)
        .saturating_add(segment.frame_len(1) as u64);
    assert_eq!(segments[0].last_indexed_offset(), expected_offset);

    let range = index
        .reader()
        .iter_from(&namespace, &origin, Seq0::ZERO, MAX_RECORD_BYTES)
        .expect("range");
    assert_eq!(range.len(), 2);
    assert_eq!(range[0].event_id.origin_seq, Seq1::from_u64(1).unwrap());
    assert_eq!(range[1].event_id.origin_seq, Seq1::from_u64(2).unwrap());
}

#[test]
fn index_catch_up_scans_new_frames() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([10u8; 16]));
    let mut records = record_chain(temp.meta(), &namespace, origin, 1, 2);
    let segment = temp
        .write_segment(&namespace, 1_700_000_000_000, &records)
        .expect("write segment");
    let index = temp.open_index().expect("open wal index");
    let limits = Limits::default();

    rebuild_index(temp.store_dir(), temp.meta(), &index, &limits).expect("rebuild index");

    let record3 = record_for_seq(
        temp.meta(),
        &namespace,
        origin,
        3,
        Some(records[1].header().sha256),
    );
    let (_offset, len) = append_record(&segment, &record3);
    records.push(record3);

    let stats =
        catch_up_index(temp.store_dir(), temp.meta(), &index, &limits).expect("catch up index");
    assert_eq!(stats.records_indexed, 1);

    let segments = index.reader().list_segments(&namespace).expect("segments");
    let expected_offset = segment
        .frame_offset(1)
        .saturating_add(segment.frame_len(1) as u64 + len as u64);
    assert_eq!(segments[0].last_indexed_offset(), expected_offset);

    let watermarks = index.reader().load_watermarks().expect("load watermarks");
    let row = watermarks
        .iter()
        .find(|row| row.origin == origin && row.namespace == namespace)
        .expect("watermark row");
    assert_eq!(row.applied_seq, 3);
    assert_eq!(row.durable_seq, 3);

    let range = index
        .reader()
        .iter_from(&namespace, &origin, Seq0::ZERO, MAX_RECORD_BYTES)
        .expect("range");
    assert_eq!(range.len(), 3);
    assert_eq!(range[2].event_id.origin_seq, Seq1::from_u64(3).unwrap());
}

#[test]
fn index_marks_sealed_segments() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([14u8; 16]));
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
    let segment2 = temp
        .write_segment(&namespace, 1_700_000_000_001, &[record2])
        .expect("write segment");
    let index = temp.open_index().expect("open wal index");
    let limits = Limits::default();

    rebuild_index(temp.store_dir(), temp.meta(), &index, &limits).expect("rebuild index");

    let segments = index.reader().list_segments(&namespace).expect("segments");
    let seg1 = segments
        .iter()
        .find(|row| row.segment_id() == segment1.header.segment_id)
        .expect("segment1 row");
    let seg2 = segments
        .iter()
        .find(|row| row.segment_id() == segment2.header.segment_id)
        .expect("segment2 row");
    let seg1_len = fs::metadata(&segment1.path)
        .expect("segment1 metadata")
        .len();

    assert!(seg1.is_sealed());
    assert_eq!(seg1.final_len(), Some(seg1_len));
    assert!(!seg2.is_sealed());
    assert_eq!(seg2.final_len(), None);
}

#[test]
fn index_replay_rejects_sealed_len_mismatch() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([15u8; 16]));
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

    let err = catch_up_index(temp.store_dir(), temp.meta(), &index, &limits)
        .expect_err("sealed length mismatch should error");
    assert!(matches!(
        err,
        WalReplayError::SealedSegmentLenMismatch { .. }
    ));
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

fn append_record(segment: &SegmentFixture, record: &VerifiedRecord) -> (u64, u32) {
    let mut file = OpenOptions::new()
        .append(true)
        .open(&segment.path)
        .expect("open segment");
    let offset = file.metadata().expect("segment metadata").len();
    let len = {
        let mut writer = FrameWriter::new(&mut file, MAX_RECORD_BYTES);
        writer.write_record(record).expect("append record")
    };
    file.sync_data().expect("sync data");
    (offset, len as u32)
}
