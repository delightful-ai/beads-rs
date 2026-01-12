//! Phase 3 tests: WAL framing + tail truncation.

mod fixtures;

use std::fs;
use std::io::{Seek, SeekFrom};

use beads_rs::daemon::wal::{FrameReader, WalReplayError, rebuild_index};
use beads_rs::{Limits, NamespaceId};

use fixtures::wal::{TempWalDir, sample_record};
use fixtures::wal_corrupt::{corrupt_frame_body, truncated_segment};

const MAX_RECORD_BYTES: usize = 1024 * 1024;

#[test]
fn phase3_wal_framing_roundtrips_records() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let record = sample_record(1);
    let fixture = temp
        .write_segment(&namespace, 1_700_000_000_000, &[record.clone()])
        .expect("write segment");

    let mut file = fs::File::open(&fixture.path).expect("open segment");
    file.seek(SeekFrom::Start(fixture.header_len))
        .expect("seek to first frame");
    let mut reader = FrameReader::new(file, MAX_RECORD_BYTES);
    let decoded = reader
        .read_next()
        .expect("read frame")
        .expect("record present");
    assert_eq!(decoded, record);
}

#[test]
fn phase3_wal_tail_truncation_repairs_partial_record() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let segment =
        truncated_segment(&temp, &namespace, 1_700_000_000_000).expect("truncated segment");
    let index = temp.open_index().expect("open wal index");
    let limits = Limits::default();

    let stats =
        rebuild_index(temp.store_dir(), temp.meta(), &index, &limits).expect("rebuild index");
    assert_eq!(stats.segments_truncated, 1);

    let meta = fs::metadata(&segment.path).expect("segment metadata");
    assert_eq!(meta.len(), segment.header_len);
}

#[test]
fn phase3_wal_mid_file_corruption_fails_fast() {
    let temp = TempWalDir::new();
    let namespace = NamespaceId::core();
    let record_a = sample_record(1);
    let record_b = sample_record(2);
    let segment = temp
        .write_segment(&namespace, 1_700_000_000_000, &[record_a, record_b])
        .expect("write segment");
    corrupt_frame_body(&segment, 0).expect("corrupt frame body");
    let original_len = fs::metadata(&segment.path).expect("segment metadata").len();
    let index = temp.open_index().expect("open wal index");
    let limits = Limits::default();

    let err = rebuild_index(temp.store_dir(), temp.meta(), &index, &limits)
        .expect_err("mid-file corruption should error");
    assert!(matches!(err, WalReplayError::MidFileCorruption { .. }));

    let meta = fs::metadata(&segment.path).expect("segment metadata");
    assert_eq!(meta.len(), original_len);
}
