//! Phase 3 tests: WAL framing + tail truncation.

mod fixtures;

use std::fs;
use std::io::{Seek, SeekFrom};

use beads_rs::daemon::wal::{FrameReader, rebuild_index};
use beads_rs::{Limits, NamespaceId};

use fixtures::wal::{TempWalDir, sample_record};
use fixtures::wal_corrupt::truncated_segment;

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
