use std::fs;
use std::sync::Arc;

use beads_rs::daemon::repl::{WalRangeError, WalRangeReader};
use beads_rs::daemon::wal::{
    IndexDurabilityMode, SegmentConfig, SegmentSyncMode, SegmentWriter, SqliteWalIndex,
    WAL_FORMAT_VERSION, rebuild_index,
};
use beads_rs::paths;
use beads_rs::{Limits, NamespaceId, Seq0, StoreMeta, StoreMetaVersions};
use rusqlite::params;
use tempfile::TempDir;

use crate::fixtures::identity;
use crate::fixtures::wal::record_for_seq;

fn setup_store(seed: u8) -> (TempDir, paths::DataDirOverride, StoreMeta, Limits) {
    let temp = TempDir::new().expect("temp dir");
    let guard = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));
    let versions = StoreMetaVersions::new(
        1,
        WAL_FORMAT_VERSION,
        1,
        1,
        StoreMetaVersions::INDEX_SCHEMA_VERSION,
    );
    let meta = identity::store_meta_with_versions(seed, 0, 1_700_000_000_000, versions);
    let store_dir = paths::store_dir(meta.store_id());
    fs::create_dir_all(&store_dir).expect("create store dir");
    (temp, guard, meta, Limits::default())
}

fn write_records(
    store_dir: &std::path::Path,
    meta: &StoreMeta,
    namespace: &NamespaceId,
    records: &[beads_rs::daemon::wal::VerifiedRecord],
    limits: &Limits,
) {
    let config = SegmentConfig::from_limits(limits).with_sync_mode(SegmentSyncMode::None);
    let mut writer =
        SegmentWriter::open(store_dir, meta, namespace, 1_700_000_000_000, config).unwrap();
    for record in records {
        writer.append(record, 1_700_000_000_000).unwrap();
    }
}

#[test]
fn wal_range_reader_returns_contiguous_frames() {
    let (_tmp, _guard, meta, limits) = setup_store(1);
    let namespace = NamespaceId::core();
    let origin = meta.replica_id;
    let record1 = record_for_seq(&meta, &namespace, origin, 1, None);
    let record2 = record_for_seq(&meta, &namespace, origin, 2, Some(record1.header().sha256));
    let store_dir = paths::store_dir(meta.store_id());
    write_records(
        &store_dir,
        &meta,
        &namespace,
        &[record1.clone(), record2.clone()],
        &limits,
    );

    let index =
        SqliteWalIndex::open(&store_dir, &meta, IndexDurabilityMode::Cache).expect("open index");
    rebuild_index(&store_dir, &meta, &index, &limits).expect("rebuild index");
    let reader = WalRangeReader::new(meta.store_id(), Arc::new(index), limits.clone());

    let frames = reader
        .read_range(&namespace, &origin, Seq0::ZERO, limits.max_frame_bytes)
        .expect("read range");
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].eid.origin_seq, record1.header().origin_seq);
    assert_eq!(frames[1].eid.origin_seq, record2.header().origin_seq);
}

#[test]
fn wal_range_reader_rejects_internal_gap() {
    let (_tmp, _guard, meta, limits) = setup_store(2);
    let namespace = NamespaceId::core();
    let origin = meta.replica_id;
    let record1 = record_for_seq(&meta, &namespace, origin, 1, None);
    let record3 = record_for_seq(&meta, &namespace, origin, 3, Some(record1.header().sha256));
    let store_dir = paths::store_dir(meta.store_id());
    write_records(&store_dir, &meta, &namespace, &[record1, record3], &limits);

    let index =
        SqliteWalIndex::open(&store_dir, &meta, IndexDurabilityMode::Cache).expect("open index");
    rebuild_index(&store_dir, &meta, &index, &limits).expect("rebuild index");
    let reader = WalRangeReader::new(meta.store_id(), Arc::new(index), limits.clone());

    let err = reader
        .read_range(&namespace, &origin, Seq0::ZERO, limits.max_frame_bytes)
        .expect_err("expected gap");
    assert!(matches!(err, WalRangeError::MissingRange { .. }));
}

#[test]
fn wal_range_reader_rejects_prev_sha_mismatch() {
    let (_tmp, _guard, meta, limits) = setup_store(3);
    let namespace = NamespaceId::core();
    let origin = meta.replica_id;
    let record1 = record_for_seq(&meta, &namespace, origin, 1, None);
    let record2 = record_for_seq(&meta, &namespace, origin, 2, Some(record1.header().sha256));
    let store_dir = paths::store_dir(meta.store_id());
    write_records(&store_dir, &meta, &namespace, &[record1, record2], &limits);

    let index =
        SqliteWalIndex::open(&store_dir, &meta, IndexDurabilityMode::Cache).expect("open index");
    rebuild_index(&store_dir, &meta, &index, &limits).expect("rebuild index");

    let db_path = store_dir.join("index").join("wal.sqlite");
    let conn = rusqlite::Connection::open(db_path).expect("open sqlite");
    let origin_blob = origin.as_uuid().as_bytes().to_vec();
    let bogus_prev = vec![9u8; 32];
    conn.execute(
        "UPDATE events SET prev_sha = ?1 WHERE namespace = ?2 AND origin_replica_id = ?3 AND origin_seq = ?4",
        params![bogus_prev, namespace.as_str(), origin_blob, 2i64],
    )
    .expect("update prev sha");

    let reader = WalRangeReader::new(meta.store_id(), Arc::new(index), limits.clone());
    let err = reader
        .read_range(&namespace, &origin, Seq0::ZERO, limits.max_frame_bytes)
        .expect_err("expected prev mismatch");
    assert!(matches!(err, WalRangeError::Corrupt { .. }));
}
