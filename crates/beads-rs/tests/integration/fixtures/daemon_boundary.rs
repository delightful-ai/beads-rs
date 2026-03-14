#![allow(dead_code)]

//! Narrow bridge for product-level integration tests.
//!
//! `beads-rs` keeps only the small WAL-facing surface needed to inspect outputs
//! from the shipped daemon process in crash-recovery and replication e2e tests.
//! This is not a general-purpose daemon testkit re-export.

pub mod wal {
    pub use beads_daemon::testkit::wal::frame::encode_frame;
    pub use beads_daemon::testkit::wal::{
        EventWalError, EventWalResult, FRAME_CRC_OFFSET, FRAME_HEADER_LEN, IndexDurabilityMode,
        RecordHeader, RequestProof, SEGMENT_HEADER_PREFIX_LEN, SegmentConfig, SegmentHeader,
        SegmentWriter, SqliteWalIndex, VerifiedRecord, WAL_FORMAT_VERSION, WalIndexError,
    };
}
