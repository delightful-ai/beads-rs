#![allow(dead_code)]

pub mod wal {
    pub use beads_daemon::testkit::wal::frame::encode_frame;
    pub use beads_daemon::testkit::wal::{
        EventWalError, EventWalResult, FRAME_CRC_OFFSET, FRAME_HEADER_LEN, IndexDurabilityMode,
        RecordHeader, RequestProof, SEGMENT_HEADER_PREFIX_LEN, SegmentConfig, SegmentHeader,
        SegmentWriter, SqliteWalIndex, VerifiedRecord, WAL_FORMAT_VERSION, WalIndexError,
    };
}
