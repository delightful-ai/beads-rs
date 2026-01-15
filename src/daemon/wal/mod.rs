//! Event WAL implementation (v0.5).

use std::path::PathBuf;

use thiserror::Error;

pub mod event_wal;
pub mod frame;
pub mod fsck;
pub mod index;
pub mod record;
pub mod replay;
pub mod segment;

pub use event_wal::{EventWal, SegmentSnapshot};
pub use frame::{FrameReader, FrameWriter};
pub use index::{
    ClientRequestRow, HlcRow, IndexDurabilityMode, IndexedRangeItem, ReplicaLivenessRow,
    SegmentRow, SqliteWalIndex, WalIndex, WalIndexError, WalIndexReader, WalIndexTxn,
    WalIndexWriter, WatermarkRow,
};
pub use record::{Record, RecordFlags, RecordHeader, RecordHeaderMismatch};
pub use replay::{
    RecordShaMismatchInfo, ReplayMode, ReplayStats, WalReplayError, catch_up_index, rebuild_index,
};
pub use segment::{AppendOutcome, SegmentConfig, SegmentHeader, SegmentWriter};

pub type EventWalResult<T> = Result<T, EventWalError>;

#[derive(Debug, Error)]
pub enum EventWalError {
    #[error("io error at {path:?}: {source}")]
    Io {
        path: Option<PathBuf>,
        #[source]
        source: std::io::Error,
    },
    #[error("path is a symlink: {path:?}")]
    Symlink { path: PathBuf },
    #[error("record exceeds max bytes {max_bytes} (got {got_bytes})")]
    RecordTooLarge { max_bytes: usize, got_bytes: usize },
    #[error("frame magic mismatch: got {got:#x}")]
    FrameMagicMismatch { got: u32 },
    #[error("frame length invalid: {reason}")]
    FrameLengthInvalid { reason: String },
    #[error("frame crc32c mismatch: expected {expected:#x}, got {got:#x}")]
    FrameCrcMismatch { expected: u32, got: u32 },
    #[error("record header invalid: {reason}")]
    RecordHeaderInvalid { reason: String },
    #[error("segment header invalid: {reason}")]
    SegmentHeaderInvalid { reason: String },
    #[error("segment header wal format unsupported: got {got}, supported {supported}")]
    SegmentHeaderUnsupportedVersion { got: u32, supported: u32 },
    #[error("segment header magic mismatch: got {got:?}")]
    SegmentHeaderMagicMismatch { got: [u8; 5] },
    #[error("segment header crc32c mismatch: expected {expected:#x}, got {got:#x}")]
    SegmentHeaderCrcMismatch { expected: u32, got: u32 },
}
