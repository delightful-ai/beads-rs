//! Event WAL implementation (v0.5) plus legacy snapshot WAL re-exports.

use std::path::PathBuf;

use thiserror::Error;

pub mod frame;
pub mod record;
pub mod segment;

pub use crate::daemon::wal_legacy_snapshot::{Wal, WalEntry, WalError, default_wal_base_dir};

pub use frame::{FrameReader, FrameWriter};
pub use record::{Record, RecordFlags, RecordHeader};
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
    #[error("segment header magic mismatch: got {got:?}")]
    SegmentHeaderMagicMismatch { got: [u8; 5] },
    #[error("segment header crc32c mismatch: expected {expected:#x}, got {got:#x}")]
    SegmentHeaderCrcMismatch { expected: u32, got: u32 },
}
