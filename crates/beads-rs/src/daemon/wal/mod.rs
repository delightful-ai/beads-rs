//! Event WAL implementation (v0.5).

use std::io::{Read, Seek};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::core::error::details as error_details;
use crate::core::{
    CliErrorCode, ErrorCode, ErrorPayload, IntoErrorPayload, ProtocolErrorCode, Transience,
};

pub mod event_wal;
pub mod frame;
pub mod fsck;
pub mod index;
pub mod memory_index;
pub mod memory_wal;
pub mod record;
pub mod replay;
pub mod segment;

pub use event_wal::{EventWal, SegmentSnapshot};
pub use frame::{FRAME_CRC_OFFSET, FRAME_HEADER_LEN, FrameReader, FrameWriter};
pub use index::{
    ClientRequestEventIds, ClientRequestEventIdsError, ClientRequestRow, HlcRow,
    IndexDurabilityMode, IndexedRangeItem, ReplicaDurabilityRole, ReplicaDurabilityRoleError,
    ReplicaLivenessRow, SegmentRow, SqliteWalIndex, WalIndex, WalIndexError, WalIndexReader,
    WalIndexTxn, WalIndexWriter, WatermarkRow,
};
pub use memory_index::MemoryWalIndex;
#[cfg(feature = "model-testing")]
pub use memory_index::MemoryWalIndexSnapshot;
pub use record::{
    Record, RecordFlags, RecordHeader, RecordHeaderMismatch, RecordVerifyError, RequestProof,
    Unverified, UnverifiedRecord, Verified, VerifiedRecord,
};
pub use replay::{
    RecordShaMismatchInfo, ReplayMode, ReplayStats, WalReplayError, catch_up_index, rebuild_index,
};
pub use segment::{
    AppendOutcome, SEGMENT_HEADER_PREFIX_LEN, SegmentConfig, SegmentHeader, SegmentSyncMode,
    SegmentWriter, WAL_FORMAT_VERSION,
};

pub type EventWalResult<T> = Result<T, EventWalError>;

pub(crate) trait ReadSeek: Read + Seek {}

impl<T: Read + Seek> ReadSeek for T {}

pub(crate) fn open_segment_reader(path: &Path) -> EventWalResult<Box<dyn ReadSeek>> {
    if let Some(bytes) = memory_wal::read_segment_bytes(path) {
        return Ok(Box::new(std::io::Cursor::new(bytes)));
    }
    let file = std::fs::File::open(path).map_err(|source| EventWalError::Io {
        path: Some(path.to_path_buf()),
        source,
    })?;
    Ok(Box::new(file))
}

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

impl EventWalError {
    pub fn code(&self) -> ErrorCode {
        match self {
            EventWalError::RecordTooLarge { .. } => ProtocolErrorCode::WalRecordTooLarge.into(),
            EventWalError::SegmentHeaderUnsupportedVersion { .. } => {
                ProtocolErrorCode::WalFormatUnsupported.into()
            }
            EventWalError::Symlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
            EventWalError::Io { source, .. } => {
                if source.kind() == std::io::ErrorKind::PermissionDenied {
                    ProtocolErrorCode::PermissionDenied.into()
                } else {
                    CliErrorCode::IoError.into()
                }
            }
            _ => ProtocolErrorCode::WalCorrupt.into(),
        }
    }

    pub fn transience(&self) -> Transience {
        match self {
            EventWalError::Symlink { .. } => Transience::Permanent,
            EventWalError::Io { source, .. } => {
                if source.kind() == std::io::ErrorKind::PermissionDenied {
                    Transience::Permanent
                } else {
                    Transience::Retryable
                }
            }
            _ => Transience::Permanent,
        }
    }
}

impl IntoErrorPayload for EventWalError {
    fn into_error_payload(self) -> ErrorPayload {
        let message = self.to_string();
        let retryable = self.transience().is_retryable();
        let code = self.code();
        match self {
            EventWalError::RecordTooLarge {
                max_bytes,
                got_bytes,
            } => ErrorPayload::new(ProtocolErrorCode::WalRecordTooLarge.into(), message, retryable)
                .with_details(error_details::WalRecordTooLargeDetails {
                    max_wal_record_bytes: max_bytes as u64,
                    estimated_bytes: got_bytes as u64,
                }),
            EventWalError::Symlink { path } => ErrorPayload::new(
                ProtocolErrorCode::PathSymlinkRejected.into(),
                message,
                retryable,
            )
            .with_details(error_details::PathSymlinkRejectedDetails {
                path: path.display().to_string(),
            }),
            _ => ErrorPayload::new(code, message.clone(), retryable).with_details(
                error_details::WalErrorDetails {
                    message,
                },
            ),
        }
    }
}
