//! WAL segment replay and SQLite index rebuild/catch-up.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use crc32c::crc32c;
use thiserror::Error;

use crate::core::error::details as error_details;
use crate::core::{
    Applied, CliErrorCode, ClientRequestId, DecodeError, Durable, EncodeError, ErrorCode,
    ErrorPayload, EventId, HeadStatus, IntoErrorPayload, Limits, NamespaceId, ProtocolErrorCode,
    ReplicaId, SegmentId, Seq0, Seq1, StoreMeta, Transience, TxnId, Watermark, WatermarkError,
    WatermarkPair, decode_event_body, decode_event_hlc_max,
};

use super::EventWalError;
use super::frame::{FRAME_HEADER_LEN, FRAME_MAGIC};
use super::record::{RecordHeaderMismatch, RecordVerifyError, UnverifiedRecord, VerifiedRecord};
use super::segment::{SEGMENT_HEADER_PREFIX_LEN, SEGMENT_MAGIC, SegmentHeader};
use super::{
    ClientRequestEventIds, HlcRow, SegmentRow, WalCursorOffset, WalIndex, WalIndexError,
    WatermarkRow,
};

#[cfg(test)]
use crate::core::sha256_bytes;
#[cfg(test)]
use std::cell::Cell;
#[cfg(test)]
use std::io::Write;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplayStats {
    pub segments_scanned: usize,
    pub records_indexed: usize,
    pub segments_truncated: usize,
    pub tail_truncations: Vec<TailTruncation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailTruncation {
    pub namespace: NamespaceId,
    pub segment_id: SegmentId,
    pub truncated_from_offset: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayMode {
    Rebuild,
    CatchUp,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayAtomicCommitStage {
    AfterRowsBeforeFrontier,
    AfterFrontierBeforeCommit,
}

#[cfg(test)]
thread_local! {
    static TEST_CRASH_STAGE: Cell<Option<ReplayAtomicCommitStage>> = const { Cell::new(None) };
}

#[cfg(test)]
thread_local! {
    static TEST_SCAN_ENTRY_COUNT: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn set_test_crash_stage(stage: Option<ReplayAtomicCommitStage>) {
    TEST_CRASH_STAGE.with(|cell| cell.set(stage));
}

#[cfg(test)]
fn reset_test_scan_entry_count() {
    TEST_SCAN_ENTRY_COUNT.with(|cell| cell.set(0));
}

#[cfg(test)]
fn test_scan_entry_count() -> usize {
    TEST_SCAN_ENTRY_COUNT.with(Cell::get)
}

#[cfg(test)]
fn increment_test_scan_entry_count() {
    TEST_SCAN_ENTRY_COUNT.with(|cell| cell.set(cell.get().saturating_add(1)));
}

#[cfg(test)]
fn maybe_inject_test_crash(stage: ReplayAtomicCommitStage) -> Result<(), WalReplayError> {
    let should_crash = TEST_CRASH_STAGE.with(|cell| cell.get() == Some(stage));
    if should_crash {
        return Err(WalReplayError::InjectedAtomicCatchUpCrash { stage });
    }
    Ok(())
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum WalReplayCorruption {
    #[error("invalid frame header (magic {magic:#x}, length {length})")]
    InvalidFrameHeader { magic: u32, length: u32 },
    #[error("frame length {length} exceeds max record bytes {max_record_bytes}")]
    FrameTooLarge {
        length: u32,
        max_record_bytes: usize,
    },
    #[error("frame length {frame_len} exceeds u32::MAX")]
    FrameLenOverflow { frame_len: u64 },
    #[error("frame crc mismatch (expected {expected}, got {actual})")]
    CrcMismatch { expected: u32, actual: u32 },
}

#[derive(Debug, Error)]
pub enum WalReplayError {
    #[error("io error at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("path is a symlink: {path:?}")]
    Symlink { path: PathBuf },
    #[error("segment header decode failed at {path:?}: {source}")]
    SegmentHeader {
        path: PathBuf,
        #[source]
        source: EventWalError,
    },
    #[error("record decode failed at {path:?}: {source}")]
    RecordDecode {
        path: PathBuf,
        #[source]
        source: EventWalError,
    },
    #[error("event body decode failed at {path:?} offset {offset}: {source}")]
    EventBodyDecode {
        path: PathBuf,
        offset: u64,
        #[source]
        source: DecodeError,
    },
    #[error("segment header mismatch at {path:?}: {reason}")]
    SegmentHeaderMismatch { path: PathBuf, reason: String },
    #[error(
        "mid-file WAL corruption at {path:?} offset {offset}: {reason}. Run `bd store fsck` to repair."
    )]
    MidFileCorruption {
        path: PathBuf,
        offset: u64,
        reason: WalReplayCorruption,
    },
    #[error("index error: {0}")]
    Index(#[from] WalIndexError),
    #[error("index offset invalid for {path:?}: offset {offset} outside [{header_len}, {len}]")]
    IndexOffsetInvalid {
        path: PathBuf,
        offset: u64,
        header_len: u64,
        len: u64,
    },
    #[error(
        "sealed segment length mismatch at {path:?}: expected {expected}, got {actual}. Run `bd store fsck` to repair."
    )]
    SealedSegmentLenMismatch {
        path: PathBuf,
        expected: u64,
        actual: u64,
    },
    #[error("non-contiguous seq for {namespace} {origin}: expected {expected}, got {got}")]
    NonContiguousSeq {
        namespace: NamespaceId,
        origin: ReplicaId,
        expected: Seq1,
        got: Seq0,
    },
    #[error("prev_sha mismatch for {namespace} {origin} seq {seq}")]
    PrevShaMismatch {
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: Seq1,
        expected_prev_sha256: [u8; 32],
        got_prev_sha256: [u8; 32],
        head_seq: Seq0,
    },
    #[error("head sha required for {namespace} {origin} seq {seq}")]
    MissingHead {
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: Seq0,
    },
    #[error("head sha must be absent for {namespace} {origin} seq {seq}")]
    UnexpectedHead {
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: Seq0,
    },
    #[error("origin_seq overflow for {namespace} {origin}")]
    OriginSeqOverflow {
        namespace: NamespaceId,
        origin: ReplicaId,
    },
    #[error("record header mismatch at {path:?} offset {offset}: {source}")]
    RecordHeaderMismatch {
        path: PathBuf,
        offset: u64,
        #[source]
        source: RecordHeaderMismatch,
    },
    #[error("{0}")]
    RecordShaMismatch(Box<RecordShaMismatchInfo>),
    #[error("{0}")]
    RecordPayloadMismatch(Box<RecordShaMismatchInfo>),
    #[error("record payload canonical encode failed at {path:?} offset {offset}: {source}")]
    RecordCanonicalEncode {
        path: PathBuf,
        offset: u64,
        #[source]
        source: EncodeError,
    },
    #[cfg(test)]
    #[error("injected atomic catch-up crash at {stage:?}")]
    InjectedAtomicCatchUpCrash { stage: ReplayAtomicCommitStage },
}

impl WalReplayError {
    pub fn code(&self) -> ErrorCode {
        match self {
            WalReplayError::Symlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
            WalReplayError::Io { source, .. } => {
                if source.kind() == std::io::ErrorKind::PermissionDenied {
                    ProtocolErrorCode::PermissionDenied.into()
                } else {
                    CliErrorCode::IoError.into()
                }
            }
            WalReplayError::SegmentHeader { source, .. } => source.code(),
            WalReplayError::SegmentHeaderMismatch { .. } => {
                ProtocolErrorCode::SegmentHeaderMismatch.into()
            }
            WalReplayError::RecordShaMismatch(_) | WalReplayError::RecordPayloadMismatch(_) => {
                ProtocolErrorCode::HashMismatch.into()
            }
            WalReplayError::RecordDecode { .. }
            | WalReplayError::EventBodyDecode { .. }
            | WalReplayError::RecordHeaderMismatch { .. }
            | WalReplayError::RecordCanonicalEncode { .. }
            | WalReplayError::MissingHead { .. }
            | WalReplayError::UnexpectedHead { .. }
            | WalReplayError::SealedSegmentLenMismatch { .. }
            | WalReplayError::MidFileCorruption { .. } => ProtocolErrorCode::WalCorrupt.into(),
            WalReplayError::NonContiguousSeq { .. } => ProtocolErrorCode::GapDetected.into(),
            WalReplayError::PrevShaMismatch { .. } => ProtocolErrorCode::PrevShaMismatch.into(),
            WalReplayError::IndexOffsetInvalid { .. }
            | WalReplayError::OriginSeqOverflow { .. } => ProtocolErrorCode::IndexCorrupt.into(),
            WalReplayError::Index(err) => err.code(),
            #[cfg(test)]
            WalReplayError::InjectedAtomicCatchUpCrash { .. } => CliErrorCode::IoError.into(),
        }
    }

    pub fn transience(&self) -> Transience {
        match self {
            WalReplayError::Symlink { .. } => Transience::Permanent,
            WalReplayError::Io { source, .. } => {
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

impl IntoErrorPayload for WalReplayError {
    fn into_error_payload(self) -> ErrorPayload {
        let message = self.to_string();
        let retryable = self.transience().is_retryable();
        let code = self.code();
        match self {
            WalReplayError::Symlink { path } => ErrorPayload::new(
                ProtocolErrorCode::PathSymlinkRejected.into(),
                message,
                retryable,
            )
            .with_details(error_details::PathSymlinkRejectedDetails {
                path: path.display().to_string(),
            }),
            WalReplayError::RecordShaMismatch(info)
            | WalReplayError::RecordPayloadMismatch(info) => {
                let info = *info;
                ErrorPayload::new(ProtocolErrorCode::HashMismatch.into(), message, retryable)
                    .with_details(error_details::HashMismatchDetails {
                        eid: error_details::EventIdDetails {
                            namespace: info.namespace,
                            origin_replica_id: info.origin,
                            origin_seq: info.seq.get(),
                        },
                        expected_sha256: hex::encode(info.expected),
                        got_sha256: hex::encode(info.got),
                    })
            }
            WalReplayError::PrevShaMismatch {
                namespace,
                origin,
                seq,
                expected_prev_sha256,
                got_prev_sha256,
                head_seq,
            } => ErrorPayload::new(
                ProtocolErrorCode::PrevShaMismatch.into(),
                message,
                retryable,
            )
            .with_details(error_details::PrevShaMismatchDetails {
                eid: error_details::EventIdDetails {
                    namespace: namespace.clone(),
                    origin_replica_id: origin,
                    origin_seq: seq.get(),
                },
                expected_prev_sha256: hex::encode(expected_prev_sha256),
                got_prev_sha256: hex::encode(got_prev_sha256),
                head_seq: head_seq.get(),
            }),
            WalReplayError::NonContiguousSeq {
                namespace,
                origin,
                expected,
                got,
            } => {
                let durable_seen = expected.prev_seq0().get();
                ErrorPayload::new(ProtocolErrorCode::GapDetected.into(), message, retryable)
                    .with_details(error_details::GapDetectedDetails {
                        namespace: namespace.clone(),
                        origin_replica_id: origin,
                        durable_seen,
                        got_seq: got.get(),
                    })
            }
            WalReplayError::IndexOffsetInvalid { .. }
            | WalReplayError::OriginSeqOverflow { .. } => {
                let reason = message.clone();
                ErrorPayload::new(ProtocolErrorCode::IndexCorrupt.into(), message, retryable)
                    .with_details(error_details::IndexCorruptDetails { reason })
            }
            WalReplayError::SegmentHeader {
                source: EventWalError::SegmentHeaderUnsupportedVersion { got, supported },
                ..
            } => ErrorPayload::new(
                ProtocolErrorCode::WalFormatUnsupported.into(),
                message,
                retryable,
            )
            .with_details(error_details::WalFormatUnsupportedDetails {
                wal_format_version: got,
                supported: vec![supported],
            }),
            WalReplayError::SegmentHeader { .. } => ErrorPayload::new(code, message, retryable),
            WalReplayError::Index(err) => err.into_payload_with_context(message, retryable),
            _ => ErrorPayload::new(code, message, retryable),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordShaMismatchInfo {
    pub namespace: NamespaceId,
    pub origin: ReplicaId,
    pub seq: Seq1,
    pub expected: [u8; 32],
    pub got: [u8; 32],
    pub path: PathBuf,
    pub offset: u64,
}

impl fmt::Display for RecordShaMismatchInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "record sha256 mismatch for {} {} seq {} at {:?} offset {}",
            self.namespace, self.origin, self.seq, self.path, self.offset
        )
    }
}

pub fn rebuild_index(
    store_dir: &Path,
    meta: &StoreMeta,
    index: &dyn WalIndex,
    limits: &Limits,
) -> Result<ReplayStats, WalReplayError> {
    replay_index(store_dir, meta, index, limits, ReplayMode::Rebuild)
}

pub fn catch_up_index(
    store_dir: &Path,
    meta: &StoreMeta,
    index: &dyn WalIndex,
    limits: &Limits,
) -> Result<ReplayStats, WalReplayError> {
    replay_index(store_dir, meta, index, limits, ReplayMode::CatchUp)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReplayStartCursor {
    offset: u64,
}

impl ReplayStartCursor {
    fn for_rebuild(segment: &SegmentDescriptor<Verified>) -> Self {
        Self {
            offset: segment.header_len,
        }
    }

    fn for_catch_up(
        segment: &SegmentDescriptor<Verified>,
        persisted: WalCursorOffset,
    ) -> Result<Self, WalReplayError> {
        let offset = persisted.get();
        if offset < segment.header_len || offset > segment.file_len {
            return Err(WalReplayError::IndexOffsetInvalid {
                path: segment.path.clone(),
                offset,
                header_len: segment.header_len,
                len: segment.file_len,
            });
        }
        Ok(Self { offset })
    }

    const fn offset(self) -> u64 {
        self.offset
    }
}

fn replay_index(
    store_dir: &Path,
    meta: &StoreMeta,
    index: &dyn WalIndex,
    limits: &Limits,
    mode: ReplayMode,
) -> Result<ReplayStats, WalReplayError> {
    let mut stats = ReplayStats::default();
    let wal_dir = store_dir.join("wal");
    let max_record_bytes = limits.policy().max_wal_record_bytes();

    let mut tracker = ReplayTracker::new(limits.policy().max_repl_gap_events().max(1));
    if mode == ReplayMode::CatchUp {
        let rows = index.reader().load_watermarks()?;
        tracker.seed_from_watermarks(rows)?;
    }
    let mut catch_up_commit = match mode {
        ReplayMode::CatchUp => Some(ReplayAtomicCatchUpCommit::new(index.writer().begin_txn()?)),
        ReplayMode::Rebuild => None,
    };

    let namespaces = match mode {
        ReplayMode::Rebuild => list_namespaces(&wal_dir)?,
        ReplayMode::CatchUp => {
            let mut namespaces: BTreeSet<NamespaceId> =
                list_namespaces(&wal_dir)?.into_iter().collect();
            for namespace in index.reader().list_segment_namespaces()? {
                namespaces.insert(namespace);
            }
            namespaces.into_iter().collect()
        }
    };
    for namespace in namespaces {
        let namespace_dir = wal_dir.join(namespace.as_str());
        cleanup_orphan_tmp_segments(store_dir, &namespace, &namespace_dir, index)?;
        let segments = list_segments(&namespace_dir)?;
        let mut segments = verify_segments(segments, meta, &namespace)?;
        segments.sort_by_key(|segment| (segment.header.created_at_ms, segment.header.segment_id));
        let last_segment_id = segments.last().map(|segment| segment.header.segment_id);

        let mut existing = BTreeMap::new();
        if mode == ReplayMode::CatchUp {
            for row in index.reader().list_segments(&namespace)? {
                existing.insert(row.segment_id(), row);
            }
        }
        let mut catch_up_segment_rows = Vec::new();

        for segment in segments {
            let is_last = Some(segment.header.segment_id) == last_segment_id;
            stats.segments_scanned += 1;
            let start_cursor = match mode {
                ReplayMode::Rebuild => ReplayStartCursor::for_rebuild(&segment),
                ReplayMode::CatchUp => match existing.get(&segment.header.segment_id) {
                    Some(row) => {
                        ReplayStartCursor::for_catch_up(&segment, row.last_indexed_offset())?
                    }
                    None => ReplayStartCursor::for_rebuild(&segment),
                },
            };

            if mode == ReplayMode::CatchUp
                && let Some(row) = existing
                    .get(&segment.header.segment_id)
                    .filter(|row| row.is_sealed())
            {
                let final_len = row
                    .final_len()
                    .expect("sealed segment rows always carry final_len");
                if segment.file_len != final_len {
                    return Err(WalReplayError::SealedSegmentLenMismatch {
                        path: segment.path.clone(),
                        expected: final_len,
                        actual: segment.file_len,
                    });
                }
            }

            let scan = match mode {
                ReplayMode::Rebuild => {
                    let mut txn = index.writer().begin_txn()?;
                    let scan = scan_segment(
                        &segment,
                        start_cursor,
                        max_record_bytes,
                        limits,
                        true,
                        |offset, record, frame_len| {
                            index_record_with_txn(
                                &mut *txn,
                                &mut tracker,
                                &namespace,
                                &segment,
                                offset,
                                record,
                                frame_len,
                                limits,
                            )
                        },
                    )?;
                    let segment_row = build_segment_row(
                        store_dir,
                        &namespace,
                        &segment,
                        is_last,
                        scan.last_indexed_offset,
                    );
                    txn.upsert_segment(&segment_row)?;
                    txn.commit()?;
                    scan
                }
                ReplayMode::CatchUp => {
                    let commit = catch_up_commit
                        .as_mut()
                        .expect("catch-up commit initialized");
                    let scan = scan_segment(
                        &segment,
                        start_cursor,
                        max_record_bytes,
                        limits,
                        true,
                        |offset, record, frame_len| {
                            index_record(
                                commit,
                                &mut tracker,
                                &namespace,
                                &segment,
                                offset,
                                record,
                                frame_len,
                                limits,
                            )
                        },
                    )?;
                    let segment_row = build_segment_row(
                        store_dir,
                        &namespace,
                        &segment,
                        is_last,
                        scan.last_indexed_offset,
                    );
                    catch_up_segment_rows.push(segment_row);
                    scan
                }
            };
            stats.records_indexed += scan.records;
            if scan.truncated {
                stats.segments_truncated += 1;
                if let Some(offset) = scan.truncated_from_offset {
                    stats.tail_truncations.push(TailTruncation {
                        namespace: namespace.clone(),
                        segment_id: segment.header.segment_id,
                        truncated_from_offset: offset,
                    });
                }
            }
        }

        if mode == ReplayMode::CatchUp {
            let commit = catch_up_commit
                .as_mut()
                .expect("catch-up commit initialized");
            commit.replace_namespace_segments(&namespace, &catch_up_segment_rows)?;
        }
    }

    let frontier_updates = collect_frontier_updates(tracker, mode)?;
    match mode {
        ReplayMode::Rebuild => {
            let mut txn = index.writer().begin_txn()?;
            apply_frontier_updates(&mut *txn, &frontier_updates)?;
            txn.commit()?;
        }
        ReplayMode::CatchUp => {
            let mut commit = catch_up_commit.expect("catch-up commit initialized");
            #[cfg(test)]
            maybe_inject_test_crash(ReplayAtomicCommitStage::AfterRowsBeforeFrontier)?;
            commit.apply_frontier_updates(&frontier_updates)?;
            #[cfg(test)]
            maybe_inject_test_crash(ReplayAtomicCommitStage::AfterFrontierBeforeCommit)?;
            commit.commit()?;
        }
    }

    Ok(stats)
}

#[derive(Clone, Debug)]
struct ReplayFrontierUpdate {
    namespace: NamespaceId,
    origin: ReplicaId,
    watermarks: WatermarkPair,
    next_seq: Seq1,
}

fn build_segment_row(
    store_dir: &Path,
    namespace: &NamespaceId,
    segment: &SegmentDescriptor<Verified>,
    is_last: bool,
    last_indexed_offset: WalCursorOffset,
) -> SegmentRow {
    if is_last {
        SegmentRow::open(
            namespace.clone(),
            segment.header.segment_id,
            segment_rel_path(store_dir, &segment.path),
            segment.header.created_at_ms,
            last_indexed_offset,
        )
    } else {
        SegmentRow::sealed(
            namespace.clone(),
            segment.header.segment_id,
            segment_rel_path(store_dir, &segment.path),
            segment.header.created_at_ms,
            last_indexed_offset,
            last_indexed_offset.get(),
        )
    }
}

fn collect_frontier_updates(
    tracker: ReplayTracker,
    mode: ReplayMode,
) -> Result<Vec<ReplayFrontierUpdate>, WalReplayError> {
    let mut updates = Vec::new();
    let update_all = mode == ReplayMode::Rebuild;
    for (namespace, origins) in tracker.origins {
        for (origin, state) in origins {
            if !update_all && !state.touched {
                continue;
            }
            let first_pending = state.first_pending_seq();
            if let Some(got) = first_pending
                && mode == ReplayMode::CatchUp
            {
                return Err(WalReplayError::NonContiguousSeq {
                    namespace: namespace.clone(),
                    origin,
                    expected: state.contiguous_seq.next(),
                    got: Seq0::new(got.get()),
                });
            }

            let head = state.head_sha;
            let seq = state.contiguous_seq;
            if seq.get() > 0 && head.is_none() {
                return Err(WalReplayError::MissingHead {
                    namespace: namespace.clone(),
                    origin,
                    seq,
                });
            }
            let next_seq = if first_pending.is_some() {
                // Preserve gap-fill ability during rebuild: don't advance past the contiguous tip.
                state.contiguous_seq.get().checked_add(1)
            } else {
                state.max_seq.get().checked_add(1)
            }
            .ok_or_else(|| WalReplayError::OriginSeqOverflow {
                namespace: namespace.clone(),
                origin,
            })?;
            let next_seq =
                Seq1::from_u64(next_seq).ok_or_else(|| WalReplayError::OriginSeqOverflow {
                    namespace: namespace.clone(),
                    origin,
                })?;

            let head_status = match head {
                Some(sha) => HeadStatus::Known(sha),
                None => HeadStatus::Genesis,
            };
            let applied = Watermark::new(seq, head_status).map_err(|err| match err {
                WatermarkError::MissingHead { .. } => WalReplayError::MissingHead {
                    namespace: namespace.clone(),
                    origin,
                    seq,
                },
                WatermarkError::UnexpectedHead { .. } => WalReplayError::UnexpectedHead {
                    namespace: namespace.clone(),
                    origin,
                    seq,
                },
                other => {
                    WalReplayError::Index(WalIndexError::WatermarkRowDecode(other.to_string()))
                }
            })?;
            let durable = Watermark::new(seq, head_status).map_err(|err| match err {
                WatermarkError::MissingHead { .. } => WalReplayError::MissingHead {
                    namespace: namespace.clone(),
                    origin,
                    seq,
                },
                WatermarkError::UnexpectedHead { .. } => WalReplayError::UnexpectedHead {
                    namespace: namespace.clone(),
                    origin,
                    seq,
                },
                other => {
                    WalReplayError::Index(WalIndexError::WatermarkRowDecode(other.to_string()))
                }
            })?;
            let applied: Watermark<Applied> = applied;
            let durable: Watermark<Durable> = durable;
            let watermarks = WatermarkPair::new(applied, durable).map_err(|err| {
                WalReplayError::Index(WalIndexError::WatermarkRowDecode(err.to_string()))
            })?;
            updates.push(ReplayFrontierUpdate {
                namespace: namespace.clone(),
                origin,
                watermarks,
                next_seq,
            });
        }
    }
    Ok(updates)
}

fn apply_frontier_updates(
    txn: &mut dyn super::WalIndexTxn,
    updates: &[ReplayFrontierUpdate],
) -> Result<(), WalReplayError> {
    for update in updates {
        txn.update_watermark(&update.namespace, &update.origin, update.watermarks)?;
        txn.set_next_origin_seq(&update.namespace, &update.origin, update.next_seq)?;
    }
    Ok(())
}

struct ReplayAtomicCatchUpCommit {
    txn: Box<dyn super::WalIndexTxn>,
}

impl ReplayAtomicCatchUpCommit {
    fn new(txn: Box<dyn super::WalIndexTxn>) -> Self {
        Self { txn }
    }

    #[allow(clippy::too_many_arguments)]
    fn record_event(
        &mut self,
        ns: &NamespaceId,
        eid: &EventId,
        sha: [u8; 32],
        prev_sha: Option<[u8; 32]>,
        segment_id: SegmentId,
        offset: u64,
        len: u32,
        event_time_ms: u64,
        txn_id: TxnId,
        client_request_id: Option<ClientRequestId>,
    ) -> Result<(), WalReplayError> {
        self.txn.record_event(
            ns,
            eid,
            sha,
            prev_sha,
            segment_id,
            offset,
            len,
            event_time_ms,
            txn_id,
            client_request_id,
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn upsert_client_request(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        client_request_id: ClientRequestId,
        request_sha256: [u8; 32],
        txn_id: TxnId,
        event_ids: &ClientRequestEventIds,
        created_at_ms: u64,
    ) -> Result<(), WalReplayError> {
        self.txn.upsert_client_request(
            ns,
            origin,
            client_request_id,
            request_sha256,
            txn_id,
            event_ids,
            created_at_ms,
        )?;
        Ok(())
    }

    fn update_hlc(&mut self, hlc: &HlcRow) -> Result<(), WalReplayError> {
        self.txn.update_hlc(hlc)?;
        Ok(())
    }

    fn replace_namespace_segments(
        &mut self,
        namespace: &NamespaceId,
        segments: &[SegmentRow],
    ) -> Result<(), WalReplayError> {
        self.txn.replace_namespace_segments(namespace, segments)?;
        Ok(())
    }

    fn apply_frontier_updates(
        &mut self,
        updates: &[ReplayFrontierUpdate],
    ) -> Result<(), WalReplayError> {
        apply_frontier_updates(&mut *self.txn, updates)
    }

    fn commit(self) -> Result<(), WalReplayError> {
        self.txn.commit()?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn index_record(
    commit: &mut ReplayAtomicCatchUpCommit,
    tracker: &mut ReplayTracker,
    namespace: &NamespaceId,
    segment: &SegmentDescriptor<Verified>,
    offset: u64,
    record: &VerifiedRecord,
    frame_len: u32,
    limits: &Limits,
) -> Result<(), WalReplayError> {
    let header = record.header();
    let origin_seq = header.origin_seq;
    let event_id = EventId::new(header.origin_replica_id, namespace.clone(), origin_seq);

    tracker.observe_record(
        namespace,
        header.origin_replica_id,
        header.origin_seq,
        header.sha256,
        header.prev_sha256,
    )?;

    commit.record_event(
        namespace,
        &event_id,
        header.sha256,
        header.prev_sha256,
        segment.header.segment_id,
        offset,
        frame_len,
        header.event_time_ms,
        header.txn_id,
        header.client_request_id(),
    )?;

    if let (Some(client_request_id), Some(request_sha256)) =
        (header.client_request_id(), header.request_sha256())
    {
        let event_ids = ClientRequestEventIds::single(event_id.clone());
        commit.upsert_client_request(
            namespace,
            &header.origin_replica_id,
            client_request_id,
            request_sha256,
            header.txn_id,
            &event_ids,
            header.event_time_ms,
        )?;
    }

    if let Some(hlc_max) =
        decode_event_hlc_max(record.payload_bytes(), limits).map_err(|source| {
            WalReplayError::EventBodyDecode {
                path: segment.path.clone(),
                offset,
                source,
            }
        })?
    {
        commit.update_hlc(&HlcRow {
            actor_id: hlc_max.actor_id,
            last_physical_ms: hlc_max.physical_ms,
            last_logical: hlc_max.logical,
        })?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn index_record_with_txn(
    txn: &mut dyn super::WalIndexTxn,
    tracker: &mut ReplayTracker,
    namespace: &NamespaceId,
    segment: &SegmentDescriptor<Verified>,
    offset: u64,
    record: &VerifiedRecord,
    frame_len: u32,
    limits: &Limits,
) -> Result<(), WalReplayError> {
    let header = record.header();
    let origin_seq = header.origin_seq;
    let event_id = EventId::new(header.origin_replica_id, namespace.clone(), origin_seq);

    tracker.observe_record(
        namespace,
        header.origin_replica_id,
        header.origin_seq,
        header.sha256,
        header.prev_sha256,
    )?;

    txn.record_event(
        namespace,
        &event_id,
        header.sha256,
        header.prev_sha256,
        segment.header.segment_id,
        offset,
        frame_len,
        header.event_time_ms,
        header.txn_id,
        header.client_request_id(),
    )?;

    if let (Some(client_request_id), Some(request_sha256)) =
        (header.client_request_id(), header.request_sha256())
    {
        let event_ids = ClientRequestEventIds::single(event_id.clone());
        txn.upsert_client_request(
            namespace,
            &header.origin_replica_id,
            client_request_id,
            request_sha256,
            header.txn_id,
            &event_ids,
            header.event_time_ms,
        )?;
    }

    if let Some(hlc_max) =
        decode_event_hlc_max(record.payload_bytes(), limits).map_err(|source| {
            WalReplayError::EventBodyDecode {
                path: segment.path.clone(),
                offset,
                source,
            }
        })?
    {
        txn.update_hlc(&HlcRow {
            actor_id: hlc_max.actor_id,
            last_physical_ms: hlc_max.physical_ms,
            last_logical: hlc_max.logical,
        })?;
    }

    Ok(())
}

fn list_namespaces(wal_dir: &Path) -> Result<Vec<NamespaceId>, WalReplayError> {
    reject_symlink(wal_dir)?;
    let entries = match fs::read_dir(wal_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(WalReplayError::Io {
                path: wal_dir.to_path_buf(),
                source: err,
            });
        }
    };

    let mut namespaces = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| WalReplayError::Io {
            path: wal_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let entry_type = entry.file_type().map_err(|source| WalReplayError::Io {
            path: wal_dir.to_path_buf(),
            source,
        })?;
        if entry_type.is_symlink() {
            return Err(WalReplayError::Symlink { path });
        }
        if !entry_type.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        let namespace =
            NamespaceId::parse(name).map_err(|err| WalReplayError::SegmentHeaderMismatch {
                path,
                reason: err.to_string(),
            })?;
        namespaces.push(namespace);
    }
    namespaces.sort();
    Ok(namespaces)
}

fn list_segments(dir: &Path) -> Result<Vec<SegmentDescriptor<Unverified>>, WalReplayError> {
    reject_symlink(dir)?;
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(WalReplayError::Io {
                path: dir.to_path_buf(),
                source: err,
            });
        }
    };

    let mut segments = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| WalReplayError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "wal") {
            continue;
        }
        segments.push(SegmentDescriptor::load(path)?);
    }
    Ok(segments)
}

fn cleanup_orphan_tmp_segments(
    store_dir: &Path,
    namespace: &NamespaceId,
    namespace_dir: &Path,
    index: &dyn WalIndex,
) -> Result<(), WalReplayError> {
    reject_symlink(namespace_dir)?;
    let referenced: BTreeSet<PathBuf> = match index.reader().list_segments(namespace) {
        Ok(rows) => rows
            .into_iter()
            .map(|row| row.segment_path().to_path_buf())
            .collect(),
        Err(err) => {
            tracing::warn!(
                namespace = %namespace,
                "failed to list wal segments for tmp cleanup: {err}"
            );
            return Ok(());
        }
    };

    let entries = match fs::read_dir(namespace_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(WalReplayError::Io {
                path: namespace_dir.to_path_buf(),
                source: err,
            });
        }
    };

    for entry in entries {
        let entry = entry.map_err(|source| WalReplayError::Io {
            path: namespace_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.ends_with(".wal.tmp") {
            continue;
        }
        let rel_path = segment_rel_path(store_dir, &path);
        if referenced.contains(&rel_path) {
            continue;
        }
        match fs::remove_file(&path) {
            Ok(()) => {
                tracing::info!(
                    namespace = %namespace,
                    path = %rel_path.display(),
                    "deleted orphan wal temp segment"
                );
            }
            Err(err) => {
                tracing::warn!(
                    namespace = %namespace,
                    path = %rel_path.display(),
                    "failed to delete orphan wal temp segment: {err}"
                );
            }
        }
    }

    Ok(())
}

fn reject_symlink(path: &Path) -> Result<(), WalReplayError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(WalReplayError::Symlink {
            path: path.to_path_buf(),
        }),
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(WalReplayError::Io {
            path: path.to_path_buf(),
            source: err,
        }),
    }
}

fn verify_segments(
    segments: Vec<SegmentDescriptor<Unverified>>,
    meta: &StoreMeta,
    namespace: &NamespaceId,
) -> Result<Vec<SegmentDescriptor<Verified>>, WalReplayError> {
    segments
        .into_iter()
        .map(|segment| segment.verify(meta, namespace))
        .collect()
}

fn segment_rel_path(store_dir: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(store_dir).unwrap_or(path).to_path_buf()
}

#[derive(Clone, Debug)]
struct SegmentDescriptor<State> {
    path: PathBuf,
    header: SegmentHeader,
    header_len: u64,
    file_len: u64,
    _state: PhantomData<State>,
}

#[derive(Clone, Debug)]
struct Unverified;

#[derive(Clone, Debug)]
struct Verified;

impl SegmentDescriptor<Unverified> {
    fn load(path: PathBuf) -> Result<Self, WalReplayError> {
        let (header, header_len) = read_segment_header(&path)?;
        let file_len = fs::metadata(&path)
            .map_err(|source| WalReplayError::Io {
                path: path.clone(),
                source,
            })?
            .len();
        if header_len > file_len {
            return Err(WalReplayError::SegmentHeaderMismatch {
                path,
                reason: "segment header length exceeds file length".to_string(),
            });
        }
        Ok(Self {
            path,
            header,
            header_len,
            file_len,
            _state: PhantomData,
        })
    }

    fn verify(
        self,
        meta: &StoreMeta,
        namespace: &NamespaceId,
    ) -> Result<SegmentDescriptor<Verified>, WalReplayError> {
        let header = &self.header;
        if header.store_id != meta.store_id() {
            return Err(WalReplayError::SegmentHeaderMismatch {
                path: self.path.clone(),
                reason: format!(
                    "store_id mismatch (expected {}, got {})",
                    meta.store_id(),
                    header.store_id
                ),
            });
        }
        if header.store_epoch != meta.store_epoch() {
            return Err(WalReplayError::SegmentHeaderMismatch {
                path: self.path.clone(),
                reason: format!(
                    "store_epoch mismatch (expected {}, got {})",
                    meta.store_epoch(),
                    header.store_epoch
                ),
            });
        }
        if &header.namespace != namespace {
            return Err(WalReplayError::SegmentHeaderMismatch {
                path: self.path.clone(),
                reason: format!(
                    "namespace mismatch (expected {}, got {})",
                    namespace, header.namespace
                ),
            });
        }
        if header.wal_format_version != meta.wal_format_version {
            return Err(WalReplayError::SegmentHeaderMismatch {
                path: self.path.clone(),
                reason: format!(
                    "wal_format_version mismatch (expected {}, got {})",
                    meta.wal_format_version, header.wal_format_version
                ),
            });
        }

        Ok(SegmentDescriptor {
            path: self.path,
            header: self.header,
            header_len: self.header_len,
            file_len: self.file_len,
            _state: PhantomData,
        })
    }
}

struct SegmentScanOutcome {
    last_indexed_offset: WalCursorOffset,
    records: usize,
    truncated: bool,
    truncated_from_offset: Option<u64>,
}

fn scan_segment<F>(
    segment: &SegmentDescriptor<Verified>,
    start: ReplayStartCursor,
    max_record_bytes: usize,
    limits: &Limits,
    repair_tail: bool,
    mut on_record: F,
) -> Result<SegmentScanOutcome, WalReplayError>
where
    F: FnMut(u64, &VerifiedRecord, u32) -> Result<(), WalReplayError>,
{
    #[cfg(test)]
    increment_test_scan_entry_count();

    let mut file = OpenOptions::new()
        .read(true)
        .write(repair_tail)
        .open(&segment.path)
        .map_err(|source| WalReplayError::Io {
            path: segment.path.clone(),
            source,
        })?;
    file.seek(SeekFrom::Start(start.offset()))
        .map_err(|source| WalReplayError::Io {
            path: segment.path.clone(),
            source,
        })?;

    let mut offset = start.offset();
    let mut records = 0usize;
    let mut truncated = false;
    let mut truncated_from_offset = None;

    while offset < segment.file_len {
        let remaining = segment.file_len - offset;
        if remaining < FRAME_HEADER_LEN as u64 {
            truncated = true;
            truncated_from_offset = Some(offset);
            break;
        }

        let mut header = [0u8; FRAME_HEADER_LEN];
        if let Err(source) = file.read_exact(&mut header) {
            return Err(WalReplayError::Io {
                path: segment.path.clone(),
                source,
            });
        }

        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let length = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let expected_crc = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

        let frame_len = FRAME_HEADER_LEN as u64 + length as u64;
        if frame_len > remaining {
            truncated = true;
            truncated_from_offset = Some(offset);
            break;
        }

        let mid_file = |reason| {
            Err(WalReplayError::MidFileCorruption {
                path: segment.path.clone(),
                offset,
                reason,
            })
        };

        if magic != FRAME_MAGIC || length == 0 {
            return mid_file(WalReplayCorruption::InvalidFrameHeader { magic, length });
        }
        if length as usize > max_record_bytes {
            return mid_file(WalReplayCorruption::FrameTooLarge {
                length,
                max_record_bytes,
            });
        }
        if frame_len > u32::MAX as u64 {
            return mid_file(WalReplayCorruption::FrameLenOverflow { frame_len });
        }

        let mut body = vec![0u8; length as usize];
        if let Err(source) = file.read_exact(&mut body) {
            return Err(WalReplayError::Io {
                path: segment.path.clone(),
                source,
            });
        }

        let actual_crc = crc32c(&body);
        if actual_crc != expected_crc {
            if offset.saturating_add(frame_len) == segment.file_len {
                truncated = true;
                truncated_from_offset = Some(offset);
                break;
            }
            return mid_file(WalReplayCorruption::CrcMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        let record = UnverifiedRecord::decode_body(&body).map_err(|source| {
            WalReplayError::RecordDecode {
                path: segment.path.clone(),
                source,
            }
        })?;
        let (_, event_body) =
            decode_event_body(record.payload_bytes(), limits).map_err(|source| {
                WalReplayError::EventBodyDecode {
                    path: segment.path.clone(),
                    offset,
                    source,
                }
            })?;
        let header = record.header().clone();
        let record = record
            .verify_with_event_body(event_body)
            .map_err(|err| match err {
                RecordVerifyError::HeaderMismatch(source) => WalReplayError::RecordHeaderMismatch {
                    path: segment.path.clone(),
                    offset,
                    source,
                },
                RecordVerifyError::PayloadMismatch { expected, got } => {
                    WalReplayError::RecordPayloadMismatch(Box::new(RecordShaMismatchInfo {
                        namespace: segment.header.namespace.clone(),
                        origin: header.origin_replica_id,
                        seq: header.origin_seq,
                        expected,
                        got,
                        path: segment.path.clone(),
                        offset,
                    }))
                }
                RecordVerifyError::ShaMismatch { expected, got } => {
                    WalReplayError::RecordShaMismatch(Box::new(RecordShaMismatchInfo {
                        namespace: segment.header.namespace.clone(),
                        origin: header.origin_replica_id,
                        seq: header.origin_seq,
                        expected,
                        got,
                        path: segment.path.clone(),
                        offset,
                    }))
                }
                RecordVerifyError::Encode(source) => WalReplayError::RecordCanonicalEncode {
                    path: segment.path.clone(),
                    offset,
                    source,
                },
            })?;
        on_record(offset, &record, frame_len as u32)?;

        records += 1;
        offset = offset.saturating_add(frame_len);
    }

    if truncated && repair_tail {
        truncate_tail(&mut file, &segment.path, offset)?;
    }

    Ok(SegmentScanOutcome {
        last_indexed_offset: WalCursorOffset::new(offset),
        records,
        truncated,
        truncated_from_offset,
    })
}

fn truncate_tail(file: &mut std::fs::File, path: &Path, len: u64) -> Result<(), WalReplayError> {
    file.set_len(len).map_err(|source| WalReplayError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    file.sync_all().map_err(|source| WalReplayError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn read_segment_header(path: &Path) -> Result<(SegmentHeader, u64), WalReplayError> {
    let mut file =
        OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|source| WalReplayError::Io {
                path: path.to_path_buf(),
                source,
            })?;

    let mut prefix = [0u8; SEGMENT_HEADER_PREFIX_LEN];
    file.read_exact(&mut prefix)
        .map_err(|source| WalReplayError::Io {
            path: path.to_path_buf(),
            source,
        })?;

    if &prefix[..SEGMENT_MAGIC.len()] != SEGMENT_MAGIC {
        return Err(WalReplayError::SegmentHeaderMismatch {
            path: path.to_path_buf(),
            reason: "segment magic mismatch".to_string(),
        });
    }

    let header_len = u32::from_le_bytes([
        prefix[SEGMENT_MAGIC.len() + 4],
        prefix[SEGMENT_MAGIC.len() + 5],
        prefix[SEGMENT_MAGIC.len() + 6],
        prefix[SEGMENT_MAGIC.len() + 7],
    ]) as usize;

    if header_len < prefix.len() {
        return Err(WalReplayError::SegmentHeaderMismatch {
            path: path.to_path_buf(),
            reason: "segment header length smaller than prefix".to_string(),
        });
    }
    let file_len = file
        .metadata()
        .map_err(|source| WalReplayError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .len();
    if header_len as u64 > file_len {
        return Err(WalReplayError::SegmentHeaderMismatch {
            path: path.to_path_buf(),
            reason: "segment header length exceeds file length".to_string(),
        });
    }

    let mut header_bytes = vec![0u8; header_len];
    header_bytes[..prefix.len()].copy_from_slice(&prefix);
    file.read_exact(&mut header_bytes[prefix.len()..])
        .map_err(|source| WalReplayError::Io {
            path: path.to_path_buf(),
            source,
        })?;

    let header =
        SegmentHeader::decode(&header_bytes).map_err(|source| WalReplayError::SegmentHeader {
            path: path.to_path_buf(),
            source,
        })?;
    Ok((header, header_len as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::core::{
        ActorId, EventBody, EventKindV1, HlcMax, Limits, NamespaceId, ReplicaId, SegmentId, Seq0,
        Seq1, StoreEpoch, StoreId, StoreIdentity, StoreMeta, StoreMetaVersions, TxnDeltaV1, TxnId,
        TxnV1, encode_event_body_canonical,
    };
    use crate::wal::SegmentConfig;
    use crate::wal::record::{RecordHeader, RequestProof};
    use crate::wal::segment::SegmentWriter;
    use crate::wal::{IndexDurabilityMode, IndexedRangeItem, SqliteWalIndex, WatermarkRow};

    fn test_meta(store_id: StoreId, store_epoch: StoreEpoch) -> StoreMeta {
        let identity = StoreIdentity::new(store_id, store_epoch);
        let versions = StoreMetaVersions::new(1, 2, 3, 4, 5);
        StoreMeta::new(
            identity,
            ReplicaId::new(Uuid::from_bytes([9u8; 16])),
            versions,
            1_700_000_000_000,
        )
    }

    fn test_event_body(
        store: StoreIdentity,
        namespace: NamespaceId,
        origin: ReplicaId,
    ) -> EventBody {
        let event_time_ms = 1_700_000_000_100;
        EventBody {
            envelope_v: 1,
            store,
            namespace,
            origin_replica_id: origin,
            origin_seq: Seq1::from_u64(1).expect("seq1"),
            event_time_ms,
            txn_id: TxnId::new(Uuid::from_bytes([2u8; 16])),
            client_request_id: None,
            trace_id: None,
            kind: EventKindV1::TxnV1(TxnV1 {
                delta: TxnDeltaV1::new(),
                hlc_max: HlcMax {
                    actor_id: ActorId::new("alice".to_string()).unwrap(),
                    physical_ms: event_time_ms,
                    logical: 1,
                },
            }),
        }
    }

    struct CatchUpAtomicFixture {
        temp: TempDir,
        meta: StoreMeta,
        index: SqliteWalIndex,
        limits: Limits,
        namespace: NamespaceId,
        origin: ReplicaId,
        touched_segment_id: SegmentId,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct ReplayStateSnapshot {
        rows: Vec<IndexedRangeItem>,
        frontier: Option<WatermarkRow>,
        next_origin_seq: Seq1,
        segment_offsets: BTreeMap<SegmentId, u64>,
    }

    #[allow(clippy::too_many_arguments)]
    fn make_replay_record(
        store: StoreIdentity,
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: Seq1,
        prev_sha256: Option<[u8; 32]>,
        event_time_ms: u64,
        txn_byte: u8,
        limits: &Limits,
    ) -> VerifiedRecord {
        let body = EventBody {
            envelope_v: 1,
            store,
            namespace,
            origin_replica_id: origin,
            origin_seq: seq,
            event_time_ms,
            txn_id: TxnId::new(Uuid::from_bytes([txn_byte; 16])),
            client_request_id: None,
            trace_id: None,
            kind: EventKindV1::TxnV1(TxnV1 {
                delta: TxnDeltaV1::new(),
                hlc_max: HlcMax {
                    actor_id: ActorId::new("alice".to_string()).unwrap(),
                    physical_ms: event_time_ms,
                    logical: 1,
                },
            }),
        };
        let body = body.into_validated(limits).expect("validated");
        let payload = encode_event_body_canonical(body.as_ref()).expect("payload");
        let sha256 = sha256_bytes(payload.as_ref()).0;
        let header = RecordHeader {
            origin_replica_id: origin,
            origin_seq: seq,
            event_time_ms,
            txn_id: TxnId::new(Uuid::from_bytes([txn_byte; 16])),
            request_proof: RequestProof::None,
            sha256,
            prev_sha256,
        };
        VerifiedRecord::new(header, payload, body).expect("verified record")
    }

    fn append_record(
        writer: &mut SegmentWriter,
        store: StoreIdentity,
        namespace: &NamespaceId,
        origin: ReplicaId,
        seq: Seq1,
        prev_sha256: Option<[u8; 32]>,
        event_time_ms: u64,
        txn_byte: u8,
        limits: &Limits,
    ) -> [u8; 32] {
        let record = make_replay_record(
            store,
            namespace.clone(),
            origin,
            seq,
            prev_sha256,
            event_time_ms,
            txn_byte,
            limits,
        );
        let sha256 = record.header().sha256;
        writer
            .append(&record, event_time_ms)
            .expect("append replay record");
        sha256
    }

    fn seed_catch_up_atomic_fixture() -> CatchUpAtomicFixture {
        let temp = TempDir::new().unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([41u8; 16]));
        let store_epoch = StoreEpoch::new(1);
        let meta = test_meta(store_id, store_epoch);
        let limits = Limits::default();
        let index = SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache).unwrap();
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([42u8; 16]));
        let store = StoreIdentity::new(store_id, store_epoch);

        let mut writer = SegmentWriter::open(
            temp.path(),
            &meta,
            &namespace,
            10,
            SegmentConfig::from_limits(&limits),
        )
        .unwrap();

        let sha1 = append_record(
            &mut writer,
            store,
            &namespace,
            origin,
            Seq1::from_u64(1).expect("seq1"),
            None,
            11,
            1,
            &limits,
        );
        let sha2 = append_record(
            &mut writer,
            store,
            &namespace,
            origin,
            Seq1::from_u64(2).expect("seq2"),
            Some(sha1),
            12,
            2,
            &limits,
        );
        let touched_segment_id = writer.current_segment_id();

        rebuild_index(temp.path(), &meta, &index, &limits).expect("seed rebuild");

        append_record(
            &mut writer,
            store,
            &namespace,
            origin,
            Seq1::from_u64(3).expect("seq3"),
            Some(sha2),
            13,
            3,
            &limits,
        );

        CatchUpAtomicFixture {
            temp,
            meta,
            index,
            limits,
            namespace,
            origin,
            touched_segment_id,
        }
    }

    fn seed_catch_up_gap_fixture() -> CatchUpAtomicFixture {
        let temp = TempDir::new().unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([51u8; 16]));
        let store_epoch = StoreEpoch::new(1);
        let meta = test_meta(store_id, store_epoch);
        let limits = Limits::default();
        let index = SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache).unwrap();
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([52u8; 16]));
        let store = StoreIdentity::new(store_id, store_epoch);

        let mut writer = SegmentWriter::open(
            temp.path(),
            &meta,
            &namespace,
            10,
            SegmentConfig::from_limits(&limits),
        )
        .unwrap();

        let sha1 = append_record(
            &mut writer,
            store,
            &namespace,
            origin,
            Seq1::from_u64(1).expect("seq1"),
            None,
            11,
            1,
            &limits,
        );
        let sha2 = append_record(
            &mut writer,
            store,
            &namespace,
            origin,
            Seq1::from_u64(2).expect("seq2"),
            Some(sha1),
            12,
            2,
            &limits,
        );
        let touched_segment_id = writer.current_segment_id();

        rebuild_index(temp.path(), &meta, &index, &limits).expect("seed rebuild");

        append_record(
            &mut writer,
            store,
            &namespace,
            origin,
            Seq1::from_u64(4).expect("seq4"),
            Some(sha2),
            14,
            4,
            &limits,
        );

        CatchUpAtomicFixture {
            temp,
            meta,
            index,
            limits,
            namespace,
            origin,
            touched_segment_id,
        }
    }

    fn capture_snapshot(
        index: &dyn WalIndex,
        namespace: &NamespaceId,
        origin: ReplicaId,
    ) -> ReplayStateSnapshot {
        let rows = index
            .reader()
            .iter_from(namespace, &origin, Seq0::ZERO, 1_000_000)
            .expect("iter rows");
        let frontier = index
            .reader()
            .load_watermarks()
            .expect("load watermarks")
            .into_iter()
            .find(|row| row.namespace == *namespace && row.origin == origin);
        let next_origin_seq = {
            let mut txn = index.writer().begin_txn().expect("begin txn");
            let next = txn
                .next_origin_seq(namespace, &origin)
                .expect("next origin seq");
            txn.rollback().expect("rollback probe");
            next
        };
        let segment_offsets = index
            .reader()
            .list_segments(namespace)
            .expect("list segments")
            .into_iter()
            .map(|row| (row.segment_id(), row.last_indexed_offset().get()))
            .collect();
        ReplayStateSnapshot {
            rows,
            frontier,
            next_origin_seq,
            segment_offsets,
        }
    }

    fn filesystem_segment_ids(
        store_dir: &Path,
        meta: &StoreMeta,
        namespace: &NamespaceId,
    ) -> std::collections::BTreeSet<SegmentId> {
        let namespace_dir = store_dir.join("wal").join(namespace.as_str());
        let segments = list_segments(&namespace_dir).expect("list filesystem segments");
        let verified = verify_segments(segments, meta, namespace).expect("verify segments");
        verified
            .into_iter()
            .map(|segment| segment.header.segment_id)
            .collect()
    }

    fn indexed_segment_ids(
        index: &dyn WalIndex,
        namespace: &NamespaceId,
    ) -> std::collections::BTreeSet<SegmentId> {
        index
            .reader()
            .list_segments(namespace)
            .expect("list indexed segments")
            .into_iter()
            .map(|row| row.segment_id())
            .collect()
    }

    fn load_verified_segment_for_row(
        store_dir: &Path,
        meta: &StoreMeta,
        row: &SegmentRow,
    ) -> SegmentDescriptor<Verified> {
        let path = if row.segment_path().is_absolute() {
            row.segment_path().to_path_buf()
        } else {
            store_dir.join(row.segment_path())
        };
        SegmentDescriptor::<Unverified>::load(path)
            .expect("load segment descriptor")
            .verify(meta, row.namespace())
            .expect("verify segment descriptor")
    }

    fn segment_row_with_offset(
        row: &SegmentRow,
        last_indexed_offset: WalCursorOffset,
    ) -> SegmentRow {
        match row {
            SegmentRow::Open {
                namespace,
                segment_id,
                segment_path,
                created_at_ms,
                ..
            } => SegmentRow::open(
                namespace.clone(),
                *segment_id,
                segment_path.clone(),
                *created_at_ms,
                last_indexed_offset,
            ),
            SegmentRow::Sealed {
                namespace,
                segment_id,
                segment_path,
                created_at_ms,
                final_len,
                ..
            } => SegmentRow::sealed(
                namespace.clone(),
                *segment_id,
                segment_path.clone(),
                *created_at_ms,
                last_indexed_offset,
                *final_len,
            ),
        }
    }

    fn assert_recovered_after_retry(fixture: &CatchUpAtomicFixture, pre: &ReplayStateSnapshot) {
        let post = capture_snapshot(&fixture.index, &fixture.namespace, fixture.origin);
        assert!(
            post.rows
                .iter()
                .any(|row| row.event_id.origin_seq == Seq1::from_u64(3).expect("seq3")),
            "seq 3 should appear after retry"
        );
        let frontier = post.frontier.expect("frontier row after retry");
        assert_eq!(frontier.applied_seq(), 3);
        assert_eq!(frontier.durable_seq(), 3);
        assert_eq!(post.next_origin_seq.get(), 4);

        let pre_offset = pre
            .segment_offsets
            .get(&fixture.touched_segment_id)
            .copied()
            .expect("pre touched segment offset");
        let post_offset = post
            .segment_offsets
            .get(&fixture.touched_segment_id)
            .copied()
            .expect("post touched segment offset");
        assert!(
            post_offset > pre_offset,
            "touched segment last_indexed_offset should advance"
        );
    }

    struct CrashStageGuard;

    impl CrashStageGuard {
        fn install(stage: ReplayAtomicCommitStage) -> Self {
            set_test_crash_stage(Some(stage));
            Self
        }
    }

    impl Drop for CrashStageGuard {
        fn drop(&mut self) {
            set_test_crash_stage(None);
        }
    }

    #[cfg(unix)]
    #[test]
    fn list_namespaces_rejects_symlinked_namespace_dir() {
        let temp = TempDir::new().unwrap();
        let wal_dir = temp.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let target = temp.path().join("ns-target");
        std::fs::create_dir_all(&target).unwrap();
        let ns_dir = wal_dir.join("core");
        symlink(&target, &ns_dir).unwrap();

        let err = list_namespaces(&wal_dir).unwrap_err();
        assert!(matches!(err, WalReplayError::Symlink { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn list_segments_rejects_symlinked_dir() {
        let temp = TempDir::new().unwrap();
        let wal_dir = temp.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let target = temp.path().join("seg-target");
        std::fs::create_dir_all(&target).unwrap();
        let ns_dir = wal_dir.join("core");
        symlink(&target, &ns_dir).unwrap();

        let err = list_segments(&ns_dir).unwrap_err();
        assert!(matches!(err, WalReplayError::Symlink { .. }));
    }

    #[test]
    fn scan_segment_detects_sha_mismatch() {
        let temp = TempDir::new().unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
        let store_epoch = StoreEpoch::new(1);
        let meta = test_meta(store_id, store_epoch);
        let namespace = NamespaceId::core();
        let limits = Limits::default();
        let mut writer = SegmentWriter::open(
            temp.path(),
            &meta,
            &namespace,
            10,
            SegmentConfig::from_limits(&limits),
        )
        .unwrap();

        let origin = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
        let body = test_event_body(
            StoreIdentity::new(store_id, store_epoch),
            namespace.clone(),
            origin,
        );
        let body = body.into_validated(&limits).expect("validated");
        let origin_seq = body.origin_seq;
        let bytes = encode_event_body_canonical(body.as_ref()).unwrap();
        let expected_sha = sha256_bytes(bytes.as_ref()).0;

        let record = VerifiedRecord::new(
            RecordHeader {
                origin_replica_id: origin,
                origin_seq: body.origin_seq,
                event_time_ms: body.event_time_ms,
                txn_id: body.txn_id,
                request_proof: body
                    .client_request_id
                    .map(|client_request_id| RequestProof::ClientNoHash { client_request_id })
                    .unwrap_or(RequestProof::None),
                sha256: expected_sha,
                prev_sha256: None,
            },
            bytes,
            body,
        )
        .unwrap();
        writer.append(&record, 10).unwrap();

        let unverified =
            SegmentDescriptor::<Unverified>::load(writer.current_path().to_path_buf()).unwrap();
        let verified = unverified.verify(&meta, &namespace).unwrap();
        let frame_offset = verified.header_len;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(writer.current_path())
            .unwrap();
        file.seek(SeekFrom::Start(frame_offset)).unwrap();
        let mut frame_header = [0u8; FRAME_HEADER_LEN];
        file.read_exact(&mut frame_header).unwrap();
        let length = u32::from_le_bytes([
            frame_header[4],
            frame_header[5],
            frame_header[6],
            frame_header[7],
        ]) as usize;
        let body_offset = frame_offset + FRAME_HEADER_LEN as u64;
        let mut frame_body = vec![0u8; length];
        file.seek(SeekFrom::Start(body_offset)).unwrap();
        file.read_exact(&mut frame_body).unwrap();
        assert!(frame_body.len() > 56);
        frame_body[56] ^= 0xFF;
        file.seek(SeekFrom::Start(body_offset)).unwrap();
        file.write_all(&frame_body).unwrap();
        let crc = crc32c(&frame_body);
        file.seek(SeekFrom::Start(frame_offset + 8)).unwrap();
        file.write_all(&crc.to_le_bytes()).unwrap();
        let max_record_bytes = limits.policy().max_wal_record_bytes();
        let err = match scan_segment(
            &verified,
            ReplayStartCursor::for_rebuild(&verified),
            max_record_bytes,
            &limits,
            false,
            |_offset, _record, _len| Ok(()),
        ) {
            Err(err) => err,
            Ok(_) => panic!("expected RecordShaMismatch"),
        };

        match err {
            WalReplayError::RecordShaMismatch(info) => {
                let info = info.as_ref();
                assert_eq!(info.namespace, namespace);
                assert_eq!(info.origin, origin);
                assert_eq!(info.seq, origin_seq);
                assert_eq!(info.expected, expected_sha);
                assert_ne!(info.got, expected_sha);
            }
            other => panic!("expected RecordShaMismatch, got {other:?}"),
        }
    }

    #[test]
    fn rebuild_rejects_gap_and_does_not_commit_next_origin_seq() {
        let temp = TempDir::new().unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([61u8; 16]));
        let store_epoch = StoreEpoch::new(1);
        let meta = test_meta(store_id, store_epoch);
        let limits = Limits::default();
        let index = SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache).unwrap();
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([62u8; 16]));
        let store = StoreIdentity::new(store_id, store_epoch);

        let mut writer = SegmentWriter::open(
            temp.path(),
            &meta,
            &namespace,
            10,
            SegmentConfig::from_limits(&limits),
        )
        .unwrap();
        let sha1 = append_record(
            &mut writer,
            store,
            &namespace,
            origin,
            Seq1::from_u64(1).expect("seq1"),
            None,
            11,
            1,
            &limits,
        );
        append_record(
            &mut writer,
            store,
            &namespace,
            origin,
            Seq1::from_u64(3).expect("seq3"),
            Some(sha1),
            13,
            3,
            &limits,
        );

        let err = rebuild_index(temp.path(), &meta, &index, &limits)
            .expect_err("rebuild must reject forward sequence gaps");
        assert!(matches!(
            err,
            WalReplayError::NonContiguousSeq {
                expected,
                got,
                ..
            } if expected == Seq1::from_u64(2).expect("seq2") && got == Seq0::new(3)
        ));

        let next_origin_seq = {
            let mut txn = index.writer().begin_txn().expect("begin txn");
            let next = txn
                .next_origin_seq(&namespace, &origin)
                .expect("next origin seq");
            txn.rollback().expect("rollback probe");
            next
        };
        assert_eq!(next_origin_seq.get(), 1);
        assert!(
            index
                .reader()
                .load_watermarks()
                .expect("load watermarks")
                .into_iter()
                .all(|row| !(row.namespace == namespace && row.origin == origin)),
            "frontier row should not be committed on rebuild gap"
        );
    }

    #[test]
    fn catch_up_rejects_persisted_cursor_before_header_without_scan_entry() {
        let fixture = seed_catch_up_atomic_fixture();
        let row = fixture
            .index
            .reader()
            .list_segments(&fixture.namespace)
            .expect("list segments")
            .into_iter()
            .find(|row| row.segment_id() == fixture.touched_segment_id)
            .expect("touched segment row");
        let segment = load_verified_segment_for_row(fixture.temp.path(), &fixture.meta, &row);
        let invalid_offset = WalCursorOffset::new(segment.header_len.saturating_sub(1));
        assert!(invalid_offset.get() < segment.header_len);

        let corrupt_row = segment_row_with_offset(&row, invalid_offset);
        let mut txn = fixture.index.writer().begin_txn().expect("begin txn");
        txn.upsert_segment(&corrupt_row)
            .expect("upsert corrupted segment row");
        txn.commit().expect("commit corrupted segment row");

        reset_test_scan_entry_count();
        let err = catch_up_index(
            fixture.temp.path(),
            &fixture.meta,
            &fixture.index,
            &fixture.limits,
        )
        .expect_err("header-before persisted offset should error");

        assert!(matches!(
            err,
            WalReplayError::IndexOffsetInvalid {
                offset,
                header_len,
                ..
            } if offset < header_len
        ));
        assert_eq!(test_scan_entry_count(), 0);
    }

    #[test]
    fn catch_up_rejects_gap_and_rolls_back_next_origin_seq() {
        let fixture = seed_catch_up_gap_fixture();
        let pre = capture_snapshot(&fixture.index, &fixture.namespace, fixture.origin);
        assert_eq!(pre.rows.len(), 2);
        assert_eq!(pre.next_origin_seq.get(), 3);

        let err = catch_up_index(
            fixture.temp.path(),
            &fixture.meta,
            &fixture.index,
            &fixture.limits,
        )
        .expect_err("catch-up must reject forward sequence gaps");
        assert!(matches!(
            err,
            WalReplayError::NonContiguousSeq {
                expected,
                got,
                ..
            } if expected == Seq1::from_u64(3).expect("seq3") && got == Seq0::new(4)
        ));

        let post = capture_snapshot(&fixture.index, &fixture.namespace, fixture.origin);
        assert_eq!(post, pre);
    }

    #[test]
    fn catch_up_rejects_persisted_cursor_beyond_segment_len_without_scan_entry() {
        let fixture = seed_catch_up_atomic_fixture();
        let row = fixture
            .index
            .reader()
            .list_segments(&fixture.namespace)
            .expect("list segments")
            .into_iter()
            .find(|row| row.segment_id() == fixture.touched_segment_id)
            .expect("touched segment row");
        let segment = load_verified_segment_for_row(fixture.temp.path(), &fixture.meta, &row);
        let invalid_offset = WalCursorOffset::new(segment.file_len.saturating_add(1));
        assert!(invalid_offset.get() > segment.file_len);

        let corrupt_row = segment_row_with_offset(&row, invalid_offset);
        let mut txn = fixture.index.writer().begin_txn().expect("begin txn");
        txn.upsert_segment(&corrupt_row)
            .expect("upsert corrupted segment row");
        txn.commit().expect("commit corrupted segment row");

        reset_test_scan_entry_count();
        let err = catch_up_index(
            fixture.temp.path(),
            &fixture.meta,
            &fixture.index,
            &fixture.limits,
        )
        .expect_err("out-of-bounds persisted offset should error");

        assert!(matches!(
            err,
            WalReplayError::IndexOffsetInvalid {
                offset, len, ..
            } if offset > len
        ));
        assert_eq!(test_scan_entry_count(), 0);
    }

    #[test]
    fn catch_up_atomic_crash_after_rows_before_frontier_rolls_back_all() {
        let fixture = seed_catch_up_atomic_fixture();
        let pre = capture_snapshot(&fixture.index, &fixture.namespace, fixture.origin);
        assert_eq!(pre.rows.len(), 2);
        assert_eq!(pre.next_origin_seq.get(), 3);

        {
            let _guard = CrashStageGuard::install(ReplayAtomicCommitStage::AfterRowsBeforeFrontier);
            let err = catch_up_index(
                fixture.temp.path(),
                &fixture.meta,
                &fixture.index,
                &fixture.limits,
            )
            .expect_err("injected crash should fail catch-up");
            assert!(matches!(
                err,
                WalReplayError::InjectedAtomicCatchUpCrash {
                    stage: ReplayAtomicCommitStage::AfterRowsBeforeFrontier
                }
            ));

            let post_crash = capture_snapshot(&fixture.index, &fixture.namespace, fixture.origin);
            assert_eq!(post_crash, pre);
        }

        catch_up_index(
            fixture.temp.path(),
            &fixture.meta,
            &fixture.index,
            &fixture.limits,
        )
        .expect("retry catch-up should succeed");
        assert_recovered_after_retry(&fixture, &pre);
    }

    #[test]
    fn catch_up_atomic_crash_after_frontier_before_commit_rolls_back_all() {
        let fixture = seed_catch_up_atomic_fixture();
        let pre = capture_snapshot(&fixture.index, &fixture.namespace, fixture.origin);
        assert_eq!(pre.rows.len(), 2);
        assert_eq!(pre.next_origin_seq.get(), 3);

        {
            let _guard =
                CrashStageGuard::install(ReplayAtomicCommitStage::AfterFrontierBeforeCommit);
            let err = catch_up_index(
                fixture.temp.path(),
                &fixture.meta,
                &fixture.index,
                &fixture.limits,
            )
            .expect_err("injected crash should fail catch-up");
            assert!(matches!(
                err,
                WalReplayError::InjectedAtomicCatchUpCrash {
                    stage: ReplayAtomicCommitStage::AfterFrontierBeforeCommit
                }
            ));

            let post_crash = capture_snapshot(&fixture.index, &fixture.namespace, fixture.origin);
            assert_eq!(post_crash, pre);
        }

        catch_up_index(
            fixture.temp.path(),
            &fixture.meta,
            &fixture.index,
            &fixture.limits,
        )
        .expect("retry catch-up should succeed");
        assert_recovered_after_retry(&fixture, &pre);
    }

    #[test]
    fn catch_up_atomic_success_commits_rows_segments_and_frontier_together() {
        let fixture = seed_catch_up_atomic_fixture();
        let pre = capture_snapshot(&fixture.index, &fixture.namespace, fixture.origin);
        assert_eq!(pre.rows.len(), 2);
        assert_eq!(pre.next_origin_seq.get(), 3);

        catch_up_index(
            fixture.temp.path(),
            &fixture.meta,
            &fixture.index,
            &fixture.limits,
        )
        .expect("catch-up should succeed");
        assert_recovered_after_retry(&fixture, &pre);
    }

    #[test]
    fn catch_up_reconcile_deletes_stale_segment_rows_and_matches_filesystem_set() {
        let fixture = seed_catch_up_atomic_fixture();
        let stale_segment_id = SegmentId::new(Uuid::from_bytes([63u8; 16]));
        let stale_row = SegmentRow::open(
            fixture.namespace.clone(),
            stale_segment_id,
            std::path::PathBuf::from(format!(
                "wal/{}/stale-segment.wal",
                fixture.namespace.as_str()
            )),
            1,
            WalCursorOffset::new(0),
        );

        let mut txn = fixture.index.writer().begin_txn().expect("begin txn");
        txn.upsert_segment(&stale_row)
            .expect("seed stale segment row");
        txn.commit().expect("commit stale segment row");

        assert!(
            indexed_segment_ids(&fixture.index, &fixture.namespace).contains(&stale_segment_id),
            "stale row should exist before catch-up"
        );

        catch_up_index(
            fixture.temp.path(),
            &fixture.meta,
            &fixture.index,
            &fixture.limits,
        )
        .expect("catch-up should reconcile segments");

        let indexed = indexed_segment_ids(&fixture.index, &fixture.namespace);
        let filesystem =
            filesystem_segment_ids(fixture.temp.path(), &fixture.meta, &fixture.namespace);
        assert_eq!(indexed, filesystem);
        assert!(
            !indexed.contains(&stale_segment_id),
            "catch-up must delete stale namespace rows"
        );
    }

    #[test]
    fn catch_up_reconcile_clears_rows_for_index_only_namespace() {
        let fixture = seed_catch_up_atomic_fixture();
        let index_only_namespace = NamespaceId::parse("indexonly").expect("namespace");
        let index_only_segment_id = SegmentId::new(Uuid::from_bytes([64u8; 16]));
        let index_only_row = SegmentRow::open(
            index_only_namespace.clone(),
            index_only_segment_id,
            std::path::PathBuf::from(format!(
                "wal/{}/index-only.wal",
                index_only_namespace.as_str()
            )),
            2,
            WalCursorOffset::new(0),
        );

        let mut txn = fixture.index.writer().begin_txn().expect("begin txn");
        txn.upsert_segment(&index_only_row)
            .expect("seed index-only namespace row");
        txn.commit().expect("commit index-only namespace row");

        assert!(
            !fixture
                .temp
                .path()
                .join("wal")
                .join(index_only_namespace.as_str())
                .exists()
        );
        assert_eq!(
            fixture
                .index
                .reader()
                .list_segments(&index_only_namespace)
                .expect("list pre catch-up")
                .len(),
            1
        );

        catch_up_index(
            fixture.temp.path(),
            &fixture.meta,
            &fixture.index,
            &fixture.limits,
        )
        .expect("catch-up should reconcile namespaces");

        assert!(
            fixture
                .index
                .reader()
                .list_segments(&index_only_namespace)
                .expect("list post catch-up")
                .is_empty()
        );
        assert!(
            !fixture
                .index
                .reader()
                .list_segment_namespaces()
                .expect("list segment namespaces")
                .contains(&index_only_namespace)
        );
    }

    #[test]
    fn origin_replay_state_enters_terminal_gap_state() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([65u8; 16]));
        let mut state = OriginReplayState::default();
        let sha1 = [1u8; 32];
        let sha2 = [2u8; 32];

        let err = state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(2).expect("seq2"),
                sha2,
                Some(sha1),
                8,
            )
            .expect_err("forward gap must transition state to terminal gap");
        assert!(matches!(
            err,
            WalReplayError::NonContiguousSeq {
                expected,
                got,
                ..
            } if expected == Seq1::from_u64(1).expect("seq1") && got == Seq0::new(2)
        ));

        let err = state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(1).expect("seq1"),
                sha1,
                None,
                8,
            )
            .expect_err("terminal gap state should reject later observations");
        assert!(matches!(
            err,
            WalReplayError::NonContiguousSeq {
                expected,
                got,
                ..
            } if expected == Seq1::from_u64(1).expect("seq1") && got == Seq0::new(2)
        ));
    }

    #[test]
    fn origin_replay_state_advances_when_gap_is_later_filled() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([23u8; 16]));
        let mut state = OriginReplayState::default();
        let sha1 = [1u8; 32];
        let sha2 = [2u8; 32];

        state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(2).expect("seq2"),
                sha2,
                Some(sha1),
                8,
            )
            .expect("record buffered until gap fills");
        assert_eq!(state.contiguous_seq, Seq0::ZERO);
        assert_eq!(
            state.first_pending_seq(),
            Some(Seq1::from_u64(2).expect("seq2"))
        );

        state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(1).expect("seq1"),
                sha1,
                None,
                8,
            )
            .expect("gap filled");

        assert_eq!(state.contiguous_seq.get(), 2);
        assert_eq!(state.head_sha, Some(sha2));
        assert!(state.first_pending_seq().is_none());
    }

    #[test]
    fn origin_replay_state_rejects_unbounded_pending_gap_buffers() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([25u8; 16]));
        let mut state = OriginReplayState::default();

        state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(3).expect("seq3"),
                [3u8; 32],
                Some([2u8; 32]),
                2,
            )
            .expect("first pending seq");
        state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(4).expect("seq4"),
                [4u8; 32],
                Some([3u8; 32]),
                2,
            )
            .expect("second pending seq");

        let err = state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(5).expect("seq5"),
                [5u8; 32],
                Some([4u8; 32]),
                2,
            )
            .expect_err("pending gap buffer should cap");
        assert!(matches!(err, WalReplayError::NonContiguousSeq { .. }));
    }

    #[test]
    fn origin_replay_state_detects_prev_sha_mismatch_when_flushing_buffer() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([24u8; 16]));
        let mut state = OriginReplayState::default();
        let sha1 = [1u8; 32];
        let sha2 = [2u8; 32];
        let wrong_prev = [9u8; 32];

        state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(2).expect("seq2"),
                sha2,
                Some(wrong_prev),
                8,
            )
            .expect("record buffered");

        let err = state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(1).expect("seq1"),
                sha1,
                None,
                8,
            )
            .expect_err("expected prev_sha mismatch");
        assert!(matches!(err, WalReplayError::PrevShaMismatch { .. }));
        assert_eq!(
            state.first_pending_seq(),
            Some(Seq1::from_u64(2).expect("seq2"))
        );
    }

    #[test]
    fn origin_replay_state_ignores_duplicate_committed_seq() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([26u8; 16]));
        let mut state = OriginReplayState::default();
        let sha1 = [1u8; 32];
        let sha2 = [2u8; 32];

        state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(1).expect("seq1"),
                sha1,
                None,
                8,
            )
            .expect("seed seq1");
        state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(2).expect("seq2"),
                sha2,
                Some(sha1),
                8,
            )
            .expect("seed seq2");
        state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(2).expect("seq2"),
                sha2,
                Some(sha1),
                8,
            )
            .expect("duplicate committed seq should be idempotent");

        assert_eq!(state.contiguous_seq.get(), 2);
        assert_eq!(state.head_sha, Some(sha2));
        assert!(state.first_pending_seq().is_none());
    }

    #[test]
    fn origin_replay_state_ignores_duplicate_pending_seq() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([27u8; 16]));
        let mut state = OriginReplayState::default();
        let sha1 = [1u8; 32];
        let sha2 = [2u8; 32];

        state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(2).expect("seq2"),
                sha2,
                Some(sha1),
                8,
            )
            .expect("seed pending seq2");
        state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(2).expect("seq2"),
                sha2,
                Some(sha1),
                8,
            )
            .expect("duplicate pending seq should be idempotent");
        state
            .observe(
                &namespace,
                origin,
                Seq1::from_u64(1).expect("seq1"),
                sha1,
                None,
                8,
            )
            .expect("gap fill should flush");

        assert_eq!(state.contiguous_seq.get(), 2);
        assert_eq!(state.head_sha, Some(sha2));
        assert!(state.first_pending_seq().is_none());
    }

    #[test]
    fn cleanup_orphan_tmp_segments_removes_unreferenced() {
        let temp = TempDir::new().unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
        let store_epoch = StoreEpoch::new(1);
        let meta = test_meta(store_id, store_epoch);
        let index = SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache).unwrap();

        let namespace = NamespaceId::core();
        let wal_dir = temp.path().join("wal");
        let namespace_dir = wal_dir.join(namespace.as_str());
        std::fs::create_dir_all(&namespace_dir).unwrap();

        let keep = namespace_dir.join("keep.wal.tmp");
        let orphan = namespace_dir.join("orphan.wal.tmp");
        std::fs::write(&keep, b"keep").unwrap();
        std::fs::write(&orphan, b"orphan").unwrap();

        let mut txn = index.writer().begin_txn().unwrap();
        let segment_row = SegmentRow::open(
            namespace.clone(),
            SegmentId::new(Uuid::from_bytes([1u8; 16])),
            segment_rel_path(temp.path(), &keep),
            1,
            WalCursorOffset::new(0),
        );
        txn.upsert_segment(&segment_row).unwrap();
        txn.commit().unwrap();

        cleanup_orphan_tmp_segments(temp.path(), &namespace, &namespace_dir, &index).unwrap();

        assert!(keep.exists());
        assert!(!orphan.exists());
    }
}

struct ReplayTracker {
    origins: BTreeMap<NamespaceId, BTreeMap<ReplicaId, OriginReplayState>>,
    max_pending_per_origin: usize,
}

impl ReplayTracker {
    fn new(max_pending_per_origin: usize) -> Self {
        Self {
            origins: BTreeMap::new(),
            max_pending_per_origin,
        }
    }

    fn seed_from_watermarks(&mut self, rows: Vec<WatermarkRow>) -> Result<(), WalReplayError> {
        for row in rows {
            let durable = row.durable();
            let durable_seq = durable.seq();
            let head = match durable.head() {
                HeadStatus::Genesis => None,
                HeadStatus::Known(sha) => Some(sha),
            };

            let state = OriginReplayState {
                max_seq: durable_seq,
                contiguous_seq: durable_seq,
                head_sha: head,
                touched: false,
                pending: BTreeMap::new(),
            };
            self.origins
                .entry(row.namespace)
                .or_default()
                .insert(row.origin, state);
        }
        Ok(())
    }

    fn observe_record(
        &mut self,
        namespace: &NamespaceId,
        origin: ReplicaId,
        seq: Seq1,
        sha: [u8; 32],
        prev_sha: Option<[u8; 32]>,
    ) -> Result<(), WalReplayError> {
        let entry = self
            .origins
            .entry(namespace.clone())
            .or_default()
            .entry(origin)
            .or_default();
        entry.observe(
            namespace,
            origin,
            seq,
            sha,
            prev_sha,
            self.max_pending_per_origin,
        )
    }
}

#[derive(Clone, Debug)]
struct OriginReplayState {
    max_seq: Seq0,
    contiguous_seq: Seq0,
    head_sha: Option<[u8; 32]>,
    touched: bool,
    pending: BTreeMap<Seq1, PendingRecord>,
}

#[derive(Clone, Copy, Debug)]
struct PendingRecord {
    sha: [u8; 32],
    prev_sha: Option<[u8; 32]>,
}

impl Default for OriginReplayState {
    fn default() -> Self {
        Self {
            max_seq: Seq0::ZERO,
            contiguous_seq: Seq0::ZERO,
            head_sha: None,
            touched: false,
            pending: BTreeMap::new(),
        }
    }
}

impl OriginReplayState {
    fn first_pending_seq(&self) -> Option<Seq1> {
        self.pending.keys().next().copied()
    }

    fn observe(
        &mut self,
        namespace: &NamespaceId,
        origin: ReplicaId,
        seq: Seq1,
        sha: [u8; 32],
        prev_sha: Option<[u8; 32]>,
        max_pending_per_origin: usize,
    ) -> Result<(), WalReplayError> {
        self.max_seq = Seq0::new(self.max_seq.get().max(seq.get()));
        self.touched = true;

        let expected = self.contiguous_seq.next();
        if seq.get() <= self.contiguous_seq.get() {
            // Catch-up can re-read records that are already indexed; treat these as idempotent.
            return Ok(());
        }

        if seq != expected && self.pending.len() >= max_pending_per_origin {
            return Err(WalReplayError::NonContiguousSeq {
                namespace: namespace.clone(),
                origin,
                expected,
                got: Seq0::new(seq.get()),
            });
        }

        if let Some(existing) = self.pending.get(&seq) {
            if existing.sha == sha && existing.prev_sha == prev_sha {
                return Ok(());
            }
            return Err(WalReplayError::NonContiguousSeq {
                namespace: namespace.clone(),
                origin,
                expected,
                got: Seq0::new(seq.get()),
            });
        }
        self.pending.insert(seq, PendingRecord { sha, prev_sha });

        while let Some(next) = self.pending.get(&self.contiguous_seq.next()).copied() {
            let seq = self.contiguous_seq.next();
            let expected_prev = self.head_sha.unwrap_or([0u8; 32]);
            let got_prev = next.prev_sha.unwrap_or([0u8; 32]);
            let head_seq = self.contiguous_seq;
            if self.contiguous_seq == Seq0::ZERO {
                if next.prev_sha.is_some() {
                    return Err(WalReplayError::PrevShaMismatch {
                        namespace: namespace.clone(),
                        origin,
                        seq,
                        expected_prev_sha256: expected_prev,
                        got_prev_sha256: got_prev,
                        head_seq,
                    });
                }
            } else if next.prev_sha != self.head_sha {
                return Err(WalReplayError::PrevShaMismatch {
                    namespace: namespace.clone(),
                    origin,
                    seq,
                    expected_prev_sha256: expected_prev,
                    got_prev_sha256: got_prev,
                    head_seq,
                });
            }

            self.pending.remove(&seq);
            self.contiguous_seq = Seq0::new(seq.get());
            self.head_sha = Some(next.sha);
        }

        Ok(())
    }
}
