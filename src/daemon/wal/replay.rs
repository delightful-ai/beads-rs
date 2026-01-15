//! WAL segment replay and SQLite index rebuild/catch-up.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use crc32c::crc32c;
use thiserror::Error;

use crate::core::{
    DecodeError, EventId, Limits, NamespaceId, ReplicaId, SegmentId, Seq1, StoreMeta,
    decode_event_body, decode_event_hlc_max, sha256_bytes,
};

use super::EventWalError;
use super::frame::{FRAME_HEADER_LEN, FRAME_MAGIC};
use super::index::{HlcRow, SegmentRow, WalIndex, WalIndexError, WatermarkRow};
use super::record::{Record, RecordHeaderMismatch, validate_header_matches_body};
use super::segment::{SEGMENT_HEADER_PREFIX_LEN, SEGMENT_MAGIC, SegmentHeader};

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
    #[error("index offset invalid for {path:?}: offset {offset} > len {len}")]
    IndexOffsetInvalid {
        path: PathBuf,
        offset: u64,
        len: u64,
    },
    #[error("sealed segment missing final_len at {path:?}. Run `bd store fsck` to repair.")]
    SealedSegmentFinalLenMissing { path: PathBuf },
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
        namespace: String,
        origin: ReplicaId,
        expected: u64,
        got: u64,
    },
    #[error("prev_sha mismatch for {namespace} {origin} seq {seq}")]
    PrevShaMismatch {
        namespace: String,
        origin: ReplicaId,
        seq: u64,
        expected_prev_sha256: [u8; 32],
        got_prev_sha256: [u8; 32],
        head_seq: u64,
    },
    #[error("head sha required for {namespace} {origin} seq {seq}")]
    MissingHead {
        namespace: String,
        origin: ReplicaId,
        seq: u64,
    },
    #[error("head sha must be absent for {namespace} {origin} seq {seq}")]
    UnexpectedHead {
        namespace: String,
        origin: ReplicaId,
        seq: u64,
    },
    #[error("origin_seq overflow for {namespace} {origin}")]
    OriginSeqOverflow {
        namespace: String,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordShaMismatchInfo {
    pub namespace: NamespaceId,
    pub origin: ReplicaId,
    pub seq: u64,
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

fn replay_index(
    store_dir: &Path,
    meta: &StoreMeta,
    index: &dyn WalIndex,
    limits: &Limits,
    mode: ReplayMode,
) -> Result<ReplayStats, WalReplayError> {
    let mut stats = ReplayStats::default();
    let wal_dir = store_dir.join("wal");
    let max_record_bytes = limits.max_wal_record_bytes.min(limits.max_frame_bytes);

    let mut tracker = ReplayTracker::new();
    if mode == ReplayMode::CatchUp {
        let rows = index.reader().load_watermarks()?;
        tracker.seed_from_watermarks(rows)?;
    }

    let namespaces = list_namespaces(&wal_dir)?;
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
                existing.insert(row.segment_id, row);
            }
        }

        for segment in segments {
            let is_last = Some(segment.header.segment_id) == last_segment_id;
            stats.segments_scanned += 1;
            let start_offset = match mode {
                ReplayMode::Rebuild => segment.header_len,
                ReplayMode::CatchUp => existing
                    .get(&segment.header.segment_id)
                    .map(|row| row.last_indexed_offset)
                    .unwrap_or(segment.header_len),
            };

            if start_offset < segment.header_len {
                return Err(WalReplayError::IndexOffsetInvalid {
                    path: segment.path.clone(),
                    offset: start_offset,
                    len: segment.file_len,
                });
            }
            if start_offset > segment.file_len {
                return Err(WalReplayError::IndexOffsetInvalid {
                    path: segment.path.clone(),
                    offset: start_offset,
                    len: segment.file_len,
                });
            }

            if mode == ReplayMode::CatchUp
                && let Some(row) = existing
                    .get(&segment.header.segment_id)
                    .filter(|row| row.sealed)
            {
                let final_len =
                    row.final_len
                        .ok_or_else(|| WalReplayError::SealedSegmentFinalLenMissing {
                            path: segment.path.clone(),
                        })?;
                if segment.file_len != final_len {
                    return Err(WalReplayError::SealedSegmentLenMismatch {
                        path: segment.path.clone(),
                        expected: final_len,
                        actual: segment.file_len,
                    });
                }
            }

            let mut txn = index.writer().begin_txn()?;
            let scan = scan_segment(
                &segment,
                start_offset,
                max_record_bytes,
                limits,
                true,
                |offset, record, frame_len| {
                    index_record(
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

            let sealed = !is_last;
            let final_len = sealed.then_some(scan.last_indexed_offset);
            let segment_row = SegmentRow {
                namespace: namespace.clone(),
                segment_id: segment.header.segment_id,
                segment_path: segment_rel_path(store_dir, &segment.path),
                created_at_ms: segment.header.created_at_ms,
                last_indexed_offset: scan.last_indexed_offset,
                sealed,
                final_len,
            };
            txn.upsert_segment(&segment_row)?;
            txn.commit()?;
        }
    }

    let mut txn = index.writer().begin_txn()?;
    let update_all = mode == ReplayMode::Rebuild;
    for (namespace, origins) in tracker.origins {
        for (origin, state) in origins {
            if !update_all && !state.touched {
                continue;
            }
            let head = state.head_sha;
            let seq = state.contiguous_seq;
            if seq > 0 && head.is_none() {
                return Err(WalReplayError::MissingHead {
                    namespace: namespace.to_string(),
                    origin,
                    seq,
                });
            }
            let next_seq = state
                .max_seq
                .checked_add(1)
                .ok_or_else(|| WalReplayError::OriginSeqOverflow {
                    namespace: namespace.to_string(),
                    origin,
                })?;
            let next_seq = Seq1::from_u64(next_seq).ok_or_else(|| {
                WalReplayError::OriginSeqOverflow {
                    namespace: namespace.to_string(),
                    origin,
                }
            })?;

            txn.update_watermark(&namespace, &origin, seq, seq, head, head)?;
            txn.set_next_origin_seq(&namespace, &origin, next_seq)?;
        }
    }
    txn.commit()?;

    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
fn index_record(
    txn: &mut dyn super::index::WalIndexTxn,
    tracker: &mut ReplayTracker,
    namespace: &NamespaceId,
    segment: &SegmentDescriptor<Verified>,
    offset: u64,
    record: &Record,
    frame_len: u32,
    limits: &Limits,
) -> Result<(), WalReplayError> {
    let header = &record.header;
    let origin_seq = header.origin_seq;
    let event_id = EventId::new(header.origin_replica_id, namespace.clone(), origin_seq);

    tracker.observe_record(
        namespace,
        header.origin_replica_id,
        header.origin_seq.get(),
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
        header.client_request_id,
        header.request_sha256,
    )?;

    if let (Some(client_request_id), Some(request_sha256)) =
        (header.client_request_id, header.request_sha256)
    {
        txn.upsert_client_request(
            namespace,
            &header.origin_replica_id,
            client_request_id,
            request_sha256,
            header.txn_id,
            &[event_id],
            header.event_time_ms,
        )?;
    }

    if let Some(hlc_max) =
        decode_event_hlc_max(record.payload.as_ref(), limits).map_err(|source| {
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
        Ok(rows) => rows.into_iter().map(|row| row.segment_path).collect(),
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
    last_indexed_offset: u64,
    records: usize,
    truncated: bool,
    truncated_from_offset: Option<u64>,
}

fn scan_segment<F>(
    segment: &SegmentDescriptor<Verified>,
    start_offset: u64,
    max_record_bytes: usize,
    limits: &Limits,
    repair_tail: bool,
    mut on_record: F,
) -> Result<SegmentScanOutcome, WalReplayError>
where
    F: FnMut(u64, &Record, u32) -> Result<(), WalReplayError>,
{
    let mut file = OpenOptions::new()
        .read(true)
        .write(repair_tail)
        .open(&segment.path)
        .map_err(|source| WalReplayError::Io {
            path: segment.path.clone(),
            source,
        })?;
    file.seek(SeekFrom::Start(start_offset))
        .map_err(|source| WalReplayError::Io {
            path: segment.path.clone(),
            source,
        })?;

    let mut offset = start_offset;
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

        let record = Record::decode_body(&body).map_err(|source| WalReplayError::RecordDecode {
            path: segment.path.clone(),
            source,
        })?;
        let (_, event_body) =
            decode_event_body(record.payload.as_ref(), limits).map_err(|source| {
                WalReplayError::EventBodyDecode {
                    path: segment.path.clone(),
                    offset,
                    source,
                }
            })?;
        validate_header_matches_body(&record.header, &event_body).map_err(|source| {
            WalReplayError::RecordHeaderMismatch {
                path: segment.path.clone(),
                offset,
                source,
            }
        })?;
        let expected_sha = sha256_bytes(record.payload.as_ref()).0;
        if expected_sha != record.header.sha256 {
            return Err(WalReplayError::RecordShaMismatch(Box::new(
                RecordShaMismatchInfo {
                    namespace: segment.header.namespace.clone(),
                    origin: record.header.origin_replica_id,
                    seq: record.header.origin_seq.get(),
                    expected: expected_sha,
                    got: record.header.sha256,
                    path: segment.path.clone(),
                    offset,
                },
            )));
        }
        on_record(offset, &record, frame_len as u32)?;

        records += 1;
        offset = offset.saturating_add(frame_len);
    }

    if truncated && repair_tail {
        truncate_tail(&mut file, &segment.path, offset)?;
    }

    Ok(SegmentScanOutcome {
        last_indexed_offset: offset,
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
    use bytes::Bytes;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::core::{
        ActorId, EventBody, EventKindV1, HlcMax, Limits, NamespaceId, ReplicaId, SegmentId, Seq1,
        StoreEpoch, StoreId, StoreIdentity, StoreMeta, StoreMetaVersions, TxnDeltaV1, TxnId,
        encode_event_body_canonical,
    };
    use crate::daemon::wal::SegmentConfig;
    use crate::daemon::wal::record::RecordHeader;
    use crate::daemon::wal::segment::SegmentWriter;
    use crate::daemon::wal::{IndexDurabilityMode, SqliteWalIndex};

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
            kind: EventKindV1::TxnV1,
            delta: TxnDeltaV1::new(),
            hlc_max: Some(HlcMax {
                actor_id: ActorId::new("alice".to_string()).unwrap(),
                physical_ms: event_time_ms,
                logical: 1,
            }),
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
        let bytes = encode_event_body_canonical(&body).unwrap();
        let expected_sha = sha256_bytes(bytes.as_ref()).0;
        let mut bad_sha = expected_sha;
        bad_sha[0] ^= 0xFF;

        let record = Record {
            header: RecordHeader {
                origin_replica_id: origin,
                origin_seq: body.origin_seq,
                event_time_ms: body.event_time_ms,
                txn_id: body.txn_id,
                client_request_id: body.client_request_id,
                request_sha256: None,
                sha256: bad_sha,
                prev_sha256: None,
            },
            payload: Bytes::copy_from_slice(bytes.as_ref()),
        };
        writer.append(&record, 10).unwrap();

        let unverified =
            SegmentDescriptor::<Unverified>::load(writer.current_path().to_path_buf()).unwrap();
        let verified = unverified.verify(&meta, &namespace).unwrap();
        let max_record_bytes = limits.max_wal_record_bytes.min(limits.max_frame_bytes);
        let err = match scan_segment(
            &verified,
            verified.header_len,
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
                assert_eq!(info.seq, body.origin_seq.get());
                assert_eq!(info.expected, expected_sha);
                assert_eq!(info.got, bad_sha);
            }
            other => panic!("expected RecordShaMismatch, got {other:?}"),
        }
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
        let segment_row = SegmentRow {
            namespace: namespace.clone(),
            segment_id: SegmentId::new(Uuid::from_bytes([1u8; 16])),
            segment_path: segment_rel_path(temp.path(), &keep),
            created_at_ms: 1,
            last_indexed_offset: 0,
            sealed: false,
            final_len: None,
        };
        txn.upsert_segment(&segment_row).unwrap();
        txn.commit().unwrap();

        cleanup_orphan_tmp_segments(temp.path(), &namespace, &namespace_dir, &index).unwrap();

        assert!(keep.exists());
        assert!(!orphan.exists());
    }
}

#[derive(Default)]
struct ReplayTracker {
    origins: BTreeMap<NamespaceId, BTreeMap<ReplicaId, OriginReplayState>>,
}

impl ReplayTracker {
    fn new() -> Self {
        Self::default()
    }

    fn seed_from_watermarks(&mut self, rows: Vec<WatermarkRow>) -> Result<(), WalReplayError> {
        for row in rows {
            let head = if row.durable_seq == 0 {
                if row.durable_head_sha.is_some() {
                    return Err(WalReplayError::UnexpectedHead {
                        namespace: row.namespace.to_string(),
                        origin: row.origin,
                        seq: row.durable_seq,
                    });
                }
                None
            } else {
                Some(
                    row.durable_head_sha
                        .ok_or_else(|| WalReplayError::MissingHead {
                            namespace: row.namespace.to_string(),
                            origin: row.origin,
                            seq: row.durable_seq,
                        })?,
                )
            };

            let state = OriginReplayState {
                max_seq: row.durable_seq,
                contiguous_seq: row.durable_seq,
                head_sha: head,
                touched: false,
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
        seq: u64,
        sha: [u8; 32],
        prev_sha: Option<[u8; 32]>,
    ) -> Result<(), WalReplayError> {
        let entry = self
            .origins
            .entry(namespace.clone())
            .or_default()
            .entry(origin)
            .or_default();
        entry.observe(namespace, origin, seq, sha, prev_sha)
    }
}

#[derive(Clone, Debug, Default)]
struct OriginReplayState {
    max_seq: u64,
    contiguous_seq: u64,
    head_sha: Option<[u8; 32]>,
    touched: bool,
}

impl OriginReplayState {
    fn observe(
        &mut self,
        namespace: &NamespaceId,
        origin: ReplicaId,
        seq: u64,
        sha: [u8; 32],
        prev_sha: Option<[u8; 32]>,
    ) -> Result<(), WalReplayError> {
        if seq == 0 {
            return Err(WalReplayError::NonContiguousSeq {
                namespace: namespace.to_string(),
                origin,
                expected: self.contiguous_seq + 1,
                got: seq,
            });
        }

        self.max_seq = self.max_seq.max(seq);
        self.touched = true;

        let expected = self.contiguous_seq + 1;
        if seq == expected {
            let expected_prev = self.head_sha.unwrap_or([0u8; 32]);
            let got_prev = prev_sha.unwrap_or([0u8; 32]);
            let head_seq = self.contiguous_seq;
            if expected == 1 {
                if prev_sha.is_some() {
                    return Err(WalReplayError::PrevShaMismatch {
                        namespace: namespace.to_string(),
                        origin,
                        seq,
                        expected_prev_sha256: expected_prev,
                        got_prev_sha256: got_prev,
                        head_seq,
                    });
                }
            } else if prev_sha != self.head_sha {
                return Err(WalReplayError::PrevShaMismatch {
                    namespace: namespace.to_string(),
                    origin,
                    seq,
                    expected_prev_sha256: expected_prev,
                    got_prev_sha256: got_prev,
                    head_seq,
                });
            }

            self.contiguous_seq = seq;
            self.head_sha = Some(sha);
            return Ok(());
        }

        if seq <= self.contiguous_seq {
            return Err(WalReplayError::NonContiguousSeq {
                namespace: namespace.to_string(),
                origin,
                expected,
                got: seq,
            });
        }

        Ok(())
    }
}
