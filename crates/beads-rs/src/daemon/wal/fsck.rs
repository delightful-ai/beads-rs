//! Offline WAL verification and repair utilities.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::core::{
    Limits, NamespaceId, ReplicaId, StoreEpoch, StoreId, StoreMeta, decode_event_body,
};
use crate::daemon::store_runtime::store_index_durability_mode;
use crate::daemon::wal::frame::{FRAME_HEADER_LEN, FRAME_MAGIC};
use crate::daemon::wal::record::{RecordVerifyError, UnverifiedRecord};
use crate::daemon::wal::{
    EventWalError, SegmentHeader, SqliteWalIndex, WalIndex, WalIndexError, WalReplayError,
    rebuild_index,
};
use crate::paths;

const QUARANTINE_DIR_NAME: &str = "quarantine";

#[derive(Clone, Debug)]
pub struct FsckOptions {
    pub repair: bool,
    pub limits: Limits,
}

impl FsckOptions {
    pub fn new(repair: bool, limits: Limits) -> Self {
        Self { repair, limits }
    }
}

#[derive(Debug, Error)]
pub enum FsckError {
    #[error("store meta missing at {path:?}")]
    StoreMetaMissing { path: PathBuf },
    #[error("store meta read failed at {path:?}: {source}")]
    StoreMetaRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("store meta parse failed at {path:?}: {source}")]
    StoreMetaParse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("store meta store_id mismatch: expected {expected}, got {got}")]
    StoreIdMismatch { expected: StoreId, got: StoreId },
    #[error("store meta store_epoch mismatch: expected {expected}, got {got}")]
    StoreEpochMismatch {
        expected: StoreEpoch,
        got: StoreEpoch,
    },
    #[error("io error at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("store config error: {0}")]
    StoreConfig(Box<crate::daemon::store_runtime::StoreRuntimeError>),
    #[error(transparent)]
    WalReplay(#[from] Box<WalReplayError>),
    #[error(transparent)]
    WalIndex(#[from] WalIndexError),
    #[error("event wal error: {0}")]
    EventWal(#[from] EventWalError),
}

impl From<WalReplayError> for FsckError {
    fn from(err: WalReplayError) -> Self {
        FsckError::WalReplay(Box::new(err))
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FsckStats {
    pub namespaces: usize,
    pub segments: usize,
    pub records: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FsckStatus {
    Pass,
    Warn,
    Fail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FsckSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FsckRisk {
    Low,
    Medium,
    High,
    Critical,
}

impl From<FsckSeverity> for FsckRisk {
    fn from(severity: FsckSeverity) -> Self {
        match severity {
            FsckSeverity::Low => FsckRisk::Low,
            FsckSeverity::Medium => FsckRisk::Medium,
            FsckSeverity::High => FsckRisk::High,
            FsckSeverity::Critical => FsckRisk::Critical,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FsckCheckId {
    SegmentHeaders,
    SegmentFrames,
    RecordHashes,
    OriginContiguity,
    IndexOffsets,
    CheckpointCache,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FsckEvidenceCode {
    SegmentHeaderInvalid,
    SegmentHeaderMismatch,
    SegmentHeaderSymlink,
    FrameHeaderInvalid,
    FrameCrcMismatch,
    FrameTruncated,
    RecordDecodeInvalid,
    RecordHeaderMismatch,
    RecordShaMismatch,
    PrevShaMismatch,
    NonContiguousSeq,
    SealedSegmentLenMismatch,
    IndexOffsetOutOfBounds,
    IndexMissingSegment,
    IndexBehindWal,
    IndexOpenFailed,
    CheckpointCacheInvalid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FsckEvidence {
    pub code: FsckEvidenceCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<NamespaceId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<ReplicaId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FsckCheck {
    pub id: FsckCheckId,
    pub status: FsckStatus,
    pub severity: FsckSeverity,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<FsckEvidence>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub suggested_actions: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FsckRepairKind {
    TruncateTail,
    QuarantineSegment,
    RebuildIndex,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FsckRepair {
    pub kind: FsckRepairKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FsckSummary {
    pub risk: FsckRisk,
    pub safe_to_accept_writes: bool,
    pub safe_to_prune_wal: bool,
    pub safe_to_rebuild_index: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FsckReport {
    pub store_id: StoreId,
    pub checked_at_ms: u64,
    pub stats: FsckStats,
    pub checks: Vec<FsckCheck>,
    pub summary: FsckSummary,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub repairs: Vec<FsckRepair>,
}

pub fn fsck_store(store_id: StoreId, options: FsckOptions) -> Result<FsckReport, FsckError> {
    let store_dir = paths::store_dir(store_id);
    let meta_path = paths::store_meta_path(store_id);
    let meta = read_store_meta(&meta_path)?;
    if meta.store_id() != store_id {
        return Err(FsckError::StoreIdMismatch {
            expected: store_id,
            got: meta.store_id(),
        });
    }
    fsck_store_dir(&store_dir, &meta, options)
}

pub fn fsck_store_dir(
    store_dir: &Path,
    meta: &StoreMeta,
    options: FsckOptions,
) -> Result<FsckReport, FsckError> {
    let checked_at_ms = crate::WallClock::now().0;
    let mut builder = FsckReportBuilder::new(meta.store_id(), checked_at_ms);

    let wal_dir = store_dir.join("wal");
    let namespaces = list_namespaces(&wal_dir, &mut builder)?;
    builder.stats.namespaces = namespaces.len();

    let max_record_bytes = options
        .limits
        .max_wal_record_bytes
        .min(options.limits.max_frame_bytes);

    let mut tracker = FsckTracker::default();
    let mut segments_by_path = BTreeMap::new();
    let mut segments_by_id = BTreeMap::new();
    for namespace in namespaces {
        let segments = list_segments(&wal_dir, &namespace, meta, &mut builder, options.repair)?;
        for segment in segments {
            builder.stats.segments += 1;
            let result = scan_segment(
                &segment,
                max_record_bytes,
                &options.limits,
                &mut tracker,
                &mut builder,
                options.repair,
            )?;
            builder.stats.records += result.records;
            if result.quarantined {
                continue;
            }

            segments_by_id.insert(segment.header.segment_id, segment.clone());
            segments_by_path.insert(segment.path.clone(), segment);
        }
    }

    if options.repair {
        if let Err(err) = rebuild_index_after_repair(store_dir, meta, &options) {
            builder.record_issue(
                FsckCheckId::IndexOffsets,
                FsckStatus::Fail,
                FsckSeverity::High,
                FsckEvidence {
                    code: FsckEvidenceCode::IndexOpenFailed,
                    message: format!("failed to rebuild wal index: {err}"),
                    path: Some(paths::wal_index_path(meta.store_id())),
                    namespace: None,
                    origin: None,
                    seq: None,
                    offset: None,
                },
                Some("re-run fsck after investigating wal.sqlite rebuild failure"),
            );
        } else {
            builder.repairs.push(FsckRepair {
                kind: FsckRepairKind::RebuildIndex,
                path: Some(paths::wal_index_path(meta.store_id())),
                detail: "rebuilt wal.sqlite from WAL segments".to_string(),
            });
        }
    }

    check_index_offsets(
        store_dir,
        meta,
        &segments_by_path,
        &segments_by_id,
        &mut builder,
    );

    Ok(builder.finish())
}

#[derive(Clone, Debug)]
struct SegmentInfo {
    path: PathBuf,
    namespace: NamespaceId,
    header: SegmentHeader,
    header_len: u64,
    file_len: u64,
}

#[derive(Clone, Debug)]
struct SegmentScanResult {
    records: usize,
    quarantined: bool,
}

fn scan_segment(
    segment: &SegmentInfo,
    max_record_bytes: usize,
    limits: &Limits,
    tracker: &mut FsckTracker,
    builder: &mut FsckReportBuilder,
    repair: bool,
) -> Result<SegmentScanResult, FsckError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(repair)
        .open(&segment.path)
        .map_err(|source| FsckError::Io {
            path: segment.path.clone(),
            source,
        })?;
    file.seek(SeekFrom::Start(segment.header_len))
        .map_err(|source| FsckError::Io {
            path: segment.path.clone(),
            source,
        })?;

    let mut offset = segment.header_len;
    let mut records = 0usize;
    let mut quarantined = false;

    while offset < segment.file_len {
        let remaining = segment.file_len - offset;
        if remaining < FRAME_HEADER_LEN as u64 {
            builder.record_issue(
                FsckCheckId::SegmentFrames,
                FsckStatus::Fail,
                FsckSeverity::High,
                FsckEvidence {
                    code: FsckEvidenceCode::FrameTruncated,
                    message: "frame header truncated at EOF".to_string(),
                    path: Some(segment.path.clone()),
                    namespace: Some(segment.namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: Some(offset),
                },
                Some("run `bd store fsck --repair` to truncate tail corruption"),
            );
            if repair {
                truncate_tail(&mut file, &segment.path, offset)?;
                builder.repairs.push(FsckRepair {
                    kind: FsckRepairKind::TruncateTail,
                    path: Some(segment.path.clone()),
                    detail: format!("truncated segment tail to offset {offset}"),
                });
            }
            break;
        }

        let mut header = [0u8; FRAME_HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|source| FsckError::Io {
                path: segment.path.clone(),
                source,
            })?;

        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let length = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let expected_crc = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

        let frame_len = FRAME_HEADER_LEN as u64 + length as u64;
        if magic != FRAME_MAGIC || length == 0 || length as usize > max_record_bytes {
            builder.record_issue(
                FsckCheckId::SegmentFrames,
                FsckStatus::Fail,
                FsckSeverity::High,
                FsckEvidence {
                    code: FsckEvidenceCode::FrameHeaderInvalid,
                    message: "invalid frame header".to_string(),
                    path: Some(segment.path.clone()),
                    namespace: Some(segment.namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: Some(offset),
                },
                Some("run `bd store fsck --repair` to quarantine corrupted segments"),
            );
            if repair {
                drop(file);
                let quarantined_path = quarantine_segment(&segment.path)?;
                builder.repairs.push(FsckRepair {
                    kind: FsckRepairKind::QuarantineSegment,
                    path: Some(quarantined_path),
                    detail: "quarantined segment with invalid frame header".to_string(),
                });
                quarantined = true;
            }
            break;
        }

        if frame_len > remaining {
            builder.record_issue(
                FsckCheckId::SegmentFrames,
                FsckStatus::Fail,
                FsckSeverity::High,
                FsckEvidence {
                    code: FsckEvidenceCode::FrameTruncated,
                    message: "frame truncated at EOF".to_string(),
                    path: Some(segment.path.clone()),
                    namespace: Some(segment.namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: Some(offset),
                },
                Some("run `bd store fsck --repair` to truncate tail corruption"),
            );
            if repair {
                truncate_tail(&mut file, &segment.path, offset)?;
                builder.repairs.push(FsckRepair {
                    kind: FsckRepairKind::TruncateTail,
                    path: Some(segment.path.clone()),
                    detail: format!("truncated segment tail to offset {offset}"),
                });
            }
            break;
        }

        let mut body = vec![0u8; length as usize];
        file.read_exact(&mut body).map_err(|source| FsckError::Io {
            path: segment.path.clone(),
            source,
        })?;

        let actual_crc = crc32c::crc32c(&body);
        if actual_crc != expected_crc {
            let is_tail = offset.saturating_add(frame_len) == segment.file_len;
            builder.record_issue(
                FsckCheckId::SegmentFrames,
                FsckStatus::Fail,
                FsckSeverity::High,
                FsckEvidence {
                    code: FsckEvidenceCode::FrameCrcMismatch,
                    message: "frame crc32c mismatch".to_string(),
                    path: Some(segment.path.clone()),
                    namespace: Some(segment.namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: Some(offset),
                },
                Some(if is_tail {
                    "run `bd store fsck --repair` to truncate tail corruption"
                } else {
                    "run `bd store fsck --repair` to quarantine corrupted segments"
                }),
            );
            if repair {
                if is_tail {
                    truncate_tail(&mut file, &segment.path, offset)?;
                    builder.repairs.push(FsckRepair {
                        kind: FsckRepairKind::TruncateTail,
                        path: Some(segment.path.clone()),
                        detail: format!("truncated segment tail to offset {offset}"),
                    });
                } else {
                    drop(file);
                    let quarantined_path = quarantine_segment(&segment.path)?;
                    builder.repairs.push(FsckRepair {
                        kind: FsckRepairKind::QuarantineSegment,
                        path: Some(quarantined_path),
                        detail: "quarantined segment with mid-file CRC mismatch".to_string(),
                    });
                    quarantined = true;
                }
            }
            break;
        }

        let record = match UnverifiedRecord::decode_body(&body) {
            Ok(record) => record,
            Err(err) => {
                builder.record_issue(
                    FsckCheckId::SegmentFrames,
                    FsckStatus::Fail,
                    FsckSeverity::High,
                    FsckEvidence {
                        code: FsckEvidenceCode::RecordDecodeInvalid,
                        message: format!("record decode failed: {err}"),
                        path: Some(segment.path.clone()),
                        namespace: Some(segment.namespace.clone()),
                        origin: None,
                        seq: None,
                        offset: Some(offset),
                    },
                    Some("run `bd store fsck --repair` to quarantine corrupted segments"),
                );
                if repair {
                    drop(file);
                    let quarantined_path = quarantine_segment(&segment.path)?;
                    builder.repairs.push(FsckRepair {
                        kind: FsckRepairKind::QuarantineSegment,
                        path: Some(quarantined_path),
                        detail: "quarantined segment with undecodable record".to_string(),
                    });
                    quarantined = true;
                }
                break;
            }
        };

        let header = record.header().clone();
        let (_, event_body) = match decode_event_body(record.payload_bytes(), limits) {
            Ok(decoded) => decoded,
            Err(err) => {
                builder.record_issue(
                    FsckCheckId::SegmentFrames,
                    FsckStatus::Fail,
                    FsckSeverity::High,
                    FsckEvidence {
                        code: FsckEvidenceCode::RecordDecodeInvalid,
                        message: format!("event body decode failed: {err}"),
                        path: Some(segment.path.clone()),
                        namespace: Some(segment.namespace.clone()),
                        origin: Some(header.origin_replica_id),
                        seq: Some(header.origin_seq.get()),
                        offset: Some(offset),
                    },
                    Some("run `bd store fsck --repair` to quarantine corrupted segments"),
                );
                if repair {
                    drop(file);
                    let quarantined_path = quarantine_segment(&segment.path)?;
                    builder.repairs.push(FsckRepair {
                        kind: FsckRepairKind::QuarantineSegment,
                        path: Some(quarantined_path),
                        detail: "quarantined segment with undecodable event body".to_string(),
                    });
                    quarantined = true;
                }
                break;
            }
        };
        let verify = record.verify_with_event_body(event_body);
        match verify {
            Ok(_) => {}
            Err(RecordVerifyError::HeaderMismatch(err)) => {
                builder.record_issue(
                    FsckCheckId::RecordHashes,
                    FsckStatus::Fail,
                    FsckSeverity::High,
                    FsckEvidence {
                        code: FsckEvidenceCode::RecordHeaderMismatch,
                        message: format!("record header mismatch: {err}"),
                        path: Some(segment.path.clone()),
                        namespace: Some(segment.namespace.clone()),
                        origin: Some(header.origin_replica_id),
                        seq: Some(header.origin_seq.get()),
                        offset: Some(offset),
                    },
                    Some("run `bd store fsck --repair` to quarantine corrupted segments"),
                );
                if repair {
                    drop(file);
                    let quarantined_path = quarantine_segment(&segment.path)?;
                    builder.repairs.push(FsckRepair {
                        kind: FsckRepairKind::QuarantineSegment,
                        path: Some(quarantined_path),
                        detail: "quarantined segment with header/body mismatch".to_string(),
                    });
                    quarantined = true;
                }
                break;
            }
            Err(RecordVerifyError::PayloadMismatch { .. }) | Err(RecordVerifyError::Encode(_)) => {
                builder.record_issue(
                    FsckCheckId::RecordHashes,
                    FsckStatus::Fail,
                    FsckSeverity::High,
                    FsckEvidence {
                        code: FsckEvidenceCode::RecordShaMismatch,
                        message: "record payload failed canonical verification".to_string(),
                        path: Some(segment.path.clone()),
                        namespace: Some(segment.namespace.clone()),
                        origin: Some(header.origin_replica_id),
                        seq: Some(header.origin_seq.get()),
                        offset: Some(offset),
                    },
                    Some("rebuild WAL from source of truth or restore from backup"),
                );
                break;
            }
            Err(RecordVerifyError::ShaMismatch { .. }) => {
                builder.record_issue(
                    FsckCheckId::RecordHashes,
                    FsckStatus::Fail,
                    FsckSeverity::High,
                    FsckEvidence {
                        code: FsckEvidenceCode::RecordShaMismatch,
                        message: "record sha256 mismatch".to_string(),
                        path: Some(segment.path.clone()),
                        namespace: Some(segment.namespace.clone()),
                        origin: Some(header.origin_replica_id),
                        seq: Some(header.origin_seq.get()),
                        offset: Some(offset),
                    },
                    Some("rebuild WAL from source of truth or restore from backup"),
                );
                break;
            }
        }

        let context = RecordContext {
            namespace: &segment.namespace,
            origin: header.origin_replica_id,
            seq: header.origin_seq.get(),
            sha: header.sha256,
            prev_sha: header.prev_sha256,
            path: &segment.path,
            offset,
        };
        tracker.observe(&context, builder);

        records += 1;
        offset = offset.saturating_add(frame_len);
    }

    Ok(SegmentScanResult {
        records,
        quarantined,
    })
}

#[derive(Default)]
struct FsckTracker {
    origins: BTreeMap<NamespaceId, BTreeMap<ReplicaId, OriginState>>,
}

struct RecordContext<'a> {
    namespace: &'a NamespaceId,
    origin: ReplicaId,
    seq: u64,
    sha: [u8; 32],
    prev_sha: Option<[u8; 32]>,
    path: &'a Path,
    offset: u64,
}

#[derive(Clone, Debug, Default)]
struct OriginState {
    contiguous_seq: u64,
    max_seq: u64,
    head_sha: Option<[u8; 32]>,
}

impl FsckTracker {
    fn observe(&mut self, context: &RecordContext<'_>, builder: &mut FsckReportBuilder) {
        let entry = self
            .origins
            .entry(context.namespace.clone())
            .or_default()
            .entry(context.origin)
            .or_default();

        let expected = entry.contiguous_seq + 1;
        entry.max_seq = entry.max_seq.max(context.seq);

        if context.seq == 0 {
            builder.record_issue(
                FsckCheckId::OriginContiguity,
                FsckStatus::Fail,
                FsckSeverity::High,
                FsckEvidence {
                    code: FsckEvidenceCode::NonContiguousSeq,
                    message: "origin_seq cannot be zero".to_string(),
                    path: Some(context.path.to_path_buf()),
                    namespace: Some(context.namespace.clone()),
                    origin: Some(context.origin),
                    seq: Some(context.seq),
                    offset: Some(context.offset),
                },
                None,
            );
            return;
        }

        if context.seq == expected {
            if context.seq == 1 && context.prev_sha.is_some() {
                builder.record_issue(
                    FsckCheckId::OriginContiguity,
                    FsckStatus::Fail,
                    FsckSeverity::High,
                    FsckEvidence {
                        code: FsckEvidenceCode::PrevShaMismatch,
                        message: "prev_sha must be empty for origin_seq=1".to_string(),
                        path: Some(context.path.to_path_buf()),
                        namespace: Some(context.namespace.clone()),
                        origin: Some(context.origin),
                        seq: Some(context.seq),
                        offset: Some(context.offset),
                    },
                    None,
                );
            } else if context.seq > 1 && context.prev_sha != entry.head_sha {
                builder.record_issue(
                    FsckCheckId::OriginContiguity,
                    FsckStatus::Fail,
                    FsckSeverity::High,
                    FsckEvidence {
                        code: FsckEvidenceCode::PrevShaMismatch,
                        message: "prev_sha mismatch for contiguous origin_seq".to_string(),
                        path: Some(context.path.to_path_buf()),
                        namespace: Some(context.namespace.clone()),
                        origin: Some(context.origin),
                        seq: Some(context.seq),
                        offset: Some(context.offset),
                    },
                    None,
                );
            }

            entry.contiguous_seq = context.seq;
            entry.head_sha = Some(context.sha);
            return;
        }

        if context.seq > expected {
            builder.record_issue(
                FsckCheckId::OriginContiguity,
                FsckStatus::Fail,
                FsckSeverity::High,
                FsckEvidence {
                    code: FsckEvidenceCode::NonContiguousSeq,
                    message: format!(
                        "non-contiguous origin_seq (expected {expected}, got {})",
                        context.seq
                    ),
                    path: Some(context.path.to_path_buf()),
                    namespace: Some(context.namespace.clone()),
                    origin: Some(context.origin),
                    seq: Some(context.seq),
                    offset: Some(context.offset),
                },
                None,
            );
            entry.contiguous_seq = context.seq;
            entry.head_sha = Some(context.sha);
            return;
        }

        builder.record_issue(
            FsckCheckId::OriginContiguity,
            FsckStatus::Fail,
            FsckSeverity::High,
            FsckEvidence {
                code: FsckEvidenceCode::NonContiguousSeq,
                message: format!(
                    "origin_seq out of order (expected {expected}, got {})",
                    context.seq
                ),
                path: Some(context.path.to_path_buf()),
                namespace: Some(context.namespace.clone()),
                origin: Some(context.origin),
                seq: Some(context.seq),
                offset: Some(context.offset),
            },
            None,
        );
    }
}

fn list_namespaces(
    wal_dir: &Path,
    builder: &mut FsckReportBuilder,
) -> Result<Vec<NamespaceId>, FsckError> {
    reject_symlink(wal_dir)?;
    let entries = match fs::read_dir(wal_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(FsckError::Io {
                path: wal_dir.to_path_buf(),
                source: err,
            });
        }
    };

    let mut namespaces = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| FsckError::Io {
            path: wal_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let entry_type = entry.file_type().map_err(|source| FsckError::Io {
            path: wal_dir.to_path_buf(),
            source,
        })?;
        if entry_type.is_symlink() {
            return Err(FsckError::Io {
                path,
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "wal namespace path is a symlink",
                ),
            });
        }
        if !entry_type.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        match NamespaceId::parse(name) {
            Ok(namespace) => namespaces.push(namespace),
            Err(err) => builder.record_issue(
                FsckCheckId::SegmentHeaders,
                FsckStatus::Warn,
                FsckSeverity::Medium,
                FsckEvidence {
                    code: FsckEvidenceCode::SegmentHeaderInvalid,
                    message: format!("invalid namespace directory: {err}"),
                    path: Some(path),
                    namespace: None,
                    origin: None,
                    seq: None,
                    offset: None,
                },
                None,
            ),
        }
    }
    namespaces.sort();
    Ok(namespaces)
}

fn list_segments(
    wal_dir: &Path,
    namespace: &NamespaceId,
    meta: &StoreMeta,
    builder: &mut FsckReportBuilder,
    repair: bool,
) -> Result<Vec<SegmentInfo>, FsckError> {
    let dir = wal_dir.join(namespace.as_str());
    reject_symlink(&dir)?;
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(FsckError::Io {
                path: dir.clone(),
                source: err,
            });
        }
    };

    let mut segments = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| FsckError::Io {
            path: dir.clone(),
            source,
        })?;
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "wal") {
            continue;
        }

        match read_segment_header(&path) {
            Ok((header, header_len, file_len)) => {
                if header.store_id != meta.store_id() {
                    builder.record_issue(
                        FsckCheckId::SegmentHeaders,
                        FsckStatus::Fail,
                        FsckSeverity::High,
                        FsckEvidence {
                            code: FsckEvidenceCode::SegmentHeaderMismatch,
                            message: "segment store_id mismatch".to_string(),
                            path: Some(path.clone()),
                            namespace: Some(namespace.clone()),
                            origin: None,
                            seq: None,
                            offset: None,
                        },
                        None,
                    );
                }
                if header.store_epoch != meta.store_epoch() {
                    builder.record_issue(
                        FsckCheckId::SegmentHeaders,
                        FsckStatus::Fail,
                        FsckSeverity::High,
                        FsckEvidence {
                            code: FsckEvidenceCode::SegmentHeaderMismatch,
                            message: "segment store_epoch mismatch".to_string(),
                            path: Some(path.clone()),
                            namespace: Some(namespace.clone()),
                            origin: None,
                            seq: None,
                            offset: None,
                        },
                        None,
                    );
                }
                if header.namespace != *namespace {
                    builder.record_issue(
                        FsckCheckId::SegmentHeaders,
                        FsckStatus::Fail,
                        FsckSeverity::High,
                        FsckEvidence {
                            code: FsckEvidenceCode::SegmentHeaderMismatch,
                            message: "segment namespace mismatch".to_string(),
                            path: Some(path.clone()),
                            namespace: Some(namespace.clone()),
                            origin: None,
                            seq: None,
                            offset: None,
                        },
                        None,
                    );
                }

                segments.push(SegmentInfo {
                    path,
                    namespace: namespace.clone(),
                    header,
                    header_len,
                    file_len,
                });
            }
            Err(err) => {
                builder.record_issue(
                    FsckCheckId::SegmentHeaders,
                    FsckStatus::Fail,
                    FsckSeverity::High,
                    FsckEvidence {
                        code: FsckEvidenceCode::SegmentHeaderInvalid,
                        message: format!("segment header invalid: {err}"),
                        path: Some(path.clone()),
                        namespace: Some(namespace.clone()),
                        origin: None,
                        seq: None,
                        offset: None,
                    },
                    Some("run `bd store fsck --repair` to quarantine corrupted segments"),
                );
                if repair && let Ok(quarantined_path) = quarantine_segment(&path) {
                    builder.repairs.push(FsckRepair {
                        kind: FsckRepairKind::QuarantineSegment,
                        path: Some(quarantined_path),
                        detail: "quarantined segment with invalid header".to_string(),
                    });
                }
            }
        }
    }

    segments.sort_by_key(|segment| (segment.header.created_at_ms, segment.header.segment_id));
    Ok(segments)
}

fn reject_symlink(path: &Path) -> Result<(), FsckError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(FsckError::Io {
            path: path.to_path_buf(),
            source: std::io::Error::new(std::io::ErrorKind::InvalidInput, "wal path is a symlink"),
        }),
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(FsckError::Io {
            path: path.to_path_buf(),
            source: err,
        }),
    }
}

fn read_segment_header(path: &Path) -> Result<(SegmentHeader, u64, u64), FsckError> {
    let mut file = fs::File::open(path).map_err(|source| FsckError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut prefix = [0u8; super::segment::SEGMENT_HEADER_PREFIX_LEN];
    file.read_exact(&mut prefix)
        .map_err(|source| FsckError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let header_len = u32::from_le_bytes([prefix[9], prefix[10], prefix[11], prefix[12]]) as usize;
    let file_len = file
        .metadata()
        .map_err(|source| FsckError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .len();
    if header_len as u64 > file_len {
        return Err(FsckError::Io {
            path: path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "segment header length exceeds file length",
            ),
        });
    }

    let mut header_bytes = vec![0u8; header_len];
    header_bytes[..prefix.len()].copy_from_slice(&prefix);
    file.read_exact(&mut header_bytes[prefix.len()..])
        .map_err(|source| FsckError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let header = SegmentHeader::decode(&header_bytes)?;
    Ok((header, header_len as u64, file_len))
}

fn check_index_offsets(
    store_dir: &Path,
    meta: &StoreMeta,
    segments_by_path: &BTreeMap<PathBuf, SegmentInfo>,
    segments_by_id: &BTreeMap<crate::core::SegmentId, SegmentInfo>,
    builder: &mut FsckReportBuilder,
) {
    let index_path = store_dir.join("index").join("wal.sqlite");
    if !index_path.exists() {
        return;
    }

    let mode = match store_index_durability_mode(meta.store_id()) {
        Ok(mode) => mode,
        Err(err) => {
            builder.record_issue(
                FsckCheckId::IndexOffsets,
                FsckStatus::Fail,
                FsckSeverity::High,
                FsckEvidence {
                    code: FsckEvidenceCode::IndexOpenFailed,
                    message: format!("failed to load store config: {err}"),
                    path: Some(index_path),
                    namespace: None,
                    origin: None,
                    seq: None,
                    offset: None,
                },
                Some("fix store_config.toml and re-run fsck"),
            );
            return;
        }
    };

    let index = match SqliteWalIndex::open(store_dir, meta, mode) {
        Ok(index) => index,
        Err(err) => {
            builder.record_issue(
                FsckCheckId::IndexOffsets,
                FsckStatus::Fail,
                FsckSeverity::High,
                FsckEvidence {
                    code: FsckEvidenceCode::IndexOpenFailed,
                    message: format!("failed to open wal index: {err}"),
                    path: Some(index_path),
                    namespace: None,
                    origin: None,
                    seq: None,
                    offset: None,
                },
                Some("rebuild wal.sqlite from WAL segments"),
            );
            return;
        }
    };

    let mut indexed_segment_ids = BTreeSet::new();
    let mut namespaces: BTreeSet<NamespaceId> = segments_by_path
        .values()
        .map(|segment| segment.namespace.clone())
        .collect();
    if let Ok(rows) = index.reader().load_watermarks() {
        namespaces.extend(rows.into_iter().map(|row| row.namespace));
    }
    for namespace in namespaces {
        match index.reader().list_segments(&namespace) {
            Ok(rows) => {
                for row in rows {
                    indexed_segment_ids.insert(row.segment_id);
                    let full_path = store_dir.join(&row.segment_path);
                    let Some(segment) = segments_by_path.get(&full_path).cloned() else {
                        builder.record_issue(
                            FsckCheckId::IndexOffsets,
                            FsckStatus::Fail,
                            FsckSeverity::High,
                            FsckEvidence {
                                code: FsckEvidenceCode::IndexMissingSegment,
                                message: "wal.sqlite references missing segment".to_string(),
                                path: Some(full_path),
                                namespace: Some(namespace.clone()),
                                origin: None,
                                seq: None,
                                offset: None,
                            },
                            Some("rebuild wal.sqlite from WAL segments"),
                        );
                        continue;
                    };

                    if row.segment_id != segment.header.segment_id {
                        builder.record_issue(
                            FsckCheckId::IndexOffsets,
                            FsckStatus::Fail,
                            FsckSeverity::High,
                            FsckEvidence {
                                code: FsckEvidenceCode::IndexOffsetOutOfBounds,
                                message:
                                    "segment_id mismatch between wal.sqlite and segment header"
                                        .to_string(),
                                path: Some(full_path.clone()),
                                namespace: Some(namespace.clone()),
                                origin: None,
                                seq: None,
                                offset: None,
                            },
                            Some("rebuild wal.sqlite from WAL segments"),
                        );
                    }

                    if row.sealed {
                        let Some(final_len) = row.final_len else {
                            builder.record_issue(
                                FsckCheckId::IndexOffsets,
                                FsckStatus::Fail,
                                FsckSeverity::High,
                                FsckEvidence {
                                    code: FsckEvidenceCode::SealedSegmentLenMismatch,
                                    message: "wal.sqlite sealed segment missing final_len"
                                        .to_string(),
                                    path: Some(full_path.clone()),
                                    namespace: Some(namespace.clone()),
                                    origin: None,
                                    seq: None,
                                    offset: None,
                                },
                                Some("rebuild wal.sqlite from WAL segments"),
                            );
                            continue;
                        };
                        if final_len != segment.file_len {
                            builder.record_issue(
                                FsckCheckId::IndexOffsets,
                                FsckStatus::Fail,
                                FsckSeverity::High,
                                FsckEvidence {
                                    code: FsckEvidenceCode::SealedSegmentLenMismatch,
                                    message: format!(
                                        "sealed segment length mismatch (expected {final_len}, got {})",
                                        segment.file_len
                                    ),
                                    path: Some(full_path.clone()),
                                    namespace: Some(namespace.clone()),
                                    origin: None,
                                    seq: None,
                                    offset: None,
                                },
                                Some("rebuild wal.sqlite from WAL segments"),
                            );
                        }
                    }

                    if row.last_indexed_offset > segment.file_len {
                        builder.record_issue(
                            FsckCheckId::IndexOffsets,
                            FsckStatus::Fail,
                            FsckSeverity::High,
                            FsckEvidence {
                                code: FsckEvidenceCode::IndexOffsetOutOfBounds,
                                message: "wal.sqlite last_indexed_offset exceeds segment length"
                                    .to_string(),
                                path: Some(full_path.clone()),
                                namespace: Some(namespace.clone()),
                                origin: None,
                                seq: None,
                                offset: Some(row.last_indexed_offset),
                            },
                            Some("rebuild wal.sqlite from WAL segments"),
                        );
                    } else if row.last_indexed_offset < segment.header_len {
                        builder.record_issue(
                            FsckCheckId::IndexOffsets,
                            FsckStatus::Fail,
                            FsckSeverity::High,
                            FsckEvidence {
                                code: FsckEvidenceCode::IndexOffsetOutOfBounds,
                                message: "wal.sqlite last_indexed_offset precedes segment header"
                                    .to_string(),
                                path: Some(full_path.clone()),
                                namespace: Some(namespace.clone()),
                                origin: None,
                                seq: None,
                                offset: Some(row.last_indexed_offset),
                            },
                            Some("rebuild wal.sqlite from WAL segments"),
                        );
                    } else if row.last_indexed_offset < segment.file_len {
                        builder.record_issue(
                            FsckCheckId::IndexOffsets,
                            FsckStatus::Warn,
                            FsckSeverity::Medium,
                            FsckEvidence {
                                code: FsckEvidenceCode::IndexBehindWal,
                                message: "wal.sqlite last_indexed_offset behind segment length"
                                    .to_string(),
                                path: Some(full_path.clone()),
                                namespace: Some(namespace.clone()),
                                origin: None,
                                seq: None,
                                offset: Some(row.last_indexed_offset),
                            },
                            Some("rebuild wal.sqlite from WAL segments"),
                        );
                    }
                }
            }
            Err(err) => builder.record_issue(
                FsckCheckId::IndexOffsets,
                FsckStatus::Fail,
                FsckSeverity::High,
                FsckEvidence {
                    code: FsckEvidenceCode::IndexOpenFailed,
                    message: format!("failed to read wal.sqlite segments: {err}"),
                    path: Some(index_path.clone()),
                    namespace: Some(namespace),
                    origin: None,
                    seq: None,
                    offset: None,
                },
                Some("rebuild wal.sqlite from WAL segments"),
            ),
        }
    }

    for (segment_id, segment) in segments_by_id {
        if !indexed_segment_ids.contains(segment_id) {
            builder.record_issue(
                FsckCheckId::IndexOffsets,
                FsckStatus::Warn,
                FsckSeverity::Medium,
                FsckEvidence {
                    code: FsckEvidenceCode::IndexMissingSegment,
                    message: "wal.sqlite missing segment entry".to_string(),
                    path: Some(segment.path.clone()),
                    namespace: Some(segment.namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: None,
                },
                Some("rebuild wal.sqlite from WAL segments"),
            );
        }
    }
}

fn truncate_tail(file: &mut std::fs::File, path: &Path, len: u64) -> Result<(), FsckError> {
    file.set_len(len).map_err(|source| FsckError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    file.sync_all().map_err(|source| FsckError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn quarantine_segment(path: &Path) -> Result<PathBuf, FsckError> {
    let Some(parent) = path.parent() else {
        return Err(FsckError::Io {
            path: path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "segment path missing parent",
            ),
        });
    };
    let quarantine_dir = parent.join(QUARANTINE_DIR_NAME);
    fs::create_dir_all(&quarantine_dir).map_err(|source| FsckError::Io {
        path: quarantine_dir.clone(),
        source,
    })?;

    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| "segment.wal".to_string());

    let mut candidate = quarantine_dir.join(&file_name);
    if candidate.exists() {
        let mut counter = 1u32;
        loop {
            let alt = format!("{file_name}.{counter}");
            candidate = quarantine_dir.join(alt);
            if !candidate.exists() {
                break;
            }
            counter += 1;
        }
    }

    fs::rename(path, &candidate).map_err(|source| FsckError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(candidate)
}

fn rebuild_index_after_repair(
    store_dir: &Path,
    meta: &StoreMeta,
    options: &FsckOptions,
) -> Result<(), FsckError> {
    remove_wal_index_files(meta.store_id())?;
    let mode = store_index_durability_mode(meta.store_id())
        .map_err(|err| FsckError::StoreConfig(Box::new(err)))?;
    let index = SqliteWalIndex::open(store_dir, meta, mode)?;
    rebuild_index(store_dir, meta, &index, &options.limits)?;
    Ok(())
}

fn remove_wal_index_files(store_id: StoreId) -> Result<(), FsckError> {
    let db_path = paths::wal_index_path(store_id);
    for suffix in ["", "-wal", "-shm"] {
        let path = if suffix.is_empty() {
            db_path.clone()
        } else {
            PathBuf::from(format!("{}{}", db_path.display(), suffix))
        };
        if path.exists() {
            fs::remove_file(&path).map_err(|source| FsckError::Io {
                path: path.clone(),
                source,
            })?;
        }
    }
    Ok(())
}

fn read_store_meta(path: &Path) -> Result<StoreMeta, FsckError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(FsckError::Io {
                path: path.to_path_buf(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "store meta path is a symlink",
                ),
            });
        }
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(FsckError::StoreMetaMissing {
                path: path.to_path_buf(),
            });
        }
        Err(err) => {
            return Err(FsckError::StoreMetaRead {
                path: path.to_path_buf(),
                source: err,
            });
        }
    }

    let bytes = fs::read(path).map_err(|source| FsckError::StoreMetaRead {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(|source| FsckError::StoreMetaParse {
        path: path.to_path_buf(),
        source,
    })
}

struct FsckReportBuilder {
    store_id: StoreId,
    checked_at_ms: u64,
    stats: FsckStats,
    checks: BTreeMap<FsckCheckId, FsckCheckBuilder>,
    repairs: Vec<FsckRepair>,
}

#[derive(Clone, Debug)]
struct FsckCheckBuilder {
    status: FsckStatus,
    severity: FsckSeverity,
    evidence: Vec<FsckEvidence>,
    suggested_actions: BTreeSet<String>,
}

impl FsckReportBuilder {
    fn new(store_id: StoreId, checked_at_ms: u64) -> Self {
        let mut checks = BTreeMap::new();
        for id in [
            FsckCheckId::SegmentHeaders,
            FsckCheckId::SegmentFrames,
            FsckCheckId::RecordHashes,
            FsckCheckId::OriginContiguity,
            FsckCheckId::IndexOffsets,
            FsckCheckId::CheckpointCache,
        ] {
            checks.insert(
                id,
                FsckCheckBuilder {
                    status: FsckStatus::Pass,
                    severity: FsckSeverity::Low,
                    evidence: Vec::new(),
                    suggested_actions: BTreeSet::new(),
                },
            );
        }

        Self {
            store_id,
            checked_at_ms,
            stats: FsckStats::default(),
            checks,
            repairs: Vec::new(),
        }
    }

    fn record_issue(
        &mut self,
        id: FsckCheckId,
        status: FsckStatus,
        severity: FsckSeverity,
        evidence: FsckEvidence,
        suggested_action: Option<&str>,
    ) {
        let check = self.checks.get_mut(&id).expect("fsck check missing");
        check.status = std::cmp::max(check.status, status);
        check.severity = std::cmp::max(check.severity, severity);
        check.evidence.push(evidence);
        if let Some(action) = suggested_action {
            check.suggested_actions.insert(action.to_string());
        }
    }

    fn finish(self) -> FsckReport {
        let mut checks = Vec::with_capacity(self.checks.len());
        for (id, builder) in self.checks {
            let mut suggested_actions: Vec<String> =
                builder.suggested_actions.into_iter().collect();
            suggested_actions.sort();
            checks.push(FsckCheck {
                id,
                status: builder.status,
                severity: builder.severity,
                evidence: builder.evidence,
                suggested_actions,
            });
        }

        let mut risk = FsckRisk::Low;
        let mut safe_to_accept_writes = true;
        let mut safe_to_prune_wal = true;
        let mut safe_to_rebuild_index = true;
        for check in &checks {
            if check.status == FsckStatus::Fail {
                safe_to_accept_writes = false;
            }
            if check.status == FsckStatus::Fail {
                match check.id {
                    FsckCheckId::SegmentFrames | FsckCheckId::RecordHashes => {
                        safe_to_prune_wal = false;
                        safe_to_rebuild_index = false;
                    }
                    FsckCheckId::IndexOffsets => {
                        safe_to_prune_wal = false;
                    }
                    _ => {}
                }
            }
            if check.status != FsckStatus::Pass {
                risk = std::cmp::max(risk, FsckRisk::from(check.severity));
            }
        }

        FsckReport {
            store_id: self.store_id,
            checked_at_ms: self.checked_at_ms,
            stats: self.stats,
            checks,
            summary: FsckSummary {
                risk,
                safe_to_accept_writes,
                safe_to_prune_wal,
                safe_to_rebuild_index,
            },
            repairs: self.repairs,
        }
    }
}
