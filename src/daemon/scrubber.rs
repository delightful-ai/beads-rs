//! Online WAL scrubber for admin.doctor/admin.scrub_now.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::api::{
    AdminHealthCheck, AdminHealthCheckId, AdminHealthEvidence, AdminHealthEvidenceCode,
    AdminHealthReport, AdminHealthRisk, AdminHealthSeverity, AdminHealthStats, AdminHealthStatus,
    AdminHealthSummary,
};
use crate::core::{Limits, NamespaceId, ReplicaId, SegmentId, decode_event_body, sha256_bytes};
use crate::daemon::store_runtime::StoreRuntime;
use crate::daemon::wal::WalIndex;
use crate::daemon::wal::frame::{FRAME_HEADER_LEN, FRAME_MAGIC};
use crate::daemon::wal::record::{Record, validate_header_matches_body};
use crate::daemon::wal::segment::{SEGMENT_HEADER_PREFIX_LEN, SegmentHeader};
use crate::git::checkpoint::CheckpointCache;
use crate::paths;

const DEFAULT_MAX_RECORDS: usize = 200;
const INDEX_BATCH_RECORDS: usize = 16;

#[derive(Clone, Debug)]
pub struct ScrubOptions {
    pub max_records_per_namespace: usize,
    pub verify_checkpoint_cache: bool,
}

impl ScrubOptions {
    pub fn new(max_records_per_namespace: usize, verify_checkpoint_cache: bool) -> Self {
        Self {
            max_records_per_namespace: max_records_per_namespace.max(1),
            verify_checkpoint_cache,
        }
    }

    pub fn default_for_scrub() -> Self {
        Self::new(DEFAULT_MAX_RECORDS, false)
    }

    pub fn default_for_doctor() -> Self {
        Self::new(DEFAULT_MAX_RECORDS, true)
    }
}

#[derive(Clone, Debug)]
struct SegmentInfo {
    namespace: NamespaceId,
    path: PathBuf,
    header_len: u64,
    file_len: u64,
    created_at_ms: u64,
    segment_id: SegmentId,
}

#[derive(Debug)]
struct SegmentHandle {
    file: File,
    len: u64,
}

pub fn scrub_store(
    store: &StoreRuntime,
    limits: &Limits,
    checkpoint_groups: &[String],
    options: ScrubOptions,
) -> AdminHealthReport {
    let checked_at_ms = crate::WallClock::now().0;
    let store_id = store.meta.store_id();
    let store_dir = paths::store_dir(store_id);
    let wal_dir = store_dir.join("wal");

    let mut builder = ScrubReportBuilder::new(checked_at_ms);
    let namespaces = collect_namespaces(store, &wal_dir, &mut builder);
    builder.stats.namespaces = namespaces.len();

    for namespace in &namespaces {
        let segments = list_segments(&wal_dir, namespace, &mut builder);
        scrub_wal_segments(
            &segments,
            limits,
            options.max_records_per_namespace,
            &mut builder,
        );
        scrub_index_offsets(
            store,
            namespace,
            limits,
            options.max_records_per_namespace,
            &mut builder,
        );
    }

    if options.verify_checkpoint_cache {
        builder.stats.checkpoint_groups_checked = checkpoint_groups.len();
        for group in checkpoint_groups {
            verify_checkpoint_cache(store_id, group, &mut builder);
        }
    }

    builder.finish()
}

fn collect_namespaces(
    store: &StoreRuntime,
    wal_dir: &Path,
    builder: &mut ScrubReportBuilder,
) -> Vec<NamespaceId> {
    let mut namespaces = BTreeSet::new();
    namespaces.extend(store.policies.keys().cloned());
    namespaces.extend(store.watermarks_applied.namespaces().cloned());

    for namespace in list_wal_namespaces(wal_dir, builder) {
        namespaces.insert(namespace);
    }

    namespaces.into_iter().collect()
}

fn list_wal_namespaces(wal_dir: &Path, builder: &mut ScrubReportBuilder) -> Vec<NamespaceId> {
    let entries = match std::fs::read_dir(wal_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(err) => {
            builder.record_issue(
                AdminHealthCheckId::WalFrames,
                AdminHealthStatus::Fail,
                AdminHealthSeverity::High,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::SegmentHeaderInvalid,
                    message: format!("failed to read wal directory: {err}"),
                    path: Some(wal_dir.display().to_string()),
                    namespace: None,
                    origin: None,
                    seq: None,
                    offset: None,
                    segment_id: None,
                },
                Some("investigate wal directory permissions"),
            );
            return Vec::new();
        }
    };

    let mut namespaces = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                builder.record_issue(
                    AdminHealthCheckId::WalFrames,
                    AdminHealthStatus::Warn,
                    AdminHealthSeverity::Medium,
                    AdminHealthEvidence {
                        code: AdminHealthEvidenceCode::SegmentHeaderInvalid,
                        message: format!("failed to read wal directory entry: {err}"),
                        path: Some(wal_dir.display().to_string()),
                        namespace: None,
                        origin: None,
                        seq: None,
                        offset: None,
                        segment_id: None,
                    },
                    None,
                );
                continue;
            }
        };
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        match NamespaceId::parse(name) {
            Ok(namespace) => namespaces.push(namespace),
            Err(err) => builder.record_issue(
                AdminHealthCheckId::WalFrames,
                AdminHealthStatus::Warn,
                AdminHealthSeverity::Medium,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::SegmentHeaderInvalid,
                    message: format!("invalid wal namespace directory: {err}"),
                    path: Some(path.display().to_string()),
                    namespace: None,
                    origin: None,
                    seq: None,
                    offset: None,
                    segment_id: None,
                },
                None,
            ),
        }
    }
    namespaces.sort();
    namespaces
}

fn list_segments(
    wal_dir: &Path,
    namespace: &NamespaceId,
    builder: &mut ScrubReportBuilder,
) -> Vec<SegmentInfo> {
    let dir = wal_dir.join(namespace.as_str());
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(err) => {
            builder.record_issue(
                AdminHealthCheckId::WalFrames,
                AdminHealthStatus::Fail,
                AdminHealthSeverity::High,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::SegmentHeaderInvalid,
                    message: format!("failed to read wal namespace dir: {err}"),
                    path: Some(dir.display().to_string()),
                    namespace: Some(namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: None,
                    segment_id: None,
                },
                Some("investigate wal directory permissions"),
            );
            return Vec::new();
        }
    };

    let mut segments = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                builder.record_issue(
                    AdminHealthCheckId::WalFrames,
                    AdminHealthStatus::Warn,
                    AdminHealthSeverity::Medium,
                    AdminHealthEvidence {
                        code: AdminHealthEvidenceCode::SegmentHeaderInvalid,
                        message: format!("failed to read wal segment entry: {err}"),
                        path: Some(dir.display().to_string()),
                        namespace: Some(namespace.clone()),
                        origin: None,
                        seq: None,
                        offset: None,
                        segment_id: None,
                    },
                    None,
                );
                continue;
            }
        };
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        match read_segment_header(&path) {
            Ok((header, header_len, file_len)) => {
                if header.namespace != *namespace {
                    builder.record_issue(
                        AdminHealthCheckId::WalFrames,
                        AdminHealthStatus::Fail,
                        AdminHealthSeverity::High,
                        AdminHealthEvidence {
                            code: AdminHealthEvidenceCode::SegmentHeaderInvalid,
                            message: format!(
                                "segment header namespace mismatch (expected {}, got {})",
                                namespace, header.namespace
                            ),
                            path: Some(path.display().to_string()),
                            namespace: Some(namespace.clone()),
                            origin: None,
                            seq: None,
                            offset: None,
                            segment_id: Some(header.segment_id),
                        },
                        Some("run `bd admin maintenance on` then `bd store fsck --repair` to quarantine corrupted segments"),
                    );
                    continue;
                }

                segments.push(SegmentInfo {
                    namespace: namespace.clone(),
                    path,
                    header_len,
                    file_len,
                    created_at_ms: header.created_at_ms,
                    segment_id: header.segment_id,
                });
            }
            Err(err) => {
                builder.record_issue(
                    AdminHealthCheckId::WalFrames,
                    AdminHealthStatus::Fail,
                    AdminHealthSeverity::High,
                    AdminHealthEvidence {
                        code: AdminHealthEvidenceCode::SegmentHeaderInvalid,
                        message: format!("segment header decode failed: {err}"),
                        path: Some(path.display().to_string()),
                        namespace: Some(namespace.clone()),
                        origin: None,
                        seq: None,
                        offset: None,
                        segment_id: None,
                    },
                    Some("run `bd admin maintenance on` then `bd store fsck --repair` to quarantine corrupted segments"),
                );
            }
        }
    }

    segments.sort_by(|a, b| {
        a.created_at_ms
            .cmp(&b.created_at_ms)
            .then_with(|| a.segment_id.cmp(&b.segment_id))
    });
    segments
}

fn read_segment_header(path: &Path) -> Result<(SegmentHeader, u64, u64), std::io::Error> {
    let mut file = File::open(path)?;
    let mut prefix = [0u8; SEGMENT_HEADER_PREFIX_LEN];
    file.read_exact(&mut prefix)?;
    let header_len = u32::from_le_bytes([prefix[9], prefix[10], prefix[11], prefix[12]]) as usize;
    let file_len = file.metadata()?.len();
    if header_len as u64 > file_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "segment header length exceeds file length",
        ));
    }
    let mut header_bytes = vec![0u8; header_len];
    header_bytes[..prefix.len()].copy_from_slice(&prefix);
    file.read_exact(&mut header_bytes[prefix.len()..])?;
    SegmentHeader::decode(&header_bytes)
        .map(|header| (header, header_len as u64, file_len))
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))
}

fn scrub_wal_segments(
    segments: &[SegmentInfo],
    limits: &Limits,
    max_records: usize,
    builder: &mut ScrubReportBuilder,
) {
    let mut remaining = max_records;
    for segment in segments {
        if remaining == 0 {
            break;
        }
        builder.stats.segments_checked = builder.stats.segments_checked.saturating_add(1);
        let checked = scan_segment_records(segment, limits, remaining, builder);
        remaining = remaining.saturating_sub(checked);
    }
}

fn scan_segment_records(
    segment: &SegmentInfo,
    limits: &Limits,
    max_records: usize,
    builder: &mut ScrubReportBuilder,
) -> usize {
    let mut file = match File::open(&segment.path) {
        Ok(file) => file,
        Err(err) => {
            builder.record_issue(
                AdminHealthCheckId::WalFrames,
                AdminHealthStatus::Fail,
                AdminHealthSeverity::High,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::SegmentHeaderInvalid,
                    message: format!("failed to open wal segment: {err}"),
                    path: Some(segment.path.display().to_string()),
                    namespace: Some(segment.namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: None,
                    segment_id: Some(segment.segment_id),
                },
                Some("run `bd admin maintenance on` then `bd store fsck --repair` to quarantine corrupted segments"),
            );
            return 0;
        }
    };

    if let Err(err) = file.seek(SeekFrom::Start(segment.header_len)) {
        builder.record_issue(
            AdminHealthCheckId::WalFrames,
            AdminHealthStatus::Fail,
            AdminHealthSeverity::High,
            AdminHealthEvidence {
                code: AdminHealthEvidenceCode::SegmentHeaderInvalid,
                message: format!("failed to seek wal segment: {err}"),
                path: Some(segment.path.display().to_string()),
                namespace: Some(segment.namespace.clone()),
                origin: None,
                seq: None,
                offset: Some(segment.header_len),
                segment_id: Some(segment.segment_id),
            },
            Some("run `bd admin maintenance on` then `bd store fsck --repair` to quarantine corrupted segments"),
        );
        return 0;
    }

    let max_record_bytes = limits.max_wal_record_bytes.min(limits.max_frame_bytes);
    let mut offset = segment.header_len;
    let mut checked = 0usize;

    while offset < segment.file_len && checked < max_records {
        let remaining = segment.file_len.saturating_sub(offset);
        if remaining < FRAME_HEADER_LEN as u64 {
            builder.record_issue(
                AdminHealthCheckId::WalFrames,
                AdminHealthStatus::Warn,
                AdminHealthSeverity::Medium,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::FrameTruncated,
                    message: "truncated wal frame header".to_string(),
                    path: Some(segment.path.display().to_string()),
                    namespace: Some(segment.namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: Some(offset),
                    segment_id: Some(segment.segment_id),
                },
                Some("run `bd admin maintenance on` then `bd store fsck --repair` to truncate tail corruption"),
            );
            break;
        }

        let mut header = [0u8; FRAME_HEADER_LEN];
        if let Err(err) = file.read_exact(&mut header) {
            builder.record_issue(
                AdminHealthCheckId::WalFrames,
                AdminHealthStatus::Warn,
                AdminHealthSeverity::Medium,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::FrameTruncated,
                    message: format!("failed to read wal frame header: {err}"),
                    path: Some(segment.path.display().to_string()),
                    namespace: Some(segment.namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: Some(offset),
                    segment_id: Some(segment.segment_id),
                },
                Some("run `bd admin maintenance on` then `bd store fsck --repair` to truncate tail corruption"),
            );
            break;
        }

        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let length = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let expected_crc = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

        if magic != FRAME_MAGIC || length == 0 {
            builder.record_issue(
                AdminHealthCheckId::WalFrames,
                AdminHealthStatus::Fail,
                AdminHealthSeverity::High,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::FrameHeaderInvalid,
                    message: format!(
                        "invalid wal frame header (magic={magic:#x}, length={length})"
                    ),
                    path: Some(segment.path.display().to_string()),
                    namespace: Some(segment.namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: Some(offset),
                    segment_id: Some(segment.segment_id),
                },
                Some("run `bd admin maintenance on` then `bd store fsck --repair` to quarantine corrupted segments"),
            );
            break;
        }
        if length as usize > max_record_bytes {
            builder.record_issue(
                AdminHealthCheckId::WalFrames,
                AdminHealthStatus::Fail,
                AdminHealthSeverity::High,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::FrameHeaderInvalid,
                    message: format!("wal frame length too large ({length} bytes)"),
                    path: Some(segment.path.display().to_string()),
                    namespace: Some(segment.namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: Some(offset),
                    segment_id: Some(segment.segment_id),
                },
                Some("run `bd admin maintenance on` then `bd store fsck --repair` to quarantine corrupted segments"),
            );
            break;
        }

        let frame_len = FRAME_HEADER_LEN as u64 + length as u64;
        if frame_len > remaining {
            builder.record_issue(
                AdminHealthCheckId::WalFrames,
                AdminHealthStatus::Warn,
                AdminHealthSeverity::Medium,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::FrameTruncated,
                    message: "wal frame truncated".to_string(),
                    path: Some(segment.path.display().to_string()),
                    namespace: Some(segment.namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: Some(offset),
                    segment_id: Some(segment.segment_id),
                },
                Some("run `bd admin maintenance on` then `bd store fsck --repair` to truncate tail corruption"),
            );
            break;
        }

        let mut body = vec![0u8; length as usize];
        if let Err(err) = file.read_exact(&mut body) {
            builder.record_issue(
                AdminHealthCheckId::WalFrames,
                AdminHealthStatus::Warn,
                AdminHealthSeverity::Medium,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::FrameTruncated,
                    message: format!("failed to read wal frame body: {err}"),
                    path: Some(segment.path.display().to_string()),
                    namespace: Some(segment.namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: Some(offset),
                    segment_id: Some(segment.segment_id),
                },
                Some("run `bd admin maintenance on` then `bd store fsck --repair` to truncate tail corruption"),
            );
            break;
        }

        let actual_crc = crc32c::crc32c(&body);
        if actual_crc != expected_crc {
            builder.record_issue(
                AdminHealthCheckId::WalFrames,
                AdminHealthStatus::Fail,
                AdminHealthSeverity::High,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::FrameCrcMismatch,
                    message: format!(
                        "wal frame crc mismatch (expected {expected_crc}, got {actual_crc})"
                    ),
                    path: Some(segment.path.display().to_string()),
                    namespace: Some(segment.namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: Some(offset),
                    segment_id: Some(segment.segment_id),
                },
                Some("run `bd admin maintenance on` then `bd store fsck --repair` to quarantine corrupted segments"),
            );
            break;
        }

        let record = match Record::decode_body(&body) {
            Ok(record) => record,
            Err(err) => {
                builder.record_issue(
                    AdminHealthCheckId::WalFrames,
                    AdminHealthStatus::Fail,
                    AdminHealthSeverity::High,
                    AdminHealthEvidence {
                        code: AdminHealthEvidenceCode::RecordDecodeInvalid,
                        message: format!("wal record decode failed: {err}"),
                        path: Some(segment.path.display().to_string()),
                        namespace: Some(segment.namespace.clone()),
                        origin: None,
                        seq: None,
                        offset: Some(offset),
                        segment_id: Some(segment.segment_id),
                    },
                    Some("run `bd admin maintenance on` then `bd store fsck --repair` to quarantine corrupted segments"),
                );
                break;
            }
        };

        let (_, event_body) = match decode_event_body(record.payload.as_ref(), limits) {
            Ok(decoded) => decoded,
            Err(err) => {
                builder.record_issue(
                    AdminHealthCheckId::WalFrames,
                    AdminHealthStatus::Fail,
                    AdminHealthSeverity::High,
                    AdminHealthEvidence {
                        code: AdminHealthEvidenceCode::EventBodyDecodeInvalid,
                        message: format!("wal event body decode failed: {err}"),
                        path: Some(segment.path.display().to_string()),
                        namespace: Some(segment.namespace.clone()),
                        origin: Some(record.header.origin_replica_id),
                        seq: Some(record.header.origin_seq),
                        offset: Some(offset),
                        segment_id: Some(segment.segment_id),
                    },
                    Some("run `bd admin maintenance on` then `bd store fsck --repair` to quarantine corrupted segments"),
                );
                break;
            }
        };

        if let Err(err) = validate_header_matches_body(&record.header, &event_body) {
            builder.record_issue(
                AdminHealthCheckId::WalHashes,
                AdminHealthStatus::Fail,
                AdminHealthSeverity::High,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::RecordHeaderMismatch,
                    message: format!("wal record header mismatch: {err}"),
                    path: Some(segment.path.display().to_string()),
                    namespace: Some(segment.namespace.clone()),
                    origin: Some(record.header.origin_replica_id),
                    seq: Some(record.header.origin_seq),
                    offset: Some(offset),
                    segment_id: Some(segment.segment_id),
                },
                Some("run `bd admin maintenance on` then `bd store fsck --repair` to quarantine corrupted segments"),
            );
            break;
        }

        let expected_sha = sha256_bytes(record.payload.as_ref()).0;
        if expected_sha != record.header.sha256 {
            builder.record_issue(
                AdminHealthCheckId::WalHashes,
                AdminHealthStatus::Fail,
                AdminHealthSeverity::High,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::RecordShaMismatch,
                    message: "wal record sha256 mismatch".to_string(),
                    path: Some(segment.path.display().to_string()),
                    namespace: Some(segment.namespace.clone()),
                    origin: Some(record.header.origin_replica_id),
                    seq: Some(record.header.origin_seq),
                    offset: Some(offset),
                    segment_id: Some(segment.segment_id),
                },
                Some("run `bd admin maintenance on` then `bd store fsck --repair` to quarantine corrupted segments"),
            );
            break;
        }

        builder.stats.records_checked = builder.stats.records_checked.saturating_add(1);
        checked = checked.saturating_add(1);
        offset = offset.saturating_add(frame_len);
    }

    checked
}

fn scrub_index_offsets(
    store: &StoreRuntime,
    namespace: &NamespaceId,
    limits: &Limits,
    max_records: usize,
    builder: &mut ScrubReportBuilder,
) {
    let reader = store.wal_index.reader();
    let segments = match reader.list_segments(namespace) {
        Ok(segments) => segments,
        Err(err) => {
            builder.record_issue(
                AdminHealthCheckId::IndexOffsets,
                AdminHealthStatus::Fail,
                AdminHealthSeverity::High,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::IndexOpenFailed,
                    message: format!("failed to read wal.sqlite segments: {err}"),
                    path: None,
                    namespace: Some(namespace.clone()),
                    origin: None,
                    seq: None,
                    offset: None,
                    segment_id: None,
                },
                Some("run `bd admin maintenance on` then `bd admin rebuild-index`"),
            );
            return;
        }
    };

    let mut segment_paths = BTreeMap::new();
    for segment in segments {
        segment_paths.insert(segment.segment_id, segment.segment_path);
    }

    if segment_paths.is_empty() {
        return;
    }

    let mut remaining = max_records;
    let max_record_bytes = limits.max_wal_record_bytes.min(limits.max_frame_bytes);
    let mut handles: HashMap<SegmentId, SegmentHandle> = HashMap::new();

    for (origin, _) in store.watermarks_applied.origins(namespace) {
        if remaining == 0 {
            break;
        }
        let mut from_seq_excl = 0u64;
        while remaining > 0 {
            let batch_records = remaining.min(INDEX_BATCH_RECORDS);
            let max_bytes = max_record_bytes.saturating_mul(batch_records);
            let items = match reader.iter_from(namespace, origin, from_seq_excl, max_bytes) {
                Ok(items) => items,
                Err(err) => {
                    builder.record_issue(
                        AdminHealthCheckId::IndexOffsets,
                        AdminHealthStatus::Fail,
                        AdminHealthSeverity::High,
                        AdminHealthEvidence {
                            code: AdminHealthEvidenceCode::IndexOpenFailed,
                            message: format!("failed to read wal.sqlite entries: {err}"),
                            path: None,
                            namespace: Some(namespace.clone()),
                            origin: Some(*origin),
                            seq: None,
                            offset: None,
                            segment_id: None,
                        },
                        Some("run `bd admin maintenance on` then `bd admin rebuild-index`"),
                    );
                    break;
                }
            };

            if items.is_empty() {
                break;
            }

            for item in items {
                if remaining == 0 {
                    break;
                }
                builder.stats.index_offsets_checked =
                    builder.stats.index_offsets_checked.saturating_add(1);
                verify_index_offset(
                    namespace,
                    origin,
                    &item,
                    max_record_bytes,
                    &segment_paths,
                    &mut handles,
                    builder,
                );
                from_seq_excl = item.event_id.origin_seq.get();
                remaining = remaining.saturating_sub(1);
            }
        }
    }
}

fn verify_index_offset(
    namespace: &NamespaceId,
    origin: &ReplicaId,
    item: &crate::daemon::wal::index::IndexedRangeItem,
    max_record_bytes: usize,
    segment_paths: &BTreeMap<SegmentId, PathBuf>,
    handles: &mut HashMap<SegmentId, SegmentHandle>,
    builder: &mut ScrubReportBuilder,
) {
    let path = match segment_paths.get(&item.segment_id) {
        Some(path) => path.clone(),
        None => {
            builder.record_issue(
                AdminHealthCheckId::IndexOffsets,
                AdminHealthStatus::Fail,
                AdminHealthSeverity::High,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::IndexSegmentMissing,
                    message: "wal.sqlite references missing segment".to_string(),
                    path: None,
                    namespace: Some(namespace.clone()),
                    origin: Some(*origin),
                    seq: Some(item.event_id.origin_seq.get()),
                    offset: Some(item.offset),
                    segment_id: Some(item.segment_id),
                },
                Some("run `bd admin maintenance on` then `bd admin rebuild-index`"),
            );
            return;
        }
    };

    let handle = match handles.entry(item.segment_id) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => match File::open(&path) {
            Ok(file) => {
                let len = match file.metadata() {
                    Ok(meta) => meta.len(),
                    Err(_) => 0,
                };
                entry.insert(SegmentHandle { file, len })
            }
            Err(err) => {
                builder.record_issue(
                    AdminHealthCheckId::IndexOffsets,
                    AdminHealthStatus::Fail,
                    AdminHealthSeverity::High,
                    AdminHealthEvidence {
                        code: AdminHealthEvidenceCode::IndexOffsetInvalid,
                        message: format!("failed to open wal segment: {err}"),
                        path: Some(path.display().to_string()),
                        namespace: Some(namespace.clone()),
                        origin: Some(*origin),
                        seq: Some(item.event_id.origin_seq.get()),
                        offset: Some(item.offset),
                        segment_id: Some(item.segment_id),
                    },
                    Some("run `bd admin maintenance on` then `bd admin rebuild-index`"),
                );
                return;
            }
        },
    };

    if item.offset >= handle.len {
        builder.record_issue(
            AdminHealthCheckId::IndexOffsets,
            AdminHealthStatus::Fail,
            AdminHealthSeverity::High,
            AdminHealthEvidence {
                code: AdminHealthEvidenceCode::IndexOffsetInvalid,
                message: "wal.sqlite offset beyond segment length".to_string(),
                path: Some(path.display().to_string()),
                namespace: Some(namespace.clone()),
                origin: Some(*origin),
                seq: Some(item.event_id.origin_seq.get()),
                offset: Some(item.offset),
                segment_id: Some(item.segment_id),
            },
            Some("run `bd admin maintenance on` then `bd admin rebuild-index`"),
        );
        return;
    }

    if let Err(err) = handle.file.seek(SeekFrom::Start(item.offset)) {
        builder.record_issue(
            AdminHealthCheckId::IndexOffsets,
            AdminHealthStatus::Fail,
            AdminHealthSeverity::High,
            AdminHealthEvidence {
                code: AdminHealthEvidenceCode::IndexOffsetInvalid,
                message: format!("failed to seek wal segment: {err}"),
                path: Some(path.display().to_string()),
                namespace: Some(namespace.clone()),
                origin: Some(*origin),
                seq: Some(item.event_id.origin_seq.get()),
                offset: Some(item.offset),
                segment_id: Some(item.segment_id),
            },
            Some("run `bd admin maintenance on` then `bd admin rebuild-index`"),
        );
        return;
    }

    let mut header = [0u8; FRAME_HEADER_LEN];
    if let Err(err) = handle.file.read_exact(&mut header) {
        builder.record_issue(
            AdminHealthCheckId::IndexOffsets,
            AdminHealthStatus::Fail,
            AdminHealthSeverity::High,
            AdminHealthEvidence {
                code: AdminHealthEvidenceCode::IndexOffsetInvalid,
                message: format!("failed to read wal frame header: {err}"),
                path: Some(path.display().to_string()),
                namespace: Some(namespace.clone()),
                origin: Some(*origin),
                seq: Some(item.event_id.origin_seq.get()),
                offset: Some(item.offset),
                segment_id: Some(item.segment_id),
            },
            Some("run `bd admin maintenance on` then `bd admin rebuild-index`"),
        );
        return;
    }

    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let length = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    let expected_crc = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

    if magic != FRAME_MAGIC || length == 0 || length as usize > max_record_bytes {
        builder.record_issue(
            AdminHealthCheckId::IndexOffsets,
            AdminHealthStatus::Fail,
            AdminHealthSeverity::High,
            AdminHealthEvidence {
                code: AdminHealthEvidenceCode::IndexOffsetInvalid,
                message: "wal.sqlite offset does not point to valid frame header".to_string(),
                path: Some(path.display().to_string()),
                namespace: Some(namespace.clone()),
                origin: Some(*origin),
                seq: Some(item.event_id.origin_seq.get()),
                offset: Some(item.offset),
                segment_id: Some(item.segment_id),
            },
            Some("run `bd admin maintenance on` then `bd admin rebuild-index`"),
        );
        return;
    }

    let frame_len = FRAME_HEADER_LEN as u64 + length as u64;
    if item.offset.saturating_add(frame_len) > handle.len {
        builder.record_issue(
            AdminHealthCheckId::IndexOffsets,
            AdminHealthStatus::Fail,
            AdminHealthSeverity::High,
            AdminHealthEvidence {
                code: AdminHealthEvidenceCode::IndexOffsetInvalid,
                message: "wal.sqlite frame extends past segment length".to_string(),
                path: Some(path.display().to_string()),
                namespace: Some(namespace.clone()),
                origin: Some(*origin),
                seq: Some(item.event_id.origin_seq.get()),
                offset: Some(item.offset),
                segment_id: Some(item.segment_id),
            },
            Some("run `bd admin maintenance on` then `bd admin rebuild-index`"),
        );
        return;
    }

    let mut body = vec![0u8; length as usize];
    if let Err(err) = handle.file.read_exact(&mut body) {
        builder.record_issue(
            AdminHealthCheckId::IndexOffsets,
            AdminHealthStatus::Fail,
            AdminHealthSeverity::High,
            AdminHealthEvidence {
                code: AdminHealthEvidenceCode::IndexOffsetInvalid,
                message: format!("failed to read wal frame body: {err}"),
                path: Some(path.display().to_string()),
                namespace: Some(namespace.clone()),
                origin: Some(*origin),
                seq: Some(item.event_id.origin_seq.get()),
                offset: Some(item.offset),
                segment_id: Some(item.segment_id),
            },
            Some("run `bd admin maintenance on` then `bd admin rebuild-index`"),
        );
        return;
    }

    let actual_crc = crc32c::crc32c(&body);
    if actual_crc != expected_crc {
        builder.record_issue(
            AdminHealthCheckId::IndexOffsets,
            AdminHealthStatus::Fail,
            AdminHealthSeverity::High,
            AdminHealthEvidence {
                code: AdminHealthEvidenceCode::IndexOffsetInvalid,
                message: "wal.sqlite offset frame crc mismatch".to_string(),
                path: Some(path.display().to_string()),
                namespace: Some(namespace.clone()),
                origin: Some(*origin),
                seq: Some(item.event_id.origin_seq.get()),
                offset: Some(item.offset),
                segment_id: Some(item.segment_id),
            },
            Some("run `bd admin maintenance on` then `bd admin rebuild-index`"),
        );
        return;
    }

    if let Err(err) = Record::decode_body(&body) {
        builder.record_issue(
            AdminHealthCheckId::IndexOffsets,
            AdminHealthStatus::Fail,
            AdminHealthSeverity::High,
            AdminHealthEvidence {
                code: AdminHealthEvidenceCode::IndexOffsetInvalid,
                message: format!("wal record decode failed: {err}"),
                path: Some(path.display().to_string()),
                namespace: Some(namespace.clone()),
                origin: Some(*origin),
                seq: Some(item.event_id.origin_seq.get()),
                offset: Some(item.offset),
                segment_id: Some(item.segment_id),
            },
            Some("run `bd admin maintenance on` then `bd admin rebuild-index`"),
        );
    }
}

fn verify_checkpoint_cache(
    store_id: crate::core::StoreId,
    group: &str,
    builder: &mut ScrubReportBuilder,
) {
    let cache = CheckpointCache::new(store_id, group.to_string());
    match cache.load_current() {
        Ok(_) => {}
        Err(err) => {
            builder.record_issue(
                AdminHealthCheckId::CheckpointCache,
                AdminHealthStatus::Fail,
                AdminHealthSeverity::Medium,
                AdminHealthEvidence {
                    code: AdminHealthEvidenceCode::CheckpointCacheInvalid,
                    message: format!("checkpoint cache invalid for group {group}: {err}"),
                    path: None,
                    namespace: None,
                    origin: None,
                    seq: None,
                    offset: None,
                    segment_id: None,
                },
                Some("delete the checkpoint cache entry and re-export a checkpoint"),
            );
        }
    }
}

struct ScrubReportBuilder {
    checked_at_ms: u64,
    stats: AdminHealthStats,
    checks: BTreeMap<AdminHealthCheckId, CheckBuilder>,
}

#[derive(Clone, Debug)]
struct CheckBuilder {
    status: AdminHealthStatus,
    severity: AdminHealthSeverity,
    evidence: Vec<AdminHealthEvidence>,
    suggested_actions: BTreeSet<String>,
}

impl ScrubReportBuilder {
    fn new(checked_at_ms: u64) -> Self {
        let mut checks = BTreeMap::new();
        for id in [
            AdminHealthCheckId::WalFrames,
            AdminHealthCheckId::WalHashes,
            AdminHealthCheckId::IndexOffsets,
            AdminHealthCheckId::CheckpointCache,
        ] {
            checks.insert(
                id,
                CheckBuilder {
                    status: AdminHealthStatus::Pass,
                    severity: AdminHealthSeverity::Low,
                    evidence: Vec::new(),
                    suggested_actions: BTreeSet::new(),
                },
            );
        }

        Self {
            checked_at_ms,
            stats: AdminHealthStats::default(),
            checks,
        }
    }

    fn record_issue(
        &mut self,
        id: AdminHealthCheckId,
        status: AdminHealthStatus,
        severity: AdminHealthSeverity,
        evidence: AdminHealthEvidence,
        suggested_action: Option<&str>,
    ) {
        let check = self.checks.get_mut(&id).expect("missing scrub check");
        check.status = std::cmp::max(check.status, status);
        check.severity = std::cmp::max(check.severity, severity);
        check.evidence.push(evidence);
        if let Some(action) = suggested_action {
            check.suggested_actions.insert(action.to_string());
        }
    }

    fn finish(self) -> AdminHealthReport {
        let mut checks = Vec::with_capacity(self.checks.len());
        for (id, builder) in self.checks {
            let mut suggested_actions: Vec<String> =
                builder.suggested_actions.into_iter().collect();
            suggested_actions.sort();
            checks.push(AdminHealthCheck {
                id,
                status: builder.status,
                severity: builder.severity,
                evidence: builder.evidence,
                suggested_actions,
            });
        }

        let mut risk = AdminHealthRisk::Low;
        let mut safe_to_accept_writes = true;
        let mut safe_to_prune_wal = true;
        let mut safe_to_rebuild_index = true;
        for check in &checks {
            if check.status == AdminHealthStatus::Fail {
                safe_to_accept_writes = false;
            }
            if check.status == AdminHealthStatus::Fail {
                match check.id {
                    AdminHealthCheckId::WalFrames | AdminHealthCheckId::WalHashes => {
                        safe_to_prune_wal = false;
                        safe_to_rebuild_index = false;
                    }
                    AdminHealthCheckId::IndexOffsets => {
                        safe_to_prune_wal = false;
                    }
                    _ => {}
                }
            }
            if check.status != AdminHealthStatus::Pass {
                risk = std::cmp::max(risk, AdminHealthRisk::from(check.severity));
            }
        }

        AdminHealthReport {
            checked_at_ms: self.checked_at_ms,
            stats: self.stats,
            checks,
            summary: AdminHealthSummary {
                risk,
                safe_to_accept_writes,
                safe_to_prune_wal,
                safe_to_rebuild_index,
            },
        }
    }
}

impl From<AdminHealthSeverity> for AdminHealthRisk {
    fn from(value: AdminHealthSeverity) -> Self {
        match value {
            AdminHealthSeverity::Low => AdminHealthRisk::Low,
            AdminHealthSeverity::Medium => AdminHealthRisk::Medium,
            AdminHealthSeverity::High => AdminHealthRisk::High,
            AdminHealthSeverity::Critical => AdminHealthRisk::Critical,
        }
    }
}
