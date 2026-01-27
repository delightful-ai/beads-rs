//! WAL segment header and append/rotation logic.

#[cfg(test)]
use std::cell::Cell;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crc32c::crc32c;
use rand::RngCore;
use uuid::Uuid;

use crate::core::{NamespaceId, SegmentId, StoreEpoch, StoreId, StoreMeta, StoreMetaVersions};

use super::frame::encode_frame;
use super::record::VerifiedRecord;
use super::{EventWalError, EventWalResult};

pub(crate) const SEGMENT_MAGIC: &[u8; 5] = b"BDWAL";
/// Current WAL format version encoded in segment headers.
pub const WAL_FORMAT_VERSION: u32 = StoreMetaVersions::WAL_FORMAT_VERSION;
/// Bytes in the fixed prefix of the segment header.
pub const SEGMENT_HEADER_PREFIX_LEN: usize = SEGMENT_MAGIC.len() + 8;

#[derive(Clone, Copy, Debug)]
pub struct SegmentConfig {
    pub max_record_bytes: usize,
    pub max_segment_bytes: u64,
    pub max_segment_age_ms: u64,
    pub sync_mode: SegmentSyncMode,
}

impl SegmentConfig {
    pub fn new(max_record_bytes: usize, max_segment_bytes: u64, max_segment_age_ms: u64) -> Self {
        Self {
            max_record_bytes,
            max_segment_bytes,
            max_segment_age_ms,
            sync_mode: SegmentSyncMode::Data,
        }
    }

    pub fn from_limits(limits: &crate::core::Limits) -> Self {
        let mut config = Self {
            max_record_bytes: limits.max_wal_record_bytes,
            max_segment_bytes: limits.wal_segment_max_bytes as u64,
            max_segment_age_ms: limits.wal_segment_max_age_ms,
            sync_mode: SegmentSyncMode::Data,
        };
        if let Some(sync_mode) = sync_mode_override() {
            config.sync_mode = sync_mode;
        }
        config
    }

    pub fn with_sync_mode(mut self, sync_mode: SegmentSyncMode) -> Self {
        self.sync_mode = sync_mode;
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentSyncMode {
    None,
    Data,
    All,
}

fn sync_mode_override() -> Option<SegmentSyncMode> {
    if let Ok(raw) = std::env::var("BD_WAL_SYNC_MODE") {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }
        if let Some(mode) = parse_sync_mode(trimmed) {
            return Some(mode);
        }
        tracing::warn!("invalid BD_WAL_SYNC_MODE, ignoring: {raw}");
        return None;
    }
    if std::env::var_os("BD_TEST_FAST").is_some() {
        return Some(SegmentSyncMode::None);
    }
    None
}

fn parse_sync_mode(raw: &str) -> Option<SegmentSyncMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "none" | "off" | "0" => Some(SegmentSyncMode::None),
        "data" | "fdatasync" | "1" => Some(SegmentSyncMode::Data),
        "all" | "fsync" | "2" => Some(SegmentSyncMode::All),
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
    pub namespace: NamespaceId,
    pub wal_format_version: u32,
    pub created_at_ms: u64,
    pub segment_id: SegmentId,
    pub flags: u32,
}

impl SegmentHeader {
    pub fn new(
        meta: &StoreMeta,
        namespace: NamespaceId,
        created_at_ms: u64,
        segment_id: SegmentId,
    ) -> Self {
        Self {
            store_id: meta.store_id(),
            store_epoch: meta.store_epoch(),
            namespace,
            wal_format_version: WAL_FORMAT_VERSION,
            created_at_ms,
            segment_id,
            flags: 0,
        }
    }

    pub fn encode(&self) -> EventWalResult<Vec<u8>> {
        let namespace_bytes = self.namespace.as_str().as_bytes();
        let header_len = segment_header_len(namespace_bytes.len())?;
        let header_len_u32 =
            u32::try_from(header_len).map_err(|_| EventWalError::SegmentHeaderInvalid {
                reason: "segment header too large".to_string(),
            })?;

        let mut buf = Vec::with_capacity(header_len);
        buf.extend_from_slice(SEGMENT_MAGIC);
        buf.extend_from_slice(&self.wal_format_version.to_le_bytes());
        buf.extend_from_slice(&header_len_u32.to_le_bytes());
        buf.extend_from_slice(self.store_id.as_uuid().as_bytes());
        buf.extend_from_slice(&self.store_epoch.get().to_le_bytes());
        buf.extend_from_slice(&(namespace_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(namespace_bytes);
        buf.extend_from_slice(&self.created_at_ms.to_le_bytes());
        buf.extend_from_slice(self.segment_id.as_uuid().as_bytes());
        buf.extend_from_slice(&self.flags.to_le_bytes());

        let crc = crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        Ok(buf)
    }

    pub fn decode(bytes: &[u8]) -> EventWalResult<Self> {
        if bytes.len() < SEGMENT_MAGIC.len() + 8 {
            return Err(EventWalError::SegmentHeaderInvalid {
                reason: "segment header truncated".to_string(),
            });
        }

        if &bytes[..SEGMENT_MAGIC.len()] != SEGMENT_MAGIC {
            let mut got = [0u8; 5];
            got.copy_from_slice(&bytes[..SEGMENT_MAGIC.len()]);
            return Err(EventWalError::SegmentHeaderMagicMismatch { got });
        }

        let mut offset = SEGMENT_MAGIC.len();
        let wal_format_version = read_u32_le(bytes, &mut offset)?;
        if wal_format_version != WAL_FORMAT_VERSION {
            return Err(EventWalError::SegmentHeaderUnsupportedVersion {
                got: wal_format_version,
                supported: WAL_FORMAT_VERSION,
            });
        }
        let header_len = read_u32_le(bytes, &mut offset)? as usize;
        if header_len > bytes.len() {
            return Err(EventWalError::SegmentHeaderInvalid {
                reason: "segment header length exceeds buffer".to_string(),
            });
        }
        if header_len < segment_header_len(0)? {
            return Err(EventWalError::SegmentHeaderInvalid {
                reason: "segment header length too small".to_string(),
            });
        }

        let header_bytes = &bytes[..header_len];
        let expected_crc_offset =
            header_len
                .checked_sub(4)
                .ok_or_else(|| EventWalError::SegmentHeaderInvalid {
                    reason: "segment header missing crc".to_string(),
                })?;
        if expected_crc_offset > header_bytes.len() {
            return Err(EventWalError::SegmentHeaderInvalid {
                reason: "segment header crc out of bounds".to_string(),
            });
        }

        let crc_bytes = &header_bytes[expected_crc_offset..header_len];
        let expected_crc =
            u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
        let actual_crc = crc32c(&header_bytes[..expected_crc_offset]);
        if actual_crc != expected_crc {
            return Err(EventWalError::SegmentHeaderCrcMismatch {
                expected: expected_crc,
                got: actual_crc,
            });
        }

        let store_id = StoreId::new(read_uuid(header_bytes, &mut offset)?);
        let store_epoch = StoreEpoch::new(read_u64_le(header_bytes, &mut offset)?);
        let namespace_len = read_u32_le(header_bytes, &mut offset)? as usize;
        let min_len = segment_header_len(namespace_len)?;
        if header_len < min_len {
            return Err(EventWalError::SegmentHeaderInvalid {
                reason: "segment header length smaller than namespace length".to_string(),
            });
        }
        let namespace = read_namespace(header_bytes, &mut offset, namespace_len)?;
        let created_at_ms = read_u64_le(header_bytes, &mut offset)?;
        let segment_id = SegmentId::new(read_uuid(header_bytes, &mut offset)?);
        let flags = read_u32_le(header_bytes, &mut offset)?;

        Ok(Self {
            store_id,
            store_epoch,
            namespace,
            wal_format_version,
            created_at_ms,
            segment_id,
            flags,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedSegment {
    pub segment_id: SegmentId,
    pub path: PathBuf,
    pub created_at_ms: u64,
    pub final_len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendOutcome {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub len: u32,
    pub rotated: bool,
    pub sealed: Option<SealedSegment>,
}

pub struct SegmentWriter {
    dir: PathBuf,
    config: SegmentConfig,
    file: File,
    path: PathBuf,
    header: SegmentHeader,
    bytes_written: u64,
}

#[cfg(test)]
thread_local! {
    static LAST_SYNC_MODE: Cell<Option<SegmentSyncMode>> = const { Cell::new(None) };
}

#[cfg(test)]
fn record_sync_mode(mode: SegmentSyncMode) {
    LAST_SYNC_MODE.with(|cell| cell.set(Some(mode)));
}

#[cfg(not(test))]
fn record_sync_mode(_mode: SegmentSyncMode) {}

fn sync_segment(file: &File, path: &Path, mode: SegmentSyncMode) -> EventWalResult<()> {
    record_sync_mode(mode);
    let result = match mode {
        SegmentSyncMode::All => file.sync_all(),
        SegmentSyncMode::Data => file.sync_data(),
        SegmentSyncMode::None => return Ok(()),
    };
    result.map_err(|source| EventWalError::Io {
        path: Some(path.to_path_buf()),
        source,
    })
}

impl SegmentWriter {
    pub fn open(
        store_dir: &Path,
        meta: &StoreMeta,
        namespace: &NamespaceId,
        now_ms: u64,
        config: SegmentConfig,
    ) -> EventWalResult<Self> {
        let wal_dir = store_dir.join("wal");
        reject_symlink(&wal_dir)?;
        fs::create_dir_all(&wal_dir).map_err(|source| EventWalError::Io {
            path: Some(wal_dir.clone()),
            source,
        })?;
        reject_symlink(&wal_dir)?;
        ensure_dir_permissions(&wal_dir)?;

        let dir = wal_dir.join(namespace.as_str());
        reject_symlink(&dir)?;
        fs::create_dir_all(&dir).map_err(|source| EventWalError::Io {
            path: Some(dir.clone()),
            source,
        })?;
        reject_symlink(&dir)?;
        ensure_dir_permissions(&dir)?;

        let header = SegmentHeader::new(meta, namespace.clone(), now_ms, new_segment_id());
        let (file, path, header_len) = create_segment(&dir, &header)?;

        Ok(Self {
            dir,
            config,
            file,
            path,
            header,
            bytes_written: header_len,
        })
    }

    pub fn current_path(&self) -> &Path {
        &self.path
    }

    pub fn current_segment_id(&self) -> SegmentId {
        self.header.segment_id
    }

    pub fn current_created_at_ms(&self) -> u64 {
        self.header.created_at_ms
    }

    pub fn update_config(&mut self, config: SegmentConfig) {
        self.config = config;
    }

    pub fn append(
        &mut self,
        record: &VerifiedRecord,
        now_ms: u64,
    ) -> EventWalResult<AppendOutcome> {
        let frame = encode_frame(record, self.config.max_record_bytes)?;

        let rotated = self.should_rotate(now_ms, frame.len() as u64);
        let sealed = if rotated {
            Some(SealedSegment {
                segment_id: self.header.segment_id,
                path: self.path.clone(),
                created_at_ms: self.header.created_at_ms,
                final_len: self.bytes_written,
            })
        } else {
            None
        };
        if rotated {
            self.rotate(now_ms)?;
        }

        let offset = self.bytes_written;
        self.file
            .write_all(&frame)
            .map_err(|source| EventWalError::Io {
                path: Some(self.path.clone()),
                source,
            })?;
        crate::daemon::test_hooks::maybe_pause("wal_after_write");
        // LocalFsync: fsync record data; metadata fsync happens on segment creation/rotation.
        sync_segment(&self.file, &self.path, self.config.sync_mode)?;
        self.bytes_written = self
            .bytes_written
            .checked_add(frame.len() as u64)
            .ok_or_else(|| EventWalError::FrameLengthInvalid {
                reason: "segment size overflow".to_string(),
            })?;

        Ok(AppendOutcome {
            segment_id: self.header.segment_id,
            offset,
            len: frame.len() as u32,
            rotated,
            sealed,
        })
    }

    pub fn flush(&mut self) -> EventWalResult<()> {
        sync_segment(&self.file, &self.path, self.config.sync_mode)
    }

    fn should_rotate(&self, now_ms: u64, next_len: u64) -> bool {
        if self.config.max_segment_bytes > 0
            && self.bytes_written.saturating_add(next_len) > self.config.max_segment_bytes
        {
            return true;
        }
        if self.config.max_segment_age_ms > 0
            && now_ms.saturating_sub(self.header.created_at_ms) >= self.config.max_segment_age_ms
        {
            return true;
        }
        false
    }

    fn rotate(&mut self, now_ms: u64) -> EventWalResult<()> {
        let header = SegmentHeader {
            store_id: self.header.store_id,
            store_epoch: self.header.store_epoch,
            namespace: self.header.namespace.clone(),
            wal_format_version: WAL_FORMAT_VERSION,
            created_at_ms: now_ms,
            segment_id: new_segment_id(),
            flags: 0,
        };
        let (file, path, header_len) = create_segment(&self.dir, &header)?;
        self.file = file;
        self.path = path;
        self.header = header;
        self.bytes_written = header_len;
        Ok(())
    }
}

fn create_segment(dir: &Path, header: &SegmentHeader) -> EventWalResult<(File, PathBuf, u64)> {
    reject_symlink(dir)?;
    let file_name = segment_file_name(header.created_at_ms, header.segment_id);
    let tmp_name = format!("{file_name}.tmp");
    let tmp_path = dir.join(&tmp_name);
    let final_path = dir.join(&file_name);

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)
        .map_err(|source| EventWalError::Io {
            path: Some(tmp_path.clone()),
            source,
        })?;

    let header_bytes = header.encode()?;
    file.write_all(&header_bytes)
        .map_err(|source| EventWalError::Io {
            path: Some(tmp_path.clone()),
            source,
        })?;
    file.sync_all().map_err(|source| EventWalError::Io {
        path: Some(tmp_path.clone()),
        source,
    })?;

    fs::rename(&tmp_path, &final_path).map_err(|source| EventWalError::Io {
        path: Some(final_path.clone()),
        source,
    })?;
    fsync_dir(dir)?;

    let file = OpenOptions::new()
        .append(true)
        .open(&final_path)
        .map_err(|source| EventWalError::Io {
            path: Some(final_path.clone()),
            source,
        })?;

    Ok((file, final_path, header_bytes.len() as u64))
}

fn fsync_dir(dir: &Path) -> EventWalResult<()> {
    let file = File::open(dir).map_err(|source| EventWalError::Io {
        path: Some(dir.to_path_buf()),
        source,
    })?;
    file.sync_all().map_err(|source| EventWalError::Io {
        path: Some(dir.to_path_buf()),
        source,
    })?;
    Ok(())
}

fn reject_symlink(path: &Path) -> EventWalResult<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(EventWalError::Symlink {
            path: path.to_path_buf(),
        }),
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(EventWalError::Io {
            path: Some(path.to_path_buf()),
            source: err,
        }),
    }
}

fn ensure_dir_permissions(path: &Path) -> EventWalResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|source| {
            EventWalError::Io {
                path: Some(path.to_path_buf()),
                source,
            }
        })?;
    }
    Ok(())
}

fn segment_file_name(created_at_ms: u64, segment_id: SegmentId) -> String {
    format!("segment-{}-{}.wal", created_at_ms, segment_id)
}

fn new_segment_id() -> SegmentId {
    let mut rng = rand::rng();
    let mut bytes = [0u8; 16];
    rng.fill_bytes(&mut bytes);
    SegmentId::new(Uuid::from_bytes(bytes))
}

fn segment_header_len(namespace_len: usize) -> EventWalResult<usize> {
    let header_len = SEGMENT_MAGIC.len() + 4 + 4 + 16 + 8 + 4 + namespace_len + 8 + 16 + 4 + 4;
    if header_len > u32::MAX as usize {
        return Err(EventWalError::SegmentHeaderInvalid {
            reason: "segment header too large".to_string(),
        });
    }
    Ok(header_len)
}

fn read_namespace(bytes: &[u8], offset: &mut usize, len: usize) -> EventWalResult<NamespaceId> {
    let slice = take(bytes, offset, len)?;
    let namespace =
        std::str::from_utf8(slice).map_err(|_| EventWalError::SegmentHeaderInvalid {
            reason: "namespace bytes not utf-8".to_string(),
        })?;
    NamespaceId::parse(namespace).map_err(|err| EventWalError::SegmentHeaderInvalid {
        reason: err.to_string(),
    })
}

fn read_u32_le(bytes: &[u8], offset: &mut usize) -> EventWalResult<u32> {
    let slice = take(bytes, offset, 4)?;
    Ok(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

fn read_u64_le(bytes: &[u8], offset: &mut usize) -> EventWalResult<u64> {
    let slice = take(bytes, offset, 8)?;
    Ok(u64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]))
}

fn read_uuid(bytes: &[u8], offset: &mut usize) -> EventWalResult<Uuid> {
    let slice = take(bytes, offset, 16)?;
    let mut arr = [0u8; 16];
    arr.copy_from_slice(slice);
    Ok(Uuid::from_bytes(arr))
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> EventWalResult<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| EventWalError::SegmentHeaderInvalid {
            reason: "segment header length overflow".to_string(),
        })?;
    if end > bytes.len() {
        return Err(EventWalError::SegmentHeaderInvalid {
            reason: "segment header truncated".to_string(),
        });
    }
    let slice = &bytes[*offset..end];
    *offset = end;
    Ok(slice)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};
    use tempfile::TempDir;

    fn test_meta(store_id: StoreId, store_epoch: StoreEpoch) -> StoreMeta {
        let identity = crate::core::StoreIdentity::new(store_id, store_epoch);
        let versions = crate::core::StoreMetaVersions::new(1, 2, 3, 4, 5);
        StoreMeta::new(
            identity,
            crate::core::ReplicaId::new(Uuid::from_bytes([9u8; 16])),
            versions,
            1_700_000_000_000,
        )
    }

    fn test_record() -> VerifiedRecord {
        let limits = crate::core::Limits::default();
        let header = crate::daemon::wal::record::RecordHeader {
            origin_replica_id: crate::core::ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            origin_seq: crate::core::Seq1::from_u64(1).unwrap(),
            event_time_ms: 1_700_000_000_100,
            txn_id: crate::core::TxnId::new(Uuid::from_bytes([2u8; 16])),
            request: None,
            sha256: [0u8; 32],
            prev_sha256: None,
        };
        let body = crate::core::EventBody {
            envelope_v: 1,
            store: crate::core::StoreIdentity::new(
                crate::core::StoreId::new(Uuid::from_bytes([9u8; 16])),
                crate::core::StoreEpoch::new(1),
            ),
            namespace: NamespaceId::core(),
            origin_replica_id: header.origin_replica_id,
            origin_seq: header.origin_seq,
            event_time_ms: header.event_time_ms,
            txn_id: header.txn_id,
            client_request_id: header.client_request_id(),
            trace_id: None,
            kind: crate::core::EventKindV1::TxnV1(crate::core::TxnV1 {
                delta: crate::core::TxnDeltaV1::new(),
                hlc_max: crate::core::HlcMax {
                    actor_id: crate::core::ActorId::new("alice").unwrap(),
                    physical_ms: header.event_time_ms,
                    logical: 0,
                },
            }),
        };
        let body = body.into_validated(&limits).expect("validated");
        let payload = crate::core::encode_event_body_canonical(body.as_ref()).expect("payload");
        let sha = crate::core::hash_event_body(&payload).0;
        let mut header = header;
        header.sha256 = sha;
        VerifiedRecord::new(header, payload, body).expect("verified record")
    }

    fn take_sync_mode() -> Option<SegmentSyncMode> {
        LAST_SYNC_MODE.with(|cell| cell.replace(None))
    }

    #[test]
    fn append_uses_data_fsync() {
        let temp = TempDir::new().unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([6u8; 16]));
        let meta = test_meta(store_id, StoreEpoch::new(1));
        let namespace = NamespaceId::core();
        let mut writer = SegmentWriter::open(
            temp.path(),
            &meta,
            &namespace,
            10,
            SegmentConfig::new(1024, 1024, 60_000),
        )
        .unwrap();

        let record = test_record();
        let _ = take_sync_mode();
        writer.append(&record, 10).unwrap();
        assert_eq!(take_sync_mode(), Some(SegmentSyncMode::Data));
    }

    #[cfg(unix)]
    #[test]
    fn segment_open_rejects_symlinked_namespace_dir() {
        let temp = TempDir::new().unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([5u8; 16]));
        let meta = test_meta(store_id, StoreEpoch::new(1));
        let namespace = NamespaceId::core();

        let wal_dir = temp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        let target = temp.path().join("real-namespace");
        fs::create_dir_all(&target).unwrap();
        let ns_dir = wal_dir.join(namespace.as_str());
        symlink(&target, &ns_dir).unwrap();

        let err = match SegmentWriter::open(
            temp.path(),
            &meta,
            &namespace,
            10,
            SegmentConfig::new(1024, 1024, 60_000),
        ) {
            Ok(_) => panic!("expected symlink rejection"),
            Err(err) => err,
        };
        assert!(matches!(err, EventWalError::Symlink { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn segment_open_sets_dir_permissions() {
        let temp = TempDir::new().unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([4u8; 16]));
        let meta = test_meta(store_id, StoreEpoch::new(1));
        let namespace = NamespaceId::core();
        let _writer = SegmentWriter::open(
            temp.path(),
            &meta,
            &namespace,
            10,
            SegmentConfig::new(1024, 1024, 60_000),
        )
        .unwrap();

        let wal_dir = temp.path().join("wal");
        let wal_mode = fs::metadata(&wal_dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(wal_mode, 0o700);

        let ns_dir = wal_dir.join(namespace.as_str());
        let ns_mode = fs::metadata(&ns_dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(ns_mode, 0o700);
    }

    #[test]
    fn segment_rotates_on_size() {
        let temp = TempDir::new().unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
        let meta = test_meta(store_id, StoreEpoch::new(1));
        let namespace = NamespaceId::core();
        let record = test_record();
        let frame_len = encode_frame(&record, 1024).expect("frame").len() as u64;
        let header_len =
            SegmentHeader::new(&meta, namespace.clone(), 10, SegmentId::new(Uuid::nil()))
                .encode()
                .expect("header")
                .len() as u64;
        let max_segment_bytes = header_len + frame_len + 1;
        let mut writer = SegmentWriter::open(
            temp.path(),
            &meta,
            &namespace,
            10,
            SegmentConfig::new(1024, max_segment_bytes, 60_000),
        )
        .unwrap();

        let first = writer.append(&record, 10).unwrap();
        assert!(!first.rotated);
        let first_name = writer
            .current_path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert!(first_name.starts_with("segment-10-"));
        assert!(first_name.ends_with(&format!("{}.wal", first.segment_id)));

        let second = writer.append(&record, 10).unwrap();
        assert!(second.rotated);
        let sealed = second.sealed.as_ref().expect("sealed segment info");
        assert_eq!(sealed.segment_id, first.segment_id);
        let sealed_len = fs::metadata(&sealed.path).expect("sealed metadata").len();
        assert_eq!(sealed.final_len, sealed_len);
        assert_ne!(first.segment_id, second.segment_id);
        let second_name = writer
            .current_path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert!(second_name.starts_with("segment-10-"));
        assert!(second_name.ends_with(&format!("{}.wal", second.segment_id)));
    }

    #[test]
    fn record_size_limit_enforced() {
        let temp = TempDir::new().unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([8u8; 16]));
        let meta = test_meta(store_id, StoreEpoch::new(1));
        let namespace = NamespaceId::core();
        let mut writer = SegmentWriter::open(
            temp.path(),
            &meta,
            &namespace,
            10,
            SegmentConfig::new(8, 1024, 60_000),
        )
        .unwrap();

        let record = test_record();
        assert!(record.payload_bytes().len() > 8);

        let err = writer.append(&record, 10).unwrap_err();
        assert!(matches!(err, EventWalError::RecordTooLarge { .. }));
    }

    #[test]
    fn segment_header_roundtrip() {
        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let meta = test_meta(store_id, StoreEpoch::new(7));
        let namespace = NamespaceId::core();
        let header = SegmentHeader::new(
            &meta,
            namespace.clone(),
            1_700_000_000_000,
            new_segment_id(),
        );
        let bytes = header.encode().unwrap();
        let decoded = SegmentHeader::decode(&bytes).unwrap();
        assert_eq!(decoded.store_id, store_id);
        assert_eq!(decoded.store_epoch, StoreEpoch::new(7));
        assert_eq!(decoded.namespace, namespace);
    }
}
