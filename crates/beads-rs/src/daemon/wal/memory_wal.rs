//! In-memory WAL backend for deterministic tests.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use crate::core::{NamespaceId, SegmentId, StoreEpoch, StoreId, StoreMeta};

use super::frame::encode_frame;
use super::segment::SealedSegment;
use super::segment::{SegmentHeader, WAL_FORMAT_VERSION};
use super::{AppendOutcome, EventWalError, EventWalResult, SegmentConfig, VerifiedRecord};

const MEMORY_DIR_NAME: &str = ".mem";

#[derive(Debug)]
struct MemorySegment {
    bytes: Vec<u8>,
}

#[derive(Default)]
struct MemoryWalRegistry {
    segments: BTreeMap<PathBuf, Arc<Mutex<MemorySegment>>>,
}

thread_local! {
    static REGISTRY: RefCell<MemoryWalRegistry> = RefCell::new(MemoryWalRegistry::default());
}

pub(crate) fn read_segment_bytes(path: &Path) -> Option<Vec<u8>> {
    REGISTRY.with(|r| {
        let guard = r.borrow();
        let segment = guard.segments.get(path)?;
        let segment = segment.lock().expect("memory wal segment poisoned");
        Some(segment.bytes.clone())
    })
}

fn register_segment(path: PathBuf, segment: Arc<Mutex<MemorySegment>>) {
    REGISTRY.with(|r| {
        r.borrow_mut().segments.insert(path, segment);
    });
}

fn purge_segments_for_store(store_dir: &Path) {
    let prefix = store_dir.join(MEMORY_DIR_NAME);
    REGISTRY.with(|r| {
        r.borrow_mut()
            .segments
            .retain(|path, _| !path.starts_with(&prefix));
    });
}

pub struct MemoryEventWal {
    store_dir: PathBuf,
    meta: StoreMeta,
    config: SegmentConfig,
    writers: BTreeMap<NamespaceId, MemorySegmentWriter>,
}

impl MemoryEventWal {
    pub fn new(store_dir: PathBuf, meta: StoreMeta, config: SegmentConfig) -> Self {
        Self {
            store_dir,
            meta,
            config,
            writers: BTreeMap::new(),
        }
    }

    pub fn append(
        &mut self,
        namespace: &NamespaceId,
        record: &VerifiedRecord,
        now_ms: u64,
    ) -> EventWalResult<AppendOutcome> {
        let writer = self.writer_mut(namespace, now_ms)?;
        writer.append(record, now_ms)
    }

    pub fn flush(
        &mut self,
        namespace: &NamespaceId,
    ) -> EventWalResult<Option<super::SegmentSnapshot>> {
        let Some(writer) = self.writers.get_mut(namespace) else {
            return Ok(None);
        };
        writer.flush()?;
        Ok(Some(writer.snapshot()))
    }

    pub fn segment_snapshot(&self, namespace: &NamespaceId) -> Option<super::SegmentSnapshot> {
        self.writers.get(namespace).map(|writer| writer.snapshot())
    }

    pub fn update_config(&mut self, config: SegmentConfig) {
        self.config = config;
        for writer in self.writers.values_mut() {
            writer.update_config(config);
        }
    }

    fn writer_mut(
        &mut self,
        namespace: &NamespaceId,
        now_ms: u64,
    ) -> EventWalResult<&mut MemorySegmentWriter> {
        if !self.writers.contains_key(namespace) {
            let writer = MemorySegmentWriter::open(
                &self.store_dir,
                &self.meta,
                namespace,
                now_ms,
                self.config,
            )?;
            self.writers.insert(namespace.clone(), writer);
        }
        Ok(self
            .writers
            .get_mut(namespace)
            .expect("memory writer missing after insert"))
    }
}

impl Drop for MemoryEventWal {
    fn drop(&mut self) {
        purge_segments_for_store(&self.store_dir);
    }
}

struct MemorySegmentWriter {
    store_dir: PathBuf,
    config: SegmentConfig,
    store_id: StoreId,
    store_epoch: StoreEpoch,
    header: SegmentHeader,
    path: PathBuf,
    segment: Arc<Mutex<MemorySegment>>,
    bytes_written: u64,
}

impl MemorySegmentWriter {
    fn open(
        store_dir: &Path,
        meta: &StoreMeta,
        namespace: &NamespaceId,
        now_ms: u64,
        config: SegmentConfig,
    ) -> EventWalResult<Self> {
        let header = SegmentHeader::new(meta, namespace.clone(), now_ms, new_segment_id());
        let header_bytes = header.encode()?;
        let path = memory_segment_path(store_dir, header.created_at_ms, header.segment_id);
        let segment = Arc::new(Mutex::new(MemorySegment {
            bytes: header_bytes.clone(),
        }));
        register_segment(path.clone(), Arc::clone(&segment));
        Ok(Self {
            store_dir: store_dir.to_path_buf(),
            config,
            store_id: meta.store_id(),
            store_epoch: meta.store_epoch(),
            header,
            path,
            segment,
            bytes_written: header_bytes.len() as u64,
        })
    }

    fn snapshot(&self) -> super::SegmentSnapshot {
        super::SegmentSnapshot {
            segment_id: self.header.segment_id,
            created_at_ms: self.header.created_at_ms,
            path: self.path.clone(),
        }
    }

    fn update_config(&mut self, config: SegmentConfig) {
        self.config = config;
    }

    fn append(&mut self, record: &VerifiedRecord, now_ms: u64) -> EventWalResult<AppendOutcome> {
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
        {
            let mut segment = self.segment.lock().expect("memory wal segment poisoned");
            segment.bytes.extend_from_slice(&frame);
        }
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

    fn flush(&mut self) -> EventWalResult<()> {
        Ok(())
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
            store_id: self.store_id,
            store_epoch: self.store_epoch,
            namespace: self.header.namespace.clone(),
            wal_format_version: WAL_FORMAT_VERSION,
            created_at_ms: now_ms,
            segment_id: new_segment_id(),
            flags: 0,
        };
        let header_bytes = header.encode()?;
        let path = memory_segment_path(&self.store_dir, header.created_at_ms, header.segment_id);
        let segment = Arc::new(Mutex::new(MemorySegment {
            bytes: header_bytes.clone(),
        }));
        register_segment(path.clone(), Arc::clone(&segment));
        self.header = header;
        self.path = path;
        self.segment = segment;
        self.bytes_written = header_bytes.len() as u64;
        Ok(())
    }
}

fn memory_segment_path(store_dir: &Path, created_at_ms: u64, segment_id: SegmentId) -> PathBuf {
    store_dir
        .join(MEMORY_DIR_NAME)
        .join(segment_file_name(created_at_ms, segment_id))
}

fn segment_file_name(created_at_ms: u64, segment_id: SegmentId) -> String {
    format!("segment-{}-{}.wal", created_at_ms, segment_id)
}

fn new_segment_id() -> SegmentId {
    SegmentId::new(Uuid::new_v4())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Seek, SeekFrom};
    use tempfile::TempDir;

    use crate::core::{
        ActorId, NamespaceId, ReplicaId, SegmentId, Seq1, StoreEpoch, StoreId, StoreIdentity,
        StoreMeta, StoreMetaVersions, TxnId,
    };
    use crate::daemon::wal::segment::SegmentHeader;
    use crate::daemon::wal::{RecordHeader, RequestProof};

    fn read_record_at_path(
        path: &Path,
        offset: u64,
        max_record_bytes: usize,
    ) -> EventWalResult<super::super::UnverifiedRecord> {
        let Some(bytes) = read_segment_bytes(path) else {
            return Err(EventWalError::Io {
                path: Some(path.to_path_buf()),
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "segment not in memory"),
            });
        };
        let mut cursor = Cursor::new(bytes);
        cursor
            .seek(SeekFrom::Start(offset))
            .map_err(|source| EventWalError::Io {
                path: Some(path.to_path_buf()),
                source,
            })?;
        let mut reader = crate::daemon::wal::frame::FrameReader::new(cursor, max_record_bytes);
        reader
            .read_next()
            .map_err(|err| match err {
                EventWalError::Io { source, .. } => EventWalError::Io {
                    path: Some(path.to_path_buf()),
                    source,
                },
                other => other,
            })?
            .ok_or_else(|| EventWalError::FrameLengthInvalid {
                reason: "unexpected eof while reading record".to_string(),
            })
    }

    fn test_meta(store_id: StoreId) -> StoreMeta {
        let identity = StoreIdentity::new(store_id, StoreEpoch::new(1));
        let versions = StoreMetaVersions::new(1, 2, 3, 4, 5);
        StoreMeta::new(
            identity,
            ReplicaId::new(Uuid::from_bytes([9u8; 16])),
            versions,
            1_700_000_000_000,
        )
    }

    fn test_record() -> VerifiedRecord {
        let limits = crate::core::Limits::default();
        let body = crate::core::EventBody {
            envelope_v: 1,
            store: StoreIdentity::new(
                StoreId::new(Uuid::from_bytes([9u8; 16])),
                StoreEpoch::new(1),
            ),
            namespace: NamespaceId::core(),
            origin_replica_id: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            origin_seq: Seq1::from_u64(1).unwrap(),
            event_time_ms: 1_700_000_000_100,
            txn_id: TxnId::new(Uuid::from_bytes([2u8; 16])),
            client_request_id: None,
            trace_id: None,
            kind: crate::core::EventKindV1::TxnV1(crate::core::TxnV1 {
                delta: crate::core::TxnDeltaV1::new(),
                hlc_max: crate::core::HlcMax {
                    actor_id: ActorId::new("alice").unwrap(),
                    physical_ms: 1_700_000_000_100,
                    logical: 0,
                },
            }),
        };
        let body = body.into_validated(&limits).expect("validated");
        let payload = crate::core::encode_event_body_canonical(body.as_ref()).expect("payload");
        let sha = crate::core::hash_event_body(&payload).0;
        let header = RecordHeader {
            origin_replica_id: body.origin_replica_id,
            origin_seq: body.origin_seq,
            event_time_ms: body.event_time_ms,
            txn_id: body.txn_id,
            request_proof: body
                .client_request_id
                .map(|client_request_id| RequestProof::ClientNoHash { client_request_id })
                .unwrap_or(RequestProof::None),
            sha256: sha,
            prev_sha256: None,
        };
        VerifiedRecord::new(header, payload, body).expect("verified record")
    }

    fn frame_len(record: &VerifiedRecord, max_record_bytes: usize) -> u64 {
        encode_frame(record, max_record_bytes).expect("frame").len() as u64
    }

    #[test]
    fn memory_wal_append_roundtrip() {
        let temp = TempDir::new().unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
        let meta = test_meta(store_id);
        let namespace = NamespaceId::core();
        let record = test_record();
        let config = SegmentConfig::from_limits(&crate::core::Limits::default());

        let mut wal = MemoryEventWal::new(temp.path().to_path_buf(), meta, config);
        let append = wal.append(&namespace, &record, 10).unwrap();
        let snapshot = wal.segment_snapshot(&namespace).unwrap();
        let read = read_record_at_path(&snapshot.path, append.offset, 16 * 1024 * 1024).unwrap();
        let (_, event_body) =
            crate::core::decode_event_body(read.payload_bytes(), &crate::core::Limits::default())
                .unwrap();
        let verified = read.verify_with_event_body(event_body).unwrap();
        assert_eq!(verified.header().origin_seq, record.header().origin_seq);
    }

    #[test]
    fn memory_wal_rotates_on_size() {
        let temp = TempDir::new().unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([8u8; 16]));
        let meta = test_meta(store_id);
        let namespace = NamespaceId::core();
        let record = test_record();

        let header_len =
            SegmentHeader::new(&meta, namespace.clone(), 10, SegmentId::new(Uuid::nil()))
                .encode()
                .unwrap()
                .len() as u64;
        let mut limits = crate::core::Limits::default();
        let frame_len = frame_len(&record, limits.policy().max_wal_record_bytes());
        limits.wal_segment_max_bytes = (header_len + frame_len + 1) as usize;
        let config = SegmentConfig::from_limits(&limits);

        let mut wal = MemoryEventWal::new(temp.path().to_path_buf(), meta, config);
        let first = wal.append(&namespace, &record, 10).unwrap();
        assert!(!first.rotated);
        let first_snapshot = wal.segment_snapshot(&namespace).unwrap();

        let second = wal.append(&namespace, &record, 10).unwrap();
        assert!(second.rotated);
        let second_snapshot = wal.segment_snapshot(&namespace).unwrap();

        assert_ne!(first.segment_id, second.segment_id);
        assert_ne!(first_snapshot.segment_id, second_snapshot.segment_id);
        assert_ne!(first_snapshot.path, second_snapshot.path);
    }
}
