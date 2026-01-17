//! Event WAL writer that reuses active segments per namespace.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::core::{Limits, NamespaceId, StoreMeta};

use super::memory_wal::MemoryEventWal;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentSnapshot {
    pub segment_id: crate::core::SegmentId,
    pub created_at_ms: u64,
    pub path: PathBuf,
}

enum EventWalBackend {
    Disk(DiskEventWal),
    Memory(MemoryEventWal),
}

pub struct EventWal {
    backend: EventWalBackend,
}

impl EventWal {
    pub fn new(store_dir: PathBuf, meta: StoreMeta, limits: &Limits) -> Self {
        Self::new_with_config(store_dir, meta, SegmentConfig::from_limits(limits))
    }

    pub fn new_with_config(store_dir: PathBuf, meta: StoreMeta, config: SegmentConfig) -> Self {
        Self {
            backend: EventWalBackend::Disk(DiskEventWal::new(store_dir, meta, config)),
        }
    }

    pub fn new_memory(store_dir: PathBuf, meta: StoreMeta, limits: &Limits) -> Self {
        Self::new_memory_with_config(store_dir, meta, SegmentConfig::from_limits(limits))
    }

    pub fn new_memory_with_config(
        store_dir: PathBuf,
        meta: StoreMeta,
        config: SegmentConfig,
    ) -> Self {
        Self {
            backend: EventWalBackend::Memory(MemoryEventWal::new(store_dir, meta, config)),
        }
    }

    pub fn append(
        &mut self,
        namespace: &NamespaceId,
        record: &VerifiedRecord,
        now_ms: u64,
    ) -> EventWalResult<AppendOutcome> {
        match &mut self.backend {
            EventWalBackend::Disk(wal) => wal.append(namespace, record, now_ms),
            EventWalBackend::Memory(wal) => wal.append(namespace, record, now_ms),
        }
    }

    pub fn segment_snapshot(&self, namespace: &NamespaceId) -> Option<SegmentSnapshot> {
        match &self.backend {
            EventWalBackend::Disk(wal) => wal.segment_snapshot(namespace),
            EventWalBackend::Memory(wal) => wal.segment_snapshot(namespace),
        }
    }

    pub fn flush(&mut self, namespace: &NamespaceId) -> EventWalResult<Option<SegmentSnapshot>> {
        match &mut self.backend {
            EventWalBackend::Disk(wal) => wal.flush(namespace),
            EventWalBackend::Memory(wal) => wal.flush(namespace),
        }
    }
}

struct DiskEventWal {
    store_dir: PathBuf,
    meta: StoreMeta,
    config: SegmentConfig,
    writers: BTreeMap<NamespaceId, SegmentWriter>,
}

impl DiskEventWal {
    fn new(store_dir: PathBuf, meta: StoreMeta, config: SegmentConfig) -> Self {
        Self {
            store_dir,
            meta,
            config,
            writers: BTreeMap::new(),
        }
    }

    fn append(
        &mut self,
        namespace: &NamespaceId,
        record: &VerifiedRecord,
        now_ms: u64,
    ) -> EventWalResult<AppendOutcome> {
        let writer = self.writer_mut(namespace, now_ms)?;
        writer.append(record, now_ms)
    }

    fn segment_snapshot(&self, namespace: &NamespaceId) -> Option<SegmentSnapshot> {
        self.writers.get(namespace).map(|writer| SegmentSnapshot {
            segment_id: writer.current_segment_id(),
            created_at_ms: writer.current_created_at_ms(),
            path: writer.current_path().to_path_buf(),
        })
    }

    fn flush(&mut self, namespace: &NamespaceId) -> EventWalResult<Option<SegmentSnapshot>> {
        let Some(writer) = self.writers.get_mut(namespace) else {
            return Ok(None);
        };
        writer.flush()?;
        Ok(Some(SegmentSnapshot {
            segment_id: writer.current_segment_id(),
            created_at_ms: writer.current_created_at_ms(),
            path: writer.current_path().to_path_buf(),
        }))
    }

    fn writer_mut(
        &mut self,
        namespace: &NamespaceId,
        now_ms: u64,
    ) -> EventWalResult<&mut SegmentWriter> {
        if !self.writers.contains_key(namespace) {
            let writer =
                SegmentWriter::open(&self.store_dir, &self.meta, namespace, now_ms, self.config)?;
            self.writers.insert(namespace.clone(), writer);
        }
        Ok(self
            .writers
            .get_mut(namespace)
            .expect("writer missing after insert"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::core::{
        ReplicaId, SegmentId, Seq1, StoreEpoch, StoreId, StoreIdentity, StoreMetaVersions,
    };
    use crate::daemon::wal::RecordHeader;
    use crate::daemon::wal::frame::encode_frame;
    use crate::daemon::wal::segment::SegmentHeader;

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
        let payload = Bytes::from_static(b"event");
        let sha = crate::core::sha256_bytes(payload.as_ref()).0;
        let header = RecordHeader {
            origin_replica_id: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            origin_seq: Seq1::from_u64(1).unwrap(),
            event_time_ms: 1_700_000_000_100,
            txn_id: crate::core::TxnId::new(Uuid::from_bytes([2u8; 16])),
            client_request_id: None,
            request_sha256: None,
            sha256: sha,
            prev_sha256: None,
        };
        let body = crate::core::EventBody {
            envelope_v: 1,
            store: crate::core::StoreIdentity::new(
                StoreId::new(Uuid::from_bytes([9u8; 16])),
                StoreEpoch::new(1),
            ),
            namespace: NamespaceId::core(),
            origin_replica_id: header.origin_replica_id,
            origin_seq: header.origin_seq,
            event_time_ms: header.event_time_ms,
            txn_id: header.txn_id,
            client_request_id: header.client_request_id,
            kind: crate::core::EventKindV1::TxnV1(crate::core::TxnV1 {
                delta: crate::core::TxnDeltaV1::new(),
                hlc_max: crate::core::HlcMax {
                    actor_id: crate::core::ActorId::new("alice").unwrap(),
                    physical_ms: header.event_time_ms,
                    logical: 0,
                },
            }),
        };
        VerifiedRecord::new(header, payload, &body).expect("verified record")
    }

    fn frame_len(record: &VerifiedRecord, max_record_bytes: usize) -> u64 {
        encode_frame(record, max_record_bytes).expect("frame").len() as u64
    }

    #[test]
    fn event_wal_reuses_active_segment() {
        let temp = TempDir::new().unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
        let meta = test_meta(store_id);
        let namespace = NamespaceId::core();
        let record = test_record();

        let header_len =
            SegmentHeader::new(&meta, namespace.clone(), 10, SegmentId::new(Uuid::nil()))
                .encode()
                .unwrap()
                .len() as u64;
        let mut limits = Limits::default();
        limits.wal_segment_max_bytes =
            (header_len + frame_len(&record, limits.max_wal_record_bytes) * 10) as usize;

        let mut wal = EventWal::new(temp.path().to_path_buf(), meta, &limits);

        let first = wal.append(&namespace, &record, 10).unwrap();
        let first_snapshot = wal.segment_snapshot(&namespace).unwrap();
        assert!(!first.rotated);

        let second = wal.append(&namespace, &record, 10).unwrap();
        let second_snapshot = wal.segment_snapshot(&namespace).unwrap();
        assert!(!second.rotated);
        assert_eq!(first.segment_id, second.segment_id);
        assert_eq!(first_snapshot.segment_id, second_snapshot.segment_id);
        assert_eq!(first_snapshot.path, second_snapshot.path);
    }

    #[test]
    fn event_wal_rotates_on_size() {
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
        let mut limits = Limits::default();
        let frame_len = frame_len(&record, limits.max_wal_record_bytes);
        limits.wal_segment_max_bytes = (header_len + frame_len + 1) as usize;

        let mut wal = EventWal::new(temp.path().to_path_buf(), meta, &limits);

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
