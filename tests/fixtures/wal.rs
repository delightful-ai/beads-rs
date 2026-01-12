#![allow(dead_code)]

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use tempfile::TempDir;
use uuid::Uuid;

use beads_rs::core::{
    ClientRequestId, NamespaceId, ReplicaId, StoreEpoch, StoreId, StoreIdentity, StoreMeta,
    StoreMetaVersions, TxnId,
};
use beads_rs::daemon::wal::frame::encode_frame;
use beads_rs::daemon::wal::{
    EventWalError, EventWalResult, IndexDurabilityMode, Record, RecordHeader, SegmentConfig,
    SegmentHeader, SegmentWriter, SqliteWalIndex, WalIndexError,
};

const WAL_FORMAT_VERSION: u32 = 2;
const SEGMENT_HEADER_PREFIX_LEN: usize = 13;
const DEFAULT_MAX_RECORD_BYTES: usize = 1024 * 1024;
const DEFAULT_SEGMENT_MAX_BYTES: u64 = u64::MAX;
const DEFAULT_SEGMENT_MAX_AGE_MS: u64 = u64::MAX;
const FRAME_HEADER_LEN: u64 = 12;

pub struct TempWalDir {
    _temp: TempDir,
    store_dir: PathBuf,
    meta: StoreMeta,
}

impl TempWalDir {
    pub fn new() -> Self {
        Self::with_seed(1)
    }

    pub fn with_seed(seed: u8) -> Self {
        let temp = TempDir::new().expect("temp wal dir");
        let store_dir = temp.path().join("store");
        fs::create_dir_all(&store_dir).expect("create store dir");

        let store_id = StoreId::new(Uuid::from_bytes([seed; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([seed.wrapping_add(1); 16]));
        let identity = StoreIdentity::new(store_id, StoreEpoch::new(0));
        let versions = StoreMetaVersions::new(1, WAL_FORMAT_VERSION, 1, 1, 1);
        let meta = StoreMeta::new(
            identity,
            replica_id,
            versions,
            1_700_000_000_000 + seed as u64,
        );

        Self {
            _temp: temp,
            store_dir,
            meta,
        }
    }

    pub fn store_dir(&self) -> &Path {
        &self.store_dir
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.store_dir.join("wal")
    }

    pub fn meta(&self) -> &StoreMeta {
        &self.meta
    }

    pub fn namespace_dir(&self, namespace: &NamespaceId) -> PathBuf {
        self.wal_dir().join(namespace.as_str())
    }

    pub fn open_index(&self) -> Result<SqliteWalIndex, WalIndexError> {
        SqliteWalIndex::open(&self.store_dir, &self.meta, IndexDurabilityMode::Cache)
    }

    pub fn open_segment_writer(
        &self,
        namespace: &NamespaceId,
        now_ms: u64,
    ) -> EventWalResult<SegmentWriter> {
        let config = SegmentConfig::new(
            DEFAULT_MAX_RECORD_BYTES,
            DEFAULT_SEGMENT_MAX_BYTES,
            DEFAULT_SEGMENT_MAX_AGE_MS,
        );
        SegmentWriter::open(&self.store_dir, &self.meta, namespace, now_ms, config)
    }

    pub fn write_segment(
        &self,
        namespace: &NamespaceId,
        now_ms: u64,
        records: &[Record],
    ) -> EventWalResult<SegmentFixture> {
        let mut writer = self.open_segment_writer(namespace, now_ms)?;
        let mut frame_offsets = Vec::with_capacity(records.len());
        let mut frame_lens = Vec::with_capacity(records.len());

        for record in records {
            let outcome = writer.append(record, now_ms)?;
            frame_offsets.push(outcome.offset);
            frame_lens.push(outcome.len);
        }

        let path = writer.current_path().to_path_buf();
        let (header, header_len) = read_segment_header(&path)?;

        Ok(SegmentFixture {
            path,
            header,
            header_len: header_len as u64,
            frame_offsets,
            frame_lens,
            records: records.to_vec(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct SegmentFixture {
    pub path: PathBuf,
    pub header: SegmentHeader,
    pub header_len: u64,
    pub frame_offsets: Vec<u64>,
    pub frame_lens: Vec<u32>,
    pub records: Vec<Record>,
}

impl SegmentFixture {
    pub fn frame_offset(&self, index: usize) -> u64 {
        self.frame_offsets[index]
    }

    pub fn frame_len(&self, index: usize) -> u32 {
        self.frame_lens[index]
    }

    pub fn frame_body_offset(&self, index: usize) -> u64 {
        self.frame_offsets[index].saturating_add(FRAME_HEADER_LEN)
    }
}

pub fn sample_record(seed: u8) -> Record {
    let payload = Bytes::from(vec![seed; 16]);
    let sha = beads_rs::sha256_bytes(payload.as_ref()).0;
    let header = RecordHeader {
        origin_replica_id: ReplicaId::new(Uuid::from_bytes([seed; 16])),
        origin_seq: seed as u64 + 1,
        event_time_ms: 1_700_000_000_000 + seed as u64,
        txn_id: TxnId::new(Uuid::from_bytes([seed.wrapping_add(1); 16])),
        client_request_id: Some(ClientRequestId::new(Uuid::from_bytes(
            [seed.wrapping_add(2); 16],
        ))),
        request_sha256: Some([seed.wrapping_add(3); 32]),
        sha256: sha,
        prev_sha256: Some([seed.wrapping_add(4); 32]),
    };
    Record { header, payload }
}

pub fn simple_record(seed: u8) -> Record {
    let payload = Bytes::from(vec![seed; 8]);
    let sha = beads_rs::sha256_bytes(payload.as_ref()).0;
    let header = RecordHeader {
        origin_replica_id: ReplicaId::new(Uuid::from_bytes([seed; 16])),
        origin_seq: seed as u64 + 1,
        event_time_ms: 1_700_000_000_000 + seed as u64,
        txn_id: TxnId::new(Uuid::from_bytes([seed; 16])),
        client_request_id: None,
        request_sha256: None,
        sha256: sha,
        prev_sha256: None,
    };
    Record { header, payload }
}

pub fn frame_bytes(record: &Record) -> EventWalResult<Vec<u8>> {
    encode_frame(record, DEFAULT_MAX_RECORD_BYTES)
}

pub fn valid_segment(
    temp: &TempWalDir,
    namespace: &NamespaceId,
    now_ms: u64,
) -> EventWalResult<SegmentFixture> {
    let record = sample_record(1);
    temp.write_segment(namespace, now_ms, &[record])
}

fn read_segment_header(path: &Path) -> EventWalResult<(SegmentHeader, usize)> {
    let mut file = fs::File::open(path).map_err(|source| EventWalError::Io {
        path: Some(path.to_path_buf()),
        source,
    })?;
    let mut prefix = [0u8; SEGMENT_HEADER_PREFIX_LEN];
    file.read_exact(&mut prefix)
        .map_err(|source| EventWalError::Io {
            path: Some(path.to_path_buf()),
            source,
        })?;
    let header_len = u32::from_le_bytes([prefix[9], prefix[10], prefix[11], prefix[12]]) as usize;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| EventWalError::Io {
            path: Some(path.to_path_buf()),
            source,
        })?;
    let mut buf = vec![0u8; header_len];
    file.read_exact(&mut buf)
        .map_err(|source| EventWalError::Io {
            path: Some(path.to_path_buf()),
            source,
        })?;
    let header = SegmentHeader::decode(&buf)?;
    Ok((header, header_len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_wal_dir_writes_segment() {
        let temp = TempWalDir::new();
        let namespace = NamespaceId::core();
        let record = sample_record(1);
        let fixture = temp
            .write_segment(&namespace, 1_700_000_000_000, &[record])
            .expect("write segment");

        assert_eq!(fixture.header.namespace, namespace);
        assert_eq!(fixture.records.len(), 1);
        let meta = fs::metadata(&fixture.path).expect("segment metadata");
        assert!(meta.len() > fixture.header_len);
    }
}
