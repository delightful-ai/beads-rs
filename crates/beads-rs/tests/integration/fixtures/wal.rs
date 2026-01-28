#![allow(dead_code)]

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use uuid::Uuid;

use beads_rs::core::{
    ActorId, Canonical, ClientRequestId, EventBody, EventBytes, EventKindV1, HlcMax, Limits,
    NamespaceId, ReplicaId, Seq1, StoreIdentity, StoreMeta, StoreMetaVersions, TraceId, TxnDeltaV1,
    TxnId, TxnV1, ValidatedEventBody, encode_event_body_canonical,
};
use beads_rs::daemon::wal::frame::encode_frame;
use beads_rs::daemon::wal::{
    EventWalError, EventWalResult, FRAME_HEADER_LEN, IndexDurabilityMode, RecordHeader,
    RequestProof, SEGMENT_HEADER_PREFIX_LEN, SegmentConfig, SegmentHeader, SegmentWriter,
    SqliteWalIndex, VerifiedRecord, WAL_FORMAT_VERSION, WalIndexError,
};

use super::identity;
const DEFAULT_MAX_RECORD_BYTES: usize = 1024 * 1024;
const DEFAULT_SEGMENT_MAX_BYTES: u64 = u64::MAX;
const DEFAULT_SEGMENT_MAX_AGE_MS: u64 = u64::MAX;

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

        let versions = StoreMetaVersions::new(1, WAL_FORMAT_VERSION, 1, 1, 1);
        let meta =
            identity::store_meta_with_versions(seed, 0, 1_700_000_000_000 + seed as u64, versions);

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
        records: &[VerifiedRecord],
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
    pub records: Vec<VerifiedRecord>,
}

impl SegmentFixture {
    pub fn frame_offset(&self, index: usize) -> u64 {
        self.frame_offsets[index]
    }

    pub fn frame_len(&self, index: usize) -> u32 {
        self.frame_lens[index]
    }

    pub fn frame_body_offset(&self, index: usize) -> u64 {
        self.frame_offsets[index].saturating_add(FRAME_HEADER_LEN as u64)
    }
}

fn event_body(
    meta: &StoreMeta,
    namespace: &NamespaceId,
    origin: ReplicaId,
    origin_seq: u64,
    event_time_ms: u64,
    txn_id: TxnId,
    client_request_id: Option<ClientRequestId>,
) -> ValidatedEventBody {
    let body = EventBody {
        envelope_v: 1,
        store: StoreIdentity::new(meta.store_id(), meta.store_epoch()),
        namespace: namespace.clone(),
        origin_replica_id: origin,
        origin_seq: Seq1::from_u64(origin_seq).expect("valid seq1"),
        event_time_ms,
        txn_id,
        client_request_id,
        trace_id: client_request_id.map(TraceId::from),
        kind: EventKindV1::TxnV1(TxnV1 {
            delta: TxnDeltaV1::new(),
            hlc_max: HlcMax {
                actor_id: ActorId::new("alice".to_string()).unwrap(),
                physical_ms: event_time_ms,
                logical: 1,
            },
        }),
    };
    body.into_validated(&Limits::default()).expect("validated")
}

fn event_body_bytes(body: &ValidatedEventBody) -> EventBytes<Canonical> {
    encode_event_body_canonical(body.as_ref()).expect("encode event body")
}

fn verified_record(
    header: RecordHeader,
    payload: EventBytes<Canonical>,
    body: ValidatedEventBody,
) -> VerifiedRecord {
    VerifiedRecord::new(header, payload, body).expect("verify record")
}

pub fn record_for_seq(
    meta: &StoreMeta,
    namespace: &NamespaceId,
    origin: ReplicaId,
    seq: u64,
    prev_sha: Option<[u8; 32]>,
) -> VerifiedRecord {
    let event_time_ms = 1_700_000_000_000 + seq;
    let txn_id = TxnId::new(Uuid::from_bytes([seq as u8; 16]));
    let body = event_body(meta, namespace, origin, seq, event_time_ms, txn_id, None);
    let payload = event_body_bytes(&body);
    let sha = beads_rs::hash_event_body(&payload).0;
    let header = RecordHeader {
        origin_replica_id: origin,
        origin_seq: Seq1::from_u64(seq).expect("seq1"),
        event_time_ms,
        txn_id,
        request_proof: RequestProof::None,
        sha256: sha,
        prev_sha256: prev_sha,
    };
    verified_record(header, payload, body)
}

pub fn sample_record(meta: &StoreMeta, namespace: &NamespaceId, seed: u8) -> VerifiedRecord {
    let origin = ReplicaId::new(Uuid::from_bytes([seed; 16]));
    let origin_seq = seed as u64 + 1;
    let event_time_ms = 1_700_000_000_000 + seed as u64;
    let txn_id = TxnId::new(Uuid::from_bytes([seed.wrapping_add(1); 16]));
    let client_request_id = Some(ClientRequestId::new(Uuid::from_bytes(
        [seed.wrapping_add(2); 16],
    )));
    let body = event_body(
        meta,
        namespace,
        origin,
        origin_seq,
        event_time_ms,
        txn_id,
        client_request_id,
    );
    let payload = event_body_bytes(&body);
    let sha = beads_rs::sha256_bytes(payload.as_ref()).0;
    let header = RecordHeader {
        origin_replica_id: origin,
        origin_seq: Seq1::from_u64(origin_seq).expect("seq1"),
        event_time_ms,
        txn_id,
        request_proof: client_request_id
            .map(|client_request_id| RequestProof::Client {
                client_request_id,
                request_sha256: [seed.wrapping_add(3); 32],
            })
            .unwrap_or(RequestProof::None),
        sha256: sha,
        prev_sha256: None,
    };
    verified_record(header, payload, body)
}

pub fn simple_record(meta: &StoreMeta, namespace: &NamespaceId, seed: u8) -> VerifiedRecord {
    let origin = ReplicaId::new(Uuid::from_bytes([seed; 16]));
    let origin_seq = seed as u64 + 1;
    let event_time_ms = 1_700_000_000_000 + seed as u64;
    let txn_id = TxnId::new(Uuid::from_bytes([seed; 16]));
    let body = event_body(
        meta,
        namespace,
        origin,
        origin_seq,
        event_time_ms,
        txn_id,
        None,
    );
    let payload = event_body_bytes(&body);
    let sha = beads_rs::sha256_bytes(payload.as_ref()).0;
    let header = RecordHeader {
        origin_replica_id: origin,
        origin_seq: Seq1::from_u64(origin_seq).expect("seq1"),
        event_time_ms,
        txn_id,
        request_proof: RequestProof::None,
        sha256: sha,
        prev_sha256: None,
    };
    verified_record(header, payload, body)
}

pub fn frame_bytes(record: &VerifiedRecord) -> EventWalResult<Vec<u8>> {
    encode_frame(record, DEFAULT_MAX_RECORD_BYTES)
}

pub fn valid_segment(
    temp: &TempWalDir,
    namespace: &NamespaceId,
    now_ms: u64,
) -> EventWalResult<SegmentFixture> {
    let record = sample_record(temp.meta(), namespace, 1);
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
        let record = sample_record(temp.meta(), &namespace, 1);
        let fixture = temp
            .write_segment(&namespace, 1_700_000_000_000, &[record])
            .expect("write segment");

        assert_eq!(fixture.header.namespace, namespace);
        assert_eq!(fixture.records.len(), 1);
        let meta = fs::metadata(&fixture.path).expect("segment metadata");
        assert!(meta.len() > fixture.header_len);
    }
}
