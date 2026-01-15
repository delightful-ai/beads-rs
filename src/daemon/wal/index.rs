//! WAL SQLite index + traits.

use std::path::{Path, PathBuf};
use std::time::Duration;

use minicbor::{Decoder, Encoder};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::core::{
    ActorId, ClientRequestId, EventId, NamespaceId, ReplicaId, SegmentId, Seq1, StoreId, StoreMeta,
    TxnId,
};

const INDEX_SCHEMA_VERSION: u32 = 1;
const BUSY_TIMEOUT_MS: u64 = 5_000;
const CACHE_SIZE_KB: i64 = -16_000;

#[derive(Debug, Error)]
pub enum WalIndexError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("path is a symlink: {path:?}")]
    Symlink { path: PathBuf },
    #[error("index schema version mismatch: expected {expected}, got {got}")]
    SchemaVersionMismatch { expected: u32, got: u32 },
    #[error("missing meta key: {key}")]
    MetaMissing { key: &'static str },
    #[error("meta mismatch for {key}: expected {expected}, got {got}")]
    MetaMismatch {
        key: &'static str,
        expected: String,
        got: String,
        store_id: StoreId,
    },
    #[error("event id encode failed: {0}")]
    CborEncode(#[from] minicbor::encode::Error<std::convert::Infallible>),
    #[error("event id decode failed: {0}")]
    CborDecode(#[from] minicbor::decode::Error),
    #[error("event id decode invalid: {0}")]
    EventIdDecode(String),
    #[error("origin_seq overflow for {namespace} {origin}")]
    OriginSeqOverflow {
        namespace: String,
        origin: ReplicaId,
    },
    #[error("equivocation for {namespace} {origin} seq {seq}")]
    Equivocation {
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: u64,
        existing_sha256: [u8; 32],
        new_sha256: [u8; 32],
    },
    #[error("client_request_id reuse mismatch for {namespace} {origin} {client_request_id}")]
    ClientRequestIdReuseMismatch {
        namespace: NamespaceId,
        origin: ReplicaId,
        client_request_id: ClientRequestId,
        expected_request_sha256: [u8; 32],
        got_request_sha256: [u8; 32],
    },
    #[error("hlc row decode failed: {0}")]
    HlcRowDecode(String),
    #[error("segment row decode failed: {0}")]
    SegmentRowDecode(String),
    #[error("watermark row decode failed: {0}")]
    WatermarkRowDecode(String),
}

pub trait WalIndex: Send + Sync {
    fn writer(&self) -> Box<dyn WalIndexWriter>;
    fn reader(&self) -> Box<dyn WalIndexReader>;
}

pub trait WalIndexWriter {
    fn begin_txn(&self) -> Result<Box<dyn WalIndexTxn>, WalIndexError>;
}

pub trait WalIndexTxn {
    fn next_origin_seq(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
    ) -> Result<u64, WalIndexError>;
    fn set_next_origin_seq(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        next_seq: u64,
    ) -> Result<(), WalIndexError>;
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
        request_sha256: Option<[u8; 32]>,
    ) -> Result<(), WalIndexError>;
    fn update_watermark(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        applied: u64,
        durable: u64,
        applied_head_sha: Option<[u8; 32]>,
        durable_head_sha: Option<[u8; 32]>,
    ) -> Result<(), WalIndexError>;
    fn update_hlc(&mut self, hlc: &HlcRow) -> Result<(), WalIndexError>;
    fn upsert_segment(&mut self, segment: &SegmentRow) -> Result<(), WalIndexError>;
    #[allow(clippy::too_many_arguments)]
    fn upsert_client_request(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        client_request_id: ClientRequestId,
        request_sha256: [u8; 32],
        txn_id: TxnId,
        event_ids: &[EventId],
        created_at_ms: u64,
    ) -> Result<(), WalIndexError>;
    fn commit(self: Box<Self>) -> Result<(), WalIndexError>;
    fn rollback(self: Box<Self>) -> Result<(), WalIndexError>;
}

pub trait WalIndexReader {
    fn lookup_event_sha(
        &self,
        ns: &NamespaceId,
        eid: &EventId,
    ) -> Result<Option<[u8; 32]>, WalIndexError>;
    fn list_segments(&self, ns: &NamespaceId) -> Result<Vec<SegmentRow>, WalIndexError>;
    fn load_watermarks(&self) -> Result<Vec<WatermarkRow>, WalIndexError>;
    fn load_hlc(&self) -> Result<Vec<HlcRow>, WalIndexError>;
    fn iter_from(
        &self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        from_seq_excl: u64,
        max_bytes: usize,
    ) -> Result<Vec<IndexedRangeItem>, WalIndexError>;
    fn lookup_client_request(
        &self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        client_request_id: ClientRequestId,
    ) -> Result<Option<ClientRequestRow>, WalIndexError>;
    fn max_origin_seq(&self, ns: &NamespaceId, origin: &ReplicaId) -> Result<u64, WalIndexError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientRequestRow {
    pub request_sha256: [u8; 32],
    pub txn_id: TxnId,
    pub event_ids: Vec<EventId>,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedRangeItem {
    pub event_id: EventId,
    pub sha: [u8; 32],
    pub prev_sha: Option<[u8; 32]>,
    pub segment_id: SegmentId,
    pub offset: u64,
    pub len: u32,
    pub event_time_ms: u64,
    pub txn_id: TxnId,
    pub client_request_id: Option<ClientRequestId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentRow {
    pub namespace: NamespaceId,
    pub segment_id: SegmentId,
    pub segment_path: PathBuf,
    pub created_at_ms: u64,
    pub last_indexed_offset: u64,
    pub sealed: bool,
    pub final_len: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatermarkRow {
    pub namespace: NamespaceId,
    pub origin: ReplicaId,
    pub applied_seq: u64,
    pub durable_seq: u64,
    pub applied_head_sha: Option<[u8; 32]>,
    pub durable_head_sha: Option<[u8; 32]>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HlcRow {
    pub actor_id: ActorId,
    pub last_physical_ms: u64,
    pub last_logical: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexDurabilityMode {
    Cache,
    Durable,
}

impl IndexDurabilityMode {
    fn synchronous_value(self) -> &'static str {
        match self {
            IndexDurabilityMode::Cache => "NORMAL",
            IndexDurabilityMode::Durable => "FULL",
        }
    }
}

pub struct SqliteWalIndex {
    db_path: PathBuf,
    mode: IndexDurabilityMode,
}

impl SqliteWalIndex {
    pub fn open(
        store_dir: &Path,
        meta: &StoreMeta,
        mode: IndexDurabilityMode,
    ) -> Result<Self, WalIndexError> {
        let index_dir = store_dir.join("index");
        reject_symlink(&index_dir)?;
        std::fs::create_dir_all(&index_dir).map_err(|source| WalIndexError::Io {
            path: index_dir.clone(),
            source,
        })?;
        reject_symlink(&index_dir)?;
        let db_path = index_dir.join("wal.sqlite");
        reject_symlink(&db_path)?;

        let conn = open_connection(&db_path, mode, true)?;
        let is_new = !table_exists(&conn, "meta")?;
        if is_new {
            initialize_schema(&conn)?;
            write_meta(&conn, meta)?;
        } else {
            validate_meta(&conn, meta)?;
        }

        ensure_permissions(&db_path)?;
        drop(conn);

        Ok(Self { db_path, mode })
    }

    pub fn durability_mode(&self) -> IndexDurabilityMode {
        self.mode
    }
}

impl WalIndex for SqliteWalIndex {
    fn writer(&self) -> Box<dyn WalIndexWriter> {
        Box::new(SqliteWalIndexWriter {
            db_path: self.db_path.clone(),
            mode: self.mode,
        })
    }

    fn reader(&self) -> Box<dyn WalIndexReader> {
        Box::new(SqliteWalIndexReader {
            db_path: self.db_path.clone(),
            mode: self.mode,
        })
    }
}

struct SqliteWalIndexWriter {
    db_path: PathBuf,
    mode: IndexDurabilityMode,
}

impl WalIndexWriter for SqliteWalIndexWriter {
    fn begin_txn(&self) -> Result<Box<dyn WalIndexTxn>, WalIndexError> {
        let conn = open_connection(&self.db_path, self.mode, false)?;
        conn.execute_batch("BEGIN IMMEDIATE")?;
        Ok(Box::new(SqliteWalIndexTxn {
            conn,
            committed: false,
        }))
    }
}

struct SqliteWalIndexTxn {
    conn: Connection,
    committed: bool,
}

impl WalIndexTxn for SqliteWalIndexTxn {
    fn next_origin_seq(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
    ) -> Result<u64, WalIndexError> {
        let namespace = ns.as_str();
        let origin_blob = uuid_blob(origin.as_uuid());
        let next_seq: Option<u64> = self
            .conn
            .query_row(
                "SELECT next_seq FROM origin_seq WHERE namespace = ?1 AND origin_replica_id = ?2",
                params![namespace, origin_blob],
                |row| row.get::<_, u64>(0),
            )
            .optional()?;

        let next = next_seq.unwrap_or(1);
        let updated = next
            .checked_add(1)
            .ok_or_else(|| WalIndexError::OriginSeqOverflow {
                namespace: namespace.to_string(),
                origin: *origin,
            })?;
        self.conn.execute(
            "INSERT INTO origin_seq (namespace, origin_replica_id, next_seq) VALUES (?1, ?2, ?3) \
             ON CONFLICT(namespace, origin_replica_id) DO UPDATE SET next_seq = excluded.next_seq",
            params![namespace, origin_blob, updated],
        )?;
        Ok(next)
    }

    fn set_next_origin_seq(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        next_seq: u64,
    ) -> Result<(), WalIndexError> {
        let namespace = ns.as_str();
        let origin_blob = uuid_blob(origin.as_uuid());
        self.conn.execute(
            "INSERT INTO origin_seq (namespace, origin_replica_id, next_seq) VALUES (?1, ?2, ?3) \
             ON CONFLICT(namespace, origin_replica_id) DO UPDATE SET next_seq = excluded.next_seq",
            params![namespace, origin_blob, next_seq as i64],
        )?;
        Ok(())
    }

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
        request_sha256: Option<[u8; 32]>,
    ) -> Result<(), WalIndexError> {
        let _ = request_sha256;
        let namespace = ns.as_str();
        let origin_blob = uuid_blob(eid.origin_replica_id.as_uuid());
        let eid_seq = eid.origin_seq.get() as i64;
        let sha_blob = sha.to_vec();
        let prev_sha_blob = prev_sha.map(|value| value.to_vec());
        let segment_blob = uuid_blob(segment_id.as_uuid());
        let txn_blob = uuid_blob(txn_id.as_uuid());
        let client_blob = client_request_id.map(|id| uuid_blob(id.as_uuid()));

        let inserted = self.conn.execute(
            "INSERT OR IGNORE INTO events \
             (namespace, origin_replica_id, origin_seq, sha, prev_sha, segment_id, segment_offset, len, event_time_ms, txn_id, client_request_id) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                namespace,
                origin_blob,
                eid_seq,
                sha_blob,
                prev_sha_blob,
                segment_blob,
                offset as i64,
                len as i64,
                event_time_ms as i64,
                txn_blob,
                client_blob,
            ],
        )?;
        if inserted == 0 {
            let existing: Option<Vec<u8>> = self
                .conn
                .query_row(
                    "SELECT sha FROM events WHERE namespace = ?1 AND origin_replica_id = ?2 AND origin_seq = ?3",
                    params![namespace, origin_blob, eid_seq],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()?;
            if let Some(existing) = existing {
                let existing_sha = blob_32(existing)?;
                if existing_sha == sha {
                    return Ok(());
                }
                return Err(WalIndexError::Equivocation {
                    namespace: ns.clone(),
                    origin: eid.origin_replica_id,
                    seq: eid.origin_seq.get(),
                    existing_sha256: existing_sha,
                    new_sha256: sha,
                });
            }
            return Err(WalIndexError::EventIdDecode(
                "missing event row after conflict".to_string(),
            ));
        }
        Ok(())
    }

    fn update_watermark(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        applied: u64,
        durable: u64,
        applied_head_sha: Option<[u8; 32]>,
        durable_head_sha: Option<[u8; 32]>,
    ) -> Result<(), WalIndexError> {
        let namespace = ns.as_str();
        let origin_blob = uuid_blob(origin.as_uuid());
        let applied_blob = applied_head_sha.map(|value| value.to_vec());
        let durable_blob = durable_head_sha.map(|value| value.to_vec());

        self.conn.execute(
            "INSERT INTO watermarks (namespace, origin_replica_id, applied_seq, durable_seq, applied_head_sha, durable_head_sha) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(namespace, origin_replica_id) DO UPDATE SET \
               applied_seq = excluded.applied_seq, \
               durable_seq = excluded.durable_seq, \
               applied_head_sha = excluded.applied_head_sha, \
               durable_head_sha = excluded.durable_head_sha",
            params![
                namespace,
                origin_blob,
                applied as i64,
                durable as i64,
                applied_blob,
                durable_blob,
            ],
        )?;
        Ok(())
    }

    fn update_hlc(&mut self, hlc: &HlcRow) -> Result<(), WalIndexError> {
        let last_physical_ms = i64::try_from(hlc.last_physical_ms).map_err(|_| {
            WalIndexError::HlcRowDecode("last_physical_ms out of range".to_string())
        })?;
        let last_logical = i64::from(hlc.last_logical);
        self.conn.execute(
            "INSERT INTO hlc (actor_id, last_physical_ms, last_logical) VALUES (?1, ?2, ?3) \
             ON CONFLICT(actor_id) DO UPDATE SET \
               last_physical_ms = excluded.last_physical_ms, \
               last_logical = excluded.last_logical",
            params![hlc.actor_id.as_str(), last_physical_ms, last_logical],
        )?;
        Ok(())
    }

    fn upsert_segment(&mut self, segment: &SegmentRow) -> Result<(), WalIndexError> {
        let namespace = segment.namespace.as_str();
        let segment_blob = uuid_blob(segment.segment_id.as_uuid());
        let path_str = segment.segment_path.to_string_lossy();
        let sealed = if segment.sealed { 1 } else { 0 };
        let final_len = segment.final_len.map(|value| value as i64);
        self.conn.execute(
            "INSERT INTO segments (namespace, segment_id, segment_path, created_at_ms, last_indexed_offset, sealed, final_len) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
             ON CONFLICT(namespace, segment_id) DO UPDATE SET \
               segment_path = excluded.segment_path, \
               created_at_ms = excluded.created_at_ms, \
               last_indexed_offset = excluded.last_indexed_offset, \
               sealed = excluded.sealed, \
               final_len = excluded.final_len",
            params![
                namespace,
                segment_blob,
                path_str.as_ref(),
                segment.created_at_ms as i64,
                segment.last_indexed_offset as i64,
                sealed,
                final_len,
            ],
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
        event_ids: &[EventId],
        created_at_ms: u64,
    ) -> Result<(), WalIndexError> {
        let namespace = ns.as_str();
        let origin_blob = uuid_blob(origin.as_uuid());
        let client_blob = uuid_blob(client_request_id.as_uuid());
        let request_blob = request_sha256.to_vec();
        let txn_blob = uuid_blob(txn_id.as_uuid());
        let event_ids_blob = encode_event_ids(event_ids)?;

        let inserted = self.conn.execute(
            "INSERT INTO client_requests (namespace, origin_replica_id, client_request_id, created_at_ms, request_sha256, txn_id, event_ids) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
             ON CONFLICT(namespace, origin_replica_id, client_request_id) DO NOTHING",
            params![
                namespace,
                &origin_blob,
                &client_blob,
                created_at_ms as i64,
                &request_blob,
                &txn_blob,
                &event_ids_blob,
            ],
        )?;
        if inserted == 0 {
            let existing = self
                .conn
                .query_row(
                    "SELECT request_sha256 FROM client_requests \
                     WHERE namespace = ?1 AND origin_replica_id = ?2 AND client_request_id = ?3",
                    params![namespace, &origin_blob, &client_blob],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()?;
            let Some(existing_sha) = existing else {
                return Err(WalIndexError::EventIdDecode(
                    "client_request_id conflict without row".to_string(),
                ));
            };
            let existing_sha = blob_32(existing_sha)?;
            if existing_sha != request_sha256 {
                return Err(WalIndexError::ClientRequestIdReuseMismatch {
                    namespace: ns.clone(),
                    origin: *origin,
                    client_request_id,
                    expected_request_sha256: existing_sha,
                    got_request_sha256: request_sha256,
                });
            }
        }
        Ok(())
    }

    fn commit(mut self: Box<Self>) -> Result<(), WalIndexError> {
        self.conn.execute_batch("COMMIT")?;
        self.committed = true;
        Ok(())
    }

    fn rollback(mut self: Box<Self>) -> Result<(), WalIndexError> {
        self.conn.execute_batch("ROLLBACK")?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for SqliteWalIndexTxn {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}

struct SqliteWalIndexReader {
    db_path: PathBuf,
    mode: IndexDurabilityMode,
}

impl SqliteWalIndexReader {
    fn with_conn<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, WalIndexError>,
    ) -> Result<T, WalIndexError> {
        let conn = open_connection(&self.db_path, self.mode, false)?;
        f(&conn)
    }
}

impl WalIndexReader for SqliteWalIndexReader {
    fn lookup_event_sha(
        &self,
        ns: &NamespaceId,
        eid: &EventId,
    ) -> Result<Option<[u8; 32]>, WalIndexError> {
        self.with_conn(|conn| {
            let namespace = ns.as_str();
            let origin_blob = uuid_blob(eid.origin_replica_id.as_uuid());
            let origin_seq = eid.origin_seq.get() as i64;
            let row: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT sha FROM events WHERE namespace = ?1 AND origin_replica_id = ?2 AND origin_seq = ?3",
                    params![namespace, origin_blob, origin_seq],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()?;
            row.map(blob_32).transpose()
        })
    }

    fn list_segments(&self, ns: &NamespaceId) -> Result<Vec<SegmentRow>, WalIndexError> {
        self.with_conn(|conn| {
            let namespace = ns.as_str();
            let mut stmt = conn.prepare(
                "SELECT segment_id, segment_path, created_at_ms, last_indexed_offset, sealed, final_len \
                 FROM segments WHERE namespace = ?1 ORDER BY created_at_ms ASC, segment_id ASC",
            )?;
            let mut rows = stmt.query(params![namespace])?;
            let mut segments = Vec::new();
            while let Some(row) = rows.next()? {
                let segment_id: Vec<u8> = row.get(0)?;
                let segment_path: String = row.get(1)?;
                let created_at_ms: i64 = row.get(2)?;
                let last_indexed_offset: i64 = row.get(3)?;
                let sealed: i64 = row.get(4)?;
                let final_len: Option<i64> = row.get(5)?;

                let created_at_ms = u64::try_from(created_at_ms).map_err(|_| {
                    WalIndexError::SegmentRowDecode("created_at_ms out of range".to_string())
                })?;
                let last_indexed_offset = u64::try_from(last_indexed_offset).map_err(|_| {
                    WalIndexError::SegmentRowDecode("last_indexed_offset out of range".to_string())
                })?;
                let final_len = match final_len {
                    Some(value) => Some(u64::try_from(value).map_err(|_| {
                        WalIndexError::SegmentRowDecode("final_len out of range".to_string())
                    })?),
                    None => None,
                };

                let segment_uuid = blob_uuid(segment_id)
                    .map_err(|err| WalIndexError::SegmentRowDecode(err.to_string()))?;

                segments.push(SegmentRow {
                    namespace: ns.clone(),
                    segment_id: SegmentId::new(segment_uuid),
                    segment_path: PathBuf::from(segment_path),
                    created_at_ms,
                    last_indexed_offset,
                    sealed: sealed != 0,
                    final_len,
                });
            }
            Ok(segments)
        })
    }

    fn load_watermarks(&self) -> Result<Vec<WatermarkRow>, WalIndexError> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT namespace, origin_replica_id, applied_seq, durable_seq, applied_head_sha, durable_head_sha \
                 FROM watermarks",
            )?;
            let mut rows = stmt.query([])?;
            let mut watermarks = Vec::new();
            while let Some(row) = rows.next()? {
                let namespace: String = row.get(0)?;
                let origin_blob: Vec<u8> = row.get(1)?;
                let applied_seq: i64 = row.get(2)?;
                let durable_seq: i64 = row.get(3)?;
                let applied_head_sha: Option<Vec<u8>> = row.get(4)?;
                let durable_head_sha: Option<Vec<u8>> = row.get(5)?;

                let namespace = NamespaceId::parse(&namespace)
                    .map_err(|err| WalIndexError::WatermarkRowDecode(err.to_string()))?;
                let origin_uuid = blob_uuid(origin_blob)
                    .map_err(|err| WalIndexError::WatermarkRowDecode(err.to_string()))?;
                let applied_seq = u64::try_from(applied_seq).map_err(|_| {
                    WalIndexError::WatermarkRowDecode("applied_seq out of range".to_string())
                })?;
                let durable_seq = u64::try_from(durable_seq).map_err(|_| {
                    WalIndexError::WatermarkRowDecode("durable_seq out of range".to_string())
                })?;
                let applied_head_sha = applied_head_sha
                    .map(blob_32)
                    .transpose()
                    .map_err(|err| WalIndexError::WatermarkRowDecode(err.to_string()))?;
                let durable_head_sha = durable_head_sha
                    .map(blob_32)
                    .transpose()
                    .map_err(|err| WalIndexError::WatermarkRowDecode(err.to_string()))?;

                watermarks.push(WatermarkRow {
                    namespace,
                    origin: ReplicaId::new(origin_uuid),
                    applied_seq,
                    durable_seq,
                    applied_head_sha,
                    durable_head_sha,
                });
            }
            Ok(watermarks)
        })
    }

    fn load_hlc(&self) -> Result<Vec<HlcRow>, WalIndexError> {
        self.with_conn(|conn| {
            let mut stmt =
                conn.prepare("SELECT actor_id, last_physical_ms, last_logical FROM hlc")?;
            let mut rows = stmt.query([])?;
            let mut hlc_rows = Vec::new();
            while let Some(row) = rows.next()? {
                let actor_id: String = row.get(0)?;
                let last_physical_ms: i64 = row.get(1)?;
                let last_logical: i64 = row.get(2)?;

                let actor_id = ActorId::new(actor_id)
                    .map_err(|err| WalIndexError::HlcRowDecode(err.to_string()))?;
                let last_physical_ms = u64::try_from(last_physical_ms).map_err(|_| {
                    WalIndexError::HlcRowDecode("last_physical_ms out of range".to_string())
                })?;
                let last_logical = u32::try_from(last_logical).map_err(|_| {
                    WalIndexError::HlcRowDecode("last_logical out of range".to_string())
                })?;

                hlc_rows.push(HlcRow {
                    actor_id,
                    last_physical_ms,
                    last_logical,
                });
            }
            Ok(hlc_rows)
        })
    }

    fn iter_from(
        &self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        from_seq_excl: u64,
        max_bytes: usize,
    ) -> Result<Vec<IndexedRangeItem>, WalIndexError> {
        self.with_conn(|conn| {
            let namespace = ns.as_str();
            let origin_blob = uuid_blob(origin.as_uuid());
            let mut stmt = conn.prepare(
                "SELECT origin_seq, sha, prev_sha, segment_id, segment_offset, len, event_time_ms, txn_id, client_request_id \
                 FROM events WHERE namespace = ?1 AND origin_replica_id = ?2 AND origin_seq > ?3 \
                 ORDER BY origin_seq ASC",
            )?;
            let mut rows = stmt.query(params![namespace, origin_blob, from_seq_excl as i64])?;
            let mut items = Vec::new();
            let mut bytes_accum = 0usize;
            while let Some(row) = rows.next()? {
                let origin_seq: i64 = row.get(0)?;
                let sha: Vec<u8> = row.get(1)?;
                let prev_sha: Option<Vec<u8>> = row.get(2)?;
                let segment_id: Vec<u8> = row.get(3)?;
                let segment_offset: i64 = row.get(4)?;
                let len: i64 = row.get(5)?;
                let event_time_ms: i64 = row.get(6)?;
                let txn_id: Vec<u8> = row.get(7)?;
                let client_request_id: Option<Vec<u8>> = row.get(8)?;

                let len_u32 = u32::try_from(len).map_err(|_| {
                    WalIndexError::EventIdDecode("event len out of range".to_string())
                })?;
                if bytes_accum + len_u32 as usize > max_bytes {
                    break;
                }
                bytes_accum += len_u32 as usize;

                let origin_seq = u64::try_from(origin_seq).map_err(|_| {
                    WalIndexError::EventIdDecode("origin_seq out of range".to_string())
                })?;
                let origin_seq = Seq1::from_u64(origin_seq).ok_or_else(|| {
                    WalIndexError::EventIdDecode("origin_seq not Seq1".to_string())
                })?;
                let event_id = EventId::new(*origin, ns.clone(), origin_seq);

                let offset = u64::try_from(segment_offset).map_err(|_| {
                    WalIndexError::EventIdDecode("segment_offset out of range".to_string())
                })?;
                let event_time_ms = u64::try_from(event_time_ms).map_err(|_| {
                    WalIndexError::EventIdDecode("event_time_ms out of range".to_string())
                })?;

                items.push(IndexedRangeItem {
                    event_id,
                    sha: blob_32(sha)?,
                    prev_sha: prev_sha.map(blob_32).transpose()?,
                    segment_id: SegmentId::new(blob_uuid(segment_id)?),
                    offset,
                    len: len_u32,
                    event_time_ms,
                    txn_id: TxnId::new(blob_uuid(txn_id)?),
                    client_request_id: client_request_id
                        .map(blob_uuid)
                        .transpose()?
                        .map(ClientRequestId::new),
                });
            }
            Ok(items)
        })
    }

    fn lookup_client_request(
        &self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        client_request_id: ClientRequestId,
    ) -> Result<Option<ClientRequestRow>, WalIndexError> {
        self.with_conn(|conn| {
            let namespace = ns.as_str();
            let origin_blob = uuid_blob(origin.as_uuid());
            let client_blob = uuid_blob(client_request_id.as_uuid());

            let row = conn
                .query_row(
                    "SELECT request_sha256, txn_id, event_ids, created_at_ms FROM client_requests \
                     WHERE namespace = ?1 AND origin_replica_id = ?2 AND client_request_id = ?3",
                    params![namespace, origin_blob, client_blob],
                    |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )
                .optional()?;

            match row {
                Some((request_sha, txn_id, event_ids, created_at_ms)) => {
                    Ok(Some(ClientRequestRow {
                        request_sha256: blob_32(request_sha)?,
                        txn_id: TxnId::new(blob_uuid(txn_id)?),
                        event_ids: decode_event_ids(&event_ids)?,
                        created_at_ms: u64::try_from(created_at_ms).map_err(|_| {
                            WalIndexError::EventIdDecode("created_at_ms out of range".to_string())
                        })?,
                    }))
                }
                None => Ok(None),
            }
        })
    }

    fn max_origin_seq(&self, ns: &NamespaceId, origin: &ReplicaId) -> Result<u64, WalIndexError> {
        self.with_conn(|conn| {
            let namespace = ns.as_str();
            let origin_blob = uuid_blob(origin.as_uuid());
            let max_seq: Option<i64> = conn.query_row(
                "SELECT MAX(origin_seq) FROM events WHERE namespace = ?1 AND origin_replica_id = ?2",
                params![namespace, origin_blob],
                |row| row.get::<_, Option<i64>>(0),
            )?;
            match max_seq {
                Some(seq) => u64::try_from(seq).map_err(|_| {
                    WalIndexError::EventIdDecode("origin_seq out of range".to_string())
                }),
                None => Ok(0),
            }
        })
    }
}

fn initialize_schema(conn: &Connection) -> Result<(), WalIndexError> {
    conn.execute_batch(
        "PRAGMA auto_vacuum = INCREMENTAL;
         CREATE TABLE IF NOT EXISTS events (
           namespace TEXT NOT NULL,
           origin_replica_id BLOB NOT NULL,
           origin_seq INTEGER NOT NULL,
           sha BLOB NOT NULL,
           prev_sha BLOB,
           segment_id BLOB NOT NULL,
           segment_offset INTEGER NOT NULL,
           len INTEGER NOT NULL,
           event_time_ms INTEGER NOT NULL,
           txn_id BLOB NOT NULL,
           client_request_id BLOB,
           PRIMARY KEY (namespace, origin_replica_id, origin_seq)
         );
         CREATE INDEX IF NOT EXISTS events_by_client_request
           ON events (namespace, origin_replica_id, client_request_id);
         CREATE INDEX IF NOT EXISTS events_by_origin_seq
           ON events (namespace, origin_replica_id, origin_seq);
         CREATE TABLE IF NOT EXISTS watermarks (
           namespace TEXT NOT NULL,
           origin_replica_id BLOB NOT NULL,
           applied_seq INTEGER NOT NULL,
           durable_seq INTEGER NOT NULL,
           applied_head_sha BLOB,
           durable_head_sha BLOB,
           PRIMARY KEY (namespace, origin_replica_id)
         );
         CREATE TABLE IF NOT EXISTS client_requests (
           namespace TEXT NOT NULL,
           origin_replica_id BLOB NOT NULL,
           client_request_id BLOB NOT NULL,
           created_at_ms INTEGER NOT NULL,
           request_sha256 BLOB NOT NULL,
           txn_id BLOB NOT NULL,
           event_ids BLOB NOT NULL,
           PRIMARY KEY (namespace, origin_replica_id, client_request_id)
         );
         CREATE TABLE IF NOT EXISTS origin_seq (
           namespace TEXT NOT NULL,
           origin_replica_id BLOB NOT NULL,
           next_seq INTEGER NOT NULL,
           PRIMARY KEY (namespace, origin_replica_id)
         );
         CREATE TABLE IF NOT EXISTS meta (
           key TEXT PRIMARY KEY,
           value TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS hlc (
           actor_id TEXT PRIMARY KEY,
           last_physical_ms INTEGER NOT NULL,
           last_logical INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS segments (
           namespace TEXT NOT NULL,
           segment_id BLOB NOT NULL,
           segment_path TEXT NOT NULL,
           created_at_ms INTEGER NOT NULL,
           last_indexed_offset INTEGER NOT NULL,
           sealed INTEGER NOT NULL DEFAULT 0,
           final_len INTEGER,
           PRIMARY KEY (namespace, segment_id)
         );
         CREATE INDEX IF NOT EXISTS segments_by_ns_created
           ON segments (namespace, created_at_ms);
         CREATE TABLE IF NOT EXISTS replica_liveness (
           replica_id BLOB PRIMARY KEY,
           last_seen_ms INTEGER NOT NULL,
           last_handshake_ms INTEGER NOT NULL,
           role TEXT NOT NULL,
           durability_eligible INTEGER NOT NULL
         );",
    )?;
    Ok(())
}

fn write_meta(conn: &Connection, meta: &StoreMeta) -> Result<(), WalIndexError> {
    set_meta(conn, "store_id", meta.store_id().to_string())?;
    set_meta(conn, "store_epoch", meta.store_epoch().get().to_string())?;
    set_meta(
        conn,
        "index_schema_version",
        meta.index_schema_version.to_string(),
    )?;
    set_meta(
        conn,
        "wal_format_version",
        meta.wal_format_version.to_string(),
    )?;
    Ok(())
}

fn validate_meta(conn: &Connection, meta: &StoreMeta) -> Result<(), WalIndexError> {
    if meta.index_schema_version != INDEX_SCHEMA_VERSION {
        return Err(WalIndexError::SchemaVersionMismatch {
            expected: INDEX_SCHEMA_VERSION,
            got: meta.index_schema_version,
        });
    }

    let stored_id = require_meta(conn, "store_id")?;
    if stored_id != meta.store_id().to_string() {
        return Err(WalIndexError::MetaMismatch {
            key: "store_id",
            expected: meta.store_id().to_string(),
            got: stored_id,
            store_id: meta.store_id(),
        });
    }

    let stored_epoch = require_meta(conn, "store_epoch")?;
    if stored_epoch != meta.store_epoch().get().to_string() {
        return Err(WalIndexError::MetaMismatch {
            key: "store_epoch",
            expected: meta.store_epoch().get().to_string(),
            got: stored_epoch,
            store_id: meta.store_id(),
        });
    }

    let schema_version = require_meta(conn, "index_schema_version")?;
    let schema_version =
        schema_version
            .parse::<u32>()
            .map_err(|_| WalIndexError::MetaMismatch {
                key: "index_schema_version",
                expected: meta.index_schema_version.to_string(),
                got: schema_version,
                store_id: meta.store_id(),
            })?;
    if schema_version != meta.index_schema_version {
        return Err(WalIndexError::SchemaVersionMismatch {
            expected: meta.index_schema_version,
            got: schema_version,
        });
    }

    let wal_version = require_meta(conn, "wal_format_version")?;
    if wal_version != meta.wal_format_version.to_string() {
        return Err(WalIndexError::MetaMismatch {
            key: "wal_format_version",
            expected: meta.wal_format_version.to_string(),
            got: wal_version,
            store_id: meta.store_id(),
        });
    }

    Ok(())
}

fn set_meta(conn: &Connection, key: &'static str, value: String) -> Result<(), WalIndexError> {
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )?;
    Ok(())
}

fn require_meta(conn: &Connection, key: &'static str) -> Result<String, WalIndexError> {
    let value: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            params![key],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    value.ok_or(WalIndexError::MetaMissing { key })
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool, WalIndexError> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        params![name],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn ensure_permissions(path: &Path) -> Result<(), WalIndexError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(
            |source| WalIndexError::Io {
                path: path.to_path_buf(),
                source,
            },
        )?;
    }
    Ok(())
}

fn reject_symlink(path: &Path) -> Result<(), WalIndexError> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(WalIndexError::Symlink {
            path: path.to_path_buf(),
        }),
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(WalIndexError::Io {
            path: path.to_path_buf(),
            source: err,
        }),
    }
}

fn open_connection(
    path: &Path,
    mode: IndexDurabilityMode,
    create: bool,
) -> Result<Connection, WalIndexError> {
    let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE;
    if create {
        flags |= OpenFlags::SQLITE_OPEN_CREATE;
    }
    let conn = Connection::open_with_flags(path, flags)?;
    apply_pragmas(&conn, mode)?;
    conn.busy_timeout(Duration::from_millis(BUSY_TIMEOUT_MS))?;
    Ok(conn)
}

fn apply_pragmas(conn: &Connection, mode: IndexDurabilityMode) -> Result<(), WalIndexError> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", mode.synchronous_value())?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "cache_size", CACHE_SIZE_KB)?;
    Ok(())
}

fn uuid_blob(uuid: Uuid) -> Vec<u8> {
    uuid.as_bytes().to_vec()
}

fn blob_uuid(blob: Vec<u8>) -> Result<Uuid, WalIndexError> {
    let bytes: [u8; 16] = blob
        .try_into()
        .map_err(|_| WalIndexError::EventIdDecode("uuid blob wrong length".to_string()))?;
    Ok(Uuid::from_bytes(bytes))
}

fn blob_32(blob: Vec<u8>) -> Result<[u8; 32], WalIndexError> {
    let bytes: [u8; 32] = blob
        .try_into()
        .map_err(|_| WalIndexError::EventIdDecode("sha blob wrong length".to_string()))?;
    Ok(bytes)
}

pub(crate) fn encode_event_ids(event_ids: &[EventId]) -> Result<Vec<u8>, WalIndexError> {
    let mut buf = Vec::new();
    let mut enc = Encoder::new(&mut buf);
    enc.array(event_ids.len() as u64)?;
    for id in event_ids {
        encode_event_id(&mut enc, id)?;
    }
    Ok(buf)
}

fn encode_event_id(enc: &mut Encoder<&mut Vec<u8>>, id: &EventId) -> Result<(), WalIndexError> {
    enc.array(3)?;
    enc.bytes(id.origin_replica_id.as_uuid().as_bytes())?;
    enc.str(id.namespace.as_str())?;
    enc.u64(id.origin_seq.get())?;
    Ok(())
}

fn decode_event_ids(bytes: &[u8]) -> Result<Vec<EventId>, WalIndexError> {
    let mut dec = Decoder::new(bytes);
    let len = dec.array()?.ok_or_else(|| {
        WalIndexError::EventIdDecode("event_ids CBOR must be definite".to_string())
    })?;
    let mut ids = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let item_len = dec.array()?.ok_or_else(|| {
            WalIndexError::EventIdDecode("event_ids entry must be definite".to_string())
        })?;
        if item_len != 3 {
            return Err(WalIndexError::EventIdDecode(
                "event_ids entry must be len 3".to_string(),
            ));
        }
        let replica_bytes = dec.bytes()?;
        let replica_uuid = blob_uuid(replica_bytes.to_vec())?;
        let namespace = dec.str()?;
        let namespace = NamespaceId::parse(namespace)
            .map_err(|err| WalIndexError::EventIdDecode(err.to_string()))?;
        let seq = dec.u64()?;
        let origin_seq = Seq1::from_u64(seq)
            .ok_or_else(|| WalIndexError::EventIdDecode("origin_seq must be >=1".to_string()))?;
        ids.push(EventId::new(
            ReplicaId::new(replica_uuid),
            namespace,
            origin_seq,
        ));
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    fn test_meta() -> StoreMeta {
        let store_id = crate::core::StoreId::new(Uuid::from_bytes([7u8; 16]));
        let identity = crate::core::StoreIdentity::new(store_id, crate::core::StoreEpoch::new(1));
        let versions = crate::core::StoreMetaVersions::new(1, 2, 3, 4, INDEX_SCHEMA_VERSION);
        StoreMeta::new(
            identity,
            crate::core::ReplicaId::new(Uuid::from_bytes([8u8; 16])),
            versions,
            1_700_000_000_000,
        )
    }

    #[test]
    fn sqlite_index_initializes_schema_and_meta() {
        let temp = TempDir::new().unwrap();
        let meta = test_meta();
        SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache).unwrap();

        let conn = open_connection(
            &temp.path().join("index").join("wal.sqlite"),
            IndexDurabilityMode::Cache,
            false,
        )
        .unwrap();
        for table in [
            "events",
            "watermarks",
            "client_requests",
            "origin_seq",
            "meta",
            "hlc",
            "segments",
            "replica_liveness",
        ] {
            assert!(table_exists(&conn, table).unwrap());
        }
        assert!(index_exists(&conn, "events_by_client_request"));
        assert!(index_exists(&conn, "events_by_origin_seq"));
        assert!(index_exists(&conn, "segments_by_ns_created"));
    }

    #[test]
    fn sqlite_index_rejects_schema_version_mismatch() {
        let temp = TempDir::new().unwrap();
        let meta = test_meta();
        let db_path = temp.path().join("index").join("wal.sqlite");
        std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();
        let conn = Connection::open(&db_path).unwrap();
        initialize_schema(&conn).unwrap();
        set_meta(&conn, "store_id", meta.store_id().to_string()).unwrap();
        set_meta(&conn, "store_epoch", meta.store_epoch().get().to_string()).unwrap();
        set_meta(&conn, "index_schema_version", "999".to_string()).unwrap();
        set_meta(
            &conn,
            "wal_format_version",
            meta.wal_format_version.to_string(),
        )
        .unwrap();

        let result = SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache);
        assert!(matches!(
            result,
            Err(WalIndexError::SchemaVersionMismatch { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn sqlite_index_rejects_symlinked_db_path() {
        let temp = TempDir::new().unwrap();
        let meta = test_meta();
        let index_dir = temp.path().join("index");
        std::fs::create_dir_all(&index_dir).unwrap();
        let target = temp.path().join("target.sqlite");
        std::fs::write(&target, b"").unwrap();
        let db_path = index_dir.join("wal.sqlite");
        symlink(&target, &db_path).unwrap();

        let err = match SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache) {
            Ok(_) => panic!("expected symlink rejection"),
            Err(err) => err,
        };
        assert!(matches!(err, WalIndexError::Symlink { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn sqlite_index_rejects_symlinked_index_dir() {
        let temp = TempDir::new().unwrap();
        let meta = test_meta();
        let target = temp.path().join("real-index");
        std::fs::create_dir_all(&target).unwrap();
        let index_dir = temp.path().join("index");
        symlink(&target, &index_dir).unwrap();

        let err = match SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache) {
            Ok(_) => panic!("expected symlink rejection"),
            Err(err) => err,
        };
        assert!(matches!(err, WalIndexError::Symlink { .. }));
    }

    #[test]
    fn sqlite_index_round_trips_events_and_requests() {
        let temp = TempDir::new().unwrap();
        let meta = test_meta();
        let index = SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache).unwrap();
        let ns = NamespaceId::core();
        let origin = meta.replica_id;
        let mut txn = index.writer().begin_txn().unwrap();
        let seq = txn.next_origin_seq(&ns, &origin).unwrap();
        let origin_seq = Seq1::from_u64(seq).unwrap();
        let event_id = EventId::new(origin, ns.clone(), origin_seq);
        let txn_id = TxnId::new(Uuid::from_bytes([2u8; 16]));
        let client_request_id = ClientRequestId::new(Uuid::from_bytes([3u8; 16]));
        let sha = [9u8; 32];
        let prev_sha = None;
        let segment_id = SegmentId::new(Uuid::from_bytes([4u8; 16]));
        txn.record_event(
            &ns,
            &event_id,
            sha,
            prev_sha,
            segment_id,
            128,
            64,
            1_700_000,
            txn_id,
            Some(client_request_id),
            Some([7u8; 32]),
        )
        .unwrap();
        txn.update_watermark(&ns, &origin, seq, seq, Some(sha), Some(sha))
            .unwrap();
        txn.upsert_client_request(
            &ns,
            &origin,
            client_request_id,
            [7u8; 32],
            txn_id,
            &[event_id.clone()],
            1_700_000,
        )
        .unwrap();
        txn.commit().unwrap();

        let reader = index.reader();
        assert_eq!(reader.lookup_event_sha(&ns, &event_id).unwrap(), Some(sha));
        let items = reader.iter_from(&ns, &origin, 0, 1024).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].event_id, event_id);
        assert_eq!(items[0].txn_id, txn_id);
        assert_eq!(items[0].client_request_id, Some(client_request_id));
        assert_eq!(items[0].segment_id, segment_id);
        assert_eq!(items[0].len, 64);
        assert_eq!(items[0].offset, 128);

        let req = reader
            .lookup_client_request(&ns, &origin, client_request_id)
            .unwrap()
            .unwrap();
        assert_eq!(req.request_sha256, [7u8; 32]);
        assert_eq!(req.txn_id, txn_id);
        assert_eq!(req.event_ids, vec![event_id]);
        assert_eq!(req.created_at_ms, 1_700_000);
        assert_eq!(reader.max_origin_seq(&ns, &origin).unwrap(), 1);
    }

    #[test]
    fn sqlite_index_preserves_client_request_mapping_on_conflict() {
        let temp = TempDir::new().unwrap();
        let meta = test_meta();
        let index = SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache).unwrap();
        let ns = NamespaceId::core();
        let origin = meta.replica_id;
        let client_request_id = ClientRequestId::new(Uuid::from_bytes([3u8; 16]));
        let first_txn_id = TxnId::new(Uuid::from_bytes([2u8; 16]));
        let second_txn_id = TxnId::new(Uuid::from_bytes([4u8; 16]));
        let first_event_id = EventId::new(origin, ns.clone(), Seq1::from_u64(1).unwrap());
        let second_event_id = EventId::new(origin, ns.clone(), Seq1::from_u64(2).unwrap());

        let mut txn = index.writer().begin_txn().unwrap();
        txn.upsert_client_request(
            &ns,
            &origin,
            client_request_id,
            [7u8; 32],
            first_txn_id,
            &[first_event_id.clone()],
            1_700_000,
        )
        .unwrap();
        txn.commit().unwrap();

        let mut txn = index.writer().begin_txn().unwrap();
        txn.upsert_client_request(
            &ns,
            &origin,
            client_request_id,
            [7u8; 32],
            second_txn_id,
            &[second_event_id.clone()],
            1_800_000,
        )
        .unwrap();
        txn.commit().unwrap();

        let reader = index.reader();
        let req = reader
            .lookup_client_request(&ns, &origin, client_request_id)
            .unwrap()
            .unwrap();
        assert_eq!(req.request_sha256, [7u8; 32]);
        assert_eq!(req.txn_id, first_txn_id);
        assert_eq!(req.event_ids, vec![first_event_id.clone()]);
        assert_eq!(req.created_at_ms, 1_700_000);

        let mut txn = index.writer().begin_txn().unwrap();
        let err = txn
            .upsert_client_request(
                &ns,
                &origin,
                client_request_id,
                [9u8; 32],
                second_txn_id,
                &[second_event_id],
                1_900_000,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            WalIndexError::ClientRequestIdReuseMismatch { .. }
        ));
        txn.rollback().unwrap();

        let reader = index.reader();
        let req = reader
            .lookup_client_request(&ns, &origin, client_request_id)
            .unwrap()
            .unwrap();
        assert_eq!(req.request_sha256, [7u8; 32]);
        assert_eq!(req.txn_id, first_txn_id);
        assert_eq!(req.event_ids, vec![first_event_id]);
        assert_eq!(req.created_at_ms, 1_700_000);
    }

    #[test]
    fn sqlite_index_persists_hlc_state() {
        let temp = TempDir::new().unwrap();
        let meta = test_meta();
        let index = SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache).unwrap();
        let actor = ActorId::new("alice").unwrap();

        let mut txn = index.writer().begin_txn().unwrap();
        txn.update_hlc(&HlcRow {
            actor_id: actor.clone(),
            last_physical_ms: 1_700_000,
            last_logical: 7,
        })
        .unwrap();
        txn.commit().unwrap();

        let rows = index.reader().load_hlc().unwrap();
        assert_eq!(
            rows,
            vec![HlcRow {
                actor_id: actor,
                last_physical_ms: 1_700_000,
                last_logical: 7,
            }]
        );
    }

    #[test]
    fn sqlite_index_hlc_upsert_overwrites() {
        let temp = TempDir::new().unwrap();
        let meta = test_meta();
        let index = SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache).unwrap();
        let actor = ActorId::new("alice").unwrap();

        let mut txn = index.writer().begin_txn().unwrap();
        txn.update_hlc(&HlcRow {
            actor_id: actor.clone(),
            last_physical_ms: 1_700_000,
            last_logical: 7,
        })
        .unwrap();
        txn.update_hlc(&HlcRow {
            actor_id: actor.clone(),
            last_physical_ms: 1_800_000,
            last_logical: 2,
        })
        .unwrap();
        txn.commit().unwrap();

        let rows = index.reader().load_hlc().unwrap();
        assert_eq!(
            rows,
            vec![HlcRow {
                actor_id: actor,
                last_physical_ms: 1_800_000,
                last_logical: 2,
            }]
        );
    }

    #[test]
    fn sqlite_index_record_event_idempotent_on_duplicate_sha() {
        let temp = TempDir::new().unwrap();
        let meta = test_meta();
        let index = SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache).unwrap();
        let ns = NamespaceId::core();
        let origin = meta.replica_id;
        let mut txn = index.writer().begin_txn().unwrap();
        let seq = txn.next_origin_seq(&ns, &origin).unwrap();
        let origin_seq = Seq1::from_u64(seq).unwrap();
        let event_id = EventId::new(origin, ns.clone(), origin_seq);
        let txn_id = TxnId::new(Uuid::from_bytes([2u8; 16]));
        let sha = [9u8; 32];
        let segment_id = SegmentId::new(Uuid::from_bytes([4u8; 16]));

        txn.record_event(
            &ns, &event_id, sha, None, segment_id, 128, 64, 1_700_000, txn_id, None, None,
        )
        .unwrap();
        txn.record_event(
            &ns, &event_id, sha, None, segment_id, 128, 64, 1_700_000, txn_id, None, None,
        )
        .unwrap();
        txn.commit().unwrap();

        let reader = index.reader();
        assert_eq!(reader.lookup_event_sha(&ns, &event_id).unwrap(), Some(sha));
    }

    #[test]
    fn sqlite_index_record_event_equivocation_errors() {
        let temp = TempDir::new().unwrap();
        let meta = test_meta();
        let index = SqliteWalIndex::open(temp.path(), &meta, IndexDurabilityMode::Cache).unwrap();
        let ns = NamespaceId::core();
        let origin = meta.replica_id;
        let mut txn = index.writer().begin_txn().unwrap();
        let seq = txn.next_origin_seq(&ns, &origin).unwrap();
        let origin_seq = Seq1::from_u64(seq).unwrap();
        let event_id = EventId::new(origin, ns.clone(), origin_seq);
        let txn_id = TxnId::new(Uuid::from_bytes([2u8; 16]));
        let segment_id = SegmentId::new(Uuid::from_bytes([4u8; 16]));
        let sha = [9u8; 32];
        let other_sha = [8u8; 32];

        txn.record_event(
            &ns, &event_id, sha, None, segment_id, 128, 64, 1_700_000, txn_id, None, None,
        )
        .unwrap();

        let err = txn
            .record_event(
                &ns, &event_id, other_sha, None, segment_id, 128, 64, 1_700_000, txn_id, None, None,
            )
            .unwrap_err();

        match err {
            WalIndexError::Equivocation {
                namespace,
                origin,
                seq,
                existing_sha256,
                new_sha256,
            } => {
                assert_eq!(namespace, ns);
                assert_eq!(origin, meta.replica_id);
                assert_eq!(seq, origin_seq.get());
                assert_eq!(existing_sha256, sha);
                assert_eq!(new_sha256, other_sha);
            }
            other => panic!("expected equivocation, got {other:?}"),
        }
    }

    fn index_exists(conn: &Connection, name: &str) -> bool {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                params![name],
                |row| row.get(0),
            )
            .unwrap();
        count > 0
    }
}
