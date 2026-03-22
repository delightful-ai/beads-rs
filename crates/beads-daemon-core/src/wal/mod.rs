//! Lightweight WAL index primitives shared with model adapters.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::core::error::details as error_details;
use crate::core::{
    ActorId, Applied, CliErrorCode, ClientRequestId, Durable, ErrorCode, ErrorPayload, EventId,
    HeadStatus, IntoErrorPayload, NamespaceId, ProtocolErrorCode, ReplicaId, SegmentId, Seq0, Seq1,
    StoreId, Transience, TxnId, Watermark, WatermarkPair,
};
pub use crate::core::{ReplicaDurabilityRole, ReplicaDurabilityRoleError};
use crate::durability::DurabilityRequestClaim;

#[cfg(any(feature = "test-harness", test))]
pub mod contract;

#[cfg(feature = "wal-fs")]
pub mod event_wal;
#[cfg(feature = "wal-fs")]
pub mod frame;
#[cfg(all(feature = "wal-fs", feature = "wal-sqlite"))]
pub mod fsck;
#[cfg(feature = "wal-sqlite")]
pub mod index;
pub mod memory_index;
#[cfg(feature = "wal-fs")]
pub mod memory_wal;
#[cfg(feature = "wal-fs")]
pub mod record;
#[cfg(feature = "wal-fs")]
pub mod replay;
#[cfg(feature = "wal-fs")]
pub(crate) mod seams;
#[cfg(feature = "wal-fs")]
pub mod segment;

#[cfg(feature = "wal-fs")]
pub use event_wal::{EventWal, SegmentSnapshot};
#[cfg(feature = "wal-fs")]
pub use frame::{FRAME_CRC_OFFSET, FRAME_HEADER_LEN, FrameReader, FrameWriter};
#[cfg(feature = "wal-sqlite")]
pub use index::SqliteWalIndex;
pub use memory_index::{MemoryWalIndex, MemoryWalIndexSnapshot};
#[cfg(feature = "wal-fs")]
pub use memory_wal::MemoryEventWal;
#[cfg(feature = "wal-fs")]
pub use record::{
    Record, RecordFlags, RecordHeader, RecordHeaderMismatch, RecordVerifyError, RequestProof,
    Unverified, UnverifiedRecord, Verified, VerifiedRecord,
};
#[cfg(feature = "wal-fs")]
pub use replay::{
    RecordShaMismatchInfo, ReplayMode, ReplayStats, WalReplayError, catch_up_index, rebuild_index,
};
#[cfg(feature = "wal-fs")]
pub use seams::{
    AcknowledgedWalAppend, PendingWalAppend, WalAppend, WalAppendDurabilityEffect,
    WalIndexTxnProvider, WalReadRange,
};
#[cfg(feature = "wal-fs")]
pub use segment::{
    AppendOutcome, SEGMENT_HEADER_PREFIX_LEN, SegmentConfig, SegmentHeader, SegmentSyncMode,
    SegmentWriter, WAL_FORMAT_VERSION,
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ClientRequestEventIdsError {
    #[error("event_ids must be non-empty")]
    Empty,
    #[error("event_ids namespace mismatch (expected {expected}, got {got})")]
    MixedNamespace {
        expected: NamespaceId,
        got: NamespaceId,
    },
    #[error("event_ids origin mismatch (expected {expected}, got {got})")]
    MixedOrigin { expected: ReplicaId, got: ReplicaId },
    #[error("event_ids must be strictly increasing (prev {prev}, next {next})")]
    NonIncreasing { prev: Seq1, next: Seq1 },
}

#[derive(Debug, Error)]
pub enum WalIndexError {
    #[error("sqlite error: {message}")]
    Sql { message: String },
    #[error("io error at {path:?}: {reason}")]
    Io {
        path: Option<PathBuf>,
        reason: String,
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
    CborEncode(String),
    #[error("event id decode failed: {0}")]
    CborDecode(String),
    #[error("event id decode invalid: {0}")]
    EventIdDecode(String),
    #[error("client request event ids invalid: {0}")]
    ClientRequestEventIds(#[from] ClientRequestEventIdsError),
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
    #[error("event conflict for {namespace} {origin} seq {seq}: {reason}")]
    EventConflict {
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: u64,
        reason: String,
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
    #[error("replica liveness row decode failed: {0}")]
    ReplicaLivenessRowDecode(String),
}

impl WalIndexError {
    pub fn code(&self) -> ErrorCode {
        match self {
            WalIndexError::SchemaVersionMismatch { .. } => {
                ProtocolErrorCode::IndexRebuildRequired.into()
            }
            WalIndexError::Equivocation { .. } => ProtocolErrorCode::Equivocation.into(),
            WalIndexError::EventConflict { .. } => ProtocolErrorCode::IndexCorrupt.into(),
            WalIndexError::ClientRequestIdReuseMismatch { .. } => {
                ProtocolErrorCode::ClientRequestIdReuseMismatch.into()
            }
            WalIndexError::Symlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
            WalIndexError::MetaMismatch { key, .. } => match *key {
                "store_id" => ProtocolErrorCode::WrongStore.into(),
                "store_epoch" => ProtocolErrorCode::StoreEpochMismatch.into(),
                _ => ProtocolErrorCode::IndexCorrupt.into(),
            },
            WalIndexError::MetaMissing { .. }
            | WalIndexError::EventIdDecode(_)
            | WalIndexError::ClientRequestEventIds(_)
            | WalIndexError::HlcRowDecode(_)
            | WalIndexError::SegmentRowDecode(_)
            | WalIndexError::WatermarkRowDecode(_)
            | WalIndexError::ReplicaLivenessRowDecode(_)
            | WalIndexError::CborDecode(_)
            | WalIndexError::CborEncode(_)
            | WalIndexError::OriginSeqOverflow { .. } => ProtocolErrorCode::IndexCorrupt.into(),
            WalIndexError::Sql { .. } => ProtocolErrorCode::IndexCorrupt.into(),
            WalIndexError::Io { .. } => CliErrorCode::IoError.into(),
        }
    }

    pub fn transience(&self) -> Transience {
        match self {
            WalIndexError::Symlink { .. } => Transience::Permanent,
            WalIndexError::SchemaVersionMismatch { .. } => Transience::Retryable,
            WalIndexError::MetaMismatch {
                key: "store_id" | "store_epoch",
                ..
            } => Transience::Permanent,
            WalIndexError::MetaMismatch { .. } => Transience::Retryable,
            WalIndexError::Equivocation { .. }
            | WalIndexError::EventConflict { .. }
            | WalIndexError::ClientRequestIdReuseMismatch { .. } => Transience::Permanent,
            _ => Transience::Retryable,
        }
    }

    pub fn into_payload_with_context(self, message: String, retryable: bool) -> ErrorPayload {
        let code = self.code();
        match self {
            WalIndexError::Symlink { path } => ErrorPayload::new(
                ProtocolErrorCode::PathSymlinkRejected.into(),
                message,
                retryable,
            )
            .with_details(error_details::PathSymlinkRejectedDetails {
                path: path.display().to_string(),
            }),
            WalIndexError::SchemaVersionMismatch { expected, got } => ErrorPayload::new(
                ProtocolErrorCode::IndexRebuildRequired.into(),
                message,
                retryable,
            )
            .with_details(error_details::IndexRebuildRequiredDetails {
                namespace: None,
                reason: format!("index schema version mismatch: expected {expected}, got {got}"),
            }),
            WalIndexError::Equivocation {
                namespace,
                origin,
                seq,
                existing_sha256,
                new_sha256,
            } => ErrorPayload::new(ProtocolErrorCode::Equivocation.into(), message, retryable)
                .with_details(error_details::EquivocationDetails {
                    eid: error_details::EventIdDetails {
                        namespace: namespace.clone(),
                        origin_replica_id: origin,
                        origin_seq: seq,
                    },
                    existing_sha256: hex::encode(existing_sha256),
                    new_sha256: hex::encode(new_sha256),
                }),
            WalIndexError::ClientRequestIdReuseMismatch {
                namespace,
                client_request_id,
                expected_request_sha256,
                got_request_sha256,
                ..
            } => ErrorPayload::new(
                ProtocolErrorCode::ClientRequestIdReuseMismatch.into(),
                message,
                retryable,
            )
            .with_details(error_details::ClientRequestIdReuseMismatchDetails {
                namespace: namespace.clone(),
                client_request_id,
                expected_request_sha256: hex::encode(expected_request_sha256),
                got_request_sha256: hex::encode(got_request_sha256),
            }),
            WalIndexError::EventConflict { reason, .. } => {
                ErrorPayload::new(ProtocolErrorCode::IndexCorrupt.into(), message, retryable)
                    .with_details(error_details::IndexCorruptDetails { reason })
            }
            WalIndexError::MetaMismatch {
                key: "store_id",
                expected,
                got,
                ..
            } => {
                let expected = StoreId::parse_str(&expected).ok();
                let got = StoreId::parse_str(&got).ok();
                if let (Some(expected), Some(got)) = (expected, got) {
                    ErrorPayload::new(ProtocolErrorCode::WrongStore.into(), message, retryable)
                        .with_details(error_details::WrongStoreDetails {
                            expected_store_id: expected,
                            got_store_id: got,
                        })
                } else {
                    let reason = message.clone();
                    ErrorPayload::new(ProtocolErrorCode::IndexCorrupt.into(), message, retryable)
                        .with_details(error_details::IndexCorruptDetails { reason })
                }
            }
            WalIndexError::MetaMismatch {
                key: "store_epoch",
                expected,
                got,
                store_id,
            } => {
                let expected_epoch = expected.parse::<u64>().ok();
                let got_epoch = got.parse::<u64>().ok();
                if let (Some(expected_epoch), Some(got_epoch)) = (expected_epoch, got_epoch) {
                    ErrorPayload::new(
                        ProtocolErrorCode::StoreEpochMismatch.into(),
                        message,
                        retryable,
                    )
                    .with_details(error_details::StoreEpochMismatchDetails {
                        store_id,
                        expected_epoch,
                        got_epoch,
                    })
                } else {
                    let reason = message.clone();
                    ErrorPayload::new(ProtocolErrorCode::IndexCorrupt.into(), message, retryable)
                        .with_details(error_details::IndexCorruptDetails { reason })
                }
            }
            WalIndexError::MetaMismatch { .. }
            | WalIndexError::MetaMissing { .. }
            | WalIndexError::EventIdDecode(_)
            | WalIndexError::ClientRequestEventIds(_)
            | WalIndexError::HlcRowDecode(_)
            | WalIndexError::SegmentRowDecode(_)
            | WalIndexError::WatermarkRowDecode(_)
            | WalIndexError::ReplicaLivenessRowDecode(_)
            | WalIndexError::CborDecode(_)
            | WalIndexError::CborEncode(_)
            | WalIndexError::OriginSeqOverflow { .. }
            | WalIndexError::Sql { .. } => ErrorPayload::new(
                ProtocolErrorCode::IndexCorrupt.into(),
                message.clone(),
                retryable,
            )
            .with_details(error_details::IndexCorruptDetails { reason: message }),
            WalIndexError::Io { .. } => ErrorPayload::new(code, message, retryable),
        }
    }
}

impl IntoErrorPayload for WalIndexError {
    fn into_error_payload(self) -> ErrorPayload {
        let message = self.to_string();
        let retryable = self.transience().is_retryable();
        self.into_payload_with_context(message, retryable)
    }
}

pub trait WalIndex: Send + Sync {
    fn writer(&self) -> Box<dyn WalIndexWriter>;
    fn reader(&self) -> Box<dyn WalIndexReader>;
    fn durability_mode(&self) -> IndexDurabilityMode;
    fn checkpoint_truncate(&self) -> Result<(), WalIndexError>;
}

pub trait WalIndexWriter {
    fn begin_txn(&self) -> Result<Box<dyn WalIndexTxn>, WalIndexError>;
}

pub trait WalIndexTxn {
    fn next_origin_seq(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
    ) -> Result<Seq1, WalIndexError>;
    fn set_next_origin_seq(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        next_seq: Seq1,
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
    ) -> Result<(), WalIndexError>;
    fn update_watermark(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        watermarks: WatermarkPair,
    ) -> Result<(), WalIndexError>;
    fn update_hlc(&mut self, hlc: &HlcRow) -> Result<(), WalIndexError>;
    fn upsert_segment(&mut self, segment: &SegmentRow) -> Result<(), WalIndexError>;
    fn replace_namespace_segments(
        &mut self,
        ns: &NamespaceId,
        segments: &[SegmentRow],
    ) -> Result<(), WalIndexError>;
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
        durability_claim: Option<&DurabilityRequestClaim>,
    ) -> Result<(), WalIndexError>;
    fn upsert_replica_liveness(&mut self, row: &ReplicaLivenessRow) -> Result<(), WalIndexError>;
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
    fn list_segment_namespaces(&self) -> Result<Vec<NamespaceId>, WalIndexError>;
    fn load_watermarks(&self) -> Result<Vec<WatermarkRow>, WalIndexError>;
    fn load_hlc(&self) -> Result<Vec<HlcRow>, WalIndexError>;
    fn load_replica_liveness(&self) -> Result<Vec<ReplicaLivenessRow>, WalIndexError>;
    fn iter_from(
        &self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        from_seq_excl: Seq0,
        max_bytes: usize,
    ) -> Result<Vec<IndexedRangeItem>, WalIndexError>;
    fn lookup_client_request(
        &self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        client_request_id: ClientRequestId,
    ) -> Result<Option<ClientRequestRow>, WalIndexError>;
    fn max_origin_seq(&self, ns: &NamespaceId, origin: &ReplicaId) -> Result<Seq0, WalIndexError>;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientRequestEventIds {
    namespace: NamespaceId,
    origin: ReplicaId,
    seqs: Vec<Seq1>,
}

impl ClientRequestEventIds {
    pub fn new(event_ids: Vec<EventId>) -> Result<Self, ClientRequestEventIdsError> {
        let mut iter = event_ids.into_iter();
        let first = iter.next().ok_or(ClientRequestEventIdsError::Empty)?;
        let namespace = first.namespace.clone();
        let origin = first.origin_replica_id;
        let mut seqs = Vec::with_capacity(iter.size_hint().0 + 1);
        let mut prev = first.origin_seq;
        seqs.push(prev);
        for id in iter {
            let EventId {
                origin_replica_id,
                namespace: id_namespace,
                origin_seq,
            } = id;
            if id_namespace != namespace {
                return Err(ClientRequestEventIdsError::MixedNamespace {
                    expected: namespace,
                    got: id_namespace,
                });
            }
            if origin_replica_id != origin {
                return Err(ClientRequestEventIdsError::MixedOrigin {
                    expected: origin,
                    got: origin_replica_id,
                });
            }
            if origin_seq <= prev {
                return Err(ClientRequestEventIdsError::NonIncreasing {
                    prev,
                    next: origin_seq,
                });
            }
            prev = origin_seq;
            seqs.push(prev);
        }
        Ok(Self {
            namespace,
            origin,
            seqs,
        })
    }

    pub fn single(event_id: EventId) -> Self {
        Self {
            namespace: event_id.namespace,
            origin: event_id.origin_replica_id,
            seqs: vec![event_id.origin_seq],
        }
    }

    pub fn ensure_matches(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
    ) -> Result<(), ClientRequestEventIdsError> {
        if &self.namespace != namespace {
            return Err(ClientRequestEventIdsError::MixedNamespace {
                expected: namespace.clone(),
                got: self.namespace.clone(),
            });
        }
        if &self.origin != origin {
            return Err(ClientRequestEventIdsError::MixedOrigin {
                expected: *origin,
                got: self.origin,
            });
        }
        Ok(())
    }

    pub fn event_ids(&self) -> Vec<EventId> {
        self.seqs
            .iter()
            .map(|seq| EventId::new(self.origin, self.namespace.clone(), *seq))
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientRequestRow {
    pub request_sha256: [u8; 32],
    pub txn_id: TxnId,
    pub event_ids: ClientRequestEventIds,
    pub created_at_ms: u64,
    pub durability_claim: Option<DurabilityRequestClaim>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WalCursorOffset(u64);

impl WalCursorOffset {
    pub const fn new(offset: u64) -> Self {
        Self(offset)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SegmentRow {
    Open {
        namespace: NamespaceId,
        segment_id: SegmentId,
        segment_path: PathBuf,
        created_at_ms: u64,
        last_indexed_offset: WalCursorOffset,
    },
    Sealed {
        namespace: NamespaceId,
        segment_id: SegmentId,
        segment_path: PathBuf,
        created_at_ms: u64,
        last_indexed_offset: WalCursorOffset,
        final_len: u64,
    },
}

impl SegmentRow {
    pub fn namespace(&self) -> &NamespaceId {
        match self {
            SegmentRow::Open { namespace, .. } | SegmentRow::Sealed { namespace, .. } => namespace,
        }
    }

    pub fn segment_id(&self) -> SegmentId {
        match self {
            SegmentRow::Open { segment_id, .. } | SegmentRow::Sealed { segment_id, .. } => {
                *segment_id
            }
        }
    }

    pub fn segment_path(&self) -> &Path {
        match self {
            SegmentRow::Open { segment_path, .. } | SegmentRow::Sealed { segment_path, .. } => {
                segment_path.as_path()
            }
        }
    }

    pub fn created_at_ms(&self) -> u64 {
        match self {
            SegmentRow::Open { created_at_ms, .. } | SegmentRow::Sealed { created_at_ms, .. } => {
                *created_at_ms
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WatermarkRow {
    pub namespace: NamespaceId,
    pub origin: ReplicaId,
    watermarks: WatermarkPair,
}

impl WatermarkRow {
    pub fn new(namespace: NamespaceId, origin: ReplicaId, watermarks: WatermarkPair) -> Self {
        Self {
            namespace,
            origin,
            watermarks,
        }
    }

    pub fn watermarks(&self) -> WatermarkPair {
        self.watermarks
    }

    pub fn applied(&self) -> Watermark<Applied> {
        self.watermarks.applied()
    }

    pub fn durable(&self) -> Watermark<Durable> {
        self.watermarks.durable()
    }

    pub fn applied_seq(&self) -> u64 {
        self.applied().seq().get()
    }

    pub fn durable_seq(&self) -> u64 {
        self.durable().seq().get()
    }

    pub fn applied_head_sha(&self) -> Option<[u8; 32]> {
        match self.applied().head() {
            HeadStatus::Known(sha) => Some(sha),
            HeadStatus::Genesis => None,
        }
    }

    pub fn durable_head_sha(&self) -> Option<[u8; 32]> {
        match self.durable().head() {
            HeadStatus::Known(sha) => Some(sha),
            HeadStatus::Genesis => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HlcRow {
    pub actor_id: ActorId,
    pub last_physical_ms: u64,
    pub last_logical: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ReplicaLivenessRow {
    pub replica_id: ReplicaId,
    pub last_seen_ms: u64,
    pub last_handshake_ms: u64,
    pub role: ReplicaDurabilityRole,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexDurabilityMode {
    Cache,
    Durable,
}

impl SegmentRow {
    pub fn open(
        namespace: NamespaceId,
        segment_id: SegmentId,
        segment_path: PathBuf,
        created_at_ms: u64,
        last_indexed_offset: WalCursorOffset,
    ) -> Self {
        Self::Open {
            namespace,
            segment_id,
            segment_path,
            created_at_ms,
            last_indexed_offset,
        }
    }

    pub fn sealed(
        namespace: NamespaceId,
        segment_id: SegmentId,
        segment_path: PathBuf,
        created_at_ms: u64,
        last_indexed_offset: WalCursorOffset,
        final_len: u64,
    ) -> Self {
        Self::Sealed {
            namespace,
            segment_id,
            segment_path,
            created_at_ms,
            last_indexed_offset,
            final_len,
        }
    }

    pub fn last_indexed_offset(&self) -> WalCursorOffset {
        match self {
            SegmentRow::Open {
                last_indexed_offset,
                ..
            }
            | SegmentRow::Sealed {
                last_indexed_offset,
                ..
            } => *last_indexed_offset,
        }
    }

    pub fn is_sealed(&self) -> bool {
        matches!(self, SegmentRow::Sealed { .. })
    }

    pub fn final_len(&self) -> Option<u64> {
        match self {
            SegmentRow::Open { .. } => None,
            SegmentRow::Sealed { final_len, .. } => Some(*final_len),
        }
    }
}

impl ClientRequestEventIds {
    pub fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    pub fn origin(&self) -> ReplicaId {
        self.origin
    }

    pub fn seqs(&self) -> &[Seq1] {
        &self.seqs
    }

    pub fn len(&self) -> usize {
        self.seqs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seqs.is_empty()
    }
}

impl IndexDurabilityMode {
    pub fn synchronous_value(self) -> &'static str {
        match self {
            IndexDurabilityMode::Cache => "NORMAL",
            IndexDurabilityMode::Durable => "FULL",
        }
    }
}

impl ClientRequestEventIds {
    pub fn max_seq(&self) -> Seq1 {
        self.seqs
            .last()
            .copied()
            .expect("ClientRequestEventIds is non-empty")
    }

    pub fn first_event_id(&self) -> EventId {
        EventId::new(self.origin, self.namespace.clone(), self.seqs[0])
    }
}

#[cfg(feature = "wal-fs")]
pub type EventWalResult<T> = Result<T, EventWalError>;

#[cfg(feature = "wal-fs")]
pub trait ReadSeek: std::io::Read + std::io::Seek {}

#[cfg(feature = "wal-fs")]
impl<T: std::io::Read + std::io::Seek> ReadSeek for T {}

#[cfg(feature = "wal-fs")]
pub fn open_segment_reader(path: &Path) -> EventWalResult<Box<dyn ReadSeek>> {
    if let Some(bytes) = memory_wal::read_segment_bytes(path) {
        return Ok(Box::new(std::io::Cursor::new(bytes)));
    }
    let file = std::fs::File::open(path).map_err(|source| EventWalError::Io {
        path: Some(path.to_path_buf()),
        source,
    })?;
    Ok(Box::new(file))
}

#[cfg(feature = "wal-fs")]
#[derive(Debug, Error)]
pub enum EventWalError {
    #[error("io error at {path:?}: {source}")]
    Io {
        path: Option<PathBuf>,
        #[source]
        source: std::io::Error,
    },
    #[error("path is a symlink: {path:?}")]
    Symlink { path: PathBuf },
    #[error("record exceeds max bytes {max_bytes} (got {got_bytes})")]
    RecordTooLarge { max_bytes: usize, got_bytes: usize },
    #[error("frame magic mismatch: got {got:#x}")]
    FrameMagicMismatch { got: u32 },
    #[error("frame length invalid: {reason}")]
    FrameLengthInvalid { reason: String },
    #[error("frame crc32c mismatch: expected {expected:#x}, got {got:#x}")]
    FrameCrcMismatch { expected: u32, got: u32 },
    #[error("record header invalid: {reason}")]
    RecordHeaderInvalid { reason: String },
    #[error("segment header invalid: {reason}")]
    SegmentHeaderInvalid { reason: String },
    #[error("segment header wal format unsupported: got {got}, supported {supported}")]
    SegmentHeaderUnsupportedVersion { got: u32, supported: u32 },
    #[error("segment header magic mismatch: got {got:?}")]
    SegmentHeaderMagicMismatch { got: [u8; 5] },
    #[error("segment header crc32c mismatch: expected {expected:#x}, got {got:#x}")]
    SegmentHeaderCrcMismatch { expected: u32, got: u32 },
}

#[cfg(feature = "wal-fs")]
impl EventWalError {
    pub fn code(&self) -> ErrorCode {
        match self {
            EventWalError::RecordTooLarge { .. } => ProtocolErrorCode::WalRecordTooLarge.into(),
            EventWalError::SegmentHeaderUnsupportedVersion { .. } => {
                ProtocolErrorCode::WalFormatUnsupported.into()
            }
            EventWalError::Symlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
            EventWalError::Io { source, .. } => {
                if source.kind() == std::io::ErrorKind::PermissionDenied {
                    ProtocolErrorCode::PermissionDenied.into()
                } else {
                    CliErrorCode::IoError.into()
                }
            }
            _ => ProtocolErrorCode::WalCorrupt.into(),
        }
    }

    pub fn transience(&self) -> Transience {
        match self {
            EventWalError::Symlink { .. } => Transience::Permanent,
            EventWalError::Io { source, .. } => {
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

#[cfg(feature = "wal-fs")]
impl IntoErrorPayload for EventWalError {
    fn into_error_payload(self) -> ErrorPayload {
        let message = self.to_string();
        let retryable = self.transience().is_retryable();
        let code = self.code();
        match self {
            EventWalError::RecordTooLarge {
                max_bytes,
                got_bytes,
            } => ErrorPayload::new(
                ProtocolErrorCode::WalRecordTooLarge.into(),
                message,
                retryable,
            )
            .with_details(error_details::WalRecordTooLargeDetails {
                max_wal_record_bytes: max_bytes as u64,
                estimated_bytes: got_bytes as u64,
            }),
            EventWalError::Symlink { path } => ErrorPayload::new(
                ProtocolErrorCode::PathSymlinkRejected.into(),
                message,
                retryable,
            )
            .with_details(error_details::PathSymlinkRejectedDetails {
                path: path.display().to_string(),
            }),
            _ => ErrorPayload::new(code, message.clone(), retryable)
                .with_details(error_details::WalErrorDetails { message }),
        }
    }
}
