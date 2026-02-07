//! Lightweight WAL index primitives shared with model adapters.

use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::core::{
    ActorId, Applied, ClientRequestId, Durable, EventId, HeadStatus, NamespaceId,
    ReplicaDurabilityRole, ReplicaId, SegmentId, Seq0, Seq1, TxnId, Watermark,
};

pub mod memory_index;

pub use memory_index::{MemoryWalIndex, MemoryWalIndexSnapshot};

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

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WalIndexError {
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
    #[error("client_request_id reuse mismatch for {namespace} {origin} {client_request_id}")]
    ClientRequestIdReuseMismatch {
        namespace: NamespaceId,
        origin: ReplicaId,
        client_request_id: ClientRequestId,
        expected_request_sha256: [u8; 32],
        got_request_sha256: [u8; 32],
    },
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
        applied: Watermark<Applied>,
        durable: Watermark<Durable>,
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
        event_ids: &ClientRequestEventIds,
        created_at_ms: u64,
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SegmentRow {
    Open {
        namespace: NamespaceId,
        segment_id: SegmentId,
        segment_path: PathBuf,
        created_at_ms: u64,
        last_indexed_offset: u64,
    },
    Sealed {
        namespace: NamespaceId,
        segment_id: SegmentId,
        segment_path: PathBuf,
        created_at_ms: u64,
        last_indexed_offset: u64,
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
    pub applied: Watermark<Applied>,
    pub durable: Watermark<Durable>,
}

impl WatermarkRow {
    pub fn applied_seq(&self) -> u64 {
        self.applied.seq().get()
    }

    pub fn durable_seq(&self) -> u64 {
        self.durable.seq().get()
    }

    pub fn applied_head_sha(&self) -> Option<[u8; 32]> {
        match self.applied.head() {
            HeadStatus::Known(sha) => Some(sha),
            HeadStatus::Genesis => None,
        }
    }

    pub fn durable_head_sha(&self) -> Option<[u8; 32]> {
        match self.durable.head() {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IndexDurabilityMode {
    Cache,
    Durable,
}
