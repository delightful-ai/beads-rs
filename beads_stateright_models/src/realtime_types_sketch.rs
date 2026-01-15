//! REALTIME_SPEC_DRAFT v0.5
//! ========================
//!
//! This file is a **types-first** sketch for the v0.5 realtime plan. It encodes
//! the core invariants and boundaries for model-checking without pulling in the
//! production implementation.
//!
//! Key v0.5 shifts captured here:
//! - Hash over raw EventBody bytes; sha/prev live in frame headers.
//! - Unknown-key preservation and self-hashing envelopes are removed.
//! - GC marker events and GitCheckpointed durability are deferred/removed.
//! - Locally-authored EventBody bytes are canonical; receivers may accept non-canonical bytes.

#![allow(dead_code)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::type_complexity)]

use bytes::Bytes;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::num::{NonZeroU32, NonZeroU64};
use thiserror::Error;
use uuid::Uuid;

// =================================================================================================
// 0. Limits (v0.5 defaults)
// =================================================================================================

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_event_batch_events: usize,
    pub max_event_batch_bytes: usize,
    pub max_wal_record_bytes: usize,

    pub max_repl_gap_events: usize,
    pub max_repl_gap_bytes: usize,
    pub repl_gap_timeout_ms: u64,

    pub keepalive_ms: u64,
    pub dead_ms: u64,

    pub wal_segment_max_bytes: usize,
    pub wal_segment_max_age_ms: u64,

    pub wal_group_commit_max_latency_ms: u64,
    pub wal_group_commit_max_events: usize,
    pub wal_group_commit_max_bytes: usize,
    pub wal_sqlite_checkpoint_interval_ms: u64,

    pub max_repl_ingest_queue_bytes: usize,
    pub max_repl_ingest_queue_events: usize,

    pub max_ipc_inflight_mutations: usize,
    pub max_checkpoint_job_queue: usize,
    pub max_broadcast_subscribers: usize,

    pub event_hot_cache_max_bytes: usize,
    pub event_hot_cache_max_events: usize,

    pub max_events_per_origin_per_batch: usize,
    pub max_bytes_per_origin_per_batch: usize,

    pub max_snapshot_bytes: usize,
    pub max_snapshot_entries: usize,
    pub max_snapshot_entry_bytes: usize,
    pub max_concurrent_snapshots: usize,
    pub max_jsonl_line_bytes: usize,
    pub max_jsonl_shard_bytes: usize,

    pub max_cbor_depth: usize,
    pub max_cbor_map_entries: usize,
    pub max_cbor_array_entries: usize,
    pub max_cbor_bytes_string_len: usize,
    pub max_cbor_text_string_len: usize,

    pub max_repl_ingest_bytes_per_sec: usize,
    pub max_background_io_bytes_per_sec: usize,

    pub hlc_max_forward_drift_ms: u64,

    pub max_note_bytes: usize,
    pub max_ops_per_txn: usize,
    pub max_note_appends_per_txn: usize,
    pub max_labels_per_bead: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 16 * 1024 * 1024,
            max_event_batch_events: 10_000,
            max_event_batch_bytes: 10 * 1024 * 1024,
            max_wal_record_bytes: 16 * 1024 * 1024,

            max_repl_gap_events: 50_000,
            max_repl_gap_bytes: 32 * 1024 * 1024,
            repl_gap_timeout_ms: 30_000,

            keepalive_ms: 5_000,
            dead_ms: 30_000,

            wal_segment_max_bytes: 32 * 1024 * 1024,
            wal_segment_max_age_ms: 60_000,

            wal_group_commit_max_latency_ms: 2,
            wal_group_commit_max_events: 64,
            wal_group_commit_max_bytes: 1024 * 1024,
            wal_sqlite_checkpoint_interval_ms: 3_600_000,

            max_repl_ingest_queue_bytes: 32 * 1024 * 1024,
            max_repl_ingest_queue_events: 50_000,

            max_ipc_inflight_mutations: 1024,
            max_checkpoint_job_queue: 8,
            max_broadcast_subscribers: 256,

            event_hot_cache_max_bytes: 64 * 1024 * 1024,
            event_hot_cache_max_events: 200_000,

            max_events_per_origin_per_batch: 1024,
            max_bytes_per_origin_per_batch: 1024 * 1024,

            max_snapshot_bytes: 512 * 1024 * 1024,
            max_snapshot_entries: 200_000,
            max_snapshot_entry_bytes: 64 * 1024 * 1024,
            max_concurrent_snapshots: 1,
            max_jsonl_line_bytes: 4 * 1024 * 1024,
            max_jsonl_shard_bytes: 256 * 1024 * 1024,

            max_cbor_depth: 32,
            max_cbor_map_entries: 10_000,
            max_cbor_array_entries: 10_000,
            max_cbor_bytes_string_len: 16 * 1024 * 1024,
            max_cbor_text_string_len: 16 * 1024 * 1024,

            max_repl_ingest_bytes_per_sec: 64 * 1024 * 1024,
            max_background_io_bytes_per_sec: 128 * 1024 * 1024,

            hlc_max_forward_drift_ms: 600_000,

            max_note_bytes: 64 * 1024,
            max_ops_per_txn: 10_000,
            max_note_appends_per_txn: 1_000,
            max_labels_per_bead: 256,
        }
    }
}

// =================================================================================================
// 1. Identity types
// =================================================================================================

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoreId(pub Uuid);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoreEpoch(pub u64);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicaId(pub Uuid);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnId(pub Uuid);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientRequestId(pub Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreIdentity {
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
}

// =================================================================================================
// 2. Seq0 vs Seq1 (contiguity is sacred)
// =================================================================================================

/// Seq0 is used for “highest contiguous seen” values.
/// Seq0(0) means “nothing yet”.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq0(pub u64);

/// Seq1 is used for actual event origin_seq, which is 1-based.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq1(pub NonZeroU64);

impl Seq0 {
    pub fn next(self) -> Seq1 {
        Seq1(NonZeroU64::new(self.0.saturating_add(1)).unwrap())
    }
}

impl Seq1 {
    pub fn as_u64(self) -> u64 {
        self.0.get()
    }

    pub fn prev(self) -> Option<Seq1> {
        let v = self.as_u64();
        if v <= 1 {
            None
        } else {
            NonZeroU64::new(v - 1).map(Seq1)
        }
    }

    pub fn prev_seq0(self) -> Seq0 {
        Seq0(self.as_u64().saturating_sub(1))
    }
}

// =================================================================================================
// 3. Namespaces
// =================================================================================================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NamespaceId(String);

#[derive(Error, Debug)]
pub enum NamespaceIdError {
    #[error("namespace must match [a-z][a-z0-9_]{{0,31}}, got: {0}")]
    Invalid(String),
}

impl NamespaceId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for NamespaceId {
    type Error = NamespaceIdError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let s = value.to_string();
        let b = s.as_bytes();
        if b.is_empty() || b.len() > 32 {
            return Err(NamespaceIdError::Invalid(s));
        }
        if !b[0].is_ascii_lowercase() {
            return Err(NamespaceIdError::Invalid(s));
        }
        for &c in b.iter().skip(1) {
            let ok = c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_';
            if !ok {
                return Err(NamespaceIdError::Invalid(s));
            }
        }
        Ok(Self(s))
    }
}

// =================================================================================================
// 4. Hashes
// =================================================================================================

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha256(pub [u8; 32]);

pub fn sha256_bytes(data: &[u8]) -> Sha256 {
    use sha2::{Digest, Sha256 as Sha2};
    let mut h = Sha2::new();
    h.update(data);
    let out = h.finalize();
    let mut buf = [0u8; 32];
    buf.copy_from_slice(&out);
    Sha256(buf)
}

// =================================================================================================
// 4.1 Event bytes (canonical vs opaque)
// =================================================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Canonical {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Opaque {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventBytes<S> {
    bytes: Bytes,
    _s: PhantomData<S>,
}

impl<S> EventBytes<S> {
    pub fn as_bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl EventBytes<Canonical> {
    pub fn new(bytes: Bytes) -> Self {
        Self {
            bytes,
            _s: PhantomData,
        }
    }
}

impl EventBytes<Opaque> {
    pub fn new(bytes: Bytes) -> Self {
        Self {
            bytes,
            _s: PhantomData,
        }
    }
}

impl From<EventBytes<Canonical>> for EventBytes<Opaque> {
    fn from(bytes: EventBytes<Canonical>) -> Self {
        EventBytes::<Opaque>::new(bytes.bytes)
    }
}

impl<S> AsRef<[u8]> for EventBytes<S> {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

// =================================================================================================
// 5. Watermarks + head sha
// =================================================================================================

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Applied;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Durable;

#[derive(Error, Debug)]
pub enum WatermarkError {
    #[error("non-contiguous advancement: expected {expected}, got {got}")]
    NonContiguous { expected: u64, got: u64 },
    #[error("head required for seq {seq}")]
    HeadRequired { seq: u64 },
    #[error("seq is zero but head is not Genesis")]
    NonGenesisAtZero,
    #[error("seq is nonzero but head is Genesis")]
    GenesisAtNonZero,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HeadStatus {
    /// Seq0(0) only.
    Genesis,
    /// Known head hash for seq>0.
    Known(Sha256),
    /// Head exists but unknown (e.g., checkpoint import without included_heads).
    Unknown,
}

impl HeadStatus {
    pub fn is_known(&self) -> bool {
        matches!(self, HeadStatus::Known(_))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Watermarks<K> {
    inner: BTreeMap<NamespaceId, BTreeMap<ReplicaId, Watermark<K>>>,
    _k: PhantomData<K>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Watermark<K> {
    seq: Seq0,
    head: HeadStatus,
    _k: PhantomData<K>,
}

impl<K> Clone for Watermark<K> {
    fn clone(&self) -> Self {
        Self {
            seq: self.seq,
            head: self.head.clone(),
            _k: PhantomData,
        }
    }
}

impl<K> Default for Watermark<K> {
    fn default() -> Self {
        Self::zero()
    }
}

impl<K> Watermark<K> {
    pub fn zero() -> Self {
        Self {
            seq: Seq0(0),
            head: HeadStatus::Genesis,
            _k: PhantomData,
        }
    }

    pub fn seq(&self) -> Seq0 {
        self.seq
    }

    pub fn head(&self) -> &HeadStatus {
        &self.head
    }

    pub fn expected_next(&self) -> Seq1 {
        self.seq.next()
    }

    pub fn advance_contiguous(&mut self, seq: Seq1, head: Sha256) -> Result<(), WatermarkError> {
        let cur = self.seq.0;
        let got = seq.as_u64();
        if got != cur + 1 {
            return Err(WatermarkError::NonContiguous {
                expected: cur + 1,
                got,
            });
        }
        self.seq = Seq0(got);
        self.head = HeadStatus::Known(head);
        Ok(())
    }

    pub fn observe_at_least(&mut self, seq: Seq0, head: HeadStatus) -> Result<(), WatermarkError> {
        if seq.0 == 0 {
            if !matches!(head, HeadStatus::Genesis) {
                return Err(WatermarkError::NonGenesisAtZero);
            }
        } else if matches!(head, HeadStatus::Genesis) {
            return Err(WatermarkError::GenesisAtNonZero);
        }

        if seq.0 > self.seq.0 {
            self.seq = seq;
            self.head = head;
            return Ok(());
        }

        if seq.0 == self.seq.0 && self.head == HeadStatus::Unknown && head.is_known() {
            self.head = head;
        }
        Ok(())
    }
}

impl<K> Watermarks<K> {
    pub fn get(&self, ns: &NamespaceId, origin: &ReplicaId) -> Watermark<K> {
        self.inner
            .get(ns)
            .and_then(|m| m.get(origin).cloned())
            .unwrap_or_else(Watermark::zero)
    }

    pub fn expected_next(&self, ns: &NamespaceId, origin: &ReplicaId) -> Seq1 {
        self.get(ns, origin).expected_next()
    }

    pub fn advance_one_contiguous(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        seq: Seq1,
        head: Sha256,
    ) -> Result<(), WatermarkError> {
        let entry = self
            .inner
            .entry(ns.clone())
            .or_default()
            .entry(*origin)
            .or_insert_with(Watermark::zero);
        entry.advance_contiguous(seq, head)
    }

    pub fn observe_at_least(
        &mut self,
        ns: NamespaceId,
        origin: ReplicaId,
        seq: Seq0,
        head: HeadStatus,
    ) -> Result<(), WatermarkError> {
        let entry = self
            .inner
            .entry(ns)
            .or_default()
            .entry(origin)
            .or_insert_with(Watermark::zero);
        entry.observe_at_least(seq, head)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AppliedDurableWatermarks {
    pub applied: Watermarks<Applied>,
    pub durable: Watermarks<Durable>,
}

pub type WatermarkHeads = BTreeMap<NamespaceId, BTreeMap<ReplicaId, Sha256>>;

// =================================================================================================
// 6. Durability receipts (v0.5)
// =================================================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurabilityClass {
    LocalFsync,
    ReplicatedFsync { k: NonZeroU32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFsyncProof {
    pub at_ms: u64,
    pub durable_seq: Watermarks<Durable>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicatedProof {
    pub k: NonZeroU32,
    pub acked_by: Vec<ReplicaId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurabilityProofV1 {
    pub local_fsync: LocalFsyncProof,
    pub replicated: Option<ReplicatedProof>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurabilityOutcome {
    Achieved {
        requested: DurabilityClass,
        achieved: DurabilityClass,
    },
    Pending {
        requested: DurabilityClass,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurabilityReceipt {
    pub store: StoreIdentity,
    pub txn_id: TxnId,
    pub event_ids: Vec<EventId>,
    pub durability_proof: DurabilityProofV1,
    pub outcome: DurabilityOutcome,
    pub min_seen: Watermarks<Applied>,
}

// =================================================================================================
// 7. Event identity + body (v0.5)
// =================================================================================================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId {
    pub origin_replica_id: ReplicaId,
    pub namespace: NamespaceId,
    pub origin_seq: Seq1,
}

#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActorId(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StampV1 {
    pub wall_ms: u64,
    pub counter: u32,
    pub by: ActorId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HlcMax {
    pub actor_id: ActorId,
    pub physical_ms: u64,
    pub logical: u32,
}

pub type BeadId = String;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NoteId(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireNoteV1 {
    pub id: NoteId,
    pub content: String,
    pub author: ActorId,
    pub at: StampV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum NotesPatch {
    #[default]
    Omitted,
    AtLeast(Vec<WireNoteV1>),
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct WireBeadPatch {
    pub id: BeadId,
    pub title: Option<String>,
    pub labels: Option<Vec<String>>,
    pub notes: NotesPatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoteAppendV1 {
    pub bead_id: BeadId,
    pub note: WireNoteV1,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TxnOpKey {
    BeadUpsert { id: BeadId },
    NoteAppend { bead_id: BeadId, note_id: NoteId },
}

impl TxnOpKey {
    pub fn kind(&self) -> &'static str {
        match self {
            TxnOpKey::BeadUpsert { .. } => "bead_upsert",
            TxnOpKey::NoteAppend { .. } => "note_append",
        }
    }

    pub fn describe(&self) -> String {
        match self {
            TxnOpKey::BeadUpsert { id } => format!("bead_upsert:{id}"),
            TxnOpKey::NoteAppend { bead_id, note_id } => {
                format!("note_append:{bead_id}:{note_id:?}")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxnOpV1 {
    BeadUpsert(WireBeadPatch),
    NoteAppend(NoteAppendV1),
}

impl TxnOpV1 {
    pub fn key(&self) -> TxnOpKey {
        match self {
            TxnOpV1::BeadUpsert(up) => TxnOpKey::BeadUpsert { id: up.id.clone() },
            TxnOpV1::NoteAppend(na) => TxnOpKey::NoteAppend {
                bead_id: na.bead_id.clone(),
                note_id: na.note.id.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TxnDeltaV1 {
    ops: BTreeMap<TxnOpKey, TxnOpV1>,
}

impl TxnDeltaV1 {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, op: TxnOpV1) -> Result<(), ValidationError> {
        let key = op.key();
        if self.ops.contains_key(&key) {
            return Err(ValidationError::DuplicateOp {
                kind: key.kind(),
                key: key.describe(),
            });
        }
        self.ops.insert(key, op);
        Ok(())
    }

    pub fn from_parts(
        bead_upserts: Vec<WireBeadPatch>,
        note_appends: Vec<NoteAppendV1>,
    ) -> Result<Self, ValidationError> {
        let mut delta = TxnDeltaV1::new();
        for up in bead_upserts {
            delta.insert(TxnOpV1::BeadUpsert(up))?;
        }
        for na in note_appends {
            delta.insert(TxnOpV1::NoteAppend(na))?;
        }
        Ok(delta)
    }

    pub fn total_ops(&self) -> usize {
        self.ops.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &TxnOpV1> {
        self.ops.values()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnV1 {
    pub hlc_max: HlcMax,
    pub delta: TxnDeltaV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventKindV1 {
    TxnV1(TxnV1),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventBody {
    pub envelope_v: u32,
    pub store: StoreIdentity,
    pub namespace: NamespaceId,
    pub origin_replica_id: ReplicaId,
    pub origin_seq: Seq1,
    pub event_time_ms: u64,
    pub txn_id: TxnId,
    pub client_request_id: Option<ClientRequestId>,
    pub kind: EventKindV1,
}

// =================================================================================================
// 8. Event frames + verification (v0.5)
// =================================================================================================

#[derive(Clone, Debug)]
pub struct EventFrameV1 {
    pub eid: EventId,
    pub sha256: Sha256,
    pub prev_sha256: Option<Sha256>,
    pub bytes: EventBytes<Opaque>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrevVerified {
    pub prev: Option<Sha256>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrevDeferred {
    pub prev: Sha256,
    pub expected_prev_seq: Seq1,
}

#[derive(Clone, Debug)]
pub struct VerifiedEvent<P> {
    pub body: EventBody,
    pub bytes: EventBytes<Opaque>,
    pub sha256: Sha256,
    pub prev: P,
}

impl<P> VerifiedEvent<P> {
    pub fn seq(&self) -> Seq1 {
        self.body.origin_seq
    }

    pub fn bytes_len(&self) -> usize {
        self.bytes.len()
    }
}

#[derive(Clone, Debug)]
pub enum VerifiedEventAny {
    Contiguous(VerifiedEvent<PrevVerified>),
    Deferred(VerifiedEvent<PrevDeferred>),
}

impl VerifiedEventAny {
    pub fn seq(&self) -> Seq1 {
        match self {
            VerifiedEventAny::Contiguous(ev) => ev.seq(),
            VerifiedEventAny::Deferred(ev) => ev.seq(),
        }
    }

    pub fn bytes_len(&self) -> usize {
        match self {
            VerifiedEventAny::Contiguous(ev) => ev.bytes_len(),
            VerifiedEventAny::Deferred(ev) => ev.bytes_len(),
        }
    }

    pub fn is_deferred(&self) -> bool {
        matches!(self, VerifiedEventAny::Deferred(_))
    }
}

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("decode limit exceeded")]
    DecodeLimit,
    #[error("indefinite-length structures rejected for hashed types")]
    IndefiniteLength,
    #[error("non-canonical encoding (optional enforcement)")]
    NonCanonical,
}

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("txn ops {ops} exceeds max {max}")]
    TooManyOps { ops: usize, max: usize },
    #[error("note content bytes {bytes} exceeds max {max}")]
    NoteTooLarge { bytes: usize, max: usize },
    #[error("labels count {count} exceeds max {max}")]
    TooManyLabels { count: usize, max: usize },
    #[error("duplicate op {kind} for key {key}")]
    DuplicateOp { kind: &'static str, key: String },
}

pub fn validate_event_kind_limits(kind: &EventKindV1, limits: &Limits) -> Result<(), ValidationError> {
    match kind {
        EventKindV1::TxnV1(txn) => {
            let delta = &txn.delta;
            let ops = delta.total_ops();
            if ops > limits.max_ops_per_txn {
                return Err(ValidationError::TooManyOps {
                    ops,
                    max: limits.max_ops_per_txn,
                });
            }
            for op in delta.iter() {
                match op {
                    TxnOpV1::BeadUpsert(up) => {
                        if let Some(labels) = &up.labels {
                            if labels.len() > limits.max_labels_per_bead {
                                return Err(ValidationError::TooManyLabels {
                                    count: labels.len(),
                                    max: limits.max_labels_per_bead,
                                });
                            }
                        }
                        if let NotesPatch::AtLeast(notes) = &up.notes {
                            for n in notes {
                                let bytes = n.content.len();
                                if bytes > limits.max_note_bytes {
                                    return Err(ValidationError::NoteTooLarge {
                                        bytes,
                                        max: limits.max_note_bytes,
                                    });
                                }
                            }
                        }
                    }
                    TxnOpV1::NoteAppend(na) => {
                        let bytes = na.note.content.len();
                        if bytes > limits.max_note_bytes {
                            return Err(ValidationError::NoteTooLarge {
                                bytes,
                                max: limits.max_note_bytes,
                            });
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("wrong_store")]
    WrongStore,
    #[error("frame_mismatch")]
    FrameMismatch,
    #[error("sha_mismatch")]
    ShaMismatch,
    #[error("prev_mismatch")]
    PrevMismatch,
    #[error("validation: {0}")]
    Validation(#[from] ValidationError),
    #[error("decode: {0}")]
    Decode(#[from] DecodeError),
    #[error("equivocation")]
    Equivocation,
}

pub trait EventBodyCodecV1 {
    fn decode_event_body(
        &self,
        bytes: &EventBytes<Opaque>,
        limits: &Limits,
    ) -> Result<EventBody, DecodeError>;
    fn encode_event_body(
        &self,
        body: &EventBody,
    ) -> Result<EventBytes<Canonical>, DecodeError>;
}

pub trait EventShaLookupV1 {
    fn lookup_event_sha(&self, eid: &EventId) -> Result<Option<Sha256>, String>;
}

pub fn verify_event_frame(
    codec: &dyn EventBodyCodecV1,
    lookup: &dyn EventShaLookupV1,
    limits: &Limits,
    expected_store: StoreIdentity,
    expected_prev_head: Option<Sha256>,
    frame: &EventFrameV1,
) -> Result<VerifiedEventAny, ProtocolError> {
    let body = codec.decode_event_body(&frame.bytes, limits)?;

    if body.store != expected_store {
        return Err(ProtocolError::WrongStore);
    }

    if body.origin_replica_id != frame.eid.origin_replica_id
        || body.namespace != frame.eid.namespace
        || body.origin_seq != frame.eid.origin_seq
    {
        return Err(ProtocolError::FrameMismatch);
    }

    let computed = sha256_bytes(frame.bytes.as_ref());
    if computed != frame.sha256 {
        return Err(ProtocolError::ShaMismatch);
    }

    let seq = body.origin_seq.as_u64();
    match (seq, frame.prev_sha256, expected_prev_head) {
        (1, None, _) => {}
        (1, Some(_), _) => return Err(ProtocolError::PrevMismatch),
        (s, None, _) if s > 1 => return Err(ProtocolError::PrevMismatch),
        (s, Some(p), Some(head)) if s > 1 && p == head => {}
        (s, Some(_), Some(_)) if s > 1 => return Err(ProtocolError::PrevMismatch),
        (s, Some(_), None) if s > 1 => {
            // Prev validation is deferred until the predecessor head is known.
        }
        _ => {}
    }

    validate_event_kind_limits(&body.kind, limits)?;

    match lookup.lookup_event_sha(&frame.eid) {
        Ok(None) => {}
        Ok(Some(existing)) if existing == frame.sha256 => {}
        Ok(Some(_)) => return Err(ProtocolError::Equivocation),
        Err(_) => return Err(ProtocolError::Equivocation),
    }

    match (seq, frame.prev_sha256, expected_prev_head) {
        (1, None, _) => Ok(VerifiedEventAny::Contiguous(VerifiedEvent {
            body,
            bytes: frame.bytes.clone(),
            sha256: frame.sha256,
            prev: PrevVerified { prev: None },
        })),
        (s, Some(prev), Some(head)) if s > 1 && prev == head => {
            Ok(VerifiedEventAny::Contiguous(VerifiedEvent {
                body,
                bytes: frame.bytes.clone(),
                sha256: frame.sha256,
                prev: PrevVerified { prev: Some(prev) },
            }))
        }
        (s, Some(prev), None) if s > 1 => {
            let expected_prev_seq = body
                .origin_seq
                .prev()
                .expect("seq>1 must have a predecessor");
            Ok(VerifiedEventAny::Deferred(VerifiedEvent {
                body,
                bytes: frame.bytes.clone(),
                sha256: frame.sha256,
                prev: PrevDeferred {
                    prev,
                    expected_prev_seq,
                },
            }))
        }
        _ => Err(ProtocolError::PrevMismatch),
    }
}

// =================================================================================================
// 9. Gap buffering + contiguity
// =================================================================================================

#[derive(Clone, Debug)]
pub struct GapBuffer {
    pub buffered: BTreeMap<Seq1, VerifiedEventAny>,
    pub buffered_bytes: usize,
    pub started_at_ms: Option<u64>,
    pub max_events: usize,
    pub max_bytes: usize,
    pub timeout_ms: u64,
}

impl GapBuffer {
    pub fn new(limits: &Limits) -> Self {
        Self {
            buffered: BTreeMap::new(),
            buffered_bytes: 0,
            started_at_ms: None,
            max_events: limits.max_repl_gap_events,
            max_bytes: limits.max_repl_gap_bytes,
            timeout_ms: limits.repl_gap_timeout_ms,
        }
    }
}

#[derive(Clone, Debug)]
pub struct OriginStreamState {
    pub ns: NamespaceId,
    pub origin: ReplicaId,
    pub durable: Watermark<Durable>,
    pub gap: GapBuffer,
}

#[derive(Clone, Debug)]
pub enum IngestDecision {
    ForwardContiguousBatch(Vec<VerifiedEvent<PrevVerified>>),
    BufferedNeedWant { want_from: Seq0 },
    DuplicateNoop,
    Reject { code: String },
}

impl OriginStreamState {
    pub fn expected_next(&self) -> Seq1 {
        self.durable.expected_next()
    }

    pub fn ingest_one(&mut self, ev: VerifiedEventAny, now_ms: u64) -> IngestDecision {
        let seq = ev.seq();

        if seq.as_u64() <= self.durable.seq().0 {
            return IngestDecision::DuplicateNoop;
        }

        if ev.is_deferred() {
            return self.buffer_gap(ev, now_ms);
        }

        if seq.as_u64() != self.durable.seq().0 + 1 {
            return self.buffer_gap(ev, now_ms);
        }

        let VerifiedEventAny::Contiguous(ev) = ev else {
            return IngestDecision::Reject {
                code: "prev_unknown".into(),
            };
        };

        let mut batch = vec![ev];
        let mut next = seq.as_u64() + 1;
        while let Some(nz) = NonZeroU64::new(next) {
            let s = Seq1(nz);
            if let Some(ev2) = self.gap.buffered.remove(&s) {
                let bytes_len = ev2.bytes_len();
                self.gap.buffered_bytes = self
                    .gap
                    .buffered_bytes
                    .saturating_sub(bytes_len);
                match ev2 {
                    VerifiedEventAny::Contiguous(ev2) => {
                        batch.push(ev2);
                        next += 1;
                        continue;
                    }
                    other => {
                        // Deferred event: reinsert and stop - prev link not verified yet.
                        self.gap.buffered_bytes += bytes_len;
                        self.gap.buffered.insert(s, other);
                        break;
                    }
                }
            } else {
                break;
            }
        }
        if self.gap.buffered.is_empty() {
            self.gap.started_at_ms = None;
        }
        IngestDecision::ForwardContiguousBatch(batch)
    }

    fn buffer_gap(&mut self, ev: VerifiedEventAny, now_ms: u64) -> IngestDecision {
        if self.gap.started_at_ms.is_none() {
            self.gap.started_at_ms = Some(now_ms);
        } else if let Some(start) = self.gap.started_at_ms {
            if now_ms.saturating_sub(start) > self.gap.timeout_ms {
                return IngestDecision::Reject {
                    code: "gap_timeout".into(),
                };
            }
        }

        if self.gap.buffered.len() >= self.gap.max_events {
            return IngestDecision::Reject {
                code: "gap_buffer_overflow".into(),
            };
        }

        let bytes = ev.bytes_len();
        if self.gap.buffered_bytes + bytes > self.gap.max_bytes {
            return IngestDecision::Reject {
                code: "gap_buffer_bytes_overflow".into(),
            };
        }

        self.gap.buffered_bytes += bytes;
        self.gap.buffered.insert(ev.seq(), ev);

        IngestDecision::BufferedNeedWant {
            want_from: self.durable.seq(),
        }
    }
}

// =================================================================================================
// 10. Handshake version negotiation
// =================================================================================================

#[derive(Clone, Debug)]
pub struct ProtocolRange {
    pub max: u32,
    pub min: u32,
}

pub fn negotiate_version(local: &ProtocolRange, peer_max: u32, peer_min: u32) -> Result<u32, String> {
    let v = local.max.min(peer_max);
    let min_ok = local.min.max(peer_min);
    if v >= min_ok {
        Ok(v)
    } else {
        Err("version_incompatible".into())
    }
}
