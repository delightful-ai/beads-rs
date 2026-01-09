//! REALTIME_SPEC_DRAFT v0.4
//! ========================
//!
//! This file is **literate types-first** Rust: a self-contained, compilable “document of types”
//! that encodes the *important correctness invariants* for the Beads realtime lane.
//!
//! It is intentionally detailed in comments and docstrings, Knuth-style: explain the *why* at the
//! exact point where the type system enforces the *what*.
//!
//! ---------------------------------------------------------------------------------------------
//! Sources of truth
//! ----------------
//! This file is aligned to the plan/spec text you provided in this chat.
//! I reference them by section number as used in your plan draft.
//!
//! - Implementation Plan v0.4 (your message):
//!   - Canonical CBOR + hashing and unknown-key preservation: Plan §0.6, §0.6.1
//!   - Watermarks semantics (applied vs durable): Plan §0.12
//!   - Hash-chain continuity across pruning: Plan §0.12.1
//!   - Replication sessions threaded and coordinator-owned apply: Plan §0.13
//!   - Replication protocol messages and rules: Plan §9
//!   - Bounds: Plan §0.19
//!   - Idempotency and receipts: Plan §0.11, §8.2
//!   - Durability classes and coordinator rules: Plan §10
//!
//! The goal here is not to “implement everything”, but to make invalid states difficult to
//! represent and easy to detect at boundaries.
//!
//! ---------------------------------------------------------------------------------------------
//! Design goals
//! ------------
//! 1) Encode the *trust boundaries* as explicit gate types.
//!    Example: `EventFrameV1` (untrusted wire) must become `VerifiedEventFrameV1`,
//!    then `NonEquivocatingEvent`, before it can affect contiguity progress.
//!
//! 2) Encode “contiguity is sacred” for watermarks.
//!    The plan/spec repeatedly imply that contiguous progression is the correctness primitive.
//!    To avoid accidental “jumping” updates, we separate:
//!    - contiguous advancement (event apply / replay only)
//!    - monotonic-at-least observation (ACK reception / checkpoint seeding)
//!    See Plan §0.12 and Plan §9.4 / §9.5.
//!
//! 3) Provide a best-of-both-worlds replication session model:
//!    - **Realistic**: includes inbound/outbound roles, handshake, sync, live, closing,
//!      and the Plan §0.13 coordinator boundary (session threads do not mutate store state).
//!    - **Model-checkable**: also provides a pure `step()` transition on a serializable enum core.
//!
//! The result: you can use the same logic in (a) your daemon and (b) Stateright models, without
//! the “model diverged from implementation” trap.
//!
//! ---------------------------------------------------------------------------------------------
//! Quick map of “little machines” in this file
//! -------------------------------------------
//! - Identity machine: StoreId / StoreEpoch / ReplicaId gating (Plan §0.1, §2.1)
//! - CBOR+hash machine: canonical preimage vs envelope bytes, unknown-key preservation (Plan §0.6)
//! - Watermark machine: applied vs durable, contiguity vs monotonic observation (Plan §0.12)
//! - Durability receipt machine: idempotency records and durability classes (Plan §0.11, §10)
//! - Equivocation machine: same EventId different sha is fatal (Plan §9.4)
//! - Gap/WANT machine: buffer bounded gaps, WANT missing prefix, never skip gaps (Plan §9.4, §9.8)
//! - Replication session phases: handshake -> syncing -> live -> closing (Plan §9.3 / §9.8)
//! - Coordinator boundary: session emits “ingest request” effects, coordinator returns results (Plan §0.13)
//!
//! ---------------------------------------------------------------------------------------------
//! What is intentionally abstracted
//! -------------------------------
//! The full CRDT bead/deps algebra is not modeled here. That algebra sits behind `ApplyEngineV1`.
//! This is not a shortcut that compromises protocol correctness. It is a seam:
//! - The replication and durability state machines must be correct regardless of bead semantics,
//!   as long as apply is deterministic and enforces invariants like note collision.
//! - You can model-check the protocol with a small state (hash accumulator) first.
//!
//! ---------------------------------------------------------------------------------------------
//! Build dependencies (Plan §1.1)
//! ------------------------------
//! This file uses: uuid, bytes, sha2, hex, thiserror.
//! In the real repo these are in Cargo.toml already per your plan.

#![allow(dead_code)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::type_complexity)]

use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::num::NonZeroU64;
use thiserror::Error;
use uuid::Uuid;

// =================================================================================================
// 0. Normative bounds (Plan §0.19)
// =================================================================================================

/// Central bound bundle. “Defaults are normative” per Plan §0.19.
/// Namespace policies may tighten these; nothing may loosen them.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Max decoded frame size on the wire. Default 16 MiB.
    pub max_frame_bytes: usize,
    /// Max number of events in one EVENTS message. Default 10k.
    pub max_event_batch_events: usize,
    /// Max bytes in one EVENTS message payload. Default 10 MiB.
    pub max_event_batch_bytes: usize,

    /// Max size of one WAL record payload. Must be enforced. Default <= 16 MiB.
    /// Note: this is *not* the same as max_frame_bytes, though defaults match.
    pub max_wal_record_bytes: usize,

    /// Backpressure bounds for buffering gaps (Plan §9.8): model as events + bytes caps.
    pub max_repl_gap_events: usize,
    pub max_repl_gap_bytes: usize,
    pub repl_gap_timeout_ms: u64,

    /// Keepalive/dead connection detection (Plan §0.19).
    pub keepalive_ms: u64,
    pub dead_ms: u64,

    /// WAL segment lifecycle (Plan §0.9).
    pub wal_segment_max_bytes: usize,
    pub wal_segment_max_age_ms: u64,

    /// Group commit bounds (Plan §0.19).
    pub wal_group_commit_max_latency_ms: u64,
    pub wal_group_commit_max_events: usize,
    pub wal_group_commit_max_bytes: usize,

    /// Replica ingest queue bounds (Plan §0.19).
    pub max_repl_ingest_queue_bytes: usize,
    pub max_repl_ingest_queue_events: usize,

    /// IPC and background queues (Plan §0.19).
    pub max_ipc_inflight_mutations: usize,
    pub max_checkpoint_job_queue: usize,
    pub max_broadcast_subscribers: usize,

    /// Hot cache bounds (Plan §0.19).
    pub event_hot_cache_max_bytes: usize,
    pub event_hot_cache_max_events: usize,

    /// Per-origin batching fairness (Plan §9.8).
    pub max_events_per_origin_per_batch: usize,
    pub max_bytes_per_origin_per_batch: usize,

    /// Snapshot bounds (Plan §0.19, §13.9.1).
    pub max_snapshot_bytes: usize,
    pub max_snapshot_entries: usize,
    pub max_snapshot_entry_bytes: usize,
    pub max_concurrent_snapshots: usize,
    pub max_jsonl_line_bytes: usize,
    pub max_jsonl_shard_bytes: usize,

    /// CBOR decode limits (Plan §0.19).
    pub max_cbor_depth: usize,
    pub max_cbor_map_entries: usize,
    pub max_cbor_array_entries: usize,
    pub max_cbor_bytes_string_len: usize,
    pub max_cbor_text_string_len: usize,

    /// Rate limits (Plan §0.19).
    pub max_repl_ingest_bytes_per_sec: usize,
    pub max_background_io_bytes_per_sec: usize,

    /// HLC safety bound (Plan §0.19).
    pub hlc_max_forward_drift_ms: u64,

    /// Content-level bounds (Plan §0.19)
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
            wal_group_commit_max_bytes: 1 * 1024 * 1024,

            max_repl_ingest_queue_bytes: 32 * 1024 * 1024,
            max_repl_ingest_queue_events: 50_000,

            max_ipc_inflight_mutations: 1024,
            max_checkpoint_job_queue: 8,
            max_broadcast_subscribers: 256,

            event_hot_cache_max_bytes: 64 * 1024 * 1024,
            event_hot_cache_max_events: 200_000,

            max_events_per_origin_per_batch: 1024,
            max_bytes_per_origin_per_batch: 1 * 1024 * 1024,

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
// 1. Core identity types (Plan §2.1)
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

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(pub Uuid);

/// Digest of a canonicalized client request (Plan §0.11 idempotency safety).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestDigest(pub [u8; 32]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreIdentity {
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreMeta {
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
    pub replica_id: ReplicaId,
    pub store_format_version: u32,
    pub wal_format_version: u32,
    pub checkpoint_format_version: u32,
    pub replication_protocol_version: u32,
    pub index_schema_version: u32,
    pub created_at_ms: u64,
}

// =================================================================================================
// 2. Seq0 vs Seq1 (contiguity is sacred) (Plan §0.12, §9.4; Spec contiguity rules)
// =================================================================================================

/// Seq0 is used for “highest contiguous seen” values.
/// Seq0(0) means “nothing yet”.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq0(pub u64);

/// Seq1 is used for actual event origin_seq, which is 1-based (Plan §2.1 EventId).
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
}

// =================================================================================================
// 3. Namespaces (Plan §2.2)
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
        if !(b'a'..=b'z').contains(&b[0]) {
            return Err(NamespaceIdError::Invalid(s));
        }
        for &c in b.iter().skip(1) {
            let ok = (b'a'..=b'z').contains(&c) || (b'0'..=b'9').contains(&c) || c == b'_';
            if !ok {
                return Err(NamespaceIdError::Invalid(s));
            }
        }
        Ok(Self(s))
    }
}

// =================================================================================================
// 4. Hashes and canonical bytes (Plan §0.6, §0.6.1)
// =================================================================================================

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha256(pub [u8; 32]);

impl Sha256 {
    pub fn to_hex_lower(&self) -> String {
        hex::encode(self.0)
    }
}

/// Canonical preimage bytes: envelope encoded with sha256 and prev_sha256 omitted.
/// sha256 MUST be computed over these bytes (Plan §0.6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalPreimageBytes(pub Bytes);

/// Canonical envelope bytes: envelope encoded including sha256 and prev_sha256.
/// WAL storage and wire transport MUST use these bytes (Plan §0.6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalEnvelopeBytes(pub Bytes);

pub fn sha256_bytes(data: &[u8]) -> Sha256 {
    use sha2::{Digest, Sha256 as Sha2};
    let mut h = Sha2::new();
    h.update(data);
    let out = h.finalize();
    let mut buf = [0u8; 32];
    buf.copy_from_slice(&out);
    Sha256(buf)
}

/// Unknown key preservation is mandatory for forward compatible hashing (Plan §0.6.1).
/// The invariant: decode must be lossless for unknown map entries, and re-encoding must
/// include unknown entries byte-for-byte, sorted with known keys by canonical CBOR ordering.
///
/// This file models that requirement as an opaque container of canonical key+value bytes.
/// The real codec must ensure:
/// - No duplicate keys (including known vs unknown) (Plan §0.6.1)
/// - Unknown entries are preserved exactly as received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownLosslessEntries {
    entries: Vec<(Bytes, Bytes)>,
}

impl UnknownLosslessEntries {
    pub fn empty_for_local() -> Self {
        Self { entries: vec![] }
    }
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &Bytes)> {
        self.entries.iter().map(|(k, v)| (k, v))
    }
}

/// Canonical CBOR key ordering comparator for already-encoded canonical key bytes.
/// RFC 8949: sort by encoded length then lexicographic bytes.
pub fn cbor_key_order(a_key: &[u8], b_key: &[u8]) -> std::cmp::Ordering {
    a_key.len().cmp(&b_key.len()).then_with(|| a_key.cmp(b_key))
}

// =================================================================================================
// 5. Capability tokens for watermark mutation (prevents silent “jumping”)
// =================================================================================================
//
// Why this matters:
// - The plan/spec allows *monotonic observation* of peer progress via ACK and checkpoints.
// - But for local correctness, applied and durable must advance contiguously while processing events.
// - A common bug is accidentally using “set max” logic in the apply path.
// - So we require a capability token for each kind of update.
//
// This is the smallest, highest-leverage “types make bugs harder” technique in this file.

mod caps {
    #[derive(Clone, Copy, Debug)]
    pub struct FromEventApply(pub(super) ());
    #[derive(Clone, Copy, Debug)]
    pub struct FromWalReplay(pub(super) ());
    #[derive(Clone, Copy, Debug)]
    pub struct FromAck(pub(super) ());
    #[derive(Clone, Copy, Debug)]
    pub struct FromCheckpoint(pub(super) ());

    pub(super) fn from_event_apply() -> FromEventApply {
        FromEventApply(())
    }
    pub(super) fn from_wal_replay() -> FromWalReplay {
        FromWalReplay(())
    }
    pub(super) fn from_ack() -> FromAck {
        FromAck(())
    }
    pub(super) fn from_checkpoint() -> FromCheckpoint {
        FromCheckpoint(())
    }
}

// Expose test-only constructors for model-check harnesses.
// In real code you would gate this behind cfg(test) or a feature.
#[cfg(any(test, feature = "model_testing"))]
pub mod testing_caps {
    pub fn from_event_apply() -> super::caps::FromEventApply {
        super::caps::from_event_apply()
    }
    pub fn from_wal_replay() -> super::caps::FromWalReplay {
        super::caps::from_wal_replay()
    }
    pub fn from_ack() -> super::caps::FromAck {
        super::caps::from_ack()
    }
    pub fn from_checkpoint() -> super::caps::FromCheckpoint {
        super::caps::from_checkpoint()
    }
}

// =================================================================================================
// 6. Watermarks: applied vs durable (Plan §0.12)
// =================================================================================================

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Applied;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Durable;

#[derive(Error, Debug)]
pub enum WatermarkError {
    #[error("non-contiguous advancement: expected {expected}, got {got}")]
    NonContiguous { expected: u64, got: u64 },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Watermarks<K> {
    inner: BTreeMap<NamespaceId, BTreeMap<ReplicaId, Seq0>>,
    _k: PhantomData<K>,
}

impl<K> Watermarks<K> {
    pub fn get(&self, ns: &NamespaceId, origin: &ReplicaId) -> Seq0 {
        self.inner
            .get(ns)
            .and_then(|m| m.get(origin).copied())
            .unwrap_or(Seq0(0))
    }

    pub fn expected_next(&self, ns: &NamespaceId, origin: &ReplicaId) -> Seq1 {
        self.get(ns, origin).next()
    }

    /// Contiguous-only advancement (event apply path).
    /// This is used when processing an event stream where skipping gaps is forbidden.
    pub fn advance_one_contiguous(
        &mut self,
        _cap: caps::FromEventApply,
        ns: &NamespaceId,
        origin: &ReplicaId,
        seq: Seq1,
    ) -> Result<(), WatermarkError> {
        let cur = self.get(ns, origin).0;
        let got = seq.as_u64();
        if got != cur + 1 {
            return Err(WatermarkError::NonContiguous {
                expected: cur + 1,
                got,
            });
        }
        self.inner.entry(ns.clone()).or_default().insert(*origin, Seq0(got));
        Ok(())
    }

    /// Contiguous-only advancement (WAL replay path).
    /// Replay must also preserve contiguity relative to the baseline watermark (Plan §7.3 step 8).
    pub fn advance_one_replay(
        &mut self,
        _cap: caps::FromWalReplay,
        ns: &NamespaceId,
        origin: &ReplicaId,
        seq: Seq1,
    ) -> Result<(), WatermarkError> {
        self.advance_one_contiguous(caps::from_event_apply(), ns, origin, seq)
    }

    /// Monotonic observation: used for ACK reception (Plan §9.5) and other “claims”.
    /// This can “jump” because it is not used as the local contiguity truth.
    pub fn observe_at_least(&mut self, _cap: caps::FromAck, ns: NamespaceId, origin: ReplicaId, seq: Seq0) {
        let m = self.inner.entry(ns).or_default();
        let cur = m.get(&origin).copied().unwrap_or(Seq0(0));
        if seq.0 > cur.0 {
            m.insert(origin, seq);
        }
    }

    /// Seed from checkpoint meta.included (Plan §13.9): monotonic-at-least import.
    pub fn seed_at_least(&mut self, _cap: caps::FromCheckpoint, ns: NamespaceId, origin: ReplicaId, seq: Seq0) {
        self.observe_at_least(caps::from_ack(), ns, origin, seq)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AppliedDurableWatermarks {
    pub applied: Watermarks<Applied>,
    pub durable: Watermarks<Durable>,
}

/// Head sha map keyed by (namespace, origin). Entries may be omitted for seq=0 (Plan §0.12.1).
pub type WatermarkHeads = BTreeMap<NamespaceId, BTreeMap<ReplicaId, Sha256>>;

// =================================================================================================
// 6.1 Durability classes and receipts (Plan §10, §0.11)
// =================================================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurabilityClass {
    LocalFsync,
    ReplicatedFsync(u8),
    GitCheckpointed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurabilityReceipt {
    pub txn_id: TxnId,
    pub event_ids: Vec<EventId>,
    pub requested: DurabilityClass,
    pub achieved: DurabilityClass,
    /// Applied watermark snapshot at commit time (Plan §0.12).
    pub min_seen: Watermarks<Applied>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReceiptState {
    Pending,
    Committed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientRequestRecord {
    pub namespace: NamespaceId,
    pub origin_replica_id: ReplicaId,
    pub client_request_id: ClientRequestId,
    pub request_digest: RequestDigest,
    pub state: ReceiptState,
    pub receipt: Option<DurabilityReceipt>,
}

// =================================================================================================
// 7. Event identity and prev link (Plan §2.1 EventId; Plan §0.12.1 continuity)
// =================================================================================================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId {
    pub origin_replica_id: ReplicaId,
    pub namespace: NamespaceId,
    pub origin_seq: Seq1,
}

/// Prev link rule (Plan §0.12.1):
/// - seq==1 has no prev (genesis)
/// - seq>1 must have prev_sha256, and it must equal the head sha for (ns, origin)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrevLink {
    Genesis,
    Link(Sha256),
}

// =================================================================================================
// 8. Core event envelope types (plan-aligned, but payload abstractable)
// =================================================================================================
//
// The plan locks the “TxnV1” envelope with lists of ops, plus NamespaceGcMarker.
// For protocol modeling, we do not need the full bead schema; but we *do* need:
// - limits enforcement (Plan §0.19)
// - note collision rule (Plan §4.2)
// - intra-namespace constraint
//
// Here we keep a minimal schema that preserves the invariants you called out.

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

/// Notes rule (Plan §0.7):
/// - bead_upsert SHOULD omit notes
/// - if present: union semantics, never truncation
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotesPatch {
    Omitted,
    AtLeast(Vec<WireNoteV1>),
}
impl Default for NotesPatch {
    fn default() -> Self {
        NotesPatch::Omitted
    }
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

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TxnDeltaV1 {
    pub bead_upserts: Vec<WireBeadPatch>,
    pub note_appends: Vec<NoteAppendV1>,
}

impl TxnDeltaV1 {
    pub fn total_ops(&self) -> usize {
        self.bead_upserts.len() + self.note_appends.len()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceGcMarkerV1 {
    pub cutoff_ms: u64,
    pub gc_authority_replica_id: ReplicaId,
    pub enforce_floor: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventKindV1 {
    TxnV1(TxnDeltaV1),
    NamespaceGcMarkerV1(NamespaceGcMarkerV1),
}

// =================================================================================================
// 8.1 GC floor enforcement helpers (Plan §2.4, §9.4; Model #5)
// =================================================================================================
//
// These helpers model the floor rule: events with event_time_ms <= gc_floor_ms[ns]
// are ignored as state mutations but still advance contiguity/ACK elsewhere.

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AppliedEvent {
    pub id: EventId,
    pub event_time_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NamespaceApplyState {
    pub gc_floor_ms: BTreeMap<NamespaceId, u64>,
    pub applied: BTreeSet<AppliedEvent>,
}

impl NamespaceApplyState {
    pub fn gc_floor_ms(&self, ns: &NamespaceId) -> u64 {
        self.gc_floor_ms.get(ns).copied().unwrap_or(0)
    }

    pub fn apply_event_with_floor(&mut self, event: AppliedEvent) -> bool {
        let floor = self.gc_floor_ms(&event.id.namespace);
        if should_ignore_event_due_to_floor(event.event_time_ms, floor) {
            return false;
        }
        self.applied.insert(event);
        true
    }

    pub fn apply_gc_marker(&mut self, ns: &NamespaceId, marker: &NamespaceGcMarkerV1) {
        if !marker.enforce_floor {
            return;
        }
        let current = self.gc_floor_ms(ns);
        if marker.cutoff_ms <= current {
            return;
        }
        self.gc_floor_ms.insert(ns.clone(), marker.cutoff_ms);
        self.applied
            .retain(|event| &event.id.namespace != ns || event.event_time_ms > marker.cutoff_ms);
    }
}

pub fn should_ignore_event_due_to_floor(event_time_ms: u64, gc_floor_ms: u64) -> bool {
    event_time_ms <= gc_floor_ms
}

/// Envelope common fields (Plan §2.4)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventEnvelopeCommon {
    pub envelope_v: u32,
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
    pub namespace: NamespaceId,
    pub origin_replica_id: ReplicaId,
    pub origin_seq: Seq1,
    pub event_time_ms: u64,
    pub txn_id: TxnId,
    pub client_request_id: Option<ClientRequestId>,
    pub hlc_max: Option<HlcMax>,
    pub kind: EventKindV1,
}

/// Lossless decoded representation (Plan §0.6.1):
/// - preserves unknown CBOR map entries so canonical preimage can be reconstructed
#[derive(Clone, Debug)]
pub struct EventLosslessEnvelope {
    pub common: EventEnvelopeCommon,
    pub declared_sha256: Sha256,
    pub declared_prev: PrevLink,
    pub unknown: UnknownLosslessEntries,
}

// =================================================================================================
// 9. Codec seam (Plan §0.6, §0.6.1)
// =================================================================================================

#[derive(Error, Debug)]
pub enum CborModelError {
    #[error("decode limit exceeded")]
    DecodeLimit,
    #[error("indefinite-length structures rejected for hashed types (Plan §0.19)")]
    IndefiniteLength,
    #[error("duplicate key rejected (Plan §0.6.1)")]
    DuplicateKey,
    #[error("sha mismatch")]
    ShaMismatch,
    #[error("prev mismatch")]
    PrevMismatch,
    #[error("wrong store identity")]
    WrongStore,
    #[error("frame mismatch")]
    FrameMismatch,
}

/// Codec contract:
/// - decode must be lossless for unknown keys (Plan §0.6.1)
/// - encode_canonical_preimage omits sha + prev for hashing (Plan §0.6)
/// - encode_canonical_envelope includes sha + prev for WAL/wire (Plan §0.6)
pub trait EventEnvelopeCodecV1 {
    fn decode_lossless_envelope(
        &self,
        bytes: CanonicalEnvelopeBytes,
        limits: &Limits,
    ) -> Result<EventLosslessEnvelope, CborModelError>;

    fn encode_canonical_preimage(&self, env: &EventLosslessEnvelope)
        -> Result<CanonicalPreimageBytes, CborModelError>;

    fn encode_canonical_envelope(&self, env: &EventLosslessEnvelope)
        -> Result<CanonicalEnvelopeBytes, CborModelError>;
}

// =================================================================================================
// 10. Validation gates (Plan §0.19 limits)
// =================================================================================================

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("txn ops {ops} exceeds max {max}")]
    TooManyOps { ops: usize, max: usize },
    #[error("note content bytes {bytes} exceeds max {max}")]
    NoteTooLarge { bytes: usize, max: usize },
    #[error("labels count {count} exceeds max {max}")]
    TooManyLabels { count: usize, max: usize },
    #[error("batch exceeds max events {max}")]
    BatchTooManyEvents { max: usize },
    #[error("batch exceeds max bytes {max}")]
    BatchTooManyBytes { max: usize },
    #[error("batch exceeds max events per origin {max}")]
    BatchTooManyEventsPerOrigin { max: usize },
    #[error("batch exceeds max bytes per origin {max}")]
    BatchTooManyBytesPerOrigin { max: usize },
    #[error("frame exceeds negotiated max_frame_bytes {max}")]
    FrameTooLarge { max: usize },
}

pub fn validate_event_kind_limits(kind: &EventKindV1, limits: &Limits) -> Result<(), ValidationError> {
    match kind {
        EventKindV1::NamespaceGcMarkerV1(_) => Ok(()),
        EventKindV1::TxnV1(delta) => {
            let ops = delta.total_ops();
            if ops > limits.max_ops_per_txn {
                return Err(ValidationError::TooManyOps { ops, max: limits.max_ops_per_txn });
            }
            for up in &delta.bead_upserts {
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
                        let bytes = n.content.as_bytes().len();
                        if bytes > limits.max_note_bytes {
                            return Err(ValidationError::NoteTooLarge { bytes, max: limits.max_note_bytes });
                        }
                    }
                }
            }
            for na in &delta.note_appends {
                let bytes = na.note.content.as_bytes().len();
                if bytes > limits.max_note_bytes {
                    return Err(ValidationError::NoteTooLarge { bytes, max: limits.max_note_bytes });
                }
            }
            Ok(())
        }
    }
}

// =================================================================================================
// 11. Verification pipeline: untrusted -> verified -> within-limits -> non-equivocating
// =================================================================================================

#[derive(Clone, Debug)]
pub struct VerifiedEvent {
    pub env: EventLosslessEnvelope,
    pub envelope_bytes: CanonicalEnvelopeBytes,
    pub preimage_bytes: CanonicalPreimageBytes,
}

#[derive(Clone, Debug)]
pub struct VerifiedEventWithinLimits(pub VerifiedEvent);

#[derive(Clone, Debug)]
pub struct NonEquivocatingEvent {
    pub ev: VerifiedEventWithinLimits,
    pub is_duplicate: bool,
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("wrong_store")]
    WrongStore,
    #[error("sha_mismatch")]
    ShaMismatch,
    #[error("prev_mismatch")]
    PrevMismatch,
    #[error("frame_mismatch")]
    FrameMismatch,
    #[error("validation: {0}")]
    Validation(#[from] ValidationError),
    #[error("cbor: {0}")]
    Cbor(#[from] CborModelError),
    #[error("equivocation")]
    Equivocation,
}

/// Verified event step:
/// - store_id/store_epoch match (Plan §9.4)
/// - sha256 matches canonical preimage (Plan §0.6)
/// - prev continuity satisfied if required (Plan §0.12.1)
pub fn verify_event(
    codec: &dyn EventEnvelopeCodecV1,
    expected_store: StoreIdentity,
    env: EventLosslessEnvelope,
    envelope_bytes: CanonicalEnvelopeBytes,
    expected_prev_head: Option<Sha256>,
) -> Result<VerifiedEvent, ProtocolError> {
    if env.common.store_id != expected_store.store_id || env.common.store_epoch != expected_store.store_epoch {
        return Err(ProtocolError::WrongStore);
    }

    let preimage = codec.encode_canonical_preimage(&env)?;
    let computed = sha256_bytes(&preimage.0);
    if computed != env.declared_sha256 {
        return Err(ProtocolError::ShaMismatch);
    }

    let seq = env.common.origin_seq.as_u64();
    match (seq, env.declared_prev, expected_prev_head) {
        (1, PrevLink::Genesis, _) => {}
        (1, _, _) => return Err(ProtocolError::PrevMismatch),
        (s, PrevLink::Link(p), Some(head)) if s > 1 && p == head => {}
        (s, PrevLink::Link(_), None) if s > 1 => {
            // If no head is known, the caller is expected to seed it from checkpoint included_heads
            // (Plan §0.12.1). Without it, continuity cannot be validated.
            return Err(ProtocolError::PrevMismatch);
        }
        (s, _, _) if s > 1 => return Err(ProtocolError::PrevMismatch),
        _ => {}
    }

    Ok(VerifiedEvent {
        env,
        envelope_bytes,
        preimage_bytes: preimage,
    })
}

/// Limits step is separate so models can intentionally fuzz “too big” payloads
/// and verify they are rejected before any state change (Plan §0.19).
pub fn enforce_limits(verified: VerifiedEvent, limits: &Limits) -> Result<VerifiedEventWithinLimits, ProtocolError> {
    validate_event_kind_limits(&verified.env.common.kind, limits)?;
    Ok(VerifiedEventWithinLimits(verified))
}

/// Equivocation lookup seam (Plan §9.4):
/// If EventId already exists but with different sha256, treat as protocol corruption.
pub trait EventShaLookupV1 {
    fn lookup_event_sha(&self, eid: &EventId) -> Result<Option<Sha256>, String>;
}

pub fn enforce_no_equivocation(
    lookup: &dyn EventShaLookupV1,
    ev: VerifiedEventWithinLimits,
) -> Result<NonEquivocatingEvent, ProtocolError> {
    let eid = EventId {
        origin_replica_id: ev.0.env.common.origin_replica_id,
        namespace: ev.0.env.common.namespace.clone(),
        origin_seq: ev.0.env.common.origin_seq,
    };
    let incoming = ev.0.env.declared_sha256;
    match lookup.lookup_event_sha(&eid) {
        Ok(None) => Ok(NonEquivocatingEvent { ev, is_duplicate: false }),
        Ok(Some(existing)) if existing == incoming => Ok(NonEquivocatingEvent { ev, is_duplicate: true }),
        Ok(Some(_)) => Err(ProtocolError::Equivocation),
        Err(_) => Err(ProtocolError::Equivocation),
    }
}

// =================================================================================================
// 12. Wire replication messages (Plan §9)
// =================================================================================================

#[derive(Clone, Debug)]
pub struct HelloV1 {
    pub protocol_version: u32,
    pub min_protocol_version: u32,
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
    pub sender_replica_id: ReplicaId,
    pub max_frame_bytes: u32,
    pub requested_namespaces: Vec<NamespaceId>,
    pub offered_namespaces: Vec<NamespaceId>,
    pub seen_durable: Watermarks<Durable>,
    pub seen_durable_heads: Option<WatermarkHeads>,
    pub seen_applied: Option<Watermarks<Applied>>,
    pub seen_applied_heads: Option<WatermarkHeads>,
}

#[derive(Clone, Debug)]
pub struct WelcomeV1 {
    pub protocol_version: u32,
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
    pub receiver_replica_id: ReplicaId,
    pub accepted_namespaces: Vec<NamespaceId>,
    pub receiver_seen_durable: Watermarks<Durable>,
    pub receiver_seen_durable_heads: Option<WatermarkHeads>,
    pub receiver_seen_applied: Option<Watermarks<Applied>>,
    pub receiver_seen_applied_heads: Option<WatermarkHeads>,
    pub max_frame_bytes: u32,
}

#[derive(Clone, Debug)]
pub struct AckV1 {
    pub durable: Watermarks<Durable>,
    pub durable_heads: Option<WatermarkHeads>,
    pub applied: Option<Watermarks<Applied>>,
    pub applied_heads: Option<WatermarkHeads>,
}

#[derive(Clone, Debug)]
pub struct WantV1 {
    /// For each (ns, origin), want means “send seq > this”.
    pub want: Watermarks<Durable>,
}

/// Untrusted wire event frame (Plan §9.4).
#[derive(Clone, Debug)]
pub struct EventFrameV1 {
    pub eid: EventId,
    pub sha256: Sha256,
    pub prev_sha256: Option<Sha256>,
    pub bytes: CanonicalEnvelopeBytes,
}

/// Verified wire event frame:
/// - eid matches decoded envelope identity
/// - sha/prev match decoded envelope declarations
/// - sha verified via canonical preimage
#[derive(Clone, Debug)]
pub struct VerifiedEventFrameV1 {
    pub eid: EventId,
    pub verified: VerifiedEventWithinLimits,
}

/// A single wire message type for session modeling.
#[derive(Clone, Debug)]
pub enum ReplMsgV1 {
    Hello(HelloV1),
    Welcome(WelcomeV1),
    Events(Vec<EventFrameV1>),
    Ack(AckV1),
    Want(WantV1),
    Error { code: String },
    Ping { nonce: u64 },
    Pong { nonce: u64 },
}

// =================================================================================================
// 13. EVENTS batch validation (Plan §0.19; Plan §9.4 ordering rule)
// =================================================================================================

pub fn validate_events_batch(frames: &[EventFrameV1], limits: &Limits, negotiated_max_frame_bytes: usize)
    -> Result<(), ValidationError>
{
    if frames.len() > limits.max_event_batch_events {
        return Err(ValidationError::BatchTooManyEvents { max: limits.max_event_batch_events });
    }

    let mut total = 0usize;
    let mut last_seq: BTreeMap<(NamespaceId, ReplicaId), u64> = BTreeMap::new();
    let mut per_origin_events: BTreeMap<(NamespaceId, ReplicaId), usize> = BTreeMap::new();
    let mut per_origin_bytes: BTreeMap<(NamespaceId, ReplicaId), usize> = BTreeMap::new();

    for f in frames {
        let len = f.bytes.0.len();
        if len > negotiated_max_frame_bytes {
            return Err(ValidationError::FrameTooLarge { max: negotiated_max_frame_bytes });
        }
        if len > limits.max_wal_record_bytes {
            // A record too large is invalid regardless of negotiated frame limit.
            return Err(ValidationError::FrameTooLarge { max: limits.max_wal_record_bytes });
        }
        total = total.saturating_add(len);
        if total > limits.max_event_batch_bytes {
            return Err(ValidationError::BatchTooManyBytes { max: limits.max_event_batch_bytes });
        }

        // Sender rule: increasing origin_seq per (ns, origin) (Plan §9.4).
        let key = (f.eid.namespace.clone(), f.eid.origin_replica_id);
        let s = f.eid.origin_seq.as_u64();
        if let Some(prev) = last_seq.get(&key) {
            if s <= *prev {
                // Treat as invalid batch; session may ERROR and close.
                return Err(ValidationError::BatchTooManyEvents { max: limits.max_event_batch_events });
            }
        }
        last_seq.insert(key.clone(), s);

        let events = per_origin_events.entry(key.clone()).or_insert(0);
        *events += 1;
        if *events > limits.max_events_per_origin_per_batch {
            return Err(ValidationError::BatchTooManyEventsPerOrigin {
                max: limits.max_events_per_origin_per_batch,
            });
        }

        let bytes = per_origin_bytes.entry(key).or_insert(0);
        *bytes = bytes.saturating_add(len);
        if *bytes > limits.max_bytes_per_origin_per_batch {
            return Err(ValidationError::BatchTooManyBytesPerOrigin {
                max: limits.max_bytes_per_origin_per_batch,
            });
        }
    }

    Ok(())
}

/// Verify a single event frame, returning a verified gate type.
/// This function is intentionally “fussy”: any mismatch becomes a hard error,
/// so invalid states do not silently pass through (your key ask).
pub fn verify_event_frame(
    codec: &dyn EventEnvelopeCodecV1,
    lookup: &dyn EventShaLookupV1,
    limits: &Limits,
    expected_store: StoreIdentity,
    expected_prev_head: Option<Sha256>,
    frame: &EventFrameV1,
) -> Result<VerifiedEventFrameV1, ProtocolError> {
    // Decode lossless envelope from the canonical bytes.
    let env = codec.decode_lossless_envelope(frame.bytes.clone(), limits)?;

    // Cross-check identity: eid must match decoded envelope fields.
    if env.common.origin_replica_id != frame.eid.origin_replica_id
        || env.common.namespace != frame.eid.namespace
        || env.common.origin_seq != frame.eid.origin_seq
    {
        return Err(ProtocolError::FrameMismatch);
    }

    // Cross-check sha declaration.
    if env.declared_sha256 != frame.sha256 {
        return Err(ProtocolError::FrameMismatch);
    }

    // Cross-check prev declaration and frame prev.
    let seq = env.common.origin_seq.as_u64();
    match (seq, frame.prev_sha256, env.declared_prev) {
        (1, None, PrevLink::Genesis) => {}
        (1, _, _) => return Err(ProtocolError::FrameMismatch),
        (s, Some(p), PrevLink::Link(p2)) if s > 1 && p == p2 => {}
        (s, _, _) if s > 1 => return Err(ProtocolError::FrameMismatch),
        _ => {}
    }

    // Verify sha and continuity.
    let verified = verify_event(codec, expected_store, env, frame.bytes.clone(), expected_prev_head)?;
    let within_limits = enforce_limits(verified, limits)?;

    // Equivocation gate.
    let non_eq = enforce_no_equivocation(lookup, within_limits)?;
    Ok(VerifiedEventFrameV1 { eid: frame.eid.clone(), verified: non_eq.ev })
}

// =================================================================================================
// 14. Gap buffering and WANT logic (Plan §9.4, §9.8)
// =================================================================================================
//
// Key invariant:
// - The receiver MUST NOT append/apply unless contiguous for (ns, origin): seq == durable_seen + 1.
// - Gaps are buffered (bounded) and WANT is issued.
// - Progress only advances contiguously. No skipping.

#[derive(Clone, Debug)]
pub struct GapBuffer {
    pub buffered: BTreeMap<Seq1, VerifiedEventWithinLimits>,
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
    pub durable: Seq0,
    pub durable_head: Option<Sha256>,
    pub gap: GapBuffer,
}

/// Decision of the contiguity machine for a single incoming verified event.
#[derive(Clone, Debug)]
pub enum IngestDecision {
    /// The event (and any buffered suffix) is now contiguous and should be forwarded for ingest.
    ForwardContiguousBatch(Vec<VerifiedEventWithinLimits>),
    /// The event was buffered due to a gap; session should WANT from durable watermark.
    BufferedNeedWant { want_from: Seq0 },
    /// Duplicate event (already durable locally): no-op.
    DuplicateNoop,
    /// Hard rejection due to overflow/timeout.
    Reject { code: String },
}

impl OriginStreamState {
    pub fn expected_next(&self) -> Seq1 {
        self.durable.next()
    }

    pub fn ingest_one(&mut self, ev: VerifiedEventWithinLimits, now_ms: u64) -> IngestDecision {
        let seq = ev.0.env.common.origin_seq;

        // If already durable, treat as duplicate no-op.
        if seq.as_u64() <= self.durable.0 {
            return IngestDecision::DuplicateNoop;
        }

        // If not the expected next, buffer and WANT.
        if seq.as_u64() != self.durable.0 + 1 {
            if self.gap.started_at_ms.is_none() {
                self.gap.started_at_ms = Some(now_ms);
            } else if let Some(start) = self.gap.started_at_ms {
                if now_ms.saturating_sub(start) > self.gap.timeout_ms {
                    return IngestDecision::Reject { code: "gap_timeout".into() };
                }
            }

            if self.gap.buffered.len() >= self.gap.max_events {
                return IngestDecision::Reject { code: "gap_buffer_overflow".into() };
            }

            let bytes = ev.0.envelope_bytes.0.len();
            if self.gap.buffered_bytes + bytes > self.gap.max_bytes {
                return IngestDecision::Reject { code: "gap_buffer_bytes_overflow".into() };
            }

            self.gap.buffered_bytes += bytes;
            self.gap.buffered.insert(seq, ev);

            return IngestDecision::BufferedNeedWant { want_from: self.durable };
        }

        // Contiguous: drain any now-contiguous buffered suffix.
        let mut batch = vec![ev];
        let mut next = seq.as_u64() + 1;
        while let Some(nz) = NonZeroU64::new(next) {
            let s = Seq1(nz);
            if let Some(ev2) = self.gap.buffered.remove(&s) {
                self.gap.buffered_bytes = self.gap.buffered_bytes.saturating_sub(ev2.0.envelope_bytes.0.len());
                batch.push(ev2);
                next += 1;
            } else {
                break;
            }
        }
        if self.gap.buffered.is_empty() {
            self.gap.started_at_ms = None;
        }
        IngestDecision::ForwardContiguousBatch(batch)
    }
}

// =================================================================================================
// 15. Coordinator boundary effects (Plan §0.13)
// =================================================================================================
//
// Plan §0.13: session threads do not mutate store state directly.
// They decode/verify/buffer/gap-manage, then forward ingest requests to the coordinator.
//
// To keep this model-checkable, the session `step()` emits **effects**.
// A real runtime executes the effects and feeds results back as messages.

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SessionId(pub u64);

#[derive(Clone, Debug)]
pub enum ToCoordinator {
    /// Request coordinator to ingest a contiguous batch for a single (ns, origin).
    /// The coordinator is responsible for:
    /// - WAL append/ingest
    /// - apply_event
    /// - advancing watermarks contiguously
    /// - producing ACK updates back to the session
    IngestContiguous {
        session_id: SessionId,
        ns: NamespaceId,
        origin: ReplicaId,
        events: Vec<VerifiedEventWithinLimits>,
    },
}

#[derive(Clone, Debug)]
pub enum ToNetwork {
    Send(ReplMsgV1),
    Close { code: String },
}

#[derive(Clone, Debug)]
pub enum SessionEffect {
    Network(ToNetwork),
    Coordinator(ToCoordinator),
}

/// Coordinator replies back into session logic.
#[derive(Clone, Debug)]
pub enum FromCoordinator {
    /// Coordinator accepted and persisted/applied events.
    /// It also returns the new durable head sha for the origin stream,
    /// which is needed to validate future prev_sha continuity.
    IngestOk {
        session_id: SessionId,
        ns: NamespaceId,
        origin: ReplicaId,
        new_durable: Seq0,
        new_durable_head: Option<Sha256>,
        ack: AckV1,
    },
    IngestErr {
        session_id: SessionId,
        code: String,
    },
}

// =================================================================================================
// 16. Replication session phases: typestate wrappers + model-checkable enum core
// =================================================================================================
//
// This is the “best of both worlds” part.
//
// - The **enum core** is easy to serialize, clone, and feed into Stateright.
// - The **typestate wrappers** give the production code compile-time safety:
//   you cannot process EVENTS before negotiation, for example.
//
// Both share the same transition logic.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionRole {
    Inbound,
    Outbound,
}

#[derive(Clone, Debug)]
pub struct ProtocolRange {
    pub max: u32,
    pub min: u32,
}

pub fn negotiate_version(local: &ProtocolRange, peer_max: u32, peer_min: u32) -> Result<u32, String> {
    // Plan §9.3 negotiation rule:
    // v = min(max_a, max_b) if v >= max(min_a, min_b)
    let v = local.max.min(peer_max);
    let min_ok = local.min.max(peer_min);
    if v >= min_ok {
        Ok(v)
    } else {
        Err("version_incompatible".into())
    }
}

/// Model-checkable core phase enum.
/// It intentionally contains only serializable-ish data and no lifetimes or IO.
#[derive(Clone, Debug)]
pub enum SessionPhaseCore {
    /// Not yet negotiated.
    New,
    /// Waiting for HELLO (inbound role).
    AwaitHello,
    /// Sent HELLO and waiting for WELCOME (outbound role).
    AwaitWelcome { hello_sent: HelloV1 },
    /// Negotiated session. May still be catching up.
    Established { negotiated: NegotiatedSession },
    /// Catching up via WANT/EVENTS to reach a target watermark.
    Syncing { negotiated: NegotiatedSession },
    /// Live streaming steady-state.
    Live { negotiated: NegotiatedSession },
    /// Closing due to error or shutdown.
    Closing { code: String },
    /// Fully closed.
    Closed { code: String },
}

/// Negotiated session parameters (Plan §9.3 effective frame limit, accepted namespaces).
#[derive(Clone, Debug)]
pub struct NegotiatedSession {
    pub store: StoreIdentity,
    pub local_replica_id: ReplicaId,
    pub peer_replica_id: ReplicaId,
    pub protocol_version: u32,
    pub max_frame_bytes: usize,
    pub accepted_namespaces: BTreeSet<NamespaceId>,
}

/// SessionCore is what you model-check.
/// It includes:
/// - phase
/// - per-origin stream states for accepted namespaces
/// - peer-advertised progress (from ACK/WELCOME)
#[derive(Clone, Debug)]
pub struct SessionCore {
    pub id: SessionId,
    pub role: SessionRole,
    pub phase: SessionPhaseCore,

    pub store: StoreIdentity,
    pub local_replica_id: ReplicaId,
    pub protocol: ProtocolRange,

    pub limits: Limits,

    /// Per (ns, origin) state for gap/contiguity and continuity head.
    pub streams: BTreeMap<(NamespaceId, ReplicaId), OriginStreamState>,

    /// Peer durable progress claims (monotonic observation, not local truth).
    pub peer_seen_durable: Watermarks<Durable>,
}

/// Typestate markers
pub enum PhaseNew {}
pub enum PhaseAwaitHello {}
pub enum PhaseAwaitWelcome {}
pub enum PhaseEstablished {}
pub enum PhaseSyncing {}
pub enum PhaseLive {}
pub enum PhaseClosed {}

/// Production wrapper: guarantees the `SessionCore.phase` is consistent with `P`.
#[derive(Clone, Debug)]
pub struct ReplSession<P> {
    pub core: SessionCore,
    _p: PhantomData<P>,
}

impl ReplSession<PhaseAwaitHello> {
    pub fn new_inbound(
        id: SessionId,
        store: StoreIdentity,
        local_replica_id: ReplicaId,
        protocol: ProtocolRange,
        limits: Limits,
    ) -> Self {
        Self {
            core: SessionCore {
                id,
                role: SessionRole::Inbound,
                phase: SessionPhaseCore::AwaitHello,
                store,
                local_replica_id,
                protocol,
                limits,
                streams: BTreeMap::new(),
                peer_seen_durable: Watermarks::default(),
            },
            _p: PhantomData,
        }
    }
}

impl ReplSession<PhaseNew> {
    pub fn new_outbound(
        id: SessionId,
        store: StoreIdentity,
        local_replica_id: ReplicaId,
        protocol: ProtocolRange,
        limits: Limits,
    ) -> Self {
        Self {
            core: SessionCore {
                id,
                role: SessionRole::Outbound,
                phase: SessionPhaseCore::New,
                store,
                local_replica_id,
                protocol,
                limits,
                streams: BTreeMap::new(),
                peer_seen_durable: Watermarks::default(),
            },
            _p: PhantomData,
        }
    }
}

// =================================================================================================
// 17. Session step logic (pure, shared by model + production)
// =================================================================================================
//
// This function is the key “shared seam”: pure input -> (new state, effects).
// - A model-check harness can drive it directly.
// - Production code can call it and then interpret effects (send network, forward to coordinator).
//
// Important: this step logic does not mutate store state. It only manages protocol/session state.

pub struct StepDeps<'a> {
    pub codec: &'a dyn EventEnvelopeCodecV1,
    pub lookup: &'a dyn EventShaLookupV1,
    pub now_ms: u64,
}

/// Transition output: updated session core + side effects to run.
#[derive(Clone, Debug)]
pub struct StepResult {
    pub next: SessionCore,
    pub effects: Vec<SessionEffect>,
}

pub fn session_step(core: &SessionCore, deps: &StepDeps<'_>, msg: ReplMsgV1) -> StepResult {
    let mut next = core.clone();
    let mut effects = Vec::new();

    // If already closed, ignore everything.
    if matches!(next.phase, SessionPhaseCore::Closed { .. }) {
        return StepResult { next, effects };
    }

    // Helper for hard close (does not capture `next` or `effects` to avoid borrow conflicts).
    let hard_close = |next: &mut SessionCore, effects: &mut Vec<SessionEffect>, code: &str| {
        next.phase = SessionPhaseCore::Closed { code: code.to_string() };
        effects.push(SessionEffect::Network(ToNetwork::Close { code: code.to_string() }));
    };

    match (&next.phase, msg) {
        // ---------------------------
        // Inbound handshake: AwaitHello -> (verify) -> send Welcome -> Established
        // ---------------------------
        (SessionPhaseCore::AwaitHello, ReplMsgV1::Hello(hello)) => {
            // Store identity check (Plan §9.3, §9.10)
            if hello.store_id != next.store.store_id || hello.store_epoch != next.store.store_epoch {
                hard_close(&mut next, &mut effects, "wrong_store");
                return StepResult { next, effects };
            }
            if hello.sender_replica_id == next.local_replica_id {
                hard_close(&mut next, &mut effects, "replica_id_collision");
                return StepResult { next, effects };
            }

            let negotiated = match negotiate_version(&next.protocol, hello.protocol_version, hello.min_protocol_version) {
                Ok(v) => v,
                Err(_) => {
                    hard_close(&mut next, &mut effects, "version_incompatible");
                    return StepResult { next, effects };
                }
            };

            let max_frame_bytes = (hello.max_frame_bytes as usize).min(next.limits.max_frame_bytes);

            // Namespace acceptance: intersection(requested, offered).
            // Policy gating is not in this file; that is a higher layer (Plan §9.9).
            let requested: BTreeSet<_> = hello.requested_namespaces.iter().cloned().collect();
            let offered: BTreeSet<_> = hello.offered_namespaces.iter().cloned().collect();
            let accepted: BTreeSet<_> = requested.intersection(&offered).cloned().collect();

            let welcome = WelcomeV1 {
                protocol_version: negotiated,
                store_id: next.store.store_id,
                store_epoch: next.store.store_epoch,
                receiver_replica_id: next.local_replica_id,
                accepted_namespaces: accepted.iter().cloned().collect(),
                receiver_seen_durable: Watermarks::<Durable>::default(),
                receiver_seen_durable_heads: None,
                receiver_seen_applied: None,
                receiver_seen_applied_heads: None,
                max_frame_bytes: max_frame_bytes as u32,
            };

            effects.push(SessionEffect::Network(ToNetwork::Send(ReplMsgV1::Welcome(welcome.clone()))));

            let negotiated_session = NegotiatedSession {
                store: next.store,
                local_replica_id: next.local_replica_id,
                peer_replica_id: hello.sender_replica_id,
                protocol_version: negotiated,
                max_frame_bytes,
                accepted_namespaces: accepted.clone(),
            };

            next.phase = SessionPhaseCore::Established { negotiated: negotiated_session };

            // Record peer progress claim (WELCOME/HELLO are claims, not local truth).
            next.peer_seen_durable = hello.seen_durable;
        }

        // ---------------------------
        // Outbound handshake: New -> emit Hello -> AwaitWelcome
        // ---------------------------
        (SessionPhaseCore::New, ReplMsgV1::Ping { .. }) => {
            // No-op placeholder: in production you likely trigger HELLO creation separately.
        }

        (SessionPhaseCore::New, ReplMsgV1::Error { code }) => {
            hard_close(&mut next, &mut effects, &format!("peer_error:{code}"));
        }

        // Outbound: AwaitWelcome -> verify -> Established
        (SessionPhaseCore::AwaitWelcome { .. }, ReplMsgV1::Welcome(welcome)) => {
            if welcome.store_id != next.store.store_id || welcome.store_epoch != next.store.store_epoch {
                hard_close(&mut next, &mut effects, "wrong_store");
                return StepResult { next, effects };
            }
            if welcome.receiver_replica_id == next.local_replica_id {
                // This is not actually a collision; it is the peer telling us our id.
                // Still, if the protocol is confused here, closing is safer.
                hard_close(&mut next, &mut effects, "welcome_identity_confused");
                return StepResult { next, effects };
            }

            // We should already have chosen a protocol on HELLO send; here we accept the welcome's.
            let max_frame_bytes = (welcome.max_frame_bytes as usize).min(next.limits.max_frame_bytes);
            let accepted: BTreeSet<_> = welcome.accepted_namespaces.iter().cloned().collect();

            let negotiated_session = NegotiatedSession {
                store: next.store,
                local_replica_id: next.local_replica_id,
                peer_replica_id: welcome.receiver_replica_id,
                protocol_version: welcome.protocol_version,
                max_frame_bytes,
                accepted_namespaces: accepted.clone(),
            };
            next.phase = SessionPhaseCore::Established { negotiated: negotiated_session };

            next.peer_seen_durable = welcome.receiver_seen_durable;
        }

        // ---------------------------
        // Established / Syncing / Live: handle EVENTS
        // ---------------------------
        (SessionPhaseCore::Established { negotiated }
        | SessionPhaseCore::Syncing { negotiated }
        | SessionPhaseCore::Live { negotiated }, ReplMsgV1::Events(frames)) => {
            // Batch bounds and negotiated frame limit (Plan §0.19, §9.8)
            if let Err(e) = validate_events_batch(&frames, &next.limits, negotiated.max_frame_bytes) {
                hard_close(&mut next, &mut effects, &format!("batch_invalid:{e}"));
                return StepResult { next, effects };
            }

            // For each frame:
            // - verify frame and hash
            // - enforce continuity head for that origin stream
            // - run contiguity machine: either forward to coordinator or buffer and WANT
            for frame in frames {
                // Namespace gating: if not accepted, reject.
                if !negotiated.accepted_namespaces.contains(&frame.eid.namespace) {
                    hard_close(&mut next, &mut effects, "namespace_not_accepted");
                    return StepResult { next, effects };
                }

                let key = (frame.eid.namespace.clone(), frame.eid.origin_replica_id);
                let stream = next.streams.entry(key.clone()).or_insert_with(|| OriginStreamState {
                    ns: frame.eid.namespace.clone(),
                    origin: frame.eid.origin_replica_id,
                    durable: Seq0(0),
                    durable_head: None,
                    gap: GapBuffer::new(&next.limits),
                });

                let expected_prev = stream.durable_head;

                // Verify, enforce limits, enforce equivocation.
                let verified_frame = match verify_event_frame(
                    deps.codec,
                    deps.lookup,
                    &next.limits,
                    next.store,
                    expected_prev,
                    &frame,
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        hard_close(&mut next, &mut effects, &format!("event_rejected:{e}"));
                        return StepResult { next, effects };
                    }
                };

                // Feed into contiguity/gap machine.
                match stream.ingest_one(verified_frame.verified, deps.now_ms) {
                    IngestDecision::DuplicateNoop => {}
                    IngestDecision::BufferedNeedWant { want_from } => {
                        // WANT: request missing prefix (Plan §9.6).
                        // In this types file we send a WANT message with that single key updated.
                        let mut want = Watermarks::<Durable>::default();
                        want.observe_at_least(caps::from_ack(), stream.ns.clone(), stream.origin, want_from);

                        effects.push(SessionEffect::Network(ToNetwork::Send(ReplMsgV1::Want(WantV1 { want }))));
                    }
                    IngestDecision::ForwardContiguousBatch(batch) => {
                        effects.push(SessionEffect::Coordinator(ToCoordinator::IngestContiguous {
                            session_id: next.id.clone(),
                            ns: stream.ns.clone(),
                            origin: stream.origin,
                            events: batch,
                        }));
                    }
                    IngestDecision::Reject { code } => {
                        hard_close(&mut next, &mut effects, &format!("gap_reject:{code}"));
                        return StepResult { next, effects };
                    }
                }
            }

            // Heuristic: once established, you typically move into Syncing or Live based on watermark deltas.
            // This policy decision can live above this layer.
        }

        // Handle ACK: monotonic observation only (Plan §9.5; do not treat as local truth).
        (SessionPhaseCore::Established { .. }
        | SessionPhaseCore::Syncing { .. }
        | SessionPhaseCore::Live { .. }, ReplMsgV1::Ack(ack)) => {
            // Observe peer progress claims.
            // These values should not be used to advance local contiguity watermarks.
            next.peer_seen_durable = ack.durable;
        }

        // Coordinator reply: update local per-origin durable head state and send ACK to peer.
        (SessionPhaseCore::Established { .. }
        | SessionPhaseCore::Syncing { .. }
        | SessionPhaseCore::Live { .. }, ReplMsgV1::Error { code }) => {
            hard_close(&mut next, &mut effects, &format!("peer_error:{code}"));
        }

        // Any message in the wrong phase: close hard.
        (_, ReplMsgV1::Welcome(_)) if matches!(next.phase, SessionPhaseCore::AwaitHello | SessionPhaseCore::New) => {
            hard_close(&mut next, &mut effects, "unexpected_welcome");
        }
        (_, ReplMsgV1::Hello(_)) if matches!(next.phase, SessionPhaseCore::AwaitWelcome { .. }) => {
            hard_close(&mut next, &mut effects, "unexpected_hello");
        }

        // Default: ignore or close based on policy.
        _ => {}
    }

    StepResult { next, effects }
}

/// Apply coordinator responses to session state.
/// This is separate from `session_step` because coordinator replies are not network messages.
/// It is also pure and model-checkable.
pub fn session_apply_coordinator(core: &SessionCore, reply: FromCoordinator) -> StepResult {
    let mut next = core.clone();
    let mut effects = Vec::new();

    match reply {
        FromCoordinator::IngestOk { session_id, ns, origin, new_durable, new_durable_head, ack } => {
            if session_id != next.id {
                // Wrong session id; ignore defensively.
                return StepResult { next, effects };
            }
            // Update the origin stream durable watermark and head sha.
            let key = (ns.clone(), origin);
            if let Some(stream) = next.streams.get_mut(&key) {
                stream.durable = new_durable;
                stream.durable_head = new_durable_head;
            }
            // Send ACK to peer (Plan §9.5).
            effects.push(SessionEffect::Network(ToNetwork::Send(ReplMsgV1::Ack(ack))));
        }
        FromCoordinator::IngestErr { session_id, code } => {
            if session_id != next.id {
                return StepResult { next, effects };
            }
            next.phase = SessionPhaseCore::Closed { code: format!("ingest_err:{code}") };
            effects.push(SessionEffect::Network(ToNetwork::Close { code: format!("ingest_err:{code}") }));
        }
    }

    StepResult { next, effects }
}

// =================================================================================================
// 18. Typestate convenience transitions (production ergonomics)
// =================================================================================================
//
// These are thin wrappers over the enum core to make “wrong-phase calls” unrepresentable.

impl ReplSession<PhaseAwaitHello> {
    pub fn on_msg(self, deps: &StepDeps<'_>, msg: ReplMsgV1) -> (SessionCore, Vec<SessionEffect>) {
        let out = session_step(&self.core, deps, msg);
        (out.next, out.effects)
    }
}

impl ReplSession<PhaseNew> {
    /// Outbound sessions often start by sending HELLO.
    /// This helper builds the HELLO and returns the updated core + effect.
    pub fn send_hello(mut self, hello: HelloV1) -> (ReplSession<PhaseAwaitWelcome>, Vec<SessionEffect>) {
        self.core.phase = SessionPhaseCore::AwaitWelcome { hello_sent: hello.clone() };
        (
            ReplSession {
                core: self.core,
                _p: PhantomData,
            },
            vec![SessionEffect::Network(ToNetwork::Send(ReplMsgV1::Hello(hello)))],
        )
    }
}

// =================================================================================================
// 19. Minimal main
// =================================================================================================

fn main() {
    // This is a types-first, literate module.
    // The “executable” property means it compiles and can be imported by models/tests.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;
    use uuid::Uuid;

    #[test]
    fn gc_floor_retroactively_clears_applied_events() {
        let ns = NamespaceId::try_from("core").expect("namespace");
        let origin = ReplicaId(Uuid::new_v4());
        let event_id = EventId {
            origin_replica_id: origin,
            namespace: ns.clone(),
            origin_seq: Seq1(NonZeroU64::new(1).unwrap()),
        };
        let event = AppliedEvent {
            id: event_id,
            event_time_ms: 5,
        };
        let marker = NamespaceGcMarkerV1 {
            cutoff_ms: 5,
            gc_authority_replica_id: origin,
            enforce_floor: true,
        };

        let mut state_event_then_gc = NamespaceApplyState::default();
        assert!(state_event_then_gc.apply_event_with_floor(event.clone()));
        state_event_then_gc.apply_gc_marker(&ns, &marker);

        let mut state_gc_then_event = NamespaceApplyState::default();
        state_gc_then_event.apply_gc_marker(&ns, &marker);
        assert!(!state_gc_then_event.apply_event_with_floor(event.clone()));

        assert!(state_event_then_gc.applied.is_empty());
        assert!(state_gc_then_event.applied.is_empty());
        assert_eq!(state_event_then_gc.gc_floor_ms(&ns), 5);
        assert_eq!(state_event_then_gc, state_gc_then_event);
    }
}
