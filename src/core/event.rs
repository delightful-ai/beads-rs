//! Event body encoding + hashing for realtime WAL and replication.

use std::collections::BTreeSet;
use std::marker::PhantomData;

use bytes::Bytes;
use minicbor::data::Type;
use minicbor::{Decoder, Encoder};
use sha2::{Digest, Sha256 as Sha2};
use thiserror::Error;

use super::domain::DepKind;
use super::identity::{
    ActorId, ClientRequestId, EventId, ReplicaId, StoreId, StoreIdentity, TraceId, TxnId,
};
use super::limits::Limits;
use super::namespace::NamespaceId;
use super::watermark::Seq1;
use super::wire_bead::{
    NoteAppendV1, NotesPatch, TxnDeltaV1, TxnOpV1, WireBeadPatch, WireDepDeleteV1, WireDepV1,
    WireNoteV1, WirePatch, WireStamp, WireTombstoneV1, WorkflowStatus,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Canonical {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Opaque {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventBytes<S> {
    bytes: Bytes,
    _state: PhantomData<S>,
}

impl<S> EventBytes<S> {
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.as_ref()
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl EventBytes<Canonical> {
    /// Construct canonical bytes without validation; only use with encoder output.
    pub(crate) fn new_unchecked(bytes: Bytes) -> Self {
        Self {
            bytes,
            _state: PhantomData,
        }
    }
}

impl EventBytes<Opaque> {
    pub fn new(bytes: Bytes) -> Self {
        Self {
            bytes,
            _state: PhantomData,
        }
    }
}

impl<S> AsRef<[u8]> for EventBytes<S> {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl From<EventBytes<Canonical>> for EventBytes<Opaque> {
    fn from(bytes: EventBytes<Canonical>) -> Self {
        EventBytes::<Opaque>::new(bytes.bytes)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Sha256(pub [u8; 32]);

impl Sha256 {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

pub fn sha256_bytes(data: &[u8]) -> Sha256 {
    let mut hasher = Sha2::new();
    hasher.update(data);
    let out = hasher.finalize();
    let mut buf = [0u8; 32];
    buf.copy_from_slice(&out);
    Sha256(buf)
}

pub fn hash_event_body<S>(bytes: &EventBytes<S>) -> Sha256 {
    sha256_bytes(bytes.as_ref())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HlcMax {
    pub actor_id: ActorId,
    pub physical_ms: u64,
    pub logical: u32,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EventKindTag {
    TxnV1,
}

impl EventKindV1 {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventKindV1::TxnV1(_) => "txn_v1",
        }
    }
}

impl EventKindTag {
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "txn_v1" => Some(EventKindTag::TxnV1),
            _ => None,
        }
    }
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
    pub trace_id: Option<TraceId>,
    pub kind: EventKindV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EventValidationError {
    #[error("txn ops {ops} exceeds max {max}")]
    TooManyOps { ops: usize, max: usize },
    #[error("note content bytes {bytes} exceeds max {max}")]
    NoteTooLarge { bytes: usize, max: usize },
    #[error("labels count {count} exceeds max {max}")]
    TooManyLabels { count: usize, max: usize },
    #[error("note_appends count {count} exceeds max {max}")]
    TooManyNoteAppends { count: usize, max: usize },
}

#[derive(Debug, Error)]
#[error("event sha lookup failed: {source}")]
pub struct EventShaLookupError {
    #[source]
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl EventShaLookupError {
    pub fn new<E>(source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            source: Box::new(source),
        }
    }
}

pub trait EventShaLookup {
    fn lookup_event_sha(&self, eid: &EventId) -> Result<Option<Sha256>, EventShaLookupError>;
}

#[derive(Debug, Error)]
pub enum EventFrameError {
    #[error("wrong store identity")]
    WrongStore {
        expected: StoreIdentity,
        got: StoreIdentity,
    },
    #[error("event id does not match decoded body")]
    FrameMismatch,
    #[error("sha256 mismatch")]
    HashMismatch,
    #[error("prev_sha256 mismatch")]
    PrevMismatch,
    #[error("event validation failed: {0}")]
    Validation(#[from] EventValidationError),
    #[error("event body decode failed: {0}")]
    Decode(#[from] DecodeError),
    #[error(transparent)]
    Lookup(#[from] EventShaLookupError),
    #[error("equivocation detected")]
    Equivocation,
}

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("cbor encode: {0}")]
    Cbor(#[from] minicbor::encode::Error<std::convert::Infallible>),
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("decode limit exceeded: {0}")]
    DecodeLimit(&'static str),
    #[error("indefinite-length CBOR not allowed")]
    IndefiniteLength,
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    #[error("invalid field {field}: {reason}")]
    InvalidField { field: &'static str, reason: String },
    #[error("unsupported event kind: {0}")]
    UnsupportedKind(String),
    #[error("unsupported delta version: {0}")]
    UnsupportedDeltaVersion(u64),
    #[error("duplicate op: {0}")]
    DuplicateOp(String),
    #[error("duplicate map key: {0}")]
    DuplicateKey(String),
    #[error("trailing bytes after event body")]
    TrailingBytes,
    #[error("cbor decode: {0}")]
    Cbor(#[from] minicbor::decode::Error),
}

pub fn encode_event_body_canonical(body: &EventBody) -> Result<EventBytes<Canonical>, EncodeError> {
    let mut buf = Vec::new();
    let mut enc = Encoder::new(&mut buf);
    encode_event_body_map(&mut enc, body)?;
    Ok(EventBytes::<Canonical>::new_unchecked(Bytes::from(buf)))
}

pub fn decode_event_body(
    bytes: &[u8],
    limits: &Limits,
) -> Result<(EventBytes<Opaque>, EventBody), DecodeError> {
    let max_bytes = limits.max_wal_record_bytes.min(limits.max_frame_bytes);
    if bytes.len() > max_bytes {
        return Err(DecodeError::DecodeLimit("max_wal_record_bytes"));
    }

    let mut dec = Decoder::new(bytes);
    let body = decode_event_body_map(&mut dec, limits, 0)?;
    if dec.datatype().is_ok() {
        return Err(DecodeError::TrailingBytes);
    }
    Ok((
        EventBytes::<Opaque>::new(Bytes::copy_from_slice(bytes)),
        body,
    ))
}

pub fn decode_event_hlc_max(bytes: &[u8], limits: &Limits) -> Result<Option<HlcMax>, DecodeError> {
    let max_bytes = limits.max_wal_record_bytes.min(limits.max_frame_bytes);
    if bytes.len() > max_bytes {
        return Err(DecodeError::DecodeLimit("max_wal_record_bytes"));
    }

    let mut dec = Decoder::new(bytes);
    let map_len = decode_map_len(&mut dec, limits, 0)?;
    let mut seen_keys = BTreeSet::new();
    let mut hlc_max = None;
    let mut event_time_ms = None;
    let mut kind: Option<EventKindTag> = None;
    for _ in 0..map_len {
        let key = decode_text(&mut dec, limits)?;
        ensure_unique_key(&mut seen_keys, key)?;
        match key {
            "kind" => {
                let raw = decode_text(&mut dec, limits)?;
                kind = EventKindTag::parse(raw);
            }
            "event_time_ms" => {
                event_time_ms = Some(decode_u64(&mut dec, "event_time_ms")?);
            }
            "hlc_max" => {
                hlc_max = Some(decode_hlc_max(&mut dec, limits, 1)?);
            }
            _ => {
                skip_value(&mut dec, limits, 1)?;
            }
        }
    }
    if dec.datatype().is_ok() {
        return Err(DecodeError::TrailingBytes);
    }
    if matches!(kind, Some(EventKindTag::TxnV1)) && hlc_max.is_none() {
        return Err(DecodeError::MissingField("hlc_max"));
    }
    if let Some(hlc_max) = &hlc_max {
        let event_time_ms = event_time_ms.ok_or(DecodeError::MissingField("event_time_ms"))?;
        if hlc_max.physical_ms != event_time_ms {
            return Err(DecodeError::InvalidField {
                field: "hlc_max.physical_ms",
                reason: "must match event_time_ms".into(),
            });
        }
    }
    Ok(hlc_max)
}

fn encode_event_body_map(
    enc: &mut Encoder<&mut Vec<u8>>,
    body: &EventBody,
) -> Result<(), EncodeError> {
    let mut len = 9;
    if body.client_request_id.is_some() {
        len += 1;
    }
    if body.trace_id.is_some() {
        len += 1;
    }
    match &body.kind {
        EventKindV1::TxnV1(_) => {
            len += 2;
        }
    }

    enc.map(len as u64)?;

    if let Some(client_request_id) = &body.client_request_id {
        enc.str("client_request_id")?;
        enc.str(&client_request_id.as_uuid().to_string())?;
    }

    match &body.kind {
        EventKindV1::TxnV1(txn) => {
            enc.str("delta")?;
            encode_txn_delta(enc, &txn.delta)?;
        }
    }

    enc.str("envelope_v")?;
    enc.u32(body.envelope_v)?;

    enc.str("event_time_ms")?;
    enc.u64(body.event_time_ms)?;

    match &body.kind {
        EventKindV1::TxnV1(txn) => {
            enc.str("hlc_max")?;
            encode_hlc_max(enc, &txn.hlc_max)?;
        }
    }

    enc.str("kind")?;
    enc.str(body.kind.as_str())?;

    enc.str("namespace")?;
    enc.str(body.namespace.as_str())?;

    enc.str("origin_replica_id")?;
    enc.str(&body.origin_replica_id.as_uuid().to_string())?;

    enc.str("origin_seq")?;
    enc.u64(body.origin_seq.get())?;

    enc.str("store_epoch")?;
    enc.u64(body.store.store_epoch.get())?;

    enc.str("store_id")?;
    enc.str(&body.store.store_id.as_uuid().to_string())?;

    if let Some(trace_id) = &body.trace_id {
        enc.str("trace_id")?;
        enc.str(&trace_id.as_uuid().to_string())?;
    }

    enc.str("txn_id")?;
    enc.str(&body.txn_id.as_uuid().to_string())?;

    Ok(())
}

fn decode_event_body_map(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<EventBody, DecodeError> {
    let map_len = decode_map_len(dec, limits, depth)?;

    let mut seen_keys = BTreeSet::new();
    let mut envelope_v = None;
    let mut store_id: Option<StoreId> = None;
    let mut store_epoch = None;
    let mut namespace: Option<NamespaceId> = None;
    let mut origin_replica_id: Option<ReplicaId> = None;
    let mut origin_seq: Option<Seq1> = None;
    let mut event_time_ms = None;
    let mut txn_id: Option<TxnId> = None;
    let mut client_request_id: Option<ClientRequestId> = None;
    let mut trace_id: Option<TraceId> = None;
    let mut kind: Option<EventKindTag> = None;
    let mut delta: Option<TxnDeltaV1> = None;
    let mut hlc_max: Option<HlcMax> = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        ensure_unique_key(&mut seen_keys, key)?;
        match key {
            "client_request_id" => {
                let raw = decode_text(dec, limits)?;
                client_request_id = Some(parse_uuid_field("client_request_id", raw)?);
            }
            "trace_id" => {
                let raw = decode_text(dec, limits)?;
                trace_id = Some(parse_uuid_field("trace_id", raw)?);
            }
            "delta" => {
                delta = Some(decode_txn_delta(dec, limits, depth + 1)?);
            }
            "envelope_v" => {
                envelope_v = Some(decode_u32(dec, "envelope_v")?);
            }
            "event_time_ms" => {
                event_time_ms = Some(decode_u64(dec, "event_time_ms")?);
            }
            "hlc_max" => {
                hlc_max = Some(decode_hlc_max(dec, limits, depth + 1)?);
            }
            "kind" => {
                let raw = decode_text(dec, limits)?;
                kind = Some(
                    EventKindTag::parse(raw)
                        .ok_or_else(|| DecodeError::UnsupportedKind(raw.to_string()))?,
                );
            }
            "namespace" => {
                let raw = decode_text(dec, limits)?;
                namespace = Some(parse_namespace(raw)?);
            }
            "origin_replica_id" => {
                let raw = decode_text(dec, limits)?;
                origin_replica_id = Some(parse_uuid_field("origin_replica_id", raw)?);
            }
            "origin_seq" => {
                let value = decode_u64(dec, "origin_seq")?;
                origin_seq =
                    Some(
                        Seq1::from_u64(value).ok_or_else(|| DecodeError::InvalidField {
                            field: "origin_seq",
                            reason: "must be nonzero".into(),
                        })?,
                    );
            }
            "store_epoch" => {
                store_epoch = Some(decode_u64(dec, "store_epoch")?);
            }
            "store_id" => {
                let raw = decode_text(dec, limits)?;
                store_id = Some(parse_uuid_field("store_id", raw)?);
            }
            "txn_id" => {
                let raw = decode_text(dec, limits)?;
                txn_id = Some(parse_uuid_field("txn_id", raw)?);
            }
            _ => {
                skip_value(dec, limits, depth + 1)?;
            }
        }
    }

    let envelope_v = envelope_v.ok_or(DecodeError::MissingField("envelope_v"))?;
    let store_id = store_id.ok_or(DecodeError::MissingField("store_id"))?;
    let store_epoch = store_epoch.ok_or(DecodeError::MissingField("store_epoch"))?;
    let namespace = namespace.ok_or(DecodeError::MissingField("namespace"))?;
    let origin_replica_id =
        origin_replica_id.ok_or(DecodeError::MissingField("origin_replica_id"))?;
    let origin_seq = origin_seq.ok_or(DecodeError::MissingField("origin_seq"))?;
    let event_time_ms = event_time_ms.ok_or(DecodeError::MissingField("event_time_ms"))?;
    let txn_id = txn_id.ok_or(DecodeError::MissingField("txn_id"))?;
    let kind = kind.ok_or(DecodeError::MissingField("kind"))?;
    let kind = match kind {
        EventKindTag::TxnV1 => {
            let delta = delta.ok_or(DecodeError::MissingField("delta"))?;
            let hlc_max = hlc_max.ok_or(DecodeError::MissingField("hlc_max"))?;
            if hlc_max.physical_ms != event_time_ms {
                return Err(DecodeError::InvalidField {
                    field: "hlc_max.physical_ms",
                    reason: "must match event_time_ms".into(),
                });
            }
            EventKindV1::TxnV1(TxnV1 { hlc_max, delta })
        }
    };

    if envelope_v != 1 {
        return Err(DecodeError::InvalidField {
            field: "envelope_v",
            reason: format!("unsupported version {envelope_v}"),
        });
    }

    Ok(EventBody {
        envelope_v,
        store: StoreIdentity::new(store_id, store_epoch.into()),
        namespace,
        origin_replica_id,
        origin_seq,
        event_time_ms,
        txn_id,
        client_request_id,
        trace_id,
        kind,
    })
}

fn encode_txn_delta(
    enc: &mut Encoder<&mut Vec<u8>>,
    delta: &TxnDeltaV1,
) -> Result<(), EncodeError> {
    let mut bead_upserts: Vec<&WireBeadPatch> = Vec::new();
    let mut bead_deletes: Vec<&WireTombstoneV1> = Vec::new();
    let mut dep_upserts: Vec<&WireDepV1> = Vec::new();
    let mut dep_deletes: Vec<&WireDepDeleteV1> = Vec::new();
    let mut note_appends: Vec<&NoteAppendV1> = Vec::new();

    for op in delta.iter() {
        match op {
            TxnOpV1::BeadUpsert(up) => bead_upserts.push(up),
            TxnOpV1::BeadDelete(delete) => bead_deletes.push(delete),
            TxnOpV1::DepUpsert(dep) => dep_upserts.push(dep),
            TxnOpV1::DepDelete(dep) => dep_deletes.push(dep),
            TxnOpV1::NoteAppend(append) => note_appends.push(append),
        }
    }

    let mut len = 1;
    if !bead_upserts.is_empty() {
        len += 1;
    }
    if !bead_deletes.is_empty() {
        len += 1;
    }
    if !dep_upserts.is_empty() {
        len += 1;
    }
    if !dep_deletes.is_empty() {
        len += 1;
    }
    if !note_appends.is_empty() {
        len += 1;
    }

    enc.map(len as u64)?;

    if !bead_upserts.is_empty() {
        enc.str("bead_upserts")?;
        enc.array(bead_upserts.len() as u64)?;
        for patch in bead_upserts {
            enc.map(1)?;
            enc.str("bead")?;
            encode_wire_bead_patch(enc, patch)?;
        }
    }

    if !bead_deletes.is_empty() {
        enc.str("bead_deletes")?;
        enc.array(bead_deletes.len() as u64)?;
        for delete in bead_deletes {
            encode_wire_tombstone(enc, delete)?;
        }
    }

    if !dep_upserts.is_empty() {
        enc.str("dep_upserts")?;
        enc.array(dep_upserts.len() as u64)?;
        for dep in dep_upserts {
            encode_wire_dep(enc, dep)?;
        }
    }

    if !dep_deletes.is_empty() {
        enc.str("dep_deletes")?;
        enc.array(dep_deletes.len() as u64)?;
        for dep in dep_deletes {
            encode_wire_dep_delete(enc, dep)?;
        }
    }

    if !note_appends.is_empty() {
        enc.str("note_appends")?;
        enc.array(note_appends.len() as u64)?;
        for append in note_appends {
            enc.map(2)?;
            enc.str("bead_id")?;
            enc.str(append.bead_id.as_str())?;
            enc.str("note")?;
            encode_wire_note(enc, &append.note)?;
        }
    }

    enc.str("v")?;
    enc.u32(1)?;

    Ok(())
}

fn decode_txn_delta(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<TxnDeltaV1, DecodeError> {
    let map_len = decode_map_len(dec, limits, depth)?;

    let mut seen_keys = BTreeSet::new();
    let mut version = None;
    let mut bead_upserts: Vec<WireBeadPatch> = Vec::new();
    let mut bead_deletes: Vec<WireTombstoneV1> = Vec::new();
    let mut dep_upserts: Vec<WireDepV1> = Vec::new();
    let mut dep_deletes: Vec<WireDepDeleteV1> = Vec::new();
    let mut note_appends: Vec<NoteAppendV1> = Vec::new();
    let mut ops_total: usize = 0;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        ensure_unique_key(&mut seen_keys, key)?;
        match key {
            "bead_upserts" => {
                let arr_len = decode_array_len(dec, limits, depth + 1)?;
                ops_total = ops_total.saturating_add(arr_len);
                if ops_total > limits.max_ops_per_txn {
                    return Err(DecodeError::DecodeLimit("max_ops_per_txn"));
                }
                for _ in 0..arr_len {
                    let entry_len = decode_map_len(dec, limits, depth + 2)?;
                    if entry_len != 1 {
                        return Err(DecodeError::InvalidField {
                            field: "bead_upserts",
                            reason: "entry must have exactly one field".into(),
                        });
                    }
                    let entry_key = decode_text(dec, limits)?;
                    if entry_key != "bead" {
                        return Err(DecodeError::InvalidField {
                            field: "bead_upserts",
                            reason: format!("unexpected key {entry_key}"),
                        });
                    }
                    bead_upserts.push(decode_wire_bead_patch(dec, limits, depth + 3)?);
                }
            }
            "bead_deletes" => {
                let arr_len = decode_array_len(dec, limits, depth + 1)?;
                ops_total = ops_total.saturating_add(arr_len);
                if ops_total > limits.max_ops_per_txn {
                    return Err(DecodeError::DecodeLimit("max_ops_per_txn"));
                }
                for _ in 0..arr_len {
                    bead_deletes.push(decode_wire_tombstone(dec, limits, depth + 2)?);
                }
            }
            "dep_upserts" => {
                let arr_len = decode_array_len(dec, limits, depth + 1)?;
                ops_total = ops_total.saturating_add(arr_len);
                if ops_total > limits.max_ops_per_txn {
                    return Err(DecodeError::DecodeLimit("max_ops_per_txn"));
                }
                for _ in 0..arr_len {
                    dep_upserts.push(decode_wire_dep(dec, limits, depth + 2)?);
                }
            }
            "dep_deletes" => {
                let arr_len = decode_array_len(dec, limits, depth + 1)?;
                ops_total = ops_total.saturating_add(arr_len);
                if ops_total > limits.max_ops_per_txn {
                    return Err(DecodeError::DecodeLimit("max_ops_per_txn"));
                }
                for _ in 0..arr_len {
                    dep_deletes.push(decode_wire_dep_delete(dec, limits, depth + 2)?);
                }
            }
            "note_appends" => {
                let arr_len = decode_array_len(dec, limits, depth + 1)?;
                if arr_len > limits.max_note_appends_per_txn {
                    return Err(DecodeError::DecodeLimit("max_note_appends_per_txn"));
                }
                ops_total = ops_total.saturating_add(arr_len);
                if ops_total > limits.max_ops_per_txn {
                    return Err(DecodeError::DecodeLimit("max_ops_per_txn"));
                }
                for _ in 0..arr_len {
                    let entry_len = decode_map_len(dec, limits, depth + 2)?;
                    if entry_len != 2 {
                        return Err(DecodeError::InvalidField {
                            field: "note_appends",
                            reason: "entry must have exactly two fields".into(),
                        });
                    }
                    let mut bead_id = None;
                    let mut note = None;
                    let mut entry_seen = BTreeSet::new();
                    for _ in 0..entry_len {
                        let entry_key = decode_text(dec, limits)?;
                        ensure_unique_key(&mut entry_seen, entry_key)?;
                        match entry_key {
                            "bead_id" => {
                                let raw = decode_text(dec, limits)?;
                                bead_id = Some(parse_bead_id(raw)?);
                            }
                            "note" => {
                                note = Some(decode_wire_note(dec, limits, depth + 3)?);
                            }
                            other => {
                                return Err(DecodeError::InvalidField {
                                    field: "note_appends",
                                    reason: format!("unexpected key {other}"),
                                });
                            }
                        }
                    }
                    note_appends.push(NoteAppendV1 {
                        bead_id: bead_id.ok_or(DecodeError::MissingField("bead_id"))?,
                        note: note.ok_or(DecodeError::MissingField("note"))?,
                    });
                }
            }
            "v" => {
                version = Some(decode_u32(dec, "v")? as u64);
            }
            other => {
                return Err(DecodeError::InvalidField {
                    field: "delta",
                    reason: format!("unknown key {other}"),
                });
            }
        }
    }

    let version = version.ok_or(DecodeError::MissingField("v"))?;
    if version != 1 {
        return Err(DecodeError::UnsupportedDeltaVersion(version));
    }

    let mut delta = TxnDeltaV1::new();
    for up in bead_upserts {
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(up)))
            .map_err(|e| DecodeError::DuplicateOp(e.to_string()))?;
    }
    for delete in bead_deletes {
        delta
            .insert(TxnOpV1::BeadDelete(delete))
            .map_err(|e| DecodeError::DuplicateOp(e.to_string()))?;
    }
    for dep in dep_upserts {
        delta
            .insert(TxnOpV1::DepUpsert(dep))
            .map_err(|e| DecodeError::DuplicateOp(e.to_string()))?;
    }
    for dep in dep_deletes {
        delta
            .insert(TxnOpV1::DepDelete(dep))
            .map_err(|e| DecodeError::DuplicateOp(e.to_string()))?;
    }
    for na in note_appends {
        delta
            .insert(TxnOpV1::NoteAppend(na))
            .map_err(|e| DecodeError::DuplicateOp(e.to_string()))?;
    }
    Ok(delta)
}

fn encode_wire_bead_patch(
    enc: &mut Encoder<&mut Vec<u8>>,
    patch: &WireBeadPatch,
) -> Result<(), EncodeError> {
    let mut len = 1;
    if patch.created_at.is_some() {
        len += 1;
    }
    if patch.created_by.is_some() {
        len += 1;
    }
    if patch.created_on_branch.is_some() {
        len += 1;
    }
    if patch.title.is_some() {
        len += 1;
    }
    if patch.description.is_some() {
        len += 1;
    }
    if !patch.design.is_keep() {
        len += 1;
    }
    if !patch.acceptance_criteria.is_keep() {
        len += 1;
    }
    if patch.priority.is_some() {
        len += 1;
    }
    if patch.bead_type.is_some() {
        len += 1;
    }
    if patch.labels.is_some() {
        len += 1;
    }
    if !patch.external_ref.is_keep() {
        len += 1;
    }
    if !patch.source_repo.is_keep() {
        len += 1;
    }
    if !patch.estimated_minutes.is_keep() {
        len += 1;
    }
    if patch.status.is_some() {
        len += 1;
    }
    if !patch.closed_reason.is_keep() {
        len += 1;
    }
    if !patch.closed_on_branch.is_keep() {
        len += 1;
    }
    if !patch.assignee.is_keep() {
        len += 1;
    }
    if !patch.assignee_expires.is_keep() {
        len += 1;
    }
    if !patch.notes.is_omitted() {
        len += 1;
    }

    enc.map(len as u64)?;

    if !patch.acceptance_criteria.is_keep() {
        enc.str("acceptance_criteria")?;
        encode_wire_patch_str(enc, &patch.acceptance_criteria)?;
    }
    if !patch.assignee.is_keep() {
        enc.str("assignee")?;
        encode_wire_patch_actor(enc, &patch.assignee)?;
    }
    if !patch.assignee_expires.is_keep() {
        enc.str("assignee_expires")?;
        encode_wire_patch_wallclock(enc, &patch.assignee_expires)?;
    }
    if !patch.closed_on_branch.is_keep() {
        enc.str("closed_on_branch")?;
        encode_wire_patch_str(enc, &patch.closed_on_branch)?;
    }
    if !patch.closed_reason.is_keep() {
        enc.str("closed_reason")?;
        encode_wire_patch_str(enc, &patch.closed_reason)?;
    }
    if let Some(created_at) = patch.created_at {
        enc.str("created_at")?;
        encode_wire_stamp(enc, &created_at)?;
    }
    if let Some(created_by) = &patch.created_by {
        enc.str("created_by")?;
        enc.str(created_by.as_str())?;
    }
    if let Some(created_on_branch) = &patch.created_on_branch {
        enc.str("created_on_branch")?;
        enc.str(created_on_branch)?;
    }
    if let Some(description) = &patch.description {
        enc.str("description")?;
        enc.str(description)?;
    }
    if !patch.design.is_keep() {
        enc.str("design")?;
        encode_wire_patch_str(enc, &patch.design)?;
    }
    if !patch.estimated_minutes.is_keep() {
        enc.str("estimated_minutes")?;
        encode_wire_patch_u32(enc, &patch.estimated_minutes)?;
    }
    if !patch.external_ref.is_keep() {
        enc.str("external_ref")?;
        encode_wire_patch_str(enc, &patch.external_ref)?;
    }
    enc.str("id")?;
    enc.str(patch.id.as_str())?;
    if let Some(labels) = &patch.labels {
        enc.str("labels")?;
        enc.array(labels.len() as u64)?;
        for label in labels.iter() {
            enc.str(label.as_str())?;
        }
    }
    if !patch.notes.is_omitted() {
        enc.str("notes")?;
        match &patch.notes {
            NotesPatch::Omitted => {
                enc.array(0)?;
            }
            NotesPatch::AtLeast(notes) => {
                enc.array(notes.len() as u64)?;
                for note in notes {
                    encode_wire_note(enc, note)?;
                }
            }
        }
    }
    if let Some(priority) = patch.priority {
        enc.str("priority")?;
        enc.u32(priority.value().into())?;
    }
    if !patch.source_repo.is_keep() {
        enc.str("source_repo")?;
        encode_wire_patch_str(enc, &patch.source_repo)?;
    }
    if let Some(status) = patch.status {
        enc.str("status")?;
        enc.str(status.as_str())?;
    }
    if let Some(title) = &patch.title {
        enc.str("title")?;
        enc.str(title)?;
    }
    if let Some(bead_type) = patch.bead_type {
        enc.str("type")?;
        enc.str(bead_type.as_str())?;
    }

    Ok(())
}

fn decode_wire_bead_patch(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<WireBeadPatch, DecodeError> {
    let map_len = decode_map_len(dec, limits, depth)?;
    let mut seen_keys = BTreeSet::new();
    let mut patch = WireBeadPatch::new(bead_id_placeholder());
    let mut id_set = false;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        ensure_unique_key(&mut seen_keys, key)?;
        match key {
            "acceptance_criteria" => {
                patch.acceptance_criteria = decode_wire_patch_str(dec, limits)?;
            }
            "assignee" => {
                patch.assignee = decode_wire_patch_actor(dec, limits)?;
            }
            "assignee_expires" => {
                patch.assignee_expires = decode_wire_patch_wallclock(dec, limits)?;
            }
            "closed_on_branch" => {
                patch.closed_on_branch = decode_wire_patch_str(dec, limits)?;
            }
            "closed_reason" => {
                patch.closed_reason = decode_wire_patch_str(dec, limits)?;
            }
            "created_at" => {
                patch.created_at = Some(decode_wire_stamp(dec, limits, depth + 1)?);
            }
            "created_by" => {
                let raw = decode_text(dec, limits)?;
                patch.created_by = Some(parse_actor_id(raw, "created_by")?);
            }
            "created_on_branch" => {
                patch.created_on_branch = Some(decode_text(dec, limits)?.to_string());
            }
            "description" => {
                patch.description = Some(decode_text(dec, limits)?.to_string());
            }
            "design" => {
                patch.design = decode_wire_patch_str(dec, limits)?;
            }
            "estimated_minutes" => {
                patch.estimated_minutes = decode_wire_patch_u32(dec, limits)?;
            }
            "external_ref" => {
                patch.external_ref = decode_wire_patch_str(dec, limits)?;
            }
            "id" => {
                let raw = decode_text(dec, limits)?;
                patch.id = parse_bead_id(raw)?;
                id_set = true;
            }
            "labels" => {
                let arr_len = decode_array_len(dec, limits, depth + 1)?;
                let mut labels = super::collections::Labels::new();
                for _ in 0..arr_len {
                    let raw = decode_text(dec, limits)?;
                    let label = super::collections::Label::parse(raw.to_string()).map_err(|e| {
                        DecodeError::InvalidField {
                            field: "labels",
                            reason: e.to_string(),
                        }
                    })?;
                    labels.insert(label);
                }
                patch.labels = Some(labels);
            }
            "notes" => {
                let arr_len = decode_array_len(dec, limits, depth + 1)?;
                let mut notes = Vec::with_capacity(arr_len);
                for _ in 0..arr_len {
                    notes.push(decode_wire_note(dec, limits, depth + 2)?);
                }
                patch.notes = NotesPatch::AtLeast(notes);
            }
            "priority" => {
                let val = decode_u32(dec, "priority")?;
                let narrowed = u8::try_from(val).map_err(|_| DecodeError::InvalidField {
                    field: "priority",
                    reason: format!("value {val} out of range for u8"),
                })?;
                patch.priority = Some(super::domain::Priority::new(narrowed).map_err(|e| {
                    DecodeError::InvalidField {
                        field: "priority",
                        reason: e.to_string(),
                    }
                })?);
            }
            "source_repo" => {
                patch.source_repo = decode_wire_patch_str(dec, limits)?;
            }
            "status" => {
                let raw = decode_text(dec, limits)?;
                patch.status =
                    Some(
                        WorkflowStatus::parse(raw).ok_or_else(|| DecodeError::InvalidField {
                            field: "status",
                            reason: format!("unknown status {raw}"),
                        })?,
                    );
            }
            "title" => {
                patch.title = Some(decode_text(dec, limits)?.to_string());
            }
            "type" => {
                let raw = decode_text(dec, limits)?;
                patch.bead_type = Some(super::domain::BeadType::parse(raw).ok_or_else(|| {
                    DecodeError::InvalidField {
                        field: "type",
                        reason: format!("unknown bead type {raw}"),
                    }
                })?);
            }
            other => {
                return Err(DecodeError::InvalidField {
                    field: "bead_patch",
                    reason: format!("unknown key {other}"),
                });
            }
        }
    }

    if !id_set {
        return Err(DecodeError::MissingField("id"));
    }
    Ok(patch)
}

fn encode_wire_note(enc: &mut Encoder<&mut Vec<u8>>, note: &WireNoteV1) -> Result<(), EncodeError> {
    enc.map(4)?;
    enc.str("at")?;
    encode_wire_stamp(enc, &note.at)?;
    enc.str("author")?;
    enc.str(note.author.as_str())?;
    enc.str("content")?;
    enc.str(&note.content)?;
    enc.str("id")?;
    enc.str(note.id.as_str())?;
    Ok(())
}

fn decode_wire_note(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<WireNoteV1, DecodeError> {
    let map_len = decode_map_len(dec, limits, depth)?;
    let mut seen_keys = BTreeSet::new();
    let mut id = None;
    let mut content = None;
    let mut author = None;
    let mut at = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        ensure_unique_key(&mut seen_keys, key)?;
        match key {
            "at" => {
                at = Some(decode_wire_stamp(dec, limits, depth + 1)?);
            }
            "author" => {
                let raw = decode_text(dec, limits)?;
                author = Some(parse_actor_id(raw, "author")?);
            }
            "content" => {
                content = Some(decode_text(dec, limits)?.to_string());
            }
            "id" => {
                let raw = decode_text(dec, limits)?;
                id = Some(parse_note_id(raw)?);
            }
            other => {
                return Err(DecodeError::InvalidField {
                    field: "note",
                    reason: format!("unknown key {other}"),
                });
            }
        }
    }

    let note = WireNoteV1 {
        id: id.ok_or(DecodeError::MissingField("id"))?,
        content: content.ok_or(DecodeError::MissingField("content"))?,
        author: author.ok_or(DecodeError::MissingField("author"))?,
        at: at.ok_or(DecodeError::MissingField("at"))?,
    };

    if note.content.len() > limits.max_note_bytes {
        return Err(DecodeError::DecodeLimit("max_note_bytes"));
    }

    Ok(note)
}

fn encode_wire_tombstone(
    enc: &mut Encoder<&mut Vec<u8>>,
    tombstone: &WireTombstoneV1,
) -> Result<(), EncodeError> {
    let mut len = 3;
    if tombstone.reason.is_some() {
        len += 1;
    }
    let has_lineage =
        tombstone.lineage_created_at.is_some() || tombstone.lineage_created_by.is_some();
    if has_lineage {
        len += 2;
    }

    enc.map(len as u64)?;
    enc.str("id")?;
    enc.str(tombstone.id.as_str())?;
    enc.str("deleted_at")?;
    encode_wire_stamp(enc, &tombstone.deleted_at)?;
    enc.str("deleted_by")?;
    enc.str(tombstone.deleted_by.as_str())?;

    if let Some(reason) = &tombstone.reason {
        enc.str("reason")?;
        enc.str(reason)?;
    }

    if let (Some(at), Some(by)) = (
        tombstone.lineage_created_at,
        tombstone.lineage_created_by.as_ref(),
    ) {
        enc.str("lineage_created_at")?;
        encode_wire_stamp(enc, &at)?;
        enc.str("lineage_created_by")?;
        enc.str(by.as_str())?;
    } else {
        debug_assert!(
            tombstone.lineage_created_at.is_none() && tombstone.lineage_created_by.is_none(),
            "lineage fields must be set together"
        );
    }

    Ok(())
}

fn decode_wire_tombstone(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<WireTombstoneV1, DecodeError> {
    let map_len = decode_map_len(dec, limits, depth)?;
    let mut seen_keys = BTreeSet::new();
    let mut id = None;
    let mut deleted_at = None;
    let mut deleted_by = None;
    let mut reason = None;
    let mut lineage_created_at = None;
    let mut lineage_created_by = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        ensure_unique_key(&mut seen_keys, key)?;
        match key {
            "id" => {
                let raw = decode_text(dec, limits)?;
                id = Some(parse_bead_id(raw)?);
            }
            "deleted_at" => {
                deleted_at = Some(decode_wire_stamp(dec, limits, depth + 1)?);
            }
            "deleted_by" => {
                let raw = decode_text(dec, limits)?;
                deleted_by = Some(parse_actor_id(raw, "deleted_by")?);
            }
            "reason" => {
                reason = Some(decode_text(dec, limits)?.to_string());
            }
            "lineage_created_at" => {
                lineage_created_at = Some(decode_wire_stamp(dec, limits, depth + 1)?);
            }
            "lineage_created_by" => {
                let raw = decode_text(dec, limits)?;
                lineage_created_by = Some(parse_actor_id(raw, "lineage_created_by")?);
            }
            other => {
                return Err(DecodeError::InvalidField {
                    field: "bead_delete",
                    reason: format!("unknown key {other}"),
                });
            }
        }
    }

    if lineage_created_at.is_some() ^ lineage_created_by.is_some() {
        return Err(DecodeError::InvalidField {
            field: "bead_delete",
            reason: "lineage_created_at and lineage_created_by must be set together".into(),
        });
    }

    Ok(WireTombstoneV1 {
        id: id.ok_or(DecodeError::MissingField("id"))?,
        deleted_at: deleted_at.ok_or(DecodeError::MissingField("deleted_at"))?,
        deleted_by: deleted_by.ok_or(DecodeError::MissingField("deleted_by"))?,
        reason,
        lineage_created_at,
        lineage_created_by,
    })
}

fn encode_wire_dep(enc: &mut Encoder<&mut Vec<u8>>, dep: &WireDepV1) -> Result<(), EncodeError> {
    let mut len = 5;
    let has_deleted = dep.deleted_at.is_some() || dep.deleted_by.is_some();
    if has_deleted {
        len += 2;
    }

    enc.map(len as u64)?;
    enc.str("from")?;
    enc.str(dep.from.as_str())?;
    enc.str("to")?;
    enc.str(dep.to.as_str())?;
    enc.str("kind")?;
    enc.str(dep.kind.as_str())?;
    enc.str("created_at")?;
    encode_wire_stamp(enc, &dep.created_at)?;
    enc.str("created_by")?;
    enc.str(dep.created_by.as_str())?;

    if let (Some(at), Some(by)) = (dep.deleted_at, dep.deleted_by.as_ref()) {
        enc.str("deleted_at")?;
        encode_wire_stamp(enc, &at)?;
        enc.str("deleted_by")?;
        enc.str(by.as_str())?;
    } else {
        debug_assert!(
            dep.deleted_at.is_none() && dep.deleted_by.is_none(),
            "deleted fields must be set together"
        );
    }

    Ok(())
}

fn decode_wire_dep(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<WireDepV1, DecodeError> {
    let map_len = decode_map_len(dec, limits, depth)?;
    let mut seen_keys = BTreeSet::new();
    let mut from = None;
    let mut to = None;
    let mut kind = None;
    let mut created_at = None;
    let mut created_by = None;
    let mut deleted_at = None;
    let mut deleted_by = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        ensure_unique_key(&mut seen_keys, key)?;
        match key {
            "from" => {
                let raw = decode_text(dec, limits)?;
                from = Some(parse_bead_id(raw)?);
            }
            "to" => {
                let raw = decode_text(dec, limits)?;
                to = Some(parse_bead_id(raw)?);
            }
            "kind" => {
                let raw = decode_text(dec, limits)?;
                kind = Some(parse_dep_kind(raw)?);
            }
            "created_at" => {
                created_at = Some(decode_wire_stamp(dec, limits, depth + 1)?);
            }
            "created_by" => {
                let raw = decode_text(dec, limits)?;
                created_by = Some(parse_actor_id(raw, "created_by")?);
            }
            "deleted_at" => {
                deleted_at = Some(decode_wire_stamp(dec, limits, depth + 1)?);
            }
            "deleted_by" => {
                let raw = decode_text(dec, limits)?;
                deleted_by = Some(parse_actor_id(raw, "deleted_by")?);
            }
            other => {
                return Err(DecodeError::InvalidField {
                    field: "dep_upsert",
                    reason: format!("unknown key {other}"),
                });
            }
        }
    }

    if deleted_at.is_some() ^ deleted_by.is_some() {
        return Err(DecodeError::InvalidField {
            field: "dep_upsert",
            reason: "deleted_at and deleted_by must be set together".into(),
        });
    }

    Ok(WireDepV1 {
        from: from.ok_or(DecodeError::MissingField("from"))?,
        to: to.ok_or(DecodeError::MissingField("to"))?,
        kind: kind.ok_or(DecodeError::MissingField("kind"))?,
        created_at: created_at.ok_or(DecodeError::MissingField("created_at"))?,
        created_by: created_by.ok_or(DecodeError::MissingField("created_by"))?,
        deleted_at,
        deleted_by,
    })
}

fn encode_wire_dep_delete(
    enc: &mut Encoder<&mut Vec<u8>>,
    dep: &WireDepDeleteV1,
) -> Result<(), EncodeError> {
    enc.map(5)?;
    enc.str("from")?;
    enc.str(dep.from.as_str())?;
    enc.str("to")?;
    enc.str(dep.to.as_str())?;
    enc.str("kind")?;
    enc.str(dep.kind.as_str())?;
    enc.str("deleted_at")?;
    encode_wire_stamp(enc, &dep.deleted_at)?;
    enc.str("deleted_by")?;
    enc.str(dep.deleted_by.as_str())?;
    Ok(())
}

fn decode_wire_dep_delete(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<WireDepDeleteV1, DecodeError> {
    let map_len = decode_map_len(dec, limits, depth)?;
    let mut seen_keys = BTreeSet::new();
    let mut from = None;
    let mut to = None;
    let mut kind = None;
    let mut deleted_at = None;
    let mut deleted_by = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        ensure_unique_key(&mut seen_keys, key)?;
        match key {
            "from" => {
                let raw = decode_text(dec, limits)?;
                from = Some(parse_bead_id(raw)?);
            }
            "to" => {
                let raw = decode_text(dec, limits)?;
                to = Some(parse_bead_id(raw)?);
            }
            "kind" => {
                let raw = decode_text(dec, limits)?;
                kind = Some(parse_dep_kind(raw)?);
            }
            "deleted_at" => {
                deleted_at = Some(decode_wire_stamp(dec, limits, depth + 1)?);
            }
            "deleted_by" => {
                let raw = decode_text(dec, limits)?;
                deleted_by = Some(parse_actor_id(raw, "deleted_by")?);
            }
            other => {
                return Err(DecodeError::InvalidField {
                    field: "dep_delete",
                    reason: format!("unknown key {other}"),
                });
            }
        }
    }

    Ok(WireDepDeleteV1 {
        from: from.ok_or(DecodeError::MissingField("from"))?,
        to: to.ok_or(DecodeError::MissingField("to"))?,
        kind: kind.ok_or(DecodeError::MissingField("kind"))?,
        deleted_at: deleted_at.ok_or(DecodeError::MissingField("deleted_at"))?,
        deleted_by: deleted_by.ok_or(DecodeError::MissingField("deleted_by"))?,
    })
}

fn encode_wire_stamp(
    enc: &mut Encoder<&mut Vec<u8>>,
    stamp: &WireStamp,
) -> Result<(), EncodeError> {
    enc.array(2)?;
    enc.u64(stamp.0)?;
    enc.u32(stamp.1)?;
    Ok(())
}

fn decode_wire_stamp(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<WireStamp, DecodeError> {
    let arr_len = decode_array_len(dec, limits, depth)?;
    if arr_len != 2 {
        return Err(DecodeError::InvalidField {
            field: "stamp",
            reason: "expected array length 2".into(),
        });
    }
    let wall_ms = decode_u64(dec, "stamp.wall_ms")?;
    let counter = decode_u32(dec, "stamp.counter")?;
    Ok(WireStamp(wall_ms, counter))
}

fn encode_wire_patch_str(
    enc: &mut Encoder<&mut Vec<u8>>,
    patch: &WirePatch<String>,
) -> Result<(), EncodeError> {
    match patch {
        WirePatch::Keep => Ok(()),
        WirePatch::Clear => {
            enc.null()?;
            Ok(())
        }
        WirePatch::Set(value) => {
            enc.str(value)?;
            Ok(())
        }
    }
}

fn encode_wire_patch_u32(
    enc: &mut Encoder<&mut Vec<u8>>,
    patch: &WirePatch<u32>,
) -> Result<(), EncodeError> {
    match patch {
        WirePatch::Keep => Ok(()),
        WirePatch::Clear => {
            enc.null()?;
            Ok(())
        }
        WirePatch::Set(value) => {
            enc.u32(*value)?;
            Ok(())
        }
    }
}

fn encode_wire_patch_wallclock(
    enc: &mut Encoder<&mut Vec<u8>>,
    patch: &WirePatch<super::time::WallClock>,
) -> Result<(), EncodeError> {
    match patch {
        WirePatch::Keep => Ok(()),
        WirePatch::Clear => {
            enc.null()?;
            Ok(())
        }
        WirePatch::Set(value) => {
            enc.u64(value.0)?;
            Ok(())
        }
    }
}

fn encode_wire_patch_actor(
    enc: &mut Encoder<&mut Vec<u8>>,
    patch: &WirePatch<ActorId>,
) -> Result<(), EncodeError> {
    match patch {
        WirePatch::Keep => Ok(()),
        WirePatch::Clear => {
            enc.null()?;
            Ok(())
        }
        WirePatch::Set(value) => {
            enc.str(value.as_str())?;
            Ok(())
        }
    }
}

fn decode_wire_patch_str(
    dec: &mut Decoder,
    limits: &Limits,
) -> Result<WirePatch<String>, DecodeError> {
    match dec.datatype()? {
        Type::Null => {
            dec.null()?;
            Ok(WirePatch::Clear)
        }
        Type::StringIndef => Err(DecodeError::IndefiniteLength),
        Type::String => Ok(WirePatch::Set(decode_text(dec, limits)?.to_string())),
        other => Err(DecodeError::InvalidField {
            field: "patch",
            reason: format!("expected null or string, got {other:?}"),
        }),
    }
}

fn decode_wire_patch_u32(
    dec: &mut Decoder,
    _limits: &Limits,
) -> Result<WirePatch<u32>, DecodeError> {
    match dec.datatype()? {
        Type::Null => {
            dec.null()?;
            Ok(WirePatch::Clear)
        }
        _ => Ok(WirePatch::Set(decode_u32(dec, "patch")?)),
    }
}

fn decode_wire_patch_wallclock(
    dec: &mut Decoder,
    _limits: &Limits,
) -> Result<WirePatch<super::time::WallClock>, DecodeError> {
    match dec.datatype()? {
        Type::Null => {
            dec.null()?;
            Ok(WirePatch::Clear)
        }
        _ => Ok(WirePatch::Set(super::time::WallClock(decode_u64(
            dec,
            "patch.wallclock",
        )?))),
    }
}

fn decode_wire_patch_actor(
    dec: &mut Decoder,
    limits: &Limits,
) -> Result<WirePatch<ActorId>, DecodeError> {
    match dec.datatype()? {
        Type::Null => {
            dec.null()?;
            Ok(WirePatch::Clear)
        }
        Type::StringIndef => Err(DecodeError::IndefiniteLength),
        Type::String => {
            let raw = decode_text(dec, limits)?;
            Ok(WirePatch::Set(parse_actor_id(raw, "assignee")?))
        }
        other => Err(DecodeError::InvalidField {
            field: "patch",
            reason: format!("expected null or string, got {other:?}"),
        }),
    }
}

fn encode_hlc_max(enc: &mut Encoder<&mut Vec<u8>>, hlc: &HlcMax) -> Result<(), EncodeError> {
    enc.map(3)?;
    enc.str("actor_id")?;
    enc.str(hlc.actor_id.as_str())?;
    enc.str("logical")?;
    enc.u32(hlc.logical)?;
    enc.str("physical_ms")?;
    enc.u64(hlc.physical_ms)?;
    Ok(())
}

fn decode_hlc_max(dec: &mut Decoder, limits: &Limits, depth: usize) -> Result<HlcMax, DecodeError> {
    let map_len = decode_map_len(dec, limits, depth)?;
    let mut seen_keys = BTreeSet::new();
    let mut actor_id = None;
    let mut logical = None;
    let mut physical_ms = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        ensure_unique_key(&mut seen_keys, key)?;
        match key {
            "actor_id" => {
                let raw = decode_text(dec, limits)?;
                actor_id = Some(parse_actor_id(raw, "actor_id")?);
            }
            "logical" => {
                logical = Some(decode_u32(dec, "logical")?);
            }
            "physical_ms" => {
                physical_ms = Some(decode_u64(dec, "hlc_max.physical_ms")?);
            }
            _ => {
                skip_value(dec, limits, depth + 1)?;
            }
        }
    }

    Ok(HlcMax {
        actor_id: actor_id.ok_or(DecodeError::MissingField("actor_id"))?,
        physical_ms: physical_ms.ok_or(DecodeError::MissingField("physical_ms"))?,
        logical: logical.ok_or(DecodeError::MissingField("logical"))?,
    })
}

fn ensure_unique_key<'a>(seen: &mut BTreeSet<&'a str>, key: &'a str) -> Result<(), DecodeError> {
    if seen.insert(key) {
        Ok(())
    } else {
        Err(DecodeError::DuplicateKey(key.to_string()))
    }
}

fn decode_map_len(dec: &mut Decoder, limits: &Limits, depth: usize) -> Result<usize, DecodeError> {
    ensure_depth(depth, limits)?;
    let first = current_byte(dec)?;
    let len = dec.map()?;
    let Some(len) = len else {
        return Err(DecodeError::IndefiniteLength);
    };
    if !canonical_len(first, len, 0xa0) {
        return Err(non_canonical_integer("map_len"));
    }
    if len > limits.max_cbor_map_entries as u64 {
        return Err(DecodeError::DecodeLimit("max_cbor_map_entries"));
    }
    usize::try_from(len).map_err(|_| DecodeError::DecodeLimit("max_cbor_map_entries"))
}

fn decode_array_len(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<usize, DecodeError> {
    ensure_depth(depth, limits)?;
    let first = current_byte(dec)?;
    let len = dec.array()?;
    let Some(len) = len else {
        return Err(DecodeError::IndefiniteLength);
    };
    if !canonical_len(first, len, 0x80) {
        return Err(non_canonical_integer("array_len"));
    }
    if len > limits.max_cbor_array_entries as u64 {
        return Err(DecodeError::DecodeLimit("max_cbor_array_entries"));
    }
    usize::try_from(len).map_err(|_| DecodeError::DecodeLimit("max_cbor_array_entries"))
}

fn skip_value(dec: &mut Decoder, limits: &Limits, depth: usize) -> Result<(), DecodeError> {
    let ty = dec.datatype()?;
    match ty {
        Type::Bool => {
            let _ = dec.bool()?;
        }
        Type::Null => {
            dec.null()?;
        }
        Type::Undefined => {
            dec.undefined()?;
        }
        Type::U8
        | Type::U16
        | Type::U32
        | Type::U64
        | Type::I8
        | Type::I16
        | Type::I32
        | Type::I64
        | Type::Int => {
            decode_canonical_int(dec, "cbor")?;
        }
        Type::F16 | Type::F32 | Type::F64 => {
            let _ = dec.f64()?;
        }
        Type::Simple => {
            let _ = dec.simple()?;
        }
        Type::Bytes => {
            let _ = decode_bytes(dec, limits)?;
        }
        Type::String => {
            let _ = decode_text(dec, limits)?;
        }
        Type::BytesIndef | Type::StringIndef | Type::ArrayIndef | Type::MapIndef | Type::Break => {
            return Err(DecodeError::IndefiniteLength);
        }
        Type::Array => {
            let len = decode_array_len(dec, limits, depth + 1)?;
            for _ in 0..len {
                skip_value(dec, limits, depth + 1)?;
            }
        }
        Type::Map => {
            let len = decode_map_len(dec, limits, depth + 1)?;
            for _ in 0..len {
                skip_value(dec, limits, depth + 1)?;
                skip_value(dec, limits, depth + 1)?;
            }
        }
        Type::Tag => {
            return Err(DecodeError::InvalidField {
                field: "cbor",
                reason: "tags not allowed".into(),
            });
        }
        Type::Unknown(_) => {
            return Err(minicbor::decode::Error::message(format!("unknown cbor type {ty}")).into());
        }
    }
    Ok(())
}

fn decode_text<'a>(dec: &mut Decoder<'a>, limits: &Limits) -> Result<&'a str, DecodeError> {
    let ty = dec.datatype()?;
    if matches!(ty, Type::StringIndef) {
        return Err(DecodeError::IndefiniteLength);
    }
    let s = dec.str()?;
    if s.len() > limits.max_cbor_text_string_len {
        return Err(DecodeError::DecodeLimit("max_cbor_text_string_len"));
    }
    Ok(s)
}

fn decode_bytes<'a>(dec: &mut Decoder<'a>, limits: &Limits) -> Result<&'a [u8], DecodeError> {
    let ty = dec.datatype()?;
    if matches!(ty, Type::BytesIndef) {
        return Err(DecodeError::IndefiniteLength);
    }
    let bytes = dec.bytes()?;
    if bytes.len() > limits.max_cbor_bytes_string_len {
        return Err(DecodeError::DecodeLimit("max_cbor_bytes_string_len"));
    }
    Ok(bytes)
}

fn current_byte(dec: &Decoder) -> Result<u8, DecodeError> {
    dec.input()
        .get(dec.position())
        .copied()
        .ok_or_else(|| minicbor::decode::Error::end_of_input().into())
}

fn canonical_unsigned(first: u8, value: u64) -> bool {
    match value {
        0..=23 => first == value as u8,
        24..=0xff => first == 0x18,
        0x100..=0xffff => first == 0x19,
        0x1_0000..=0xffff_ffff => first == 0x1a,
        _ => first == 0x1b,
    }
}

fn canonical_negative(first: u8, n: u64) -> bool {
    match n {
        0..=23 => first == 0x20 + n as u8,
        24..=0xff => first == 0x38,
        0x100..=0xffff => first == 0x39,
        0x1_0000..=0xffff_ffff => first == 0x3a,
        _ => first == 0x3b,
    }
}

fn canonical_len(first: u8, len: u64, base: u8) -> bool {
    match len {
        0..=23 => first == base + len as u8,
        24..=0xff => first == base + 24,
        0x100..=0xffff => first == base + 25,
        0x1_0000..=0xffff_ffff => first == base + 26,
        _ => first == base + 27,
    }
}

fn non_canonical_integer(field: &'static str) -> DecodeError {
    DecodeError::InvalidField {
        field,
        reason: "non-canonical integer encoding".into(),
    }
}

fn decode_u64(dec: &mut Decoder, field: &'static str) -> Result<u64, DecodeError> {
    let first = current_byte(dec)?;
    match dec.datatype()? {
        Type::U8 | Type::U16 | Type::U32 | Type::U64 => {
            let value = dec.u64()?;
            if !canonical_unsigned(first, value) {
                return Err(non_canonical_integer(field));
            }
            Ok(value)
        }
        Type::Tag => Err(DecodeError::InvalidField {
            field,
            reason: "tagged integer not allowed".into(),
        }),
        other => Err(DecodeError::InvalidField {
            field,
            reason: format!("expected unsigned integer, got {other:?}"),
        }),
    }
}

fn decode_u32(dec: &mut Decoder, field: &'static str) -> Result<u32, DecodeError> {
    let value = decode_u64(dec, field)?;
    u32::try_from(value).map_err(|_| DecodeError::InvalidField {
        field,
        reason: format!("value {value} out of range for u32"),
    })
}

fn decode_canonical_int(dec: &mut Decoder, field: &'static str) -> Result<(), DecodeError> {
    let first = current_byte(dec)?;
    match dec.datatype()? {
        Type::U8
        | Type::U16
        | Type::U32
        | Type::U64
        | Type::I8
        | Type::I16
        | Type::I32
        | Type::I64
        | Type::Int => {
            let value = dec.int()?;
            let value = i128::from(value);
            if value >= 0 {
                let value = u64::try_from(value).expect("positive int fits in u64");
                if !canonical_unsigned(first, value) {
                    return Err(non_canonical_integer(field));
                }
            } else {
                let n = (-1i128 - value) as u64;
                if !canonical_negative(first, n) {
                    return Err(non_canonical_integer(field));
                }
            }
            Ok(())
        }
        Type::Tag => Err(DecodeError::InvalidField {
            field,
            reason: "tagged integer not allowed".into(),
        }),
        other => Err(DecodeError::InvalidField {
            field,
            reason: format!("expected integer, got {other:?}"),
        }),
    }
}

fn ensure_depth(depth: usize, limits: &Limits) -> Result<(), DecodeError> {
    if depth > limits.max_cbor_depth {
        return Err(DecodeError::DecodeLimit("max_cbor_depth"));
    }
    Ok(())
}

fn parse_uuid_field<T>(field: &'static str, raw: &str) -> Result<T, DecodeError>
where
    T: TryFrom<String>,
    T::Error: std::fmt::Display,
{
    T::try_from(raw.to_string()).map_err(|e| DecodeError::InvalidField {
        field,
        reason: e.to_string(),
    })
}

fn parse_namespace(raw: &str) -> Result<NamespaceId, DecodeError> {
    NamespaceId::parse(raw.to_string()).map_err(|e| DecodeError::InvalidField {
        field: "namespace",
        reason: e.to_string(),
    })
}

fn parse_actor_id(raw: &str, field: &'static str) -> Result<ActorId, DecodeError> {
    ActorId::new(raw.to_string()).map_err(|e| DecodeError::InvalidField {
        field,
        reason: e.to_string(),
    })
}

fn parse_bead_id(raw: &str) -> Result<super::identity::BeadId, DecodeError> {
    super::identity::BeadId::parse(raw).map_err(|e| DecodeError::InvalidField {
        field: "bead_id",
        reason: e.to_string(),
    })
}

fn parse_dep_kind(raw: &str) -> Result<DepKind, DecodeError> {
    DepKind::parse(raw).map_err(|e| DecodeError::InvalidField {
        field: "kind",
        reason: e.to_string(),
    })
}

fn parse_note_id(raw: &str) -> Result<super::identity::NoteId, DecodeError> {
    super::identity::NoteId::new(raw.to_string()).map_err(|e| DecodeError::InvalidField {
        field: "note_id",
        reason: e.to_string(),
    })
}

fn bead_id_placeholder() -> super::identity::BeadId {
    super::identity::BeadId::parse("bd-placeholder").unwrap()
}

pub fn validate_event_body_limits(
    body: &EventBody,
    limits: &Limits,
) -> Result<(), EventValidationError> {
    let delta = match &body.kind {
        EventKindV1::TxnV1(txn) => &txn.delta,
    };
    let ops = delta.total_ops();
    if ops > limits.max_ops_per_txn {
        return Err(EventValidationError::TooManyOps {
            ops,
            max: limits.max_ops_per_txn,
        });
    }

    let mut note_appends = 0usize;
    for op in delta.iter() {
        match op {
            TxnOpV1::BeadUpsert(patch) => {
                if let Some(labels) = &patch.labels
                    && labels.len() > limits.max_labels_per_bead
                {
                    return Err(EventValidationError::TooManyLabels {
                        count: labels.len(),
                        max: limits.max_labels_per_bead,
                    });
                }
                if let NotesPatch::AtLeast(notes) = &patch.notes {
                    for note in notes {
                        let bytes = note.content.len();
                        if bytes > limits.max_note_bytes {
                            return Err(EventValidationError::NoteTooLarge {
                                bytes,
                                max: limits.max_note_bytes,
                            });
                        }
                    }
                }
            }
            TxnOpV1::BeadDelete(_) => {}
            TxnOpV1::DepUpsert(_) => {}
            TxnOpV1::DepDelete(_) => {}
            TxnOpV1::NoteAppend(append) => {
                note_appends += 1;
                let bytes = append.note.content.len();
                if bytes > limits.max_note_bytes {
                    return Err(EventValidationError::NoteTooLarge {
                        bytes,
                        max: limits.max_note_bytes,
                    });
                }
            }
        }
    }

    if note_appends > limits.max_note_appends_per_txn {
        return Err(EventValidationError::TooManyNoteAppends {
            count: note_appends,
            max: limits.max_note_appends_per_txn,
        });
    }

    Ok(())
}

pub fn verify_event_frame(
    frame: &EventFrameV1,
    limits: &Limits,
    expected_store: StoreIdentity,
    expected_prev_head: Option<Sha256>,
    lookup: &dyn EventShaLookup,
) -> Result<VerifiedEventAny, EventFrameError> {
    let (_, body) = decode_event_body(frame.bytes.as_ref(), limits)?;

    if body.store != expected_store {
        return Err(EventFrameError::WrongStore {
            expected: expected_store,
            got: body.store,
        });
    }

    if body.origin_replica_id != frame.eid.origin_replica_id
        || body.namespace != frame.eid.namespace
        || body.origin_seq != frame.eid.origin_seq
    {
        return Err(EventFrameError::FrameMismatch);
    }

    let computed = hash_event_body(&frame.bytes);
    if computed != frame.sha256 {
        return Err(EventFrameError::HashMismatch);
    }

    validate_event_body_limits(&body, limits)?;

    match lookup.lookup_event_sha(&frame.eid)? {
        None => {}
        Some(existing) if existing == frame.sha256 => {}
        Some(_) => return Err(EventFrameError::Equivocation),
    }

    let seq = body.origin_seq.get();
    match (seq, frame.prev_sha256, expected_prev_head) {
        (1, None, _) => Ok(VerifiedEventAny::Contiguous(VerifiedEvent {
            body,
            bytes: frame.bytes.clone(),
            sha256: frame.sha256,
            prev: PrevVerified { prev: None },
        })),
        (1, Some(_), _) => Err(EventFrameError::PrevMismatch),
        (s, None, _) if s > 1 => Err(EventFrameError::PrevMismatch),
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
                .expect("seq > 1 must have predecessor");
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
        _ => Err(EventFrameError::PrevMismatch),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::identity::{BeadId, NoteId, StoreEpoch};
    use uuid::Uuid;

    fn actor_id(actor: &str) -> ActorId {
        ActorId::new(actor).unwrap()
    }

    fn txn(body: &EventBody) -> &TxnV1 {
        match &body.kind {
            EventKindV1::TxnV1(txn) => txn,
        }
    }

    fn txn_mut(body: &mut EventBody) -> &mut TxnV1 {
        match &mut body.kind {
            EventKindV1::TxnV1(txn) => txn,
        }
    }

    fn sample_body() -> EventBody {
        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::new(2));
        let origin = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let txn_id = TxnId::new(Uuid::from_bytes([3u8; 16]));
        let client_request_id = ClientRequestId::new(Uuid::from_bytes([4u8; 16]));
        let trace_id = TraceId::from(client_request_id);

        let mut patch = WireBeadPatch::new(BeadId::parse("bd-test1").unwrap());
        patch.created_at = Some(WireStamp(10, 1));
        patch.created_by = Some(actor_id("alice"));
        patch.title = Some("title".to_string());

        let mut delta = TxnDeltaV1::new();
        delta.insert(TxnOpV1::BeadUpsert(Box::new(patch))).unwrap();

        EventBody {
            envelope_v: 1,
            store,
            namespace: NamespaceId::core(),
            origin_replica_id: origin,
            origin_seq: Seq1::from_u64(1).unwrap(),
            event_time_ms: 123,
            txn_id,
            client_request_id: Some(client_request_id),
            trace_id: Some(trace_id),
            kind: EventKindV1::TxnV1(TxnV1 {
                delta,
                hlc_max: HlcMax {
                    actor_id: actor_id("alice"),
                    physical_ms: 123,
                    logical: 7,
                },
            }),
        }
    }

    fn sample_body_with_ops() -> EventBody {
        let mut body = sample_body();
        let txn = txn_mut(&mut body);
        txn.delta
            .insert(TxnOpV1::BeadDelete(WireTombstoneV1 {
                id: BeadId::parse("bd-delete").unwrap(),
                deleted_at: WireStamp(20, 1),
                deleted_by: actor_id("alice"),
                reason: Some("cleanup".to_string()),
                lineage_created_at: Some(WireStamp(1, 0)),
                lineage_created_by: Some(actor_id("bob")),
            }))
            .unwrap();
        txn.delta
            .insert(TxnOpV1::DepUpsert(WireDepV1 {
                from: BeadId::parse("bd-test1").unwrap(),
                to: BeadId::parse("bd-dep").unwrap(),
                kind: DepKind::Blocks,
                created_at: WireStamp(15, 0),
                created_by: actor_id("alice"),
                deleted_at: Some(WireStamp(18, 2)),
                deleted_by: Some(actor_id("carol")),
            }))
            .unwrap();
        txn.delta
            .insert(TxnOpV1::DepDelete(WireDepDeleteV1 {
                from: BeadId::parse("bd-test1").unwrap(),
                to: BeadId::parse("bd-dep2").unwrap(),
                kind: DepKind::Related,
                deleted_at: WireStamp(22, 0),
                deleted_by: actor_id("alice"),
            }))
            .unwrap();
        txn.delta
            .insert(TxnOpV1::NoteAppend(NoteAppendV1 {
                bead_id: BeadId::parse("bd-test1").unwrap(),
                note: WireNoteV1 {
                    id: NoteId::new("note-7".to_string()).unwrap(),
                    content: "note".to_string(),
                    author: actor_id("alice"),
                    at: WireStamp(13, 2),
                },
            }))
            .unwrap();
        body
    }

    fn encode_body_with_unknown_fields(body: &EventBody) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut enc = Encoder::new(&mut buf);

        let mut len = 9;
        if body.client_request_id.is_some() {
            len += 1;
        }
        if body.trace_id.is_some() {
            len += 1;
        }
        len += 2;
        len += 1;

        enc.map(len as u64).unwrap();

        enc.str("future_field").unwrap();
        enc.map(1).unwrap();
        enc.str("nested").unwrap();
        enc.u64(42).unwrap();

        if let Some(client_request_id) = &body.client_request_id {
            enc.str("client_request_id").unwrap();
            enc.str(&client_request_id.as_uuid().to_string()).unwrap();
        }
        if let Some(trace_id) = &body.trace_id {
            enc.str("trace_id").unwrap();
            enc.str(&trace_id.as_uuid().to_string()).unwrap();
        }

        let txn = txn(body);
        enc.str("delta").unwrap();
        encode_txn_delta(&mut enc, &txn.delta).unwrap();

        enc.str("envelope_v").unwrap();
        enc.u32(body.envelope_v).unwrap();

        enc.str("event_time_ms").unwrap();
        enc.u64(body.event_time_ms).unwrap();

        enc.str("hlc_max").unwrap();
        enc.map(4).unwrap();
        enc.str("actor_id").unwrap();
        enc.str(txn.hlc_max.actor_id.as_str()).unwrap();
        enc.str("logical").unwrap();
        enc.u32(txn.hlc_max.logical).unwrap();
        enc.str("physical_ms").unwrap();
        enc.u64(txn.hlc_max.physical_ms).unwrap();
        enc.str("future_hlc").unwrap();
        enc.u64(999).unwrap();

        enc.str("kind").unwrap();
        enc.str(body.kind.as_str()).unwrap();

        enc.str("namespace").unwrap();
        enc.str(body.namespace.as_str()).unwrap();

        enc.str("origin_replica_id").unwrap();
        enc.str(&body.origin_replica_id.as_uuid().to_string())
            .unwrap();

        enc.str("origin_seq").unwrap();
        enc.u64(body.origin_seq.get()).unwrap();

        enc.str("store_epoch").unwrap();
        enc.u64(body.store.store_epoch.get()).unwrap();

        enc.str("store_id").unwrap();
        enc.str(&body.store.store_id.as_uuid().to_string()).unwrap();

        enc.str("txn_id").unwrap();
        enc.str(&body.txn_id.as_uuid().to_string()).unwrap();

        buf
    }

    fn append_duplicate_map_entry<F>(mut bytes: Vec<u8>, key: &str, encode_value: F) -> Vec<u8>
    where
        F: FnOnce(&mut Encoder<&mut Vec<u8>>),
    {
        let header = bytes[0];
        assert!((0xa0..=0xb7).contains(&header), "expected small map header");
        let len = header - 0xa0;
        assert!(len < 23, "map too large for duplicate helper");
        bytes[0] = 0xa0 + len + 1;

        let mut extra = Vec::new();
        let mut enc = Encoder::new(&mut extra);
        enc.str(key).unwrap();
        encode_value(&mut enc);
        bytes.extend(extra);
        bytes
    }

    fn encode_body_with_custom_delta_and_hlc<F, G>(
        body: &EventBody,
        encode_delta: F,
        encode_hlc: G,
    ) -> Vec<u8>
    where
        F: FnOnce(&mut Encoder<&mut Vec<u8>>),
        G: FnOnce(&mut Encoder<&mut Vec<u8>>),
    {
        let mut buf = Vec::new();
        let mut enc = Encoder::new(&mut buf);

        let mut len = 10;
        if body.client_request_id.is_some() {
            len += 1;
        }
        if body.trace_id.is_some() {
            len += 1;
        }
        len += 1;

        enc.map(len as u64).unwrap();

        if let Some(client_request_id) = &body.client_request_id {
            enc.str("client_request_id").unwrap();
            enc.str(&client_request_id.as_uuid().to_string()).unwrap();
        }
        if let Some(trace_id) = &body.trace_id {
            enc.str("trace_id").unwrap();
            enc.str(&trace_id.as_uuid().to_string()).unwrap();
        }

        enc.str("delta").unwrap();
        encode_delta(&mut enc);

        enc.str("envelope_v").unwrap();
        enc.u32(body.envelope_v).unwrap();

        enc.str("event_time_ms").unwrap();
        enc.u64(body.event_time_ms).unwrap();

        enc.str("hlc_max").unwrap();
        encode_hlc(&mut enc);

        enc.str("kind").unwrap();
        enc.str(body.kind.as_str()).unwrap();

        enc.str("namespace").unwrap();
        enc.str(body.namespace.as_str()).unwrap();

        enc.str("origin_replica_id").unwrap();
        enc.str(&body.origin_replica_id.as_uuid().to_string())
            .unwrap();

        enc.str("origin_seq").unwrap();
        enc.u64(body.origin_seq.get()).unwrap();

        enc.str("store_epoch").unwrap();
        enc.u64(body.store.store_epoch.get()).unwrap();

        enc.str("store_id").unwrap();
        enc.str(&body.store.store_id.as_uuid().to_string()).unwrap();

        enc.str("txn_id").unwrap();
        enc.str(&body.txn_id.as_uuid().to_string()).unwrap();

        buf
    }

    fn encode_body_without_hlc_max(body: &EventBody) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut enc = Encoder::new(&mut buf);

        let mut len = 10;
        if body.client_request_id.is_some() {
            len += 1;
        }
        if body.trace_id.is_some() {
            len += 1;
        }

        enc.map(len as u64).unwrap();

        if let Some(client_request_id) = &body.client_request_id {
            enc.str("client_request_id").unwrap();
            enc.str(&client_request_id.as_uuid().to_string()).unwrap();
        }
        if let Some(trace_id) = &body.trace_id {
            enc.str("trace_id").unwrap();
            enc.str(&trace_id.as_uuid().to_string()).unwrap();
        }

        enc.str("delta").unwrap();
        encode_txn_delta(&mut enc, &txn(body).delta).unwrap();

        enc.str("envelope_v").unwrap();
        enc.u32(body.envelope_v).unwrap();

        enc.str("event_time_ms").unwrap();
        enc.u64(body.event_time_ms).unwrap();

        enc.str("kind").unwrap();
        enc.str(body.kind.as_str()).unwrap();

        enc.str("namespace").unwrap();
        enc.str(body.namespace.as_str()).unwrap();

        enc.str("origin_replica_id").unwrap();
        enc.str(&body.origin_replica_id.as_uuid().to_string())
            .unwrap();

        enc.str("origin_seq").unwrap();
        enc.u64(body.origin_seq.get()).unwrap();

        enc.str("store_epoch").unwrap();
        enc.u64(body.store.store_epoch.get()).unwrap();

        enc.str("store_id").unwrap();
        enc.str(&body.store.store_id.as_uuid().to_string()).unwrap();

        enc.str("txn_id").unwrap();
        enc.str(&body.txn_id.as_uuid().to_string()).unwrap();

        buf
    }

    fn replace_value_bytes(
        mut bytes: Vec<u8>,
        key: &str,
        original_len: usize,
        replacement: &[u8],
    ) -> Vec<u8> {
        let key_bytes = key.as_bytes();
        assert!(
            key_bytes.len() <= 23,
            "key length must fit in single CBOR byte"
        );
        let mut marker = Vec::with_capacity(1 + key_bytes.len());
        marker.push(0x60 + key_bytes.len() as u8);
        marker.extend_from_slice(key_bytes);
        let Some(pos) = bytes.windows(marker.len()).position(|win| win == marker) else {
            panic!("key {key} not found in encoded bytes");
        };
        let value_pos = pos + marker.len();
        assert!(
            value_pos + original_len <= bytes.len(),
            "value bytes out of range"
        );
        bytes.splice(
            value_pos..value_pos + original_len,
            replacement.iter().copied(),
        );
        bytes
    }

    fn sample_body_with_seq(seq: u64) -> EventBody {
        let mut body = sample_body();
        body.origin_seq = Seq1::from_u64(seq).unwrap();
        body
    }

    fn sample_frame(body: &EventBody, prev_sha256: Option<Sha256>) -> EventFrameV1 {
        let bytes = encode_event_body_canonical(body).unwrap();
        let sha256 = hash_event_body(&bytes);
        EventFrameV1 {
            eid: EventId::new(
                body.origin_replica_id,
                body.namespace.clone(),
                body.origin_seq,
            ),
            sha256,
            prev_sha256,
            bytes: bytes.into(),
        }
    }

    struct MapLookup(std::collections::BTreeMap<EventId, Sha256>);

    impl EventShaLookup for MapLookup {
        fn lookup_event_sha(&self, eid: &EventId) -> Result<Option<Sha256>, EventShaLookupError> {
            Ok(self.0.get(eid).copied())
        }
    }

    #[test]
    fn canonical_encode_is_stable() {
        let body = sample_body();
        let bytes1 = encode_event_body_canonical(&body).unwrap();
        let bytes2 = encode_event_body_canonical(&body).unwrap();
        assert_eq!(bytes1.as_ref(), bytes2.as_ref());
        assert_eq!(hash_event_body(&bytes1), hash_event_body(&bytes2));
    }

    #[test]
    fn decode_roundtrip_event_body() {
        let body = sample_body();
        let encoded = encode_event_body_canonical(&body).unwrap();
        let (_, decoded) = decode_event_body(encoded.as_ref(), &Limits::default()).unwrap();
        assert_eq!(body, decoded);
    }

    #[test]
    fn decode_roundtrip_event_body_with_ops() {
        let body = sample_body_with_ops();
        let encoded = encode_event_body_canonical(&body).unwrap();
        let (_, decoded) = decode_event_body(encoded.as_ref(), &Limits::default()).unwrap();
        assert_eq!(body, decoded);
    }

    #[test]
    fn decode_accepts_unknown_event_body_fields() {
        let body = sample_body();
        let encoded = encode_body_with_unknown_fields(&body);
        let (_, decoded) = decode_event_body(encoded.as_ref(), &Limits::default()).unwrap();
        assert_eq!(body, decoded);
    }

    #[test]
    fn decode_rejects_duplicate_event_body_keys() {
        let body = sample_body();
        let encoded = encode_event_body_canonical(&body).unwrap();
        let bytes = append_duplicate_map_entry(encoded.as_ref().to_vec(), "namespace", |enc| {
            enc.str(body.namespace.as_str()).unwrap();
        });
        let err = decode_event_body(&bytes, &Limits::default()).unwrap_err();
        assert!(matches!(err, DecodeError::DuplicateKey(key) if key == "namespace"));
    }

    #[test]
    fn decode_rejects_duplicate_txn_delta_keys() {
        let body = sample_body();
        let txn = txn(&body);
        let bytes = encode_body_with_custom_delta_and_hlc(
            &body,
            |enc| {
                enc.map(2).unwrap();
                enc.str("v").unwrap();
                enc.u32(1).unwrap();
                enc.str("v").unwrap();
                enc.u32(1).unwrap();
            },
            |enc| {
                encode_hlc_max(enc, &txn.hlc_max).unwrap();
            },
        );
        let err = decode_event_body(&bytes, &Limits::default()).unwrap_err();
        assert!(matches!(err, DecodeError::DuplicateKey(key) if key == "v"));
    }

    #[test]
    fn decode_rejects_duplicate_hlc_max_keys() {
        let body = sample_body();
        let hlc = &txn(&body).hlc_max;
        let bytes = encode_body_with_custom_delta_and_hlc(
            &body,
            |enc| {
                encode_txn_delta(enc, &txn(&body).delta).unwrap();
            },
            |enc| {
                enc.map(4).unwrap();
                enc.str("actor_id").unwrap();
                enc.str(hlc.actor_id.as_str()).unwrap();
                enc.str("logical").unwrap();
                enc.u32(hlc.logical).unwrap();
                enc.str("physical_ms").unwrap();
                enc.u64(hlc.physical_ms).unwrap();
                enc.str("logical").unwrap();
                enc.u32(hlc.logical).unwrap();
            },
        );
        let err = decode_event_body(&bytes, &Limits::default()).unwrap_err();
        assert!(matches!(err, DecodeError::DuplicateKey(key) if key == "logical"));
    }

    #[test]
    fn decode_rejects_duplicate_bead_patch_keys() {
        let body = sample_body();
        let txn = txn(&body);
        let bytes = encode_body_with_custom_delta_and_hlc(
            &body,
            |enc| {
                enc.map(2).unwrap();
                enc.str("bead_upserts").unwrap();
                enc.array(1).unwrap();
                enc.map(1).unwrap();
                enc.str("bead").unwrap();
                enc.map(2).unwrap();
                enc.str("id").unwrap();
                enc.str("bd-test1").unwrap();
                enc.str("id").unwrap();
                enc.str("bd-test1").unwrap();
                enc.str("v").unwrap();
                enc.u32(1).unwrap();
            },
            |enc| {
                encode_hlc_max(enc, &txn.hlc_max).unwrap();
            },
        );
        let err = decode_event_body(&bytes, &Limits::default()).unwrap_err();
        assert!(matches!(err, DecodeError::DuplicateKey(key) if key == "id"));
    }

    #[test]
    fn decode_rejects_duplicate_note_keys() {
        let body = sample_body();
        let txn = txn(&body);
        let bytes = encode_body_with_custom_delta_and_hlc(
            &body,
            |enc| {
                enc.map(2).unwrap();
                enc.str("note_appends").unwrap();
                enc.array(1).unwrap();
                enc.map(2).unwrap();
                enc.str("bead_id").unwrap();
                enc.str("bd-test1").unwrap();
                enc.str("note").unwrap();
                enc.map(5).unwrap();
                enc.str("at").unwrap();
                encode_wire_stamp(enc, &WireStamp(10, 1)).unwrap();
                enc.str("author").unwrap();
                enc.str(actor_id("alice").as_str()).unwrap();
                enc.str("content").unwrap();
                enc.str("note").unwrap();
                enc.str("id").unwrap();
                enc.str("note-1").unwrap();
                enc.str("id").unwrap();
                enc.str("note-1").unwrap();
                enc.str("v").unwrap();
                enc.u32(1).unwrap();
            },
            |enc| {
                encode_hlc_max(enc, &txn.hlc_max).unwrap();
            },
        );
        let err = decode_event_body(&bytes, &Limits::default()).unwrap_err();
        assert!(matches!(err, DecodeError::DuplicateKey(key) if key == "id"));
    }

    #[test]
    fn decode_rejects_overlong_u32() {
        let body = sample_body();
        let encoded = encode_event_body_canonical(&body).unwrap();
        let bytes = replace_value_bytes(encoded.as_ref().to_vec(), "envelope_v", 1, &[0x18, 0x01]);
        let err = decode_event_body(&bytes, &Limits::default()).unwrap_err();
        assert!(matches!(
            err,
            DecodeError::InvalidField {
                field: "envelope_v",
                ..
            }
        ));
    }

    #[test]
    fn decode_rejects_overlong_u64() {
        let body = sample_body();
        let encoded = encode_event_body_canonical(&body).unwrap();
        let bytes = replace_value_bytes(
            encoded.as_ref().to_vec(),
            "event_time_ms",
            2,
            &[0x19, 0x00, 0x7b],
        );
        let err = decode_event_body(&bytes, &Limits::default()).unwrap_err();
        assert!(matches!(
            err,
            DecodeError::InvalidField {
                field: "event_time_ms",
                ..
            }
        ));
    }

    #[test]
    fn decode_rejects_tagged_integer() {
        let body = sample_body();
        let encoded = encode_event_body_canonical(&body).unwrap();
        let bytes = replace_value_bytes(encoded.as_ref().to_vec(), "envelope_v", 1, &[0xc0, 0x01]);
        let err = decode_event_body(&bytes, &Limits::default()).unwrap_err();
        assert!(matches!(
            err,
            DecodeError::InvalidField {
                field: "envelope_v",
                ..
            }
        ));
    }

    #[test]
    fn decode_rejects_hlc_physical_mismatch() {
        let mut body = sample_body();
        let event_time_ms = body.event_time_ms;
        let hlc_max = &mut txn_mut(&mut body).hlc_max;
        hlc_max.physical_ms = event_time_ms + 1;
        let encoded = encode_event_body_canonical(&body).unwrap();
        let err = decode_event_body(encoded.as_ref(), &Limits::default()).unwrap_err();
        assert!(matches!(
            err,
            DecodeError::InvalidField {
                field: "hlc_max.physical_ms",
                ..
            }
        ));
    }

    #[test]
    fn decode_event_hlc_max_rejects_physical_mismatch() {
        let mut body = sample_body();
        let event_time_ms = body.event_time_ms;
        let hlc_max = &mut txn_mut(&mut body).hlc_max;
        hlc_max.physical_ms = event_time_ms + 1;
        let encoded = encode_event_body_canonical(&body).unwrap();
        let err = decode_event_hlc_max(encoded.as_ref(), &Limits::default()).unwrap_err();
        assert!(matches!(
            err,
            DecodeError::InvalidField {
                field: "hlc_max.physical_ms",
                ..
            }
        ));
    }

    #[test]
    fn decode_event_hlc_max_extracts() {
        let body = sample_body();
        let encoded = encode_event_body_canonical(&body).unwrap();
        let hlc = decode_event_hlc_max(encoded.as_ref(), &Limits::default()).unwrap();
        assert_eq!(hlc, Some(txn(&body).hlc_max.clone()));
    }

    #[test]
    fn decode_rejects_missing_hlc_max_for_txn() {
        let body = sample_body();
        let encoded = encode_body_without_hlc_max(&body);
        let err = decode_event_body(encoded.as_ref(), &Limits::default()).unwrap_err();
        assert!(matches!(err, DecodeError::MissingField("hlc_max")));
    }

    #[test]
    fn decode_rejects_indefinite_length() {
        let limits = Limits::default();
        let bytes = [0xbf, 0xff];
        let err = decode_event_body(&bytes, &limits).unwrap_err();
        assert!(matches!(err, DecodeError::IndefiniteLength));
    }

    #[test]
    fn decode_rejects_text_bounds() {
        let limits = Limits {
            max_cbor_text_string_len: 3,
            ..Default::default()
        };
        let body = sample_body();
        let encoded = encode_event_body_canonical(&body).unwrap();
        let err = decode_event_body(encoded.as_ref(), &limits).unwrap_err();
        assert!(matches!(
            err,
            DecodeError::DecodeLimit("max_cbor_text_string_len")
        ));
    }

    #[test]
    fn decode_rejects_map_entry_bounds() {
        let limits = Limits {
            max_cbor_map_entries: 1,
            ..Default::default()
        };
        let body = sample_body();
        let encoded = encode_event_body_canonical(&body).unwrap();
        let err = decode_event_body(encoded.as_ref(), &limits).unwrap_err();
        assert!(matches!(
            err,
            DecodeError::DecodeLimit("max_cbor_map_entries")
        ));
    }

    #[test]
    fn decode_rejects_array_entry_bounds() {
        let limits = Limits {
            max_cbor_array_entries: 0,
            ..Default::default()
        };
        let body = sample_body();
        let encoded = encode_event_body_canonical(&body).unwrap();
        let err = decode_event_body(encoded.as_ref(), &limits).unwrap_err();
        assert!(matches!(
            err,
            DecodeError::DecodeLimit("max_cbor_array_entries")
        ));
    }

    #[test]
    fn decode_rejects_depth_bounds() {
        let limits = Limits {
            max_cbor_depth: 0,
            ..Default::default()
        };
        let body = sample_body();
        let encoded = encode_event_body_canonical(&body).unwrap();
        let err = decode_event_body(encoded.as_ref(), &limits).unwrap_err();
        assert!(matches!(err, DecodeError::DecodeLimit("max_cbor_depth")));
    }

    #[test]
    fn decode_rejects_record_size_bounds() {
        let body = sample_body();
        let encoded = encode_event_body_canonical(&body).unwrap();
        let limits = Limits {
            max_wal_record_bytes: encoded.as_ref().len() - 1,
            ..Default::default()
        };
        let err = decode_event_body(encoded.as_ref(), &limits).unwrap_err();
        assert!(matches!(
            err,
            DecodeError::DecodeLimit("max_wal_record_bytes")
        ));
    }

    #[test]
    fn verify_event_frame_defers_prev_when_head_unknown() {
        let limits = Limits::default();
        let first = sample_body_with_seq(1);
        let first_frame = sample_frame(&first, None);
        let second = sample_body_with_seq(2);
        let second_frame = sample_frame(&second, Some(first_frame.sha256));
        let lookup = MapLookup(std::collections::BTreeMap::new());

        let verified =
            verify_event_frame(&second_frame, &limits, second.store, None, &lookup).unwrap();

        match verified {
            VerifiedEventAny::Deferred(ev) => {
                assert_eq!(ev.prev.prev, first_frame.sha256);
                assert_eq!(ev.prev.expected_prev_seq, Seq1::from_u64(1).unwrap());
            }
            other => panic!("expected deferred, got {other:?}"),
        }
    }

    #[test]
    fn verify_event_frame_accepts_contiguous_prev() {
        let limits = Limits::default();
        let first = sample_body_with_seq(1);
        let first_frame = sample_frame(&first, None);
        let second = sample_body_with_seq(2);
        let second_frame = sample_frame(&second, Some(first_frame.sha256));
        let lookup = MapLookup(std::collections::BTreeMap::new());

        let verified = verify_event_frame(
            &second_frame,
            &limits,
            second.store,
            Some(first_frame.sha256),
            &lookup,
        )
        .unwrap();

        match verified {
            VerifiedEventAny::Contiguous(ev) => {
                assert_eq!(ev.prev.prev, Some(first_frame.sha256));
                assert_eq!(ev.seq(), Seq1::from_u64(2).unwrap());
            }
            other => panic!("expected contiguous, got {other:?}"),
        }
    }
}
