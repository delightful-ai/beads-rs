//! Event body encoding + hashing for realtime WAL and replication.

use std::marker::PhantomData;

use bytes::Bytes;
use minicbor::data::Type;
use minicbor::{Decoder, Encoder};
use sha2::{Digest, Sha256 as Sha2};
use thiserror::Error;

use super::identity::{
    ActorId, ClientRequestId, EventId, ReplicaId, StoreId, StoreIdentity, TxnId,
};
use super::limits::Limits;
use super::namespace::NamespaceId;
use super::watermark::Seq1;
use super::wire_bead::{
    NoteAppendV1, NotesPatch, TxnDeltaV1, TxnOpV1, WireBeadPatch, WireNoteV1, WirePatch, WireStamp,
    WorkflowStatus,
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
    pub fn new(bytes: Bytes) -> Self {
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventKindV1 {
    TxnV1,
}

impl EventKindV1 {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventKindV1::TxnV1 => "txn_v1",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "txn_v1" => Some(EventKindV1::TxnV1),
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
    pub kind: EventKindV1,
    pub delta: TxnDeltaV1,
    pub hlc_max: Option<HlcMax>,
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
    #[error("trailing bytes after event body")]
    TrailingBytes,
    #[error("cbor decode: {0}")]
    Cbor(#[from] minicbor::decode::Error),
}

pub fn encode_event_body_canonical(body: &EventBody) -> Result<EventBytes<Canonical>, EncodeError> {
    let mut buf = Vec::new();
    let mut enc = Encoder::new(&mut buf);
    encode_event_body_map(&mut enc, body)?;
    Ok(EventBytes::<Canonical>::new(Bytes::from(buf)))
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

fn encode_event_body_map(
    enc: &mut Encoder<&mut Vec<u8>>,
    body: &EventBody,
) -> Result<(), EncodeError> {
    let mut len = 10;
    if body.client_request_id.is_some() {
        len += 1;
    }
    if body.hlc_max.is_some() {
        len += 1;
    }

    enc.map(len as u64)?;

    if let Some(client_request_id) = &body.client_request_id {
        enc.str("client_request_id")?;
        enc.str(&client_request_id.as_uuid().to_string())?;
    }

    enc.str("delta")?;
    encode_txn_delta(enc, &body.delta)?;

    enc.str("envelope_v")?;
    enc.u32(body.envelope_v)?;

    enc.str("event_time_ms")?;
    enc.u64(body.event_time_ms)?;

    if let Some(hlc_max) = &body.hlc_max {
        enc.str("hlc_max")?;
        encode_hlc_max(enc, hlc_max)?;
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

    let mut envelope_v = None;
    let mut store_id: Option<StoreId> = None;
    let mut store_epoch = None;
    let mut namespace: Option<NamespaceId> = None;
    let mut origin_replica_id: Option<ReplicaId> = None;
    let mut origin_seq: Option<Seq1> = None;
    let mut event_time_ms = None;
    let mut txn_id: Option<TxnId> = None;
    let mut client_request_id: Option<ClientRequestId> = None;
    let mut kind: Option<EventKindV1> = None;
    let mut delta: Option<TxnDeltaV1> = None;
    let mut hlc_max: Option<HlcMax> = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "client_request_id" => {
                let raw = decode_text(dec, limits)?;
                client_request_id = Some(parse_uuid_field("client_request_id", raw)?);
            }
            "delta" => {
                delta = Some(decode_txn_delta(dec, limits, depth + 1)?);
            }
            "envelope_v" => {
                envelope_v = Some(decode_u32(dec, "envelope_v")?);
            }
            "event_time_ms" => {
                event_time_ms = Some(dec.u64()?);
            }
            "hlc_max" => {
                hlc_max = Some(decode_hlc_max(dec, limits, depth + 1)?);
            }
            "kind" => {
                let raw = decode_text(dec, limits)?;
                kind = Some(
                    EventKindV1::parse(raw)
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
                let value = dec.u64()?;
                origin_seq =
                    Some(
                        Seq1::from_u64(value).ok_or_else(|| DecodeError::InvalidField {
                            field: "origin_seq",
                            reason: "must be nonzero".into(),
                        })?,
                    );
            }
            "store_epoch" => {
                store_epoch = Some(dec.u64()?);
            }
            "store_id" => {
                let raw = decode_text(dec, limits)?;
                store_id = Some(parse_uuid_field("store_id", raw)?);
            }
            "txn_id" => {
                let raw = decode_text(dec, limits)?;
                txn_id = Some(parse_uuid_field("txn_id", raw)?);
            }
            other => {
                return Err(DecodeError::InvalidField {
                    field: "event_body",
                    reason: format!("unknown key {other}"),
                });
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
    let delta = delta.ok_or(DecodeError::MissingField("delta"))?;

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
        kind,
        delta,
        hlc_max,
    })
}

fn encode_txn_delta(
    enc: &mut Encoder<&mut Vec<u8>>,
    delta: &TxnDeltaV1,
) -> Result<(), EncodeError> {
    let mut bead_upserts: Vec<&WireBeadPatch> = Vec::new();
    let mut note_appends: Vec<&NoteAppendV1> = Vec::new();

    for op in delta.iter() {
        match op {
            TxnOpV1::BeadUpsert(up) => bead_upserts.push(up),
            TxnOpV1::NoteAppend(append) => note_appends.push(append),
        }
    }

    let mut len = 1;
    if !bead_upserts.is_empty() {
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

    let mut version = None;
    let mut bead_upserts: Vec<WireBeadPatch> = Vec::new();
    let mut note_appends: Vec<NoteAppendV1> = Vec::new();

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "bead_upserts" => {
                let arr_len = decode_array_len(dec, limits, depth + 1)?;
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
            "note_appends" => {
                let arr_len = decode_array_len(dec, limits, depth + 1)?;
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
                    for _ in 0..entry_len {
                        let entry_key = decode_text(dec, limits)?;
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
    let mut patch = WireBeadPatch::new(bead_id_placeholder());
    let mut id_set = false;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
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
                patch.status = Some(match raw {
                    "open" => WorkflowStatus::Open,
                    "in_progress" => WorkflowStatus::InProgress,
                    "closed" => WorkflowStatus::Closed,
                    _ => {
                        return Err(DecodeError::InvalidField {
                            field: "status",
                            reason: format!("unknown status {raw}"),
                        });
                    }
                });
            }
            "title" => {
                patch.title = Some(decode_text(dec, limits)?.to_string());
            }
            "type" => {
                let raw = decode_text(dec, limits)?;
                patch.bead_type = Some(match raw {
                    "bug" => super::domain::BeadType::Bug,
                    "feature" => super::domain::BeadType::Feature,
                    "task" => super::domain::BeadType::Task,
                    "epic" => super::domain::BeadType::Epic,
                    "chore" => super::domain::BeadType::Chore,
                    _ => {
                        return Err(DecodeError::InvalidField {
                            field: "type",
                            reason: format!("unknown bead type {raw}"),
                        });
                    }
                });
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
    let mut id = None;
    let mut content = None;
    let mut author = None;
    let mut at = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
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

    Ok(WireNoteV1 {
        id: id.ok_or(DecodeError::MissingField("id"))?,
        content: content.ok_or(DecodeError::MissingField("content"))?,
        author: author.ok_or(DecodeError::MissingField("author"))?,
        at: at.ok_or(DecodeError::MissingField("at"))?,
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
    let wall_ms = dec.u64()?;
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
        _ => Ok(WirePatch::Set(super::time::WallClock(dec.u64()?))),
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
    let mut actor_id = None;
    let mut logical = None;
    let mut physical_ms = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "actor_id" => {
                let raw = decode_text(dec, limits)?;
                actor_id = Some(parse_actor_id(raw, "actor_id")?);
            }
            "logical" => {
                logical = Some(decode_u32(dec, "logical")?);
            }
            "physical_ms" => {
                physical_ms = Some(dec.u64()?);
            }
            other => {
                return Err(DecodeError::InvalidField {
                    field: "hlc_max",
                    reason: format!("unknown key {other}"),
                });
            }
        }
    }

    Ok(HlcMax {
        actor_id: actor_id.ok_or(DecodeError::MissingField("actor_id"))?,
        physical_ms: physical_ms.ok_or(DecodeError::MissingField("physical_ms"))?,
        logical: logical.ok_or(DecodeError::MissingField("logical"))?,
    })
}

fn decode_map_len(dec: &mut Decoder, limits: &Limits, depth: usize) -> Result<usize, DecodeError> {
    ensure_depth(depth, limits)?;
    let len = dec.map()?;
    let Some(len) = len else {
        return Err(DecodeError::IndefiniteLength);
    };
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
    let len = dec.array()?;
    let Some(len) = len else {
        return Err(DecodeError::IndefiniteLength);
    };
    if len > limits.max_cbor_array_entries as u64 {
        return Err(DecodeError::DecodeLimit("max_cbor_array_entries"));
    }
    usize::try_from(len).map_err(|_| DecodeError::DecodeLimit("max_cbor_array_entries"))
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

fn decode_u32(dec: &mut Decoder, field: &'static str) -> Result<u32, DecodeError> {
    let value = dec.u64()?;
    u32::try_from(value).map_err(|_| DecodeError::InvalidField {
        field,
        reason: format!("value {value} out of range for u32"),
    })
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
    let delta = &body.delta;
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
    use crate::core::identity::{BeadId, StoreEpoch};
    use uuid::Uuid;

    fn actor_id(actor: &str) -> ActorId {
        ActorId::new(actor).unwrap()
    }

    fn sample_body() -> EventBody {
        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::new(2));
        let origin = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let txn_id = TxnId::new(Uuid::from_bytes([3u8; 16]));
        let client_request_id = ClientRequestId::new(Uuid::from_bytes([4u8; 16]));

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
            kind: EventKindV1::TxnV1,
            delta,
            hlc_max: Some(HlcMax {
                actor_id: actor_id("alice"),
                physical_ms: 123,
                logical: 7,
            }),
        }
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
    fn decode_rejects_indefinite_length() {
        let limits = Limits::default();
        let bytes = [0xbf, 0xff];
        let err = decode_event_body(&bytes, &limits).unwrap_err();
        assert!(matches!(err, DecodeError::IndefiniteLength));
    }

    #[test]
    fn decode_rejects_text_bounds() {
        let mut limits = Limits::default();
        limits.max_cbor_text_string_len = 3;
        let body = sample_body();
        let encoded = encode_event_body_canonical(&body).unwrap();
        let err = decode_event_body(encoded.as_ref(), &limits).unwrap_err();
        assert!(matches!(
            err,
            DecodeError::DecodeLimit("max_cbor_text_string_len")
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
