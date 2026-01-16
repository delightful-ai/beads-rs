//! Replication protocol message schemas and CBOR encoding.

use std::collections::BTreeMap;
use std::convert::Infallible;

use bytes::Bytes;
use minicbor::data::Type;
use minicbor::{Decoder, Encoder};
use serde_json::Value;
use thiserror::Error;

use crate::core::error::details::{
    BatchTooLargeDetails, InvalidRequestDetails, MalformedPayloadDetails, ParserKind,
};
use crate::core::{
    ErrorCode, ErrorPayload, EventBytes, EventFrameV1, EventId, Limits, NamespaceId, Opaque,
    ProtocolErrorCode, ReplicaId, Seq0, Seq1, Sha256, StoreEpoch, StoreId,
};

pub const PROTOCOL_VERSION_V1: u32 = crate::core::StoreMetaVersions::REPLICATION_PROTOCOL_VERSION;

pub type WatermarkMap = BTreeMap<NamespaceId, BTreeMap<ReplicaId, Seq0>>;
pub type WatermarkHeads = BTreeMap<NamespaceId, BTreeMap<ReplicaId, Sha256>>;

#[derive(Clone, Debug, PartialEq)]
pub struct ReplEnvelope {
    pub version: u32,
    pub message: ReplMessage,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ReplMessage {
    Hello(Hello),
    Welcome(Welcome),
    Events(Events),
    Ack(Ack),
    Want(Want),
    Ping(Ping),
    Pong(Pong),
    Error(ErrorPayload),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Capabilities {
    pub supports_snapshots: bool,
    pub supports_live_stream: bool,
    pub supports_compression: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Hello {
    pub protocol_version: u32,
    pub min_protocol_version: u32,
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
    pub sender_replica_id: ReplicaId,
    pub hello_nonce: u64,
    pub max_frame_bytes: u32,
    pub requested_namespaces: Vec<NamespaceId>,
    pub offered_namespaces: Vec<NamespaceId>,
    pub seen_durable: WatermarkMap,
    pub seen_durable_heads: Option<WatermarkHeads>,
    pub seen_applied: Option<WatermarkMap>,
    pub seen_applied_heads: Option<WatermarkHeads>,
    pub capabilities: Capabilities,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Welcome {
    pub protocol_version: u32,
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
    pub receiver_replica_id: ReplicaId,
    pub welcome_nonce: u64,
    pub accepted_namespaces: Vec<NamespaceId>,
    pub receiver_seen_durable: WatermarkMap,
    pub receiver_seen_durable_heads: Option<WatermarkHeads>,
    pub receiver_seen_applied: Option<WatermarkMap>,
    pub receiver_seen_applied_heads: Option<WatermarkHeads>,
    pub live_stream_enabled: bool,
    pub max_frame_bytes: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Events {
    pub events: Vec<EventFrameV1>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Ack {
    pub durable: WatermarkMap,
    pub durable_heads: Option<WatermarkHeads>,
    pub applied: Option<WatermarkMap>,
    pub applied_heads: Option<WatermarkHeads>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Want {
    pub want: WatermarkMap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ping {
    pub nonce: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pong {
    pub nonce: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MessageType {
    Hello,
    Welcome,
    Events,
    Ack,
    Want,
    Ping,
    Pong,
    Error,
}

impl MessageType {
    fn as_str(self) -> &'static str {
        match self {
            MessageType::Hello => "HELLO",
            MessageType::Welcome => "WELCOME",
            MessageType::Events => "EVENTS",
            MessageType::Ack => "ACK",
            MessageType::Want => "WANT",
            MessageType::Ping => "PING",
            MessageType::Pong => "PONG",
            MessageType::Error => "ERROR",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "HELLO" => Some(MessageType::Hello),
            "WELCOME" => Some(MessageType::Welcome),
            "EVENTS" => Some(MessageType::Events),
            "ACK" => Some(MessageType::Ack),
            "WANT" => Some(MessageType::Want),
            "PING" => Some(MessageType::Ping),
            "PONG" => Some(MessageType::Pong),
            "ERROR" => Some(MessageType::Error),
            _ => None,
        }
    }
}

impl ReplMessage {
    fn message_type(&self) -> MessageType {
        match self {
            ReplMessage::Hello(_) => MessageType::Hello,
            ReplMessage::Welcome(_) => MessageType::Welcome,
            ReplMessage::Events(_) => MessageType::Events,
            ReplMessage::Ack(_) => MessageType::Ack,
            ReplMessage::Want(_) => MessageType::Want,
            ReplMessage::Ping(_) => MessageType::Ping,
            ReplMessage::Pong(_) => MessageType::Pong,
            ReplMessage::Error(_) => MessageType::Error,
        }
    }
}

#[derive(Debug, Error)]
pub enum ProtoEncodeError {
    #[error("cbor encode: {0}")]
    Cbor(#[from] minicbor::encode::Error<Infallible>),
    #[error("envelope version {envelope} does not match body version {body}")]
    VersionMismatch { envelope: u32, body: u32 },
}

#[derive(Debug, Error)]
pub enum ProtoDecodeError {
    #[error("decode limit exceeded: {0}")]
    DecodeLimit(&'static str),
    #[error("indefinite-length CBOR not allowed")]
    IndefiniteLength,
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    #[error("invalid field {field}: {reason}")]
    InvalidField { field: &'static str, reason: String },
    #[error("unknown message type: {0}")]
    UnknownMessageType(String),
    #[error("trailing bytes after message body")]
    TrailingBytes,
    #[error(
        "event batch too large: events {got_events}/{max_events} bytes {got_bytes}/{max_bytes}"
    )]
    BatchTooLarge {
        max_events: usize,
        max_bytes: usize,
        got_events: usize,
        got_bytes: usize,
    },
    #[error("cbor decode: {0}")]
    Cbor(#[from] minicbor::decode::Error),
}

impl ProtoDecodeError {
    pub fn as_error_payload(&self) -> Option<ErrorPayload> {
        match self {
            ProtoDecodeError::BatchTooLarge {
                max_events,
                max_bytes,
                got_events,
                got_bytes,
            } => Some(
                ErrorPayload::new(
                    ProtocolErrorCode::BatchTooLarge.into(),
                    "event batch exceeds limits",
                    false,
                )
                .with_details(BatchTooLargeDetails {
                    max_events: *max_events as u64,
                    max_bytes: *max_bytes as u64,
                    got_events: *got_events as u64,
                    got_bytes: *got_bytes as u64,
                }),
            ),
            ProtoDecodeError::MissingField(field) => Some(
                ErrorPayload::new(
                    ProtocolErrorCode::InvalidRequest.into(),
                    format!("missing field {field}"),
                    false,
                )
                .with_details(InvalidRequestDetails {
                    field: Some(field.to_string()),
                    reason: None,
                }),
            ),
            ProtoDecodeError::InvalidField { field, reason } => Some(
                ErrorPayload::new(
                    ProtocolErrorCode::InvalidRequest.into(),
                    format!("invalid field {field}: {reason}"),
                    false,
                )
                .with_details(InvalidRequestDetails {
                    field: Some(field.to_string()),
                    reason: Some(reason.clone()),
                }),
            ),
            ProtoDecodeError::UnknownMessageType(raw) => Some(
                ErrorPayload::new(
                    ProtocolErrorCode::InvalidRequest.into(),
                    format!("unknown message type {raw}"),
                    false,
                )
                .with_details(InvalidRequestDetails {
                    field: Some("type".into()),
                    reason: Some(format!("unknown message type {raw}")),
                }),
            ),
            ProtoDecodeError::DecodeLimit(_)
            | ProtoDecodeError::IndefiniteLength
            | ProtoDecodeError::TrailingBytes
            | ProtoDecodeError::Cbor(_) => Some(
                ErrorPayload::new(
                    ProtocolErrorCode::MalformedPayload.into(),
                    "failed to decode CBOR payload",
                    false,
                )
                .with_details(MalformedPayloadDetails {
                    parser: ParserKind::Cbor,
                    reason: Some(reason_string(self)),
                }),
            ),
        }
    }
}

pub fn encode_envelope(envelope: &ReplEnvelope) -> Result<Vec<u8>, ProtoEncodeError> {
    let mut buf = Vec::new();
    let mut enc = Encoder::new(&mut buf);
    enc.map(3)?;
    enc.str("v")?;
    enc.u32(envelope.version)?;
    enc.str("type")?;
    enc.str(envelope.message.message_type().as_str())?;
    enc.str("body")?;
    encode_message_body(&mut enc, envelope)?;
    Ok(buf)
}

pub fn decode_envelope(bytes: &[u8], limits: &Limits) -> Result<ReplEnvelope, ProtoDecodeError> {
    let mut dec = Decoder::new(bytes);
    let map_len = decode_map_len(&mut dec, limits, 0)?;

    let mut version = None;
    let mut message_type = None;
    let mut body_span = None;

    for _ in 0..map_len {
        let key = decode_text(&mut dec, limits)?;
        match key {
            "v" => version = Some(decode_u32(&mut dec, "v")?),
            "type" => {
                let raw = decode_text(&mut dec, limits)?;
                message_type = Some(
                    MessageType::parse(raw)
                        .ok_or_else(|| ProtoDecodeError::UnknownMessageType(raw.to_string()))?,
                );
            }
            "body" => {
                let start = dec.position();
                dec.skip()?;
                let end = dec.position();
                body_span = Some((start, end));
            }
            _ => {
                if is_indefinite(&dec)? {
                    return Err(ProtoDecodeError::IndefiniteLength);
                }
                dec.skip()?;
            }
        }
    }

    if dec.datatype().is_ok() {
        return Err(ProtoDecodeError::TrailingBytes);
    }

    let version = version.ok_or(ProtoDecodeError::MissingField("v"))?;
    let message_type = message_type.ok_or(ProtoDecodeError::MissingField("type"))?;
    let (start, end) = body_span.ok_or(ProtoDecodeError::MissingField("body"))?;
    let body_bytes = &bytes[start..end];

    let message = decode_message_body(version, message_type, body_bytes, limits)?;

    Ok(ReplEnvelope { version, message })
}

fn encode_message_body(
    enc: &mut Encoder<&mut Vec<u8>>,
    envelope: &ReplEnvelope,
) -> Result<(), ProtoEncodeError> {
    match &envelope.message {
        ReplMessage::Hello(msg) => {
            if envelope.version != msg.protocol_version {
                return Err(ProtoEncodeError::VersionMismatch {
                    envelope: envelope.version,
                    body: msg.protocol_version,
                });
            }
            encode_hello(enc, msg)
        }
        ReplMessage::Welcome(msg) => {
            if envelope.version != msg.protocol_version {
                return Err(ProtoEncodeError::VersionMismatch {
                    envelope: envelope.version,
                    body: msg.protocol_version,
                });
            }
            encode_welcome(enc, msg)
        }
        ReplMessage::Events(msg) => encode_events(enc, msg),
        ReplMessage::Ack(msg) => encode_ack(enc, msg),
        ReplMessage::Want(msg) => encode_want(enc, msg),
        ReplMessage::Ping(msg) => encode_ping(enc, msg),
        ReplMessage::Pong(msg) => encode_pong(enc, msg),
        ReplMessage::Error(msg) => encode_error_payload(enc, msg),
    }
}

fn decode_message_body(
    version: u32,
    message_type: MessageType,
    bytes: &[u8],
    limits: &Limits,
) -> Result<ReplMessage, ProtoDecodeError> {
    let mut dec = Decoder::new(bytes);
    let message = match message_type {
        MessageType::Hello => ReplMessage::Hello(decode_hello(&mut dec, limits)?),
        MessageType::Welcome => ReplMessage::Welcome(decode_welcome(&mut dec, limits)?),
        MessageType::Events => ReplMessage::Events(decode_events(&mut dec, limits)?),
        MessageType::Ack => ReplMessage::Ack(decode_ack(&mut dec, limits)?),
        MessageType::Want => ReplMessage::Want(decode_want(&mut dec, limits)?),
        MessageType::Ping => ReplMessage::Ping(decode_ping(&mut dec, limits)?),
        MessageType::Pong => ReplMessage::Pong(decode_pong(&mut dec, limits)?),
        MessageType::Error => ReplMessage::Error(decode_error_payload(&mut dec, limits)?),
    };

    if dec.datatype().is_ok() {
        return Err(ProtoDecodeError::TrailingBytes);
    }

    match &message {
        ReplMessage::Hello(msg) if msg.protocol_version != version => {
            Err(ProtoDecodeError::InvalidField {
                field: "protocol_version",
                reason: format!(
                    "body {body} does not match envelope v {version}",
                    body = msg.protocol_version
                ),
            })
        }
        ReplMessage::Welcome(msg) if msg.protocol_version != version => {
            Err(ProtoDecodeError::InvalidField {
                field: "protocol_version",
                reason: format!(
                    "body {body} does not match envelope v {version}",
                    body = msg.protocol_version
                ),
            })
        }
        _ => Ok(message),
    }
}

fn encode_hello(enc: &mut Encoder<&mut Vec<u8>>, hello: &Hello) -> Result<(), ProtoEncodeError> {
    let mut len = 11;
    if hello.seen_durable_heads.is_some() {
        len += 1;
    }
    if hello.seen_applied.is_some() {
        len += 1;
    }
    if hello.seen_applied_heads.is_some() {
        len += 1;
    }
    enc.map(len)?;

    enc.str("protocol_version")?;
    enc.u32(hello.protocol_version)?;
    enc.str("min_protocol_version")?;
    enc.u32(hello.min_protocol_version)?;
    enc.str("store_id")?;
    encode_store_id(enc, &hello.store_id)?;
    enc.str("store_epoch")?;
    enc.u64(hello.store_epoch.get())?;
    enc.str("sender_replica_id")?;
    encode_replica_id(enc, &hello.sender_replica_id)?;
    enc.str("hello_nonce")?;
    enc.u64(hello.hello_nonce)?;
    enc.str("max_frame_bytes")?;
    enc.u32(hello.max_frame_bytes)?;
    enc.str("requested_namespaces")?;
    encode_namespace_list(enc, &hello.requested_namespaces)?;
    enc.str("offered_namespaces")?;
    encode_namespace_list(enc, &hello.offered_namespaces)?;
    enc.str("seen_durable")?;
    encode_watermark_map(enc, &hello.seen_durable)?;

    if let Some(heads) = &hello.seen_durable_heads {
        enc.str("seen_durable_heads")?;
        encode_watermark_heads(enc, heads)?;
    }
    if let Some(applied) = &hello.seen_applied {
        enc.str("seen_applied")?;
        encode_watermark_map(enc, applied)?;
    }
    if let Some(heads) = &hello.seen_applied_heads {
        enc.str("seen_applied_heads")?;
        encode_watermark_heads(enc, heads)?;
    }

    enc.str("capabilities")?;
    encode_capabilities(enc, &hello.capabilities)?;

    Ok(())
}

fn decode_hello(dec: &mut Decoder, limits: &Limits) -> Result<Hello, ProtoDecodeError> {
    let map_len = decode_map_len(dec, limits, 0)?;

    let mut protocol_version = None;
    let mut min_protocol_version = None;
    let mut store_id = None;
    let mut store_epoch = None;
    let mut sender_replica_id = None;
    let mut hello_nonce = None;
    let mut max_frame_bytes = None;
    let mut requested_namespaces = None;
    let mut offered_namespaces = None;
    let mut seen_durable = None;
    let mut seen_durable_heads = None;
    let mut seen_applied = None;
    let mut seen_applied_heads = None;
    let mut capabilities = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "protocol_version" => protocol_version = Some(decode_u32(dec, "protocol_version")?),
            "min_protocol_version" => {
                min_protocol_version = Some(decode_u32(dec, "min_protocol_version")?)
            }
            "store_id" => store_id = Some(decode_store_id(dec, limits, "store_id")?),
            "store_epoch" => store_epoch = Some(StoreEpoch::new(dec.u64()?)),
            "sender_replica_id" => {
                sender_replica_id = Some(decode_replica_id(dec, limits, "sender_replica_id")?)
            }
            "hello_nonce" => hello_nonce = Some(dec.u64()?),
            "max_frame_bytes" => max_frame_bytes = Some(decode_u32(dec, "max_frame_bytes")?),
            "requested_namespaces" => {
                requested_namespaces = Some(decode_namespace_list(dec, limits, 1)?)
            }
            "offered_namespaces" => {
                offered_namespaces = Some(decode_namespace_list(dec, limits, 1)?)
            }
            "seen_durable" => seen_durable = Some(decode_watermark_map(dec, limits, 1)?),
            "seen_durable_heads" => {
                seen_durable_heads = Some(decode_watermark_heads(dec, limits, 1)?)
            }
            "seen_applied" => seen_applied = Some(decode_watermark_map(dec, limits, 1)?),
            "seen_applied_heads" => {
                seen_applied_heads = Some(decode_watermark_heads(dec, limits, 1)?)
            }
            "capabilities" => capabilities = Some(decode_capabilities(dec, limits, 1)?),
            _ => {
                if is_indefinite(dec)? {
                    return Err(ProtoDecodeError::IndefiniteLength);
                }
                dec.skip()?;
            }
        }
    }

    Ok(Hello {
        protocol_version: protocol_version
            .ok_or(ProtoDecodeError::MissingField("protocol_version"))?,
        min_protocol_version: min_protocol_version
            .ok_or(ProtoDecodeError::MissingField("min_protocol_version"))?,
        store_id: store_id.ok_or(ProtoDecodeError::MissingField("store_id"))?,
        store_epoch: store_epoch.ok_or(ProtoDecodeError::MissingField("store_epoch"))?,
        sender_replica_id: sender_replica_id
            .ok_or(ProtoDecodeError::MissingField("sender_replica_id"))?,
        hello_nonce: hello_nonce.ok_or(ProtoDecodeError::MissingField("hello_nonce"))?,
        max_frame_bytes: max_frame_bytes
            .ok_or(ProtoDecodeError::MissingField("max_frame_bytes"))?,
        requested_namespaces: requested_namespaces
            .ok_or(ProtoDecodeError::MissingField("requested_namespaces"))?,
        offered_namespaces: offered_namespaces
            .ok_or(ProtoDecodeError::MissingField("offered_namespaces"))?,
        seen_durable: seen_durable.ok_or(ProtoDecodeError::MissingField("seen_durable"))?,
        seen_durable_heads,
        seen_applied,
        seen_applied_heads,
        capabilities: capabilities.ok_or(ProtoDecodeError::MissingField("capabilities"))?,
    })
}

fn encode_welcome(
    enc: &mut Encoder<&mut Vec<u8>>,
    welcome: &Welcome,
) -> Result<(), ProtoEncodeError> {
    let mut len = 9;
    if welcome.receiver_seen_durable_heads.is_some() {
        len += 1;
    }
    if welcome.receiver_seen_applied.is_some() {
        len += 1;
    }
    if welcome.receiver_seen_applied_heads.is_some() {
        len += 1;
    }
    enc.map(len)?;

    enc.str("protocol_version")?;
    enc.u32(welcome.protocol_version)?;
    enc.str("store_id")?;
    encode_store_id(enc, &welcome.store_id)?;
    enc.str("store_epoch")?;
    enc.u64(welcome.store_epoch.get())?;
    enc.str("receiver_replica_id")?;
    encode_replica_id(enc, &welcome.receiver_replica_id)?;
    enc.str("welcome_nonce")?;
    enc.u64(welcome.welcome_nonce)?;
    enc.str("accepted_namespaces")?;
    encode_namespace_list(enc, &welcome.accepted_namespaces)?;
    enc.str("receiver_seen_durable")?;
    encode_watermark_map(enc, &welcome.receiver_seen_durable)?;
    if let Some(heads) = &welcome.receiver_seen_durable_heads {
        enc.str("receiver_seen_durable_heads")?;
        encode_watermark_heads(enc, heads)?;
    }
    if let Some(applied) = &welcome.receiver_seen_applied {
        enc.str("receiver_seen_applied")?;
        encode_watermark_map(enc, applied)?;
    }
    if let Some(heads) = &welcome.receiver_seen_applied_heads {
        enc.str("receiver_seen_applied_heads")?;
        encode_watermark_heads(enc, heads)?;
    }
    enc.str("live_stream_enabled")?;
    enc.bool(welcome.live_stream_enabled)?;
    enc.str("max_frame_bytes")?;
    enc.u32(welcome.max_frame_bytes)?;

    Ok(())
}

fn decode_welcome(dec: &mut Decoder, limits: &Limits) -> Result<Welcome, ProtoDecodeError> {
    let map_len = decode_map_len(dec, limits, 0)?;

    let mut protocol_version = None;
    let mut store_id = None;
    let mut store_epoch = None;
    let mut receiver_replica_id = None;
    let mut welcome_nonce = None;
    let mut accepted_namespaces = None;
    let mut receiver_seen_durable = None;
    let mut receiver_seen_durable_heads = None;
    let mut receiver_seen_applied = None;
    let mut receiver_seen_applied_heads = None;
    let mut live_stream_enabled = None;
    let mut max_frame_bytes = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "protocol_version" => protocol_version = Some(decode_u32(dec, "protocol_version")?),
            "store_id" => store_id = Some(decode_store_id(dec, limits, "store_id")?),
            "store_epoch" => store_epoch = Some(StoreEpoch::new(dec.u64()?)),
            "receiver_replica_id" => {
                receiver_replica_id = Some(decode_replica_id(dec, limits, "receiver_replica_id")?)
            }
            "welcome_nonce" => welcome_nonce = Some(dec.u64()?),
            "accepted_namespaces" => {
                accepted_namespaces = Some(decode_namespace_list(dec, limits, 1)?)
            }
            "receiver_seen_durable" => {
                receiver_seen_durable = Some(decode_watermark_map(dec, limits, 1)?)
            }
            "receiver_seen_durable_heads" => {
                receiver_seen_durable_heads = Some(decode_watermark_heads(dec, limits, 1)?)
            }
            "receiver_seen_applied" => {
                receiver_seen_applied = Some(decode_watermark_map(dec, limits, 1)?)
            }
            "receiver_seen_applied_heads" => {
                receiver_seen_applied_heads = Some(decode_watermark_heads(dec, limits, 1)?)
            }
            "live_stream_enabled" => live_stream_enabled = Some(dec.bool()?),
            "max_frame_bytes" => max_frame_bytes = Some(decode_u32(dec, "max_frame_bytes")?),
            _ => {
                if is_indefinite(dec)? {
                    return Err(ProtoDecodeError::IndefiniteLength);
                }
                dec.skip()?;
            }
        }
    }

    Ok(Welcome {
        protocol_version: protocol_version
            .ok_or(ProtoDecodeError::MissingField("protocol_version"))?,
        store_id: store_id.ok_or(ProtoDecodeError::MissingField("store_id"))?,
        store_epoch: store_epoch.ok_or(ProtoDecodeError::MissingField("store_epoch"))?,
        receiver_replica_id: receiver_replica_id
            .ok_or(ProtoDecodeError::MissingField("receiver_replica_id"))?,
        welcome_nonce: welcome_nonce.ok_or(ProtoDecodeError::MissingField("welcome_nonce"))?,
        accepted_namespaces: accepted_namespaces
            .ok_or(ProtoDecodeError::MissingField("accepted_namespaces"))?,
        receiver_seen_durable: receiver_seen_durable
            .ok_or(ProtoDecodeError::MissingField("receiver_seen_durable"))?,
        receiver_seen_durable_heads,
        receiver_seen_applied,
        receiver_seen_applied_heads,
        live_stream_enabled: live_stream_enabled
            .ok_or(ProtoDecodeError::MissingField("live_stream_enabled"))?,
        max_frame_bytes: max_frame_bytes
            .ok_or(ProtoDecodeError::MissingField("max_frame_bytes"))?,
    })
}

fn encode_events(enc: &mut Encoder<&mut Vec<u8>>, events: &Events) -> Result<(), ProtoEncodeError> {
    enc.map(1)?;
    enc.str("events")?;
    enc.array(events.events.len() as u64)?;
    for frame in &events.events {
        encode_event_frame(enc, frame)?;
    }
    Ok(())
}

fn decode_events(dec: &mut Decoder, limits: &Limits) -> Result<Events, ProtoDecodeError> {
    let map_len = decode_map_len(dec, limits, 0)?;

    let mut events: Option<Vec<EventFrameV1>> = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "events" => {
                let arr_len = decode_array_len(dec, limits, 1)?;
                let mut total_bytes = 0usize;
                let mut collected = Vec::new();
                let over_count = arr_len > limits.max_event_batch_events;
                for _ in 0..arr_len {
                    let frame = decode_event_frame(dec, limits, 2)?;
                    total_bytes = total_bytes.saturating_add(frame.bytes.len());
                    if total_bytes > limits.max_event_batch_bytes {
                        return Err(ProtoDecodeError::BatchTooLarge {
                            max_events: limits.max_event_batch_events,
                            max_bytes: limits.max_event_batch_bytes,
                            got_events: arr_len,
                            got_bytes: total_bytes,
                        });
                    }
                    if !over_count {
                        collected.push(frame);
                    }
                }
                if over_count {
                    return Err(ProtoDecodeError::BatchTooLarge {
                        max_events: limits.max_event_batch_events,
                        max_bytes: limits.max_event_batch_bytes,
                        got_events: arr_len,
                        got_bytes: total_bytes,
                    });
                }
                events = Some(collected);
            }
            _ => {
                if is_indefinite(dec)? {
                    return Err(ProtoDecodeError::IndefiniteLength);
                }
                dec.skip()?;
            }
        }
    }

    Ok(Events {
        events: events.ok_or(ProtoDecodeError::MissingField("events"))?,
    })
}

fn encode_ack(enc: &mut Encoder<&mut Vec<u8>>, ack: &Ack) -> Result<(), ProtoEncodeError> {
    let mut len = 1;
    if ack.durable_heads.is_some() {
        len += 1;
    }
    if ack.applied.is_some() {
        len += 1;
    }
    if ack.applied_heads.is_some() {
        len += 1;
    }

    enc.map(len)?;
    enc.str("durable")?;
    encode_watermark_map(enc, &ack.durable)?;
    if let Some(heads) = &ack.durable_heads {
        enc.str("durable_heads")?;
        encode_watermark_heads(enc, heads)?;
    }
    if let Some(applied) = &ack.applied {
        enc.str("applied")?;
        encode_watermark_map(enc, applied)?;
    }
    if let Some(heads) = &ack.applied_heads {
        enc.str("applied_heads")?;
        encode_watermark_heads(enc, heads)?;
    }
    Ok(())
}

fn decode_ack(dec: &mut Decoder, limits: &Limits) -> Result<Ack, ProtoDecodeError> {
    let map_len = decode_map_len(dec, limits, 0)?;

    let mut durable = None;
    let mut durable_heads = None;
    let mut applied = None;
    let mut applied_heads = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "durable" => durable = Some(decode_watermark_map(dec, limits, 1)?),
            "durable_heads" => durable_heads = Some(decode_watermark_heads(dec, limits, 1)?),
            "applied" => applied = Some(decode_watermark_map(dec, limits, 1)?),
            "applied_heads" => applied_heads = Some(decode_watermark_heads(dec, limits, 1)?),
            _ => {
                if is_indefinite(dec)? {
                    return Err(ProtoDecodeError::IndefiniteLength);
                }
                dec.skip()?;
            }
        }
    }

    Ok(Ack {
        durable: durable.ok_or(ProtoDecodeError::MissingField("durable"))?,
        durable_heads,
        applied,
        applied_heads,
    })
}

fn encode_want(enc: &mut Encoder<&mut Vec<u8>>, want: &Want) -> Result<(), ProtoEncodeError> {
    enc.map(1)?;
    enc.str("want")?;
    encode_watermark_map(enc, &want.want)?;
    Ok(())
}

fn decode_want(dec: &mut Decoder, limits: &Limits) -> Result<Want, ProtoDecodeError> {
    let map_len = decode_map_len(dec, limits, 0)?;
    let mut want = None;
    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "want" => want = Some(decode_watermark_map(dec, limits, 1)?),
            _ => {
                if is_indefinite(dec)? {
                    return Err(ProtoDecodeError::IndefiniteLength);
                }
                dec.skip()?;
            }
        }
    }
    Ok(Want {
        want: want.ok_or(ProtoDecodeError::MissingField("want"))?,
    })
}

fn encode_ping(enc: &mut Encoder<&mut Vec<u8>>, ping: &Ping) -> Result<(), ProtoEncodeError> {
    enc.map(1)?;
    enc.str("nonce")?;
    enc.u64(ping.nonce)?;
    Ok(())
}

fn decode_ping(dec: &mut Decoder, limits: &Limits) -> Result<Ping, ProtoDecodeError> {
    let map_len = decode_map_len(dec, limits, 0)?;
    let mut nonce = None;
    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "nonce" => nonce = Some(dec.u64()?),
            _ => {
                if is_indefinite(dec)? {
                    return Err(ProtoDecodeError::IndefiniteLength);
                }
                dec.skip()?;
            }
        }
    }
    Ok(Ping {
        nonce: nonce.ok_or(ProtoDecodeError::MissingField("nonce"))?,
    })
}

fn encode_pong(enc: &mut Encoder<&mut Vec<u8>>, pong: &Pong) -> Result<(), ProtoEncodeError> {
    enc.map(1)?;
    enc.str("nonce")?;
    enc.u64(pong.nonce)?;
    Ok(())
}

fn decode_pong(dec: &mut Decoder, limits: &Limits) -> Result<Pong, ProtoDecodeError> {
    let map_len = decode_map_len(dec, limits, 0)?;
    let mut nonce = None;
    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "nonce" => nonce = Some(dec.u64()?),
            _ => {
                if is_indefinite(dec)? {
                    return Err(ProtoDecodeError::IndefiniteLength);
                }
                dec.skip()?;
            }
        }
    }
    Ok(Pong {
        nonce: nonce.ok_or(ProtoDecodeError::MissingField("nonce"))?,
    })
}

fn encode_capabilities(
    enc: &mut Encoder<&mut Vec<u8>>,
    caps: &Capabilities,
) -> Result<(), ProtoEncodeError> {
    enc.map(3)?;
    enc.str("supports_snapshots")?;
    enc.bool(caps.supports_snapshots)?;
    enc.str("supports_live_stream")?;
    enc.bool(caps.supports_live_stream)?;
    enc.str("supports_compression")?;
    enc.bool(caps.supports_compression)?;
    Ok(())
}

fn decode_capabilities(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<Capabilities, ProtoDecodeError> {
    let map_len = decode_map_len(dec, limits, depth)?;
    let mut supports_snapshots = None;
    let mut supports_live_stream = None;
    let mut supports_compression = Some(false);

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "supports_snapshots" => supports_snapshots = Some(dec.bool()?),
            "supports_live_stream" => supports_live_stream = Some(dec.bool()?),
            "supports_compression" => supports_compression = Some(dec.bool()?),
            _ => {
                if is_indefinite(dec)? {
                    return Err(ProtoDecodeError::IndefiniteLength);
                }
                dec.skip()?;
            }
        }
    }

    Ok(Capabilities {
        supports_snapshots: supports_snapshots
            .ok_or(ProtoDecodeError::MissingField("supports_snapshots"))?,
        supports_live_stream: supports_live_stream
            .ok_or(ProtoDecodeError::MissingField("supports_live_stream"))?,
        supports_compression: supports_compression.unwrap_or(false),
    })
}

fn encode_event_frame(
    enc: &mut Encoder<&mut Vec<u8>>,
    frame: &EventFrameV1,
) -> Result<(), ProtoEncodeError> {
    let mut len = 3;
    if frame.prev_sha256.is_some() {
        len += 1;
    }
    enc.map(len)?;
    enc.str("eid")?;
    encode_event_id(enc, &frame.eid)?;
    enc.str("sha256")?;
    enc.bytes(frame.sha256.as_bytes())?;
    if let Some(prev) = &frame.prev_sha256 {
        enc.str("prev_sha256")?;
        enc.bytes(prev.as_bytes())?;
    }
    enc.str("bytes")?;
    enc.bytes(frame.bytes.as_ref())?;
    Ok(())
}

fn decode_event_frame(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<EventFrameV1, ProtoDecodeError> {
    let map_len = decode_map_len(dec, limits, depth)?;
    let mut eid = None;
    let mut sha256 = None;
    let mut prev_sha256 = None;
    let mut bytes = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "eid" => eid = Some(decode_event_id(dec, limits, depth + 1)?),
            "sha256" => sha256 = Some(decode_sha256(dec, limits, "sha256")?),
            "prev_sha256" => prev_sha256 = Some(decode_sha256(dec, limits, "prev_sha256")?),
            "bytes" => {
                let raw = decode_bytes(dec, limits, "bytes")?;
                let max_payload = limits.max_wal_record_bytes.min(limits.max_frame_bytes);
                if raw.len() > max_payload {
                    return Err(ProtoDecodeError::DecodeLimit("max_wal_record_bytes"));
                }
                bytes = Some(EventBytes::<Opaque>::new(Bytes::copy_from_slice(raw)));
            }
            _ => {
                if is_indefinite(dec)? {
                    return Err(ProtoDecodeError::IndefiniteLength);
                }
                dec.skip()?;
            }
        }
    }

    Ok(EventFrameV1 {
        eid: eid.ok_or(ProtoDecodeError::MissingField("eid"))?,
        sha256: sha256.ok_or(ProtoDecodeError::MissingField("sha256"))?,
        prev_sha256,
        bytes: bytes.ok_or(ProtoDecodeError::MissingField("bytes"))?,
    })
}

fn encode_event_id(enc: &mut Encoder<&mut Vec<u8>>, eid: &EventId) -> Result<(), ProtoEncodeError> {
    enc.map(3)?;
    enc.str("origin_replica_id")?;
    encode_replica_id(enc, &eid.origin_replica_id)?;
    enc.str("namespace")?;
    enc.str(eid.namespace.as_str())?;
    enc.str("origin_seq")?;
    enc.u64(eid.origin_seq.get())?;
    Ok(())
}

fn decode_event_id(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<EventId, ProtoDecodeError> {
    let map_len = decode_map_len(dec, limits, depth)?;
    let mut origin_replica_id = None;
    let mut namespace = None;
    let mut origin_seq = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "origin_replica_id" => {
                origin_replica_id = Some(decode_replica_id(dec, limits, "origin_replica_id")?)
            }
            "namespace" => {
                let raw = decode_text(dec, limits)?;
                namespace = Some(parse_namespace(raw)?);
            }
            "origin_seq" => {
                let value = dec.u64()?;
                origin_seq =
                    Some(
                        Seq1::from_u64(value).ok_or_else(|| ProtoDecodeError::InvalidField {
                            field: "origin_seq",
                            reason: "must be >= 1".into(),
                        })?,
                    );
            }
            _ => {
                if is_indefinite(dec)? {
                    return Err(ProtoDecodeError::IndefiniteLength);
                }
                dec.skip()?;
            }
        }
    }

    Ok(EventId::new(
        origin_replica_id.ok_or(ProtoDecodeError::MissingField("origin_replica_id"))?,
        namespace.ok_or(ProtoDecodeError::MissingField("namespace"))?,
        origin_seq.ok_or(ProtoDecodeError::MissingField("origin_seq"))?,
    ))
}

fn encode_namespace_list(
    enc: &mut Encoder<&mut Vec<u8>>,
    namespaces: &[NamespaceId],
) -> Result<(), ProtoEncodeError> {
    enc.array(namespaces.len() as u64)?;
    for ns in namespaces {
        enc.str(ns.as_str())?;
    }
    Ok(())
}

fn decode_namespace_list(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<Vec<NamespaceId>, ProtoDecodeError> {
    let arr_len = decode_array_len(dec, limits, depth)?;
    let mut out = Vec::with_capacity(arr_len);
    for _ in 0..arr_len {
        let raw = decode_text(dec, limits)?;
        out.push(parse_namespace(raw)?);
    }
    Ok(out)
}

fn encode_watermark_map(
    enc: &mut Encoder<&mut Vec<u8>>,
    map: &WatermarkMap,
) -> Result<(), ProtoEncodeError> {
    enc.map(map.len() as u64)?;
    for (ns, origins) in map {
        enc.str(ns.as_str())?;
        enc.map(origins.len() as u64)?;
        for (origin, seq) in origins {
            encode_replica_id(enc, origin)?;
            enc.u64(seq.get())?;
        }
    }
    Ok(())
}

fn decode_watermark_map(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<WatermarkMap, ProtoDecodeError> {
    let map_len = decode_map_len(dec, limits, depth)?;
    let mut out = WatermarkMap::new();
    for _ in 0..map_len {
        let ns_raw = decode_text(dec, limits)?;
        let namespace = parse_namespace(ns_raw)?;
        let inner_len = decode_map_len(dec, limits, depth + 1)?;
        let mut origins = BTreeMap::new();
        for _ in 0..inner_len {
            let origin = decode_replica_id(dec, limits, "origin_replica_id")?;
            let seq = dec.u64()?;
            origins.insert(origin, Seq0::new(seq));
        }
        out.insert(namespace, origins);
    }
    Ok(out)
}

fn encode_watermark_heads(
    enc: &mut Encoder<&mut Vec<u8>>,
    map: &WatermarkHeads,
) -> Result<(), ProtoEncodeError> {
    enc.map(map.len() as u64)?;
    for (ns, origins) in map {
        enc.str(ns.as_str())?;
        enc.map(origins.len() as u64)?;
        for (origin, sha) in origins {
            encode_replica_id(enc, origin)?;
            enc.bytes(sha.as_bytes())?;
        }
    }
    Ok(())
}

fn decode_watermark_heads(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<WatermarkHeads, ProtoDecodeError> {
    let map_len = decode_map_len(dec, limits, depth)?;
    let mut out = WatermarkHeads::new();
    for _ in 0..map_len {
        let ns_raw = decode_text(dec, limits)?;
        let namespace = parse_namespace(ns_raw)?;
        let inner_len = decode_map_len(dec, limits, depth + 1)?;
        let mut origins = BTreeMap::new();
        for _ in 0..inner_len {
            let origin = decode_replica_id(dec, limits, "origin_replica_id")?;
            let sha = decode_sha256(dec, limits, "sha256")?;
            origins.insert(origin, sha);
        }
        out.insert(namespace, origins);
    }
    Ok(out)
}

fn encode_store_id(
    enc: &mut Encoder<&mut Vec<u8>>,
    store_id: &StoreId,
) -> Result<(), ProtoEncodeError> {
    let raw = store_id.as_uuid().to_string();
    enc.str(&raw)?;
    Ok(())
}

fn encode_replica_id(
    enc: &mut Encoder<&mut Vec<u8>>,
    replica_id: &ReplicaId,
) -> Result<(), ProtoEncodeError> {
    let raw = replica_id.as_uuid().to_string();
    enc.str(&raw)?;
    Ok(())
}

fn decode_store_id(
    dec: &mut Decoder,
    limits: &Limits,
    field: &'static str,
) -> Result<StoreId, ProtoDecodeError> {
    let raw = decode_text(dec, limits)?;
    StoreId::parse_str(raw).map_err(|e| ProtoDecodeError::InvalidField {
        field,
        reason: e.to_string(),
    })
}

fn decode_replica_id(
    dec: &mut Decoder,
    limits: &Limits,
    field: &'static str,
) -> Result<ReplicaId, ProtoDecodeError> {
    let raw = decode_text(dec, limits)?;
    ReplicaId::parse_str(raw).map_err(|e| ProtoDecodeError::InvalidField {
        field,
        reason: e.to_string(),
    })
}

fn parse_namespace(raw: &str) -> Result<NamespaceId, ProtoDecodeError> {
    NamespaceId::parse(raw.to_string()).map_err(|e| ProtoDecodeError::InvalidField {
        field: "namespace",
        reason: e.to_string(),
    })
}

fn decode_sha256(
    dec: &mut Decoder,
    limits: &Limits,
    field: &'static str,
) -> Result<Sha256, ProtoDecodeError> {
    let raw = decode_bytes(dec, limits, field)?;
    let bytes: [u8; 32] = raw.try_into().map_err(|_| ProtoDecodeError::InvalidField {
        field,
        reason: "expected 32-byte sha256".into(),
    })?;
    Ok(Sha256(bytes))
}

fn encode_error_payload(
    enc: &mut Encoder<&mut Vec<u8>>,
    payload: &ErrorPayload,
) -> Result<(), ProtoEncodeError> {
    let mut len = 3;
    if payload.retry_after_ms.is_some() {
        len += 1;
    }
    if payload.details.is_some() {
        len += 1;
    }
    if payload.receipt.is_some() {
        len += 1;
    }
    enc.map(len)?;
    enc.str("code")?;
    enc.str(payload.code.as_str())?;
    enc.str("message")?;
    enc.str(&payload.message)?;
    enc.str("retryable")?;
    enc.bool(payload.retryable)?;
    if let Some(retry) = payload.retry_after_ms {
        enc.str("retry_after_ms")?;
        enc.u64(retry)?;
    }
    if let Some(details) = &payload.details {
        enc.str("details")?;
        encode_json_value(enc, details)?;
    }
    if let Some(receipt) = &payload.receipt {
        enc.str("receipt")?;
        encode_json_value(enc, receipt)?;
    }
    Ok(())
}

fn decode_error_payload(
    dec: &mut Decoder,
    limits: &Limits,
) -> Result<ErrorPayload, ProtoDecodeError> {
    let map_len = decode_map_len(dec, limits, 0)?;
    let mut code = None;
    let mut message = None;
    let mut retryable = None;
    let mut retry_after_ms = None;
    let mut details = None;
    let mut receipt = None;

    for _ in 0..map_len {
        let key = decode_text(dec, limits)?;
        match key {
            "code" => {
                let raw = decode_text(dec, limits)?;
                code = Some(ErrorCode::parse(raw));
            }
            "message" => message = Some(decode_text(dec, limits)?.to_string()),
            "retryable" => retryable = Some(dec.bool()?),
            "retry_after_ms" => retry_after_ms = Some(dec.u64()?),
            "details" => {
                details = Some(decode_json_value(dec, limits, 1, "details")?);
            }
            "receipt" => {
                receipt = Some(decode_json_value(dec, limits, 1, "receipt")?);
            }
            _ => {
                if is_indefinite(dec)? {
                    return Err(ProtoDecodeError::IndefiniteLength);
                }
                dec.skip()?;
            }
        }
    }

    Ok(ErrorPayload {
        code: code.ok_or(ProtoDecodeError::MissingField("code"))?,
        message: message.ok_or(ProtoDecodeError::MissingField("message"))?,
        retryable: retryable.ok_or(ProtoDecodeError::MissingField("retryable"))?,
        retry_after_ms,
        details,
        receipt,
    })
}

fn encode_json_value(
    enc: &mut Encoder<&mut Vec<u8>>,
    value: &Value,
) -> Result<(), ProtoEncodeError> {
    match value {
        Value::Null => {
            enc.null()?;
        }
        Value::Bool(val) => {
            enc.bool(*val)?;
        }
        Value::Number(num) => {
            if let Some(n) = num.as_i64() {
                enc.i64(n)?;
            } else if let Some(n) = num.as_u64() {
                enc.u64(n)?;
            } else if let Some(n) = num.as_f64() {
                enc.f64(n)?;
            }
        }
        Value::String(s) => {
            enc.str(s)?;
        }
        Value::Array(items) => {
            enc.array(items.len() as u64)?;
            for item in items {
                encode_json_value(enc, item)?;
            }
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            enc.map(keys.len() as u64)?;
            for key in keys {
                enc.str(key)?;
                encode_json_value(enc, &map[key])?;
            }
        }
    }
    Ok(())
}

fn decode_json_value(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
    field: &'static str,
) -> Result<Value, ProtoDecodeError> {
    ensure_depth(limits, depth)?;
    let ty = dec.datatype()?;
    match ty {
        Type::Null => {
            dec.null()?;
            Ok(Value::Null)
        }
        Type::Bool => Ok(Value::Bool(dec.bool()?)),
        Type::U8 | Type::U16 | Type::U32 | Type::U64 => Ok(Value::Number(dec.u64()?.into())),
        Type::I8 | Type::I16 | Type::I32 | Type::I64 => Ok(Value::Number(dec.i64()?.into())),
        Type::Int => Err(ProtoDecodeError::InvalidField {
            field,
            reason: "integer out of range".into(),
        }),
        Type::F16 | Type::F32 | Type::F64 => {
            let value = dec.f64()?;
            let num = serde_json::Number::from_f64(value).ok_or_else(|| {
                ProtoDecodeError::InvalidField {
                    field,
                    reason: "float is not finite".into(),
                }
            })?;
            Ok(Value::Number(num))
        }
        Type::String => {
            let s = decode_text(dec, limits)?;
            Ok(Value::String(s.to_string()))
        }
        Type::Bytes => Err(ProtoDecodeError::InvalidField {
            field,
            reason: "bytes not supported in error payload".into(),
        }),
        Type::Array => {
            let len = decode_array_len(dec, limits, depth)?;
            let mut out = Vec::with_capacity(len);
            for _ in 0..len {
                out.push(decode_json_value(dec, limits, depth + 1, field)?);
            }
            Ok(Value::Array(out))
        }
        Type::Map => {
            let len = decode_map_len(dec, limits, depth)?;
            let mut out = serde_json::Map::with_capacity(len);
            for _ in 0..len {
                let key = decode_text(dec, limits)?;
                let value = decode_json_value(dec, limits, depth + 1, field)?;
                out.insert(key.to_string(), value);
            }
            Ok(Value::Object(out))
        }
        Type::Tag => Err(ProtoDecodeError::InvalidField {
            field,
            reason: "tags not supported in error payload".into(),
        }),
        Type::Undefined | Type::Simple | Type::Break => Err(ProtoDecodeError::InvalidField {
            field,
            reason: "unsupported CBOR type".into(),
        }),
        Type::BytesIndef | Type::StringIndef | Type::ArrayIndef | Type::MapIndef => {
            Err(ProtoDecodeError::IndefiniteLength)
        }
        Type::Unknown(_) => Err(ProtoDecodeError::InvalidField {
            field,
            reason: "unknown CBOR type".into(),
        }),
    }
}

fn decode_map_len(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<usize, ProtoDecodeError> {
    ensure_depth(limits, depth)?;
    let len = dec.map()?;
    let Some(len) = len else {
        return Err(ProtoDecodeError::IndefiniteLength);
    };
    if len > limits.max_cbor_map_entries as u64 {
        return Err(ProtoDecodeError::DecodeLimit("max_cbor_map_entries"));
    }
    usize::try_from(len).map_err(|_| ProtoDecodeError::DecodeLimit("max_cbor_map_entries"))
}

fn decode_array_len(
    dec: &mut Decoder,
    limits: &Limits,
    depth: usize,
) -> Result<usize, ProtoDecodeError> {
    ensure_depth(limits, depth)?;
    let len = dec.array()?;
    let Some(len) = len else {
        return Err(ProtoDecodeError::IndefiniteLength);
    };
    if len > limits.max_cbor_array_entries as u64 {
        return Err(ProtoDecodeError::DecodeLimit("max_cbor_array_entries"));
    }
    usize::try_from(len).map_err(|_| ProtoDecodeError::DecodeLimit("max_cbor_array_entries"))
}

fn decode_text<'a>(dec: &mut Decoder<'a>, limits: &Limits) -> Result<&'a str, ProtoDecodeError> {
    let ty = dec.datatype()?;
    if matches!(ty, Type::StringIndef) {
        return Err(ProtoDecodeError::IndefiniteLength);
    }
    let s = dec.str()?;
    if s.len() > limits.max_cbor_text_string_len {
        return Err(ProtoDecodeError::DecodeLimit("max_cbor_text_string_len"));
    }
    Ok(s)
}

fn decode_bytes<'a>(
    dec: &mut Decoder<'a>,
    limits: &Limits,
    field: &'static str,
) -> Result<&'a [u8], ProtoDecodeError> {
    let ty = dec.datatype()?;
    if matches!(ty, Type::BytesIndef) {
        return Err(ProtoDecodeError::IndefiniteLength);
    }
    let bytes = dec.bytes()?;
    if bytes.len() > limits.max_cbor_bytes_string_len {
        return Err(ProtoDecodeError::InvalidField {
            field,
            reason: "bytes length exceeds limit".into(),
        });
    }
    Ok(bytes)
}

fn decode_u32(dec: &mut Decoder, field: &'static str) -> Result<u32, ProtoDecodeError> {
    let value = dec.u64()?;
    u32::try_from(value).map_err(|_| ProtoDecodeError::InvalidField {
        field,
        reason: format!("value {value} out of range for u32"),
    })
}

fn ensure_depth(limits: &Limits, depth: usize) -> Result<(), ProtoDecodeError> {
    if depth > limits.max_cbor_depth {
        return Err(ProtoDecodeError::DecodeLimit("max_cbor_depth"));
    }
    Ok(())
}

fn is_indefinite(dec: &Decoder) -> Result<bool, ProtoDecodeError> {
    let ty = dec.datatype()?;
    Ok(matches!(
        ty,
        Type::BytesIndef | Type::StringIndef | Type::ArrayIndef | Type::MapIndef
    ))
}

fn reason_string(err: &ProtoDecodeError) -> String {
    match err {
        ProtoDecodeError::DecodeLimit(reason) => reason.to_string(),
        ProtoDecodeError::IndefiniteLength => "indefinite-length CBOR".to_string(),
        ProtoDecodeError::TrailingBytes => "trailing bytes".to_string(),
        ProtoDecodeError::Cbor(e) => e.to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        ActorId, BeadId, ClientRequestId, EventBody, EventKindV1, HlcMax, NamespaceId,
        NoteAppendV1, ReplicaId, StoreIdentity, TxnDeltaV1, TxnId, TxnV1, WireBeadPatch,
        WireNoteV1, WireStamp, encode_event_body_canonical, hash_event_body,
    };
    use uuid::Uuid;

    fn sample_event_body(seq: u64) -> EventBody {
        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::new(2));
        let origin = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let txn_id = TxnId::new(Uuid::from_bytes([3u8; 16]));
        let client_request_id = ClientRequestId::new(Uuid::from_bytes([4u8; 16]));

        let mut patch = WireBeadPatch::new(BeadId::parse("bd-test1").unwrap());
        patch.created_at = Some(WireStamp(10, 1));
        patch.created_by = Some(ActorId::new("alice".to_string()).unwrap());
        patch.title = Some("title".to_string());

        let note = WireNoteV1 {
            id: crate::core::NoteId::new("note-1".to_string()).unwrap(),
            content: "hello".to_string(),
            author: ActorId::new("alice".to_string()).unwrap(),
            at: WireStamp(11, 2),
        };

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(crate::core::TxnOpV1::BeadUpsert(Box::new(patch)))
            .unwrap();
        delta
            .insert(crate::core::TxnOpV1::NoteAppend(NoteAppendV1 {
                bead_id: BeadId::parse("bd-test1").unwrap(),
                note,
            }))
            .unwrap();

        EventBody {
            envelope_v: 1,
            store,
            namespace: NamespaceId::core(),
            origin_replica_id: origin,
            origin_seq: Seq1::from_u64(seq).unwrap(),
            event_time_ms: 123,
            txn_id,
            client_request_id: Some(client_request_id),
            kind: EventKindV1::TxnV1(TxnV1 {
                delta,
                hlc_max: HlcMax {
                    actor_id: ActorId::new("alice".to_string()).unwrap(),
                    physical_ms: 123,
                    logical: 1,
                },
            }),
        }
    }

    fn sample_event_frame(seq: u64, prev: Option<Sha256>) -> EventFrameV1 {
        let body = sample_event_body(seq);
        let bytes = encode_event_body_canonical(&body).unwrap();
        let sha256 = hash_event_body(&bytes);
        EventFrameV1 {
            eid: EventId::new(body.origin_replica_id, body.namespace, body.origin_seq),
            sha256,
            prev_sha256: prev,
            bytes: bytes.into(),
        }
    }

    fn sample_hello() -> Hello {
        Hello {
            protocol_version: PROTOCOL_VERSION_V1,
            min_protocol_version: PROTOCOL_VERSION_V1,
            store_id: StoreId::new(Uuid::from_bytes([9u8; 16])),
            store_epoch: StoreEpoch::new(5),
            sender_replica_id: ReplicaId::new(Uuid::from_bytes([8u8; 16])),
            hello_nonce: 42,
            max_frame_bytes: 1_024,
            requested_namespaces: vec![NamespaceId::core()],
            offered_namespaces: vec![NamespaceId::core()],
            seen_durable: WatermarkMap::new(),
            seen_durable_heads: None,
            seen_applied: None,
            seen_applied_heads: None,
            capabilities: Capabilities {
                supports_snapshots: false,
                supports_live_stream: true,
                supports_compression: false,
            },
        }
    }

    fn sample_welcome() -> Welcome {
        Welcome {
            protocol_version: PROTOCOL_VERSION_V1,
            store_id: StoreId::new(Uuid::from_bytes([9u8; 16])),
            store_epoch: StoreEpoch::new(5),
            receiver_replica_id: ReplicaId::new(Uuid::from_bytes([8u8; 16])),
            welcome_nonce: 24,
            accepted_namespaces: vec![NamespaceId::core()],
            receiver_seen_durable: WatermarkMap::new(),
            receiver_seen_durable_heads: None,
            receiver_seen_applied: None,
            receiver_seen_applied_heads: None,
            live_stream_enabled: true,
            max_frame_bytes: 2_048,
        }
    }

    #[test]
    fn repl_message_roundtrip_hello() {
        let envelope = ReplEnvelope {
            version: PROTOCOL_VERSION_V1,
            message: ReplMessage::Hello(sample_hello()),
        };
        let bytes = encode_envelope(&envelope).unwrap();
        let decoded = decode_envelope(&bytes, &Limits::default()).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn repl_message_roundtrip_welcome() {
        let envelope = ReplEnvelope {
            version: PROTOCOL_VERSION_V1,
            message: ReplMessage::Welcome(sample_welcome()),
        };
        let bytes = encode_envelope(&envelope).unwrap();
        let decoded = decode_envelope(&bytes, &Limits::default()).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn repl_message_roundtrip_events() {
        let frame1 = sample_event_frame(1, None);
        let frame2 = sample_event_frame(2, Some(frame1.sha256));
        let envelope = ReplEnvelope {
            version: PROTOCOL_VERSION_V1,
            message: ReplMessage::Events(Events {
                events: vec![frame1, frame2],
            }),
        };
        let bytes = encode_envelope(&envelope).unwrap();
        let decoded = decode_envelope(&bytes, &Limits::default()).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn repl_message_roundtrip_ack() {
        let envelope = ReplEnvelope {
            version: PROTOCOL_VERSION_V1,
            message: ReplMessage::Ack(Ack {
                durable: WatermarkMap::new(),
                durable_heads: None,
                applied: None,
                applied_heads: None,
            }),
        };
        let bytes = encode_envelope(&envelope).unwrap();
        let decoded = decode_envelope(&bytes, &Limits::default()).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn repl_message_roundtrip_want() {
        let envelope = ReplEnvelope {
            version: PROTOCOL_VERSION_V1,
            message: ReplMessage::Want(Want {
                want: WatermarkMap::new(),
            }),
        };
        let bytes = encode_envelope(&envelope).unwrap();
        let decoded = decode_envelope(&bytes, &Limits::default()).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn repl_message_roundtrip_ping() {
        let envelope = ReplEnvelope {
            version: PROTOCOL_VERSION_V1,
            message: ReplMessage::Ping(Ping { nonce: 7 }),
        };
        let bytes = encode_envelope(&envelope).unwrap();
        let decoded = decode_envelope(&bytes, &Limits::default()).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn repl_message_roundtrip_pong() {
        let envelope = ReplEnvelope {
            version: PROTOCOL_VERSION_V1,
            message: ReplMessage::Pong(Pong { nonce: 9 }),
        };
        let bytes = encode_envelope(&envelope).unwrap();
        let decoded = decode_envelope(&bytes, &Limits::default()).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn repl_message_roundtrip_error_payload() {
        let payload = ErrorPayload::new(ProtocolErrorCode::Overloaded.into(), "busy", true).with_details(
            crate::core::error::details::OverloadedDetails {
                subsystem: None,
                retry_after_ms: Some(10),
                queue_bytes: Some(5),
                queue_events: Some(1),
            },
        );
        let envelope = ReplEnvelope {
            version: PROTOCOL_VERSION_V1,
            message: ReplMessage::Error(payload),
        };
        let bytes = encode_envelope(&envelope).unwrap();
        let decoded = decode_envelope(&bytes, &Limits::default()).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn repl_batch_too_large_maps_to_error_payload() {
        let frame = sample_event_frame(1, None);
        let envelope = ReplEnvelope {
            version: PROTOCOL_VERSION_V1,
            message: ReplMessage::Events(Events {
                events: vec![frame],
            }),
        };
        let bytes = encode_envelope(&envelope).unwrap();
        let mut limits = Limits::default();
        limits.max_event_batch_bytes = 1;
        let err = decode_envelope(&bytes, &limits).unwrap_err();
        let payload = err.as_error_payload().unwrap();
        assert_eq!(payload.code, ProtocolErrorCode::BatchTooLarge.into());
        let details: BatchTooLargeDetails = payload.details_as().unwrap().unwrap();
        assert_eq!(details.max_bytes, 1);
        assert!(details.got_bytes > 1);
    }
}
