//! WAL record header encoding/decoding (v0.5 framing).

use bytes::Bytes;
use std::marker::PhantomData;
use thiserror::Error;
use uuid::Uuid;

use crate::core::{ClientRequestId, EventBody, ReplicaId, Seq1, TxnId, sha256_bytes};

use super::{EventWalError, EventWalResult};

const RECORD_HEADER_VERSION: u16 = 1;
pub(crate) const RECORD_HEADER_BASE_LEN: usize = 88;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecordFlags {
    pub has_prev_sha: bool,
    pub has_client_request_id: bool,
    pub has_request_sha256: bool,
}

impl RecordFlags {
    fn to_bits(self) -> u16 {
        let mut bits = 0u16;
        if self.has_prev_sha {
            bits |= 1 << 0;
        }
        if self.has_client_request_id {
            bits |= 1 << 1;
        }
        if self.has_request_sha256 {
            bits |= 1 << 2;
        }
        bits
    }

    fn from_bits(bits: u16) -> EventWalResult<Self> {
        if bits & !0b111 != 0 {
            return Err(EventWalError::RecordHeaderInvalid {
                reason: format!("unknown flags bits {bits:#x}"),
            });
        }
        Ok(Self {
            has_prev_sha: bits & (1 << 0) != 0,
            has_client_request_id: bits & (1 << 1) != 0,
            has_request_sha256: bits & (1 << 2) != 0,
        })
    }

    fn expected_len(self) -> usize {
        let mut len = RECORD_HEADER_BASE_LEN;
        if self.has_client_request_id {
            len += 16;
        }
        if self.has_request_sha256 {
            len += 32;
        }
        if self.has_prev_sha {
            len += 32;
        }
        len
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordHeader {
    pub origin_replica_id: ReplicaId,
    pub origin_seq: Seq1,
    pub event_time_ms: u64,
    pub txn_id: TxnId,
    pub client_request_id: Option<ClientRequestId>,
    pub request_sha256: Option<[u8; 32]>,
    pub sha256: [u8; 32],
    pub prev_sha256: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RecordHeaderMismatch {
    #[error("origin_replica_id mismatch (header {header}, body {body})")]
    OriginReplicaId { header: ReplicaId, body: ReplicaId },
    #[error("origin_seq mismatch (header {header}, body {body})")]
    OriginSeq { header: Seq1, body: Seq1 },
    #[error("event_time_ms mismatch (header {header}, body {body})")]
    EventTimeMs { header: u64, body: u64 },
    #[error("txn_id mismatch (header {header}, body {body})")]
    TxnId { header: TxnId, body: TxnId },
    #[error("client_request_id mismatch (header {header:?}, body {body:?})")]
    ClientRequestId {
        header: Option<ClientRequestId>,
        body: Option<ClientRequestId>,
    },
}

impl RecordHeader {
    pub fn flags(&self) -> RecordFlags {
        RecordFlags {
            has_prev_sha: self.prev_sha256.is_some(),
            has_client_request_id: self.client_request_id.is_some(),
            has_request_sha256: self.request_sha256.is_some(),
        }
    }

    pub fn encode(&self) -> EventWalResult<Vec<u8>> {
        if self.request_sha256.is_some() && self.client_request_id.is_none() {
            return Err(EventWalError::RecordHeaderInvalid {
                reason: "request_sha256 requires client_request_id".to_string(),
            });
        }

        let flags = self.flags();
        let header_len = flags.expected_len();
        let header_len_u16 =
            u16::try_from(header_len).map_err(|_| EventWalError::RecordHeaderInvalid {
                reason: "record header too large".to_string(),
            })?;

        let mut buf = Vec::with_capacity(header_len);
        buf.extend_from_slice(&RECORD_HEADER_VERSION.to_le_bytes());
        buf.extend_from_slice(&header_len_u16.to_le_bytes());
        buf.extend_from_slice(&flags.to_bits().to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(self.origin_replica_id.as_uuid().as_bytes());
        buf.extend_from_slice(&self.origin_seq.get().to_le_bytes());
        buf.extend_from_slice(&self.event_time_ms.to_le_bytes());
        buf.extend_from_slice(self.txn_id.as_uuid().as_bytes());

        if let Some(client_request_id) = self.client_request_id {
            buf.extend_from_slice(client_request_id.as_uuid().as_bytes());
        }
        if let Some(request_sha256) = self.request_sha256 {
            buf.extend_from_slice(&request_sha256);
        }

        buf.extend_from_slice(&self.sha256);

        if let Some(prev_sha256) = self.prev_sha256 {
            buf.extend_from_slice(&prev_sha256);
        }

        debug_assert_eq!(buf.len(), header_len);
        Ok(buf)
    }

    pub fn decode(bytes: &[u8]) -> EventWalResult<(Self, usize)> {
        if bytes.len() < RECORD_HEADER_BASE_LEN {
            return Err(EventWalError::RecordHeaderInvalid {
                reason: "record header truncated".to_string(),
            });
        }

        let mut offset = 0usize;
        let header_version = read_u16_le(bytes, &mut offset)?;
        if header_version != RECORD_HEADER_VERSION {
            return Err(EventWalError::RecordHeaderInvalid {
                reason: format!("unsupported record header version {header_version}"),
            });
        }

        let header_len = read_u16_le(bytes, &mut offset)? as usize;
        if header_len < RECORD_HEADER_BASE_LEN {
            return Err(EventWalError::RecordHeaderInvalid {
                reason: format!("record header too short {header_len}"),
            });
        }
        if bytes.len() < header_len {
            return Err(EventWalError::RecordHeaderInvalid {
                reason: "record header length exceeds frame".to_string(),
            });
        }

        let flags_bits = read_u16_le(bytes, &mut offset)?;
        let reserved = read_u16_le(bytes, &mut offset)?;
        if reserved != 0 {
            return Err(EventWalError::RecordHeaderInvalid {
                reason: format!("record header reserved field not zero ({reserved})"),
            });
        }
        let flags = RecordFlags::from_bits(flags_bits)?;
        if flags.has_request_sha256 && !flags.has_client_request_id {
            return Err(EventWalError::RecordHeaderInvalid {
                reason: "request_sha256 flag set without client_request_id".to_string(),
            });
        }
        if header_len < flags.expected_len() {
            return Err(EventWalError::RecordHeaderInvalid {
                reason: "record header length smaller than flags imply".to_string(),
            });
        }

        let origin_replica_id = ReplicaId::new(read_uuid(bytes, &mut offset)?);
        let origin_seq_raw = read_u64_le(bytes, &mut offset)?;
        let origin_seq =
            Seq1::from_u64(origin_seq_raw).ok_or_else(|| EventWalError::RecordHeaderInvalid {
                reason: "origin_seq must be >= 1".to_string(),
            })?;
        let event_time_ms = read_u64_le(bytes, &mut offset)?;
        let txn_id = TxnId::new(read_uuid(bytes, &mut offset)?);

        let client_request_id = if flags.has_client_request_id {
            Some(ClientRequestId::new(read_uuid(bytes, &mut offset)?))
        } else {
            None
        };

        let request_sha256 = if flags.has_request_sha256 {
            Some(read_array::<32>(bytes, &mut offset)?)
        } else {
            None
        };

        let sha256 = read_array::<32>(bytes, &mut offset)?;
        let prev_sha256 = if flags.has_prev_sha {
            Some(read_array::<32>(bytes, &mut offset)?)
        } else {
            None
        };

        if offset > header_len {
            return Err(EventWalError::RecordHeaderInvalid {
                reason: "record header overran declared length".to_string(),
            });
        }

        Ok((
            RecordHeader {
                origin_replica_id,
                origin_seq,
                event_time_ms,
                txn_id,
                client_request_id,
                request_sha256,
                sha256,
                prev_sha256,
            },
            header_len,
        ))
    }
}

pub fn validate_header_matches_body(
    header: &RecordHeader,
    body: &EventBody,
) -> Result<(), RecordHeaderMismatch> {
    if header.origin_replica_id != body.origin_replica_id {
        return Err(RecordHeaderMismatch::OriginReplicaId {
            header: header.origin_replica_id,
            body: body.origin_replica_id,
        });
    }
    if header.origin_seq != body.origin_seq {
        return Err(RecordHeaderMismatch::OriginSeq {
            header: header.origin_seq,
            body: body.origin_seq,
        });
    }
    if header.event_time_ms != body.event_time_ms {
        return Err(RecordHeaderMismatch::EventTimeMs {
            header: header.event_time_ms,
            body: body.event_time_ms,
        });
    }
    if header.txn_id != body.txn_id {
        return Err(RecordHeaderMismatch::TxnId {
            header: header.txn_id,
            body: body.txn_id,
        });
    }
    if header.client_request_id != body.client_request_id {
        return Err(RecordHeaderMismatch::ClientRequestId {
            header: header.client_request_id,
            body: body.client_request_id,
        });
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unverified;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verified;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record<State> {
    header: RecordHeader,
    payload: Bytes,
    _state: PhantomData<State>,
}

pub type UnverifiedRecord = Record<Unverified>;
pub type VerifiedRecord = Record<Verified>;

impl<State> Record<State> {
    pub fn header(&self) -> &RecordHeader {
        &self.header
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn payload_bytes(&self) -> &[u8] {
        self.payload.as_ref()
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RecordVerifyError {
    #[error(transparent)]
    HeaderMismatch(#[from] RecordHeaderMismatch),
    #[error("record sha256 mismatch (expected {expected:?}, got {got:?})")]
    ShaMismatch { expected: [u8; 32], got: [u8; 32] },
}

impl Record<Unverified> {
    pub(crate) fn new(header: RecordHeader, payload: Bytes) -> Self {
        Self {
            header,
            payload,
            _state: PhantomData,
        }
    }

    pub fn decode_body(body: &[u8]) -> EventWalResult<Self> {
        let (header, header_len) = RecordHeader::decode(body)?;
        let payload = Bytes::copy_from_slice(&body[header_len..]);
        Ok(Self::new(header, payload))
    }

    pub fn verify_with_event_body(
        self,
        body: &EventBody,
    ) -> Result<Record<Verified>, RecordVerifyError> {
        validate_header_matches_body(&self.header, body)?;
        let expected = sha256_bytes(self.payload.as_ref()).0;
        if expected != self.header.sha256 {
            return Err(RecordVerifyError::ShaMismatch {
                expected,
                got: self.header.sha256,
            });
        }
        Ok(Record {
            header: self.header,
            payload: self.payload,
            _state: PhantomData,
        })
    }
}

impl Record<Verified> {
    pub(crate) fn new(
        header: RecordHeader,
        payload: Bytes,
        event_body: &EventBody,
    ) -> Result<Self, RecordVerifyError> {
        Record::<Unverified>::new(header, payload).verify_with_event_body(event_body)
    }

    pub fn encode_body(&self) -> EventWalResult<Vec<u8>> {
        let header = self.header.encode()?;
        let mut buf = Vec::with_capacity(header.len() + self.payload.len());
        buf.extend_from_slice(&header);
        buf.extend_from_slice(self.payload.as_ref());
        Ok(buf)
    }
}

fn read_u16_le(bytes: &[u8], offset: &mut usize) -> EventWalResult<u16> {
    let slice = take(bytes, offset, 2)?;
    Ok(u16::from_le_bytes([slice[0], slice[1]]))
}

fn read_u64_le(bytes: &[u8], offset: &mut usize) -> EventWalResult<u64> {
    let slice = take(bytes, offset, 8)?;
    Ok(u64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]))
}

fn read_uuid(bytes: &[u8], offset: &mut usize) -> EventWalResult<Uuid> {
    let slice = read_array::<16>(bytes, offset)?;
    Ok(Uuid::from_bytes(slice))
}

fn read_array<const N: usize>(bytes: &[u8], offset: &mut usize) -> EventWalResult<[u8; N]> {
    let slice = take(bytes, offset, N)?;
    let mut out = [0u8; N];
    out.copy_from_slice(slice);
    Ok(out)
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> EventWalResult<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| EventWalError::RecordHeaderInvalid {
            reason: "record header length overflow".to_string(),
        })?;
    if end > bytes.len() {
        return Err(EventWalError::RecordHeaderInvalid {
            reason: "record header truncated".to_string(),
        });
    }
    let slice = &bytes[*offset..end];
    *offset = end;
    Ok(slice)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        ActorId, EventKindV1, HlcMax, NamespaceId, StoreEpoch, StoreId, StoreIdentity, TxnDeltaV1,
        TxnV1,
    };

    fn event_body_for_header(header: &RecordHeader) -> EventBody {
        EventBody {
            envelope_v: 1,
            store: StoreIdentity::new(StoreId::new(Uuid::from_bytes([9u8; 16])), StoreEpoch::ZERO),
            namespace: NamespaceId::core(),
            origin_replica_id: header.origin_replica_id,
            origin_seq: header.origin_seq,
            event_time_ms: header.event_time_ms,
            txn_id: header.txn_id,
            client_request_id: header.client_request_id,
            kind: EventKindV1::TxnV1(TxnV1 {
                delta: TxnDeltaV1::new(),
                hlc_max: HlcMax {
                    actor_id: ActorId::new("alice").unwrap(),
                    physical_ms: header.event_time_ms,
                    logical: 0,
                },
            }),
        }
    }

    #[test]
    fn record_roundtrip_with_optional_fields() {
        let payload = Bytes::from_static(b"hello");
        let sha = sha256_bytes(payload.as_ref()).0;
        let header = RecordHeader {
            origin_replica_id: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            origin_seq: Seq1::from_u64(42).unwrap(),
            event_time_ms: 1_700_000_000_000,
            txn_id: TxnId::new(Uuid::from_bytes([2u8; 16])),
            client_request_id: Some(ClientRequestId::new(Uuid::from_bytes([3u8; 16]))),
            request_sha256: Some([4u8; 32]),
            sha256: sha,
            prev_sha256: Some([6u8; 32]),
        };
        let event_body = event_body_for_header(&header);
        let record = VerifiedRecord::new(header.clone(), payload.clone(), &event_body).unwrap();

        let body = record.encode_body().unwrap();
        let decoded = UnverifiedRecord::decode_body(&body).unwrap();
        let verified = decoded.verify_with_event_body(&event_body).unwrap();
        assert_eq!(verified.header(), &header);
        assert_eq!(verified.payload(), &payload);
    }

    #[test]
    fn record_encode_rejects_request_sha_without_client_request_id() {
        let header = RecordHeader {
            origin_replica_id: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            origin_seq: Seq1::from_u64(42).unwrap(),
            event_time_ms: 1_700_000_000_000,
            txn_id: TxnId::new(Uuid::from_bytes([2u8; 16])),
            client_request_id: None,
            request_sha256: Some([4u8; 32]),
            sha256: [5u8; 32],
            prev_sha256: None,
        };
        let err = header.encode().unwrap_err();
        assert!(matches!(err, EventWalError::RecordHeaderInvalid { .. }));
    }

    #[test]
    fn record_decode_rejects_request_sha_without_client_request_id() {
        let origin = Uuid::from_bytes([1u8; 16]);
        let txn_id = Uuid::from_bytes([2u8; 16]);
        let header_len = u16::try_from(RECORD_HEADER_BASE_LEN + 32).unwrap();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&RECORD_HEADER_VERSION.to_le_bytes());
        bytes.extend_from_slice(&header_len.to_le_bytes());
        bytes.extend_from_slice(&(1u16 << 2).to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(origin.as_bytes());
        bytes.extend_from_slice(&42u64.to_le_bytes());
        bytes.extend_from_slice(&1_700_000_000_000u64.to_le_bytes());
        bytes.extend_from_slice(txn_id.as_bytes());
        bytes.extend_from_slice(&[4u8; 32]);
        bytes.extend_from_slice(&[5u8; 32]);

        let err = RecordHeader::decode(&bytes).unwrap_err();
        assert!(matches!(err, EventWalError::RecordHeaderInvalid { .. }));
    }

    #[test]
    fn record_decode_rejects_seq0() {
        let origin = Uuid::from_bytes([1u8; 16]);
        let txn_id = Uuid::from_bytes([2u8; 16]);
        let header_len = u16::try_from(RECORD_HEADER_BASE_LEN).unwrap();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&RECORD_HEADER_VERSION.to_le_bytes());
        bytes.extend_from_slice(&header_len.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(origin.as_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&1_700_000_000_000u64.to_le_bytes());
        bytes.extend_from_slice(txn_id.as_bytes());
        bytes.extend_from_slice(&[5u8; 32]);

        let err = RecordHeader::decode(&bytes).unwrap_err();
        assert!(matches!(err, EventWalError::RecordHeaderInvalid { .. }));
    }

    #[test]
    fn record_verify_rejects_header_mismatch() {
        let payload = Bytes::from_static(b"payload");
        let sha = sha256_bytes(payload.as_ref()).0;
        let header = RecordHeader {
            origin_replica_id: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            origin_seq: Seq1::from_u64(1).unwrap(),
            event_time_ms: 1_700_000_000_000,
            txn_id: TxnId::new(Uuid::from_bytes([2u8; 16])),
            client_request_id: None,
            request_sha256: None,
            sha256: sha,
            prev_sha256: None,
        };
        let mut event_body = event_body_for_header(&header);
        event_body.txn_id = TxnId::new(Uuid::from_bytes([3u8; 16]));

        let record = UnverifiedRecord::new(header, payload);
        let err = record.verify_with_event_body(&event_body).unwrap_err();
        assert!(matches!(err, RecordVerifyError::HeaderMismatch(_)));
    }
}
