//! WAL record header encoding/decoding (v0.5 framing).

use bytes::Bytes;
use uuid::Uuid;

use crate::core::{ClientRequestId, ReplicaId, TxnId};

use super::{EventWalError, EventWalResult};

const RECORD_HEADER_VERSION: u16 = 1;
const RECORD_HEADER_BASE_LEN: usize = 88;

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
    pub origin_seq: u64,
    pub event_time_ms: u64,
    pub txn_id: TxnId,
    pub client_request_id: Option<ClientRequestId>,
    pub request_sha256: Option<[u8; 32]>,
    pub sha256: [u8; 32],
    pub prev_sha256: Option<[u8; 32]>,
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
        buf.extend_from_slice(&self.origin_seq.to_le_bytes());
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
        if header_len < flags.expected_len() {
            return Err(EventWalError::RecordHeaderInvalid {
                reason: "record header length smaller than flags imply".to_string(),
            });
        }

        let origin_replica_id = ReplicaId::new(read_uuid(bytes, &mut offset)?);
        let origin_seq = read_u64_le(bytes, &mut offset)?;
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub header: RecordHeader,
    pub payload: Bytes,
}

impl Record {
    pub fn encode_body(&self) -> EventWalResult<Vec<u8>> {
        let header = self.header.encode()?;
        let mut buf = Vec::with_capacity(header.len() + self.payload.len());
        buf.extend_from_slice(&header);
        buf.extend_from_slice(self.payload.as_ref());
        Ok(buf)
    }

    pub fn decode_body(body: &[u8]) -> EventWalResult<Self> {
        let (header, header_len) = RecordHeader::decode(body)?;
        let payload = Bytes::copy_from_slice(&body[header_len..]);
        Ok(Self { header, payload })
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

    #[test]
    fn record_roundtrip_with_optional_fields() {
        let header = RecordHeader {
            origin_replica_id: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            origin_seq: 42,
            event_time_ms: 1_700_000_000_000,
            txn_id: TxnId::new(Uuid::from_bytes([2u8; 16])),
            client_request_id: Some(ClientRequestId::new(Uuid::from_bytes([3u8; 16]))),
            request_sha256: Some([4u8; 32]),
            sha256: [5u8; 32],
            prev_sha256: Some([6u8; 32]),
        };
        let record = Record {
            header: header.clone(),
            payload: Bytes::from_static(b"hello"),
        };

        let body = record.encode_body().unwrap();
        let decoded = Record::decode_body(&body).unwrap();
        assert_eq!(decoded.header, header);
        assert_eq!(decoded.payload, record.payload);
    }
}
