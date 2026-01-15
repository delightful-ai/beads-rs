//! Record framing (magic + length + crc32c).

use std::io::{Read, Write};

use crc32c::crc32c;

use super::{EventWalError, EventWalResult};
use crate::daemon::wal::record::{UnverifiedRecord, VerifiedRecord};

pub(crate) const FRAME_MAGIC: u32 = 0x4244_5232; // "BDR2"
pub(crate) const FRAME_HEADER_LEN: usize = 12;

pub struct FrameReader<R> {
    reader: R,
    max_record_bytes: usize,
}

impl<R: Read> FrameReader<R> {
    pub fn new(reader: R, max_record_bytes: usize) -> Self {
        Self {
            reader,
            max_record_bytes,
        }
    }

    pub fn read_next(&mut self) -> EventWalResult<Option<UnverifiedRecord>> {
        let mut header = [0u8; FRAME_HEADER_LEN];
        let mut read = 0usize;
        while read < header.len() {
            let n = self
                .reader
                .read(&mut header[read..])
                .map_err(|source| EventWalError::Io { path: None, source })?;
            if n == 0 {
                return Ok(None);
            }
            read += n;
        }

        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        if magic != FRAME_MAGIC {
            return Err(EventWalError::FrameMagicMismatch { got: magic });
        }

        let length = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
        if length == 0 {
            return Err(EventWalError::FrameLengthInvalid {
                reason: "frame length cannot be zero".to_string(),
            });
        }
        if length > self.max_record_bytes {
            return Err(EventWalError::RecordTooLarge {
                max_bytes: self.max_record_bytes,
                got_bytes: length,
            });
        }

        let expected_crc = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let mut body = vec![0u8; length];
        let mut read_body = 0usize;
        while read_body < length {
            let n = self
                .reader
                .read(&mut body[read_body..])
                .map_err(|source| EventWalError::Io { path: None, source })?;
            if n == 0 {
                return Ok(None);
            }
            read_body += n;
        }

        let actual_crc = crc32c(&body);
        if actual_crc != expected_crc {
            return Err(EventWalError::FrameCrcMismatch {
                expected: expected_crc,
                got: actual_crc,
            });
        }

        let record = UnverifiedRecord::decode_body(&body)?;
        Ok(Some(record))
    }
}

pub struct FrameWriter<W> {
    writer: W,
    max_record_bytes: usize,
}

impl<W: Write> FrameWriter<W> {
    pub fn new(writer: W, max_record_bytes: usize) -> Self {
        Self {
            writer,
            max_record_bytes,
        }
    }

    pub fn write_record(&mut self, record: &VerifiedRecord) -> EventWalResult<usize> {
        let frame = encode_frame(record, self.max_record_bytes)?;
        self.writer
            .write_all(&frame)
            .map_err(|source| EventWalError::Io { path: None, source })?;
        Ok(frame.len())
    }
}

pub fn encode_frame(
    record: &VerifiedRecord,
    max_record_bytes: usize,
) -> EventWalResult<Vec<u8>> {
    let body = record.encode_body()?;
    if body.len() > max_record_bytes {
        return Err(EventWalError::RecordTooLarge {
            max_bytes: max_record_bytes,
            got_bytes: body.len(),
        });
    }

    let length = u32::try_from(body.len()).map_err(|_| EventWalError::FrameLengthInvalid {
        reason: "frame length exceeds u32".to_string(),
    })?;
    let crc = crc32c(&body);

    let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + body.len());
    buf.extend_from_slice(&FRAME_MAGIC.to_le_bytes());
    buf.extend_from_slice(&length.to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&body);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        ActorId, ClientRequestId, EventBody, EventKindV1, HlcMax, NamespaceId, ReplicaId, Seq1,
        StoreEpoch, StoreId, StoreIdentity, TxnDeltaV1, TxnId, TxnV1, sha256_bytes,
    };
    use crate::daemon::wal::record::{RecordHeader, VerifiedRecord};
    use bytes::Bytes;
    use std::io::Cursor;
    use uuid::Uuid;

    fn sample_event_body(header: &RecordHeader) -> EventBody {
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

    fn sample_record() -> VerifiedRecord {
        let payload = Bytes::from_static(b"payload");
        let sha = sha256_bytes(payload.as_ref()).0;
        let header = RecordHeader {
            origin_replica_id: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            origin_seq: Seq1::from_u64(7).unwrap(),
            event_time_ms: 1_700_000_000_000,
            txn_id: TxnId::new(Uuid::from_bytes([2u8; 16])),
            client_request_id: Some(ClientRequestId::new(Uuid::from_bytes([3u8; 16]))),
            request_sha256: Some([4u8; 32]),
            sha256: sha,
            prev_sha256: Some([6u8; 32]),
        };
        let body = sample_event_body(&header);
        VerifiedRecord::new(header, payload, &body).expect("verified record")
    }

    #[test]
    fn frame_roundtrip_validates_crc() {
        let record = sample_record();
        let frame = encode_frame(&record, 1024).unwrap();

        let mut reader = FrameReader::new(Cursor::new(frame), 1024);
        let decoded = reader.read_next().unwrap().unwrap();
        let body = sample_event_body(record.header());
        let verified = decoded
            .verify_with_event_body(&body)
            .expect("verify record");
        assert_eq!(verified, record);
    }

    #[test]
    fn frame_crc_mismatch_fails() {
        let record = sample_record();
        let mut frame = encode_frame(&record, 1024).unwrap();
        let last = frame.len() - 1;
        frame[last] ^= 0xFF;

        let mut reader = FrameReader::new(Cursor::new(frame), 1024);
        let err = reader.read_next().unwrap_err();
        assert!(matches!(err, EventWalError::FrameCrcMismatch { .. }));
    }
}
