//! Replication framing (length + crc32c).

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use beads_core::error::details::FrameTooLargeDetails;
use beads_core::{ErrorPayload, ProtocolErrorCode};
use crc32c::crc32c;
use thiserror::Error;

pub const FRAME_HEADER_LEN: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NegotiatedFrameLimit {
    max_frame_bytes: usize,
}

impl NegotiatedFrameLimit {
    #[must_use]
    pub fn new(max_frame_bytes: usize) -> Self {
        Self { max_frame_bytes }
    }

    #[must_use]
    pub fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }
}

#[derive(Clone, Debug)]
pub struct FrameLimitState {
    max_frame_bytes: Arc<AtomicUsize>,
}

impl FrameLimitState {
    #[must_use]
    pub fn unnegotiated(max_frame_bytes: usize) -> Self {
        Self {
            max_frame_bytes: Arc::new(AtomicUsize::new(max_frame_bytes)),
        }
    }

    pub fn apply_negotiated_limit(&self, limit: NegotiatedFrameLimit) {
        self.max_frame_bytes
            .store(limit.max_frame_bytes(), Ordering::SeqCst);
    }

    fn max_frame_bytes(&self) -> usize {
        self.max_frame_bytes.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame length invalid: {reason}")]
    FrameLengthInvalid { reason: String },
    #[error("frame too large: max {max_frame_bytes} got {got_bytes}")]
    FrameTooLarge {
        max_frame_bytes: usize,
        got_bytes: usize,
    },
    #[error("frame crc mismatch: expected {expected} got {got}")]
    FrameCrcMismatch { expected: u32, got: u32 },
}

impl FrameError {
    #[must_use]
    pub fn as_error_payload(&self) -> Option<ErrorPayload> {
        match self {
            FrameError::FrameTooLarge {
                max_frame_bytes,
                got_bytes,
            } => Some(
                ErrorPayload::new(
                    ProtocolErrorCode::FrameTooLarge.into(),
                    "frame exceeds max_frame_bytes",
                    false,
                )
                .with_details(FrameTooLargeDetails {
                    max_frame_bytes: *max_frame_bytes as u64,
                    got_bytes: *got_bytes as u64,
                }),
            ),
            FrameError::Io(_)
            | FrameError::FrameLengthInvalid { .. }
            | FrameError::FrameCrcMismatch { .. } => None,
        }
    }
}

pub struct FrameReader<R> {
    reader: R,
    limits: FrameLimitState,
}

impl<R: Read> FrameReader<R> {
    #[must_use]
    pub fn new(reader: R, limits: FrameLimitState) -> Self {
        Self { reader, limits }
    }

    pub fn read_next(&mut self) -> Result<Option<Vec<u8>>, FrameError> {
        let mut header = [0u8; FRAME_HEADER_LEN];
        let mut read = 0usize;
        while read < header.len() {
            let n = self.reader.read(&mut header[read..])?;
            if n == 0 {
                if read == 0 {
                    return Ok(None);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "frame header truncated",
                )
                .into());
            }
            read += n;
        }

        let length = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        if length == 0 {
            return Err(FrameError::FrameLengthInvalid {
                reason: "frame length cannot be zero".to_string(),
            });
        }
        let max_frame_bytes = self.limits.max_frame_bytes();
        if length > max_frame_bytes {
            return Err(FrameError::FrameTooLarge {
                max_frame_bytes,
                got_bytes: length,
            });
        }

        let expected_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let mut body = vec![0u8; length];
        let mut read_body = 0usize;
        while read_body < length {
            let n = self.reader.read(&mut body[read_body..])?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "frame body truncated",
                )
                .into());
            }
            read_body += n;
        }

        let actual_crc = crc32c(&body);
        if actual_crc != expected_crc {
            return Err(FrameError::FrameCrcMismatch {
                expected: expected_crc,
                got: actual_crc,
            });
        }

        Ok(Some(body))
    }
}

pub struct FrameWriter<W> {
    writer: W,
    max_frame_bytes: usize,
}

impl<W: Write> FrameWriter<W> {
    #[must_use]
    pub fn new(writer: W, max_frame_bytes: usize) -> Self {
        Self {
            writer,
            max_frame_bytes,
        }
    }

    pub fn write_frame(&mut self, payload: &[u8]) -> Result<usize, FrameError> {
        let frame = encode_frame(payload, self.max_frame_bytes)?;
        self.writer.write_all(&frame)?;
        Ok(frame.len())
    }

    pub fn write_frame_with_limit(
        &mut self,
        payload: &[u8],
        max_frame_bytes: usize,
    ) -> Result<usize, FrameError> {
        let frame = encode_frame(payload, max_frame_bytes)?;
        self.writer.write_all(&frame)?;
        Ok(frame.len())
    }
}

impl FrameWriter<TcpStream> {
    pub fn shutdown(&self) {
        let _ = self.writer.shutdown(Shutdown::Both);
    }
}

pub fn encode_frame(payload: &[u8], max_frame_bytes: usize) -> Result<Vec<u8>, FrameError> {
    if payload.len() > max_frame_bytes {
        return Err(FrameError::FrameTooLarge {
            max_frame_bytes,
            got_bytes: payload.len(),
        });
    }
    let length = u32::try_from(payload.len()).map_err(|_| FrameError::FrameLengthInvalid {
        reason: "frame length exceeds u32".to_string(),
    })?;
    let crc = crc32c(payload);

    let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    buf.extend_from_slice(&length.to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(payload);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn frame_roundtrip_validates_crc() {
        let payload = b"hello";
        let frame = encode_frame(payload, 1024).unwrap();

        let mut reader = FrameReader::new(Cursor::new(frame), FrameLimitState::unnegotiated(1024));
        let decoded = reader.read_next().unwrap().unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn frame_too_large_maps_to_error_payload() {
        let payload = vec![0u8; 10];
        let err = encode_frame(&payload, 5).unwrap_err();
        assert!(matches!(err, FrameError::FrameTooLarge { .. }));

        let payload = err.as_error_payload().unwrap();
        assert_eq!(payload.code, ProtocolErrorCode::FrameTooLarge.into());
        let details: FrameTooLargeDetails = payload.details_as().unwrap().unwrap();
        assert_eq!(details.max_frame_bytes, 5);
        assert_eq!(details.got_bytes, 10);
    }

    #[test]
    fn frame_reader_rejects_oversize_frame() {
        let payload = vec![0u8; 10];
        let frame = encode_frame(&payload, 1024).unwrap();

        let mut reader = FrameReader::new(Cursor::new(frame), FrameLimitState::unnegotiated(5));
        let err = reader.read_next().unwrap_err();
        assert!(matches!(err, FrameError::FrameTooLarge { .. }));
    }

    #[test]
    fn frame_reader_enforces_negotiated_limit() {
        let payload = vec![1u8; 10];
        let frame = encode_frame(&payload, 100).unwrap();
        let limits = FrameLimitState::unnegotiated(100);
        limits.apply_negotiated_limit(NegotiatedFrameLimit::new(5));

        let mut reader = FrameReader::new(Cursor::new(frame), limits);
        let err = reader.read_next().unwrap_err();
        assert!(matches!(err, FrameError::FrameTooLarge { .. }));
    }
}
