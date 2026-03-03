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
    state: FrameReadState,
}

enum FrameReadState {
    Header {
        buf: [u8; FRAME_HEADER_LEN],
        read: usize,
    },
    Body {
        expected_crc: u32,
        body: Vec<u8>,
        read: usize,
    },
}

impl Default for FrameReadState {
    fn default() -> Self {
        Self::Header {
            buf: [0u8; FRAME_HEADER_LEN],
            read: 0,
        }
    }
}

impl<R: Read> FrameReader<R> {
    #[must_use]
    pub fn new(reader: R, limits: FrameLimitState) -> Self {
        Self {
            reader,
            limits,
            state: FrameReadState::default(),
        }
    }

    pub fn read_next(&mut self) -> Result<Option<Vec<u8>>, FrameError> {
        loop {
            let state = std::mem::take(&mut self.state);
            match state {
                FrameReadState::Header { mut buf, mut read } => {
                    while read < buf.len() {
                        match self.reader.read(&mut buf[read..]) {
                            Ok(0) => {
                                if read == 0 {
                                    self.state = FrameReadState::Header { buf, read };
                                    return Ok(None);
                                }
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    "frame header truncated",
                                )
                                .into());
                            }
                            Ok(n) => read += n,
                            Err(err) => {
                                self.state = FrameReadState::Header { buf, read };
                                return Err(err.into());
                            }
                        }
                    }

                    let length = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
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

                    let expected_crc = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
                    self.state = FrameReadState::Body {
                        expected_crc,
                        body: vec![0u8; length],
                        read: 0,
                    };
                }
                FrameReadState::Body {
                    expected_crc,
                    mut body,
                    mut read,
                } => {
                    while read < body.len() {
                        match self.reader.read(&mut body[read..]) {
                            Ok(0) => {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    "frame body truncated",
                                )
                                .into());
                            }
                            Ok(n) => read += n,
                            Err(err) => {
                                self.state = FrameReadState::Body {
                                    expected_crc,
                                    body,
                                    read,
                                };
                                return Err(err.into());
                            }
                        }
                    }

                    let actual_crc = crc32c(&body);
                    if actual_crc != expected_crc {
                        return Err(FrameError::FrameCrcMismatch {
                            expected: expected_crc,
                            got: actual_crc,
                        });
                    }

                    return Ok(Some(body));
                }
            }
        }
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
    use std::collections::BTreeSet;
    use std::io::Cursor;
    use std::io::{self, Read};

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

    struct ChunkedTimeoutReader<R> {
        inner: R,
        max_chunk: usize,
        timeout_reads: BTreeSet<usize>,
        read_calls: usize,
    }

    impl<R> ChunkedTimeoutReader<R> {
        fn new(inner: R, max_chunk: usize, timeout_reads: &[usize]) -> Self {
            Self {
                inner,
                max_chunk,
                timeout_reads: timeout_reads.iter().copied().collect(),
                read_calls: 0,
            }
        }
    }

    impl<R: Read> Read for ChunkedTimeoutReader<R> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.read_calls += 1;
            if self.timeout_reads.contains(&self.read_calls) {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "synthetic timeout"));
            }
            let max_len = self.max_chunk.min(buf.len());
            self.inner.read(&mut buf[..max_len])
        }
    }

    #[test]
    fn frame_reader_resumes_across_timeout_with_partial_progress() {
        let payload = b"hello".to_vec();
        let frame = encode_frame(&payload, 1024).expect("encode frame");
        let flaky = ChunkedTimeoutReader::new(Cursor::new(frame), 2, &[2, 5]);
        let mut reader = FrameReader::new(flaky, FrameLimitState::unnegotiated(1024));

        let mut timeout_count = 0usize;
        loop {
            match reader.read_next() {
                Ok(Some(decoded)) => {
                    assert_eq!(decoded, payload);
                    break;
                }
                Err(FrameError::Io(err)) if err.kind() == io::ErrorKind::TimedOut => {
                    timeout_count += 1;
                    assert!(
                        timeout_count <= 4,
                        "reader should make progress and eventually decode frame"
                    );
                }
                other => panic!("unexpected read result: {other:?}"),
            }
        }
        assert!(timeout_count >= 1, "test should include timeout retries");
    }
}
