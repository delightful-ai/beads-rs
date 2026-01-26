use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;

use beads_core::Limits;
use serde_json::Value;

use super::{IpcError, Request, Response};

/// Encode a response to bytes.
pub fn encode_response(resp: &Response) -> Result<Vec<u8>, IpcError> {
    let mut bytes = serde_json::to_vec(resp)?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Decode a request from a line, enforcing limits.
pub fn decode_request_with_limits(line: &str, limits: &Limits) -> Result<Request, IpcError> {
    let got_bytes = line.len();
    if got_bytes > limits.max_frame_bytes {
        return Err(IpcError::FrameTooLarge {
            max_bytes: limits.max_frame_bytes,
            got_bytes,
        });
    }
    let value: Value = serde_json::from_str(line)?;
    serde_json::from_value(value).map_err(|err| IpcError::InvalidRequest {
        field: None,
        reason: err.to_string(),
    })
}

/// Decode a request from a line using default limits.
pub fn decode_request(line: &str) -> Result<Request, IpcError> {
    decode_request_with_limits(line, &Limits::default())
}

/// Send a response over a stream.
pub fn send_response(stream: &mut UnixStream, resp: &Response) -> Result<(), IpcError> {
    let bytes = encode_response(resp)?;
    stream.write_all(&bytes)?;
    Ok(())
}

/// Read requests from a stream.
pub fn read_requests(stream: UnixStream) -> impl Iterator<Item = Result<Request, IpcError>> {
    let reader = BufReader::new(stream);
    reader.lines().map(|line| {
        let line = line?;
        decode_request_with_limits(&line, &Limits::default())
    })
}
