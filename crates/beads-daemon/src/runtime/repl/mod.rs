//! Replication protocol modules.

use std::net::TcpStream;
use std::time::Duration;

pub mod contiguous_batch;
pub mod error;
pub mod gap_buffer;
mod keepalive;
pub mod manager;
pub mod peer_acks;
mod pending;
pub mod runtime;
pub mod server;
pub mod session;
pub mod store;
mod want;

pub use beads_daemon_core::repl::frame::{
    FrameError, FrameLimitState, FrameReader, FrameWriter, NegotiatedFrameLimit,
};
pub use beads_daemon_core::repl::proto::{
    Ack, Capabilities, Events, Hello, NamespaceSet, PROTOCOL_VERSION_V1, ProtoDecodeError,
    ProtoEncodeError, ReplEnvelope, ReplMessage, Want, WatermarkMap, WatermarkState, WireEvents,
    WireReplEnvelope, WireReplMessage, decode_envelope, decode_envelope_with_version,
    encode_envelope,
};
pub use contiguous_batch::{ContiguousBatch, ContiguousBatchError};
pub use error::{ReplError, ReplErrorDetails};
pub use gap_buffer::{GapBufferByNsOrigin, IngestDecision, OriginStreamState};
pub use manager::{
    BackoffPolicy, PeerConfig, ReplicationManager, ReplicationManagerConfig,
    ReplicationManagerHandle,
};
pub use peer_acks::{PeerAckError, PeerAckTable, QuorumOutcome};
pub(crate) use runtime::{ReplIngestRequest, ReplSessionStore};
pub use runtime::{WalRangeError, WalRangeReader};
pub use server::{
    ReplicationServer, ReplicationServerConfig, ReplicationServerError, ReplicationServerHandle,
};
pub use session::{
    IngestOutcome, ProtocolRange, SessionAction, SessionConfig, SessionStore, ValidatedAck,
    WatermarkSnapshot,
};
pub use store::SharedSessionStore;

const REPL_STREAM_READ_TIMEOUT: Duration = Duration::from_secs(1);
const REPL_STREAM_WRITE_TIMEOUT: Duration = Duration::from_secs(3);

fn configure_repl_stream(stream: &TcpStream) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(REPL_STREAM_READ_TIMEOUT))?;
    stream.set_write_timeout(Some(REPL_STREAM_WRITE_TIMEOUT))?;
    Ok(())
}

fn is_timeout_error(err: &FrameError) -> bool {
    matches!(
        err,
        FrameError::Io(io_err)
            if matches!(
                io_err.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            )
    )
}
