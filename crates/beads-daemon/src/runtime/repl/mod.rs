//! Replication protocol modules.

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
pub use runtime::{ReplIngestRequest, ReplSessionStore, WalRangeError, WalRangeReader};
pub use server::{
    ReplicationServer, ReplicationServerConfig, ReplicationServerError, ReplicationServerHandle,
};
pub use session::{
    IngestOutcome, ProtocolRange, Session, SessionAction, SessionConfig, SessionPeer, SessionPhase,
    SessionStore, ValidatedAck, WatermarkSnapshot,
};
pub use store::SharedSessionStore;
