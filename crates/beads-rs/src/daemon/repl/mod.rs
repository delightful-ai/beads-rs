//! Replication protocol modules.

pub mod contiguous_batch;
pub mod error;
pub mod frame;
pub mod gap_buffer;
mod keepalive;
pub mod manager;
pub mod peer_acks;
mod pending;
pub mod proto;
pub mod runtime;
pub mod server;
pub mod session;
pub mod store;
mod want;

pub use contiguous_batch::{ContiguousBatch, ContiguousBatchError};
pub use error::{ReplError, ReplErrorDetails};
pub use frame::{FrameError, FrameReader, FrameWriter};
pub use gap_buffer::{GapBufferByNsOrigin, IngestDecision, OriginStreamState};
pub use manager::{
    BackoffPolicy, PeerConfig, ReplicationManager, ReplicationManagerConfig,
    ReplicationManagerHandle,
};
pub use peer_acks::{PeerAckError, PeerAckTable, QuorumOutcome};
pub use proto::{
    Ack, Capabilities, Events, Hello, ProtoDecodeError, ProtoEncodeError, ReplEnvelope,
    ReplMessage, Want, WatermarkMap, WatermarkState, decode_envelope, encode_envelope,
};
pub use runtime::{ReplIngestRequest, ReplSessionStore, WalRangeError, WalRangeReader};
pub use server::{
    ReplicationServer, ReplicationServerConfig, ReplicationServerError, ReplicationServerHandle,
};
pub use session::{
    IngestOutcome, ProtocolRange, Session, SessionAction, SessionConfig, SessionPeer, SessionPhase,
    SessionStore, ValidatedAck, WatermarkSnapshot,
};
pub use store::SharedSessionStore;
