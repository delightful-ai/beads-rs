//! Replication protocol modules.

pub mod frame;
pub mod gap_buffer;
pub mod manager;
pub mod peer_acks;
pub mod proto;
pub mod server;
pub mod session;
pub mod store;

pub use frame::{FrameError, FrameReader, FrameWriter};
pub use gap_buffer::{GapBufferByNsOrigin, IngestDecision, OriginStreamState};
pub use manager::{
    BackoffPolicy, PeerConfig, ReplicationManager, ReplicationManagerConfig,
    ReplicationManagerHandle,
};
pub use peer_acks::{PeerAckError, PeerAckTable, QuorumOutcome};
pub use proto::{
    Ack, Capabilities, Events, Hello, ProtoDecodeError, ProtoEncodeError, ReplEnvelope,
    ReplMessage, Want, WatermarkHeads, WatermarkMap, decode_envelope, encode_envelope,
};
pub use server::{
    ReplicationServer, ReplicationServerConfig, ReplicationServerError, ReplicationServerHandle,
};
pub use session::{
    IngestOutcome, ProtocolRange, Session, SessionAction, SessionConfig, SessionPeer, SessionPhase,
    SessionRole, SessionStore, WatermarkSnapshot,
};
pub use store::SharedSessionStore;
