//! Replication protocol modules.

pub mod frame;
pub mod gap_buffer;
pub mod peer_acks;
pub mod proto;
pub mod session;

pub use frame::{FrameError, FrameReader, FrameWriter};
pub use gap_buffer::{GapBufferByNsOrigin, IngestDecision, OriginStreamState};
pub use peer_acks::{PeerAckError, PeerAckTable, QuorumOutcome};
pub use proto::{
    Ack, Capabilities, Events, Hello, ProtoDecodeError, ProtoEncodeError, ReplEnvelope,
    ReplMessage, Want, WatermarkHeads, WatermarkMap, decode_envelope, encode_envelope,
};
pub use session::{
    IngestOutcome, ProtocolRange, Session, SessionAction, SessionConfig, SessionPeer, SessionPhase,
    SessionRole, SessionStore, WatermarkSnapshot,
};
