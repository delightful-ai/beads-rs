//! Replication protocol primitives shared outside daemon runtime internals.

pub mod contiguous_batch;
pub mod frame;
pub mod gap_buffer;
pub mod peer_acks;
pub mod proto;

pub use contiguous_batch::{ContiguousBatch, ContiguousBatchError};
pub use frame::{FrameError, FrameLimitState, FrameReader, FrameWriter, NegotiatedFrameLimit};
pub use gap_buffer::{
    BufferedEventSnapshot, BufferedPrevSnapshot, GapBufferByNsOrigin, GapBufferByNsOriginSnapshot,
    GapBufferSnapshot, HeadSnapshot, IngestDecision, OriginStreamSnapshot, OriginStreamState,
    WatermarkSnapshot,
};
pub use peer_acks::{PeerAckError, PeerAckTable, QuorumOutcome, WatermarkState};
