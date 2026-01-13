//! Replication protocol modules.

pub mod frame;
pub mod gap_buffer;
pub mod proto;

pub use frame::{FrameError, FrameReader, FrameWriter};
pub use gap_buffer::{GapBufferByNsOrigin, IngestDecision, OriginStreamState};
pub use proto::{
    Ack, Capabilities, Events, Hello, ProtoDecodeError, ProtoEncodeError, ReplEnvelope,
    ReplMessage, Want, WatermarkHeads, WatermarkMap, decode_envelope, encode_envelope,
};
