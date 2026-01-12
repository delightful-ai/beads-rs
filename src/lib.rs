#![forbid(unsafe_code)]

pub mod api;
#[cfg(feature = "cli")]
pub mod cli;
pub mod compat;
pub mod config;
pub mod core;
pub mod daemon;
pub mod error;
pub mod git;
pub mod migrate;
mod paths;
pub mod repo;
pub mod upgrade;

pub use error::{Effect, Error, Transience};
pub type Result<T> = std::result::Result<T, Error>;

// Re-export core types at crate root for convenience
pub use crate::core::{
    decode_event_body, encode_event_body_canonical, hash_event_body, sha256_bytes, ActorId,
    Applied, Bead, BeadCore, BeadFields, BeadId, BeadType, Canonical, CanonicalState,
    CheckpointGroup, Claim, ClientRequestId, Closure, DecodeError, DepEdge, DepKey, DepKind,
    DurabilityClass, DurabilityOutcome, DurabilityProofV1, DurabilityReceipt, Durable, EncodeError,
    ErrorCode, ErrorPayload, EventBody, EventBytes, EventId, EventKindV1, GcAuthority, HeadStatus,
    HlcMax, Labels, Limits, LocalFsyncProof, Lww, NamespaceId, NamespacePolicy,
    NamespaceVisibility, Note, NoteId, NoteLog, Opaque, Priority, ReceiptMergeError, ReplicaId,
    ReplicateMode, ReplicatedProof, RetentionPolicy, SegmentId, Seq0, Seq1, Sha256, Stamp,
    StoreEpoch, StoreId, StoreIdentity, StoreMeta, StoreMetaVersions, StoreState, Tombstone,
    TtlBasis, TxnDeltaError, TxnDeltaV1, TxnId, TxnOpKey, TxnOpV1, WallClock, Watermark,
    WatermarkError, Watermarks, WireBeadFull, WireBeadPatch, WireFieldStamp, WireNoteV1,
    WirePatch, WireStamp, Workflow, WorkflowStatus, WriteStamp,
};
