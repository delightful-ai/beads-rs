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
    ActorId, Applied, Bead, BeadCore, BeadFields, BeadId, BeadType, CanonicalState,
    CheckpointGroup, Claim, ClientRequestId, Closure, DepEdge, DepKey, DepKind, DurabilityClass,
    DurabilityOutcome, DurabilityProofV1, DurabilityReceipt, Durable, ErrorCode, ErrorPayload,
    EventId, GcAuthority, HeadStatus, Labels, Limits, LocalFsyncProof, Lww, NamespaceId,
    NamespacePolicy, NamespaceVisibility, Note, NoteId, NoteLog, Priority, ReceiptMergeError,
    ReplicaId, ReplicateMode, ReplicatedProof, RetentionPolicy, SegmentId, Seq0, Seq1, Stamp,
    StoreEpoch, StoreId, StoreIdentity, StoreMeta, StoreMetaVersions, StoreState, Tombstone,
    TtlBasis, TxnId, WallClock, Watermark, WatermarkError, Watermarks, Workflow, WriteStamp,
};
