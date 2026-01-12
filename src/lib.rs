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
    ActorId, Bead, BeadCore, BeadFields, BeadId, BeadType, CanonicalState, Claim, ClientRequestId,
    CheckpointGroup, Closure, DepEdge, DepKey, DepKind, GcAuthority, Labels, Lww, NamespaceId,
    NamespacePolicy, NamespaceVisibility, Note, NoteId, NoteLog, Priority, ReplicaId,
    ReplicateMode, RetentionPolicy, SegmentId, Stamp, StoreEpoch, StoreId, StoreIdentity,
    StoreMeta, StoreMetaVersions, StoreState, Tombstone, TtlBasis, TxnId, WallClock, Workflow,
    WriteStamp,
};
