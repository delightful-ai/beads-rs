pub mod api;
#[cfg(feature = "cli")]
pub mod cli;
pub mod core;
pub mod daemon;
pub mod error;
pub mod git;
pub mod migrate;
pub mod repo;

pub use error::{Effect, Error, Transience};
pub type Result<T> = std::result::Result<T, Error>;

// Re-export core types at crate root for convenience
pub use crate::core::{
    ActorId, Bead, BeadCore, BeadFields, BeadId, BeadType, CanonicalState, Claim, Closure, DepEdge,
    DepKey, DepKind, Labels, Lww, Note, NoteId, NoteLog, Priority, Stamp, Tombstone, WallClock,
    Workflow, WriteStamp,
};
