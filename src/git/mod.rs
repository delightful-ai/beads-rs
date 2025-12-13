//! Git integration module.
//!
//! Provides:
//! - GitWorker for managing git2 repository handles
//! - Sync typestate machine (Idle → Fetched → Merged → Committed)
//! - Wire format serialization (JSONL with sparse _v)
//! - ID collision detection and resolution

pub mod collision;
pub mod error;
pub mod sync;
pub mod wire;

pub use collision::{detect_collisions, resolve_collisions, Collision, CollisionSide};
pub use error::{SyncError, WireError};
pub use sync::{init_beads_ref, read_state_at_oid, sync_with_retry, SyncDiff, SyncProcess};
