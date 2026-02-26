//! Git integration module.
//!
//! Provides:
//! - Sync typestate machine (Idle → Fetched → Merged → Committed)
//! - Wire format serialization (JSONL with sparse _v)
//! - Checkpoint export/import/cache/publish helpers

pub use beads_core as core;

pub mod checkpoint;
pub mod error;
pub mod observe;
mod paths;
pub mod sync;
pub mod wire;

pub use error::{SyncError, WireError};
pub use observe::{NoopSyncObserver, SyncObserver};
pub use paths::init_data_dir_override;
pub use sync::{
    DivergenceInfo, LoadedStore, SyncDiff, SyncOutcome, SyncProcess, init_beads_ref,
    read_state_at_oid, sync_with_retry, sync_with_retry_with_observer,
};
