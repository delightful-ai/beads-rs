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
pub use paths::{DataDirOverride, init_data_dir_override, override_data_dir_for_tests};
pub use sync::{
    DivergenceInfo, LoadedStore, LoadedStoreMigration, MigrateStoreToV1Outcome, SyncDiff,
    SyncOutcome, SyncProcess, init_beads_ref, load_state, load_store, migrate_store_ref_to_v1,
    read_state_at_oid, read_state_at_oid_for_migration, sync_with_retry,
    sync_with_retry_with_observer,
};
