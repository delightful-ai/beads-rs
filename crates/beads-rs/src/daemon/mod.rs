//! Daemon module - the beads service.
//!
//! Provides:
//! - HLC clock for causal ordering
//! - Operations (create, update, close, delete, etc.)
//! - Queries (show, list, ready, etc.)
//! - IPC over Unix socket
//! - Background sync scheduling

// Public daemon surface is intentionally narrow (IPC + protocol/test harness types).
// Orchestration/runtime internals stay crate-private to avoid coupling.
pub(crate) mod admin;
pub(crate) mod checkpoint_scheduler;
pub(crate) mod coord;
pub(crate) mod core;
pub(crate) mod durability_coordinator;
pub(crate) mod executor;
mod export_worker;
pub(crate) mod fingerprint;
pub(crate) mod git_worker;
pub(crate) mod io_budget;
pub mod ipc;
pub(crate) mod metrics;
pub(crate) mod mutation_engine;
pub(crate) mod ops;
pub(crate) mod query;
pub(crate) mod query_executor;
pub mod repl;
pub(crate) mod run;
pub(crate) mod scrubber;
pub(crate) mod server;
pub(crate) mod store;
pub(crate) mod subscription;
pub(crate) mod test_hooks;
pub mod wal;

pub use core::{Daemon, LoadedStore};

pub use beads_api::QueryResult;
pub use beads_daemon::clock::Clock;
pub use beads_daemon::git_lane::GitLaneState;
pub use beads_daemon::remote::RemoteUrl;
pub use beads_daemon::scheduler::SyncScheduler;
pub use beads_surface::ops::{BeadPatch, OpResult, Patch};
pub use beads_surface::query::{Filters, SortField};
pub(crate) use git_worker::{GitOp, GitResult, GitWorker, run_git_loop};
pub use ipc::{IpcClient, IpcError, Request, Response, ResponsePayload};
pub use ops::OpError;
pub use query::Query;
pub use run::run_daemon;

pub use store::lock::{
    StoreLock, StoreLockError, StoreLockMeta, StoreLockOperation, read_lock_meta,
};
pub use store::runtime::{StoreRuntime, StoreRuntimeError};
