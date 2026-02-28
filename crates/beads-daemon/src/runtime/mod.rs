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
pub mod admin;
pub mod checkpoint_scheduler;
pub mod coord;
pub mod core;
pub mod durability_coordinator;
pub mod executor;
mod export_worker;
pub mod fingerprint;
pub mod git_backend;
pub(crate) mod git_worker;
pub mod io_budget;
pub mod ipc;
pub mod metrics;
pub mod mutation_engine;
pub mod ops;
pub mod query;
pub mod query_executor;
pub mod repl;
pub mod run;
pub mod scrubber;
pub mod server;
pub mod store;
pub mod subscription;
pub mod test_hooks;
pub mod wal;
mod wal_atomic_commit;

pub use core::Daemon;

pub use crate::clock::Clock;
pub use crate::git_lane::GitLaneState;
pub use crate::remote::RemoteUrl;
pub use crate::scheduler::SyncScheduler;
pub use beads_api::QueryResult;
pub use beads_surface::ops::{BeadPatch, OpResult, Patch};
pub use beads_surface::query::{Filters, SortField};
pub(in crate::runtime) use git_worker::{GitOp, GitResult};
pub(crate) use git_worker::{GitWorker, run_git_loop};
pub use ipc::{IpcClient, IpcError, Request, Response, ResponsePayload};
pub use ops::OpError;
pub use query::Query;
pub use run::run_daemon;
