//! Daemon module - the beads service.
//!
//! Provides:
//! - HLC clock for causal ordering
//! - Operations (create, update, close, delete, etc.)
//! - Queries (show, list, ready, etc.)
//! - IPC over Unix socket
//! - Background sync scheduling

pub mod admin;
pub mod admission;
pub(crate) mod broadcast;
pub(crate) mod checkpoint_scheduler;
pub mod clock;
pub(crate) mod coord;
pub mod core;
pub mod durability_coordinator;
pub mod executor;
mod export_worker;
pub(crate) mod fingerprint;
pub(crate) mod git_lane;
pub mod git_worker;
pub(crate) mod io_budget;
pub mod ipc;
pub(crate) mod metrics;
pub mod mutation_engine;
pub mod ops;
pub mod query;
pub mod query_executor;
pub mod remote;
pub mod repl;
pub mod run;
pub mod scheduler;
pub(crate) mod scrubber;
pub mod server;
pub(crate) mod store;
pub mod store_lock;
pub mod store_runtime;
pub(crate) mod subscription;
pub(crate) mod test_hooks;
pub mod wal;

pub use core::{Daemon, LoadedStore};

pub use beads_api::QueryResult;
pub use beads_surface::ops::{BeadPatch, OpResult, Patch};
pub use beads_surface::query::{Filters, SortField};
pub use clock::Clock;
pub use git_lane::GitLaneState;
pub use git_worker::{GitOp, GitResult, GitWorker, SyncResult, run_git_loop};
pub use ipc::{IpcClient, IpcError, Request, Response, ResponsePayload};
pub use ops::OpError;
pub use query::Query;
pub use remote::RemoteUrl;
pub use run::run_daemon;
pub use scheduler::SyncScheduler;
pub use server::{run_socket_thread, run_state_loop};
