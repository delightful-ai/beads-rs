//! Daemon module - the beads service.
//!
//! Provides:
//! - HLC clock for causal ordering
//! - Operations (create, update, close, delete, etc.)
//! - Queries (show, list, ready, etc.)
//! - IPC over Unix socket
//! - Background sync scheduling

pub mod clock;
pub mod core;
pub mod executor;
pub mod git_worker;
pub mod ipc;
pub mod ops;
pub mod query;
pub mod query_executor;
pub mod repo;
pub mod remote;
pub mod run;
pub mod scheduler;
pub mod server;

pub use clock::Clock;
pub use core::Daemon;
pub use git_worker::{run_git_loop, GitOp, GitWorker, SyncResult};
pub use ipc::{
    decode_request, encode_response, socket_path, ErrorPayload, IpcError, Request, Response,
    ResponsePayload,
};
pub use ops::{BeadOp, BeadPatch, OpError, OpResult, Patch};
pub use query::{Filters, Query, QueryResult, SortField};
pub use repo::RepoState;
pub use remote::RemoteUrl;
pub use run::run_daemon;
pub use scheduler::SyncScheduler;
pub use server::{run_socket_thread, run_state_loop};
