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
pub mod remote;
pub mod repo;
pub mod run;
pub mod scheduler;
pub mod server;
pub mod store_lock;
pub mod wal;

pub use core::{Daemon, LoadedRemote};

pub use clock::Clock;
pub use git_worker::{GitOp, GitResult, GitWorker, SyncResult, run_git_loop};
pub use ipc::{
    ErrorPayload, IpcError, OpResponse, Request, Response, ResponsePayload, decode_request,
    encode_response, socket_path,
};
pub use ops::{BeadPatch, OpError, OpResult, Patch};
pub use query::{Filters, Query, QueryResult, SortField};
pub use remote::RemoteUrl;
pub use repo::RepoState;
pub use run::run_daemon;
pub use scheduler::SyncScheduler;
pub use server::{run_socket_thread, run_state_loop};
