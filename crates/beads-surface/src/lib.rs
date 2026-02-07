//! IPC and surface types for beads-rs.

pub mod ipc;
pub mod ops;
pub mod query;
pub mod store_admin;

pub use ipc::{IpcClient, IpcError, Request, Response, ResponsePayload};
pub use ops::{BeadPatch, OpResult, OpenInProgress, Patch};
pub use query::{Filters, SortField};
