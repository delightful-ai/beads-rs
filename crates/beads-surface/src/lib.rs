//! IPC and surface types for beads-rs.

pub mod ipc;
pub mod ops;
pub mod query;

pub use ipc::{IpcClient, IpcError, Request, Response, ResponsePayload};
pub use ops::{BeadPatch, OpResult, Patch};
pub use query::{Filters, SortField};
