//! Canonical API schemas for daemon IPC and CLI `--json`.
//!
//! These types are the *truthful boundary*: we avoid lossy “view” structs that
//! silently drop information. If a smaller payload is desirable, we define an
//! explicit summary type.

pub use beads_core::DurabilityReceipt;

mod admin;
mod deps;
mod issues;
mod query;
mod realtime;

pub use admin::*;
pub use deps::*;
pub use issues::*;
pub use query::*;
pub use realtime::*;
