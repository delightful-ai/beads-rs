//! Typed public boundary for the daemon crate.

#![forbid(unsafe_code)]

pub mod admission;
pub mod broadcast;
pub mod clock;
pub mod git_lane;
pub mod io_budget;
pub mod metrics;
pub mod remote;
pub mod scheduler;
pub mod test_utils;

pub use beads_api as api;
pub use beads_api::DaemonInfo;
pub use beads_core as core;
pub use beads_core::StoreId;
pub use beads_surface as surface;
pub use beads_surface::{Request, Response};

/// Immutable identity and runtime metadata for a daemon instance.
#[derive(Debug, Clone)]
pub struct DaemonIdentity {
    pub store_id: StoreId,
    pub info: DaemonInfo,
}

impl DaemonIdentity {
    #[must_use]
    pub fn new(store_id: StoreId, info: DaemonInfo) -> Self {
        Self { store_id, info }
    }
}

/// A typed IPC exchange handled by the daemon.
#[derive(Debug, Clone)]
pub struct DaemonExchange {
    pub request: Request,
    pub response: Response,
}

impl DaemonExchange {
    #[must_use]
    pub fn new(request: Request, response: Response) -> Self {
        Self { request, response }
    }
}
