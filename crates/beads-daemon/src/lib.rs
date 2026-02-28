//! Typed public boundary for the daemon crate.

#![forbid(unsafe_code)]

pub use beads_macros::enum_str;

pub mod admission;
pub mod broadcast;
pub mod clock;
pub mod compat;
pub mod config;
mod env_flags;
pub mod git_lane;
pub mod io_budget;
pub mod layout;
pub mod metrics;
pub mod paths;
pub mod remote;
pub mod repo;
pub mod runtime;
pub mod scheduler;
pub mod telemetry;
pub mod test_utils;
#[cfg(any(test, feature = "test-harness"))]
pub mod testkit;

pub mod error {
    pub use beads_core::{Effect, Transience};
}

pub mod git {
    pub use beads_git::*;
}

pub use beads_api as api;
pub use beads_api::DaemonInfo;
pub use beads_core as core;
pub use beads_core::StoreId;
pub use beads_core::WallClock;
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

pub use runtime::run_daemon;
pub type Result<T> = std::result::Result<T, runtime::ipc::IpcError>;

#[must_use]
pub fn daemon_layout_from_paths() -> layout::DaemonLayout {
    layout::DaemonLayout::new(
        paths::data_dir(),
        runtime::ipc::socket_path(),
        paths::log_dir(),
    )
}

#[must_use]
pub fn daemon_runtime_config_from_config(config: &config::Config) -> config::DaemonRuntimeConfig {
    paths::init_from_config(&config.paths);
    config::daemon_runtime_from_config(config)
}
