use thiserror::Error;

use crate::core::CoreError;
use crate::daemon::{IpcError, OpError};
use crate::git::SyncError;

// Re-export Effect and Transience from beads-core
pub use beads_core::{Effect, Transience};

/// Crate-level convenience error.
///
/// Not a "god error": it is a thin wrapper over canonical capability errors.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Core(#[from] CoreError),

    #[error(transparent)]
    Op(#[from] OpError),

    #[error(transparent)]
    Sync(#[from] SyncError),

    #[error(transparent)]
    Ipc(#[from] IpcError),
}

impl Error {
    pub fn transience(&self) -> Transience {
        match self {
            Error::Core(e) => e.transience(),
            Error::Op(e) => e.transience(),
            Error::Sync(e) => e.transience(),
            Error::Ipc(e) => e.transience(),
        }
    }

    pub fn effect(&self) -> Effect {
        match self {
            Error::Core(e) => e.effect(),
            Error::Op(e) => e.effect(),
            Error::Sync(e) => e.effect(),
            Error::Ipc(e) => e.effect(),
        }
    }
}
