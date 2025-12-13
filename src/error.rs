use thiserror::Error;

use crate::core::CoreError;
use crate::daemon::{IpcError, OpError};
use crate::git::SyncError;

/// Whether retrying this operation may succeed.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Transience {
    /// Retry will never help without changing inputs/state.
    Permanent,
    /// Retry may help (transient contention/outage).
    Retryable,
    /// Unknown if retry will help.
    Unknown,
}

impl Transience {
    pub fn is_retryable(self) -> bool {
        matches!(self, Transience::Retryable)
    }
}

/// What we know about side effects when an error is returned.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Effect {
    /// Definitely no side effects occurred.
    None,
    /// Side effects definitely occurred (locally or remotely).
    Some,
    /// We don't know if side effects occurred.
    Unknown,
}

impl Effect {
    pub fn as_str(self) -> &'static str {
        match self {
            Effect::None => "none",
            Effect::Some => "some",
            Effect::Unknown => "unknown",
        }
    }
}

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
