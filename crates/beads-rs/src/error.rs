use thiserror::Error;

use crate::core::CoreError;
use crate::git::SyncError;
use crate::surface::IpcError;
use beads_cli::commands::CommandError;
use beads_cli::commands::setup::SetupError;
use beads_cli::filters::FilterError;
use beads_cli::migrate::GoImportError;
use beads_cli::validation::ValidationError;
use beads_core::ErrorPayload;

// Re-export Effect and Transience from beads-core
pub use beads_core::{Effect, Transience};

/// Crate-level operation error surface.
///
/// Keeps common validation/request semantics stable while decoupling callers
/// from daemon-only internal error variants.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum OpError {
    #[error("validation failed for field {field}: {reason}")]
    ValidationFailed { field: String, reason: String },

    #[error("invalid request: {reason}")]
    InvalidRequest {
        field: Option<String>,
        reason: String,
    },

    #[error("{message}")]
    Daemon {
        message: String,
        payload: ErrorPayload,
        transience: Transience,
        effect: Effect,
    },
}

impl OpError {
    pub fn transience(&self) -> Transience {
        match self {
            OpError::ValidationFailed { .. } | OpError::InvalidRequest { .. } => {
                Transience::Permanent
            }
            OpError::Daemon { transience, .. } => *transience,
        }
    }

    pub fn effect(&self) -> Effect {
        match self {
            OpError::ValidationFailed { .. } | OpError::InvalidRequest { .. } => Effect::None,
            OpError::Daemon { effect, .. } => *effect,
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

impl From<ValidationError> for Error {
    fn from(err: ValidationError) -> Self {
        match err {
            ValidationError::Field { field, reason } => {
                Error::Op(OpError::ValidationFailed { field, reason })
            }
        }
    }
}

impl From<FilterError> for Error {
    fn from(err: FilterError) -> Self {
        match err {
            FilterError::Core(err) => Error::Core(err),
            FilterError::Validation { field, reason } => {
                Error::Op(OpError::ValidationFailed { field, reason })
            }
        }
    }
}

impl From<SetupError> for Error {
    fn from(err: SetupError) -> Self {
        match err {
            SetupError::Validation { field, reason } => {
                Error::Op(OpError::ValidationFailed { field, reason })
            }
            SetupError::Io(err) => Error::Ipc(IpcError::from(err)),
        }
    }
}

impl From<GoImportError> for Error {
    fn from(err: GoImportError) -> Self {
        match err {
            GoImportError::Io(err) => Error::Ipc(IpcError::from(err)),
            GoImportError::Parse(err) => Error::Ipc(IpcError::from(err)),
            GoImportError::Core(err) => Error::Core(err),
            GoImportError::Validation { field, reason } => Error::Op(OpError::ValidationFailed {
                field: field.into(),
                reason,
            }),
        }
    }
}

impl From<CommandError> for Error {
    fn from(err: CommandError) -> Self {
        match err {
            CommandError::Core(err) => Error::Core(err),
            CommandError::Ipc(err) => Error::Ipc(err),
            CommandError::Validation(err) => Error::from(err),
            CommandError::Filter(err) => Error::from(err),
        }
    }
}
