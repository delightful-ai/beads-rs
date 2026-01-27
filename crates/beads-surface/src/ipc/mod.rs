pub mod client;
pub mod codec;
pub mod ctx;
pub mod payload;
pub mod types;

use beads_api::DaemonInfo;
use beads_core::{CliErrorCode, Effect, ErrorCode, InvalidId, ProtocolErrorCode, Transience};
use thiserror::Error;

pub use client::*;
pub use codec::*;
pub use types::*;

pub(crate) fn default_lease_secs() -> u64 {
    3600 // 1 hour default
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum IpcError {
    #[error("parse error: {0}")]
    Parse(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    InvalidId(#[from] InvalidId),

    #[error("invalid request: {reason}")]
    InvalidRequest {
        field: Option<String>,
        reason: String,
    },

    #[error("client disconnected")]
    Disconnected,

    #[error("daemon unavailable: {0}")]
    DaemonUnavailable(String),

    #[error("daemon version mismatch; restart the daemon and retry")]
    DaemonVersionMismatch {
        daemon: Option<DaemonInfo>,
        client_version: String,
        protocol_version: u32,
        /// If set, the mismatch was detected via a parse failure.
        parse_error: Option<String>,
    },

    #[error("frame too large: max {max_bytes} bytes, got {got_bytes} bytes")]
    FrameTooLarge { max_bytes: usize, got_bytes: usize },
}

impl IpcError {
    pub fn code(&self) -> ErrorCode {
        match self {
            IpcError::Parse(_) => ProtocolErrorCode::MalformedPayload.into(),
            IpcError::Io(_) => CliErrorCode::IoError.into(),
            IpcError::InvalidId(_) => CliErrorCode::InvalidId.into(),
            IpcError::InvalidRequest { .. } => ProtocolErrorCode::InvalidRequest.into(),
            IpcError::Disconnected => CliErrorCode::Disconnected.into(),
            IpcError::DaemonUnavailable(_) => CliErrorCode::DaemonUnavailable.into(),
            IpcError::DaemonVersionMismatch { .. } => CliErrorCode::DaemonVersionMismatch.into(),
            IpcError::FrameTooLarge { .. } => ProtocolErrorCode::FrameTooLarge.into(),
        }
    }

    /// Whether retrying the IPC operation may succeed.
    pub fn transience(&self) -> Transience {
        match self {
            IpcError::DaemonUnavailable(_) | IpcError::Io(_) | IpcError::Disconnected => {
                Transience::Retryable
            }
            IpcError::DaemonVersionMismatch { .. } => Transience::Retryable,
            IpcError::Parse(_)
            | IpcError::InvalidId(_)
            | IpcError::InvalidRequest { .. }
            | IpcError::FrameTooLarge { .. } => Transience::Permanent,
        }
    }

    /// What we know about side effects when this IPC error is returned.
    pub fn effect(&self) -> Effect {
        match self {
            IpcError::Io(_) | IpcError::Disconnected => Effect::Unknown,
            IpcError::DaemonUnavailable(_)
            | IpcError::Parse(_)
            | IpcError::InvalidId(_)
            | IpcError::InvalidRequest { .. } => Effect::None,
            IpcError::DaemonVersionMismatch { .. } => Effect::None,
            IpcError::FrameTooLarge { .. } => Effect::None,
        }
    }
}
