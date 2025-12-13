//! Git sync error types.

use std::path::PathBuf;

use thiserror::Error;

use crate::core::BeadId;
use crate::error::{Effect, Transience};
use crate::core::CoreError;

/// Errors that can occur during git sync operations.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum SyncError {
    #[error("failed to open repository at {0}: {1}")]
    OpenRepo(PathBuf, #[source] git2::Error),

    #[error("failed to fetch from remote: {0}")]
    Fetch(#[source] git2::Error),

    #[error("local ref not found: {0}")]
    NoLocalRef(String),

    #[error("remote ref not found: {0}")]
    NoRemoteRef(String),

    #[error("failed to find merge base: {0}")]
    MergeBase(#[source] git2::Error),

    #[error("missing file in tree: {0}")]
    MissingFile(String),

    #[error("expected blob but got different object type: {0}")]
    NotABlob(&'static str),

    #[error("failed to write blob: {0}")]
    WriteBlob(#[source] git2::Error),

    #[error("failed to build tree: {0}")]
    BuildTree(#[source] git2::Error),

    #[error("failed to create commit: {0}")]
    Commit(#[source] git2::Error),

    #[error("push rejected (non-fast-forward)")]
    NonFastForward,

    #[error("failed to push: {0}")]
    Push(#[source] git2::Error),

    #[error("too many sync retries ({0})")]
    TooManyRetries(usize),

    #[error("ID collision detected: {0} and {1} have same ID but different content")]
    IdCollision(BeadId, BeadId),

    #[error("no common ancestor between local and remote")]
    NoCommonAncestor,

    #[error(transparent)]
    Wire(#[from] WireError),

    #[error("merge conflict: {errors:?}")]
    MergeConflict { errors: Vec<CoreError> },

    #[error(transparent)]
    PushRejected(#[from] PushRejected),

    #[error("init failed: {0}")]
    InitFailed(#[source] git2::Error),

    #[error("git operation failed: {0}")]
    Git(#[from] git2::Error),
}

impl SyncError {
    /// Whether retrying this sync may succeed.
    pub fn transience(&self) -> Transience {
        match self {
            SyncError::Fetch(_)
            | SyncError::NonFastForward
            | SyncError::Push(_)
            | SyncError::PushRejected(_)
            | SyncError::TooManyRetries(_)
            | SyncError::InitFailed(_) => Transience::Retryable,

            SyncError::OpenRepo(_, _)
            | SyncError::NoLocalRef(_)
            | SyncError::NoRemoteRef(_)
            | SyncError::MergeBase(_)
            | SyncError::MissingFile(_)
            | SyncError::NotABlob(_)
            | SyncError::WriteBlob(_)
            | SyncError::BuildTree(_)
            | SyncError::Commit(_)
            | SyncError::IdCollision(_, _)
            | SyncError::NoCommonAncestor
            | SyncError::Wire(_)
            | SyncError::MergeConflict { .. }
            | SyncError::Git(_) => Transience::Permanent,
        }
    }

    /// What we know about side effects when this error is returned.
    pub fn effect(&self) -> Effect {
        match self {
            // Push-phase errors occur after a local commit was created.
            SyncError::NonFastForward
            | SyncError::Push(_)
            | SyncError::PushRejected(_)
            | SyncError::TooManyRetries(_)
            | SyncError::InitFailed(_) => Effect::Some,

            // Low-level git2 errors can happen at any phase.
            SyncError::Git(_) => Effect::Unknown,

            // Everything else fails before committing.
            _ => Effect::None,
        }
    }
}

/// Errors that can occur during wire format serialization/deserialization.
#[derive(Error, Debug)]
pub enum WireError {
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("missing required field: {0}")]
    MissingField(&'static str),

    #[error("invalid field value: {0}")]
    InvalidValue(String),
}

/// Push was rejected by the remote with a status message.
#[derive(Error, Debug)]
#[error("push rejected: {message}")]
pub struct PushRejected {
    pub message: String,
}
