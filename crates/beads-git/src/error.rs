//! Git sync error types.

use std::path::PathBuf;

use thiserror::Error;

use crate::core::error::details as error_details;
use crate::core::{
    CliErrorCode, Effect, ErrorCode, ErrorPayload, IntoErrorPayload, ProtocolErrorCode,
    StateJsonlSha256, Transience,
};

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

    #[error("I/O error: {0}")]
    Io(#[source] std::io::Error),

    #[error("too many sync retries ({0})")]
    TooManyRetries(usize),

    #[error("no common ancestor between local and remote")]
    NoCommonAncestor,

    #[error("migration aborted due to parse warnings; rerun with --force to proceed")]
    MigrationWarnings(Vec<String>),

    #[error(transparent)]
    Wire(#[from] WireError),

    #[error(transparent)]
    PushRejected(#[from] PushRejected),

    #[error("init failed: {0}")]
    InitFailed(#[source] git2::Error),

    #[error("git operation failed: {0}")]
    Git(#[from] git2::Error),
}

impl SyncError {
    pub fn legacy_deps_runtime_hint(&self) -> Option<&'static str> {
        match self {
            SyncError::Wire(WireError::InvalidValue(msg))
                if is_legacy_deps_missing_cc_message(msg) =>
            {
                Some("Run `bd migrate to 1`")
            }
            SyncError::Wire(WireError::Json(err))
                if is_legacy_deps_missing_cc_message(&err.to_string()) =>
            {
                Some("Run `bd migrate to 1`")
            }
            _ => None,
        }
    }

    pub fn code(&self) -> ErrorCode {
        match self {
            err if err.legacy_deps_runtime_hint().is_some() => {
                CliErrorCode::ValidationFailed.into()
            }
            SyncError::Wire(WireError::ChecksumMismatch { .. }) => {
                ProtocolErrorCode::Corruption.into()
            }
            _ => CliErrorCode::SyncFailed.into(),
        }
    }

    /// Whether retrying this sync may succeed.
    pub fn transience(&self) -> Transience {
        match self {
            SyncError::Fetch(_)
            | SyncError::NonFastForward
            | SyncError::Push(_)
            | SyncError::Io(_)
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
            | SyncError::NoCommonAncestor
            | SyncError::MigrationWarnings(_)
            | SyncError::Wire(_)
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

            // Low-level/git-adjacent failures can happen before or after a local commit exists.
            SyncError::Git(_) | SyncError::Io(_) => Effect::Unknown,

            // Everything else fails before committing.
            _ => Effect::None,
        }
    }
}

impl IntoErrorPayload for SyncError {
    fn into_error_payload(self) -> ErrorPayload {
        let hint = self.legacy_deps_runtime_hint();
        let base_message = self.to_string();
        let message = match hint {
            Some(hint) => format!("{base_message}. {hint}"),
            None => base_message.clone(),
        };
        let retryable = self.transience().is_retryable();
        match self {
            SyncError::Wire(WireError::ChecksumMismatch {
                blob,
                expected,
                actual,
            }) => ErrorPayload::new(ProtocolErrorCode::Corruption.into(), message, retryable)
                .with_details(error_details::StoreChecksumMismatchDetails {
                    blob: blob.to_string(),
                    expected_sha256: expected.to_hex(),
                    got_sha256: actual.to_hex(),
                }),
            _ if hint.is_some() => {
                ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                    .with_details(error_details::ValidationFailedDetails {
                        field: "store".into(),
                        reason: format!(
                            "strict store load rejected legacy deps format: {base_message}"
                        ),
                    })
            }
            _ => ErrorPayload::new(CliErrorCode::SyncFailed.into(), message, retryable),
        }
    }
}

fn is_legacy_deps_missing_cc_message(message: &str) -> bool {
    message.contains("deps.jsonl") && message.contains("missing field `cc`")
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

    #[error("checksum mismatch for {blob}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        blob: &'static str,
        expected: StateJsonlSha256,
        actual: StateJsonlSha256,
    },
}

/// Push was rejected by the remote with a status message.
#[derive(Error, Debug)]
#[error("push rejected: {message}")]
pub struct PushRejected {
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::error::details;

    #[test]
    fn legacy_deps_runtime_hint_only_matches_strict_deps_missing_cc() {
        let legacy = SyncError::Wire(WireError::InvalidValue(
            "deps.jsonl: missing field `cc` at line 1 column 42".into(),
        ));
        let unrelated = SyncError::Wire(WireError::InvalidValue(
            "deps.jsonl: invalid type: sequence, expected struct WireDepStoreV1 at line 1 column 1"
                .into(),
        ));
        let other_file = SyncError::Wire(WireError::InvalidValue(
            "state.jsonl: missing field `cc` at line 1 column 7".into(),
        ));

        assert_eq!(
            legacy.legacy_deps_runtime_hint(),
            Some("Run `bd migrate to 1`")
        );
        assert_eq!(unrelated.legacy_deps_runtime_hint(), None);
        assert_eq!(other_file.legacy_deps_runtime_hint(), None);
    }

    #[test]
    fn legacy_deps_runtime_hint_maps_payload_to_validation_failed() {
        let payload = SyncError::Wire(WireError::InvalidValue(
            "deps.jsonl: missing field `cc` at line 1 column 42".into(),
        ))
        .into_error_payload();

        assert_eq!(payload.code, CliErrorCode::ValidationFailed.into());
        assert!(
            payload.message.contains("Run `bd migrate to 1`"),
            "{}",
            payload.message
        );
        let details = serde_json::from_value::<details::ValidationFailedDetails>(
            payload.details.expect("details"),
        )
        .expect("validation details");
        assert_eq!(details.field, "store");
        assert!(
            details
                .reason
                .contains("strict store load rejected legacy deps format"),
            "{}",
            details.reason
        );
    }

    #[test]
    fn migration_warnings_report_no_side_effects() {
        let err = SyncError::MigrationWarnings(vec!["warning".into()]);

        assert_eq!(err.effect(), Effect::None);
    }

    #[test]
    fn io_errors_report_no_side_effects() {
        let err = SyncError::Io(std::io::Error::other("preview tempdir failed"));

        assert_eq!(err.effect(), Effect::Unknown);
    }
}
