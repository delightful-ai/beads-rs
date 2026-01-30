//! Daemon-only operation logic and errors.
//!
//! Provides:
//! - `OpError` - Operation errors
//! - `BeadPatchDaemonExt` - Daemon-only patch validation and application

use std::path::PathBuf;

use thiserror::Error;

use crate::core::error::details::OverloadedSubsystem;
use crate::core::{
    ActorId, Applied, BeadFields, BeadId, CliErrorCode, ClientRequestId, DurabilityClass,
    DurabilityReceipt, ErrorCode, InvalidId, Lww, NamespaceId, ProtocolErrorCode, ReplicaId, Stamp,
    WallClock, Watermarks, WorkflowStatus,
};
use crate::daemon::admission::AdmissionRejection;
use crate::daemon::store_runtime::StoreRuntimeError;
use crate::daemon::wal::EventWalError;
use crate::error::{Effect, Transience};
use crate::git::SyncError;

pub use beads_surface::ops::{BeadPatch, OpResult, Patch};

// =============================================================================
// OpError - Operation errors
// =============================================================================

/// Errors that can occur during operations.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum OpError {
    #[error("bead not found: {0}")]
    NotFound(BeadId),

    #[error("bead already exists: {0}")]
    AlreadyExists(BeadId),

    #[error("bead already claimed by {by}, expires at {expires:?}")]
    AlreadyClaimed {
        by: ActorId,
        expires: Option<WallClock>,
    },

    #[error("CAS mismatch: expected {expected}, got {actual}")]
    CasMismatch { expected: String, actual: String },

    #[error("invalid transition from {from} to {to}")]
    InvalidTransition { from: String, to: String },

    #[error("validation failed for field {field}: {reason}")]
    ValidationFailed { field: String, reason: String },

    #[error("invalid request: {reason}")]
    InvalidRequest {
        field: Option<String>,
        reason: String,
    },

    #[error(transparent)]
    InvalidId(#[from] InvalidId),

    #[error("overloaded ({subsystem:?})")]
    Overloaded {
        subsystem: OverloadedSubsystem,
        retry_after_ms: Option<u64>,
        queue_bytes: Option<u64>,
        queue_events: Option<u64>,
    },

    #[error("rate limited")]
    RateLimited {
        retry_after_ms: Option<u64>,
        limit_bytes_per_sec: u64,
    },

    #[error("maintenance mode enabled")]
    MaintenanceMode { reason: Option<String> },

    #[error("client_request_id reuse mismatch for {client_request_id}")]
    ClientRequestIdReuseMismatch {
        namespace: NamespaceId,
        client_request_id: ClientRequestId,
        expected_request_sha256: Box<[u8; 32]>,
        got_request_sha256: Box<[u8; 32]>,
    },

    #[error("not a git repo: {0}")]
    NotAGitRepo(PathBuf),

    #[error("no origin remote configured for repo: {0}")]
    NoRemote(PathBuf),

    #[error("repo not initialized: {0}")]
    RepoNotInitialized(PathBuf),

    #[error(transparent)]
    StoreRuntime(#[from] Box<StoreRuntimeError>),

    #[error(transparent)]
    Sync(#[from] Box<SyncError>),

    #[error("bead is deleted: {0}")]
    BeadDeleted(BeadId),

    #[error("note exceeds max bytes {max_bytes} (got {got_bytes})")]
    NoteTooLarge { max_bytes: usize, got_bytes: usize },

    #[error("transaction exceeds max ops {max_ops} (got {got_ops})")]
    OpsTooMany { max_ops: usize, got_ops: usize },

    #[error("labels exceed max {max_labels} (got {got_labels})")]
    LabelsTooMany {
        max_labels: usize,
        got_labels: usize,
        bead_id: Option<BeadId>,
    },

    #[error("wal record exceeds max bytes {max_wal_record_bytes} (estimated {estimated_bytes})")]
    WalRecordTooLarge {
        max_wal_record_bytes: usize,
        estimated_bytes: usize,
    },

    #[error("durability timeout after {waited_ms}ms for {requested}")]
    DurabilityTimeout {
        requested: DurabilityClass,
        waited_ms: u64,
        pending_replica_ids: Option<Vec<ReplicaId>>,
        receipt: Box<DurabilityReceipt>,
    },

    #[error("durability unavailable for {requested} (eligible={eligible_total})")]
    DurabilityUnavailable {
        requested: DurabilityClass,
        eligible_total: u32,
        eligible_replica_ids: Option<Vec<ReplicaId>>,
    },

    #[error("require_min_seen not satisfied within {waited_ms}ms")]
    RequireMinSeenTimeout {
        waited_ms: u64,
        required: Box<Watermarks<Applied>>,
        current_applied: Box<Watermarks<Applied>>,
    },

    #[error("require_min_seen not currently satisfied")]
    RequireMinSeenUnsatisfied {
        required: Box<Watermarks<Applied>>,
        current_applied: Box<Watermarks<Applied>>,
    },

    #[error("namespace invalid: {namespace} ({reason})")]
    NamespaceInvalid { namespace: String, reason: String },

    #[error("namespace unknown: {namespace}")]
    NamespaceUnknown { namespace: NamespaceId },

    #[error("namespace policy violation for {namespace}: {rule}")]
    NamespacePolicyViolation {
        namespace: NamespaceId,
        rule: String,
        reason: Option<String>,
    },

    #[error("cross-namespace dependency from {from_namespace} to {to_namespace}")]
    CrossNamespaceDependency {
        from_namespace: NamespaceId,
        to_namespace: NamespaceId,
    },

    #[error(transparent)]
    EventWal(#[from] Box<EventWalError>),

    #[error("cannot unclaim - not claimed by you")]
    NotClaimedByYou,

    #[error("dependency not found")]
    DepNotFound,

    #[error(
        "initial fetch timed out after {timeout_secs}s for {repo} (remote: {remote}). \
Check SSH auth (ssh-add -l) or run `git fetch origin refs/heads/beads/store:refs/remotes/origin/beads/store`."
    )]
    LoadTimeout {
        repo: PathBuf,
        timeout_secs: u64,
        remote: String,
    },

    #[error("daemon internal error: {0}")]
    Internal(&'static str),
}

impl OpError {
    /// Get the error code for IPC responses.
    pub fn code(&self) -> ErrorCode {
        match self {
            OpError::NotFound(_) => CliErrorCode::NotFound.into(),
            OpError::AlreadyExists(_) => CliErrorCode::AlreadyExists.into(),
            OpError::AlreadyClaimed { .. } => CliErrorCode::AlreadyClaimed.into(),
            OpError::CasMismatch { .. } => CliErrorCode::CasMismatch.into(),
            OpError::InvalidTransition { .. } => CliErrorCode::InvalidTransition.into(),
            OpError::ValidationFailed { .. } => CliErrorCode::ValidationFailed.into(),
            OpError::InvalidRequest { .. } => ProtocolErrorCode::InvalidRequest.into(),
            OpError::InvalidId(_) => CliErrorCode::InvalidId.into(),
            OpError::Overloaded { .. } => ProtocolErrorCode::Overloaded.into(),
            OpError::RateLimited { .. } => ProtocolErrorCode::RateLimited.into(),
            OpError::MaintenanceMode { .. } => ProtocolErrorCode::MaintenanceMode.into(),
            OpError::ClientRequestIdReuseMismatch { .. } => {
                ProtocolErrorCode::ClientRequestIdReuseMismatch.into()
            }
            OpError::NotAGitRepo(_) => CliErrorCode::NotAGitRepo.into(),
            OpError::NoRemote(_) => CliErrorCode::NoRemote.into(),
            OpError::RepoNotInitialized(_) => CliErrorCode::RepoNotInitialized.into(),
            OpError::StoreRuntime(err) => err.code(),
            OpError::Sync(err) => err.code(),
            OpError::BeadDeleted(_) => CliErrorCode::BeadDeleted.into(),
            OpError::NoteTooLarge { .. } => ProtocolErrorCode::NoteTooLarge.into(),
            OpError::OpsTooMany { .. } => ProtocolErrorCode::OpsTooMany.into(),
            OpError::LabelsTooMany { .. } => ProtocolErrorCode::LabelsTooMany.into(),
            OpError::DurabilityTimeout { .. } => ProtocolErrorCode::DurabilityTimeout.into(),
            OpError::DurabilityUnavailable { .. } => {
                ProtocolErrorCode::DurabilityUnavailable.into()
            }
            OpError::RequireMinSeenTimeout { .. } => {
                ProtocolErrorCode::RequireMinSeenTimeout.into()
            }
            OpError::RequireMinSeenUnsatisfied { .. } => {
                ProtocolErrorCode::RequireMinSeenUnsatisfied.into()
            }
            OpError::NamespaceInvalid { .. } => ProtocolErrorCode::NamespaceInvalid.into(),
            OpError::NamespaceUnknown { .. } => ProtocolErrorCode::NamespaceUnknown.into(),
            OpError::NamespacePolicyViolation { .. } => {
                ProtocolErrorCode::NamespacePolicyViolation.into()
            }
            OpError::CrossNamespaceDependency { .. } => {
                ProtocolErrorCode::CrossNamespaceDependency.into()
            }
            OpError::WalRecordTooLarge { .. } => ProtocolErrorCode::WalRecordTooLarge.into(),
            OpError::EventWal(err) => err.code(),
            OpError::NotClaimedByYou => CliErrorCode::NotClaimedByYou.into(),
            OpError::DepNotFound => CliErrorCode::DepNotFound.into(),
            OpError::LoadTimeout { .. } => CliErrorCode::LoadTimeout.into(),
            OpError::Internal(_) => CliErrorCode::Internal.into(),
        }
    }

    /// Whether retrying this operation may succeed.
    pub fn transience(&self) -> Transience {
        match self {
            OpError::Sync(e) => e.transience(),
            OpError::AlreadyClaimed { .. } => Transience::Retryable,
            OpError::Overloaded { .. } => Transience::Retryable,
            OpError::RateLimited { .. } => Transience::Retryable,
            OpError::MaintenanceMode { .. } => Transience::Retryable,
            OpError::NotFound(_)
            | OpError::AlreadyExists(_)
            | OpError::CasMismatch { .. }
            | OpError::InvalidTransition { .. }
            | OpError::ValidationFailed { .. }
            | OpError::InvalidRequest { .. }
            | OpError::InvalidId(_)
            | OpError::ClientRequestIdReuseMismatch { .. }
            | OpError::NotAGitRepo(_)
            | OpError::NoRemote(_)
            | OpError::RepoNotInitialized(_)
            | OpError::BeadDeleted(_)
            | OpError::NoteTooLarge { .. }
            | OpError::OpsTooMany { .. }
            | OpError::WalRecordTooLarge { .. }
            | OpError::LabelsTooMany { .. }
            | OpError::NotClaimedByYou
            | OpError::DepNotFound
            | OpError::NamespacePolicyViolation { .. }
            | OpError::CrossNamespaceDependency { .. } => Transience::Permanent,
            OpError::StoreRuntime(err) => err.transience(),
            OpError::DurabilityTimeout { .. } => Transience::Retryable,
            OpError::DurabilityUnavailable { .. } => Transience::Permanent,
            OpError::RequireMinSeenTimeout { .. } => Transience::Retryable,
            OpError::RequireMinSeenUnsatisfied { .. } => Transience::Retryable,
            OpError::NamespaceInvalid { .. } | OpError::NamespaceUnknown { .. } => {
                Transience::Permanent
            }
            OpError::LoadTimeout { .. } => Transience::Retryable,
            OpError::Internal(_) => Transience::Retryable,
            OpError::EventWal(err) => err.transience(),
        }
    }

    /// What we know about side effects when this error is returned.
    pub fn effect(&self) -> Effect {
        match self {
            OpError::Sync(e) => e.effect(),
            OpError::EventWal(_) => Effect::None,
            OpError::DurabilityTimeout { .. } => Effect::Some,
            OpError::StoreRuntime(_) => Effect::None,
            _ => Effect::None,
        }
    }
}

impl From<StoreRuntimeError> for OpError {
    fn from(err: StoreRuntimeError) -> Self {
        OpError::StoreRuntime(Box::new(err))
    }
}

impl From<SyncError> for OpError {
    fn from(err: SyncError) -> Self {
        OpError::Sync(Box::new(err))
    }
}

impl From<EventWalError> for OpError {
    fn from(err: EventWalError) -> Self {
        OpError::EventWal(Box::new(err))
    }
}

impl From<AdmissionRejection> for OpError {
    fn from(rejection: AdmissionRejection) -> Self {
        OpError::Overloaded {
            subsystem: rejection.subsystem,
            retry_after_ms: Some(rejection.retry_after_ms),
            queue_bytes: rejection.queue_bytes,
            queue_events: rejection.queue_events,
        }
    }
}

impl OpError {
    /// Convert a LiveLookupError to OpError with the given bead ID.
    pub fn from_live_lookup(err: crate::core::LiveLookupError, id: BeadId) -> Self {
        match err {
            crate::core::LiveLookupError::NotFound => OpError::NotFound(id),
            crate::core::LiveLookupError::Deleted => OpError::BeadDeleted(id),
        }
    }
}

/// Extension trait for mapping LiveLookupError to OpError.
pub trait MapLiveError<T> {
    /// Map a LiveLookupError to OpError using the given bead ID.
    fn map_live_err(self, id: &BeadId) -> Result<T, OpError>;
}

impl<T> MapLiveError<T> for Result<T, crate::core::LiveLookupError> {
    fn map_live_err(self, id: &BeadId) -> Result<T, OpError> {
        self.map_err(|e| OpError::from_live_lookup(e, id.clone()))
    }
}

pub trait BeadPatchDaemonExt {
    fn validate_for_daemon(&self) -> Result<(), OpError>;
    fn apply_to_fields(&self, fields: &mut BeadFields, stamp: &Stamp) -> Result<(), OpError>;
}

#[derive(Clone, Debug)]
pub struct ValidatedSurfaceBeadPatch {
    inner: BeadPatch,
}

impl ValidatedSurfaceBeadPatch {
    pub fn as_inner(&self) -> &BeadPatch {
        &self.inner
    }

    pub fn into_inner(self) -> BeadPatch {
        self.inner
    }
}

impl std::ops::Deref for ValidatedSurfaceBeadPatch {
    type Target = BeadPatch;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl AsRef<BeadPatch> for ValidatedSurfaceBeadPatch {
    fn as_ref(&self) -> &BeadPatch {
        &self.inner
    }
}

impl TryFrom<BeadPatch> for ValidatedSurfaceBeadPatch {
    type Error = OpError;

    fn try_from(mut patch: BeadPatch) -> Result<Self, Self::Error> {
        normalize_required_patch(&mut patch)?;
        Ok(Self { inner: patch })
    }
}

impl BeadPatchDaemonExt for BeadPatch {
    fn validate_for_daemon(&self) -> Result<(), OpError> {
        validate_surface_patch(self)
    }

    fn apply_to_fields(&self, fields: &mut BeadFields, stamp: &Stamp) -> Result<(), OpError> {
        if let Patch::Set(v) = &self.title {
            fields.title = Lww::new(v.clone(), stamp.clone());
        }
        if let Patch::Set(v) = &self.description {
            fields.description = Lww::new(v.clone(), stamp.clone());
        }
        match &self.design {
            Patch::Set(v) => fields.design = Lww::new(Some(v.clone()), stamp.clone()),
            Patch::Clear => fields.design = Lww::new(None, stamp.clone()),
            Patch::Keep => {}
        }
        match &self.acceptance_criteria {
            Patch::Set(v) => fields.acceptance_criteria = Lww::new(Some(v.clone()), stamp.clone()),
            Patch::Clear => fields.acceptance_criteria = Lww::new(None, stamp.clone()),
            Patch::Keep => {}
        }
        if let Patch::Set(v) = &self.priority {
            fields.priority = Lww::new(*v, stamp.clone());
        }
        if let Patch::Set(v) = &self.bead_type {
            fields.bead_type = Lww::new(*v, stamp.clone());
        }
        match &self.external_ref {
            Patch::Set(v) => fields.external_ref = Lww::new(Some(v.clone()), stamp.clone()),
            Patch::Clear => fields.external_ref = Lww::new(None, stamp.clone()),
            Patch::Keep => {}
        }
        match &self.source_repo {
            Patch::Set(v) => fields.source_repo = Lww::new(Some(v.clone()), stamp.clone()),
            Patch::Clear => fields.source_repo = Lww::new(None, stamp.clone()),
            Patch::Keep => {}
        }
        match &self.estimated_minutes {
            Patch::Set(v) => fields.estimated_minutes = Lww::new(Some(*v), stamp.clone()),
            Patch::Clear => fields.estimated_minutes = Lww::new(None, stamp.clone()),
            Patch::Keep => {}
        }
        if let Patch::Set(status) = &self.status {
            let workflow_status: WorkflowStatus = (*status).into();
            fields.workflow = Lww::new(workflow_status.into_workflow(None, None), stamp.clone());
        }

        Ok(())
    }
}

fn validate_surface_patch(patch: &BeadPatch) -> Result<(), OpError> {
    validate_required_patch_field("title", &patch.title)?;
    validate_required_patch_field("description", &patch.description)?;
    Ok(())
}

fn normalize_required_patch(patch: &mut BeadPatch) -> Result<(), OpError> {
    normalize_required_patch_field("title", &mut patch.title)?;
    normalize_required_patch_field("description", &mut patch.description)?;
    Ok(())
}

fn validate_required_patch_field(field: &str, patch: &Patch<String>) -> Result<(), OpError> {
    match patch {
        Patch::Clear => Err(OpError::ValidationFailed {
            field: field.to_string(),
            reason: "cannot clear required field".into(),
        }),
        Patch::Set(value) if value.trim().is_empty() => Err(OpError::ValidationFailed {
            field: field.to_string(),
            reason: "cannot set required field to empty".into(),
        }),
        _ => Ok(()),
    }
}

fn normalize_required_patch_field(field: &str, patch: &mut Patch<String>) -> Result<(), OpError> {
    match patch {
        Patch::Set(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Err(OpError::ValidationFailed {
                    field: field.to_string(),
                    reason: "cannot set required field to empty".into(),
                });
            }
            if trimmed.len() != value.len() {
                *value = trimmed.to_string();
            }
            Ok(())
        }
        Patch::Clear => Err(OpError::ValidationFailed {
            field: field.to_string(),
            reason: "cannot clear required field".into(),
        }),
        Patch::Keep => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bead_patch_validation() {
        let mut patch = BeadPatch::default();
        assert!(patch.validate_for_daemon().is_ok());

        patch.title = Patch::Clear;
        assert!(patch.validate_for_daemon().is_err());
    }
}
