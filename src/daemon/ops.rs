//! Operations and patches for bead mutations.
//!
//! Provides:
//! - `Patch<T>` - Three-way patch enum (Keep, Clear, Set)
//! - `BeadPatch` - Partial update for bead fields
//! - `OpError` - Operation errors
//! - `OpResult` - Operation results

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::core::error::details::OverloadedSubsystem;
use crate::core::{
    ActorId, Applied, BeadFields, BeadId, BeadType, CliErrorCode, ClientRequestId, DurabilityClass,
    DurabilityReceipt, ErrorCode, InvalidId, Lww, NamespaceId, Priority, ProtocolErrorCode,
    ReplicaId, Stamp, WallClock, Watermarks, WorkflowStatus,
};
use crate::daemon::admission::AdmissionRejection;
use crate::daemon::store_lock::StoreLockError;
use crate::daemon::store_runtime::StoreRuntimeError;
use crate::daemon::wal::{EventWalError, WalIndexError, WalReplayError};
use crate::error::{Effect, Transience};
use crate::git::SyncError;

// =============================================================================
// Patch<T> - Three-way field update
// =============================================================================

/// Three-way patch for updating a field.
///
/// This is the clean solution to the "Option<Option<T>>" problem for nullable fields:
/// - `Keep` - Don't change the field
/// - `Clear` - Set the field to None
/// - `Set(T)` - Set the field to Some(T)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Patch<T> {
    /// Don't change the field.
    #[default]
    Keep,
    /// Clear the field (set to None).
    Clear,
    /// Set the field to a new value.
    Set(T),
}

impl<T> Patch<T> {
    /// Check if this patch would change the value.
    pub fn is_keep(&self) -> bool {
        matches!(self, Patch::Keep)
    }

    /// Apply the patch to a current value.
    pub fn apply(self, current: Option<T>) -> Option<T> {
        match self {
            Patch::Keep => current,
            Patch::Clear => None,
            Patch::Set(v) => Some(v),
        }
    }
}

// Custom serde for Patch: absent = Keep, null = Clear, value = Set
impl<T: Serialize> Serialize for Patch<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Patch::Keep => serializer.serialize_none(), // Won't actually be serialized if skip_serializing_if
            Patch::Clear => serializer.serialize_none(),
            Patch::Set(v) => v.serialize(serializer),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Patch<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // If present and null -> Clear
        // If present and value -> Set
        // If absent -> Keep (handled by #[serde(default)])
        let opt: Option<T> = Option::deserialize(deserializer)?;
        match opt {
            None => Ok(Patch::Clear),
            Some(v) => Ok(Patch::Set(v)),
        }
    }
}

// =============================================================================
// BeadPatch - Partial update for bead fields
// =============================================================================

/// Partial update for bead fields.
///
/// All fields default to `Keep`, meaning no change.
/// Use `Patch::Set(value)` to update a field.
/// Use `Patch::Clear` to clear a nullable field.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BeadPatch {
    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub title: Patch<String>,

    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub description: Patch<String>,

    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub design: Patch<String>,

    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub acceptance_criteria: Patch<String>,

    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub priority: Patch<Priority>,

    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub bead_type: Patch<BeadType>,

    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub labels: Patch<Vec<String>>,

    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub external_ref: Patch<String>,

    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub source_repo: Patch<String>,

    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub estimated_minutes: Patch<u32>,

    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub status: Patch<WorkflowStatus>,
}

impl BeadPatch {
    /// Validate the patch, returning error if invalid.
    ///
    /// Rules:
    /// - Cannot clear required fields (title, description)
    pub fn validate(&self) -> Result<(), OpError> {
        if matches!(self.title, Patch::Clear) {
            return Err(OpError::ValidationFailed {
                field: "title".into(),
                reason: "cannot clear required field".into(),
            });
        }
        if matches!(self.description, Patch::Clear) {
            return Err(OpError::ValidationFailed {
                field: "description".into(),
                reason: "cannot clear required field".into(),
            });
        }

        if let Patch::Set(labels) = &self.labels {
            for raw in labels {
                crate::core::Label::parse(raw.clone()).map_err(|e| OpError::ValidationFailed {
                    field: "labels".into(),
                    reason: e.to_string(),
                })?;
            }
        }
        Ok(())
    }

    /// Check if this patch has any changes.
    pub fn is_empty(&self) -> bool {
        self.title.is_keep()
            && self.description.is_keep()
            && self.design.is_keep()
            && self.acceptance_criteria.is_keep()
            && self.priority.is_keep()
            && self.bead_type.is_keep()
            && self.labels.is_keep()
            && self.external_ref.is_keep()
            && self.source_repo.is_keep()
            && self.estimated_minutes.is_keep()
            && self.status.is_keep()
    }

    /// Apply this patch to bead fields.
    pub fn apply_to_fields(&self, fields: &mut BeadFields, stamp: &Stamp) -> Result<(), OpError> {
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
        if !self.labels.is_keep() {
            return Err(OpError::Internal(
                "labels patch must be applied via label store",
            ));
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
            fields.workflow = Lww::new(status.into_workflow(None, None), stamp.clone());
        }

        Ok(())
    }
}

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
            OpError::StoreRuntime(err) => store_runtime_error_code(err.as_ref()),
            OpError::Sync(_) => CliErrorCode::SyncFailed.into(),
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
            OpError::EventWal(err) => event_wal_error_code(err.as_ref()),
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
            OpError::StoreRuntime(err) => store_runtime_transience(err.as_ref()),
            OpError::DurabilityTimeout { .. } => Transience::Retryable,
            OpError::DurabilityUnavailable { .. } => Transience::Permanent,
            OpError::RequireMinSeenTimeout { .. } => Transience::Retryable,
            OpError::RequireMinSeenUnsatisfied { .. } => Transience::Retryable,
            OpError::NamespaceInvalid { .. } | OpError::NamespaceUnknown { .. } => {
                Transience::Permanent
            }
            OpError::LoadTimeout { .. } => Transience::Retryable,
            OpError::Internal(_) => Transience::Retryable,
            OpError::EventWal(err) => event_wal_transience(err.as_ref()),
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

fn event_wal_error_code(err: &EventWalError) -> ErrorCode {
    match err {
        EventWalError::RecordTooLarge { .. } => ProtocolErrorCode::WalRecordTooLarge.into(),
        EventWalError::SegmentHeaderUnsupportedVersion { .. } => wal_segment_header_error_code(err),
        EventWalError::Symlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
        EventWalError::Io { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ProtocolErrorCode::PermissionDenied.into()
            } else {
                CliErrorCode::IoError.into()
            }
        }
        _ => ProtocolErrorCode::WalCorrupt.into(),
    }
}

fn event_wal_transience(err: &EventWalError) -> Transience {
    match err {
        EventWalError::Symlink { .. } => Transience::Permanent,
        EventWalError::Io { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                Transience::Permanent
            } else {
                Transience::Retryable
            }
        }
        _ => Transience::Permanent,
    }
}

fn store_runtime_error_code(err: &StoreRuntimeError) -> ErrorCode {
    match err {
        StoreRuntimeError::Lock(lock_err) => store_lock_error_code(lock_err),
        StoreRuntimeError::MetaSymlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
        StoreRuntimeError::MetaRead { source, .. }
        | StoreRuntimeError::MetaWrite { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ProtocolErrorCode::PermissionDenied.into()
            } else {
                ProtocolErrorCode::InternalError.into()
            }
        }
        StoreRuntimeError::MetaParse { .. } => ProtocolErrorCode::Corruption.into(),
        StoreRuntimeError::MetaMismatch { .. } => ProtocolErrorCode::WrongStore.into(),
        StoreRuntimeError::UnsupportedStoreMetaVersion { .. } => {
            ProtocolErrorCode::VersionIncompatible.into()
        }
        StoreRuntimeError::NamespacePoliciesSymlink { .. }
        | StoreRuntimeError::ReplicaRosterSymlink { .. } => {
            ProtocolErrorCode::PathSymlinkRejected.into()
        }
        StoreRuntimeError::NamespacePoliciesRead { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ProtocolErrorCode::PermissionDenied.into()
            } else {
                CliErrorCode::ValidationFailed.into()
            }
        }
        StoreRuntimeError::NamespacePoliciesParse { .. } => CliErrorCode::ValidationFailed.into(),
        StoreRuntimeError::ReplicaRosterRead { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ProtocolErrorCode::PermissionDenied.into()
            } else {
                CliErrorCode::ValidationFailed.into()
            }
        }
        StoreRuntimeError::ReplicaRosterParse { .. } => CliErrorCode::ValidationFailed.into(),
        StoreRuntimeError::StoreConfigSymlink { .. } => {
            ProtocolErrorCode::PathSymlinkRejected.into()
        }
        StoreRuntimeError::StoreConfigRead { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ProtocolErrorCode::PermissionDenied.into()
            } else {
                CliErrorCode::ValidationFailed.into()
            }
        }
        StoreRuntimeError::StoreConfigParse { .. } => CliErrorCode::ValidationFailed.into(),
        StoreRuntimeError::StoreConfigSerialize { .. } => ProtocolErrorCode::InternalError.into(),
        StoreRuntimeError::StoreConfigWrite { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ProtocolErrorCode::PermissionDenied.into()
            } else {
                ProtocolErrorCode::InternalError.into()
            }
        }
        StoreRuntimeError::WalIndex(err) => wal_index_error_code(err),
        StoreRuntimeError::WalReplay(err) => wal_replay_error_code(err),
        StoreRuntimeError::WatermarkInvalid { .. } => ProtocolErrorCode::IndexCorrupt.into(),
    }
}

fn store_lock_error_code(err: &StoreLockError) -> ErrorCode {
    match err {
        StoreLockError::Held { .. } => ProtocolErrorCode::LockHeld.into(),
        StoreLockError::Symlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
        StoreLockError::MetadataCorrupt { .. } => ProtocolErrorCode::Corruption.into(),
        StoreLockError::Io { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ProtocolErrorCode::PermissionDenied.into()
            } else {
                ProtocolErrorCode::InternalError.into()
            }
        }
    }
}

fn store_runtime_transience(err: &StoreRuntimeError) -> Transience {
    match err {
        StoreRuntimeError::Lock(lock_err) => match lock_err {
            StoreLockError::Held { .. } => Transience::Permanent,
            StoreLockError::Symlink { .. } | StoreLockError::MetadataCorrupt { .. } => {
                Transience::Permanent
            }
            StoreLockError::Io { source, .. } => {
                if source.kind() == std::io::ErrorKind::PermissionDenied {
                    Transience::Permanent
                } else {
                    Transience::Retryable
                }
            }
        },
        StoreRuntimeError::MetaSymlink { .. } => Transience::Permanent,
        StoreRuntimeError::MetaParse { .. }
        | StoreRuntimeError::MetaMismatch { .. }
        | StoreRuntimeError::UnsupportedStoreMetaVersion { .. } => Transience::Permanent,
        StoreRuntimeError::MetaRead { source, .. }
        | StoreRuntimeError::MetaWrite { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                Transience::Permanent
            } else {
                Transience::Retryable
            }
        }
        StoreRuntimeError::NamespacePoliciesSymlink { .. }
        | StoreRuntimeError::ReplicaRosterSymlink { .. } => Transience::Permanent,
        StoreRuntimeError::NamespacePoliciesRead { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                Transience::Permanent
            } else {
                Transience::Retryable
            }
        }
        StoreRuntimeError::NamespacePoliciesParse { .. } => Transience::Permanent,
        StoreRuntimeError::ReplicaRosterRead { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                Transience::Permanent
            } else {
                Transience::Retryable
            }
        }
        StoreRuntimeError::ReplicaRosterParse { .. } => Transience::Permanent,
        StoreRuntimeError::StoreConfigSymlink { .. }
        | StoreRuntimeError::StoreConfigParse { .. }
        | StoreRuntimeError::StoreConfigSerialize { .. } => Transience::Permanent,
        StoreRuntimeError::StoreConfigRead { source, .. }
        | StoreRuntimeError::StoreConfigWrite { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                Transience::Permanent
            } else {
                Transience::Retryable
            }
        }
        StoreRuntimeError::WalIndex(err) => wal_index_transience(err),
        StoreRuntimeError::WalReplay(err) => wal_replay_transience(err),
        StoreRuntimeError::WatermarkInvalid { .. } => Transience::Permanent,
    }
}

fn wal_index_error_code(err: &WalIndexError) -> ErrorCode {
    match err {
        WalIndexError::SchemaVersionMismatch { .. } => {
            ProtocolErrorCode::IndexRebuildRequired.into()
        }
        WalIndexError::Equivocation { .. } => ProtocolErrorCode::Equivocation.into(),
        WalIndexError::ClientRequestIdReuseMismatch { .. } => {
            ProtocolErrorCode::ClientRequestIdReuseMismatch.into()
        }
        WalIndexError::Symlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
        WalIndexError::MetaMismatch { key, .. } => match *key {
            "store_id" => ProtocolErrorCode::WrongStore.into(),
            "store_epoch" => ProtocolErrorCode::StoreEpochMismatch.into(),
            _ => ProtocolErrorCode::IndexCorrupt.into(),
        },
        WalIndexError::MetaMissing { .. }
        | WalIndexError::EventIdDecode(_)
        | WalIndexError::HlcRowDecode(_)
        | WalIndexError::SegmentRowDecode(_)
        | WalIndexError::WatermarkRowDecode(_)
        | WalIndexError::ReplicaLivenessRowDecode(_)
        | WalIndexError::CborDecode(_)
        | WalIndexError::CborEncode(_)
        | WalIndexError::ConcurrentWrite { .. }
        | WalIndexError::OriginSeqOverflow { .. } => ProtocolErrorCode::IndexCorrupt.into(),
        WalIndexError::Sqlite(_) => ProtocolErrorCode::IndexCorrupt.into(),
        WalIndexError::Io { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ProtocolErrorCode::PermissionDenied.into()
            } else {
                CliErrorCode::IoError.into()
            }
        }
    }
}

fn wal_replay_error_code(err: &WalReplayError) -> ErrorCode {
    match err {
        WalReplayError::Symlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
        WalReplayError::Io { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ProtocolErrorCode::PermissionDenied.into()
            } else {
                CliErrorCode::IoError.into()
            }
        }
        WalReplayError::SegmentHeader { source, .. } => wal_segment_header_error_code(source),
        WalReplayError::SegmentHeaderMismatch { .. } => {
            ProtocolErrorCode::SegmentHeaderMismatch.into()
        }
        WalReplayError::RecordShaMismatch(_) => ProtocolErrorCode::HashMismatch.into(),
        WalReplayError::RecordDecode { .. }
        | WalReplayError::EventBodyDecode { .. }
        | WalReplayError::RecordHeaderMismatch { .. }
        | WalReplayError::MissingHead { .. }
        | WalReplayError::UnexpectedHead { .. }
        | WalReplayError::SealedSegmentFinalLenMissing { .. }
        | WalReplayError::SealedSegmentLenMismatch { .. }
        | WalReplayError::MidFileCorruption { .. } => ProtocolErrorCode::WalCorrupt.into(),
        WalReplayError::NonContiguousSeq { .. } => ProtocolErrorCode::GapDetected.into(),
        WalReplayError::PrevShaMismatch { .. } => ProtocolErrorCode::PrevShaMismatch.into(),
        WalReplayError::IndexOffsetInvalid { .. } | WalReplayError::OriginSeqOverflow { .. } => {
            ProtocolErrorCode::IndexCorrupt.into()
        }
        WalReplayError::Index(err) => wal_index_error_code(err),
    }
}

fn wal_segment_header_error_code(source: &EventWalError) -> ErrorCode {
    match source {
        EventWalError::SegmentHeaderUnsupportedVersion { .. } => {
            ProtocolErrorCode::WalFormatUnsupported.into()
        }
        _ => ProtocolErrorCode::WalCorrupt.into(),
    }
}

fn wal_index_transience(err: &WalIndexError) -> Transience {
    match err {
        WalIndexError::Symlink { .. } => Transience::Permanent,
        WalIndexError::SchemaVersionMismatch { .. } => Transience::Retryable,
        WalIndexError::Io { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                Transience::Permanent
            } else {
                Transience::Retryable
            }
        }
        WalIndexError::MetaMismatch {
            key: "store_id" | "store_epoch",
            ..
        } => Transience::Permanent,
        WalIndexError::MetaMismatch { .. } => Transience::Retryable,
        WalIndexError::Equivocation { .. } | WalIndexError::ClientRequestIdReuseMismatch { .. } => {
            Transience::Permanent
        }
        WalIndexError::ConcurrentWrite { .. } => Transience::Retryable,
        _ => Transience::Retryable,
    }
}

fn wal_replay_transience(err: &WalReplayError) -> Transience {
    match err {
        WalReplayError::Symlink { .. } => Transience::Permanent,
        WalReplayError::Io { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                Transience::Permanent
            } else {
                Transience::Retryable
            }
        }
        _ => Transience::Permanent,
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

// =============================================================================
// OpResult - Operation results
// =============================================================================

/// Result of a successful operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum OpResult {
    /// Bead was created.
    Created { id: BeadId },

    /// Bead was updated.
    Updated { id: BeadId },

    /// Bead was closed.
    Closed { id: BeadId },

    /// Bead was reopened.
    Reopened { id: BeadId },

    /// Bead was deleted.
    Deleted { id: BeadId },

    /// Dependency was added.
    DepAdded { from: BeadId, to: BeadId },

    /// Dependency was removed.
    DepRemoved { from: BeadId, to: BeadId },

    /// Note was added.
    NoteAdded { bead_id: BeadId, note_id: String },

    /// Bead was claimed.
    Claimed { id: BeadId, expires: WallClock },

    /// Claim was released.
    Unclaimed { id: BeadId },

    /// Claim was extended.
    ClaimExtended { id: BeadId, expires: WallClock },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patch_default_is_keep() {
        let patch: Patch<String> = Patch::default();
        assert!(patch.is_keep());
    }

    #[test]
    fn patch_apply() {
        let current = Some("old".to_string());

        assert_eq!(Patch::Keep.apply(current.clone()), Some("old".to_string()));
        assert_eq!(Patch::<String>::Clear.apply(current.clone()), None);
        assert_eq!(
            Patch::Set("new".to_string()).apply(current),
            Some("new".to_string())
        );
    }

    #[test]
    fn bead_patch_validation() {
        let mut patch = BeadPatch::default();
        assert!(patch.validate().is_ok());

        patch.title = Patch::Clear;
        assert!(patch.validate().is_err());
    }
}
