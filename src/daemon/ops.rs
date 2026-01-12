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

use crate::core::{
    ActorId, BeadFields, BeadId, BeadType, Closure, CoreError, ErrorCode, Label, Labels, Lww,
    Priority, Stamp, WallClock, Workflow,
};
use crate::daemon::wal::WalError;
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
    pub status: Patch<String>,
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
        if let Patch::Set(v) = &self.labels {
            let mut labels = Labels::new();
            for raw in v {
                let label = Label::parse(raw.clone()).map_err(|e| OpError::ValidationFailed {
                    field: "labels".into(),
                    reason: e.to_string(),
                })?;
                labels.insert(label);
            }
            fields.labels = Lww::new(labels, stamp.clone());
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
            match status.as_str() {
                "open" => fields.workflow = Lww::new(Workflow::Open, stamp.clone()),
                "in_progress" => {
                    fields.workflow = Lww::new(Workflow::InProgress, stamp.clone())
                }
                "closed" => {
                    let closure = Closure::new(None, None);
                    fields.workflow = Lww::new(Workflow::Closed(closure), stamp.clone());
                }
                other => {
                    return Err(OpError::ValidationFailed {
                        field: "status".into(),
                        reason: format!("unknown status {other:?}"),
                    });
                }
            }
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

    #[error("not a git repo: {0}")]
    NotAGitRepo(PathBuf),

    #[error("no origin remote configured for repo: {0}")]
    NoRemote(PathBuf),

    #[error("repo not initialized: {0}")]
    RepoNotInitialized(PathBuf),

    #[error(transparent)]
    Sync(#[from] SyncError),

    #[error("bead is deleted: {0}")]
    BeadDeleted(BeadId),

    #[error(transparent)]
    Wal(#[from] WalError),

    #[error("wal merge conflict: {errors:?}")]
    WalMerge { errors: Vec<CoreError> },

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
            OpError::NotFound(_) => ErrorCode::NotFound,
            OpError::AlreadyExists(_) => ErrorCode::AlreadyExists,
            OpError::AlreadyClaimed { .. } => ErrorCode::AlreadyClaimed,
            OpError::CasMismatch { .. } => ErrorCode::CasMismatch,
            OpError::InvalidTransition { .. } => ErrorCode::InvalidTransition,
            OpError::ValidationFailed { .. } => ErrorCode::ValidationFailed,
            OpError::NotAGitRepo(_) => ErrorCode::NotAGitRepo,
            OpError::NoRemote(_) => ErrorCode::NoRemote,
            OpError::RepoNotInitialized(_) => ErrorCode::RepoNotInitialized,
            OpError::Sync(_) => ErrorCode::SyncFailed,
            OpError::BeadDeleted(_) => ErrorCode::BeadDeleted,
            OpError::Wal(_) => ErrorCode::WalError,
            OpError::WalMerge { .. } => ErrorCode::WalMergeConflict,
            OpError::NotClaimedByYou => ErrorCode::NotClaimedByYou,
            OpError::DepNotFound => ErrorCode::DepNotFound,
            OpError::LoadTimeout { .. } => ErrorCode::LoadTimeout,
            OpError::Internal(_) => ErrorCode::Internal,
        }
    }

    /// Whether retrying this operation may succeed.
    pub fn transience(&self) -> Transience {
        match self {
            OpError::Sync(e) => e.transience(),
            OpError::Wal(e) => match e {
                WalError::Io(_) => Transience::Retryable,
                WalError::Json(_) | WalError::VersionMismatch { .. } => Transience::Permanent,
            },
            OpError::WalMerge { .. } => Transience::Permanent,
            OpError::AlreadyClaimed { .. } => Transience::Retryable,
            OpError::NotFound(_)
            | OpError::AlreadyExists(_)
            | OpError::CasMismatch { .. }
            | OpError::InvalidTransition { .. }
            | OpError::ValidationFailed { .. }
            | OpError::NotAGitRepo(_)
            | OpError::NoRemote(_)
            | OpError::RepoNotInitialized(_)
            | OpError::BeadDeleted(_)
            | OpError::NotClaimedByYou
            | OpError::DepNotFound => Transience::Permanent,
            OpError::LoadTimeout { .. } => Transience::Retryable,
            OpError::Internal(_) => Transience::Retryable,
        }
    }

    /// What we know about side effects when this error is returned.
    pub fn effect(&self) -> Effect {
        match self {
            OpError::Sync(e) => e.effect(),
            OpError::Wal(_) | OpError::WalMerge { .. } => Effect::None,
            _ => Effect::None,
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
