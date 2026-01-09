//! Operations and patches for bead mutations.
//!
//! Provides:
//! - `Patch<T>` - Three-way patch enum (Keep, Clear, Set)
//! - `BeadPatch` - Partial update for bead fields
//! - `BeadOp` - All mutation operations
//! - `OpError` - Operation errors
//! - `OpResult` - Operation results

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::core::{ActorId, BeadId, BeadType, CoreError, DepKind, Priority, WallClock};
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
}

// =============================================================================
// BeadOp - All mutation operations
// =============================================================================

/// Mutation operations on beads.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum BeadOp {
    /// Create a new bead.
    Create {
        repo: PathBuf,
        #[serde(default)]
        id: Option<String>,
        #[serde(default)]
        parent: Option<String>,
        title: String,
        #[serde(rename = "type")]
        bead_type: BeadType,
        priority: Priority,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        design: Option<String>,
        #[serde(default)]
        acceptance_criteria: Option<String>,
        #[serde(default)]
        assignee: Option<String>,
        #[serde(default)]
        external_ref: Option<String>,
        #[serde(default)]
        estimated_minutes: Option<u32>,
        #[serde(default)]
        labels: Vec<String>,
        #[serde(default)]
        dependencies: Vec<String>,
    },

    /// Update an existing bead.
    Update {
        repo: PathBuf,
        id: BeadId,
        patch: BeadPatch,
        /// Optional CAS check - if provided, operation fails if content hash doesn't match.
        #[serde(default)]
        cas: Option<String>,
    },

    /// Add labels to a bead.
    AddLabels {
        repo: PathBuf,
        id: BeadId,
        labels: Vec<String>,
    },

    /// Remove labels from a bead.
    RemoveLabels {
        repo: PathBuf,
        id: BeadId,
        labels: Vec<String>,
    },

    /// Set or clear a parent relationship.
    SetParent {
        repo: PathBuf,
        id: BeadId,
        #[serde(default)]
        parent: Option<BeadId>,
    },

    /// Close a bead.
    Close {
        repo: PathBuf,
        id: BeadId,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        on_branch: Option<String>,
    },

    /// Reopen a closed bead.
    Reopen { repo: PathBuf, id: BeadId },

    /// Delete a bead (soft delete via tombstone).
    Delete {
        repo: PathBuf,
        id: BeadId,
        #[serde(default)]
        reason: Option<String>,
    },

    /// Add a dependency.
    AddDep {
        repo: PathBuf,
        from: BeadId,
        to: BeadId,
        kind: DepKind,
    },

    /// Remove a dependency (soft delete).
    RemoveDep {
        repo: PathBuf,
        from: BeadId,
        to: BeadId,
        kind: DepKind,
    },

    /// Add a note to a bead.
    AddNote {
        repo: PathBuf,
        id: BeadId,
        content: String,
    },

    /// Claim a bead for the current actor.
    Claim {
        repo: PathBuf,
        id: BeadId,
        /// Lease duration in seconds.
        lease_secs: u64,
    },

    /// Release a claim on a bead.
    Unclaim { repo: PathBuf, id: BeadId },

    /// Extend an existing claim.
    ExtendClaim {
        repo: PathBuf,
        id: BeadId,
        /// New lease duration in seconds.
        lease_secs: u64,
    },
}

impl BeadOp {
    /// Get the repo path for this operation.
    pub fn repo(&self) -> &PathBuf {
        match self {
            BeadOp::Create { repo, .. } => repo,
            BeadOp::Update { repo, .. } => repo,
            BeadOp::AddLabels { repo, .. } => repo,
            BeadOp::RemoveLabels { repo, .. } => repo,
            BeadOp::SetParent { repo, .. } => repo,
            BeadOp::Close { repo, .. } => repo,
            BeadOp::Reopen { repo, .. } => repo,
            BeadOp::Delete { repo, .. } => repo,
            BeadOp::AddDep { repo, .. } => repo,
            BeadOp::RemoveDep { repo, .. } => repo,
            BeadOp::AddNote { repo, .. } => repo,
            BeadOp::Claim { repo, .. } => repo,
            BeadOp::Unclaim { repo, .. } => repo,
            BeadOp::ExtendClaim { repo, .. } => repo,
        }
    }

    /// Get the bead ID if this operation targets a specific bead.
    pub fn bead_id(&self) -> Option<&BeadId> {
        match self {
            BeadOp::Create { .. } => None,
            BeadOp::Update { id, .. } => Some(id),
            BeadOp::AddLabels { id, .. } => Some(id),
            BeadOp::RemoveLabels { id, .. } => Some(id),
            BeadOp::SetParent { id, .. } => Some(id),
            BeadOp::Close { id, .. } => Some(id),
            BeadOp::Reopen { id, .. } => Some(id),
            BeadOp::Delete { id, .. } => Some(id),
            BeadOp::AddDep { from, .. } => Some(from),
            BeadOp::RemoveDep { from, .. } => Some(from),
            BeadOp::AddNote { id, .. } => Some(id),
            BeadOp::Claim { id, .. } => Some(id),
            BeadOp::Unclaim { id, .. } => Some(id),
            BeadOp::ExtendClaim { id, .. } => Some(id),
        }
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
    pub fn code(&self) -> &'static str {
        match self {
            OpError::NotFound(_) => "not_found",
            OpError::AlreadyExists(_) => "already_exists",
            OpError::AlreadyClaimed { .. } => "already_claimed",
            OpError::CasMismatch { .. } => "cas_mismatch",
            OpError::InvalidTransition { .. } => "invalid_transition",
            OpError::ValidationFailed { .. } => "validation_failed",
            OpError::NotAGitRepo(_) => "not_a_git_repo",
            OpError::NoRemote(_) => "no_remote",
            OpError::RepoNotInitialized(_) => "repo_not_initialized",
            OpError::Sync(_) => "sync_failed",
            OpError::BeadDeleted(_) => "bead_deleted",
            OpError::Wal(_) => "wal_error",
            OpError::WalMerge { .. } => "wal_merge_conflict",
            OpError::NotClaimedByYou => "not_claimed_by_you",
            OpError::DepNotFound => "dep_not_found",
            OpError::LoadTimeout { .. } => "load_timeout",
            OpError::Internal(_) => "internal",
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

    #[test]
    fn bead_op_repo() {
        let op = BeadOp::Create {
            repo: PathBuf::from("/test"),
            id: None,
            parent: None,
            title: "test".into(),
            bead_type: BeadType::Task,
            priority: Priority::default(),
            description: None,
            design: None,
            acceptance_criteria: None,
            assignee: None,
            external_ref: None,
            estimated_minutes: None,
            labels: Vec::new(),
            dependencies: Vec::new(),
        };
        assert_eq!(op.repo(), &PathBuf::from("/test"));
    }
}
