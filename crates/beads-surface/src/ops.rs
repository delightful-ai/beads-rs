use serde::{Deserialize, Serialize};
use thiserror::Error;

use beads_core::{BeadId, BeadType, Priority, WallClock, WorkflowStatus};

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
// OpenInProgress - Workflow states allowed in update patches
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenInProgress {
    Open,
    InProgress,
}

impl From<OpenInProgress> for WorkflowStatus {
    fn from(status: OpenInProgress) -> Self {
        match status {
            OpenInProgress::Open => WorkflowStatus::Open,
            OpenInProgress::InProgress => WorkflowStatus::InProgress,
        }
    }
}

// =============================================================================
// BeadPatch - Partial update for bead fields
// =============================================================================

/// Required `BeadPatch` fields that may be updated but never cleared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredPatchField {
    Title,
    Description,
}

impl RequiredPatchField {
    pub const fn as_str(self) -> &'static str {
        match self {
            RequiredPatchField::Title => "title",
            RequiredPatchField::Description => "description",
        }
    }
}

/// Typed validation failures for update patch semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BeadPatchValidationError {
    #[error("cannot clear required field")]
    RequiredFieldCleared { field: RequiredPatchField },

    #[error("cannot set required field to empty")]
    RequiredFieldEmpty { field: RequiredPatchField },
}

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
    pub external_ref: Patch<String>,

    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub source_repo: Patch<String>,

    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub estimated_minutes: Patch<u32>,

    #[serde(default, skip_serializing_if = "Patch::is_keep")]
    pub status: Patch<OpenInProgress>,
}

impl BeadPatch {
    /// Check if this patch has any changes.
    pub fn is_empty(&self) -> bool {
        self.title.is_keep()
            && self.description.is_keep()
            && self.design.is_keep()
            && self.acceptance_criteria.is_keep()
            && self.priority.is_keep()
            && self.bead_type.is_keep()
            && self.external_ref.is_keep()
            && self.source_repo.is_keep()
            && self.estimated_minutes.is_keep()
            && self.status.is_keep()
    }

    /// Validate patch semantics for update operations.
    pub fn validate_for_update(&self) -> Result<(), BeadPatchValidationError> {
        validate_required_patch_field(RequiredPatchField::Title, &self.title)?;
        validate_required_patch_field(RequiredPatchField::Description, &self.description)?;
        Ok(())
    }

    /// Normalize required string fields by trimming whitespace.
    pub fn normalize_for_update(&mut self) -> Result<(), BeadPatchValidationError> {
        normalize_required_patch_field(RequiredPatchField::Title, &mut self.title)?;
        normalize_required_patch_field(RequiredPatchField::Description, &mut self.description)?;
        Ok(())
    }
}

fn validate_required_patch_field(
    field: RequiredPatchField,
    patch: &Patch<String>,
) -> Result<(), BeadPatchValidationError> {
    match patch {
        Patch::Clear => Err(BeadPatchValidationError::RequiredFieldCleared { field }),
        Patch::Set(value) if value.trim().is_empty() => {
            Err(BeadPatchValidationError::RequiredFieldEmpty { field })
        }
        Patch::Set(_) | Patch::Keep => Ok(()),
    }
}

fn normalize_required_patch_field(
    field: RequiredPatchField,
    patch: &mut Patch<String>,
) -> Result<(), BeadPatchValidationError> {
    match patch {
        Patch::Set(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Err(BeadPatchValidationError::RequiredFieldEmpty { field });
            }
            if trimmed.len() != value.len() {
                *value = trimmed.to_string();
            }
            Ok(())
        }
        Patch::Clear => Err(BeadPatchValidationError::RequiredFieldCleared { field }),
        Patch::Keep => Ok(()),
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
    fn bead_patch_validation_rejects_required_field_clear() {
        let mut patch = BeadPatch::default();
        patch.title = Patch::Clear;

        assert_eq!(
            patch.validate_for_update(),
            Err(BeadPatchValidationError::RequiredFieldCleared {
                field: RequiredPatchField::Title
            })
        );
    }

    #[test]
    fn bead_patch_validation_rejects_required_field_empty() {
        let mut patch = BeadPatch::default();
        patch.description = Patch::Set("   ".to_string());

        assert_eq!(
            patch.validate_for_update(),
            Err(BeadPatchValidationError::RequiredFieldEmpty {
                field: RequiredPatchField::Description
            })
        );
    }

    #[test]
    fn bead_patch_validation_rejects_description_clear() {
        let mut patch = BeadPatch::default();
        patch.description = Patch::Clear;

        assert_eq!(
            patch.validate_for_update(),
            Err(BeadPatchValidationError::RequiredFieldCleared {
                field: RequiredPatchField::Description
            })
        );
    }

    #[test]
    fn bead_patch_validation_allows_optional_field_clear() {
        let patch = BeadPatch {
            design: Patch::Clear,
            acceptance_criteria: Patch::Set("done".to_string()),
            ..BeadPatch::default()
        };

        assert_eq!(patch.validate_for_update(), Ok(()));
    }

    #[test]
    fn bead_patch_normalization_trims_required_fields() {
        let mut patch = BeadPatch {
            title: Patch::Set("  title  ".to_string()),
            description: Patch::Set("\tdesc\t".to_string()),
            ..BeadPatch::default()
        };

        patch
            .normalize_for_update()
            .expect("normalization succeeds");

        assert_eq!(patch.title, Patch::Set("title".to_string()));
        assert_eq!(patch.description, Patch::Set("desc".to_string()));
    }

    #[test]
    fn bead_patch_normalization_rejects_required_field_clear() {
        let mut patch = BeadPatch::default();
        patch.title = Patch::Clear;

        assert_eq!(
            patch.normalize_for_update(),
            Err(BeadPatchValidationError::RequiredFieldCleared {
                field: RequiredPatchField::Title
            })
        );
    }

    #[test]
    fn bead_patch_normalization_rejects_whitespace_only_required_field() {
        let mut patch = BeadPatch::default();
        patch.title = Patch::Set(" \t ".to_string());

        assert_eq!(
            patch.normalize_for_update(),
            Err(BeadPatchValidationError::RequiredFieldEmpty {
                field: RequiredPatchField::Title
            })
        );
    }
}
