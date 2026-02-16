use beads_core::{
    BeadId, BeadSlug, ClientRequestId, DepSpec, NamespaceId, ValidatedBeadId, ValidatedNamespaceId,
};

pub type Result<T> = std::result::Result<T, ValidationError>;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ValidationError {
    #[error("validation failed for field {field}: {reason}")]
    Field { field: String, reason: String },
}

pub fn validation_error(field: impl Into<String>, reason: impl Into<String>) -> ValidationError {
    ValidationError::Field {
        field: field.into(),
        reason: reason.into(),
    }
}

pub fn normalize_bead_id(id: &str) -> Result<BeadId> {
    normalize_bead_id_for("id", id)
}

pub fn normalize_bead_id_for(field: &str, id: &str) -> Result<BeadId> {
    ValidatedBeadId::parse(id)
        .map(Into::into)
        .map_err(|err| validation_error(field, err.to_string()))
}

pub fn normalize_bead_ids(ids: Vec<String>) -> Result<Vec<BeadId>> {
    ids.into_iter().map(|id| normalize_bead_id(&id)).collect()
}

pub fn normalize_bead_slug_for(field: &str, slug: &str) -> Result<BeadSlug> {
    BeadSlug::parse(slug).map_err(|err| validation_error(field, err.to_string()))
}

pub fn normalize_optional_namespace(raw: Option<&str>) -> Result<Option<NamespaceId>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    ValidatedNamespaceId::parse(raw)
        .map(Into::into)
        .map(Some)
        .map_err(|err| validation_error("namespace", err.to_string()))
}

pub fn normalize_optional_client_request_id(raw: Option<&str>) -> Result<Option<ClientRequestId>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(validation_error(
            "client_request_id",
            "client_request_id cannot be empty",
        ));
    }
    ClientRequestId::parse_str(trimmed)
        .map(Some)
        .map_err(|err| validation_error("client_request_id", err.to_string()))
}

pub fn normalize_dep_specs(specs: Vec<String>) -> Result<Vec<String>> {
    let parsed =
        DepSpec::parse_list(&specs).map_err(|err| validation_error("deps", err.to_string()))?;

    Ok(parsed.iter().map(|spec| spec.to_spec_string()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_bead_id_valid() {
        let id = normalize_bead_id("beads-rs-123").unwrap();
        assert_eq!(id.as_str(), "beads-rs-123");
    }

    #[test]
    fn normalize_bead_id_invalid() {
        let err = normalize_bead_id("invalid").unwrap_err();
        match err {
            ValidationError::Field { field, .. } => {
                assert_eq!(field, "id");
            }
        }
    }

    #[test]
    fn normalize_bead_id_for_valid() {
        let id = normalize_bead_id_for("custom_field", "beads-rs-123").unwrap();
        assert_eq!(id.as_str(), "beads-rs-123");
    }

    #[test]
    fn normalize_bead_id_for_invalid() {
        let err = normalize_bead_id_for("custom_field", "invalid").unwrap_err();
        match err {
            ValidationError::Field { field, reason } => {
                assert_eq!(field, "custom_field");
                assert!(!reason.is_empty());
            }
        }
    }

    #[test]
    fn normalize_bead_ids_valid() {
        let input = vec!["beads-rs-1".to_string(), "beads-rs-2".to_string()];
        let ids = normalize_bead_ids(input).unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0].as_str(), "beads-rs-1");
        assert_eq!(ids[1].as_str(), "beads-rs-2");
    }

    #[test]
    fn normalize_bead_ids_invalid() {
        let input = vec!["beads-rs-1".to_string(), "invalid".to_string()];
        let err = normalize_bead_ids(input).unwrap_err();
        match err {
            ValidationError::Field { field, .. } => {
                assert_eq!(field, "id");
            }
        }
    }

    #[test]
    fn normalize_bead_slug_for_valid() {
        let slug = normalize_bead_slug_for("slug", "beads-rs").unwrap();
        assert_eq!(slug.as_str(), "beads-rs");
    }

    #[test]
    fn normalize_bead_slug_for_invalid() {
        let err = normalize_bead_slug_for("slug", "-invalid").unwrap_err();
        match err {
            ValidationError::Field { field, .. } => {
                assert_eq!(field, "slug");
            }
        }
    }

    #[test]
    fn normalize_optional_namespace_none() {
        assert!(normalize_optional_namespace(None).unwrap().is_none());
    }

    #[test]
    fn normalize_optional_namespace_valid() {
        let ns = normalize_optional_namespace(Some("core")).unwrap();
        assert_eq!(ns.unwrap().as_str(), "core");
    }

    #[test]
    fn normalize_optional_namespace_invalid() {
        let err = normalize_optional_namespace(Some("1invalid")).unwrap_err();
        match err {
            ValidationError::Field { field, .. } => {
                assert_eq!(field, "namespace");
            }
        }
    }

    #[test]
    fn normalize_optional_client_request_id_none() {
        assert!(normalize_optional_client_request_id(None).unwrap().is_none());
    }

    #[test]
    fn normalize_optional_client_request_id_valid() {
        let uuid = uuid::Uuid::new_v4().to_string();
        let id = normalize_optional_client_request_id(Some(&uuid)).unwrap();
        assert_eq!(id.unwrap().as_uuid().to_string(), uuid);
    }

    #[test]
    fn normalize_optional_client_request_id_empty() {
        let err = normalize_optional_client_request_id(Some("   ")).unwrap_err();
        match err {
            ValidationError::Field { field, reason } => {
                assert_eq!(field, "client_request_id");
                assert!(reason.contains("empty"));
            }
        }
    }

    #[test]
    fn normalize_optional_client_request_id_invalid() {
        let err = normalize_optional_client_request_id(Some("not-uuid")).unwrap_err();
        match err {
            ValidationError::Field { field, .. } => {
                assert_eq!(field, "client_request_id");
            }
        }
    }

    #[test]
    fn normalize_dep_specs_valid() {
        let specs = vec![
            "beads-rs-1".to_string(),
            "related:beads-rs-2".to_string(),
            "blocks:beads-rs-3".to_string(),
        ];
        let normalized = normalize_dep_specs(specs).unwrap();
        // beads-rs-1 (blocks) < beads-rs-3 (blocks) < related:beads-rs-2
        assert_eq!(normalized.len(), 3);
        assert_eq!(normalized[0], "beads-rs-1");
        assert_eq!(normalized[1], "beads-rs-3");
        assert_eq!(normalized[2], "related:beads-rs-2");
    }

    #[test]
    fn normalize_dep_specs_invalid_kind() {
        let specs = vec!["unknown:beads-rs-1".to_string()];
        let err = normalize_dep_specs(specs).unwrap_err();
        match err {
            ValidationError::Field { field, .. } => {
                assert_eq!(field, "deps");
            }
        }
    }

    #[test]
    fn normalize_dep_specs_invalid_id() {
        let specs = vec!["blocks:invalid".to_string()];
        let err = normalize_dep_specs(specs).unwrap_err();
        match err {
            ValidationError::Field { field, .. } => {
                assert_eq!(field, "deps");
            }
        }
    }

    #[test]
    fn normalize_dep_specs_parent_not_allowed() {
        let specs = vec!["parent:beads-rs-1".to_string()];
        let err = normalize_dep_specs(specs).unwrap_err();
        match err {
            ValidationError::Field { field, .. } => {
                assert_eq!(field, "deps");
            }
        }
    }
}
