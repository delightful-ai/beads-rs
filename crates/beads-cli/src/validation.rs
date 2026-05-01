use beads_core::{
    BeadId, BeadRef, BeadSlug, ClientRequestId, DepKind, NamespaceId, ValidatedBeadId,
    ValidatedDepKind, ValidatedNamespaceId,
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

pub fn normalize_bead_ref_for(
    field: &str,
    raw: &str,
    default_namespace: &NamespaceId,
) -> Result<BeadRef> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(validation_error(field, "bead ref cannot be empty"));
    }
    let (namespace, id) = match raw.split_once('/') {
        Some((namespace_raw, id_raw)) => {
            let namespace = ValidatedNamespaceId::parse(namespace_raw.trim())
                .map_err(|err| validation_error(format!("{field}.namespace"), err.to_string()))?;
            let id = ValidatedBeadId::parse(id_raw.trim())
                .map_err(|err| validation_error(format!("{field}.id"), err.to_string()))?;
            (namespace.into(), id.into())
        }
        None => {
            let id = ValidatedBeadId::parse(raw)
                .map_err(|err| validation_error(field, err.to_string()))?;
            (default_namespace.clone(), id.into())
        }
    };
    Ok(BeadRef::new(namespace, id))
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
    normalize_dep_specs_for(specs, &NamespaceId::core())
}

pub fn normalize_dep_specs_for(
    specs: Vec<String>,
    default_namespace: &NamespaceId,
) -> Result<Vec<String>> {
    let mut parsed = Vec::new();
    for spec in specs {
        for part in spec.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            parsed.push(normalize_dep_spec_for(part, default_namespace)?);
        }
    }
    parsed.sort();
    parsed.dedup();
    Ok(parsed)
}

fn normalize_dep_spec_for(raw: &str, default_namespace: &NamespaceId) -> Result<String> {
    let (kind, ref_raw) = if let Some((kind_raw, id_raw)) = raw.split_once(':') {
        let kind = ValidatedDepKind::parse(kind_raw.trim())
            .map_err(|err| validation_error("deps.kind", err.to_string()))?
            .into_inner();
        (kind, id_raw.trim())
    } else {
        (DepKind::Blocks, raw.trim())
    };
    if kind == DepKind::Parent {
        return Err(validation_error(
            "deps",
            "parent edges must use --parent or bd parent",
        ));
    }
    let bead_ref = normalize_bead_ref_for("deps", ref_raw, default_namespace)?;
    let formatted_ref = if bead_ref.namespace() == default_namespace {
        bead_ref.id().as_str().to_string()
    } else {
        bead_ref.to_string()
    };
    if kind == DepKind::Blocks {
        Ok(formatted_ref)
    } else {
        Ok(format!("{}:{formatted_ref}", kind.as_str()))
    }
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
    fn normalize_bead_ref_defaults_namespace() {
        let sessions = NamespaceId::parse("sessions").unwrap();
        let bead_ref = normalize_bead_ref_for("id", "beads-rs-1", &sessions).unwrap();
        assert_eq!(bead_ref.namespace(), &sessions);
        assert_eq!(bead_ref.id().as_str(), "beads-rs-1");
    }

    #[test]
    fn normalize_bead_ref_accepts_namespace_prefix() {
        let sessions = NamespaceId::parse("sessions").unwrap();
        let bead_ref = normalize_bead_ref_for("id", "core/beads-rs-1", &sessions).unwrap();
        assert_eq!(bead_ref.namespace(), &NamespaceId::core());
        assert_eq!(bead_ref.id().as_str(), "beads-rs-1");
    }

    #[test]
    fn normalize_bead_ref_distinguishes_bad_namespace_and_bad_id() {
        let default_namespace = NamespaceId::core();
        let err = normalize_bead_ref_for("id", "1bad/beads-rs-1", &default_namespace).unwrap_err();
        match err {
            ValidationError::Field { field, .. } => assert_eq!(field, "id.namespace"),
        }

        let err = normalize_bead_ref_for("id", "core/not", &default_namespace).unwrap_err();
        match err {
            ValidationError::Field { field, .. } => assert_eq!(field, "id.id"),
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
        assert!(
            normalize_optional_client_request_id(None)
                .unwrap()
                .is_none()
        );
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
                assert_eq!(field, "deps.kind");
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
