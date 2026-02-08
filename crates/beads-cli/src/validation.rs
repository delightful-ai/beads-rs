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
