use crate::core::{
    BeadId, BeadSlug, ClientRequestId, NamespaceId, ValidatedBeadId, ValidatedNamespaceId,
};
use crate::{Error, Result};

pub(crate) fn validation_error(field: impl Into<String>, reason: impl Into<String>) -> Error {
    Error::Op(crate::OpError::ValidationFailed {
        field: field.into(),
        reason: reason.into(),
    })
}

pub(crate) fn normalize_bead_id(id: &str) -> Result<BeadId> {
    normalize_bead_id_for("id", id)
}

pub(crate) fn normalize_bead_id_for(field: &str, id: &str) -> Result<BeadId> {
    ValidatedBeadId::parse(id).map(Into::into).map_err(|e| {
        Error::Op(crate::OpError::ValidationFailed {
            field: field.into(),
            reason: e.to_string(),
        })
    })
}

pub(crate) fn normalize_bead_ids(ids: Vec<String>) -> Result<Vec<BeadId>> {
    ids.into_iter().map(|id| normalize_bead_id(&id)).collect()
}

pub(crate) fn normalize_bead_slug_for(field: &str, slug: &str) -> Result<BeadSlug> {
    BeadSlug::parse(slug).map_err(|e| {
        Error::Op(crate::OpError::ValidationFailed {
            field: field.into(),
            reason: e.to_string(),
        })
    })
}

pub(crate) fn normalize_optional_namespace(raw: Option<&str>) -> Result<Option<NamespaceId>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    ValidatedNamespaceId::parse(raw)
        .map(Into::into)
        .map(Some)
        .map_err(|e| {
            Error::Op(crate::OpError::ValidationFailed {
                field: "namespace".into(),
                reason: e.to_string(),
            })
        })
}

pub(crate) fn normalize_optional_client_request_id(
    raw: Option<&str>,
) -> Result<Option<ClientRequestId>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::Op(crate::OpError::ValidationFailed {
            field: "client_request_id".into(),
            reason: "client_request_id cannot be empty".into(),
        }));
    }
    ClientRequestId::parse_str(trimmed).map(Some).map_err(|e| {
        Error::Op(crate::OpError::ValidationFailed {
            field: "client_request_id".into(),
            reason: e.to_string(),
        })
    })
}

pub(crate) fn normalize_dep_specs(specs: Vec<String>) -> Result<Vec<String>> {
    let parsed = crate::core::DepSpec::parse_list(&specs).map_err(|e| {
        Error::Op(crate::OpError::ValidationFailed {
            field: "deps".into(),
            reason: e.to_string(),
        })
    })?;

    Ok(parsed.iter().map(|spec| spec.to_spec_string()).collect())
}
