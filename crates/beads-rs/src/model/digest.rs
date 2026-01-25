//! Digest helpers for model state summaries.

use crate::core::{CanonJsonError, CanonicalState, Sha256, sha256_bytes, to_canon_json_bytes};

pub fn canonical_state_sha(state: &CanonicalState) -> Result<Sha256, CanonJsonError> {
    let bytes = to_canon_json_bytes(state)?;
    Ok(sha256_bytes(&bytes))
}
