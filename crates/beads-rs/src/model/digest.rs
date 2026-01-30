//! Digest helpers for model state summaries.

use crate::core::{CanonJsonError, CanonicalState, StateCanonicalJsonSha256};

pub fn canonical_state_canon_json_sha256(
    state: &CanonicalState,
) -> Result<StateCanonicalJsonSha256, CanonJsonError> {
    StateCanonicalJsonSha256::from_canonical_state(state)
}
