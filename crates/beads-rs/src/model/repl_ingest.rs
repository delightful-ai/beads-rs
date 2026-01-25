//! Thin wrappers for replication ingest helpers.

use crate::core::{
    EventFrameError, EventFrameV1, EventShaLookup, Limits, Sha256, StoreIdentity, VerifiedEventAny,
    verify_event_frame,
};

pub fn verify_frame(
    frame: &EventFrameV1,
    limits: &Limits,
    expected_store: StoreIdentity,
    expected_prev_head: Option<Sha256>,
    lookup: &dyn EventShaLookup,
) -> Result<VerifiedEventAny, EventFrameError> {
    verify_event_frame(frame, limits, expected_store, expected_prev_head, lookup)
}
