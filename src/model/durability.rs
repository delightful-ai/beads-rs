//! Durability helpers for models.

use std::num::NonZeroU32;

use crate::core::{NamespaceId, ReplicaId, Seq1};
use crate::daemon::durability_coordinator::{DurabilityCoordinator, ReplicatedPoll};
use crate::daemon::ops::OpError;

pub fn poll_replicated(
    coordinator: &DurabilityCoordinator,
    namespace: &NamespaceId,
    origin: ReplicaId,
    seq: Seq1,
    k: NonZeroU32,
) -> Result<ReplicatedPoll, OpError> {
    coordinator.poll_replicated(namespace, origin, seq, k)
}
