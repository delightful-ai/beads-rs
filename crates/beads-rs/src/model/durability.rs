//! Durability helpers for models.

use std::num::NonZeroU32;

use crate::core::{DurabilityClass, DurabilityReceipt, NamespaceId, ReplicaId, Seq1};
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

pub fn pending_receipt(
    receipt: DurabilityReceipt,
    requested: DurabilityClass,
) -> DurabilityReceipt {
    DurabilityCoordinator::pending_receipt(receipt, requested)
}

pub fn achieved_receipt(
    receipt: DurabilityReceipt,
    requested: DurabilityClass,
    k: NonZeroU32,
    acked_by: Vec<ReplicaId>,
) -> DurabilityReceipt {
    DurabilityCoordinator::achieved_receipt(receipt, requested, k, acked_by)
}
