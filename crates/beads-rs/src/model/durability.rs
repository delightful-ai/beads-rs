//! Durability helpers for models.

use std::num::NonZeroU32;

use crate::core::{DurabilityClass, DurabilityReceipt, NamespaceId, ReplicaId, Seq1};
use beads_daemon_core::durability::{DurabilityCoordinator, DurabilityError, ReplicatedPoll};

pub type OpError = DurabilityError;

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
    acked_by: Vec<ReplicaId>,
) -> DurabilityReceipt {
    DurabilityCoordinator::pending_receipt(receipt, requested, acked_by)
}

pub fn achieved_receipt(
    receipt: DurabilityReceipt,
    requested: DurabilityClass,
    k: NonZeroU32,
    acked_by: Vec<ReplicaId>,
) -> DurabilityReceipt {
    DurabilityCoordinator::achieved_receipt(receipt, requested, k, acked_by)
}
