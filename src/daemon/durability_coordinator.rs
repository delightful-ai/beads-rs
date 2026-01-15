//! Durability coordination for replication ACKs.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::core::{
    DurabilityClass, DurabilityOutcome, DurabilityReceipt, NamespaceId, NamespacePolicy, ReplicaId,
    ReplicaRole, ReplicaRoster, ReplicateMode, ReplicatedProof, Seq0, Seq1,
};
use crate::daemon::ops::OpError;
use crate::daemon::repl::{PeerAckTable, QuorumOutcome};

#[derive(Clone, Debug)]
pub struct DurabilityCoordinator {
    local_replica_id: ReplicaId,
    policies: std::collections::BTreeMap<NamespaceId, NamespacePolicy>,
    roster: Option<ReplicaRoster>,
    peer_acks: Arc<Mutex<PeerAckTable>>,
}

#[derive(Debug)]
pub(crate) enum ReplicatedPoll {
    Satisfied {
        acked_by: Vec<ReplicaId>,
    },
    Pending {
        acked_by: Vec<ReplicaId>,
        eligible: BTreeSet<ReplicaId>,
    },
}

impl DurabilityCoordinator {
    pub fn new(
        local_replica_id: ReplicaId,
        policies: std::collections::BTreeMap<NamespaceId, NamespacePolicy>,
        roster: Option<ReplicaRoster>,
        peer_acks: Arc<Mutex<PeerAckTable>>,
    ) -> Self {
        Self {
            local_replica_id,
            policies,
            roster,
            peer_acks,
        }
    }

    pub fn ensure_available(
        &self,
        namespace: &NamespaceId,
        requested: DurabilityClass,
    ) -> Result<(), OpError> {
        let DurabilityClass::ReplicatedFsync { k } = requested else {
            return Ok(());
        };

        let eligible = self.eligible_replicas(namespace);
        {
            let mut table = self.peer_acks.lock().expect("peer ack lock poisoned");
            table.set_eligibility(namespace.clone(), eligible.clone());
        }

        if eligible.len() < k.get() as usize {
            return Err(OpError::DurabilityUnavailable {
                requested,
                eligible_total: eligible.len() as u32,
                eligible_replica_ids: Some(eligible.into_iter().collect()),
            });
        }

        Ok(())
    }

    pub fn await_durability(
        &self,
        namespace: &NamespaceId,
        origin: ReplicaId,
        seq: Seq1,
        requested: DurabilityClass,
        receipt: DurabilityReceipt,
        wait_timeout: Duration,
    ) -> Result<DurabilityReceipt, OpError> {
        let DurabilityClass::ReplicatedFsync { k } = requested else {
            return Ok(receipt);
        };

        let start = Instant::now();
        let mut backoff = Duration::from_millis(5);

        loop {
            match self.poll_replicated(namespace, origin, seq, k) {
                Ok(ReplicatedPoll::Satisfied { acked_by }) => {
                    return Ok(Self::achieved_receipt(receipt, requested, k, acked_by));
                }
                Ok(ReplicatedPoll::Pending { acked_by, eligible }) => {
                    let elapsed = start.elapsed();
                    if wait_timeout.is_zero() || elapsed >= wait_timeout {
                        let pending = Self::pending_replica_ids(&eligible, &acked_by);
                        let pending_receipt = Self::pending_receipt(receipt, requested);
                        return Err(OpError::DurabilityTimeout {
                            requested,
                            waited_ms: elapsed.as_millis() as u64,
                            pending_replica_ids: Some(pending),
                            receipt: Box::new(pending_receipt),
                        });
                    }
                }
                Err(err) => return Err(err),
            }

            let elapsed = start.elapsed();
            if elapsed >= wait_timeout {
                continue;
            }
            let remaining = wait_timeout - elapsed;
            let sleep_for = std::cmp::min(backoff, remaining);
            std::thread::sleep(sleep_for);
            backoff = std::cmp::min(backoff.saturating_mul(2), Duration::from_millis(50));
        }
    }

    pub(crate) fn poll_replicated(
        &self,
        namespace: &NamespaceId,
        origin: ReplicaId,
        seq: Seq1,
        k: NonZeroU32,
    ) -> Result<ReplicatedPoll, OpError> {
        let eligible = self.eligible_replicas(namespace);
        {
            let mut table = self.peer_acks.lock().expect("peer ack lock poisoned");
            table.set_eligibility(namespace.clone(), eligible.clone());
        }

        let outcome = {
            let table = self.peer_acks.lock().expect("peer ack lock poisoned");
            table.satisfied_k(namespace, &origin, Seq0::new(seq.get()), k.get())
        };

        match outcome {
            QuorumOutcome::Satisfied { acked_by, .. } => Ok(ReplicatedPoll::Satisfied { acked_by }),
            QuorumOutcome::Pending { acked_by, .. } => {
                Ok(ReplicatedPoll::Pending { acked_by, eligible })
            }
            QuorumOutcome::InsufficientEligible { eligible_total, .. } => {
                Err(OpError::DurabilityUnavailable {
                    requested: DurabilityClass::ReplicatedFsync { k },
                    eligible_total: eligible_total as u32,
                    eligible_replica_ids: Some(eligible.iter().copied().collect()),
                })
            }
        }
    }

    fn eligible_replicas(&self, namespace: &NamespaceId) -> BTreeSet<ReplicaId> {
        let Some(roster) = &self.roster else {
            return BTreeSet::new();
        };
        let Some(policy) = self.policies.get(namespace) else {
            return BTreeSet::new();
        };

        let mut eligible = BTreeSet::new();
        for entry in &roster.replicas {
            if entry.replica_id == self.local_replica_id {
                continue;
            }
            if !entry.durability_eligible {
                continue;
            }
            if !role_allows_policy(entry.role, policy.replicate_mode) {
                continue;
            }
            if let Some(allowed) = &entry.allowed_namespaces
                && !allowed.contains(namespace)
            {
                continue;
            }
            eligible.insert(entry.replica_id);
        }

        eligible
    }

    pub(crate) fn pending_receipt(
        mut receipt: DurabilityReceipt,
        requested: DurabilityClass,
    ) -> DurabilityReceipt {
        receipt.outcome = DurabilityOutcome::Pending { requested };
        receipt
    }

    pub(crate) fn achieved_receipt(
        mut receipt: DurabilityReceipt,
        requested: DurabilityClass,
        k: NonZeroU32,
        acked_by: Vec<ReplicaId>,
    ) -> DurabilityReceipt {
        receipt.durability_proof.replicated = Some(ReplicatedProof { k, acked_by });
        receipt.outcome = DurabilityOutcome::Achieved {
            requested,
            achieved: DurabilityClass::ReplicatedFsync { k },
        };
        receipt
    }

    pub(crate) fn pending_replica_ids(
        eligible: &BTreeSet<ReplicaId>,
        acked_by: &[ReplicaId],
    ) -> Vec<ReplicaId> {
        let acked: BTreeSet<ReplicaId> = acked_by.iter().copied().collect();
        eligible
            .iter()
            .filter(|replica_id| !acked.contains(replica_id))
            .copied()
            .collect()
    }
}

fn role_allows_policy(role: ReplicaRole, mode: ReplicateMode) -> bool {
    match mode {
        ReplicateMode::None => false,
        ReplicateMode::Anchors => role == ReplicaRole::Anchor,
        ReplicateMode::Peers => matches!(role, ReplicaRole::Anchor | ReplicaRole::Peer),
        ReplicateMode::P2p => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{NamespaceId, StoreEpoch, StoreId, StoreIdentity};
    use crate::daemon::repl::proto::WatermarkMap;
    use uuid::Uuid;

    fn replica(seed: u128) -> ReplicaId {
        ReplicaId::new(Uuid::from_u128(seed))
    }

    fn roster(entries: Vec<ReplicaEntry>) -> ReplicaRoster {
        ReplicaRoster { replicas: entries }
    }

    fn policy_peers() -> std::collections::BTreeMap<NamespaceId, NamespacePolicy> {
        let mut policies = std::collections::BTreeMap::new();
        policies.insert(NamespaceId::core(), NamespacePolicy::core_default());
        policies
    }

    fn receipt_for(store: StoreIdentity) -> DurabilityReceipt {
        DurabilityReceipt::local_fsync_defaults(
            store,
            crate::core::TxnId::new(Uuid::from_u128(42)),
            Vec::new(),
            1,
        )
    }

    use crate::core::replica_roster::ReplicaEntry;

    #[test]
    fn replicated_fsync_succeeds_after_k_acks() {
        let namespace = NamespaceId::core();
        let local = replica(1);
        let peer_a = replica(2);
        let peer_b = replica(3);
        let roster = roster(vec![
            ReplicaEntry {
                replica_id: local,
                name: "local".to_string(),
                role: ReplicaRole::Anchor,
                durability_eligible: true,
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer_a,
                name: "peer-a".to_string(),
                role: ReplicaRole::Peer,
                durability_eligible: true,
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer_b,
                name: "peer-b".to_string(),
                role: ReplicaRole::Peer,
                durability_eligible: true,
                allowed_namespaces: None,
                expire_after_ms: None,
            },
        ]);

        let peer_acks = Arc::new(Mutex::new(PeerAckTable::new()));
        let coordinator =
            DurabilityCoordinator::new(local, policy_peers(), Some(roster), peer_acks.clone());

        let mut durable = WatermarkMap::new();
        durable
            .entry(namespace.clone())
            .or_default()
            .insert(local, 2);
        peer_acks
            .lock()
            .unwrap()
            .update_peer(peer_a, &durable, None, None, None, 10)
            .unwrap();
        peer_acks
            .lock()
            .unwrap()
            .update_peer(peer_b, &durable, None, None, None, 12)
            .unwrap();

        let store = StoreIdentity::new(StoreId::new(Uuid::from_u128(100)), StoreEpoch::ZERO);
        let receipt = receipt_for(store);
        let updated = coordinator
            .await_durability(
                &namespace,
                local,
                Seq1::from_u64(2).unwrap(),
                DurabilityClass::ReplicatedFsync {
                    k: std::num::NonZeroU32::new(2).unwrap(),
                },
                receipt,
                Duration::from_millis(0),
            )
            .unwrap();

        match updated.outcome {
            DurabilityOutcome::Achieved { achieved, .. } => {
                assert_eq!(
                    achieved,
                    DurabilityClass::ReplicatedFsync {
                        k: std::num::NonZeroU32::new(2).unwrap()
                    }
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
        let proof = updated
            .durability_proof
            .replicated
            .expect("replicated proof");
        assert_eq!(proof.k.get(), 2);
        assert_eq!(proof.acked_by.len(), 2);
    }

    #[test]
    fn replicated_fsync_unavailable_with_insufficient_eligible() {
        let namespace = NamespaceId::core();
        let local = replica(1);
        let peer = replica(2);
        let roster = roster(vec![
            ReplicaEntry {
                replica_id: local,
                name: "local".to_string(),
                role: ReplicaRole::Anchor,
                durability_eligible: true,
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer,
                name: "peer".to_string(),
                role: ReplicaRole::Peer,
                durability_eligible: true,
                allowed_namespaces: None,
                expire_after_ms: None,
            },
        ]);

        let coordinator = DurabilityCoordinator::new(
            local,
            policy_peers(),
            Some(roster),
            Arc::new(Mutex::new(PeerAckTable::new())),
        );

        let err = coordinator
            .ensure_available(
                &namespace,
                DurabilityClass::ReplicatedFsync {
                    k: std::num::NonZeroU32::new(2).unwrap(),
                },
            )
            .unwrap_err();

        assert!(matches!(err, OpError::DurabilityUnavailable { .. }));
    }

    #[test]
    fn timeout_returns_pending_receipt() {
        let namespace = NamespaceId::core();
        let local = replica(1);
        let peer = replica(2);
        let roster = roster(vec![
            ReplicaEntry {
                replica_id: local,
                name: "local".to_string(),
                role: ReplicaRole::Anchor,
                durability_eligible: true,
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer,
                name: "peer".to_string(),
                role: ReplicaRole::Peer,
                durability_eligible: true,
                allowed_namespaces: None,
                expire_after_ms: None,
            },
        ]);

        let coordinator = DurabilityCoordinator::new(
            local,
            policy_peers(),
            Some(roster),
            Arc::new(Mutex::new(PeerAckTable::new())),
        );

        let store = StoreIdentity::new(StoreId::new(Uuid::from_u128(200)), StoreEpoch::ZERO);
        let receipt = receipt_for(store);
        let err = coordinator
            .await_durability(
                &namespace,
                local,
                Seq1::from_u64(1).unwrap(),
                DurabilityClass::ReplicatedFsync {
                    k: std::num::NonZeroU32::new(1).unwrap(),
                },
                receipt,
                Duration::from_millis(0),
            )
            .unwrap_err();

        match err {
            OpError::DurabilityTimeout { receipt, .. } => match receipt.outcome {
                DurabilityOutcome::Pending { .. } => {}
                other => panic!("unexpected outcome: {other:?}"),
            },
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
