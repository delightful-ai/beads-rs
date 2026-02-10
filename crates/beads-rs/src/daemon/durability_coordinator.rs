//! Durability coordination for replication ACKs.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beads_daemon_core::durability::DurabilityCoordinator as CoreCoordinator;
pub use beads_daemon_core::durability::ReplicatedPoll;

use crate::core::{
    DurabilityClass, DurabilityReceipt, NamespaceId, NamespacePolicy, ReplicaId, ReplicaRoster,
    Seq1,
};
use crate::daemon::ops::OpError;
use crate::daemon::repl::PeerAckTable;

#[derive(Clone, Debug)]
pub struct DurabilityCoordinator(CoreCoordinator);

impl DurabilityCoordinator {
    pub fn new(
        local_replica_id: ReplicaId,
        policies: std::collections::BTreeMap<NamespaceId, NamespacePolicy>,
        roster: Option<ReplicaRoster>,
        peer_acks: Arc<Mutex<PeerAckTable>>,
    ) -> Self {
        Self(CoreCoordinator::new(
            local_replica_id,
            policies,
            roster,
            peer_acks,
        ))
    }

    pub fn ensure_available(
        &self,
        namespace: &NamespaceId,
        requested: DurabilityClass,
    ) -> Result<(), OpError> {
        self.0
            .ensure_available(namespace, requested)
            .map_err(Into::into)
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
        self.0
            .await_durability(namespace, origin, seq, requested, receipt, wait_timeout)
            .map_err(Into::into)
    }

    pub(crate) fn poll_replicated(
        &self,
        namespace: &NamespaceId,
        origin: ReplicaId,
        seq: Seq1,
        k: NonZeroU32,
    ) -> Result<ReplicatedPoll, OpError> {
        self.0
            .poll_replicated(namespace, origin, seq, k)
            .map_err(Into::into)
    }

    pub(crate) fn pending_receipt(
        receipt: DurabilityReceipt,
        requested: DurabilityClass,
        acked_by: Vec<ReplicaId>,
    ) -> DurabilityReceipt {
        CoreCoordinator::pending_receipt(receipt, requested, acked_by)
    }

    pub(crate) fn achieved_receipt(
        receipt: DurabilityReceipt,
        requested: DurabilityClass,
        k: NonZeroU32,
        acked_by: Vec<ReplicaId>,
    ) -> DurabilityReceipt {
        CoreCoordinator::achieved_receipt(receipt, requested, k, acked_by)
    }

    pub(crate) fn pending_replica_ids(
        eligible: &BTreeSet<ReplicaId>,
        acked_by: &[ReplicaId],
    ) -> Vec<ReplicaId> {
        CoreCoordinator::pending_replica_ids(eligible, acked_by)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        Durable, HeadStatus, NamespaceId, ReplicaDurabilityRole, Seq0, StoreEpoch, StoreId,
        StoreIdentity, Watermark,
    };
    use beads_daemon_core::repl::proto::WatermarkState;
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
                role: ReplicaDurabilityRole::anchor(true),
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer_a,
                name: "peer-a".to_string(),
                role: ReplicaDurabilityRole::peer(true),
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer_b,
                name: "peer-b".to_string(),
                role: ReplicaDurabilityRole::peer(true),
                allowed_namespaces: None,
                expire_after_ms: None,
            },
        ]);

        let peer_acks = Arc::new(Mutex::new(PeerAckTable::new()));
        let coordinator =
            DurabilityCoordinator::new(local, policy_peers(), Some(roster), peer_acks.clone());

        let mut durable: WatermarkState<Durable> = std::collections::BTreeMap::new();
        durable.entry(namespace.clone()).or_default().insert(
            local,
            Watermark::new(Seq0::new(2), HeadStatus::Known([2u8; 32])).unwrap(),
        );
        peer_acks
            .lock()
            .unwrap()
            .update_peer(peer_a, &durable, None, 10)
            .unwrap();
        peer_acks
            .lock()
            .unwrap()
            .update_peer(peer_b, &durable, None, 12)
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

        assert_eq!(
            updated.outcome().achieved(),
            Some(DurabilityClass::ReplicatedFsync {
                k: std::num::NonZeroU32::new(2).unwrap()
            })
        );
        let proof = updated
            .durability_proof()
            .replicated
            .as_ref()
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
                role: ReplicaDurabilityRole::anchor(true),
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer,
                name: "peer".to_string(),
                role: ReplicaDurabilityRole::peer(true),
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
                role: ReplicaDurabilityRole::anchor(true),
                allowed_namespaces: None,
                expire_after_ms: None,
            },
            ReplicaEntry {
                replica_id: peer,
                name: "peer".to_string(),
                role: ReplicaDurabilityRole::peer(true),
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
            OpError::DurabilityTimeout { receipt, .. } => {
                assert!(receipt.outcome().is_pending());
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
