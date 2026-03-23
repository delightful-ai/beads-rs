//! Peer ACK tracking for durability coordination.

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::core::{
    Applied, Durable, HeadStatus, NamespaceId, ReplicaId, Seq0, Watermark, WatermarkError,
    Watermarks,
};

pub type WatermarkState<K> = BTreeMap<NamespaceId, BTreeMap<ReplicaId, Watermark<K>>>;

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct PeerAckTable {
    peers: BTreeMap<ReplicaId, PeerAckState>,
    eligible: BTreeMap<NamespaceId, BTreeSet<ReplicaId>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PeerAckQuarantineReason {
    DivergedHead {
        kind: WatermarkKind,
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: Seq0,
        expected: [u8; 32],
        got: [u8; 32],
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum PeerAckStatus {
    #[default]
    Healthy,
    Quarantined {
        reason: PeerAckQuarantineReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WatermarkKind {
    Durable,
    Applied,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PeerAckState {
    durable: Watermarks<Durable>,
    applied: Watermarks<Applied>,
    last_ack_at_ms: u64,
    status: PeerAckStatus,
}

impl Default for PeerAckState {
    fn default() -> Self {
        Self {
            durable: Watermarks::new(),
            applied: Watermarks::new(),
            last_ack_at_ms: 0,
            status: PeerAckStatus::Healthy,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PeerAckSnapshot {
    pub peer: ReplicaId,
    pub durable: Watermarks<Durable>,
    pub applied: Watermarks<Applied>,
    pub last_ack_at_ms: u64,
    pub status: PeerAckStatus,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PeerAckError {
    #[error(
        "non-monotonic ack from {peer} for {namespace} {origin}: current {current}, got {attempted}"
    )]
    NonMonotonic {
        peer: ReplicaId,
        namespace: NamespaceId,
        origin: ReplicaId,
        current: Seq0,
        attempted: Seq0,
    },

    #[error(
        "divergent head from {peer} for {namespace} {origin} at {seq}: expected {expected:?}, got {got:?}"
    )]
    Diverged {
        peer: ReplicaId,
        namespace: NamespaceId,
        origin: ReplicaId,
        seq: Seq0,
        expected: [u8; 32],
        got: [u8; 32],
    },

    #[error("invalid watermark from {peer} for {namespace} {origin}: {source}")]
    InvalidWatermark {
        peer: ReplicaId,
        namespace: NamespaceId,
        origin: ReplicaId,
        #[source]
        source: WatermarkError,
    },

    #[error("peer {peer} is quarantined: {reason:?}")]
    PeerQuarantined {
        peer: ReplicaId,
        reason: PeerAckQuarantineReason,
    },
}

pub type PeerAckResult<T> = Result<T, Box<PeerAckError>>;

enum WatermarkUpdateError {
    Peer(Box<PeerAckError>),
    Quarantine(PeerAckQuarantineReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuorumOutcome {
    Satisfied {
        required: u32,
        eligible_total: usize,
        acked_by: Vec<ReplicaId>,
    },
    Pending {
        required: u32,
        eligible_total: usize,
        acked_by: Vec<ReplicaId>,
    },
    InsufficientEligible {
        required: u32,
        eligible_total: usize,
    },
}

impl QuorumOutcome {
    pub fn is_satisfied(&self) -> bool {
        matches!(self, QuorumOutcome::Satisfied { .. })
    }
}

impl PeerAckTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> Vec<PeerAckSnapshot> {
        let mut peers: BTreeSet<ReplicaId> = self.peers.keys().copied().collect();
        for eligible in self.eligible.values() {
            peers.extend(eligible.iter().copied());
        }

        peers
            .into_iter()
            .map(|peer| {
                let state = self.peers.get(&peer).cloned().unwrap_or_default();
                PeerAckSnapshot {
                    peer,
                    durable: state.durable,
                    applied: state.applied,
                    last_ack_at_ms: state.last_ack_at_ms,
                    status: state.status,
                }
            })
            .collect()
    }

    pub fn set_eligibility(&mut self, namespace: NamespaceId, eligible: BTreeSet<ReplicaId>) {
        self.eligible.insert(namespace, eligible);
    }

    pub fn update_peer(
        &mut self,
        peer: ReplicaId,
        durable: &WatermarkState<Durable>,
        applied: Option<&WatermarkState<Applied>>,
        now_ms: u64,
    ) -> PeerAckResult<()> {
        let state = self.peers.entry(peer).or_default();
        if let PeerAckStatus::Quarantined { reason } = &state.status
            && !reason.is_recovered_by(durable, applied)
        {
            return Err(Box::new(PeerAckError::PeerQuarantined {
                peer,
                reason: reason.clone(),
            }));
        }

        let mut next_durable = state.durable.clone();
        if let Err(err) =
            update_watermarks(&mut next_durable, peer, durable, WatermarkKind::Durable)
        {
            return match err {
                WatermarkUpdateError::Peer(err) => Err(err),
                WatermarkUpdateError::Quarantine(reason) => {
                    state.status = PeerAckStatus::Quarantined {
                        reason: reason.clone(),
                    };
                    Err(Box::new(reason.into_peer_error(peer)))
                }
            };
        }
        let mut next_applied = state.applied.clone();
        if let Some(applied) = applied
            && let Err(err) =
                update_watermarks(&mut next_applied, peer, applied, WatermarkKind::Applied)
        {
            return match err {
                WatermarkUpdateError::Peer(err) => Err(err),
                WatermarkUpdateError::Quarantine(reason) => {
                    state.status = PeerAckStatus::Quarantined {
                        reason: reason.clone(),
                    };
                    Err(Box::new(reason.into_peer_error(peer)))
                }
            };
        }

        state.durable = next_durable;
        state.applied = next_applied;
        state.status = PeerAckStatus::Healthy;
        state.last_ack_at_ms = now_ms;
        Ok(())
    }

    pub fn acked_by(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        seq: Seq0,
    ) -> Vec<ReplicaId> {
        let Some(eligible) = self.eligible.get(namespace) else {
            return Vec::new();
        };

        eligible
            .iter()
            .filter_map(|peer| {
                let state = self.peers.get(peer)?;
                if !state.status.is_healthy() {
                    return None;
                }
                let acked_seq = state
                    .durable
                    .get(namespace, origin)
                    .map(|watermark| watermark.seq())
                    .unwrap_or(Seq0::ZERO);
                if acked_seq >= seq { Some(*peer) } else { None }
            })
            .collect()
    }

    pub fn satisfied_k(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        seq: Seq0,
        k: u32,
    ) -> QuorumOutcome {
        let Some(eligible) = self.eligible.get(namespace) else {
            return QuorumOutcome::InsufficientEligible {
                required: k,
                eligible_total: 0,
            };
        };

        self.satisfied_k_with_eligible(namespace, origin, seq, k, eligible)
    }

    pub fn satisfied_k_with_eligible(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        seq: Seq0,
        k: u32,
        eligible: &BTreeSet<ReplicaId>,
    ) -> QuorumOutcome {
        let eligible_total = eligible.len();
        if eligible_total < k as usize {
            return QuorumOutcome::InsufficientEligible {
                required: k,
                eligible_total,
            };
        }

        let mut acked_by = Vec::new();

        for peer in eligible {
            let state = self.peers.get(peer);
            if let Some(state) = state {
                if !state.status.is_healthy() {
                    continue;
                }
                let acked_seq = state
                    .durable
                    .get(namespace, origin)
                    .map(|watermark| watermark.seq())
                    .unwrap_or(Seq0::ZERO);
                if acked_seq >= seq {
                    acked_by.push(*peer);
                }
            }
        }

        if acked_by.len() >= k as usize {
            QuorumOutcome::Satisfied {
                required: k,
                eligible_total,
                acked_by,
            }
        } else {
            QuorumOutcome::Pending {
                required: k,
                eligible_total,
                acked_by,
            }
        }
    }
}

fn update_watermarks<K>(
    watermarks: &mut Watermarks<K>,
    peer: ReplicaId,
    updates: &WatermarkState<K>,
    kind: WatermarkKind,
) -> Result<(), WatermarkUpdateError> {
    for (namespace, origins) in updates {
        for (origin, watermark) in origins {
            let seq0 = watermark.seq();
            let incoming_head = watermark.head();
            let (current_seq, current_head) = watermarks
                .get(namespace, origin)
                .map(|watermark| (watermark.seq(), watermark.head()))
                .unwrap_or((Seq0::ZERO, HeadStatus::Genesis));

            if seq0 < current_seq {
                return Err(WatermarkUpdateError::Peer(Box::new(
                    PeerAckError::NonMonotonic {
                        peer,
                        namespace: namespace.clone(),
                        origin: *origin,
                        current: current_seq,
                        attempted: seq0,
                    },
                )));
            }

            if seq0 == current_seq
                && let (HeadStatus::Known(expected), HeadStatus::Known(got)) =
                    (current_head, incoming_head)
                && expected != got
            {
                return Err(WatermarkUpdateError::Quarantine(
                    PeerAckQuarantineReason::DivergedHead {
                        kind,
                        namespace: namespace.clone(),
                        origin: *origin,
                        seq: seq0,
                        expected,
                        got,
                    },
                ));
            }

            watermarks
                .observe_at_least(namespace, origin, seq0, incoming_head)
                .map_err(|source| PeerAckError::InvalidWatermark {
                    peer,
                    namespace: namespace.clone(),
                    origin: *origin,
                    source,
                })
                .map_err(Box::new)
                .map_err(WatermarkUpdateError::Peer)?;
        }
    }
    Ok(())
}

impl PeerAckQuarantineReason {
    fn is_recovered_by(
        &self,
        durable: &WatermarkState<Durable>,
        applied: Option<&WatermarkState<Applied>>,
    ) -> bool {
        match self {
            Self::DivergedHead {
                kind,
                namespace,
                origin,
                seq,
                expected,
                ..
            } => match kind {
                WatermarkKind::Durable => durable
                    .get(namespace)
                    .and_then(|origins| origins.get(origin))
                    .is_some_and(|watermark| {
                        watermark.seq() > *seq
                            || (watermark.seq() == *seq
                                && watermark.head() == HeadStatus::Known(*expected))
                    }),
                WatermarkKind::Applied => applied
                    .and_then(|state| state.get(namespace))
                    .and_then(|origins| origins.get(origin))
                    .is_some_and(|watermark| {
                        watermark.seq() > *seq
                            || (watermark.seq() == *seq
                                && watermark.head() == HeadStatus::Known(*expected))
                    }),
            },
        }
    }

    fn into_peer_error(self, peer: ReplicaId) -> PeerAckError {
        match self {
            Self::DivergedHead {
                kind: _,
                namespace,
                origin,
                seq,
                expected,
                got,
            } => PeerAckError::Diverged {
                peer,
                namespace,
                origin,
                seq,
                expected,
                got,
            },
        }
    }
}

impl PeerAckStatus {
    fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Watermark;
    use uuid::Uuid;

    fn replica(seed: u128) -> ReplicaId {
        ReplicaId::new(Uuid::from_u128(seed))
    }

    fn namespace() -> NamespaceId {
        NamespaceId::core()
    }

    #[test]
    fn updates_require_monotonic_seq() {
        let peer = replica(1);
        let origin = replica(2);
        let ns = namespace();
        let mut table = PeerAckTable::new();
        let mut eligible = BTreeSet::new();
        eligible.insert(peer);
        table.set_eligibility(ns.clone(), eligible);

        let mut durable: WatermarkState<Durable> = BTreeMap::new();
        durable.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(3), HeadStatus::Known([3u8; 32])).unwrap(),
        );
        table.update_peer(peer, &durable, None, 10).unwrap();

        let mut backwards: WatermarkState<Durable> = BTreeMap::new();
        backwards.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([2u8; 32])).unwrap(),
        );
        let err = table.update_peer(peer, &backwards, None, 12).unwrap_err();
        assert!(matches!(*err, PeerAckError::NonMonotonic { .. }));

        let acked = table.acked_by(&ns, &origin, Seq0::new(3));
        assert_eq!(acked, vec![peer]);
    }

    #[test]
    fn divergent_heads_are_excluded_from_quorum() {
        let peer = replica(3);
        let origin = replica(4);
        let ns = namespace();
        let mut table = PeerAckTable::new();
        let mut eligible = BTreeSet::new();
        eligible.insert(peer);
        table.set_eligibility(ns.clone(), eligible);

        let mut durable: WatermarkState<Durable> = BTreeMap::new();
        durable.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([1u8; 32])).unwrap(),
        );
        table.update_peer(peer, &durable, None, 10).unwrap();

        let mut diverged: WatermarkState<Durable> = BTreeMap::new();
        diverged.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([2u8; 32])).unwrap(),
        );
        let err = table.update_peer(peer, &diverged, None, 11).unwrap_err();
        assert!(matches!(*err, PeerAckError::Diverged { .. }));

        let acked = table.acked_by(&ns, &origin, Seq0::new(2));
        assert!(acked.is_empty());
    }

    #[test]
    fn diverged_peer_recovers_after_advancing_past_conflict() {
        let peer = replica(30);
        let origin = replica(31);
        let ns = namespace();
        let mut table = PeerAckTable::new();
        let mut eligible = BTreeSet::new();
        eligible.insert(peer);
        table.set_eligibility(ns.clone(), eligible);

        let mut durable: WatermarkState<Durable> = BTreeMap::new();
        durable.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([1u8; 32])).unwrap(),
        );
        table.update_peer(peer, &durable, None, 10).unwrap();

        let mut diverged: WatermarkState<Durable> = BTreeMap::new();
        diverged.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([2u8; 32])).unwrap(),
        );
        let err = table.update_peer(peer, &diverged, None, 11).unwrap_err();
        assert!(matches!(*err, PeerAckError::Diverged { .. }));

        let mut recovered: WatermarkState<Durable> = BTreeMap::new();
        recovered.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(3), HeadStatus::Known([3u8; 32])).unwrap(),
        );
        table.update_peer(peer, &recovered, None, 12).unwrap();

        let acked = table.acked_by(&ns, &origin, Seq0::new(3));
        assert_eq!(acked, vec![peer]);
        assert!(matches!(
            table.satisfied_k(&ns, &origin, Seq0::new(3), 1),
            QuorumOutcome::Satisfied { .. }
        ));
    }

    #[test]
    fn satisfied_k_distinguishes_failure_modes() {
        let ns = namespace();
        let origin = replica(5);
        let peer_a = replica(6);
        let peer_b = replica(7);
        let peer_c = replica(8);
        let mut table = PeerAckTable::new();
        let mut eligible = BTreeSet::new();
        eligible.insert(peer_a);
        eligible.insert(peer_b);
        eligible.insert(peer_c);
        table.set_eligibility(ns.clone(), eligible);

        let mut durable_a: WatermarkState<Durable> = BTreeMap::new();
        durable_a.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(5), HeadStatus::Known([5u8; 32])).unwrap(),
        );
        table.update_peer(peer_a, &durable_a, None, 10).unwrap();

        let mut durable_b: WatermarkState<Durable> = BTreeMap::new();
        durable_b.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(4), HeadStatus::Known([4u8; 32])).unwrap(),
        );
        table.update_peer(peer_b, &durable_b, None, 11).unwrap();

        let mut durable_c: WatermarkState<Durable> = BTreeMap::new();
        durable_c.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(5), HeadStatus::Known([5u8; 32])).unwrap(),
        );
        table.update_peer(peer_c, &durable_c, None, 12).unwrap();

        let status = table.satisfied_k(&ns, &origin, Seq0::new(5), 2);
        assert!(status.is_satisfied());

        let pending = table.satisfied_k(&ns, &origin, Seq0::new(6), 2);
        assert!(matches!(pending, QuorumOutcome::Pending { .. }));

        let insufficient = table.satisfied_k(&ns, &origin, Seq0::new(5), 4);
        assert!(matches!(
            insufficient,
            QuorumOutcome::InsufficientEligible { .. }
        ));
    }

    #[test]
    fn quarantined_peer_stays_quarantined_until_conflicted_watermark_realigns() {
        let peer = replica(40);
        let origin = replica(41);
        let other_origin = replica(42);
        let ns = namespace();
        let mut table = PeerAckTable::new();
        let mut eligible = BTreeSet::new();
        eligible.insert(peer);
        table.set_eligibility(ns.clone(), eligible);

        let mut durable: WatermarkState<Durable> = BTreeMap::new();
        durable.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([1u8; 32])).unwrap(),
        );
        table.update_peer(peer, &durable, None, 10).unwrap();

        let mut diverged: WatermarkState<Durable> = BTreeMap::new();
        diverged.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([2u8; 32])).unwrap(),
        );
        table.update_peer(peer, &diverged, None, 11).unwrap_err();

        let mut unrelated: WatermarkState<Durable> = BTreeMap::new();
        unrelated.entry(ns.clone()).or_default().insert(
            other_origin,
            Watermark::new(Seq0::new(5), HeadStatus::Known([5u8; 32])).unwrap(),
        );
        let err = table.update_peer(peer, &unrelated, None, 12).unwrap_err();
        assert!(matches!(*err, PeerAckError::PeerQuarantined { .. }));

        let mut same_conflict: WatermarkState<Durable> = BTreeMap::new();
        same_conflict.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([2u8; 32])).unwrap(),
        );
        let err = table
            .update_peer(peer, &same_conflict, None, 13)
            .unwrap_err();
        assert!(matches!(*err, PeerAckError::PeerQuarantined { .. }));

        let mut realigned: WatermarkState<Durable> = BTreeMap::new();
        realigned.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([1u8; 32])).unwrap(),
        );
        table.update_peer(peer, &realigned, None, 14).unwrap();
        assert_eq!(table.acked_by(&ns, &origin, Seq0::new(2)), vec![peer]);
        assert!(matches!(
            table.peers.get(&peer).map(|state| &state.status),
            Some(PeerAckStatus::Healthy)
        ));
    }

    #[test]
    fn quarantine_recovery_keeps_quarantine_and_watermarks_on_later_peer_error() {
        let peer = replica(60);
        let origin = replica(61);
        let other_origin = replica(62);
        let ns = namespace();
        let mut table = PeerAckTable::new();
        let mut eligible = BTreeSet::new();
        eligible.insert(peer);
        table.set_eligibility(ns.clone(), eligible);

        let mut durable: WatermarkState<Durable> = BTreeMap::new();
        let durable_ns = durable.entry(ns.clone()).or_default();
        durable_ns.insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([1u8; 32])).unwrap(),
        );
        durable_ns.insert(
            other_origin,
            Watermark::new(Seq0::new(5), HeadStatus::Known([5u8; 32])).unwrap(),
        );
        table.update_peer(peer, &durable, None, 10).unwrap();

        let mut diverged: WatermarkState<Durable> = BTreeMap::new();
        diverged.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([2u8; 32])).unwrap(),
        );
        table.update_peer(peer, &diverged, None, 11).unwrap_err();

        let mut recovery_then_error: WatermarkState<Durable> = BTreeMap::new();
        let recovery_ns = recovery_then_error.entry(ns.clone()).or_default();
        recovery_ns.insert(
            origin,
            Watermark::new(Seq0::new(3), HeadStatus::Known([3u8; 32])).unwrap(),
        );
        recovery_ns.insert(
            other_origin,
            Watermark::new(Seq0::new(4), HeadStatus::Known([4u8; 32])).unwrap(),
        );
        let err = table
            .update_peer(peer, &recovery_then_error, None, 12)
            .unwrap_err();
        assert!(matches!(
            *err,
            PeerAckError::NonMonotonic {
                peer: got_peer,
                namespace: got_ns,
                origin: got_origin,
                current,
                attempted,
            } if got_peer == peer
                && got_ns == ns
                && got_origin == other_origin
                && current == Seq0::new(5)
                && attempted == Seq0::new(4)
        ));

        let state = table.peers.get(&peer).expect("peer state");
        assert!(matches!(state.status, PeerAckStatus::Quarantined { .. }));
        assert_eq!(
            state
                .durable
                .get(&ns, &origin)
                .expect("conflicted watermark still present")
                .seq(),
            Seq0::new(2)
        );
        assert_eq!(
            state
                .durable
                .get(&ns, &other_origin)
                .expect("other origin watermark still present")
                .seq(),
            Seq0::new(5)
        );
        assert_eq!(state.last_ack_at_ms, 10);
        assert!(table.acked_by(&ns, &origin, Seq0::new(2)).is_empty());
    }

    #[test]
    fn applied_quarantine_stays_quarantined_until_applied_watermark_realigns() {
        let peer = replica(50);
        let origin = replica(51);
        let ns = namespace();
        let mut table = PeerAckTable::new();
        let mut eligible = BTreeSet::new();
        eligible.insert(peer);
        table.set_eligibility(ns.clone(), eligible);

        let durable: WatermarkState<Durable> = BTreeMap::new();
        let mut applied: WatermarkState<Applied> = BTreeMap::new();
        applied.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([1u8; 32])).unwrap(),
        );
        table
            .update_peer(peer, &durable, Some(&applied), 10)
            .unwrap();

        let mut diverged_applied: WatermarkState<Applied> = BTreeMap::new();
        diverged_applied.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([2u8; 32])).unwrap(),
        );
        table
            .update_peer(peer, &durable, Some(&diverged_applied), 11)
            .unwrap_err();

        let mut durable_advance: WatermarkState<Durable> = BTreeMap::new();
        durable_advance.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(3), HeadStatus::Known([3u8; 32])).unwrap(),
        );
        let empty_applied: WatermarkState<Applied> = BTreeMap::new();
        let err = table
            .update_peer(peer, &durable_advance, Some(&empty_applied), 12)
            .unwrap_err();
        assert!(matches!(*err, PeerAckError::PeerQuarantined { .. }));

        let mut applied_realigned: WatermarkState<Applied> = BTreeMap::new();
        applied_realigned.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([1u8; 32])).unwrap(),
        );
        table
            .update_peer(peer, &durable, Some(&applied_realigned), 13)
            .unwrap();
        assert!(matches!(
            table.peers.get(&peer).map(|state| &state.status),
            Some(PeerAckStatus::Healthy)
        ));
    }

    #[test]
    fn applied_quarantine_recovery_keeps_quarantine_and_watermarks_on_later_applied_error() {
        let peer = replica(70);
        let origin = replica(71);
        let other_origin = replica(72);
        let ns = namespace();
        let mut table = PeerAckTable::new();
        let mut eligible = BTreeSet::new();
        eligible.insert(peer);
        table.set_eligibility(ns.clone(), eligible);

        let durable: WatermarkState<Durable> = BTreeMap::new();
        let mut applied: WatermarkState<Applied> = BTreeMap::new();
        let applied_ns = applied.entry(ns.clone()).or_default();
        applied_ns.insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([1u8; 32])).unwrap(),
        );
        applied_ns.insert(
            other_origin,
            Watermark::new(Seq0::new(5), HeadStatus::Known([5u8; 32])).unwrap(),
        );
        table
            .update_peer(peer, &durable, Some(&applied), 10)
            .unwrap();

        let mut diverged_applied: WatermarkState<Applied> = BTreeMap::new();
        diverged_applied.entry(ns.clone()).or_default().insert(
            origin,
            Watermark::new(Seq0::new(2), HeadStatus::Known([2u8; 32])).unwrap(),
        );
        table
            .update_peer(peer, &durable, Some(&diverged_applied), 11)
            .unwrap_err();

        let mut recovery_then_error: WatermarkState<Applied> = BTreeMap::new();
        let recovery_ns = recovery_then_error.entry(ns.clone()).or_default();
        recovery_ns.insert(
            origin,
            Watermark::new(Seq0::new(3), HeadStatus::Known([3u8; 32])).unwrap(),
        );
        recovery_ns.insert(
            other_origin,
            Watermark::new(Seq0::new(4), HeadStatus::Known([4u8; 32])).unwrap(),
        );
        let err = table
            .update_peer(peer, &durable, Some(&recovery_then_error), 12)
            .unwrap_err();
        assert!(matches!(
            *err,
            PeerAckError::NonMonotonic {
                peer: got_peer,
                namespace: got_ns,
                origin: got_origin,
                current,
                attempted,
            } if got_peer == peer
                && got_ns == ns
                && got_origin == other_origin
                && current == Seq0::new(5)
                && attempted == Seq0::new(4)
        ));

        let state = table.peers.get(&peer).expect("peer state");
        assert!(matches!(state.status, PeerAckStatus::Quarantined { .. }));
        assert_eq!(
            state
                .applied
                .get(&ns, &origin)
                .expect("conflicted applied watermark still present")
                .seq(),
            Seq0::new(2)
        );
        assert_eq!(
            state
                .applied
                .get(&ns, &other_origin)
                .expect("other applied watermark still present")
                .seq(),
            Seq0::new(5)
        );
        assert_eq!(state.last_ack_at_ms, 10);
    }
}
