//! Peer ACK tracking for durability coordination.

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::core::{
    Applied, Durable, HeadStatus, NamespaceId, ReplicaId, Seq0, WatermarkError, Watermarks,
};

use super::proto::{WatermarkHeads, WatermarkMap};

#[derive(Debug, Default)]
pub struct PeerAckTable {
    peers: BTreeMap<ReplicaId, PeerAckState>,
    eligible: BTreeMap<NamespaceId, BTreeSet<ReplicaId>>,
}

#[derive(Debug, Clone)]
struct PeerAckState {
    durable: Watermarks<Durable>,
    applied: Watermarks<Applied>,
    last_ack_at_ms: u64,
    diverged: bool,
}

impl Default for PeerAckState {
    fn default() -> Self {
        Self {
            durable: Watermarks::new(),
            applied: Watermarks::new(),
            last_ack_at_ms: 0,
            diverged: false,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PeerAckSnapshot {
    pub peer: ReplicaId,
    pub durable: Watermarks<Durable>,
    pub applied: Watermarks<Applied>,
    pub last_ack_at_ms: u64,
    pub diverged: bool,
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

    #[error("peer {peer} is marked diverged")]
    PeerDiverged { peer: ReplicaId },
}

pub type PeerAckResult<T> = Result<T, Box<PeerAckError>>;

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

    pub(crate) fn snapshot(&self) -> Vec<PeerAckSnapshot> {
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
                    diverged: state.diverged,
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
        durable: &WatermarkMap,
        durable_heads: Option<&WatermarkHeads>,
        applied: Option<&WatermarkMap>,
        applied_heads: Option<&WatermarkHeads>,
        now_ms: u64,
    ) -> PeerAckResult<()> {
        let state = self.peers.entry(peer).or_default();
        if state.diverged {
            return Err(Box::new(PeerAckError::PeerDiverged { peer }));
        }
        update_watermarks(
            &mut state.durable,
            peer,
            durable,
            durable_heads,
            &mut state.diverged,
        )?;

        if let Some(applied) = applied {
            update_watermarks(
                &mut state.applied,
                peer,
                applied,
                applied_heads,
                &mut state.diverged,
            )?;
        }

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
                if state.diverged {
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

        let mut eligible_total = 0usize;
        let mut acked_by = Vec::new();

        for peer in eligible {
            let state = self.peers.get(peer);
            if state.map(|state| state.diverged).unwrap_or(false) {
                continue;
            }
            eligible_total += 1;

            if let Some(state) = state {
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

        let required = k as usize;
        if eligible_total < required {
            return QuorumOutcome::InsufficientEligible {
                required: k,
                eligible_total,
            };
        }

        if acked_by.len() >= required {
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

fn head_status_for(
    seq: Seq0,
    heads: Option<&WatermarkHeads>,
    namespace: &NamespaceId,
    origin: &ReplicaId,
) -> HeadStatus {
    if let Some(sha) = heads
        .and_then(|map| map.get(namespace))
        .and_then(|origins| origins.get(origin))
    {
        return HeadStatus::Known(sha.0);
    }

    if seq.get() == 0 {
        HeadStatus::Genesis
    } else {
        HeadStatus::Unknown
    }
}

fn update_watermarks<K>(
    watermarks: &mut Watermarks<K>,
    peer: ReplicaId,
    updates: &WatermarkMap,
    heads: Option<&WatermarkHeads>,
    diverged: &mut bool,
) -> PeerAckResult<()> {
    for (namespace, origins) in updates {
        for (origin, seq) in origins {
            let seq0 = Seq0::new(*seq);
            let incoming_head = head_status_for(seq0, heads, namespace, origin);
            let (current_seq, current_head) = watermarks
                .get(namespace, origin)
                .map(|watermark| (watermark.seq(), watermark.head()))
                .unwrap_or((Seq0::ZERO, HeadStatus::Genesis));

            if seq0 < current_seq {
                return Err(Box::new(PeerAckError::NonMonotonic {
                    peer,
                    namespace: namespace.clone(),
                    origin: *origin,
                    current: current_seq,
                    attempted: seq0,
                }));
            }

            if seq0 == current_seq
                && let (HeadStatus::Known(expected), HeadStatus::Known(got)) =
                    (current_head, incoming_head)
                && expected != got
            {
                *diverged = true;
                return Err(Box::new(PeerAckError::Diverged {
                    peer,
                    namespace: namespace.clone(),
                    origin: *origin,
                    seq: seq0,
                    expected,
                    got,
                }));
            }

            watermarks
                .observe_at_least(namespace, origin, seq0, incoming_head)
                .map_err(|source| PeerAckError::InvalidWatermark {
                    peer,
                    namespace: namespace.clone(),
                    origin: *origin,
                    source,
                })
                .map_err(Box::new)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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

        let mut durable = WatermarkMap::new();
        durable.entry(ns.clone()).or_default().insert(origin, 3);
        table
            .update_peer(peer, &durable, None, None, None, 10)
            .unwrap();

        let mut backwards = WatermarkMap::new();
        backwards.entry(ns.clone()).or_default().insert(origin, 2);
        let err = table
            .update_peer(peer, &backwards, None, None, None, 12)
            .unwrap_err();
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

        let mut durable = WatermarkMap::new();
        durable.entry(ns.clone()).or_default().insert(origin, 2);
        let mut heads = WatermarkHeads::new();
        heads
            .entry(ns.clone())
            .or_default()
            .insert(origin, crate::core::Sha256([1u8; 32]));
        table
            .update_peer(peer, &durable, Some(&heads), None, None, 10)
            .unwrap();

        let mut bad_heads = WatermarkHeads::new();
        bad_heads
            .entry(ns.clone())
            .or_default()
            .insert(origin, crate::core::Sha256([2u8; 32]));
        let err = table
            .update_peer(peer, &durable, Some(&bad_heads), None, None, 11)
            .unwrap_err();
        assert!(matches!(*err, PeerAckError::Diverged { .. }));

        let acked = table.acked_by(&ns, &origin, Seq0::new(2));
        assert!(acked.is_empty());
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

        let mut durable_a = WatermarkMap::new();
        durable_a.entry(ns.clone()).or_default().insert(origin, 5);
        table
            .update_peer(peer_a, &durable_a, None, None, None, 10)
            .unwrap();

        let mut durable_b = WatermarkMap::new();
        durable_b.entry(ns.clone()).or_default().insert(origin, 4);
        table
            .update_peer(peer_b, &durable_b, None, None, None, 11)
            .unwrap();

        let mut durable_c = WatermarkMap::new();
        durable_c.entry(ns.clone()).or_default().insert(origin, 5);
        table
            .update_peer(peer_c, &durable_c, None, None, None, 12)
            .unwrap();

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
}
