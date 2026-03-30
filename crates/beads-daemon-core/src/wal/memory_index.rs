use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use crate::core::{
    ActorId, ClientRequestId, EventId, NamespaceId, ReplicaId, SegmentId, Seq0, Seq1, TxnId,
    WatermarkPair,
};
use crate::durability::DurabilityRequestClaim;

use super::{
    ClientRequestEventIds, ClientRequestRow, HlcRow, IndexDurabilityMode, IndexedRangeItem,
    ReplicaLivenessRow, SegmentRow, WalIndex, WalIndexError, WalIndexReader, WalIndexTxn,
    WalIndexWriter, WatermarkRow,
};

type EventKey = (NamespaceId, ReplicaId, Seq1);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EventEntry {
    sha: [u8; 32],
    prev_sha: Option<[u8; 32]>,
    segment_id: SegmentId,
    offset: u64,
    len: u32,
    event_time_ms: u64,
    txn_id: TxnId,
    client_request_id: Option<ClientRequestId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct MemoryWalIndexState {
    orset_counter: u64,
    origin_next_seq: BTreeMap<(NamespaceId, ReplicaId), u64>,
    events: BTreeMap<EventKey, EventEntry>,
    segments: BTreeMap<(NamespaceId, SegmentId), SegmentRow>,
    watermarks: BTreeMap<(NamespaceId, ReplicaId), WatermarkRow>,
    hlc: BTreeMap<ActorId, HlcRow>,
    client_requests: BTreeMap<(NamespaceId, ReplicaId, ClientRequestId), ClientRequestRow>,
    replica_liveness: BTreeMap<ReplicaId, ReplicaLivenessRow>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MemoryWalIndexSnapshot {
    state: MemoryWalIndexState,
    mode: IndexDurabilityMode,
}

#[derive(Clone)]
pub struct MemoryWalIndex {
    state: Arc<RwLock<MemoryWalIndexState>>,
    txn_gate: Arc<AtomicBool>,
    mode: IndexDurabilityMode,
}

impl MemoryWalIndex {
    pub fn new() -> Self {
        Self::with_mode(IndexDurabilityMode::Cache)
    }

    pub fn with_mode(mode: IndexDurabilityMode) -> Self {
        Self {
            state: Arc::new(RwLock::new(MemoryWalIndexState::default())),
            txn_gate: Arc::new(AtomicBool::new(false)),
            mode,
        }
    }
    pub fn model_snapshot(&self) -> MemoryWalIndexSnapshot {
        let state = self
            .state
            .read()
            .expect("memory wal index lock poisoned")
            .clone();
        MemoryWalIndexSnapshot {
            state,
            mode: self.mode,
        }
    }
    pub fn from_snapshot(snapshot: MemoryWalIndexSnapshot) -> Self {
        Self {
            state: Arc::new(RwLock::new(snapshot.state)),
            txn_gate: Arc::new(AtomicBool::new(false)),
            mode: snapshot.mode,
        }
    }
}

impl Default for MemoryWalIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl WalIndex for MemoryWalIndex {
    fn writer(&self) -> Box<dyn WalIndexWriter> {
        Box::new(MemoryWalIndexWriter {
            state: Arc::clone(&self.state),
            txn_gate: Arc::clone(&self.txn_gate),
        })
    }

    fn reader(&self) -> Box<dyn WalIndexReader> {
        Box::new(MemoryWalIndexReader {
            state: Arc::clone(&self.state),
        })
    }

    fn durability_mode(&self) -> IndexDurabilityMode {
        self.mode
    }

    fn checkpoint_truncate(&self) -> Result<(), WalIndexError> {
        Ok(())
    }
}

struct MemoryWalIndexWriter {
    state: Arc<RwLock<MemoryWalIndexState>>,
    txn_gate: Arc<AtomicBool>,
}

impl WalIndexWriter for MemoryWalIndexWriter {
    fn begin_txn(&self) -> Result<Box<dyn WalIndexTxn>, WalIndexError> {
        acquire_gate(&self.txn_gate);
        let snapshot = self
            .state
            .read()
            .expect("memory wal index lock poisoned")
            .clone();
        Ok(Box::new(MemoryWalIndexTxn {
            state: Arc::clone(&self.state),
            txn_gate: Arc::clone(&self.txn_gate),
            working: snapshot,
            committed: false,
        }))
    }
}

struct MemoryWalIndexTxn {
    state: Arc<RwLock<MemoryWalIndexState>>,
    txn_gate: Arc<AtomicBool>,
    working: MemoryWalIndexState,
    committed: bool,
}

impl MemoryWalIndexTxn {
    fn ensure_live(&self) -> Result<(), WalIndexError> {
        if self.committed {
            return Err(WalIndexError::EventIdDecode(
                "memory wal index txn already finished".to_string(),
            ));
        }
        Ok(())
    }
}

impl WalIndexTxn for MemoryWalIndexTxn {
    fn next_orset_counter(&mut self) -> Result<u64, WalIndexError> {
        self.ensure_live()?;
        let next =
            self.working.orset_counter.checked_add(1).ok_or_else(|| {
                WalIndexError::EventIdDecode("orset counter overflow".to_string())
            })?;
        self.working.orset_counter = next;
        Ok(next)
    }

    fn observe_orset_counter(&mut self, counter: u64) -> Result<(), WalIndexError> {
        self.ensure_live()?;
        self.working.orset_counter = self.working.orset_counter.max(counter);
        Ok(())
    }

    fn next_origin_seq(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
    ) -> Result<Seq1, WalIndexError> {
        self.ensure_live()?;
        let key = (ns.clone(), *origin);
        let next_raw = self.working.origin_next_seq.get(&key).copied().unwrap_or(1);
        let updated = next_raw
            .checked_add(1)
            .ok_or_else(|| WalIndexError::OriginSeqOverflow {
                namespace: ns.as_str().to_string(),
                origin: *origin,
            })?;
        let next = Seq1::from_u64(next_raw)
            .ok_or_else(|| WalIndexError::EventIdDecode("origin_seq must be >= 1".to_string()))?;
        self.working.origin_next_seq.insert(key, updated);
        Ok(next)
    }

    fn set_next_origin_seq(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        next_seq: Seq1,
    ) -> Result<(), WalIndexError> {
        self.ensure_live()?;
        self.working
            .origin_next_seq
            .insert((ns.clone(), *origin), next_seq.get());
        Ok(())
    }

    fn record_event(
        &mut self,
        ns: &NamespaceId,
        eid: &EventId,
        sha: [u8; 32],
        prev_sha: Option<[u8; 32]>,
        segment_id: SegmentId,
        offset: u64,
        len: u32,
        event_time_ms: u64,
        txn_id: TxnId,
        client_request_id: Option<ClientRequestId>,
    ) -> Result<(), WalIndexError> {
        self.ensure_live()?;
        let key = (ns.clone(), eid.origin_replica_id, eid.origin_seq);
        if let Some(existing) = self.working.events.get(&key) {
            if existing.sha == sha {
                let mut mismatches = Vec::new();
                if existing.prev_sha != prev_sha {
                    mismatches.push("prev_sha");
                }
                if existing.event_time_ms != event_time_ms {
                    mismatches.push("event_time_ms");
                }
                if existing.txn_id != txn_id {
                    mismatches.push("txn_id");
                }
                if existing.client_request_id != client_request_id {
                    mismatches.push("client_request_id");
                }

                if mismatches.is_empty() {
                    return Ok(());
                }

                return Err(WalIndexError::EventConflict {
                    namespace: ns.clone(),
                    origin: eid.origin_replica_id,
                    seq: eid.origin_seq.get(),
                    reason: format!(
                        "duplicate event id with mismatched fields: {}; run `bd store fsck --repair` and rebuild wal index",
                        mismatches.join(", ")
                    ),
                });
            }
            return Err(WalIndexError::Equivocation {
                namespace: ns.clone(),
                origin: eid.origin_replica_id,
                seq: eid.origin_seq.get(),
                existing_sha256: existing.sha,
                new_sha256: sha,
            });
        }

        self.working.events.insert(
            key,
            EventEntry {
                sha,
                prev_sha,
                segment_id,
                offset,
                len,
                event_time_ms,
                txn_id,
                client_request_id,
            },
        );
        Ok(())
    }

    fn update_watermark(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        watermarks: WatermarkPair,
    ) -> Result<(), WalIndexError> {
        self.ensure_live()?;
        self.working.watermarks.insert(
            (ns.clone(), *origin),
            WatermarkRow::new(ns.clone(), *origin, watermarks),
        );
        Ok(())
    }

    fn update_hlc(&mut self, hlc: &HlcRow) -> Result<(), WalIndexError> {
        self.ensure_live()?;
        self.working.hlc.insert(hlc.actor_id.clone(), hlc.clone());
        Ok(())
    }

    fn upsert_segment(&mut self, segment: &SegmentRow) -> Result<(), WalIndexError> {
        self.ensure_live()?;
        self.working.segments.insert(
            (segment.namespace().clone(), segment.segment_id()),
            segment.clone(),
        );
        Ok(())
    }

    fn replace_namespace_segments(
        &mut self,
        ns: &NamespaceId,
        segments: &[SegmentRow],
    ) -> Result<(), WalIndexError> {
        self.ensure_live()?;
        for segment in segments {
            if segment.namespace() != ns {
                return Err(WalIndexError::SegmentRowDecode(format!(
                    "segment namespace mismatch (expected {}, got {})",
                    ns,
                    segment.namespace()
                )));
            }
        }
        self.working.segments.retain(|(row_ns, _), _| row_ns != ns);
        for segment in segments {
            self.working
                .segments
                .insert((ns.clone(), segment.segment_id()), segment.clone());
        }
        Ok(())
    }

    fn upsert_client_request(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        client_request_id: ClientRequestId,
        request_sha256: [u8; 32],
        txn_id: TxnId,
        event_ids: &ClientRequestEventIds,
        created_at_ms: u64,
        durability_claim: Option<&DurabilityRequestClaim>,
    ) -> Result<(), WalIndexError> {
        self.ensure_live()?;
        event_ids.ensure_matches(ns, origin)?;
        let key = (ns.clone(), *origin, client_request_id);
        if let Some(existing) = self.working.client_requests.get(&key) {
            if existing.request_sha256 != request_sha256 {
                return Err(WalIndexError::ClientRequestIdReuseMismatch {
                    namespace: ns.clone(),
                    origin: *origin,
                    client_request_id,
                    expected_request_sha256: existing.request_sha256,
                    got_request_sha256: request_sha256,
                });
            }
            return Ok(());
        }

        self.working.client_requests.insert(
            key,
            ClientRequestRow {
                request_sha256,
                txn_id,
                event_ids: event_ids.clone(),
                created_at_ms,
                durability_claim: durability_claim.cloned(),
            },
        );
        Ok(())
    }

    fn upsert_replica_liveness(&mut self, row: &ReplicaLivenessRow) -> Result<(), WalIndexError> {
        self.ensure_live()?;
        self.working
            .replica_liveness
            .entry(row.replica_id)
            .and_modify(|existing| {
                existing.last_seen_ms = existing.last_seen_ms.max(row.last_seen_ms);
                existing.last_handshake_ms = existing.last_handshake_ms.max(row.last_handshake_ms);
                existing.role = row.role;
            })
            .or_insert_with(|| row.clone());
        Ok(())
    }

    fn commit(mut self: Box<Self>) -> Result<(), WalIndexError> {
        if self.committed {
            return Ok(());
        }
        let mut guard = self.state.write().expect("memory wal index lock poisoned");
        *guard = std::mem::take(&mut self.working);
        self.committed = true;
        self.txn_gate.store(false, Ordering::Release);
        Ok(())
    }

    fn rollback(mut self: Box<Self>) -> Result<(), WalIndexError> {
        if self.committed {
            return Ok(());
        }
        self.committed = true;
        self.txn_gate.store(false, Ordering::Release);
        Ok(())
    }
}

impl Drop for MemoryWalIndexTxn {
    fn drop(&mut self) {
        if !self.committed {
            self.txn_gate.store(false, Ordering::Release);
        }
    }
}

fn acquire_gate(gate: &Arc<AtomicBool>) {
    let mut backoff = std::time::Duration::from_micros(50);
    while gate
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        std::thread::sleep(backoff);
        backoff = std::cmp::min(
            backoff.saturating_mul(2),
            std::time::Duration::from_millis(5),
        );
    }
}

struct MemoryWalIndexReader {
    state: Arc<RwLock<MemoryWalIndexState>>,
}

impl MemoryWalIndexReader {
    fn with_state<T>(
        &self,
        f: impl FnOnce(&MemoryWalIndexState) -> Result<T, WalIndexError>,
    ) -> Result<T, WalIndexError> {
        let guard = self.state.read().expect("memory wal index lock poisoned");
        f(&guard)
    }
}

impl WalIndexReader for MemoryWalIndexReader {
    fn load_orset_counter(&self) -> Result<u64, WalIndexError> {
        Ok(self
            .state
            .read()
            .expect("memory wal index lock poisoned")
            .orset_counter)
    }

    fn lookup_event_sha(
        &self,
        ns: &NamespaceId,
        eid: &EventId,
    ) -> Result<Option<[u8; 32]>, WalIndexError> {
        self.with_state(|state| {
            let key = (ns.clone(), eid.origin_replica_id, eid.origin_seq);
            Ok(state.events.get(&key).map(|entry| entry.sha))
        })
    }

    fn list_segments(&self, ns: &NamespaceId) -> Result<Vec<SegmentRow>, WalIndexError> {
        self.with_state(|state| {
            let mut rows: Vec<SegmentRow> = state
                .segments
                .values()
                .filter(|row| row.namespace() == ns)
                .cloned()
                .collect();
            rows.sort_by_key(|row| (row.created_at_ms(), row.segment_id()));
            Ok(rows)
        })
    }

    fn list_segment_namespaces(&self) -> Result<Vec<NamespaceId>, WalIndexError> {
        self.with_state(|state| {
            let mut namespaces: Vec<NamespaceId> = state
                .segments
                .keys()
                .map(|(namespace, _)| namespace.clone())
                .collect();
            namespaces.sort();
            namespaces.dedup();
            Ok(namespaces)
        })
    }

    fn load_watermarks(&self) -> Result<Vec<WatermarkRow>, WalIndexError> {
        self.with_state(|state| {
            let mut rows: Vec<WatermarkRow> = state.watermarks.values().cloned().collect();
            rows.sort_by_key(|row| (row.namespace.clone(), row.origin));
            Ok(rows)
        })
    }

    fn load_hlc(&self) -> Result<Vec<HlcRow>, WalIndexError> {
        self.with_state(|state| {
            let mut rows: Vec<HlcRow> = state.hlc.values().cloned().collect();
            rows.sort_by_key(|row| row.actor_id.clone());
            Ok(rows)
        })
    }

    fn load_replica_liveness(&self) -> Result<Vec<ReplicaLivenessRow>, WalIndexError> {
        self.with_state(|state| {
            let mut rows: Vec<ReplicaLivenessRow> =
                state.replica_liveness.values().cloned().collect();
            rows.sort_by_key(|row| row.replica_id);
            Ok(rows)
        })
    }

    fn iter_from(
        &self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        from_seq_excl: Seq0,
        max_bytes: usize,
    ) -> Result<Vec<IndexedRangeItem>, WalIndexError> {
        self.with_state(|state| {
            let mut items = Vec::new();
            let mut bytes_accum = 0usize;
            let start_seq = match from_seq_excl.get().checked_add(1) {
                Some(value) => value,
                None => return Ok(items),
            };
            let Some(start_seq) = Seq1::from_u64(start_seq) else {
                return Err(WalIndexError::EventIdDecode(
                    "origin_seq not Seq1".to_string(),
                ));
            };
            let start_key = (ns.clone(), *origin, start_seq);
            for ((key_ns, key_origin, key_seq), entry) in state.events.range(start_key..) {
                if key_ns != ns || key_origin != origin {
                    break;
                }
                if bytes_accum + entry.len as usize > max_bytes {
                    break;
                }
                bytes_accum += entry.len as usize;
                items.push(IndexedRangeItem {
                    event_id: EventId::new(*origin, ns.clone(), *key_seq),
                    sha: entry.sha,
                    prev_sha: entry.prev_sha,
                    segment_id: entry.segment_id,
                    offset: entry.offset,
                    len: entry.len,
                    event_time_ms: entry.event_time_ms,
                    txn_id: entry.txn_id,
                    client_request_id: entry.client_request_id,
                });
            }
            Ok(items)
        })
    }

    fn lookup_client_request(
        &self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        client_request_id: ClientRequestId,
    ) -> Result<Option<ClientRequestRow>, WalIndexError> {
        self.with_state(|state| {
            let key = (ns.clone(), *origin, client_request_id);
            Ok(state.client_requests.get(&key).cloned())
        })
    }

    fn max_origin_seq(&self, ns: &NamespaceId, origin: &ReplicaId) -> Result<Seq0, WalIndexError> {
        self.with_state(|state| {
            let max_seq = Seq1::from_u64(u64::MAX).expect("MAX is not zero");
            let max_key = (ns.clone(), *origin, max_seq);
            if let Some((key, _)) = state.events.range(..=max_key).next_back() {
                let (k_ns, k_origin, k_seq) = key;
                if k_ns == ns && k_origin == origin {
                    return Ok(Seq0::new(k_seq.get()));
                }
            }
            Ok(Seq0::new(0))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    use crate::core::{
        Applied, Durable, EventId, HeadStatus, NamespaceId, SegmentId, Seq0, Seq1, TxnId, Watermark,
    };

    #[test]
    fn memory_index_records_event_and_watermarks() {
        let index = MemoryWalIndex::new();
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
        let mut txn = index.writer().begin_txn().expect("begin txn");
        let seq = txn.next_origin_seq(&namespace, &origin).expect("next seq");
        let event_id = EventId::new(origin, namespace.clone(), seq);
        let sha = [9u8; 32];
        let segment_id = SegmentId::new(Uuid::from_bytes([2u8; 16]));
        let txn_id = TxnId::new(Uuid::from_bytes([3u8; 16]));

        txn.record_event(
            &namespace,
            &event_id,
            sha,
            None,
            segment_id,
            12,
            64,
            1_700_000_000_000,
            txn_id,
            None,
        )
        .expect("record event");
        let applied =
            Watermark::<Applied>::new(Seq0::new(1), HeadStatus::Known(sha)).expect("watermark");
        let durable =
            Watermark::<Durable>::new(Seq0::new(1), HeadStatus::Known(sha)).expect("watermark");
        let watermarks = WatermarkPair::new(applied, durable).expect("watermark pair");
        txn.update_watermark(&namespace, &origin, watermarks)
            .expect("update watermark");
        txn.commit().expect("commit");

        let reader = index.reader();
        let stored = reader
            .lookup_event_sha(&namespace, &event_id)
            .expect("lookup");
        assert_eq!(stored, Some(sha));

        let rows = reader.load_watermarks().expect("load watermarks");
        let row = rows.iter().find(|row| row.origin == origin).expect("row");
        assert_eq!(row.applied_seq(), 1);
        assert_eq!(row.durable_seq(), 1);

        let max = reader
            .max_origin_seq(&namespace, &origin)
            .expect("max origin seq");
        assert_eq!(max, Seq0::new(1));

        let mut next = index.writer().begin_txn().expect("begin txn");
        let seq2 = next.next_origin_seq(&namespace, &origin).expect("next seq");
        assert_eq!(seq2, Seq1::from_u64(2).expect("seq1"));
    }

    #[test]
    fn memory_index_rejects_client_request_id_mismatch() {
        let index = MemoryWalIndex::new();
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([4u8; 16]));
        let request_id = ClientRequestId::new(Uuid::from_bytes([5u8; 16]));
        let event_id = EventId::new(origin, namespace.clone(), Seq1::from_u64(1).expect("seq1"));
        let event_ids = ClientRequestEventIds::single(event_id.clone());

        let mut first = index.writer().begin_txn().expect("begin txn");
        first
            .upsert_client_request(
                &namespace,
                &origin,
                request_id,
                [1u8; 32],
                TxnId::new(Uuid::from_bytes([6u8; 16])),
                &event_ids,
                10,
                None,
            )
            .expect("upsert client request");
        first.commit().expect("commit");

        let mut second = index.writer().begin_txn().expect("begin txn");
        let err = second
            .upsert_client_request(
                &namespace,
                &origin,
                request_id,
                [2u8; 32],
                TxnId::new(Uuid::from_bytes([7u8; 16])),
                &event_ids,
                11,
                None,
            )
            .expect_err("reuse mismatch");
        assert!(matches!(
            err,
            WalIndexError::ClientRequestIdReuseMismatch { .. }
        ));
    }

    #[test]
    fn model_snapshot_ignores_transaction_history() {
        let namespace = NamespaceId::core();
        let segment = SegmentRow::open(
            namespace.clone(),
            SegmentId::new(Uuid::from_bytes([8u8; 16])),
            "seg-0".into(),
            10,
            crate::wal::WalCursorOffset::new(1),
        );

        let once = MemoryWalIndex::new();
        let mut txn = once.writer().begin_txn().expect("begin txn");
        txn.upsert_segment(&segment).expect("upsert segment");
        txn.commit().expect("commit");

        let twice = MemoryWalIndex::new();
        let mut txn = twice.writer().begin_txn().expect("begin txn");
        txn.upsert_segment(&segment).expect("upsert segment");
        txn.commit().expect("commit first");
        let mut txn = twice.writer().begin_txn().expect("begin txn");
        txn.upsert_segment(&segment)
            .expect("upsert segment idempotent");
        txn.commit().expect("commit second");

        assert_eq!(
            once.reader()
                .list_segments(&namespace)
                .expect("list segments"),
            twice
                .reader()
                .list_segments(&namespace)
                .expect("list segments"),
            "logical index state should match"
        );
        assert_eq!(
            once.model_snapshot(),
            twice.model_snapshot(),
            "snapshot should not encode transaction history"
        );

        let snapshot = twice.model_snapshot();
        let restored = MemoryWalIndex::from_snapshot(snapshot.clone());
        assert_eq!(
            restored.model_snapshot(),
            snapshot,
            "snapshot round-trip should preserve logical state"
        );
        assert_eq!(
            restored
                .reader()
                .list_segments(&namespace)
                .expect("list restored segments"),
            vec![segment],
            "restored index should expose the same reader-visible state"
        );
    }

    #[test]
    fn memory_index_satisfies_contract() {
        crate::wal::contract::wal_index_laws(MemoryWalIndex::new);
    }
}
