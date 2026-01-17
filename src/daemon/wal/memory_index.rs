use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use crate::core::{
    ActorId, ClientRequestId, EventId, NamespaceId, ReplicaId, SegmentId, Seq0, Seq1, TxnId,
};

use super::{
    ClientRequestRow, HlcRow, IndexDurabilityMode, IndexedRangeItem, ReplicaLivenessRow,
    SegmentRow, WalIndex, WalIndexError, WalIndexReader, WalIndexTxn, WalIndexWriter, WatermarkRow,
};

type EventKey = (NamespaceId, ReplicaId, Seq1);

#[derive(Clone, Debug)]
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

#[derive(Clone, Default)]
struct MemoryWalIndexState {
    version: u64,
    origin_next_seq: BTreeMap<(NamespaceId, ReplicaId), u64>,
    events: BTreeMap<EventKey, EventEntry>,
    segments: BTreeMap<(NamespaceId, SegmentId), SegmentRow>,
    watermarks: BTreeMap<(NamespaceId, ReplicaId), WatermarkRow>,
    hlc: BTreeMap<ActorId, HlcRow>,
    client_requests: BTreeMap<(NamespaceId, ReplicaId, ClientRequestId), ClientRequestRow>,
    replica_liveness: BTreeMap<ReplicaId, ReplicaLivenessRow>,
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
        let base_version = snapshot.version;
        Ok(Box::new(MemoryWalIndexTxn {
            state: Arc::clone(&self.state),
            txn_gate: Arc::clone(&self.txn_gate),
            working: snapshot,
            base_version,
            committed: false,
        }))
    }
}

struct MemoryWalIndexTxn {
    state: Arc<RwLock<MemoryWalIndexState>>,
    txn_gate: Arc<AtomicBool>,
    working: MemoryWalIndexState,
    base_version: u64,
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
                return Ok(());
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
        applied: u64,
        durable: u64,
        applied_head_sha: Option<[u8; 32]>,
        durable_head_sha: Option<[u8; 32]>,
    ) -> Result<(), WalIndexError> {
        self.ensure_live()?;
        self.working.watermarks.insert(
            (ns.clone(), *origin),
            WatermarkRow {
                namespace: ns.clone(),
                origin: *origin,
                applied_seq: applied,
                durable_seq: durable,
                applied_head_sha,
                durable_head_sha,
            },
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
            (segment.namespace.clone(), segment.segment_id),
            segment.clone(),
        );
        Ok(())
    }

    fn upsert_client_request(
        &mut self,
        ns: &NamespaceId,
        origin: &ReplicaId,
        client_request_id: ClientRequestId,
        request_sha256: [u8; 32],
        txn_id: TxnId,
        event_ids: &[EventId],
        created_at_ms: u64,
    ) -> Result<(), WalIndexError> {
        self.ensure_live()?;
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
                event_ids: event_ids.to_vec(),
                created_at_ms,
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
                existing.durability_eligible = row.durability_eligible;
            })
            .or_insert_with(|| row.clone());
        Ok(())
    }

    fn commit(mut self: Box<Self>) -> Result<(), WalIndexError> {
        if self.committed {
            return Ok(());
        }
        let mut guard = self.state.write().expect("memory wal index lock poisoned");
        let _ = self.base_version;
        let mut working = std::mem::take(&mut self.working);
        working.version = guard.version.wrapping_add(1);
        *guard = working;
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
                .filter(|row| &row.namespace == ns)
                .cloned()
                .collect();
            rows.sort_by(|a, b| {
                a.created_at_ms
                    .cmp(&b.created_at_ms)
                    .then_with(|| a.segment_id.cmp(&b.segment_id))
            });
            Ok(rows)
        })
    }

    fn load_watermarks(&self) -> Result<Vec<WatermarkRow>, WalIndexError> {
        self.with_state(|state| {
            let mut rows: Vec<WatermarkRow> = state.watermarks.values().cloned().collect();
            rows.sort_by(|a, b| {
                a.namespace
                    .cmp(&b.namespace)
                    .then_with(|| a.origin.cmp(&b.origin))
            });
            Ok(rows)
        })
    }

    fn load_hlc(&self) -> Result<Vec<HlcRow>, WalIndexError> {
        self.with_state(|state| {
            let mut rows: Vec<HlcRow> = state.hlc.values().cloned().collect();
            rows.sort_by(|a, b| a.actor_id.cmp(&b.actor_id));
            Ok(rows)
        })
    }

    fn load_replica_liveness(&self) -> Result<Vec<ReplicaLivenessRow>, WalIndexError> {
        self.with_state(|state| {
            let mut rows: Vec<ReplicaLivenessRow> =
                state.replica_liveness.values().cloned().collect();
            rows.sort_by(|a, b| a.replica_id.cmp(&b.replica_id));
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
            let mut max = 0u64;
            for ((key_ns, key_origin, key_seq), _entry) in &state.events {
                if key_ns == ns && key_origin == origin {
                    max = max.max(key_seq.get());
                }
            }
            Ok(Seq0::new(max))
        })
    }
}
