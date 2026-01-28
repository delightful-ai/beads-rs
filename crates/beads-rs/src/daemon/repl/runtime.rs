//! Replication runtime adapters for the daemon coordinator.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use crossbeam::channel::{Sender, TrySendError};
use thiserror::Error;

use crate::core::error::details::{
    IndexCorruptDetails, OverloadedDetails, OverloadedSubsystem, WalCorruptDetails,
};
use crate::core::{
    Applied, CliErrorCode, Durable, ErrorPayload, EventBytes, EventFrameV1, EventId,
    EventShaLookupError, Limits, NamespaceId, Opaque, ProtocolErrorCode, ReplicaId, SegmentId,
    Seq0, Seq1, Sha256, StoreId, decode_event_body,
};
use crate::daemon::repl::error::{ReplError, ReplErrorDetails};
use crate::daemon::repl::proto::WatermarkState;
use crate::daemon::repl::{ContiguousBatch, IngestOutcome, SessionStore, WatermarkSnapshot};
use crate::daemon::wal::{
    EventWalError, FrameReader, IndexedRangeItem, ReplicaDurabilityRole, ReplicaLivenessRow,
    VerifiedRecord, WalIndex, WalIndexError, open_segment_reader,
};
use crate::paths;

const DEFAULT_RETRY_AFTER_MS: u64 = 100;

pub struct ReplIngestRequest {
    pub store_id: StoreId,
    pub batch: ContiguousBatch,
    pub now_ms: u64,
    pub respond: Sender<Result<IngestOutcome, ReplError>>,
}

#[derive(Clone)]
pub struct ReplSessionStore {
    store_id: StoreId,
    wal_index: Arc<dyn WalIndex>,
    ingest_tx: Sender<ReplIngestRequest>,
}

impl ReplSessionStore {
    pub fn new(
        store_id: StoreId,
        wal_index: Arc<dyn WalIndex>,
        ingest_tx: Sender<ReplIngestRequest>,
    ) -> Self {
        Self {
            store_id,
            wal_index,
            ingest_tx,
        }
    }

    fn overload_error() -> ReplError {
        ReplError::new(ProtocolErrorCode::Overloaded.into(), "overloaded", true).with_details(
            ReplErrorDetails::Overloaded(OverloadedDetails {
                subsystem: Some(OverloadedSubsystem::Repl),
                retry_after_ms: Some(DEFAULT_RETRY_AFTER_MS),
                queue_bytes: None,
                queue_events: None,
            }),
        )
    }
}

impl SessionStore for ReplSessionStore {
    fn watermark_snapshot(&self, namespaces: &[NamespaceId]) -> WatermarkSnapshot {
        let mut durable: WatermarkState<Durable> = BTreeMap::new();
        let mut applied: WatermarkState<Applied> = BTreeMap::new();

        let namespace_filter: Option<BTreeSet<NamespaceId>> = if namespaces.is_empty() {
            None
        } else {
            Some(namespaces.iter().cloned().collect())
        };

        let rows = match self.wal_index.reader().load_watermarks() {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(
                    "replication watermark snapshot failed for {}: {err}",
                    self.store_id
                );
                return WatermarkSnapshot { durable, applied };
            }
        };

        for row in rows {
            if let Some(filter) = &namespace_filter
                && !filter.contains(&row.namespace)
            {
                continue;
            }

            let ns = row.namespace.clone();
            let origin = row.origin;
            durable
                .entry(ns.clone())
                .or_default()
                .insert(origin, row.durable);
            applied.entry(ns).or_default().insert(origin, row.applied);
        }

        WatermarkSnapshot { durable, applied }
    }

    fn lookup_event_sha(&self, eid: &EventId) -> Result<Option<Sha256>, EventShaLookupError> {
        self.wal_index
            .reader()
            .lookup_event_sha(&eid.namespace, eid)
            .map(|opt| opt.map(Sha256))
            .map_err(EventShaLookupError::new)
    }

    fn ingest_remote_batch(
        &mut self,
        batch: &ContiguousBatch,
        now_ms: u64,
    ) -> Result<IngestOutcome, ReplError> {
        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        let request = ReplIngestRequest {
            store_id: self.store_id,
            batch: batch.clone(),
            now_ms,
            respond: respond_tx,
        };

        match self.ingest_tx.try_send(request) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                return Err(Self::overload_error());
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err(ReplError::new(
                    CliErrorCode::Internal.into(),
                    "replication ingest channel closed",
                    true,
                ));
            }
        }

        respond_rx.recv().unwrap_or_else(|_| {
            Err(ReplError::new(
                CliErrorCode::Internal.into(),
                "replication ingest response dropped",
                true,
            ))
        })
    }

    fn update_replica_liveness(
        &mut self,
        replica_id: ReplicaId,
        last_seen_ms: u64,
        last_handshake_ms: u64,
        role: ReplicaDurabilityRole,
    ) -> Result<(), WalIndexError> {
        let mut txn = self.wal_index.writer().begin_txn()?;
        txn.upsert_replica_liveness(&ReplicaLivenessRow {
            replica_id,
            last_seen_ms,
            last_handshake_ms,
            role,
        })?;
        txn.commit()
    }
}

#[derive(Clone)]
pub struct WalRangeReader {
    store_dir: PathBuf,
    wal_index: Arc<dyn WalIndex>,
    limits: Limits,
}

impl WalRangeReader {
    pub fn new(store_id: StoreId, wal_index: Arc<dyn WalIndex>, limits: Limits) -> Self {
        Self {
            store_dir: paths::store_dir(store_id),
            wal_index,
            limits,
        }
    }

    pub fn read_range(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        from_seq_excl: Seq0,
        max_bytes: usize,
    ) -> Result<Vec<EventFrameV1>, WalRangeError> {
        let items = self
            .wal_index
            .reader()
            .iter_from(namespace, origin, from_seq_excl, max_bytes)
            .map_err(WalRangeError::Index)?;

        if items.is_empty() {
            return Err(WalRangeError::MissingRange {
                namespace: namespace.clone(),
                origin: *origin,
                from_seq_excl,
            });
        }

        let segments =
            segment_paths_for_namespace(&self.store_dir, self.wal_index.as_ref(), namespace)?;

        let mut batch = ContiguousFrames::new(from_seq_excl);
        for item in items {
            if batch.len() >= self.limits.max_event_batch_events {
                break;
            }

            let segment_path = match segments.get(&item.segment_id) {
                Some(path) => path,
                None => {
                    return Err(WalRangeError::MissingRange {
                        namespace: namespace.clone(),
                        origin: *origin,
                        from_seq_excl,
                    });
                }
            };

            let record = read_record_at(
                segment_path,
                item.offset,
                self.limits.max_wal_record_bytes,
                &self.limits,
            )
            .map_err(|err| WalRangeError::Corrupt {
                namespace: namespace.clone(),
                segment_id: Some(item.segment_id),
                offset: Some(item.offset),
                reason: err.to_string(),
            })?;

            if record.header().origin_replica_id != *origin
                || record.header().origin_seq != item.event_id.origin_seq
            {
                return Err(WalRangeError::Corrupt {
                    namespace: namespace.clone(),
                    segment_id: Some(item.segment_id),
                    offset: Some(item.offset),
                    reason: "record header mismatch".to_string(),
                });
            }
            if record.header().sha256 != item.sha {
                return Err(WalRangeError::Corrupt {
                    namespace: namespace.clone(),
                    segment_id: Some(item.segment_id),
                    offset: Some(item.offset),
                    reason: "record sha mismatch".to_string(),
                });
            }

            batch.push(namespace, origin, &item, &record)?;
        }

        Ok(batch.into_vec())
    }
}

struct ContiguousFrames {
    expected_seq: Seq1,
    prev_sha: Option<[u8; 32]>,
    frames: Vec<EventFrameV1>,
}

impl ContiguousFrames {
    fn new(from_seq_excl: Seq0) -> Self {
        Self {
            expected_seq: from_seq_excl.next(),
            prev_sha: None,
            frames: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.frames.len()
    }

    fn push(
        &mut self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        item: &IndexedRangeItem,
        record: &VerifiedRecord,
    ) -> Result<(), WalRangeError> {
        if item.event_id.origin_seq != self.expected_seq {
            let missing_from = Seq0::new(self.expected_seq.get().saturating_sub(1));
            return Err(WalRangeError::MissingRange {
                namespace: namespace.clone(),
                origin: *origin,
                from_seq_excl: missing_from,
            });
        }

        let record_prev = record.header().prev_sha256;
        if record_prev != item.prev_sha {
            return Err(WalRangeError::Corrupt {
                namespace: namespace.clone(),
                segment_id: Some(item.segment_id),
                offset: Some(item.offset),
                reason: "record prev sha mismatch with index".to_string(),
            });
        }
        if let Some(prev_sha) = self.prev_sha
            && record_prev != Some(prev_sha)
        {
            return Err(WalRangeError::Corrupt {
                namespace: namespace.clone(),
                segment_id: Some(item.segment_id),
                offset: Some(item.offset),
                reason: "record prev sha mismatch with previous frame".to_string(),
            });
        }

        let frame = EventFrameV1::try_from_parts(
            item.event_id.clone(),
            Sha256(item.sha),
            item.prev_sha.map(Sha256),
            EventBytes::<Opaque>::new(Bytes::copy_from_slice(record.payload_bytes())),
        )
        .map_err(|err| WalRangeError::Corrupt {
            namespace: namespace.clone(),
            segment_id: Some(item.segment_id),
            offset: Some(item.offset),
            reason: format!("wal frame invariant failed: {err}"),
        })?;
        self.frames.push(frame);
        self.prev_sha = Some(item.sha);
        self.expected_seq = self.expected_seq.next();
        Ok(())
    }

    fn into_vec(self) -> Vec<EventFrameV1> {
        self.frames
    }
}

#[derive(Debug, Error)]
pub enum WalRangeError {
    #[error("wal range missing for {namespace} {origin} after seq {from_seq_excl}")]
    MissingRange {
        namespace: NamespaceId,
        origin: ReplicaId,
        from_seq_excl: Seq0,
    },
    #[error("wal corrupt: {reason}")]
    Corrupt {
        namespace: NamespaceId,
        segment_id: Option<SegmentId>,
        offset: Option<u64>,
        reason: String,
    },
    #[error("wal index error: {0}")]
    Index(#[from] WalIndexError),
}

impl WalRangeError {
    pub fn as_error_payload(&self) -> ErrorPayload {
        match self {
            WalRangeError::MissingRange { .. } => ErrorPayload::new(
                ProtocolErrorCode::BootstrapRequired.into(),
                "bootstrap required",
                false,
            ),
            WalRangeError::Corrupt {
                namespace,
                segment_id,
                offset,
                reason,
            } => ErrorPayload::new(ProtocolErrorCode::WalCorrupt.into(), "wal corrupt", false)
                .with_details(WalCorruptDetails {
                    namespace: namespace.clone(),
                    segment_id: *segment_id,
                    offset: *offset,
                    reason: reason.clone(),
                }),
            WalRangeError::Index(err) => ErrorPayload::new(
                ProtocolErrorCode::IndexCorrupt.into(),
                "index corrupt",
                false,
            )
            .with_details(IndexCorruptDetails {
                reason: err.to_string(),
            }),
        }
    }
}

fn segment_paths_for_namespace(
    store_dir: &Path,
    wal_index: &dyn WalIndex,
    namespace: &NamespaceId,
) -> Result<HashMap<SegmentId, PathBuf>, WalRangeError> {
    let segments = wal_index.reader().list_segments(namespace)?;
    let mut map = HashMap::new();
    for segment in segments {
        let path = if segment.segment_path().is_absolute() {
            segment.segment_path().to_path_buf()
        } else {
            store_dir.join(segment.segment_path())
        };
        map.insert(segment.segment_id(), path);
    }
    Ok(map)
}

fn read_record_at(
    path: &Path,
    offset: u64,
    max_record_bytes: usize,
    limits: &Limits,
) -> Result<VerifiedRecord, EventWalError> {
    let mut reader = open_segment_reader(path)?;
    reader
        .seek(SeekFrom::Start(offset))
        .map_err(|source| EventWalError::Io {
            path: Some(path.to_path_buf()),
            source,
        })?;

    let mut reader = FrameReader::new(reader, max_record_bytes);
    let record = reader
        .read_next()
        .map_err(|err| match err {
            EventWalError::Io { source, .. } => EventWalError::Io {
                path: Some(path.to_path_buf()),
                source,
            },
            other => other,
        })?
        .ok_or_else(|| EventWalError::FrameLengthInvalid {
            reason: "unexpected eof while reading record".to_string(),
        })?;
    let (_, event_body) = decode_event_body(record.payload_bytes(), limits).map_err(|err| {
        EventWalError::RecordHeaderInvalid {
            reason: format!("event body decode failed: {err}"),
        }
    })?;
    record
        .verify_with_event_body(event_body)
        .map_err(|err| EventWalError::RecordHeaderInvalid {
            reason: err.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::error::details::{IndexCorruptDetails, WalCorruptDetails};

    #[test]
    fn wal_range_corrupt_maps_to_wal_corrupt_details() {
        let namespace = NamespaceId::core();
        let segment_id = SegmentId::new(uuid::Uuid::nil());
        let payload = WalRangeError::Corrupt {
            namespace: namespace.clone(),
            segment_id: Some(segment_id),
            offset: Some(42),
            reason: "header mismatch".to_string(),
        }
        .as_error_payload();

        assert_eq!(payload.code, ProtocolErrorCode::WalCorrupt.into());
        let details: WalCorruptDetails = payload.details_as().unwrap().unwrap();
        assert_eq!(details.namespace, namespace);
        assert_eq!(details.segment_id, Some(segment_id));
        assert_eq!(details.offset, Some(42));
        assert_eq!(details.reason, "header mismatch");
    }

    #[test]
    fn wal_range_index_maps_to_index_corrupt_details() {
        let payload =
            WalRangeError::Index(WalIndexError::MetaMissing { key: "store_id" }).as_error_payload();

        assert_eq!(payload.code, ProtocolErrorCode::IndexCorrupt.into());
        let details: IndexCorruptDetails = payload.details_as().unwrap().unwrap();
        assert_eq!(details.reason, "missing meta key: store_id");
    }
}
