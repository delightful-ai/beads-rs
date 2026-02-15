//! Internal WAL seam traits.
//!
//! These traits keep capabilities small and law-driven so daemon orchestration can
//! depend on stable behavior contracts rather than concrete storage structs.

use crate::core::{EventFrameV1, NamespaceId, ReplicaId, Seq0};

use super::{
    AppendOutcome, EventWal, EventWalResult, VerifiedRecord, WalIndex, WalIndexError, WalIndexTxn,
};

/// Capability: append a verified record to namespace WAL storage.
///
/// # Laws
/// - Appends are per-namespace and preserve monotonic byte offsets.
/// - Returned [`AppendOutcome`] describes the exact persisted segment/offset/len.
pub(crate) trait WalAppend {
    fn wal_append(
        &mut self,
        namespace: &NamespaceId,
        record: &VerifiedRecord,
        now_ms: u64,
    ) -> EventWalResult<AppendOutcome>;
}

impl WalAppend for EventWal {
    fn wal_append(
        &mut self,
        namespace: &NamespaceId,
        record: &VerifiedRecord,
        now_ms: u64,
    ) -> EventWalResult<AppendOutcome> {
        self.append(namespace, record, now_ms)
    }
}

/// Capability: start a WAL index transaction.
///
/// # Laws
/// - The returned transaction has exclusive write intent for its backend semantics.
/// - Callers must eventually `commit` or `rollback` (drop rolls back where supported).
pub(crate) trait WalIndexTxnProvider {
    fn begin_wal_txn(&self) -> Result<Box<dyn WalIndexTxn>, WalIndexError>;
}

impl<T: WalIndex + ?Sized> WalIndexTxnProvider for T {
    fn begin_wal_txn(&self) -> Result<Box<dyn WalIndexTxn>, WalIndexError> {
        self.writer().begin_txn()
    }
}

/// Capability: read contiguous WAL frames for a namespace+origin range.
///
/// # Laws
/// - Frames are contiguous by origin sequence with no gaps.
/// - Frames are ordered ascending by origin sequence.
pub(crate) trait WalReadRange {
    type Error;

    fn read_range(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        from_seq_excl: Seq0,
        max_bytes: usize,
    ) -> Result<Vec<EventFrameV1>, Self::Error>;
}
