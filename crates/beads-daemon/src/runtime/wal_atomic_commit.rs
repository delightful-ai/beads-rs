use super::wal::{WalIndexError, WalIndexTxn, WalIndexTxnProvider};
use crate::core::{
    Applied, Durable, HeadStatus, NamespaceId, ReplicaId, Seq0, Seq1, Watermark, WatermarkPair,
};
use crate::runtime::test_hooks;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AtomicWalCommitPath {
    Mutation,
    ReplIngest,
}

impl AtomicWalCommitPath {
    fn pause_stage(self) -> &'static str {
        match self {
            Self::Mutation => "wal_mutation_before_atomic_commit",
            Self::ReplIngest => "wal_repl_ingest_before_atomic_commit",
        }
    }
}

pub(crate) struct AtomicWalDurabilityTxn {
    txn: Box<dyn WalIndexTxn>,
    namespace: NamespaceId,
    origin: ReplicaId,
    path: AtomicWalCommitPath,
}

impl AtomicWalDurabilityTxn {
    pub(crate) fn begin(
        provider: &(impl WalIndexTxnProvider + ?Sized),
        namespace: NamespaceId,
        origin: ReplicaId,
        path: AtomicWalCommitPath,
    ) -> Result<Self, WalIndexError> {
        let txn = provider.begin_wal_txn()?;
        Ok(Self {
            txn,
            namespace,
            origin,
            path,
        })
    }

    pub(crate) fn index_mut(&mut self) -> &mut dyn WalIndexTxn {
        &mut *self.txn
    }

    pub(crate) fn commit_with_watermarks(
        mut self,
        watermarks: WatermarkPair,
    ) -> Result<(), WalIndexError> {
        self.txn
            .update_watermark(&self.namespace, &self.origin, watermarks)?;
        let stage = self.path.pause_stage();
        test_hooks::maybe_pause(stage);
        test_hooks::maybe_fail_atomic_commit(stage)?;
        self.txn.commit()
    }
}

pub(crate) fn tip_watermark_pair(seq: Seq1, sha: [u8; 32]) -> Result<WatermarkPair, WalIndexError> {
    let tip_seq = Seq0::new(seq.get());
    let tip_head = HeadStatus::Known(sha);
    let applied =
        Watermark::<Applied>::new(tip_seq, tip_head).map_err(watermark_row_decode_error)?;
    let durable =
        Watermark::<Durable>::new(tip_seq, tip_head).map_err(watermark_row_decode_error)?;
    WatermarkPair::new(applied, durable).map_err(watermark_row_decode_error)
}

fn watermark_row_decode_error(err: impl std::fmt::Display) -> WalIndexError {
    WalIndexError::WatermarkRowDecode(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_stage_names_are_stable() {
        assert_eq!(
            AtomicWalCommitPath::Mutation.pause_stage(),
            "wal_mutation_before_atomic_commit"
        );
        assert_eq!(
            AtomicWalCommitPath::ReplIngest.pause_stage(),
            "wal_repl_ingest_before_atomic_commit"
        );
    }

    #[test]
    fn tip_watermark_pair_sets_applied_and_durable_to_tip() {
        let seq = Seq1::from_u64(9).expect("nonzero seq");
        let sha = [42u8; 32];
        let pair = tip_watermark_pair(seq, sha).expect("watermark pair");
        assert_eq!(pair.applied().seq().get(), 9);
        assert_eq!(pair.durable().seq().get(), 9);
        assert_eq!(pair.applied().head(), HeadStatus::Known(sha));
        assert_eq!(pair.durable().head(), HeadStatus::Known(sha));
    }
}
