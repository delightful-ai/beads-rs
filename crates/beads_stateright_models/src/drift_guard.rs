//! Compile-time drift guard for production-backed models.
//!
//! If this file fails to compile, it means production APIs changed and the
//! Stateright models are now out of sync. Fix the model adapters first; do not
//! paper over errors or delete this file. Keeping models in lockstep with prod
//! is required for correctness.

#[allow(unused_imports)]
use beads_rs::model::{
    BufferedEventSnapshot, BufferedPrevSnapshot, GapBufferByNsOrigin, GapBufferByNsOriginSnapshot,
    GapBufferSnapshot, HeadSnapshot, IngestDecision, MemoryWalIndex, OriginStreamSnapshot,
    OriginStreamState, PeerAckTable, VerifiedEventAny, WatermarkSnapshot, digest, durability,
    event_factory, repl_ingest,
};
#[allow(unused_imports)]
use beads_rs::{
    CanonicalState, EventBody, EventFrameV1, EventId, EventKindV1, HlcMax, NamespaceId,
    PrevDeferred, PrevVerified, ReplicaId, Seq0, Seq1, Sha256, StoreIdentity, TxnDeltaV1, TxnId,
    TxnV1, VerifiedEvent,
};

#[allow(dead_code, clippy::too_many_arguments)]
fn _drift_guard_examples(
    state: &CanonicalState,
    factory: &event_factory::EventFactory,
    namespace: &NamespaceId,
    origin: ReplicaId,
    seq: Seq1,
    frame: &EventFrameV1,
    store: StoreIdentity,
    limits: &beads_rs::Limits,
    lookup: &dyn beads_rs::EventShaLookup,
) {
    let _digest = digest::canonical_state_canon_json_sha256(state);
    let _event = factory.txn_body(
        seq,
        TxnId::new(origin.as_uuid()),
        0,
        0,
        TxnDeltaV1::new(),
        None,
    );
    let _frame = event_factory::encode_frame(&_event, None);
    let _verified = repl_ingest::verify_frame(frame, limits, store, None, lookup);
    let _ = GapBufferByNsOrigin::new(limits.clone());
    let _ = OriginStreamState::new(
        namespace.clone(),
        origin,
        beads_rs::Watermark::genesis(),
        limits,
    );
    let _ = IngestDecision::DuplicateNoop;
    let _ = PeerAckTable::new();
    let _ = MemoryWalIndex::new();
    let _ = durability::poll_replicated;
    let _ = durability::pending_receipt;
    let _ = durability::achieved_receipt;
    let _: GapBufferByNsOriginSnapshot = GapBufferByNsOriginSnapshot {
        origins: Vec::new(),
    };
    let _: GapBufferSnapshot = GapBufferSnapshot {
        buffered: Vec::new(),
        buffered_bytes: 0,
        started_at_ms: None,
        max_events: 0,
        max_bytes: 0,
        timeout_ms: 0,
    };
    let _: OriginStreamSnapshot = OriginStreamSnapshot {
        namespace: namespace.clone(),
        origin,
        durable: WatermarkSnapshot {
            seq: beads_rs::Seq0::ZERO,
            head: HeadSnapshot::Genesis,
        },
        gap: GapBufferSnapshot {
            buffered: Vec::new(),
            buffered_bytes: 0,
            started_at_ms: None,
            max_events: 0,
            max_bytes: 0,
            timeout_ms: 0,
        },
    };
    let _: BufferedEventSnapshot = BufferedEventSnapshot {
        seq,
        sha256: Sha256([0u8; 32]),
        prev: BufferedPrevSnapshot::Contiguous { prev: None },
        bytes_len: 0,
    };
    let _ = MemoryWalIndex::new().model_snapshot();
}
