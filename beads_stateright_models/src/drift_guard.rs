//! Compile-time drift guard for production-backed models.
//!
//! If this file fails to compile, it means production APIs changed and the
//! Stateright models are now out of sync. Fix the model adapters first; do not
//! paper over errors or delete this file. Keeping models in lockstep with prod
//! is required for correctness.

#[allow(unused_imports)]
use beads_rs::model::{
    GapBufferByNsOrigin, IngestDecision, OriginStreamState, PeerAckTable, VerifiedEventAny,
    digest, durability, event_factory, repl_ingest,
};
#[allow(unused_imports)]
use beads_rs::{
    CanonicalState, EventBody, EventFrameV1, EventId, EventKindV1, HlcMax, NamespaceId,
    PrevDeferred, PrevVerified, ReplicaId, Seq0, Seq1, Sha256, StoreIdentity, TxnDeltaV1, TxnId,
    TxnV1, VerifiedEvent,
};

#[allow(dead_code)]
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
    let _digest = digest::canonical_state_sha(state);
    let _event = factory.txn_body(seq, TxnId::new(origin.as_uuid()), 0, 0, TxnDeltaV1::new(), None);
    let _frame = event_factory::encode_frame(&_event, None);
    let _verified = repl_ingest::verify_frame(frame, limits, store, None, lookup);
    let _ = GapBufferByNsOrigin::new(limits.clone());
    let _ = OriginStreamState::new(namespace.clone(), origin, beads_rs::Watermark::genesis(), limits);
    let _ = IngestDecision::DuplicateNoop;
    let _ = PeerAckTable::new();
    let _ = durability::poll_replicated;
}
