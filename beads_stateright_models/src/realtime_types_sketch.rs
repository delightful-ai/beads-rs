//! Reference types for the realtime system.
//!
//! This module intentionally re-exports production types and model adapters so
//! Stateright models can stay in lockstep with the real codebase. Do not define
//! toy-only types here; if production changes, compilation failures are the
//! correct signal to update the models.

pub mod core {
    pub use beads_rs::{
        ActorId, Applied, Canonical, CanonicalState, ClientRequestId, DecodeError, DurabilityClass,
        DurabilityProofV1, DurabilityReceipt, EncodeError, ErrorCode, ErrorPayload, EventBody,
        EventBytes, EventFrameError, EventFrameV1, EventId, EventKindV1, EventValidationError,
        HlcMax, Limits, NamespaceId, Opaque, PrevDeferred, PrevVerified, ReplicaId, Seq0, Seq1,
        Sha256, StoreEpoch, StoreId, StoreIdentity, StoreState, TxnDeltaV1, TxnId, TxnV1,
        Watermark, Watermarks,
    };
}

pub mod adapters {
    pub use beads_rs::model::{
        digest, durability, event_factory, repl_ingest, BufferedEventSnapshot,
        BufferedPrevSnapshot, GapBufferByNsOrigin, GapBufferByNsOriginSnapshot, GapBufferSnapshot,
        HeadSnapshot, IngestDecision, MemoryWalIndex, OriginStreamSnapshot, OriginStreamState,
        PeerAckTable, WatermarkSnapshot,
    };
}
