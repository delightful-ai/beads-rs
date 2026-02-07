//! Production-backed helpers for Stateright models.
//!
//! This module is feature-gated (`model-testing`) and intentionally thin: it should
//! only expose adapters around production logic, not re-implement it.

pub mod digest;
pub mod durability;
pub mod event_factory;
pub mod repl_ingest;

pub use crate::core::{
    EventBody, EventFrameV1, EventId, EventKindV1, HlcMax, NamespaceId, PrevDeferred, PrevVerified,
    ReplicaId, Seq0, Seq1, Sha256, StoreIdentity, TxnDeltaV1, TxnId, TxnV1, VerifiedEvent,
    VerifiedEventAny, encode_event_body_canonical, hash_event_body, verify_event_frame,
};
pub use beads_daemon_core::durability::{DurabilityCoordinator, ReplicatedPoll};
pub use beads_daemon_core::repl::{
    BufferedEventSnapshot, BufferedPrevSnapshot, ContiguousBatch, GapBufferByNsOrigin,
    GapBufferByNsOriginSnapshot, GapBufferSnapshot, HeadSnapshot, IngestDecision,
    OriginStreamSnapshot, OriginStreamState, PeerAckTable, WatermarkSnapshot,
};
pub use beads_daemon_core::wal::{
    ClientRequestEventIds, MemoryWalIndex, MemoryWalIndexSnapshot, WalIndex,
};
