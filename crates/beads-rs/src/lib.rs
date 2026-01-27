#![forbid(unsafe_code)]

// Re-export enum_str! macro from beads-macros for internal use and downstream consumers
pub use beads_macros::enum_str;

// Re-export beads-core as core module for backwards compatibility
pub use beads_core as core;

pub use beads_api as api;
// Optional direct access to surface types.
pub use beads_surface as surface;
#[cfg(feature = "cli")]
pub mod cli;
pub mod compat;
pub mod config;
pub mod daemon;
pub mod error;
pub mod git;
pub mod migrate;
#[cfg(feature = "model-testing")]
pub mod model;
pub mod paths;
pub mod repo;
pub mod telemetry;
#[cfg(feature = "test-harness")]
pub mod test_harness;
pub mod upgrade;

pub use error::{Effect, Error, Transience};
pub type Result<T> = std::result::Result<T, Error>;

// Re-export core types at crate root for convenience
pub use crate::core::{
    ActorId, AcyclicDepKey, Applied, ApplyError, ApplyOutcome, Bead, BeadCore, BeadFields, BeadId,
    BeadType, CanonJsonError, Canonical, CanonicalState, CheckpointGroup, Claim, CliErrorCode,
    ClientRequestId, Closure, DecodeError, DepAddKey, DepKey, DepKind, DurabilityClass,
    DurabilityOutcome, DurabilityProofV1, DurabilityReceipt, Durable, EncodeError, ErrorCode,
    ErrorPayload, EventBody, EventBytes, EventFrameError, EventFrameV1, EventId, EventKindV1,
    EventShaLookup, EventShaLookupError, EventValidationError, FreeDepKey, GcAuthority, HeadStatus,
    HlcMax, Labels, Limits, LocalFsyncProof, Lww, NamespaceId, NamespacePolicies,
    NamespacePoliciesError, NamespacePolicy, NamespaceVisibility, NoCycleProof, Note, NoteId,
    NoteKey, Opaque, PrevDeferred, PrevVerified, Priority, ProtocolErrorCode, ReceiptMergeError,
    ReplicaDurabilityRole, ReplicaDurabilityRoleError, ReplicaEntry, ReplicaId, ReplicaRole,
    ReplicaRoster, ReplicaRosterError, ReplicateMode, ReplicatedProof, RetentionPolicy, SegmentId,
    Seq0, Seq1, Sha256, Stamp, StoreEpoch, StoreId, StoreIdentity, StoreMeta, StoreMetaVersions,
    StoreState, Tombstone, TraceId, TtlBasis, TxnDeltaError, TxnDeltaV1, TxnId, TxnOpKey, TxnOpV1,
    TxnV1, ValidatedBeadPatch, ValidatedDepAdd, ValidatedDepRemove, ValidatedEventBody,
    ValidatedEventKindV1, ValidatedTombstone, ValidatedTxnDeltaV1, ValidatedTxnOpV1,
    ValidatedTxnV1, VerifiedEvent, VerifiedEventAny, WallClock, Watermark, WatermarkError,
    Watermarks, WireBeadFull, WireBeadPatch, WireDepAddV1, WireDepRemoveV1, WireDotV1, WireDvvV1,
    WireFieldStamp, WireLabelAddV1, WireLabelRemoveV1, WireLabelStateV1, WireNoteV1, WirePatch,
    WireStamp, WireTombstoneV1, Workflow, WorkflowStatus, WriteStamp, apply_event,
    decode_event_body, decode_event_hlc_max, encode_event_body_canonical, hash_event_body,
    sha256_bytes, to_canon_json_bytes, verify_event_frame,
};
