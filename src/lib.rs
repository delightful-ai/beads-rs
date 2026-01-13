#![forbid(unsafe_code)]

pub mod api;
#[cfg(feature = "cli")]
pub mod cli;
pub mod compat;
pub mod config;
pub mod core;
pub mod daemon;
pub mod error;
pub mod git;
pub mod migrate;
pub mod paths;
pub mod repo;
pub mod upgrade;

pub use error::{Effect, Error, Transience};
pub type Result<T> = std::result::Result<T, Error>;

// Re-export core types at crate root for convenience
pub use crate::core::{
    ActorId, Applied, ApplyError, ApplyOutcome, Bead, BeadCore, BeadFields, BeadId, BeadType,
    CanonJsonError, Canonical, CanonicalState, CheckpointGroup, Claim, ClientRequestId, Closure,
    DecodeError, DepEdge, DepKey, DepKind, DurabilityClass, DurabilityOutcome, DurabilityProofV1,
    DurabilityReceipt, Durable, EncodeError, ErrorCode, ErrorPayload, EventBody, EventBytes,
    EventFrameError, EventFrameV1, EventId, EventKindV1, EventShaLookup, EventShaLookupError,
    EventValidationError, GcAuthority, HeadStatus, HlcMax, Labels, Limits, LocalFsyncProof, Lww,
    NamespaceId, NamespacePolicy, NamespaceVisibility, Note, NoteId, NoteKey, NoteLog, Opaque,
    PrevDeferred, PrevVerified, Priority, ReceiptMergeError, ReplicaId, ReplicateMode,
    ReplicatedProof, RetentionPolicy, SegmentId, Seq0, Seq1, Sha256, Stamp, StoreEpoch, StoreId,
    StoreIdentity, StoreMeta, StoreMetaVersions, StoreState, Tombstone, TtlBasis, TxnDeltaError,
    TxnDeltaV1, TxnId, TxnOpKey, TxnOpV1, VerifiedEvent, VerifiedEventAny, WallClock, Watermark,
    WatermarkError, Watermarks, WireBeadFull, WireBeadPatch, WireDepDeleteV1, WireDepV1,
    WireFieldStamp, WireNoteV1, WirePatch, WireStamp, WireTombstoneV1, Workflow, WorkflowStatus,
    WriteStamp, apply_event, decode_event_body, encode_event_body_canonical, hash_event_body,
    sha256_bytes, to_canon_json_bytes, validate_event_body_limits, verify_event_frame,
};
