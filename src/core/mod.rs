//! Core domain types for beads (Layers 0-9)
//!
//! Module hierarchy follows type dependency order:
//! - time: HLC primitives (Layer 0)
//! - identity: ActorId, BeadId, NoteId (Layer 1)
//! - domain: BeadType, DepKind, Priority (Layer 2)
//! - crdt: Lww<T> (Layer 3)
//! - composite: Note, Claim, Closure, Workflow (Layer 5)
//! - collections: Labels, NoteLog (Layer 4)
//! - dep: DepKey, DepEdge (Layer 6)
//! - tombstone: Tombstone (Layer 7)
//! - bead: BeadCore, BeadFields, Bead (Layer 8)
//! - state: CanonicalState (Layer 9)

pub mod apply;
pub mod bead;
pub mod collections;
pub mod composite;
pub mod crdt;
pub mod dep;
pub mod domain;
pub mod durability;
pub mod error;
pub mod event;
pub mod identity;
pub mod json_canon;
pub mod limits;
pub mod meta;
pub mod namespace;
pub mod namespace_policies;
pub mod replica_roster;
pub mod state;
pub mod store_meta;
pub mod store_state;
pub mod stores;
pub mod time;
pub mod tombstone;
pub mod watermark;
pub mod wire_bead;

pub use apply::{ApplyError, ApplyOutcome, NoteKey, apply_event};
pub use bead::{Bead, BeadCore, BeadFields};
pub use collections::{Label, Labels, NoteLog};
pub use composite::{Claim, Closure, Note, Workflow};
pub use crdt::Lww;
pub use dep::{DepEdge, DepKey, DepLife, DepSpec};
pub use domain::{BeadType, DepKind, Priority};
pub use durability::{
    DurabilityClass, DurabilityOutcome, DurabilityParseError, DurabilityProofV1, DurabilityReceipt,
    LocalFsyncProof, ReceiptMergeError, ReplicatedProof,
};
pub use error::{
    CollisionError, CoreError, ErrorCode, ErrorPayload, InvalidDependency, InvalidId, InvalidLabel,
    RangeError,
};
pub use event::{
    Canonical, DecodeError, EncodeError, EventBody, EventBytes, EventFrameError, EventFrameV1,
    EventKindV1, EventShaLookup, EventShaLookupError, EventValidationError, HlcMax, Opaque,
    PrevDeferred, PrevVerified, Sha256, VerifiedEvent, VerifiedEventAny, decode_event_body,
    decode_event_hlc_max, encode_event_body_canonical, hash_event_body, sha256_bytes,
    validate_event_body_limits, verify_event_frame,
};
pub use identity::{
    ActorId, BeadId, BeadSlug, BranchName, ClientRequestId, ContentHash, EventId, NoteId,
    ReplicaId, SegmentId, StoreEpoch, StoreId, StoreIdentity, TxnId,
};
pub use json_canon::{CanonJsonError, to_canon_json_bytes};
pub use limits::Limits;
pub use meta::{FormatVersion, Meta};
pub use namespace::{
    CheckpointGroup, GcAuthority, NamespaceId, NamespacePolicy, NamespaceVisibility, ReplicateMode,
    RetentionPolicy, TtlBasis,
};
pub use namespace_policies::{NamespacePolicies, NamespacePoliciesError};
pub use replica_roster::{ReplicaEntry, ReplicaRole, ReplicaRoster, ReplicaRosterError};
pub use state::{CanonicalState, DepIndexes, LiveLookupError};
pub use store_meta::{StoreMeta, StoreMetaVersions};
pub use store_state::StoreState;
pub use stores::{DepStore, TombstoneStore};
pub use time::{Stamp, WallClock, WriteStamp};
pub use tombstone::{Tombstone, TombstoneKey};
pub use watermark::{
    Applied, Durable, HeadStatus, Seq0, Seq1, Watermark, WatermarkError, Watermarks,
};
pub use wire_bead::{
    NoteAppendV1, NotesPatch, TxnDeltaError, TxnDeltaV1, TxnOpKey, TxnOpV1, WireBeadFull,
    WireBeadPatch, WireDepDeleteV1, WireDepV1, WireFieldStamp, WireNoteV1, WirePatch, WireStamp,
    WireTombstoneV1, WorkflowStatus,
};
