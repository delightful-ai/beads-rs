//! Core domain types for beads (Layers 0-9)
//!
//! Module hierarchy follows type dependency order:
//! - time: HLC primitives (Layer 0)
//! - identity: ActorId, BeadId, NoteId (Layer 1)
//! - domain: BeadType, DepKind, Priority (Layer 2)
//! - crdt: Lww<T> (Layer 3)
//! - orset: OrSet (Layer 3)
//! - composite: Note, Claim, Closure, Workflow (Layer 5)
//! - collections: Labels (Layer 4)
//! - dep: DepKey (Layer 6)
//! - tombstone: Tombstone (Layer 7)
//! - bead: BeadCore, BeadFields, Bead (Layer 8)
//! - state: CanonicalState (Layer 9)

#![forbid(unsafe_code)]

// Re-export enum_str! macro from beads-macros for internal use and downstream consumers
pub use beads_macros::enum_str;

pub mod apply;
pub mod bead;
pub mod collections;
pub mod composite;
pub mod crdt;
pub mod dep;
mod dep_tombstone_store;
pub mod domain;
pub mod durability;
pub mod effect;
pub mod error;
pub mod event;
pub mod identity;
pub mod json_canon;
pub mod limits;
pub mod meta;
pub mod namespace;
pub mod namespace_policies;
mod namespaced_state;
pub mod orset;
pub mod replica_roster;
pub mod state;
pub mod store_meta;
pub mod time;
pub mod tombstone;
pub mod watermark;
pub mod wire_bead;

pub use apply::{ApplyError, ApplyOutcome, NoteKey, apply_event};
pub use bead::{Bead, BeadCore, BeadFields, BeadView};
pub use collections::{Label, Labels};
pub use composite::{Claim, Closure, Note, Workflow};
pub use crdt::Lww;
pub use dep::{AcyclicDepKey, DepAddKey, DepKey, DepSpec, FreeDepKey, NoCycleProof};
pub use domain::{BeadType, DepKind, Priority};
pub use durability::{
    DurabilityClass, DurabilityOutcome, DurabilityParseError, DurabilityProofV1, DurabilityReceipt,
    LocalFsyncProof, ReceiptMergeError, ReplicatedProof,
};
pub use effect::{Effect, Transience};
pub use error::{
    CliErrorCode, CollisionError, CoreError, ErrorCode, ErrorPayload, InvalidDependency, InvalidId,
    InvalidLabel, ProtocolErrorCode, RangeError,
};
pub use event::{
    Canonical, DecodeError, EncodeError, EventBody, EventBytes, EventFrameError, EventFrameV1,
    EventKindV1, EventShaLookup, EventShaLookupError, EventValidationError, HlcMax, Opaque,
    PrevDeferred, PrevVerified, Sha256, TxnV1, ValidatedBeadPatch, ValidatedDepAdd,
    ValidatedDepRemove, ValidatedEventBody, ValidatedEventKindV1, ValidatedTombstone,
    ValidatedTxnDeltaV1, ValidatedTxnOpV1, ValidatedTxnV1, VerifiedEvent, VerifiedEventAny,
    VerifiedEventFrame, decode_event_body, decode_event_hlc_max, encode_event_body_canonical,
    hash_event_body, sha256_bytes, verify_event_frame,
};
pub use identity::{
    ActorId, BeadId, BeadSlug, BranchName, ClientRequestId, ContentHash, EventId, NoteId,
    ReplicaId, SegmentId, StoreEpoch, StoreId, StoreIdentity, TraceId, TxnId,
};
pub use json_canon::{CanonJsonError, to_canon_json_bytes};
pub use limits::Limits;
pub use meta::{FormatVersion, Meta};
pub use namespace::{
    CheckpointGroup, GcAuthority, NamespaceId, NamespacePolicy, NamespaceVisibility,
    NonCoreNamespaceId, ReplicateMode, RetentionPolicy, TtlBasis,
};
pub use namespace_policies::{NamespacePolicies, NamespacePoliciesError};
pub use namespaced_state::StoreState;
pub use orset::{Dot, Dvv, OrSet, OrSetChange, OrSetError, OrSetNormalization, OrSetValue};
pub use replica_roster::{
    ReplicaDurabilityRole, ReplicaDurabilityRoleError, ReplicaEntry, ReplicaRole, ReplicaRoster,
    ReplicaRosterError,
};
pub use state::{CanonicalState, DepIndexes, DepStore, LabelStore, LiveLookupError, NoteStore};
pub use store_meta::{StoreMeta, StoreMetaVersions};
pub mod store_state {
    pub use super::namespaced_state::*;
}
pub use dep_tombstone_store::TombstoneStore;
pub mod stores {
    pub use super::dep_tombstone_store::*;
    pub use super::state::{DepStore, LabelStore, NoteStore};
}
pub use time::{Stamp, WallClock, WriteStamp};
pub use tombstone::{Tombstone, TombstoneKey};
pub use watermark::{
    Applied, Durable, HeadStatus, Seq0, Seq1, Watermark, WatermarkError, Watermarks,
};
pub use wire_bead::{
    BeadPatchWireV1, BeadSnapshotWireV1, NoteAppendV1, TxnDeltaError, TxnDeltaV1, TxnOpKey,
    TxnOpV1, WireBeadFull, WireBeadPatch, WireClaimSnapshot, WireDepAddV1, WireDepEntryV1,
    WireDepRemoveV1, WireDepStoreV1, WireDotV1, WireDvvV1, WireFieldStamp, WireLabelAddV1,
    WireLabelRemoveV1, WireLabelStateV1, WireLineageStamp, WireNoteV1, WirePatch, WireStamp,
    WireTombstoneV1, WireWorkflowSnapshot, WorkflowStatus,
};
