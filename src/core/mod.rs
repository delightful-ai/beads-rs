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

pub mod bead;
pub mod collections;
pub mod composite;
pub mod crdt;
pub mod dep;
pub mod durability;
pub mod domain;
pub mod error;
pub mod identity;
pub mod limits;
pub mod meta;
pub mod namespace;
pub mod store_meta;
pub mod store_state;
pub mod state;
pub mod stores;
pub mod time;
pub mod tombstone;
pub mod watermark;

pub use bead::{Bead, BeadCore, BeadFields};
pub use collections::{Label, Labels, NoteLog};
pub use composite::{Claim, Closure, Note, Workflow};
pub use crdt::Lww;
pub use dep::{DepEdge, DepKey, DepLife, DepSpec};
pub use durability::{
    DurabilityClass, DurabilityOutcome, DurabilityProofV1, LocalFsyncProof, ReplicatedProof,
};
pub use domain::{BeadType, DepKind, Priority};
pub use error::{
    CollisionError, CoreError, ErrorCode, ErrorPayload, InvalidDependency, InvalidId, InvalidLabel,
    RangeError,
};
pub use identity::{
    ActorId, BeadId, BeadSlug, BranchName, ClientRequestId, ContentHash, NoteId, ReplicaId,
    SegmentId, StoreEpoch, StoreId, StoreIdentity, TxnId,
};
pub use limits::Limits;
pub use meta::{FormatVersion, Meta};
pub use namespace::{
    CheckpointGroup, GcAuthority, NamespaceId, NamespacePolicy, NamespaceVisibility, ReplicateMode,
    RetentionPolicy, TtlBasis,
};
pub use store_meta::{StoreMeta, StoreMetaVersions};
pub use store_state::StoreState;
pub use state::{CanonicalState, DepIndexes, LiveLookupError};
pub use stores::{DepStore, TombstoneStore};
pub use time::{Stamp, WallClock, WriteStamp};
pub use tombstone::{Tombstone, TombstoneKey};
pub use watermark::{Applied, Durable, HeadStatus, Seq0, Seq1, Watermark, WatermarkError, Watermarks};
