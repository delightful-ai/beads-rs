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
pub mod domain;
pub mod error;
pub mod identity;
pub mod meta;
pub mod state;
pub mod stores;
pub mod time;
pub mod tombstone;

pub use bead::{Bead, BeadCore, BeadFields};
pub use collections::{Label, Labels, NoteLog};
pub use composite::{Claim, Closure, Note, Workflow};
pub use crdt::Lww;
pub use dep::{DepEdge, DepKey, DepLife, DepSpec};
pub use domain::{BeadType, DepKind, Priority};
pub use error::{
    CollisionError, CoreError, InvalidDependency, InvalidId, InvalidLabel, RangeError,
};
pub use identity::{ActorId, BeadId, BeadSlug, BranchName, ContentHash, NoteId};
pub use meta::{FormatVersion, Meta};
pub use state::{CanonicalState, DepIndexes, LiveLookupError};
pub use stores::{DepStore, TombstoneStore};
pub use time::{Stamp, WallClock, WriteStamp};
pub use tombstone::{Tombstone, TombstoneKey};
