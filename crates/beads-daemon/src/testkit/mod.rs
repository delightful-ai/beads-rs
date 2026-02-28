//! Explicit daemon test surface for cross-crate integration tests.
//!
//! This module is feature-gated (`test-harness`) so production builds do not
//! expose daemon internals.

pub use crate::clock::Clock;
pub use crate::runtime::GitOp;

pub mod core {
    pub use crate::runtime::core::{
        Daemon, HandleOutcome, PendingReplayApply, ReplayApplyOutcome, insert_store_for_tests,
        replay_event_wal,
    };
}

pub mod durability_coordinator {
    pub use crate::runtime::durability_coordinator::{DurabilityCoordinator, ReplicatedPoll};
}

pub mod durability {
    pub use crate::runtime::durability_coordinator::{DurabilityCoordinator, ReplicatedPoll};
}

pub mod executor {
    pub use crate::runtime::executor::DurabilityWait;
}

pub mod ipc {
    pub use crate::runtime::ipc::{
        CreatePayload, MutationCtx, MutationMeta, Request, Response, ResponseExt, ResponsePayload,
    };
}

pub mod ops {
    pub use crate::runtime::ops::OpError;
}

pub mod repl {
    pub use crate::runtime::repl::session::{
        Session, SessionAction, SessionConfig, SessionPeer, SessionPhase, SessionStore,
        ValidatedAck,
    };
    pub use crate::runtime::repl::*;
}

pub mod store {
    pub use crate::runtime::store::discovery::*;
    pub use crate::runtime::store::lock::*;
    pub use crate::runtime::store::runtime::*;
}

pub mod wal {
    pub use crate::runtime::wal::*;
}
