//! Explicit daemon test surface for cross-crate integration tests.
//!
//! This module is feature-gated (`test-harness`) so production builds do not
//! expose daemon internals.

use crossbeam::channel::{Receiver, Sender};

pub use crate::clock::Clock;

#[derive(Clone)]
pub struct TestGitTx(Sender<crate::runtime::git_worker::GitOp>);

#[allow(dead_code)]
pub struct TestGitRx(Receiver<crate::runtime::git_worker::GitOp>);

pub fn new_test_git_channel() -> (TestGitTx, TestGitRx) {
    let (tx, rx) = crossbeam::channel::unbounded();
    (TestGitTx(tx), TestGitRx(rx))
}

pub mod core {
    #[cfg(any(test, feature = "test-harness"))]
    pub use crate::runtime::core::insert_store_for_tests;
    pub use crate::runtime::core::{
        Daemon, HandleOutcome, PendingReplayApply, ReplayApplyOutcome, replay_event_wal,
    };
}

pub mod mutation_engine {
    pub use crate::runtime::mutation_engine::PlannedMutation;
}

pub fn handle_request_for_tests(
    daemon: &mut core::Daemon,
    request: ipc::Request,
    git_tx: &TestGitTx,
) -> core::HandleOutcome {
    daemon.handle_request(request, &git_tx.0)
}

impl TestGitTx {
    pub fn clone_for_tests(&self) -> Self {
        self.clone()
    }
}

pub mod durability_coordinator {
    pub use crate::runtime::durability_coordinator::{
        DurabilityCoordinator, DurabilityRequestClaim, ReplicatedDurabilityClaim, ReplicatedPoll,
    };
}

// The E2E harness depends on other helpers that are only available with the
// explicit cross-crate test-harness feature.
#[cfg(feature = "test-harness")]
pub mod e2e;

pub mod durability {
    pub use crate::runtime::durability_coordinator::{
        DurabilityCoordinator, DurabilityRequestClaim, ReplicatedDurabilityClaim, ReplicatedPoll,
    };
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
