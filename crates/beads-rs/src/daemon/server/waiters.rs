use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use crossbeam::channel::Sender;
use tracing::Span;

use super::super::core::ReadScope;
use super::super::executor::DurabilityWait;
use super::super::ipc::Request;
use super::ServerReply;
use crate::core::{NamespaceId, StoreId};
use beads_daemon::remote::RemoteUrl;

pub(super) struct ReadGateWaiter {
    pub(super) request: Request,
    pub(super) respond: Sender<ServerReply>,
    pub(super) repo: PathBuf,
    pub(super) read: ReadScope,
    pub(super) span: Span,
    pub(super) started_at: Instant,
    pub(super) deadline: Instant,
}

pub(super) struct DurabilityWaiter {
    pub(super) respond: Sender<ServerReply>,
    pub(super) wait: DurabilityWait,
    pub(super) span: Span,
    pub(super) started_at: Instant,
    pub(super) deadline: Instant,
}

pub(super) struct CheckpointWaiter {
    pub(super) respond: Sender<ServerReply>,
    pub(super) store_id: StoreId,
    pub(super) namespace: NamespaceId,
    pub(super) min_checkpoint_wall_ms: u64,
    pub(super) groups: Vec<String>,
}

pub(super) enum RequestOutcome {
    Continue,
    Shutdown,
}

pub(super) struct RequestWaiters<'a> {
    pub(super) sync_waiters: &'a mut HashMap<RemoteUrl, Vec<Sender<ServerReply>>>,
    pub(super) checkpoint_waiters: &'a mut Vec<CheckpointWaiter>,
    pub(super) durability_waiters: &'a mut Vec<DurabilityWaiter>,
}
