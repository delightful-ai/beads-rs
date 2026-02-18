//! Coordinator logic: request dispatch, read gating, and scheduling.

mod loaded_store;
mod request;
mod sync;

use std::path::Path;
use std::time::{Duration, Instant};

use crossbeam::channel::Sender;
use uuid::Uuid;

use super::core::{
    Daemon, HandleOutcome, LoadedStore, ParsedMutationMeta, ReadGateStatus, ReadScope,
    detect_clock_skew, max_write_stamp,
};
use super::git_worker::{GitOp, LoadResult};
use super::ipc::{
    AdminOp, ErrorPayload, MutationMeta, Request, Response, ResponseExt, ResponsePayload,
};
use super::ops::OpError;
use crate::api::DaemonInfo as ApiDaemonInfo;
use crate::api::QueryResult;
use crate::core::{
    ActorId, CliErrorCode, DurabilityClass, ErrorCode, NamespaceId, ProtocolErrorCode, TraceId,
    WallClock,
};
use crate::git::{SyncError, SyncOutcome};
use beads_daemon::remote::RemoteUrl;

const REFRESH_TTL: Duration = Duration::from_millis(1000);
const INIT_TIMEOUT_SECS: u64 = 30;

fn init_timeout() -> Duration {
    let override_secs = std::env::var("BD_LOAD_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|v| *v > 0);
    Duration::from_secs(override_secs.unwrap_or(INIT_TIMEOUT_SECS))
}

fn error_payload(code: ErrorCode, message: &str, retryable: bool) -> ErrorPayload {
    ErrorPayload::new(code, message, retryable)
}
