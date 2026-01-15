//! IPC protocol types and codec.
//!
//! Protocol: newline-delimited JSON (ndjson) over Unix socket.
//!
//! Request format: `{"op": "create", ...}\n`
//! Response format: `{"ok": ...}\n` or `{"err": {"code": "...", "message": "..."}}\n`

use std::fs;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::ops::{BeadPatch, OpError, OpResult};
use super::query::{Filters, QueryResult};
use super::store_lock::{StoreLockError, StoreLockOperation};
use super::store_runtime::StoreRuntimeError;
use crate::api::{AdminFingerprintMode, AdminFingerprintSample};
use crate::core::error::details as error_details;
use crate::core::{
    Applied, BeadType, DepKind, DurabilityReceipt, InvalidId, Limits, NamespaceId, Priority,
    StoreId, Watermarks,
};
pub use crate::core::{ErrorCode, ErrorPayload};
use crate::daemon::wal::{EventWalError, WalIndexError, WalReplayError};
use crate::error::{Effect, Transience};
use crate::git::error::{SyncError, WireError};

pub const IPC_PROTOCOL_VERSION: u32 = 2;

// =============================================================================
// Request - All IPC requests
// =============================================================================

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MutationMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durability: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReadConsistency {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_min_seen: Option<Watermarks<Applied>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_timeout_ms: Option<u64>,
}

/// IPC request (mutation or query).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    // === Mutations ===
    /// Create a new bead.
    Create {
        repo: PathBuf,
        #[serde(default)]
        id: Option<String>,
        #[serde(default)]
        parent: Option<String>,
        title: String,
        #[serde(rename = "type")]
        bead_type: BeadType,
        priority: Priority,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        design: Option<String>,
        #[serde(default)]
        acceptance_criteria: Option<String>,
        #[serde(default)]
        assignee: Option<String>,
        #[serde(default)]
        external_ref: Option<String>,
        #[serde(default)]
        estimated_minutes: Option<u32>,
        #[serde(default)]
        labels: Vec<String>,
        #[serde(default)]
        dependencies: Vec<String>,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    /// Update an existing bead.
    Update {
        repo: PathBuf,
        id: String,
        patch: BeadPatch,
        #[serde(default)]
        cas: Option<String>,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    /// Add labels to a bead.
    AddLabels {
        repo: PathBuf,
        id: String,
        labels: Vec<String>,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    /// Remove labels from a bead.
    RemoveLabels {
        repo: PathBuf,
        id: String,
        labels: Vec<String>,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    /// Set or clear a parent relationship.
    SetParent {
        repo: PathBuf,
        id: String,
        #[serde(default)]
        parent: Option<String>,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    /// Close a bead.
    Close {
        repo: PathBuf,
        id: String,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        on_branch: Option<String>,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    /// Reopen a closed bead.
    Reopen {
        repo: PathBuf,
        id: String,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    /// Delete a bead (soft delete).
    Delete {
        repo: PathBuf,
        id: String,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    /// Add a dependency.
    AddDep {
        repo: PathBuf,
        from: String,
        to: String,
        kind: DepKind,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    /// Remove a dependency.
    RemoveDep {
        repo: PathBuf,
        from: String,
        to: String,
        kind: DepKind,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    /// Add a note.
    AddNote {
        repo: PathBuf,
        id: String,
        content: String,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    /// Claim a bead.
    Claim {
        repo: PathBuf,
        id: String,
        #[serde(default = "default_lease_secs")]
        lease_secs: u64,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    /// Release a claim.
    Unclaim {
        repo: PathBuf,
        id: String,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    /// Extend a claim.
    ExtendClaim {
        repo: PathBuf,
        id: String,
        lease_secs: u64,
        #[serde(default, flatten)]
        meta: MutationMeta,
    },

    // === Queries ===
    /// Get a single bead.
    Show {
        repo: PathBuf,
        id: String,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Show multiple beads (batch fetch for summaries).
    ShowMultiple {
        repo: PathBuf,
        ids: Vec<String>,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// List beads.
    List {
        repo: PathBuf,
        #[serde(default)]
        filters: Filters,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Get ready beads.
    Ready {
        repo: PathBuf,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Get dependency tree.
    DepTree {
        repo: PathBuf,
        id: String,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Get dependencies.
    Deps {
        repo: PathBuf,
        id: String,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Get notes.
    Notes {
        repo: PathBuf,
        id: String,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Get blocked issues.
    Blocked {
        repo: PathBuf,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Get stale issues.
    Stale {
        repo: PathBuf,
        #[serde(default)]
        days: u32,
        #[serde(default)]
        status: Option<String>,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Count issues matching filters.
    Count {
        repo: PathBuf,
        #[serde(default)]
        filters: Filters,
        #[serde(default)]
        group_by: Option<String>,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Show deleted (tombstoned) issues.
    Deleted {
        repo: PathBuf,
        #[serde(default)]
        since_ms: Option<u64>,
        #[serde(default)]
        id: Option<String>,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Epic completion status.
    EpicStatus {
        repo: PathBuf,
        #[serde(default)]
        eligible_only: bool,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    // === Control ===
    /// Force reload state from git (invalidates cache).
    /// Use after external changes to refs/heads/beads/store (e.g., migration).
    Refresh { repo: PathBuf },

    /// Force sync now.
    Sync { repo: PathBuf },

    /// Wait until repo is clean (debounced sync flushed).
    SyncWait { repo: PathBuf },

    /// Initialize beads ref.
    Init { repo: PathBuf },

    /// Get sync status.
    Status {
        repo: PathBuf,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Admin status snapshot.
    AdminStatus {
        repo: PathBuf,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Admin metrics snapshot.
    AdminMetrics {
        repo: PathBuf,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Admin doctor report.
    AdminDoctor {
        repo: PathBuf,
        #[serde(default, flatten)]
        read: ReadConsistency,
        #[serde(default)]
        max_records_per_namespace: Option<u64>,
        #[serde(default)]
        verify_checkpoint_cache: bool,
    },

    /// Admin scrub now.
    AdminScrub {
        repo: PathBuf,
        #[serde(default, flatten)]
        read: ReadConsistency,
        #[serde(default)]
        max_records_per_namespace: Option<u64>,
        #[serde(default)]
        verify_checkpoint_cache: bool,
    },

    /// Admin fingerprint report.
    AdminFingerprint {
        repo: PathBuf,
        #[serde(default, flatten)]
        read: ReadConsistency,
        mode: AdminFingerprintMode,
        #[serde(default)]
        sample: Option<AdminFingerprintSample>,
    },

    /// Admin reload namespace policies.
    AdminReloadPolicies { repo: PathBuf },

    /// Admin rotate replica id.
    AdminRotateReplicaId { repo: PathBuf },

    /// Admin maintenance mode toggle.
    AdminMaintenanceMode { repo: PathBuf, enabled: bool },

    /// Rebuild WAL index from segments.
    AdminRebuildIndex { repo: PathBuf },

    /// Validate state.
    Validate {
        repo: PathBuf,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Subscribe to realtime events.
    Subscribe {
        repo: PathBuf,
        #[serde(default, flatten)]
        read: ReadConsistency,
    },

    /// Ping (health check).
    Ping,

    /// Shutdown daemon.
    Shutdown,
}

fn default_lease_secs() -> u64 {
    3600 // 1 hour default
}

// =============================================================================
// Response - IPC responses
// =============================================================================

/// IPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum Response {
    Ok { ok: ResponsePayload },
    Err { err: ErrorPayload },
}

impl Response {
    /// Create a success response.
    pub fn ok(payload: ResponsePayload) -> Self {
        Response::Ok { ok: payload }
    }

    /// Create an error response.
    pub fn err(error: impl Into<ErrorPayload>) -> Self {
        Response::Err { err: error.into() }
    }
}

/// Successful response payload.
///
/// Uses untagged serialization for backward compatibility. Unit-like variants
/// use wrapper structs with a `result` field to avoid serializing as `null`,
/// which would be ambiguous during deserialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpResponse {
    #[serde(flatten)]
    pub result: OpResult,
    pub receipt: DurabilityReceipt,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<crate::api::Issue>,
}

impl OpResponse {
    pub fn new(result: OpResult, receipt: DurabilityReceipt) -> Self {
        Self {
            result,
            receipt,
            issue: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum ResponsePayload {
    /// Mutation result.
    Op(OpResponse),

    /// Query result.
    Query(QueryResult),

    /// Sync completed.
    Synced(SyncedPayload),

    /// Refresh completed (state reloaded from git).
    Refreshed(RefreshedPayload),

    /// Init completed.
    Initialized(InitializedPayload),

    /// Shutdown ack.
    ShuttingDown(ShuttingDownPayload),

    /// Subscription ack.
    Subscribed(SubscribedPayload),

    /// Streamed event.
    Event(StreamEventPayload),
}

impl ResponsePayload {
    /// Create a synced payload.
    pub fn synced() -> Self {
        ResponsePayload::Synced(SyncedPayload::default())
    }

    /// Create an initialized payload.
    pub fn initialized() -> Self {
        ResponsePayload::Initialized(InitializedPayload::default())
    }

    /// Create a refreshed payload.
    pub fn refreshed() -> Self {
        ResponsePayload::Refreshed(RefreshedPayload::default())
    }

    /// Create a shutting down payload.
    pub fn shutting_down() -> Self {
        ResponsePayload::ShuttingDown(ShuttingDownPayload::default())
    }

    /// Create a subscribed payload.
    pub fn subscribed(info: crate::api::SubscribeInfo) -> Self {
        ResponsePayload::Subscribed(SubscribedPayload { subscribed: info })
    }

    /// Create a stream event payload.
    pub fn event(event: crate::api::StreamEvent) -> Self {
        ResponsePayload::Event(StreamEventPayload { event })
    }
}

/// Payload for sync completion. Uses typed discriminant for unambiguous deserialization.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SyncedPayload {
    result: SyncedTag,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
enum SyncedTag {
    #[default]
    #[serde(rename = "synced")]
    Synced,
}

/// Payload for refresh completion. Uses typed discriminant for unambiguous deserialization.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RefreshedPayload {
    result: RefreshedTag,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
enum RefreshedTag {
    #[default]
    #[serde(rename = "refreshed")]
    Refreshed,
}

/// Payload for init completion. Uses typed discriminant for unambiguous deserialization.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InitializedPayload {
    result: InitializedTag,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
enum InitializedTag {
    #[default]
    #[serde(rename = "initialized")]
    Initialized,
}

/// Payload for shutdown acknowledgment. Uses typed discriminant for unambiguous deserialization.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShuttingDownPayload {
    result: ShuttingDownTag,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
enum ShuttingDownTag {
    #[default]
    #[serde(rename = "shutting_down")]
    ShuttingDown,
}

/// Payload for subscription acknowledgement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribedPayload {
    pub subscribed: crate::api::SubscribeInfo,
}

/// Payload for streamed events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEventPayload {
    pub event: crate::api::StreamEvent,
}

impl From<OpError> for ErrorPayload {
    fn from(e: OpError) -> Self {
        let message = e.to_string();
        let retryable = e.transience().is_retryable();
        match e {
            OpError::NotFound(id) => ErrorPayload::new(ErrorCode::NotFound, message, retryable)
                .with_details(error_details::NotFoundDetails { id }),
            OpError::AlreadyExists(id) => {
                ErrorPayload::new(ErrorCode::AlreadyExists, message, retryable)
                    .with_details(error_details::AlreadyExistsDetails { id })
            }
            OpError::AlreadyClaimed { by, expires } => {
                let expires_at_ms = expires.map(|value| value.0);
                ErrorPayload::new(ErrorCode::AlreadyClaimed, message, retryable)
                    .with_details(error_details::AlreadyClaimedDetails { by, expires_at_ms })
            }
            OpError::CasMismatch { expected, actual } => {
                ErrorPayload::new(ErrorCode::CasMismatch, message, retryable)
                    .with_details(error_details::CasMismatchDetails { expected, actual })
            }
            OpError::InvalidTransition { from, to } => {
                ErrorPayload::new(ErrorCode::InvalidTransition, message, retryable)
                    .with_details(error_details::InvalidTransitionDetails { from, to })
            }
            OpError::ValidationFailed { field, reason } => {
                ErrorPayload::new(ErrorCode::ValidationFailed, message, retryable)
                    .with_details(error_details::ValidationFailedDetails { field, reason })
            }
            OpError::InvalidRequest { field, reason } => {
                ErrorPayload::new(ErrorCode::InvalidRequest, message, retryable).with_details(
                    error_details::InvalidRequestDetails {
                        field,
                        reason: Some(reason),
                    },
                )
            }
            OpError::Overloaded {
                subsystem,
                retry_after_ms,
                queue_bytes,
                queue_events,
            } => ErrorPayload::new(ErrorCode::Overloaded, message, retryable).with_details(
                error_details::OverloadedDetails {
                    subsystem: Some(subsystem),
                    retry_after_ms,
                    queue_bytes,
                    queue_events,
                },
            ),
            OpError::RateLimited {
                retry_after_ms,
                limit_bytes_per_sec,
            } => ErrorPayload::new(ErrorCode::RateLimited, message, retryable).with_details(
                error_details::RateLimitedDetails {
                    retry_after_ms,
                    limit_bytes_per_sec,
                },
            ),
            OpError::MaintenanceMode { reason } => {
                ErrorPayload::new(ErrorCode::MaintenanceMode, message, retryable).with_details(
                    error_details::MaintenanceModeDetails {
                        reason,
                        until_ms: None,
                    },
                )
            }
            OpError::ClientRequestIdReuseMismatch {
                namespace,
                client_request_id,
                expected_request_sha256,
                got_request_sha256,
            } => ErrorPayload::new(ErrorCode::ClientRequestIdReuseMismatch, message, retryable)
                .with_details(error_details::ClientRequestIdReuseMismatchDetails {
                    namespace,
                    client_request_id,
                    expected_request_sha256: hex::encode(expected_request_sha256.as_ref()),
                    got_request_sha256: hex::encode(got_request_sha256.as_ref()),
                }),
            OpError::NotAGitRepo(path) => {
                ErrorPayload::new(ErrorCode::NotAGitRepo, message, retryable).with_details(
                    error_details::PathDetails {
                        path: path.display().to_string(),
                    },
                )
            }
            OpError::NoRemote(path) => ErrorPayload::new(ErrorCode::NoRemote, message, retryable)
                .with_details(error_details::PathDetails {
                    path: path.display().to_string(),
                }),
            OpError::RepoNotInitialized(path) => {
                ErrorPayload::new(ErrorCode::RepoNotInitialized, message, retryable).with_details(
                    error_details::PathDetails {
                        path: path.display().to_string(),
                    },
                )
            }
            OpError::Sync(err) => match err.as_ref() {
                SyncError::Wire(WireError::ChecksumMismatch {
                    blob,
                    expected,
                    actual,
                }) => ErrorPayload::new(ErrorCode::Corruption, message, retryable).with_details(
                    error_details::StoreChecksumMismatchDetails {
                        blob: (*blob).to_string(),
                        expected_sha256: expected.to_hex(),
                        got_sha256: actual.to_hex(),
                    },
                ),
                _ => ErrorPayload::new(ErrorCode::SyncFailed, message, retryable),
            },
            OpError::BeadDeleted(id) => {
                ErrorPayload::new(ErrorCode::BeadDeleted, message, retryable)
                    .with_details(error_details::BeadDeletedDetails { id })
            }
            OpError::NoteTooLarge {
                max_bytes,
                got_bytes,
            } => ErrorPayload::new(ErrorCode::NoteTooLarge, message, retryable).with_details(
                error_details::NoteTooLargeDetails {
                    max_note_bytes: max_bytes as u64,
                    got_bytes: got_bytes as u64,
                },
            ),
            OpError::OpsTooMany { max_ops, got_ops } => {
                ErrorPayload::new(ErrorCode::OpsTooMany, message, retryable).with_details(
                    error_details::OpsTooManyDetails {
                        max_ops_per_txn: max_ops as u64,
                        got_ops: got_ops as u64,
                    },
                )
            }
            OpError::LabelsTooMany {
                max_labels,
                got_labels,
                bead_id,
            } => ErrorPayload::new(ErrorCode::LabelsTooMany, message, retryable).with_details(
                error_details::LabelsTooManyDetails {
                    max_labels_per_bead: max_labels as u64,
                    got_labels: got_labels as u64,
                    bead_id: bead_id.map(|id| id.as_str().to_string()),
                },
            ),
            OpError::WalRecordTooLarge {
                max_wal_record_bytes,
                estimated_bytes,
            } => ErrorPayload::new(ErrorCode::WalRecordTooLarge, message, retryable).with_details(
                error_details::WalRecordTooLargeDetails {
                    max_wal_record_bytes: max_wal_record_bytes as u64,
                    estimated_bytes: estimated_bytes as u64,
                },
            ),
            OpError::DurabilityUnavailable {
                requested,
                eligible_total,
                eligible_replica_ids,
            } => ErrorPayload::new(ErrorCode::DurabilityUnavailable, message, retryable)
                .with_details(error_details::DurabilityUnavailableDetails {
                    requested,
                    eligible_total,
                    eligible_replica_ids,
                }),
            OpError::DurabilityTimeout {
                requested,
                waited_ms,
                pending_replica_ids,
                receipt,
            } => ErrorPayload::new(ErrorCode::DurabilityTimeout, message, retryable)
                .with_details(error_details::DurabilityTimeoutDetails {
                    requested,
                    waited_ms,
                    pending_replica_ids,
                })
                .with_receipt(receipt),
            OpError::RequireMinSeenTimeout {
                waited_ms,
                required,
                current_applied,
            } => ErrorPayload::new(ErrorCode::RequireMinSeenTimeout, message, retryable)
                .with_details(error_details::RequireMinSeenTimeoutDetails {
                    waited_ms,
                    required: required.as_ref().clone(),
                    current_applied: current_applied.as_ref().clone(),
                }),
            OpError::RequireMinSeenUnsatisfied {
                required,
                current_applied,
            } => ErrorPayload::new(ErrorCode::RequireMinSeenUnsatisfied, message, retryable)
                .with_details(error_details::RequireMinSeenUnsatisfiedDetails {
                    required: required.as_ref().clone(),
                    current_applied: current_applied.as_ref().clone(),
                }),
            OpError::NamespaceInvalid { namespace, .. } => {
                ErrorPayload::new(ErrorCode::NamespaceInvalid, message, retryable).with_details(
                    error_details::NamespaceInvalidDetails {
                        namespace,
                        pattern: "[a-z][a-z0-9_]{0,31}".to_string(),
                    },
                )
            }
            OpError::NamespaceUnknown { namespace } => {
                ErrorPayload::new(ErrorCode::NamespaceUnknown, message, retryable)
                    .with_details(error_details::NamespaceUnknownDetails { namespace })
            }
            OpError::NamespacePolicyViolation {
                namespace,
                rule,
                reason,
            } => ErrorPayload::new(ErrorCode::NamespacePolicyViolation, message, retryable)
                .with_details(error_details::NamespacePolicyViolationDetails {
                    namespace,
                    rule,
                    reason,
                }),
            OpError::CrossNamespaceDependency {
                from_namespace,
                to_namespace,
            } => ErrorPayload::new(ErrorCode::CrossNamespaceDependency, message, retryable)
                .with_details(error_details::CrossNamespaceDependencyDetails {
                    from_namespace,
                    to_namespace,
                }),
            OpError::Wal(e) => match e.as_ref() {
                crate::daemon::wal::WalError::TooLarge {
                    max_bytes,
                    got_bytes,
                } => ErrorPayload::new(ErrorCode::WalRecordTooLarge, message, retryable)
                    .with_details(error_details::WalRecordTooLargeDetails {
                        max_wal_record_bytes: *max_bytes as u64,
                        estimated_bytes: *got_bytes as u64,
                    }),
                _ => ErrorPayload::new(ErrorCode::WalError, message, retryable).with_details(
                    error_details::WalErrorDetails {
                        message: e.to_string(),
                    },
                ),
            },
            OpError::EventWal(e) => match e.as_ref() {
                EventWalError::RecordTooLarge {
                    max_bytes,
                    got_bytes,
                } => ErrorPayload::new(ErrorCode::WalRecordTooLarge, message, retryable)
                    .with_details(error_details::WalRecordTooLargeDetails {
                        max_wal_record_bytes: *max_bytes as u64,
                        estimated_bytes: *got_bytes as u64,
                    }),
                _ => ErrorPayload::new(event_wal_error_code(e.as_ref()), message, retryable)
                    .with_details(error_details::WalErrorDetails {
                        message: e.to_string(),
                    }),
            },
            OpError::WalMerge { errors } => {
                let errors = errors.iter().map(|err| err.to_string()).collect();
                ErrorPayload::new(ErrorCode::WalMergeConflict, message, retryable)
                    .with_details(error_details::WalMergeConflictDetails { errors })
            }
            OpError::NotClaimedByYou => {
                ErrorPayload::new(ErrorCode::NotClaimedByYou, message, retryable)
            }
            OpError::DepNotFound => ErrorPayload::new(ErrorCode::DepNotFound, message, retryable),
            OpError::LoadTimeout {
                repo,
                timeout_secs,
                remote,
            } => ErrorPayload::new(ErrorCode::LoadTimeout, message, retryable).with_details(
                error_details::LoadTimeoutDetails {
                    repo: repo.display().to_string(),
                    timeout_secs,
                    remote,
                },
            ),
            OpError::StoreRuntime(err) => {
                store_runtime_error_payload(err.as_ref(), message, retryable)
            }
            OpError::Internal(_) => ErrorPayload::new(ErrorCode::Internal, message, retryable),
        }
    }
}

fn store_runtime_error_payload(
    err: &StoreRuntimeError,
    message: String,
    retryable: bool,
) -> ErrorPayload {
    match err {
        StoreRuntimeError::Lock(lock_err) => store_lock_error_payload(lock_err, message, retryable),
        StoreRuntimeError::MetaSymlink { path } => {
            ErrorPayload::new(ErrorCode::PathSymlinkRejected, message, retryable).with_details(
                error_details::PathSymlinkRejectedDetails {
                    path: path.display().to_string(),
                },
            )
        }
        StoreRuntimeError::MetaRead { path, source } => match source.kind() {
            std::io::ErrorKind::PermissionDenied => {
                ErrorPayload::new(ErrorCode::PermissionDenied, message, retryable).with_details(
                    error_details::PermissionDeniedDetails {
                        path: path.display().to_string(),
                        operation: error_details::PermissionOperation::Read,
                    },
                )
            }
            _ => ErrorPayload::new(ErrorCode::InternalError, message, retryable),
        },
        StoreRuntimeError::MetaParse { source, .. } => {
            ErrorPayload::new(ErrorCode::Corruption, message, retryable).with_details(
                error_details::CorruptionDetails {
                    reason: source.to_string(),
                },
            )
        }
        StoreRuntimeError::MetaMismatch { expected, got } => {
            ErrorPayload::new(ErrorCode::WrongStore, message, retryable).with_details(
                error_details::WrongStoreDetails {
                    expected_store_id: *expected,
                    got_store_id: *got,
                },
            )
        }
        StoreRuntimeError::MetaWrite { path, source } => match source.kind() {
            std::io::ErrorKind::PermissionDenied => {
                ErrorPayload::new(ErrorCode::PermissionDenied, message, retryable).with_details(
                    error_details::PermissionDeniedDetails {
                        path: path.display().to_string(),
                        operation: error_details::PermissionOperation::Write,
                    },
                )
            }
            _ => ErrorPayload::new(ErrorCode::InternalError, message, retryable),
        },
        StoreRuntimeError::NamespacePoliciesRead { path, source } => match source.kind() {
            std::io::ErrorKind::PermissionDenied => {
                ErrorPayload::new(ErrorCode::PermissionDenied, message, retryable).with_details(
                    error_details::PermissionDeniedDetails {
                        path: path.display().to_string(),
                        operation: error_details::PermissionOperation::Read,
                    },
                )
            }
            _ => ErrorPayload::new(ErrorCode::ValidationFailed, message, retryable).with_details(
                error_details::ValidationFailedDetails {
                    field: "namespaces".to_string(),
                    reason: format!("failed to read {}: {source}", path.display()),
                },
            ),
        },
        StoreRuntimeError::NamespacePoliciesParse { source, .. } => {
            ErrorPayload::new(ErrorCode::ValidationFailed, message, retryable).with_details(
                error_details::ValidationFailedDetails {
                    field: "namespaces".to_string(),
                    reason: source.to_string(),
                },
            )
        }
        StoreRuntimeError::WatermarkInvalid {
            kind,
            namespace,
            origin,
            source,
        } => ErrorPayload::new(ErrorCode::IndexCorrupt, message, retryable).with_details(
            error_details::IndexCorruptDetails {
                reason: format!("{kind} watermark for {namespace} {origin}: {source}"),
            },
        ),
        StoreRuntimeError::WalIndex(err) => wal_index_error_payload(err, message, retryable),
        StoreRuntimeError::WalReplay(err) => wal_replay_error_payload(err, message, retryable),
    }
}

fn wal_replay_error_payload(
    err: &WalReplayError,
    message: String,
    retryable: bool,
) -> ErrorPayload {
    match err {
        WalReplayError::RecordShaMismatch(info) => {
            let info = info.as_ref();
            ErrorPayload::new(ErrorCode::HashMismatch, message, retryable).with_details(
                error_details::HashMismatchDetails {
                    eid: error_details::EventIdDetails {
                        namespace: info.namespace.clone(),
                        origin_replica_id: info.origin,
                        origin_seq: info.seq,
                    },
                    expected_sha256: hex::encode(info.expected),
                    got_sha256: hex::encode(info.got),
                },
            )
        }
        WalReplayError::PrevShaMismatch {
            namespace,
            origin,
            seq,
            expected_prev_sha256,
            got_prev_sha256,
            head_seq,
        } => {
            let namespace = match NamespaceId::parse(namespace.clone()) {
                Ok(ns) => ns,
                Err(_) => return ErrorPayload::new(wal_replay_error_code(err), message, retryable),
            };
            ErrorPayload::new(ErrorCode::PrevShaMismatch, message, retryable).with_details(
                error_details::PrevShaMismatchDetails {
                    eid: error_details::EventIdDetails {
                        namespace,
                        origin_replica_id: *origin,
                        origin_seq: *seq,
                    },
                    expected_prev_sha256: hex::encode(expected_prev_sha256),
                    got_prev_sha256: hex::encode(got_prev_sha256),
                    head_seq: *head_seq,
                },
            )
        }
        WalReplayError::NonContiguousSeq {
            namespace,
            origin,
            expected,
            got,
        } => {
            let namespace = match NamespaceId::parse(namespace.clone()) {
                Ok(ns) => ns,
                Err(_) => return ErrorPayload::new(wal_replay_error_code(err), message, retryable),
            };
            let durable_seen = expected.saturating_sub(1);
            ErrorPayload::new(ErrorCode::GapDetected, message, retryable).with_details(
                error_details::GapDetectedDetails {
                    namespace,
                    origin_replica_id: *origin,
                    durable_seen,
                    got_seq: *got,
                },
            )
        }
        WalReplayError::IndexOffsetInvalid { .. } | WalReplayError::OriginSeqOverflow { .. } => {
            ErrorPayload::new(ErrorCode::IndexCorrupt, message, retryable).with_details(
                error_details::IndexCorruptDetails {
                    reason: err.to_string(),
                },
            )
        }
        WalReplayError::SegmentHeader {
            source: EventWalError::SegmentHeaderUnsupportedVersion { got, supported },
            ..
        } => ErrorPayload::new(ErrorCode::WalFormatUnsupported, message, retryable).with_details(
            error_details::WalFormatUnsupportedDetails {
                wal_format_version: *got,
                supported: vec![*supported],
            },
        ),
        WalReplayError::SegmentHeader { .. } => {
            ErrorPayload::new(wal_replay_error_code(err), message, retryable)
        }
        WalReplayError::Index(err) => wal_index_error_payload(err, message, retryable),
        _ => ErrorPayload::new(wal_replay_error_code(err), message, retryable),
    }
}

fn wal_index_error_payload(err: &WalIndexError, message: String, retryable: bool) -> ErrorPayload {
    match err {
        WalIndexError::SchemaVersionMismatch { expected, got } => ErrorPayload::new(
            ErrorCode::IndexRebuildRequired,
            message,
            retryable,
        )
        .with_details(error_details::IndexRebuildRequiredDetails {
            namespace: None,
            reason: format!("index schema version mismatch: expected {expected}, got {got}"),
        }),
        WalIndexError::Equivocation {
            namespace,
            origin,
            seq,
            existing_sha256,
            new_sha256,
        } => ErrorPayload::new(ErrorCode::Equivocation, message, retryable).with_details(
            error_details::EquivocationDetails {
                eid: error_details::EventIdDetails {
                    namespace: namespace.clone(),
                    origin_replica_id: *origin,
                    origin_seq: *seq,
                },
                existing_sha256: hex::encode(existing_sha256),
                new_sha256: hex::encode(new_sha256),
            },
        ),
        WalIndexError::ClientRequestIdReuseMismatch {
            namespace,
            client_request_id,
            expected_request_sha256,
            got_request_sha256,
            ..
        } => ErrorPayload::new(ErrorCode::ClientRequestIdReuseMismatch, message, retryable)
            .with_details(error_details::ClientRequestIdReuseMismatchDetails {
                namespace: namespace.clone(),
                client_request_id: *client_request_id,
                expected_request_sha256: hex::encode(expected_request_sha256),
                got_request_sha256: hex::encode(got_request_sha256),
            }),
        WalIndexError::MetaMismatch {
            key: "store_id",
            expected,
            got,
            ..
        } => {
            let expected = StoreId::parse_str(expected).ok();
            let got = StoreId::parse_str(got).ok();
            if let (Some(expected), Some(got)) = (expected, got) {
                ErrorPayload::new(ErrorCode::WrongStore, message, retryable).with_details(
                    error_details::WrongStoreDetails {
                        expected_store_id: expected,
                        got_store_id: got,
                    },
                )
            } else {
                ErrorPayload::new(ErrorCode::IndexCorrupt, message, retryable).with_details(
                    error_details::IndexCorruptDetails {
                        reason: err.to_string(),
                    },
                )
            }
        }
        WalIndexError::MetaMismatch {
            key: "store_epoch",
            expected,
            got,
            store_id,
        } => {
            let expected_epoch = expected.parse::<u64>().ok();
            let got_epoch = got.parse::<u64>().ok();
            if let (Some(expected_epoch), Some(got_epoch)) = (expected_epoch, got_epoch) {
                ErrorPayload::new(ErrorCode::StoreEpochMismatch, message, retryable).with_details(
                    error_details::StoreEpochMismatchDetails {
                        store_id: *store_id,
                        expected_epoch,
                        got_epoch,
                    },
                )
            } else {
                ErrorPayload::new(ErrorCode::IndexCorrupt, message, retryable).with_details(
                    error_details::IndexCorruptDetails {
                        reason: err.to_string(),
                    },
                )
            }
        }
        WalIndexError::MetaMismatch { .. }
        | WalIndexError::MetaMissing { .. }
        | WalIndexError::EventIdDecode(_)
        | WalIndexError::HlcRowDecode(_)
        | WalIndexError::SegmentRowDecode(_)
        | WalIndexError::WatermarkRowDecode(_)
        | WalIndexError::CborDecode(_)
        | WalIndexError::CborEncode(_)
        | WalIndexError::OriginSeqOverflow { .. }
        | WalIndexError::Sqlite(_) => {
            ErrorPayload::new(ErrorCode::IndexCorrupt, message, retryable).with_details(
                error_details::IndexCorruptDetails {
                    reason: err.to_string(),
                },
            )
        }
        _ => ErrorPayload::new(wal_index_error_code(err), message, retryable),
    }
}

fn wal_index_error_code(err: &WalIndexError) -> ErrorCode {
    match err {
        WalIndexError::SchemaVersionMismatch { .. } => ErrorCode::IndexRebuildRequired,
        WalIndexError::Equivocation { .. } => ErrorCode::Equivocation,
        WalIndexError::ClientRequestIdReuseMismatch { .. } => {
            ErrorCode::ClientRequestIdReuseMismatch
        }
        WalIndexError::MetaMismatch { key, .. } => match *key {
            "store_id" => ErrorCode::WrongStore,
            "store_epoch" => ErrorCode::StoreEpochMismatch,
            _ => ErrorCode::IndexCorrupt,
        },
        WalIndexError::MetaMissing { .. }
        | WalIndexError::EventIdDecode(_)
        | WalIndexError::HlcRowDecode(_)
        | WalIndexError::SegmentRowDecode(_)
        | WalIndexError::WatermarkRowDecode(_)
        | WalIndexError::CborDecode(_)
        | WalIndexError::CborEncode(_)
        | WalIndexError::OriginSeqOverflow { .. } => ErrorCode::IndexCorrupt,
        WalIndexError::Sqlite(_) => ErrorCode::IndexCorrupt,
        WalIndexError::Io { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ErrorCode::PermissionDenied
            } else {
                ErrorCode::IoError
            }
        }
    }
}

fn wal_replay_error_code(err: &WalReplayError) -> ErrorCode {
    match err {
        WalReplayError::Io { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ErrorCode::PermissionDenied
            } else {
                ErrorCode::IoError
            }
        }
        WalReplayError::SegmentHeader { source, .. } => wal_segment_header_error_code(source),
        WalReplayError::SegmentHeaderMismatch { .. } => ErrorCode::SegmentHeaderMismatch,
        WalReplayError::RecordShaMismatch(_) => ErrorCode::HashMismatch,
        WalReplayError::RecordDecode { .. }
        | WalReplayError::EventBodyDecode { .. }
        | WalReplayError::RecordHeaderMismatch { .. }
        | WalReplayError::MissingHead { .. }
        | WalReplayError::UnexpectedHead { .. }
        | WalReplayError::SealedSegmentFinalLenMissing { .. }
        | WalReplayError::SealedSegmentLenMismatch { .. }
        | WalReplayError::MidFileCorruption { .. } => ErrorCode::WalCorrupt,
        WalReplayError::NonContiguousSeq { .. } => ErrorCode::GapDetected,
        WalReplayError::PrevShaMismatch { .. } => ErrorCode::PrevShaMismatch,
        WalReplayError::IndexOffsetInvalid { .. } | WalReplayError::OriginSeqOverflow { .. } => {
            ErrorCode::IndexCorrupt
        }
        WalReplayError::Index(err) => wal_index_error_code(err),
    }
}

fn event_wal_error_code(err: &EventWalError) -> ErrorCode {
    match err {
        EventWalError::RecordTooLarge { .. } => ErrorCode::WalRecordTooLarge,
        EventWalError::SegmentHeaderUnsupportedVersion { .. } => wal_segment_header_error_code(err),
        EventWalError::Io { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ErrorCode::PermissionDenied
            } else {
                ErrorCode::IoError
            }
        }
        _ => ErrorCode::WalCorrupt,
    }
}

fn wal_segment_header_error_code(source: &EventWalError) -> ErrorCode {
    match source {
        EventWalError::SegmentHeaderUnsupportedVersion { .. } => ErrorCode::WalFormatUnsupported,
        _ => ErrorCode::WalCorrupt,
    }
}

fn store_lock_error_payload(
    err: &StoreLockError,
    message: String,
    retryable: bool,
) -> ErrorPayload {
    match err {
        StoreLockError::Held { store_id, meta, .. } => {
            let (holder_pid, holder_replica_id, started_at_ms, daemon_version) = meta
                .as_deref()
                .map(|meta| {
                    (
                        Some(meta.pid),
                        Some(meta.replica_id),
                        Some(meta.started_at_ms),
                        Some(meta.daemon_version.clone()),
                    )
                })
                .unwrap_or((None, None, None, None));
            ErrorPayload::new(ErrorCode::LockHeld, message, retryable).with_details(
                error_details::LockHeldDetails {
                    store_id: *store_id,
                    holder_pid,
                    holder_replica_id,
                    started_at_ms,
                    daemon_version,
                },
            )
        }
        StoreLockError::Symlink { path } => {
            ErrorPayload::new(ErrorCode::PathSymlinkRejected, message, retryable).with_details(
                error_details::PathSymlinkRejectedDetails {
                    path: path.display().to_string(),
                },
            )
        }
        StoreLockError::MetadataCorrupt { source, .. } => {
            ErrorPayload::new(ErrorCode::Corruption, message, retryable).with_details(
                error_details::CorruptionDetails {
                    reason: source.to_string(),
                },
            )
        }
        StoreLockError::Io {
            path,
            operation,
            source,
        } => match source.kind() {
            std::io::ErrorKind::PermissionDenied => {
                ErrorPayload::new(ErrorCode::PermissionDenied, message, retryable).with_details(
                    error_details::PermissionDeniedDetails {
                        path: path.display().to_string(),
                        operation: lock_permission_operation(*operation),
                    },
                )
            }
            _ => ErrorPayload::new(ErrorCode::InternalError, message, retryable),
        },
    }
}

fn lock_permission_operation(operation: StoreLockOperation) -> error_details::PermissionOperation {
    match operation {
        StoreLockOperation::Read => error_details::PermissionOperation::Read,
        StoreLockOperation::Write => error_details::PermissionOperation::Write,
        StoreLockOperation::Fsync => error_details::PermissionOperation::Fsync,
    }
}

impl From<IpcError> for ErrorPayload {
    fn from(e: IpcError) -> Self {
        let message = e.to_string();
        let retryable = e.transience().is_retryable();
        match e {
            IpcError::Parse(err) => {
                ErrorPayload::new(ErrorCode::MalformedPayload, message, retryable).with_details(
                    error_details::MalformedPayloadDetails {
                        parser: error_details::ParserKind::Json,
                        reason: Some(err.to_string()),
                    },
                )
            }
            IpcError::InvalidRequest { field, reason } => {
                ErrorPayload::new(ErrorCode::InvalidRequest, message, retryable).with_details(
                    error_details::InvalidRequestDetails {
                        field,
                        reason: Some(reason),
                    },
                )
            }
            IpcError::Io(_) => ErrorPayload::new(ErrorCode::IoError, message, retryable),
            IpcError::InvalidId(err) => ErrorPayload::new(ErrorCode::InvalidId, message, retryable)
                .with_details(invalid_id_details(&err)),
            IpcError::Disconnected => {
                ErrorPayload::new(ErrorCode::Disconnected, message, retryable)
            }
            IpcError::DaemonUnavailable(_) => {
                ErrorPayload::new(ErrorCode::DaemonUnavailable, message, retryable)
            }
            IpcError::DaemonVersionMismatch { .. } => {
                ErrorPayload::new(ErrorCode::DaemonVersionMismatch, message, retryable)
            }
            IpcError::FrameTooLarge {
                max_bytes,
                got_bytes,
            } => ErrorPayload::new(ErrorCode::FrameTooLarge, message, retryable).with_details(
                error_details::FrameTooLargeDetails {
                    max_frame_bytes: max_bytes as u64,
                    got_bytes: got_bytes as u64,
                },
            ),
        }
    }
}

fn invalid_id_details(err: &InvalidId) -> error_details::InvalidIdDetails {
    match err {
        InvalidId::Bead { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::Bead,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::Actor { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::Actor,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::Note { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::Note,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::Branch { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::Branch,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::ContentHash { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::ContentHash,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::Namespace { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::Namespace,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::StoreId { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::StoreId,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::ReplicaId { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::ReplicaId,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::TxnId { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::TxnId,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::ClientRequestId { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::ClientRequestId,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::SegmentId { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::SegmentId,
            raw: raw.clone(),
            reason: reason.clone(),
        },
    }
}

// =============================================================================
// IpcError
// =============================================================================

/// IPC-specific errors.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum IpcError {
    #[error("parse error: {0}")]
    Parse(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    InvalidId(#[from] InvalidId),

    #[error("invalid request: {reason}")]
    InvalidRequest {
        field: Option<String>,
        reason: String,
    },

    #[error("client disconnected")]
    Disconnected,

    #[error("daemon unavailable: {0}")]
    DaemonUnavailable(String),

    #[error("daemon version mismatch; restart the daemon and retry")]
    DaemonVersionMismatch {
        daemon: Option<crate::api::DaemonInfo>,
        client_version: String,
        protocol_version: u32,
        /// If set, the mismatch was detected via a parse failure.
        parse_error: Option<String>,
    },

    #[error("frame too large: max {max_bytes} bytes, got {got_bytes} bytes")]
    FrameTooLarge { max_bytes: usize, got_bytes: usize },
}

impl IpcError {
    pub fn code(&self) -> ErrorCode {
        match self {
            IpcError::Parse(_) => ErrorCode::MalformedPayload,
            IpcError::Io(_) => ErrorCode::IoError,
            IpcError::InvalidId(_) => ErrorCode::InvalidId,
            IpcError::InvalidRequest { .. } => ErrorCode::InvalidRequest,
            IpcError::Disconnected => ErrorCode::Disconnected,
            IpcError::DaemonUnavailable(_) => ErrorCode::DaemonUnavailable,
            IpcError::DaemonVersionMismatch { .. } => ErrorCode::DaemonVersionMismatch,
            IpcError::FrameTooLarge { .. } => ErrorCode::FrameTooLarge,
        }
    }

    /// Whether retrying the IPC operation may succeed.
    pub fn transience(&self) -> Transience {
        match self {
            IpcError::DaemonUnavailable(_) | IpcError::Io(_) | IpcError::Disconnected => {
                Transience::Retryable
            }
            IpcError::DaemonVersionMismatch { .. } => Transience::Retryable,
            IpcError::Parse(_)
            | IpcError::InvalidId(_)
            | IpcError::InvalidRequest { .. }
            | IpcError::FrameTooLarge { .. } => Transience::Permanent,
        }
    }

    /// What we know about side effects when this IPC error is returned.
    pub fn effect(&self) -> Effect {
        match self {
            IpcError::Io(_) | IpcError::Disconnected => Effect::Unknown,
            IpcError::DaemonUnavailable(_)
            | IpcError::Parse(_)
            | IpcError::InvalidId(_)
            | IpcError::InvalidRequest { .. } => Effect::None,
            IpcError::DaemonVersionMismatch { .. } => Effect::None,
            IpcError::FrameTooLarge { .. } => Effect::None,
        }
    }
}

// =============================================================================
// Codec - Encoding/decoding
// =============================================================================

/// Encode a response to bytes.
pub fn encode_response(resp: &Response) -> Result<Vec<u8>, IpcError> {
    let mut bytes = serde_json::to_vec(resp)?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Decode a request from a line, enforcing limits.
pub fn decode_request_with_limits(line: &str, limits: &Limits) -> Result<Request, IpcError> {
    let got_bytes = line.len();
    if got_bytes > limits.max_frame_bytes {
        return Err(IpcError::FrameTooLarge {
            max_bytes: limits.max_frame_bytes,
            got_bytes,
        });
    }
    let value: serde_json::Value = serde_json::from_str(line)?;
    serde_json::from_value(value).map_err(|err| IpcError::InvalidRequest {
        field: None,
        reason: err.to_string(),
    })
}

/// Decode a request from a line using default limits.
pub fn decode_request(line: &str) -> Result<Request, IpcError> {
    decode_request_with_limits(line, &Limits::default())
}

/// Send a response over a stream.
pub fn send_response(stream: &mut UnixStream, resp: &Response) -> Result<(), IpcError> {
    let bytes = encode_response(resp)?;
    stream.write_all(&bytes)?;
    Ok(())
}

/// Read requests from a stream.
pub fn read_requests(stream: UnixStream) -> impl Iterator<Item = Result<Request, IpcError>> {
    let reader = BufReader::new(stream);
    reader.lines().map(|line| {
        let line = line?;
        decode_request_with_limits(&line, &Limits::default())
    })
}

// =============================================================================
// Socket path
// =============================================================================

/// Get the directory that will contain the daemon socket.
pub fn socket_dir() -> PathBuf {
    socket_dir_candidates()
        .into_iter()
        .next()
        .unwrap_or_else(per_user_tmp_dir)
}

/// Ensure the socket directory exists and is user-private.
pub fn ensure_socket_dir() -> Result<PathBuf, IpcError> {
    let mut last_err: Option<std::io::Error> = None;
    for dir in socket_dir_candidates() {
        match fs::create_dir_all(&dir) {
            Ok(()) => {
                #[cfg(unix)]
                {
                    let mode = fs::metadata(&dir)?.permissions().mode() & 0o777;
                    if mode != 0o700 {
                        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
                    }
                }
                return Ok(dir);
            }
            Err(e) => last_err = Some(e),
        }
    }

    Err(IpcError::Io(last_err.unwrap_or_else(|| {
        std::io::Error::other("unable to create a writable socket directory")
    })))
}

/// Get the daemon socket path.
pub fn socket_path() -> PathBuf {
    ensure_socket_dir()
        .map(|dir| dir.join("daemon.sock"))
        .unwrap_or_else(|_| per_user_tmp_dir().join("daemon.sock"))
}

/// Build a daemon socket path for a specific runtime directory.
pub fn socket_path_for_runtime_dir(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("beads").join("daemon.sock")
}

/// Read daemon metadata from the meta file in the socket directory.
/// Returns None if file doesn't exist or is corrupt.
fn read_daemon_meta_at(socket: &Path) -> Option<crate::api::DaemonInfo> {
    let meta_path = socket.with_file_name("daemon.meta.json");
    let contents = fs::read_to_string(&meta_path).ok()?;
    serde_json::from_str(&contents).ok()
}

/// Read daemon metadata from the default socket directory.
fn read_daemon_meta() -> Option<crate::api::DaemonInfo> {
    let dir = ensure_socket_dir().ok()?;
    read_daemon_meta_at(&dir.join("daemon.sock"))
}

fn daemon_pid_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), None).is_ok()
}

fn per_user_tmp_dir() -> PathBuf {
    let uid = nix::unistd::geteuid();
    PathBuf::from("/tmp").join(format!("beads-{}", uid))
}

fn socket_dir_candidates() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR")
        && !dir.trim().is_empty()
    {
        dirs.push(PathBuf::from(dir).join("beads"));
    }
    if let Ok(home) = std::env::var("HOME")
        && !home.trim().is_empty()
    {
        dirs.push(PathBuf::from(home).join(".beads"));
    }
    dirs.push(per_user_tmp_dir());
    dirs
}

// =============================================================================
// Client - Send requests to daemon
// =============================================================================

#[derive(Clone, Debug)]
pub struct IpcClient {
    socket: PathBuf,
    autostart: bool,
}

impl IpcClient {
    pub fn new() -> Self {
        Self {
            socket: socket_path(),
            autostart: true,
        }
    }

    pub fn for_socket_path(socket: PathBuf) -> Self {
        Self {
            socket,
            autostart: true,
        }
    }

    pub fn for_runtime_dir(runtime_dir: &Path) -> Self {
        Self::for_socket_path(socket_path_for_runtime_dir(runtime_dir))
    }

    pub fn with_autostart(mut self, autostart: bool) -> Self {
        self.autostart = autostart;
        self
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket
    }

    pub fn send_request(&self, req: &Request) -> Result<Response, IpcError> {
        if self.autostart {
            send_request_at(&self.socket, req)
        } else {
            send_request_no_autostart_at(&self.socket, req)
        }
    }

    pub fn send_request_no_autostart(&self, req: &Request) -> Result<Response, IpcError> {
        send_request_no_autostart_at(&self.socket, req)
    }

    pub fn subscribe_stream(&self, req: &Request) -> Result<SubscriptionStream, IpcError> {
        if self.autostart {
            subscribe_stream_at(&self.socket, req)
        } else {
            subscribe_stream_no_autostart_at(&self.socket, req)
        }
    }

    pub fn wait_for_daemon_ready(&self, expected_version: &str) -> Result<(), IpcError> {
        wait_for_daemon_ready_at(&self.socket, expected_version)
    }
}

impl Default for IpcClient {
    fn default() -> Self {
        Self::new()
    }
}

fn should_autostart(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
    )
}

fn maybe_remove_stale_lock(lock_path: &PathBuf) {
    if let Ok(meta) = fs::metadata(lock_path)
        && let Ok(modified) = meta.modified()
        && let Ok(age) = modified.elapsed()
        && age > Duration::from_secs(10)
    {
        let _ = fs::remove_file(lock_path);
    }
}

fn daemon_command() -> Command {
    if let Ok(exe) = std::env::current_exe() {
        let mut cmd = Command::new(exe);
        cmd.arg("daemon").arg("run");
        return cmd;
    }

    let mut cmd = Command::new("bd");
    cmd.arg("daemon").arg("run");
    cmd
}

fn connect_with_autostart(socket: &PathBuf) -> Result<UnixStream, IpcError> {
    match UnixStream::connect(socket) {
        Ok(stream) => Ok(stream),
        Err(e) if should_autostart(&e) => {
            // Try to autostart daemon with a simple lock to avoid herds.
            let dir = ensure_socket_dir()?;
            let lock_path = dir.join("daemon.lock");
            maybe_remove_stale_lock(&lock_path);

            let mut we_spawned = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
                .is_ok();

            if we_spawned {
                let mut cmd = daemon_command();
                cmd.stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null());
                cmd.spawn().map_err(|e| {
                    IpcError::DaemonUnavailable(format!("failed to spawn daemon: {}", e))
                })?;
            }

            let deadline = SystemTime::now() + Duration::from_secs(30);
            let mut backoff = Duration::from_millis(50);

            loop {
                match UnixStream::connect(socket) {
                    Ok(stream) => {
                        if we_spawned {
                            let _ = fs::remove_file(&lock_path);
                        }
                        return Ok(stream);
                    }
                    Err(e) if should_autostart(&e) => {
                        if !we_spawned {
                            // If the lock disappeared (spawner died), try to take over.
                            maybe_remove_stale_lock(&lock_path);
                            if OpenOptions::new()
                                .write(true)
                                .create_new(true)
                                .open(&lock_path)
                                .is_ok()
                            {
                                we_spawned = true;
                                let mut cmd = daemon_command();
                                cmd.stdin(Stdio::null())
                                    .stdout(Stdio::null())
                                    .stderr(Stdio::null());
                                if let Err(e) = cmd.spawn() {
                                    let _ = fs::remove_file(&lock_path);
                                    return Err(IpcError::DaemonUnavailable(format!(
                                        "failed to spawn daemon: {}",
                                        e
                                    )));
                                }
                            }
                        }
                        if SystemTime::now() >= deadline {
                            if we_spawned {
                                let _ = fs::remove_file(&lock_path);
                            }
                            return Err(IpcError::DaemonUnavailable(
                                "timed out waiting for daemon socket".into(),
                            ));
                        }
                        std::thread::sleep(backoff);
                        backoff = std::cmp::min(backoff * 2, Duration::from_millis(200));
                    }
                    Err(e) => {
                        if we_spawned {
                            let _ = fs::remove_file(&lock_path);
                        }
                        return Err(IpcError::Io(e));
                    }
                }
            }
        }
        Err(e) => Err(IpcError::Io(e)),
    }
}

fn write_req_line(stream: &mut UnixStream, req: &Request) -> Result<(), IpcError> {
    let mut json = serde_json::to_string(req)?;
    json.push('\n');
    stream.write_all(json.as_bytes())?;
    Ok(())
}

fn read_resp_line(reader: &mut BufReader<UnixStream>) -> Result<Response, IpcError> {
    let mut line = String::new();
    let bytes_read = reader.read_line(&mut line)?;
    // EOF means daemon closed connection (likely just shut down)
    if bytes_read == 0 || line.trim().is_empty() {
        return Err(IpcError::DaemonUnavailable(
            "daemon not running (stale socket)".into(),
        ));
    }
    Ok(serde_json::from_str(&line)?)
}

/// Read response line, converting parse errors to version mismatch.
///
/// Used during version verification where a parse failure likely indicates
/// an incompatible daemon version.
fn read_resp_line_version_check(reader: &mut BufReader<UnixStream>) -> Result<Response, IpcError> {
    let mut line = String::new();
    let bytes_read = reader.read_line(&mut line)?;
    if bytes_read == 0 || line.trim().is_empty() {
        return Err(IpcError::DaemonUnavailable(
            "daemon not running (stale socket)".into(),
        ));
    }
    serde_json::from_str(&line).map_err(|e| IpcError::DaemonVersionMismatch {
        daemon: None,
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: IPC_PROTOCOL_VERSION,
        parse_error: Some(e.to_string()),
    })
}

fn verify_daemon_version(
    socket: &Path,
    writer: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
) -> Result<(), IpcError> {
    if let Some(meta) = read_daemon_meta_at(socket)
        && daemon_pid_alive(meta.pid)
    {
        if meta.protocol_version == IPC_PROTOCOL_VERSION
            && meta.version == env!("CARGO_PKG_VERSION")
        {
            return Ok(());
        }
        return Err(IpcError::DaemonVersionMismatch {
            daemon: Some(meta),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            parse_error: None,
        });
    }

    write_req_line(writer, &Request::Ping)?;
    // Use version-check variant that converts parse errors to version mismatch
    let resp = read_resp_line_version_check(reader)?;
    let Response::Ok { ok } = resp else {
        return Err(IpcError::DaemonVersionMismatch {
            daemon: None,
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            parse_error: None,
        });
    };

    let ResponsePayload::Query(QueryResult::DaemonInfo(info)) = ok else {
        return Err(IpcError::DaemonVersionMismatch {
            daemon: None,
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            parse_error: Some("unexpected response payload type".into()),
        });
    };

    if info.protocol_version != IPC_PROTOCOL_VERSION || info.version != env!("CARGO_PKG_VERSION") {
        return Err(IpcError::DaemonVersionMismatch {
            daemon: Some(info),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            parse_error: None,
        });
    }

    Ok(())
}

fn send_request_over_stream(
    stream: UnixStream,
    socket: &Path,
    req: &Request,
) -> Result<Response, IpcError> {
    let mut writer = stream;
    let reader_stream = writer.try_clone()?;
    let mut reader = BufReader::new(reader_stream);

    // Verify daemon version/protocol once per connection.
    if !matches!(req, Request::Ping) {
        verify_daemon_version(socket, &mut writer, &mut reader)?;
    }

    write_req_line(&mut writer, req)?;
    read_resp_line(&mut reader)
}

/// Send a request to the daemon and receive a response.
///
/// Retries up to 3 times on version mismatch or stale socket errors,
/// with exponential backoff between attempts.
pub fn send_request_at(socket: &PathBuf, req: &Request) -> Result<Response, IpcError> {
    const MAX_ATTEMPTS: u32 = 3;

    for attempt in 1..=MAX_ATTEMPTS {
        let stream = match connect_with_autostart(socket) {
            Ok(s) => s,
            Err(e) => {
                if attempt >= MAX_ATTEMPTS {
                    return Err(e);
                }
                let backoff = Duration::from_millis(100 * (1 << (attempt - 1)));
                std::thread::sleep(backoff);
                continue;
            }
        };

        match send_request_over_stream(stream, socket, req) {
            Ok(resp) => return Ok(resp),
            Err(IpcError::DaemonVersionMismatch { daemon, .. }) if attempt < MAX_ATTEMPTS => {
                tracing::info!(
                    "daemon version mismatch, restarting (attempt {}/{})",
                    attempt,
                    MAX_ATTEMPTS
                );

                // Try to restart the daemon
                if let Some(info) = daemon {
                    let _ = kill_daemon_forcefully(info.pid, socket);
                } else {
                    let _ = try_restart_daemon_by_socket(socket);
                }

                // Exponential backoff: 100ms, 200ms, 400ms
                let backoff = Duration::from_millis(100 * (1 << (attempt - 1)));
                std::thread::sleep(backoff);
            }
            Err(IpcError::DaemonUnavailable(ref msg)) if attempt < MAX_ATTEMPTS => {
                tracing::debug!("daemon unavailable ({}), retrying", msg);
                // Socket might have gone stale mid-request
                let _ = try_restart_daemon_by_socket(socket);
                let backoff = Duration::from_millis(100 * (1 << (attempt - 1)));
                std::thread::sleep(backoff);
            }
            Err(e) => return Err(e),
        }
    }

    Err(IpcError::DaemonUnavailable(
        "max retry attempts exceeded".into(),
    ))
}

pub fn send_request(req: &Request) -> Result<Response, IpcError> {
    let socket = socket_path();
    send_request_at(&socket, req)
}

/// Send a request without auto-starting the daemon.
///
/// Returns `DaemonUnavailable` if daemon is not running.
pub fn send_request_no_autostart_at(socket: &PathBuf, req: &Request) -> Result<Response, IpcError> {
    let stream = UnixStream::connect(socket)
        .map_err(|e| IpcError::DaemonUnavailable(format!("daemon not running: {}", e)))?;
    send_request_over_stream(stream, socket, req)
}

pub fn send_request_no_autostart(req: &Request) -> Result<Response, IpcError> {
    let socket = socket_path();
    send_request_no_autostart_at(&socket, req)
}

/// Stream responses for a subscribe request.
pub struct SubscriptionStream {
    _writer: UnixStream,
    reader: BufReader<UnixStream>,
}

impl SubscriptionStream {
    pub fn read_response(&mut self) -> Result<Option<Response>, IpcError> {
        let mut line = String::new();
        let bytes_read = self.reader.read_line(&mut line)?;
        if bytes_read == 0 || line.trim().is_empty() {
            return Ok(None);
        }
        let response = serde_json::from_str(&line)?;
        Ok(Some(response))
    }
}

/// Send a subscribe request and return a stream of responses.
pub fn subscribe_stream_at(
    socket: &PathBuf,
    req: &Request,
) -> Result<SubscriptionStream, IpcError> {
    if !matches!(req, Request::Subscribe { .. }) {
        return Err(IpcError::InvalidRequest {
            field: Some("op".into()),
            reason: "subscribe_stream expects subscribe request".into(),
        });
    }

    const MAX_ATTEMPTS: u32 = 3;
    for attempt in 1..=MAX_ATTEMPTS {
        let stream = match connect_with_autostart(socket) {
            Ok(s) => s,
            Err(e) => {
                if attempt >= MAX_ATTEMPTS {
                    return Err(e);
                }
                let backoff = Duration::from_millis(100 * (1 << (attempt - 1)));
                std::thread::sleep(backoff);
                continue;
            }
        };

        let mut writer = stream;
        let reader_stream = writer.try_clone()?;
        let mut reader = BufReader::new(reader_stream);

        if let Err(err) = verify_daemon_version(socket, &mut writer, &mut reader) {
            match err {
                IpcError::DaemonVersionMismatch { daemon, .. } if attempt < MAX_ATTEMPTS => {
                    tracing::info!(
                        "daemon version mismatch, restarting (attempt {}/{})",
                        attempt,
                        MAX_ATTEMPTS
                    );
                    if let Some(info) = daemon {
                        let _ = kill_daemon_forcefully(info.pid, socket);
                    } else {
                        let _ = try_restart_daemon_by_socket(socket);
                    }
                    let backoff = Duration::from_millis(100 * (1 << (attempt - 1)));
                    std::thread::sleep(backoff);
                    continue;
                }
                _ => return Err(err),
            }
        }

        write_req_line(&mut writer, req)?;
        return Ok(SubscriptionStream {
            _writer: writer,
            reader,
        });
    }

    Err(IpcError::DaemonUnavailable(
        "max retry attempts exceeded".into(),
    ))
}

/// Send a subscribe request without auto-starting the daemon.
pub fn subscribe_stream_no_autostart_at(
    socket: &PathBuf,
    req: &Request,
) -> Result<SubscriptionStream, IpcError> {
    if !matches!(req, Request::Subscribe { .. }) {
        return Err(IpcError::InvalidRequest {
            field: Some("op".into()),
            reason: "subscribe_stream expects subscribe request".into(),
        });
    }

    let stream = UnixStream::connect(socket)
        .map_err(|e| IpcError::DaemonUnavailable(format!("daemon not running: {}", e)))?;
    let mut writer = stream;
    let reader_stream = writer.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    verify_daemon_version(socket, &mut writer, &mut reader)?;
    write_req_line(&mut writer, req)?;
    Ok(SubscriptionStream {
        _writer: writer,
        reader,
    })
}

/// Send a subscribe request and return a stream of responses.
pub fn subscribe_stream(req: &Request) -> Result<SubscriptionStream, IpcError> {
    let socket = socket_path();
    subscribe_stream_at(&socket, req)
}

/// Wait for daemon to be ready and responding with expected version.
///
/// Returns Ok if daemon is responsive with matching version, Err on timeout (30s).
pub fn wait_for_daemon_ready(expected_version: &str) -> Result<(), IpcError> {
    let socket = socket_path();
    wait_for_daemon_ready_at(&socket, expected_version)
}

/// Wait for daemon to be ready and responding with expected version.
pub fn wait_for_daemon_ready_at(socket: &PathBuf, expected_version: &str) -> Result<(), IpcError> {
    let deadline = SystemTime::now() + Duration::from_secs(30);
    let mut backoff = Duration::from_millis(50);

    while SystemTime::now() < deadline {
        match UnixStream::connect(socket) {
            Ok(stream) => {
                let mut writer = stream;
                let reader_stream = match writer.try_clone() {
                    Ok(s) => s,
                    Err(_) => {
                        std::thread::sleep(backoff);
                        backoff = std::cmp::min(backoff * 2, Duration::from_millis(500));
                        continue;
                    }
                };
                let mut reader = BufReader::new(reader_stream);

                if write_req_line(&mut writer, &Request::Ping).is_err() {
                    std::thread::sleep(backoff);
                    backoff = std::cmp::min(backoff * 2, Duration::from_millis(500));
                    continue;
                }

                if let Ok(Response::Ok {
                    ok: ResponsePayload::Query(QueryResult::DaemonInfo(info)),
                }) = read_resp_line(&mut reader)
                {
                    if info.version == expected_version {
                        tracing::info!("daemon ready with version {}", expected_version);
                        return Ok(());
                    }
                    // Wrong version - old daemon hasn't fully died yet
                    tracing::debug!(
                        "daemon has version {}, waiting for {}",
                        info.version,
                        expected_version
                    );
                }
                std::thread::sleep(backoff);
                backoff = std::cmp::min(backoff * 2, Duration::from_millis(500));
            }
            Err(_) => {
                std::thread::sleep(backoff);
                backoff = std::cmp::min(backoff * 2, Duration::from_millis(200));
            }
        }
    }

    Err(IpcError::DaemonUnavailable(format!(
        "timed out waiting for daemon version {}",
        expected_version
    )))
}

/// Kill daemon with SIGTERM, escalating to SIGKILL if needed.
fn kill_daemon_forcefully(pid: u32, socket: &PathBuf) -> Result<(), IpcError> {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let nix_pid = Pid::from_raw(pid as i32);

    // First try SIGTERM (graceful)
    if let Err(e) = kill(nix_pid, Signal::SIGTERM) {
        // ESRCH = no such process - already dead, that's fine
        if e == nix::errno::Errno::ESRCH {
            let _ = fs::remove_file(socket);
            let _ = fs::remove_file(socket.with_file_name("daemon.meta.json"));
            return Ok(());
        }
        return Err(IpcError::DaemonUnavailable(format!(
            "failed to stop daemon pid {pid}: {e}"
        )));
    }

    // Wait for graceful shutdown (3 seconds)
    let deadline = SystemTime::now() + Duration::from_secs(3);
    while SystemTime::now() < deadline {
        if UnixStream::connect(socket).is_err() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Escalate to SIGKILL
    tracing::warn!(
        "daemon pid {} did not stop gracefully, sending SIGKILL",
        pid
    );
    if let Err(e) = kill(nix_pid, Signal::SIGKILL)
        && e != nix::errno::Errno::ESRCH
    {
        return Err(IpcError::DaemonUnavailable(format!(
            "failed to kill daemon pid {pid}: {e}"
        )));
    }

    // Wait for socket to become stale (2 more seconds)
    let deadline = SystemTime::now() + Duration::from_secs(2);
    while SystemTime::now() < deadline {
        if UnixStream::connect(socket).is_err() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Force remove socket and meta as last resort
    let _ = fs::remove_file(socket);
    let _ = fs::remove_file(socket.with_file_name("daemon.meta.json"));
    Ok(())
}

/// Try to restart daemon when we don't have the PID from response.
///
/// Uses daemon.meta.json to find PID if available.
fn try_restart_daemon_by_socket(socket: &PathBuf) -> Result<(), IpcError> {
    // Try to get PID from meta file first
    if let Some(meta) = read_daemon_meta() {
        tracing::debug!("found daemon pid {} from meta file", meta.pid);
        return kill_daemon_forcefully(meta.pid, socket);
    }

    // No meta file (very old daemon or corrupt state)
    tracing::warn!("no daemon meta file found, removing stale socket");

    // Best effort: remove socket file. The orphaned daemon will eventually
    // exit when it has no clients and no work.
    if let Err(e) = fs::remove_file(socket)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(IpcError::DaemonUnavailable(format!(
            "failed to remove stale socket: {e}"
        )));
    }

    // Also remove meta file if present
    let _ = fs::remove_file(socket.with_file_name("daemon.meta.json"));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        DurabilityClass, DurabilityReceipt, NamespaceId, ReplicaId, StoreEpoch, StoreId,
        StoreIdentity, TxnId,
    };
    use crate::daemon::store_lock::{StoreLockError, StoreLockOperation};
    use crate::daemon::wal::{EventWalError, RecordShaMismatchInfo, WalIndexError, WalReplayError};
    use std::io;
    use uuid::Uuid;

    #[test]
    fn request_roundtrip() {
        let req = Request::Create {
            repo: PathBuf::from("/test"),
            id: None,
            parent: None,
            title: "test".to_string(),
            bead_type: BeadType::Task,
            priority: Priority::default(),
            description: None,
            design: None,
            acceptance_criteria: None,
            assignee: None,
            external_ref: None,
            estimated_minutes: None,
            labels: Vec::new(),
            dependencies: Vec::new(),
            meta: MutationMeta::default(),
        };

        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();

        match parsed {
            Request::Create { title, .. } => assert_eq!(title, "test"),
            _ => panic!("wrong request type"),
        }
    }

    #[test]
    fn show_multiple_roundtrip() {
        let req = Request::ShowMultiple {
            repo: PathBuf::from("/test"),
            ids: vec!["bd-abc".to_string(), "bd-xyz".to_string()],
            read: ReadConsistency::default(),
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("show_multiple"));
        assert!(json.contains("bd-abc"));

        let parsed: Request = serde_json::from_str(&json).unwrap();
        match parsed {
            Request::ShowMultiple { ids, .. } => {
                assert_eq!(ids.len(), 2);
                assert_eq!(ids[0], "bd-abc");
            }
            _ => panic!("wrong request type"),
        }
    }

    #[test]
    fn subscribe_roundtrip() {
        let req = Request::Subscribe {
            repo: PathBuf::from("/test"),
            read: ReadConsistency {
                namespace: Some("core".to_string()),
                require_min_seen: None,
                wait_timeout_ms: Some(50),
            },
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("subscribe"));

        let parsed: Request = serde_json::from_str(&json).unwrap();
        match parsed {
            Request::Subscribe { read, .. } => {
                assert_eq!(read.namespace.as_deref(), Some("core"));
                assert_eq!(read.wait_timeout_ms, Some(50));
            }
            _ => panic!("wrong request type"),
        }
    }

    #[test]
    fn response_ok() {
        let resp = Response::ok(ResponsePayload::synced());
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"ok\""));
        // Synced now serializes to {"result":"synced"}, not null
        assert!(json.contains("\"result\":\"synced\""));
    }

    #[test]
    fn unit_variants_are_distinguishable() {
        // Each variant must serialize to a distinct, non-null value with result field
        let synced = serde_json::to_string(&ResponsePayload::synced()).unwrap();
        let initialized = serde_json::to_string(&ResponsePayload::initialized()).unwrap();
        let shutting_down = serde_json::to_string(&ResponsePayload::shutting_down()).unwrap();

        assert!(synced.contains("\"result\":\"synced\""));
        assert!(initialized.contains("\"result\":\"initialized\""));
        assert!(shutting_down.contains("\"result\":\"shutting_down\""));

        // None serialize as null
        assert!(!synced.contains("null"));
        assert!(!initialized.contains("null"));
        assert!(!shutting_down.contains("null"));

        // All are distinct
        assert_ne!(synced, initialized);
        assert_ne!(synced, shutting_down);
        assert_ne!(initialized, shutting_down);
    }

    #[test]
    fn unit_variants_roundtrip() {
        // Verify each variant can be deserialized back correctly
        let synced_json = serde_json::to_string(&ResponsePayload::synced()).unwrap();
        let parsed: ResponsePayload = serde_json::from_str(&synced_json).unwrap();
        assert!(matches!(parsed, ResponsePayload::Synced(_)));

        let init_json = serde_json::to_string(&ResponsePayload::initialized()).unwrap();
        let parsed: ResponsePayload = serde_json::from_str(&init_json).unwrap();
        assert!(matches!(parsed, ResponsePayload::Initialized(_)));
    }

    #[test]
    fn response_err() {
        let resp = Response::err(ErrorPayload::new(
            ErrorCode::NotFound,
            "bead not found",
            false,
        ));
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"err\""));
        assert!(json.contains("not_found"));
    }

    #[test]
    fn wal_replay_hash_mismatch_includes_details() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
        let err = WalReplayError::RecordShaMismatch(Box::new(RecordShaMismatchInfo {
            namespace: namespace.clone(),
            origin,
            seq: 7,
            expected: [3u8; 32],
            got: [4u8; 32],
            path: PathBuf::from("/tmp/segment.wal"),
            offset: 12,
        }));

        let store_err = StoreRuntimeError::WalReplay(Box::new(err));
        let payload = store_runtime_error_payload(&store_err, "boom".to_string(), false);

        assert_eq!(payload.code, ErrorCode::HashMismatch);
        let details = payload
            .details_as::<error_details::HashMismatchDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.eid.namespace, namespace);
        assert_eq!(details.eid.origin_replica_id, origin);
        assert_eq!(details.eid.origin_seq, 7);
        assert_eq!(details.expected_sha256, hex::encode([3u8; 32]));
        assert_eq!(details.got_sha256, hex::encode([4u8; 32]));
    }

    #[test]
    fn wal_replay_format_unsupported_includes_details() {
        let err = WalReplayError::SegmentHeader {
            path: PathBuf::from("/tmp/segment.wal"),
            source: EventWalError::SegmentHeaderUnsupportedVersion {
                got: 3,
                supported: 2,
            },
        };

        let store_err = StoreRuntimeError::WalReplay(Box::new(err));
        let payload = store_runtime_error_payload(&store_err, "boom".to_string(), false);

        assert_eq!(payload.code, ErrorCode::WalFormatUnsupported);
        let details = payload
            .details_as::<error_details::WalFormatUnsupportedDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.wal_format_version, 3);
        assert_eq!(details.supported, vec![2]);
    }

    #[test]
    fn wal_replay_header_decode_is_corrupt() {
        let err = WalReplayError::SegmentHeader {
            path: PathBuf::from("/tmp/segment.wal"),
            source: EventWalError::SegmentHeaderMagicMismatch { got: [0u8; 5] },
        };

        let store_err = StoreRuntimeError::WalReplay(Box::new(err));
        let payload = store_runtime_error_payload(&store_err, "boom".to_string(), false);

        assert_eq!(payload.code, ErrorCode::WalCorrupt);
    }

    #[test]
    fn wal_index_equivocation_includes_details() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
        let err = WalIndexError::Equivocation {
            namespace: namespace.clone(),
            origin,
            seq: 7,
            existing_sha256: [1u8; 32],
            new_sha256: [2u8; 32],
        };

        let store_err = StoreRuntimeError::WalIndex(err);
        let payload = store_runtime_error_payload(&store_err, "boom".to_string(), false);

        assert_eq!(payload.code, ErrorCode::Equivocation);
        let details = payload
            .details_as::<error_details::EquivocationDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.eid.namespace, namespace);
        assert_eq!(details.eid.origin_replica_id, origin);
        assert_eq!(details.eid.origin_seq, 7);
        assert_eq!(details.existing_sha256, hex::encode([1u8; 32]));
        assert_eq!(details.new_sha256, hex::encode([2u8; 32]));
    }

    #[test]
    fn wal_index_store_epoch_mismatch_includes_details() {
        let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
        let err = WalIndexError::MetaMismatch {
            key: "store_epoch",
            expected: "1".to_string(),
            got: "2".to_string(),
            store_id,
        };

        let store_err = StoreRuntimeError::WalIndex(err);
        let payload = store_runtime_error_payload(&store_err, "boom".to_string(), false);

        assert_eq!(payload.code, ErrorCode::StoreEpochMismatch);
        let details = payload
            .details_as::<error_details::StoreEpochMismatchDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.store_id, store_id);
        assert_eq!(details.expected_epoch, 1);
        assert_eq!(details.got_epoch, 2);
    }

    #[test]
    fn wal_replay_prev_sha_mismatch_includes_details() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([8u8; 16]));
        let err = WalReplayError::PrevShaMismatch {
            namespace: namespace.to_string(),
            origin,
            seq: 2,
            expected_prev_sha256: [3u8; 32],
            got_prev_sha256: [4u8; 32],
            head_seq: 1,
        };

        let store_err = StoreRuntimeError::WalReplay(Box::new(err));
        let payload = store_runtime_error_payload(&store_err, "boom".to_string(), false);

        assert_eq!(payload.code, ErrorCode::PrevShaMismatch);
        let details = payload
            .details_as::<error_details::PrevShaMismatchDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.eid.namespace, namespace);
        assert_eq!(details.eid.origin_replica_id, origin);
        assert_eq!(details.eid.origin_seq, 2);
        assert_eq!(details.expected_prev_sha256, hex::encode([3u8; 32]));
        assert_eq!(details.got_prev_sha256, hex::encode([4u8; 32]));
        assert_eq!(details.head_seq, 1);
    }

    #[test]
    fn lock_permission_denied_includes_details() {
        let err = StoreLockError::Io {
            path: PathBuf::from("/tmp/beads.lock"),
            operation: StoreLockOperation::Write,
            source: io::Error::from(io::ErrorKind::PermissionDenied),
        };

        let payload = store_lock_error_payload(&err, "boom".to_string(), false);

        assert_eq!(payload.code, ErrorCode::PermissionDenied);
        let details = payload
            .details_as::<error_details::PermissionDeniedDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.path, "/tmp/beads.lock");
        assert_eq!(details.operation, error_details::PermissionOperation::Write);
    }

    #[test]
    fn ping_info_serializes_as_query() {
        let info = crate::api::DaemonInfo {
            version: "0.0.0-test".to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            pid: 123,
        };
        let resp = Response::ok(ResponsePayload::Query(QueryResult::DaemonInfo(info)));
        let json = serde_json::to_string(&resp).unwrap();
        // Query variant serializes directly to its content (untagged)
        assert!(json.contains("\"result\":\"daemon_info\""));
        assert!(json.contains("\"version\""));
        assert!(json.contains("\"protocol_version\""));
        assert!(json.contains("\"pid\""));
    }

    #[test]
    fn version_mismatch_error_includes_parse_error() {
        let err = IpcError::DaemonVersionMismatch {
            daemon: None,
            client_version: "0.1.0".into(),
            protocol_version: 1,
            parse_error: Some("data did not match any variant".into()),
        };
        // Error should indicate version mismatch
        assert_eq!(err.code(), ErrorCode::DaemonVersionMismatch);
        // The error message should be actionable
        assert!(err.to_string().contains("restart the daemon"));
    }

    #[test]
    fn version_mismatch_is_retryable() {
        let err = IpcError::DaemonVersionMismatch {
            daemon: None,
            client_version: "0.1.0".into(),
            protocol_version: 1,
            parse_error: None,
        };
        assert!(err.transience().is_retryable());
    }

    #[test]
    fn version_check_uses_meta_when_matching() {
        use tempfile::TempDir;
        let temp = TempDir::new().expect("temp dir");
        let socket = temp.path().join("daemon.sock");
        let meta = crate::api::DaemonInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            pid: std::process::id(),
        };
        let meta_path = socket.with_file_name("daemon.meta.json");
        std::fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();

        let (stream, _peer) = UnixStream::pair().expect("socket pair");
        let mut writer = stream;
        let reader_stream = writer.try_clone().expect("clone stream");
        let mut reader = BufReader::new(reader_stream);
        verify_daemon_version(&socket, &mut writer, &mut reader).expect("meta match");
    }

    #[test]
    fn version_check_rejects_meta_mismatch() {
        use tempfile::TempDir;
        let temp = TempDir::new().expect("temp dir");
        let socket = temp.path().join("daemon.sock");
        let meta = crate::api::DaemonInfo {
            version: "0.0.0-fake".to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            pid: std::process::id(),
        };
        let meta_path = socket.with_file_name("daemon.meta.json");
        std::fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();

        let (stream, _peer) = UnixStream::pair().expect("socket pair");
        let mut writer = stream;
        let reader_stream = writer.try_clone().expect("clone stream");
        let mut reader = BufReader::new(reader_stream);
        let err = verify_daemon_version(&socket, &mut writer, &mut reader).unwrap_err();
        assert!(matches!(
            err,
            IpcError::DaemonVersionMismatch {
                daemon: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn invalid_json_would_cause_parse_error() {
        // Simulate what an old/incompatible daemon might send
        let bad_json = r#"{"unexpected": "format"}"#;
        let result: Result<Response, _> = serde_json::from_str(bad_json);
        assert!(result.is_err());
        // This is the error type that would be converted to DaemonVersionMismatch
        let err = result.unwrap_err();
        assert!(err.to_string().contains("did not match"));
    }

    #[test]
    fn decode_request_invalid_json_is_malformed_payload() {
        let err = decode_request_with_limits("{", &Limits::default()).unwrap_err();
        let payload: ErrorPayload = err.into();
        assert_eq!(payload.code, ErrorCode::MalformedPayload);
        let details = payload
            .details_as::<error_details::MalformedPayloadDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.parser, error_details::ParserKind::Json);
        assert!(details.reason.is_some());
    }

    #[test]
    fn decode_request_semantic_error_is_invalid_request() {
        let err = decode_request_with_limits(r#"{"op":"create"}"#, &Limits::default()).unwrap_err();
        let payload: ErrorPayload = err.into();
        assert_eq!(payload.code, ErrorCode::InvalidRequest);
        let details = payload
            .details_as::<error_details::InvalidRequestDetails>()
            .unwrap()
            .expect("details");
        assert!(details.reason.is_some());
        assert!(details.field.is_none());
    }

    #[test]
    fn decode_request_rejects_oversize_frames() {
        let limits = Limits {
            max_frame_bytes: 1,
            ..Limits::default()
        };
        let err = decode_request_with_limits(r#"{"op":"ping"}"#, &limits).unwrap_err();
        assert!(matches!(err, IpcError::FrameTooLarge { .. }));
    }

    #[test]
    fn durability_timeout_includes_receipt() {
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([4u8; 16])), StoreEpoch::ZERO);
        let txn_id = TxnId::new(Uuid::from_bytes([5u8; 16]));
        let receipt = DurabilityReceipt::local_fsync_defaults(store, txn_id, Vec::new(), 123);

        let err = OpError::DurabilityTimeout {
            requested: DurabilityClass::LocalFsync,
            waited_ms: 500,
            pending_replica_ids: None,
            receipt: Box::new(receipt.clone()),
        };

        let payload: ErrorPayload = err.into();
        assert_eq!(payload.code, ErrorCode::DurabilityTimeout);
        let parsed: DurabilityReceipt = payload
            .receipt_as()
            .expect("receipt decode")
            .expect("receipt");
        assert_eq!(parsed, receipt);
    }

    #[test]
    fn namespace_policy_violation_includes_details() {
        let err = OpError::NamespacePolicyViolation {
            namespace: NamespaceId::core(),
            rule: "replicate_mode".to_string(),
            reason: Some("replicate_mode=none".to_string()),
        };
        let payload: ErrorPayload = err.into();
        assert_eq!(payload.code, ErrorCode::NamespacePolicyViolation);
        let details = payload
            .details_as::<error_details::NamespacePolicyViolationDetails>()
            .expect("details decode")
            .expect("details");
        assert_eq!(details.namespace, NamespaceId::core());
        assert_eq!(details.rule, "replicate_mode");
        assert_eq!(details.reason.as_deref(), Some("replicate_mode=none"));
    }

    #[test]
    fn cross_namespace_dependency_includes_details() {
        let err = OpError::CrossNamespaceDependency {
            from_namespace: NamespaceId::core(),
            to_namespace: NamespaceId::parse("tmp").expect("namespace"),
        };
        let payload: ErrorPayload = err.into();
        assert_eq!(payload.code, ErrorCode::CrossNamespaceDependency);
        let details = payload
            .details_as::<error_details::CrossNamespaceDependencyDetails>()
            .expect("details decode")
            .expect("details");
        assert_eq!(details.from_namespace, NamespaceId::core());
        assert_eq!(details.to_namespace.as_str(), "tmp");
    }

    #[test]
    fn rate_limited_includes_details() {
        let err = OpError::RateLimited {
            retry_after_ms: Some(250),
            limit_bytes_per_sec: 1024,
        };
        let payload: ErrorPayload = err.into();
        assert_eq!(payload.code, ErrorCode::RateLimited);
        let details = payload
            .details_as::<error_details::RateLimitedDetails>()
            .expect("details decode")
            .expect("details");
        assert_eq!(details.retry_after_ms, Some(250));
        assert_eq!(details.limit_bytes_per_sec, 1024);
    }

    // Regression tests: verify all ResponsePayload variants roundtrip through Response
    mod response_roundtrip {
        use super::*;
        use crate::core::{
            ActorId, Applied, BeadId, DurabilityReceipt, EventId, EventKindV1, HlcMax, NamespaceId,
            ReplicaId, Seq1, StoreEpoch, StoreId, StoreIdentity, TxnDeltaV1, TxnId, Watermarks,
        };
        use uuid::Uuid;

        fn sample_receipt() -> DurabilityReceipt {
            let store =
                StoreIdentity::new(StoreId::new(Uuid::from_bytes([1u8; 16])), StoreEpoch::ZERO);
            let txn_id = TxnId::new(Uuid::from_bytes([2u8; 16]));
            DurabilityReceipt::local_fsync_defaults(store, txn_id, Vec::new(), 1_726_000_000_000)
        }

        fn roundtrip_response(resp: Response) {
            let json = serde_json::to_string(&resp).unwrap();
            let parsed: Response = serde_json::from_str(&json)
                .unwrap_or_else(|e| panic!("Failed to parse: {e}\nJSON: {json}"));
            // Re-serialize to verify structural equality
            let json2 = serde_json::to_string(&parsed).unwrap();
            assert_eq!(json, json2, "Roundtrip mismatch");
        }

        #[test]
        fn op_created() {
            let receipt = sample_receipt();
            let resp = Response::ok(ResponsePayload::Op(OpResponse::new(
                OpResult::Created {
                    id: BeadId::parse("bd-abc").unwrap(),
                },
                receipt,
            )));
            roundtrip_response(resp);
        }

        #[test]
        fn op_updated() {
            let receipt = sample_receipt();
            let resp = Response::ok(ResponsePayload::Op(OpResponse::new(
                OpResult::Updated {
                    id: BeadId::parse("bd-abc").unwrap(),
                },
                receipt,
            )));
            roundtrip_response(resp);
        }

        #[test]
        fn query_issues_empty() {
            let resp = Response::ok(ResponsePayload::Query(QueryResult::Issues(vec![])));
            roundtrip_response(resp);
        }

        #[test]
        fn query_daemon_info() {
            let info = crate::api::DaemonInfo {
                version: "0.1.0".into(),
                protocol_version: IPC_PROTOCOL_VERSION,
                pid: 12345,
            };
            let resp = Response::ok(ResponsePayload::Query(QueryResult::DaemonInfo(info)));
            roundtrip_response(resp);
        }

        #[test]
        fn synced() {
            roundtrip_response(Response::ok(ResponsePayload::synced()));
        }

        #[test]
        fn initialized() {
            roundtrip_response(Response::ok(ResponsePayload::initialized()));
        }

        #[test]
        fn shutting_down() {
            roundtrip_response(Response::ok(ResponsePayload::shutting_down()));
        }

        #[test]
        fn subscribed() {
            let info = crate::api::SubscribeInfo {
                namespace: NamespaceId::core(),
                watermarks_applied: Watermarks::<Applied>::new(),
            };
            let resp = Response::ok(ResponsePayload::subscribed(info));
            roundtrip_response(resp);
        }

        #[test]
        fn stream_event() {
            let store =
                StoreIdentity::new(StoreId::new(Uuid::from_bytes([9u8; 16])), StoreEpoch::ZERO);
            let origin = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
            let seq = Seq1::from_u64(1).unwrap();
            let body = crate::core::EventBody {
                envelope_v: 1,
                store,
                namespace: NamespaceId::core(),
                origin_replica_id: origin,
                origin_seq: seq,
                event_time_ms: 123,
                txn_id: TxnId::new(Uuid::from_bytes([3u8; 16])),
                client_request_id: None,
                kind: EventKindV1::TxnV1,
                delta: TxnDeltaV1::new(),
                hlc_max: Some(HlcMax {
                    actor_id: ActorId::new("tester").unwrap(),
                    physical_ms: 123,
                    logical: 0,
                }),
            };
            let event = crate::api::StreamEvent {
                event_id: EventId::new(origin, NamespaceId::core(), seq),
                sha256: hex::encode([1u8; 32]),
                prev_sha256: None,
                body: crate::api::EventBody::from(&body),
                body_bytes_hex: Some("00".to_string()),
            };
            let resp = Response::ok(ResponsePayload::event(event));
            roundtrip_response(resp);
        }

        #[test]
        fn error_response() {
            let receipt = sample_receipt();
            let resp = Response::err(
                ErrorPayload::new(
                    ErrorCode::Unknown("test_error".into()),
                    "Something went wrong",
                    false,
                )
                .with_details(serde_json::json!({"key": "value"}))
                .with_receipt(receipt),
            );
            roundtrip_response(resp);
        }
    }
}
