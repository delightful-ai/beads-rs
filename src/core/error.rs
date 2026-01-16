//! Core capability errors (parsing, validation, CRDT invariants).
//!
//! These are bounded and stable: core errors represent domain/refusal states,
//! not library implementation details.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

use crate::error::{Effect, Transience};

/// Invalid ID or content identifier.
#[derive(Debug, Error, Clone)]
#[non_exhaustive]
pub enum InvalidId {
    #[error("bead id `{raw}` is invalid: {reason}")]
    Bead { raw: String, reason: String },
    #[error("actor id `{raw}` is invalid: {reason}")]
    Actor { raw: String, reason: String },
    #[error("note id `{raw}` is invalid: {reason}")]
    Note { raw: String, reason: String },
    #[error("branch name `{raw}` is invalid: {reason}")]
    Branch { raw: String, reason: String },
    #[error("content hash `{raw}` is invalid: {reason}")]
    ContentHash { raw: String, reason: String },
    #[error("namespace id `{raw}` is invalid: {reason}")]
    Namespace { raw: String, reason: String },
    #[error("store id `{raw}` is invalid: {reason}")]
    StoreId { raw: String, reason: String },
    #[error("replica id `{raw}` is invalid: {reason}")]
    ReplicaId { raw: String, reason: String },
    #[error("txn id `{raw}` is invalid: {reason}")]
    TxnId { raw: String, reason: String },
    #[error("client request id `{raw}` is invalid: {reason}")]
    ClientRequestId { raw: String, reason: String },
    #[error("segment id `{raw}` is invalid: {reason}")]
    SegmentId { raw: String, reason: String },
}

/// Invalid label string.
#[derive(Debug, Error, Clone)]
#[error("label `{raw}` is invalid: {reason}")]
pub struct InvalidLabel {
    pub raw: String,
    pub reason: String,
}

/// Generic range violation.
#[derive(Debug, Error, Clone)]
#[error("{field} value {value} out of range {min}..={max}")]
pub struct RangeError {
    pub field: &'static str,
    pub value: u8,
    pub min: u8,
    pub max: u8,
}

/// ID collision between independently-created beads.
#[derive(Debug, Error, Clone)]
#[error("bead id collision: {id} has conflicting creation stamps")]
pub struct CollisionError {
    pub id: String,
}

/// Invalid dependency edge.
#[derive(Debug, Error, Clone)]
#[error("invalid dependency: {reason}")]
pub struct InvalidDependency {
    pub reason: String,
}

/// Invalid dependency kind string.
#[derive(Debug, Error, Clone)]
#[error("dependency kind `{raw}` is invalid")]
pub struct InvalidDepKind {
    pub raw: String,
}

/// Canonical error enum for core capability.
#[derive(Debug, Error, Clone)]
#[non_exhaustive]
pub enum CoreError {
    #[error(transparent)]
    InvalidId(#[from] InvalidId),
    #[error(transparent)]
    InvalidLabel(#[from] InvalidLabel),
    #[error(transparent)]
    Range(#[from] RangeError),
    #[error(transparent)]
    Collision(#[from] CollisionError),
    #[error(transparent)]
    InvalidDependency(#[from] InvalidDependency),
    #[error(transparent)]
    InvalidDepKind(#[from] InvalidDepKind),
}

impl CoreError {
    pub fn transience(&self) -> Transience {
        // Core errors are pure domain/input failures.
        Transience::Permanent
    }

    pub fn effect(&self) -> Effect {
        Effect::None
    }
}

// =============================================================================
// Protocol error codes + payload
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    // Protocol and identity
    WrongStore,
    StoreEpochMismatch,
    ReplicaIdCollision,
    VersionIncompatible,
    Diverged,
    AuthFailed,
    UnknownReplica,

    // Operational
    Overloaded,
    MaintenanceMode,
    DurabilityTimeout,
    DurabilityUnavailable,
    RequireMinSeenTimeout,
    RequireMinSeenUnsatisfied,

    // Replication
    SnapshotRequired,
    SnapshotExpired,
    BootstrapRequired,
    SubscriberLagged,

    // Request / protocol framing
    InvalidRequest,
    MalformedPayload,
    FrameTooLarge,
    BatchTooLarge,
    RateLimited,

    // Client request / mutation planning
    CasFailed,
    ClientRequestIdReuseMismatch,
    PayloadTooLarge,
    WalRecordTooLarge,
    RequestTooLarge,
    OpsTooMany,
    NoteTooLarge,
    LabelsTooMany,

    // Data integrity / contiguity
    Corruption,
    NonCanonical,
    HashMismatch,
    PrevShaMismatch,
    GapDetected,
    Equivocation,
    WalCorrupt,
    WalTailTruncated,
    SegmentHeaderMismatch,
    WalFormatUnsupported,
    IndexCorrupt,
    IndexRebuildRequired,

    // Checkpoint / snapshot
    CheckpointHashMismatch,
    CheckpointFormatUnsupported,
    SnapshotTooLarge,
    SnapshotCorrupt,
    ArchiveUnsafe,
    JsonlParseError,

    // Namespace / policy
    NamespaceInvalid,
    NamespaceUnknown,
    NamespacePolicyViolation,
    CrossNamespaceDependency,

    // Locking / filesystem safety
    LockHeld,
    LockStale,
    PathSymlinkRejected,
    PermissionDenied,

    // Generic internal
    InternalError,

    // Legacy IPC / CLI codes (pre-realtime)
    NotFound,
    AlreadyExists,
    AlreadyClaimed,
    CasMismatch,
    InvalidTransition,
    ValidationFailed,
    NotAGitRepo,
    NoRemote,
    RepoNotInitialized,
    SyncFailed,
    BeadDeleted,
    WalError,
    WalMergeConflict,
    NotClaimedByYou,
    DepNotFound,
    LoadTimeout,
    Internal,
    ParseError,
    IoError,
    InvalidId,
    Disconnected,
    DaemonUnavailable,
    DaemonVersionMismatch,
    InitFailed,

    Unknown(String),
}

impl ErrorCode {
    pub fn as_str(&self) -> &str {
        match self {
            // Protocol and identity
            ErrorCode::WrongStore => "wrong_store",
            ErrorCode::StoreEpochMismatch => "store_epoch_mismatch",
            ErrorCode::ReplicaIdCollision => "replica_id_collision",
            ErrorCode::VersionIncompatible => "version_incompatible",
            ErrorCode::Diverged => "diverged",
            ErrorCode::AuthFailed => "auth_failed",
            ErrorCode::UnknownReplica => "unknown_replica",

            // Operational
            ErrorCode::Overloaded => "overloaded",
            ErrorCode::MaintenanceMode => "maintenance_mode",
            ErrorCode::DurabilityTimeout => "durability_timeout",
            ErrorCode::DurabilityUnavailable => "durability_unavailable",
            ErrorCode::RequireMinSeenTimeout => "require_min_seen_timeout",
            ErrorCode::RequireMinSeenUnsatisfied => "require_min_seen_unsatisfied",

            // Replication
            ErrorCode::SnapshotRequired => "snapshot_required",
            ErrorCode::SnapshotExpired => "snapshot_expired",
            ErrorCode::BootstrapRequired => "bootstrap_required",
            ErrorCode::SubscriberLagged => "subscriber_lagged",

            // Request / protocol framing
            ErrorCode::InvalidRequest => "invalid_request",
            ErrorCode::MalformedPayload => "malformed_payload",
            ErrorCode::FrameTooLarge => "frame_too_large",
            ErrorCode::BatchTooLarge => "batch_too_large",
            ErrorCode::RateLimited => "rate_limited",

            // Client request / mutation planning
            ErrorCode::CasFailed => "cas_failed",
            ErrorCode::ClientRequestIdReuseMismatch => "client_request_id_reuse_mismatch",
            ErrorCode::PayloadTooLarge => "payload_too_large",
            ErrorCode::WalRecordTooLarge => "wal_record_too_large",
            ErrorCode::RequestTooLarge => "request_too_large",
            ErrorCode::OpsTooMany => "ops_too_many",
            ErrorCode::NoteTooLarge => "note_too_large",
            ErrorCode::LabelsTooMany => "labels_too_many",

            // Data integrity / contiguity
            ErrorCode::Corruption => "corruption",
            ErrorCode::NonCanonical => "non_canonical",
            ErrorCode::HashMismatch => "hash_mismatch",
            ErrorCode::PrevShaMismatch => "prev_sha_mismatch",
            ErrorCode::GapDetected => "gap_detected",
            ErrorCode::Equivocation => "equivocation",
            ErrorCode::WalCorrupt => "wal_corrupt",
            ErrorCode::WalTailTruncated => "wal_tail_truncated",
            ErrorCode::SegmentHeaderMismatch => "segment_header_mismatch",
            ErrorCode::WalFormatUnsupported => "wal_format_unsupported",
            ErrorCode::IndexCorrupt => "index_corrupt",
            ErrorCode::IndexRebuildRequired => "index_rebuild_required",

            // Checkpoint / snapshot
            ErrorCode::CheckpointHashMismatch => "checkpoint_hash_mismatch",
            ErrorCode::CheckpointFormatUnsupported => "checkpoint_format_unsupported",
            ErrorCode::SnapshotTooLarge => "snapshot_too_large",
            ErrorCode::SnapshotCorrupt => "snapshot_corrupt",
            ErrorCode::ArchiveUnsafe => "archive_unsafe",
            ErrorCode::JsonlParseError => "jsonl_parse_error",

            // Namespace / policy
            ErrorCode::NamespaceInvalid => "namespace_invalid",
            ErrorCode::NamespaceUnknown => "namespace_unknown",
            ErrorCode::NamespacePolicyViolation => "namespace_policy_violation",
            ErrorCode::CrossNamespaceDependency => "cross_namespace_dependency",

            // Locking / filesystem safety
            ErrorCode::LockHeld => "lock_held",
            ErrorCode::LockStale => "lock_stale",
            ErrorCode::PathSymlinkRejected => "path_symlink_rejected",
            ErrorCode::PermissionDenied => "permission_denied",

            // Generic internal
            ErrorCode::InternalError => "internal_error",

            // Legacy IPC / CLI codes
            ErrorCode::NotFound => "not_found",
            ErrorCode::AlreadyExists => "already_exists",
            ErrorCode::AlreadyClaimed => "already_claimed",
            ErrorCode::CasMismatch => "cas_mismatch",
            ErrorCode::InvalidTransition => "invalid_transition",
            ErrorCode::ValidationFailed => "validation_failed",
            ErrorCode::NotAGitRepo => "not_a_git_repo",
            ErrorCode::NoRemote => "no_remote",
            ErrorCode::RepoNotInitialized => "repo_not_initialized",
            ErrorCode::SyncFailed => "sync_failed",
            ErrorCode::BeadDeleted => "bead_deleted",
            ErrorCode::WalError => "wal_error",
            ErrorCode::WalMergeConflict => "wal_merge_conflict",
            ErrorCode::NotClaimedByYou => "not_claimed_by_you",
            ErrorCode::DepNotFound => "dep_not_found",
            ErrorCode::LoadTimeout => "load_timeout",
            ErrorCode::Internal => "internal",
            ErrorCode::ParseError => "parse_error",
            ErrorCode::IoError => "io_error",
            ErrorCode::InvalidId => "invalid_id",
            ErrorCode::Disconnected => "disconnected",
            ErrorCode::DaemonUnavailable => "daemon_unavailable",
            ErrorCode::DaemonVersionMismatch => "daemon_version_mismatch",
            ErrorCode::InitFailed => "init_failed",

            ErrorCode::Unknown(code) => code.as_str(),
        }
    }

    pub fn parse(code: &str) -> Self {
        match code {
            // Protocol and identity
            "wrong_store" => ErrorCode::WrongStore,
            "store_epoch_mismatch" => ErrorCode::StoreEpochMismatch,
            "replica_id_collision" => ErrorCode::ReplicaIdCollision,
            "version_incompatible" => ErrorCode::VersionIncompatible,
            "diverged" => ErrorCode::Diverged,
            "auth_failed" => ErrorCode::AuthFailed,
            "unknown_replica" => ErrorCode::UnknownReplica,

            // Operational
            "overloaded" => ErrorCode::Overloaded,
            "maintenance_mode" => ErrorCode::MaintenanceMode,
            "durability_timeout" => ErrorCode::DurabilityTimeout,
            "durability_unavailable" => ErrorCode::DurabilityUnavailable,
            "require_min_seen_timeout" => ErrorCode::RequireMinSeenTimeout,
            "require_min_seen_unsatisfied" => ErrorCode::RequireMinSeenUnsatisfied,

            // Replication
            "snapshot_required" => ErrorCode::SnapshotRequired,
            "snapshot_expired" => ErrorCode::SnapshotExpired,
            "bootstrap_required" => ErrorCode::BootstrapRequired,
            "subscriber_lagged" => ErrorCode::SubscriberLagged,

            // Request / protocol framing
            "invalid_request" => ErrorCode::InvalidRequest,
            "malformed_payload" => ErrorCode::MalformedPayload,
            "frame_too_large" => ErrorCode::FrameTooLarge,
            "batch_too_large" => ErrorCode::BatchTooLarge,
            "rate_limited" => ErrorCode::RateLimited,

            // Client request / mutation planning
            "cas_failed" => ErrorCode::CasFailed,
            "client_request_id_reuse_mismatch" => ErrorCode::ClientRequestIdReuseMismatch,
            "payload_too_large" => ErrorCode::PayloadTooLarge,
            "wal_record_too_large" => ErrorCode::WalRecordTooLarge,
            "request_too_large" => ErrorCode::RequestTooLarge,
            "ops_too_many" => ErrorCode::OpsTooMany,
            "note_too_large" => ErrorCode::NoteTooLarge,
            "labels_too_many" => ErrorCode::LabelsTooMany,

            // Data integrity / contiguity
            "corruption" => ErrorCode::Corruption,
            "non_canonical" => ErrorCode::NonCanonical,
            "hash_mismatch" => ErrorCode::HashMismatch,
            "prev_sha_mismatch" => ErrorCode::PrevShaMismatch,
            "gap_detected" => ErrorCode::GapDetected,
            "equivocation" => ErrorCode::Equivocation,
            "wal_corrupt" => ErrorCode::WalCorrupt,
            "wal_tail_truncated" => ErrorCode::WalTailTruncated,
            "segment_header_mismatch" => ErrorCode::SegmentHeaderMismatch,
            "wal_format_unsupported" => ErrorCode::WalFormatUnsupported,
            "index_corrupt" => ErrorCode::IndexCorrupt,
            "index_rebuild_required" => ErrorCode::IndexRebuildRequired,

            // Checkpoint / snapshot
            "checkpoint_hash_mismatch" => ErrorCode::CheckpointHashMismatch,
            "checkpoint_format_unsupported" => ErrorCode::CheckpointFormatUnsupported,
            "snapshot_too_large" => ErrorCode::SnapshotTooLarge,
            "snapshot_corrupt" => ErrorCode::SnapshotCorrupt,
            "archive_unsafe" => ErrorCode::ArchiveUnsafe,
            "jsonl_parse_error" => ErrorCode::JsonlParseError,

            // Namespace / policy
            "namespace_invalid" => ErrorCode::NamespaceInvalid,
            "namespace_unknown" => ErrorCode::NamespaceUnknown,
            "namespace_policy_violation" => ErrorCode::NamespacePolicyViolation,
            "cross_namespace_dependency" => ErrorCode::CrossNamespaceDependency,

            // Locking / filesystem safety
            "lock_held" => ErrorCode::LockHeld,
            "lock_stale" => ErrorCode::LockStale,
            "path_symlink_rejected" => ErrorCode::PathSymlinkRejected,
            "permission_denied" => ErrorCode::PermissionDenied,

            // Generic internal
            "internal_error" => ErrorCode::InternalError,

            // Legacy IPC / CLI codes
            "not_found" => ErrorCode::NotFound,
            "already_exists" => ErrorCode::AlreadyExists,
            "already_claimed" => ErrorCode::AlreadyClaimed,
            "cas_mismatch" => ErrorCode::CasMismatch,
            "invalid_transition" => ErrorCode::InvalidTransition,
            "validation_failed" => ErrorCode::ValidationFailed,
            "not_a_git_repo" => ErrorCode::NotAGitRepo,
            "no_remote" => ErrorCode::NoRemote,
            "repo_not_initialized" => ErrorCode::RepoNotInitialized,
            "sync_failed" => ErrorCode::SyncFailed,
            "bead_deleted" => ErrorCode::BeadDeleted,
            "wal_error" => ErrorCode::WalError,
            "wal_merge_conflict" => ErrorCode::WalMergeConflict,
            "not_claimed_by_you" => ErrorCode::NotClaimedByYou,
            "dep_not_found" => ErrorCode::DepNotFound,
            "load_timeout" => ErrorCode::LoadTimeout,
            "internal" => ErrorCode::Internal,
            "parse_error" => ErrorCode::ParseError,
            "io_error" => ErrorCode::IoError,
            "invalid_id" => ErrorCode::InvalidId,
            "disconnected" => ErrorCode::Disconnected,
            "daemon_unavailable" => ErrorCode::DaemonUnavailable,
            "daemon_version_mismatch" => ErrorCode::DaemonVersionMismatch,
            "init_failed" => ErrorCode::InitFailed,

            other => ErrorCode::Unknown(other.to_string()),
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for ErrorCode {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ErrorCode::parse(s))
    }
}

impl Serialize for ErrorCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Ok(ErrorCode::parse(&raw))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ErrorPayload {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt: Option<Value>,
}

impl ErrorPayload {
    pub fn new(code: ErrorCode, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code,
            message: message.into(),
            retryable,
            retry_after_ms: None,
            details: None,
            receipt: None,
        }
    }

    pub fn with_retry_after(mut self, retry_after_ms: u64) -> Self {
        self.retry_after_ms = Some(retry_after_ms);
        self
    }

    pub fn with_details<T: Serialize>(mut self, details: T) -> Self {
        self.details = serialize_optional(details);
        self
    }

    pub fn with_receipt<T: Serialize>(mut self, receipt: T) -> Self {
        self.receipt = serialize_optional(receipt);
        self
    }

    pub fn details_as<T: DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        match &self.details {
            Some(value) => serde_json::from_value(value.clone()).map(Some),
            None => Ok(None),
        }
    }

    pub fn receipt_as<T: DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        match &self.receipt {
            Some(value) => serde_json::from_value(value.clone()).map(Some),
            None => Ok(None),
        }
    }
}

fn serialize_optional<T: Serialize>(value: T) -> Option<Value> {
    match serde_json::to_value(value) {
        Ok(Value::Null) => None,
        Ok(value) => Some(value),
        Err(_) => None,
    }
}

// =============================================================================
// Typed error details
// =============================================================================

pub mod details {
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use uuid::Uuid;

    use super::super::{
        ActorId, Applied, BeadId, ClientRequestId, DurabilityClass, NamespaceId, ReplicaId,
        SegmentId, StoreId, Watermarks,
    };

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct InvalidRequestDetails {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub field: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub reason: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct MalformedPayloadDetails {
        pub parser: ParserKind,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub reason: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ParserKind {
        Json,
        Cbor,
        Ndjson,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct FrameTooLargeDetails {
        pub max_frame_bytes: u64,
        pub got_bytes: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BatchTooLargeDetails {
        pub max_events: u64,
        pub max_bytes: u64,
        pub got_events: u64,
        pub got_bytes: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RateLimitedDetails {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub retry_after_ms: Option<u64>,
        pub limit_bytes_per_sec: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct OverloadedDetails {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub subsystem: Option<OverloadedSubsystem>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub retry_after_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub queue_bytes: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub queue_events: Option<u64>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum OverloadedSubsystem {
        Ipc,
        Repl,
        Checkpoint,
        Wal,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct MaintenanceModeDetails {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub reason: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub until_ms: Option<u64>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct InternalErrorDetails {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub trace_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub component: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WrongStoreDetails {
        pub expected_store_id: StoreId,
        pub got_store_id: StoreId,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct StoreEpochMismatchDetails {
        pub store_id: StoreId,
        pub expected_epoch: u64,
        pub got_epoch: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct VersionIncompatibleDetails {
        pub local_min: u32,
        pub local_max: u32,
        pub peer_min: u32,
        pub peer_max: u32,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct AuthFailedDetails {
        pub mode: AuthMode,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub reason: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum AuthMode {
        PskV1,
        Other,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct UnknownReplicaDetails {
        pub replica_id: ReplicaId,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub roster_hash: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ReplicaIdCollisionDetails {
        pub replica_id: ReplicaId,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct DivergedDetails {
        pub namespace: NamespaceId,
        pub origin_replica_id: ReplicaId,
        pub seq: u64,
        pub expected_sha256: String,
        pub got_sha256: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct NamespaceInvalidDetails {
        pub namespace: String,
        pub pattern: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct NamespaceUnknownDetails {
        pub namespace: NamespaceId,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct NamespacePolicyViolationDetails {
        pub namespace: NamespaceId,
        pub rule: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub reason: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct CrossNamespaceDependencyDetails {
        pub from_namespace: NamespaceId,
        pub to_namespace: NamespaceId,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct CasFailedDetails {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub expected: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub actual: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub key: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ClientRequestIdReuseMismatchDetails {
        pub namespace: NamespaceId,
        pub client_request_id: ClientRequestId,
        pub expected_request_sha256: String,
        pub got_request_sha256: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct PayloadTooLargeDetails {
        pub limit_bytes: u64,
        pub got_bytes: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WalRecordTooLargeDetails {
        pub max_wal_record_bytes: u64,
        pub estimated_bytes: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct OpsTooManyDetails {
        pub max_ops_per_txn: u64,
        pub got_ops: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct NoteTooLargeDetails {
        pub max_note_bytes: u64,
        pub got_bytes: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct LabelsTooManyDetails {
        pub max_labels_per_bead: u64,
        pub got_labels: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub bead_id: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct DurabilityUnavailableDetails {
        pub requested: DurabilityClass,
        pub eligible_total: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub eligible_replica_ids: Option<Vec<ReplicaId>>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct DurabilityTimeoutDetails {
        pub requested: DurabilityClass,
        pub waited_ms: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub pending_replica_ids: Option<Vec<ReplicaId>>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RequireMinSeenTimeoutDetails {
        pub waited_ms: u64,
        pub required: Watermarks<Applied>,
        pub current_applied: Watermarks<Applied>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RequireMinSeenUnsatisfiedDetails {
        pub required: Watermarks<Applied>,
        pub current_applied: Watermarks<Applied>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct NonCanonicalDetails {
        pub format: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub reason: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct EventIdDetails {
        pub namespace: NamespaceId,
        pub origin_replica_id: ReplicaId,
        pub origin_seq: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct HashMismatchDetails {
        pub eid: EventIdDetails,
        pub expected_sha256: String,
        pub got_sha256: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct PrevShaMismatchDetails {
        pub eid: EventIdDetails,
        pub expected_prev_sha256: String,
        pub got_prev_sha256: String,
        pub head_seq: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct GapDetectedDetails {
        pub namespace: NamespaceId,
        pub origin_replica_id: ReplicaId,
        pub durable_seen: u64,
        pub got_seq: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct EquivocationDetails {
        pub eid: EventIdDetails,
        pub existing_sha256: String,
        pub new_sha256: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WalCorruptDetails {
        pub namespace: NamespaceId,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub segment_id: Option<SegmentId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub offset: Option<u64>,
        pub reason: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct CorruptionDetails {
        pub reason: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct StoreChecksumMismatchDetails {
        pub blob: String,
        pub expected_sha256: String,
        pub got_sha256: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WalTailTruncatedDetails {
        pub namespace: NamespaceId,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub segment_id: Option<SegmentId>,
        pub truncated_from_offset: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SegmentHeaderMismatchDetails {
        pub path: String,
        pub expected_store_id: StoreId,
        pub got_store_id: StoreId,
        pub expected_epoch: u64,
        pub got_epoch: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WalFormatUnsupportedDetails {
        pub wal_format_version: u32,
        pub supported: Vec<u32>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct IndexCorruptDetails {
        pub reason: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct IndexRebuildRequiredDetails {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub namespace: Option<NamespaceId>,
        pub reason: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct CheckpointHashMismatchDetails {
        pub which: CheckpointHashKind,
        pub expected: String,
        pub got: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum CheckpointHashKind {
        ContentHash,
        ManifestHash,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct CheckpointFormatUnsupportedDetails {
        pub checkpoint_format_version: u32,
        pub supported: Vec<u32>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SnapshotRequiredDetails {
        pub namespaces: Vec<NamespaceId>,
        pub reason: SnapshotRangeReason,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BootstrapRequiredDetails {
        pub namespaces: Vec<NamespaceId>,
        pub reason: SnapshotRangeReason,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum SnapshotRangeReason {
        RangePruned,
        RangeMissing,
        OverLimit,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SnapshotExpiredDetails {
        pub snapshot_id: Uuid,
        pub restart_from_chunk: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SnapshotTooLargeDetails {
        pub max_snapshot_bytes: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub announced_bytes: Option<u64>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SnapshotCorruptDetails {
        pub reason: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ArchiveUnsafeDetails {
        pub reason: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub path: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct JsonlParseErrorDetails {
        pub path: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub line: Option<u64>,
        pub reason: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct LockHeldDetails {
        pub store_id: StoreId,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub holder_pid: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub holder_replica_id: Option<ReplicaId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub started_at_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub daemon_version: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct LockStaleDetails {
        pub store_id: StoreId,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub holder_pid: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub started_at_ms: Option<u64>,
        pub suggested_action: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct PathSymlinkRejectedDetails {
        pub path: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct PermissionDeniedDetails {
        pub path: String,
        pub operation: PermissionOperation,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum PermissionOperation {
        Read,
        Write,
        Fsync,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ReplRejectReason {
        PrevUnknown,
        GapTimeout,
        GapBufferOverflow,
        GapBufferBytesOverflow,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SubscriberLaggedDetails {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub reason: Option<ReplRejectReason>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub max_queue_bytes: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub max_queue_events: Option<u64>,
    }

    // Legacy IPC / CLI detail structs (optional, but typed for consistency).
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct NotFoundDetails {
        pub id: BeadId,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct AlreadyExistsDetails {
        pub id: BeadId,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct AlreadyClaimedDetails {
        pub by: ActorId,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub expires_at_ms: Option<u64>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct CasMismatchDetails {
        pub expected: String,
        pub actual: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct InvalidTransitionDetails {
        pub from: String,
        pub to: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ValidationFailedDetails {
        pub field: String,
        pub reason: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct PathDetails {
        pub path: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WalMergeConflictDetails {
        pub errors: Vec<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct LoadTimeoutDetails {
        pub repo: String,
        pub timeout_secs: u64,
        pub remote: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BeadDeletedDetails {
        pub id: BeadId,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WalErrorDetails {
        pub message: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct InvalidIdDetails {
        pub kind: InvalidIdKind,
        pub raw: String,
        pub reason: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum InvalidIdKind {
        Bead,
        Actor,
        Note,
        Branch,
        ContentHash,
        Namespace,
        StoreId,
        ReplicaId,
        TxnId,
        ClientRequestId,
        SegmentId,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_payload_roundtrip_preserves_retryable_and_receipt() {
        let payload = ErrorPayload::new(ErrorCode::NotFound, "missing", true)
            .with_retry_after(1500)
            .with_details(serde_json::json!({ "k": "v" }))
            .with_receipt(serde_json::json!({ "receipt": "ok" }));

        let json = serde_json::to_string(&payload).unwrap();
        let parsed: ErrorPayload = serde_json::from_str(&json).unwrap();

        assert!(parsed.retryable);
        assert_eq!(parsed.retry_after_ms, Some(1500));
        assert_eq!(parsed.receipt, payload.receipt);
    }

    #[test]
    fn unknown_error_code_decodes() {
        let json = r#"{"code":"new_error_code","message":"msg","retryable":false}"#;
        let parsed: ErrorPayload = serde_json::from_str(json).unwrap();
        assert_eq!(
            parsed.code,
            ErrorCode::Unknown("new_error_code".to_string())
        );

        let json2 = serde_json::to_string(&parsed).unwrap();
        assert!(json2.contains("new_error_code"));
    }
}
