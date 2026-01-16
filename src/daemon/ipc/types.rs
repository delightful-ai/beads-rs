use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::daemon::ops::{BeadPatch, OpResult};
use crate::daemon::query::{Filters, QueryResult};
use crate::api::{AdminFingerprintMode, AdminFingerprintSample};
use crate::core::{
    Applied, BeadType, DepKind, DurabilityReceipt, Priority, Watermarks,
};
use crate::core::ErrorPayload;

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

    /// Get dependency cycles.
    DepCycles {
        repo: PathBuf,
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

    /// Admin flush WAL namespace.
    AdminFlush {
        repo: PathBuf,
        #[serde(default)]
        namespace: Option<String>,
        #[serde(default)]
        checkpoint_now: bool,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::DaemonInfo;
    use crate::core::Watermarks;

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
            crate::core::ErrorCode::NotFound,
            "bead not found",
            false,
        ));
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"err\""));
        assert!(json.contains("not_found"));
    }

    #[test]
    fn ping_info_serializes_as_query() {
        let info = DaemonInfo {
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
    fn read_consistency_roundtrip_preserves_namespace() {
        let read = ReadConsistency {
            namespace: Some("core".to_string()),
            require_min_seen: Some(Watermarks::new()),
            wait_timeout_ms: None,
        };
        let json = serde_json::to_string(&read).unwrap();
        let parsed: ReadConsistency = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.namespace, Some("core".to_string()));
        assert_eq!(parsed.require_min_seen, Some(Watermarks::new()));
    }
}
