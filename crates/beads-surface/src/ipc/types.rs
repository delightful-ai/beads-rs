use std::path::Path;

use serde::{Deserialize, Serialize};

use beads_api::StreamEvent;
use beads_api::{Issue, QueryResult, SubscribeInfo};
use beads_core::ErrorPayload;
use beads_core::{Applied, DurabilityReceipt, Watermarks};

use super::{ctx::*, payload::*};

use crate::ops::OpResult;

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
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: CreatePayload,
    },

    /// Update an existing bead.
    Update {
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: UpdatePayload,
    },

    /// Add labels to a bead.
    AddLabels {
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: LabelsPayload,
    },

    /// Remove labels from a bead.
    RemoveLabels {
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: LabelsPayload,
    },

    /// Set or clear a parent relationship.
    SetParent {
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: ParentPayload,
    },

    /// Close a bead.
    Close {
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: ClosePayload,
    },

    /// Reopen a closed bead.
    Reopen {
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: IdPayload,
    },

    /// Delete a bead (soft delete).
    Delete {
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: DeletePayload,
    },

    /// Add a dependency.
    AddDep {
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: DepPayload,
    },

    /// Remove a dependency.
    RemoveDep {
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: DepPayload,
    },

    /// Add a note.
    AddNote {
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: AddNotePayload,
    },

    /// Claim a bead.
    Claim {
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: ClaimPayload,
    },

    /// Release a claim.
    Unclaim {
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: IdPayload,
    },

    /// Extend a claim.
    ExtendClaim {
        #[serde(flatten)]
        ctx: MutationCtx,
        #[serde(flatten)]
        payload: LeasePayload,
    },

    // === Queries ===
    /// Get a single bead.
    Show {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: IdPayload,
    },

    /// Show multiple beads (batch fetch for summaries).
    ShowMultiple {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: IdsPayload,
    },

    /// List beads.
    List {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: ListPayload,
    },

    /// Get ready beads.
    Ready {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: ReadyPayload,
    },

    /// Get dependency tree.
    DepTree {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: IdPayload,
    },

    /// Get dependency cycles.
    DepCycles {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Get dependencies.
    Deps {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: IdPayload,
    },

    /// Get notes.
    Notes {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: IdPayload,
    },

    /// Get blocked issues.
    Blocked {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Get stale issues.
    Stale {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: StalePayload,
    },

    /// Count issues matching filters.
    Count {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: CountPayload,
    },

    /// Show deleted (tombstoned) issues.
    Deleted {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: DeletedPayload,
    },

    /// Epic completion status.
    EpicStatus {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: EpicStatusPayload,
    },

    // === Control ===
    /// Force reload state from git (invalidates cache).
    /// Use after external changes to refs/heads/beads/store (e.g., migration).
    Refresh {
        #[serde(flatten)]
        ctx: RepoCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Force sync now.
    Sync {
        #[serde(flatten)]
        ctx: RepoCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Wait until repo is clean (debounced sync flushed).
    SyncWait {
        #[serde(flatten)]
        ctx: RepoCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Initialize beads ref.
    Init {
        #[serde(flatten)]
        ctx: RepoCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Get sync status.
    Status {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Admin status snapshot.
    AdminStatus {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Admin metrics snapshot.
    AdminMetrics {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Admin doctor report.
    AdminDoctor {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: AdminDoctorPayload,
    },

    /// Admin scrub now.
    AdminScrub {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: AdminScrubPayload,
    },

    /// Admin flush WAL namespace.
    AdminFlush {
        #[serde(flatten)]
        ctx: RepoCtx,
        #[serde(flatten)]
        payload: AdminFlushPayload,
    },

    /// Admin checkpoint wait (force checkpoint and block until complete).
    AdminCheckpointWait {
        #[serde(flatten)]
        ctx: RepoCtx,
        #[serde(flatten)]
        payload: AdminCheckpointWaitPayload,
    },

    /// Admin fingerprint report.
    AdminFingerprint {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: AdminFingerprintPayload,
    },

    /// Admin reload namespace policies.
    AdminReloadPolicies {
        #[serde(flatten)]
        ctx: RepoCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Admin reload limits.
    AdminReloadLimits {
        #[serde(flatten)]
        ctx: RepoCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Admin reload replication runtime.
    AdminReloadReplication {
        #[serde(flatten)]
        ctx: RepoCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Admin rotate replica id.
    AdminRotateReplicaId {
        #[serde(flatten)]
        ctx: RepoCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Admin maintenance mode toggle.
    AdminMaintenanceMode {
        #[serde(flatten)]
        ctx: RepoCtx,
        #[serde(flatten)]
        payload: AdminMaintenanceModePayload,
    },

    /// Rebuild WAL index from segments.
    AdminRebuildIndex {
        #[serde(flatten)]
        ctx: RepoCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Validate state.
    Validate {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Subscribe to realtime events.
    Subscribe {
        #[serde(flatten)]
        ctx: ReadCtx,
        #[serde(flatten)]
        payload: EmptyPayload,
    },

    /// Ping (health check).
    Ping,

    /// Shutdown daemon.
    Shutdown,
}

#[derive(Debug, Clone, Copy)]
pub struct RequestInfo<'a> {
    pub op: &'static str,
    pub repo: Option<&'a Path>,
    pub namespace: Option<&'a str>,
    pub actor_id: Option<&'a str>,
    pub client_request_id: Option<&'a str>,
    pub read: Option<&'a ReadConsistency>,
}

impl Request {
    pub fn info(&self) -> RequestInfo<'_> {
        match self {
            Request::Create { ctx, .. } => info_from_mutation("create", ctx),
            Request::Update { ctx, .. } => info_from_mutation("update", ctx),
            Request::AddLabels { ctx, .. } => info_from_mutation("add_labels", ctx),
            Request::RemoveLabels { ctx, .. } => info_from_mutation("remove_labels", ctx),
            Request::SetParent { ctx, .. } => info_from_mutation("set_parent", ctx),
            Request::Close { ctx, .. } => info_from_mutation("close", ctx),
            Request::Reopen { ctx, .. } => info_from_mutation("reopen", ctx),
            Request::Delete { ctx, .. } => info_from_mutation("delete", ctx),
            Request::AddDep { ctx, .. } => info_from_mutation("add_dep", ctx),
            Request::RemoveDep { ctx, .. } => info_from_mutation("remove_dep", ctx),
            Request::AddNote { ctx, .. } => info_from_mutation("add_note", ctx),
            Request::Claim { ctx, .. } => info_from_mutation("claim", ctx),
            Request::Unclaim { ctx, .. } => info_from_mutation("unclaim", ctx),
            Request::ExtendClaim { ctx, .. } => info_from_mutation("extend_claim", ctx),
            Request::Show { ctx, .. } => info_from_read("show", ctx),
            Request::ShowMultiple { ctx, .. } => info_from_read("show_multiple", ctx),
            Request::List { ctx, .. } => info_from_read("list", ctx),
            Request::Ready { ctx, .. } => info_from_read("ready", ctx),
            Request::DepTree { ctx, .. } => info_from_read("dep_tree", ctx),
            Request::DepCycles { ctx, .. } => info_from_read("dep_cycles", ctx),
            Request::Deps { ctx, .. } => info_from_read("deps", ctx),
            Request::Notes { ctx, .. } => info_from_read("notes", ctx),
            Request::Blocked { ctx, .. } => info_from_read("blocked", ctx),
            Request::Stale { ctx, .. } => info_from_read("stale", ctx),
            Request::Count { ctx, .. } => info_from_read("count", ctx),
            Request::Deleted { ctx, .. } => info_from_read("deleted", ctx),
            Request::EpicStatus { ctx, .. } => info_from_read("epic_status", ctx),
            Request::Refresh { ctx, .. } => info_from_repo("refresh", ctx),
            Request::Sync { ctx, .. } => info_from_repo("sync", ctx),
            Request::SyncWait { ctx, .. } => info_from_repo("sync_wait", ctx),
            Request::Init { ctx, .. } => info_from_repo("init", ctx),
            Request::Status { ctx, .. } => info_from_read("status", ctx),
            Request::AdminStatus { ctx, .. } => info_from_read("admin_status", ctx),
            Request::AdminMetrics { ctx, .. } => info_from_read("admin_metrics", ctx),
            Request::AdminDoctor { ctx, .. } => info_from_read("admin_doctor", ctx),
            Request::AdminScrub { ctx, .. } => info_from_read("admin_scrub", ctx),
            Request::AdminFlush { ctx, payload, .. } => {
                info_from_namespace("admin_flush", ctx, payload.namespace.as_deref())
            }
            Request::AdminCheckpointWait { ctx, payload, .. } => {
                info_from_namespace("admin_checkpoint_wait", ctx, payload.namespace.as_deref())
            }
            Request::AdminFingerprint { ctx, .. } => info_from_read("admin_fingerprint", ctx),
            Request::AdminReloadPolicies { ctx, .. } => {
                info_from_repo("admin_reload_policies", ctx)
            }
            Request::AdminReloadLimits { ctx, .. } => info_from_repo("admin_reload_limits", ctx),
            Request::AdminReloadReplication { ctx, .. } => {
                info_from_repo("admin_reload_replication", ctx)
            }
            Request::AdminRotateReplicaId { ctx, .. } => {
                info_from_repo("admin_rotate_replica_id", ctx)
            }
            Request::AdminMaintenanceMode { ctx, .. } => {
                info_from_repo("admin_maintenance_mode", ctx)
            }
            Request::AdminRebuildIndex { ctx, .. } => info_from_repo("admin_rebuild_index", ctx),
            Request::Validate { ctx, .. } => info_from_read("validate", ctx),
            Request::Subscribe { ctx, .. } => info_from_read("subscribe", ctx),
            Request::Ping => RequestInfo {
                op: "ping",
                repo: None,
                namespace: None,
                actor_id: None,
                client_request_id: None,
                read: None,
            },
            Request::Shutdown => RequestInfo {
                op: "shutdown",
                repo: None,
                namespace: None,
                actor_id: None,
                client_request_id: None,
                read: None,
            },
        }
    }
}

fn info_from_mutation<'a>(op: &'static str, ctx: &'a MutationCtx) -> RequestInfo<'a> {
    RequestInfo {
        op,
        repo: Some(&ctx.repo.path),
        namespace: ctx.meta.namespace.as_deref(),
        actor_id: ctx.meta.actor_id.as_deref(),
        client_request_id: ctx.meta.client_request_id.as_deref(),
        read: None,
    }
}

fn info_from_read<'a>(op: &'static str, ctx: &'a ReadCtx) -> RequestInfo<'a> {
    RequestInfo {
        op,
        repo: Some(&ctx.repo.path),
        namespace: ctx.read.namespace.as_deref(),
        actor_id: None,
        client_request_id: None,
        read: Some(&ctx.read),
    }
}

fn info_from_repo<'a>(op: &'static str, ctx: &'a RepoCtx) -> RequestInfo<'a> {
    RequestInfo {
        op,
        repo: Some(&ctx.path),
        namespace: None,
        actor_id: None,
        client_request_id: None,
        read: None,
    }
}

fn info_from_namespace<'a>(
    op: &'static str,
    ctx: &'a RepoCtx,
    namespace: Option<&'a str>,
) -> RequestInfo<'a> {
    RequestInfo {
        op,
        repo: Some(&ctx.path),
        namespace,
        actor_id: None,
        client_request_id: None,
        read: None,
    }
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
    pub fn err(error: ErrorPayload) -> Self {
        Response::Err { err: error }
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
    pub issue: Option<Issue>,
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
    /// Create a query payload.
    pub fn query(result: QueryResult) -> Self {
        ResponsePayload::Query(result)
    }

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
    pub fn subscribed(info: SubscribeInfo) -> Self {
        ResponsePayload::Subscribed(SubscribedPayload { subscribed: info })
    }

    /// Create a stream event payload.
    pub fn event(event: StreamEvent) -> Self {
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
    pub subscribed: SubscribeInfo,
}

/// Payload for streamed events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEventPayload {
    pub event: StreamEvent,
}

#[cfg(test)]
mod tests {
    use super::*;
    use beads_api::DaemonInfo;
    use beads_core::{BeadType, Priority, Watermarks};
    use std::path::PathBuf;

    #[test]
    fn request_roundtrip() {
        let req = Request::Create {
            ctx: MutationCtx::new(PathBuf::from("/test"), MutationMeta::default()),
            payload: CreatePayload {
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
            },
        };

        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();

        match parsed {
            Request::Create { payload, .. } => assert_eq!(payload.title, "test"),
            _ => panic!("wrong request type"),
        }
    }

    #[test]
    fn show_multiple_roundtrip() {
        let req = Request::ShowMultiple {
            ctx: ReadCtx::new(PathBuf::from("/test"), ReadConsistency::default()),
            payload: IdsPayload {
                ids: vec!["bd-abc".to_string(), "bd-xyz".to_string()],
            },
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("show_multiple"));
        assert!(json.contains("bd-abc"));

        let parsed: Request = serde_json::from_str(&json).unwrap();
        match parsed {
            Request::ShowMultiple { payload, .. } => {
                assert_eq!(payload.ids.len(), 2);
                assert_eq!(payload.ids[0], "bd-abc");
            }
            _ => panic!("wrong request type"),
        }
    }

    #[test]
    fn subscribe_roundtrip() {
        let req = Request::Subscribe {
            ctx: ReadCtx::new(
                PathBuf::from("/test"),
                ReadConsistency {
                    namespace: Some("core".to_string()),
                    require_min_seen: None,
                    wait_timeout_ms: Some(50),
                },
            ),
            payload: EmptyPayload {},
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("subscribe"));

        let parsed: Request = serde_json::from_str(&json).unwrap();
        match parsed {
            Request::Subscribe { ctx, .. } => {
                assert_eq!(ctx.read.namespace.as_deref(), Some("core"));
                assert_eq!(ctx.read.wait_timeout_ms, Some(50));
            }
            _ => panic!("wrong request type"),
        }
    }

    #[test]
    fn claim_defaults_lease_secs() {
        let json = r#"{"op":"claim","repo":"/test","id":"bd-123"}"#;
        let parsed: Request = serde_json::from_str(json).unwrap();
        match parsed {
            Request::Claim { payload, .. } => {
                assert_eq!(payload.lease_secs, 3600);
            }
            _ => panic!("wrong request type"),
        }
    }

    #[test]
    fn extend_claim_requires_lease_secs() {
        let json = r#"{"op":"extend_claim","repo":"/test","id":"bd-123"}"#;
        assert!(serde_json::from_str::<Request>(json).is_err());
    }

    #[test]
    fn repo_ctx_serializes_as_repo_field() {
        let req = Request::Refresh {
            ctx: RepoCtx::new(PathBuf::from("/test")),
            payload: EmptyPayload {},
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"repo\""));
        assert!(!json.contains("\"path\""));
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
            beads_core::CliErrorCode::NotFound.into(),
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
