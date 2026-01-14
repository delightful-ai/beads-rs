//! Canonical API schemas for daemon IPC and CLI `--json`.
//!
//! These types are the *truthful boundary*: we avoid lossy “view” structs that
//! silently drop information. If a smaller payload is desirable, we define an
//! explicit summary type.

use serde::{Deserialize, Serialize};

pub use crate::core::DurabilityReceipt;

use crate::core::{
    ActorId, Applied, Bead, Claim, ClientRequestId, ContentHash, DepEdge as CoreDepEdge,
    DepKey as CoreDepKey, Durable, EventId, HlcMax, NamespaceId, ReplicaId, SegmentId, Seq1,
    StoreId, StoreIdentity, Tombstone as CoreTombstone, TxnDeltaV1, TxnId, WallClock, Watermarks,
    Workflow, WriteStamp,
};

// =============================================================================
// Daemon Info
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonInfo {
    pub version: String,
    pub protocol_version: u32,
    pub pid: u32,
}

// =============================================================================
// Admin status / metrics
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminStatusOutput {
    pub store_id: StoreId,
    pub replica_id: ReplicaId,
    pub namespaces: Vec<NamespaceId>,
    pub watermarks_applied: Watermarks<Applied>,
    pub watermarks_durable: Watermarks<Durable>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_clock_anomaly: Option<AdminClockAnomaly>,
    pub wal: Vec<AdminWalNamespace>,
    pub replication: Vec<AdminReplicationPeer>,
    pub checkpoints: Vec<AdminCheckpointGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminClockAnomaly {
    pub at_wall_ms: u64,
    pub kind: AdminClockAnomalyKind,
    pub delta_ms: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdminClockAnomalyKind {
    ForwardJumpClamped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminWalNamespace {
    pub namespace: NamespaceId,
    pub segment_count: usize,
    pub total_bytes: u64,
    pub segments: Vec<AdminWalSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminWalSegment {
    pub segment_id: SegmentId,
    pub created_at_ms: u64,
    pub last_indexed_offset: u64,
    pub sealed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_len: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminReplicationPeer {
    pub peer: ReplicaId,
    pub last_ack_at_ms: u64,
    pub diverged: bool,
    pub lag_by_namespace: Vec<AdminReplicationNamespace>,
    pub watermarks_durable: Watermarks<Durable>,
    pub watermarks_applied: Watermarks<Applied>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminReplicationNamespace {
    pub namespace: NamespaceId,
    pub local_durable_seq: u64,
    pub peer_durable_seq: u64,
    pub durable_lag: u64,
    pub local_applied_seq: u64,
    pub peer_applied_seq: u64,
    pub applied_lag: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminCheckpointGroup {
    pub group: String,
    pub namespaces: Vec<NamespaceId>,
    pub git_ref: String,
    pub dirty: bool,
    pub in_flight: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_checkpoint_wall_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminMetricsOutput {
    pub counters: Vec<AdminMetricSample>,
    pub gauges: Vec<AdminMetricSample>,
    pub histograms: Vec<AdminMetricHistogram>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminDoctorOutput {
    pub report: AdminHealthReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminScrubOutput {
    pub report: AdminHealthReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminHealthReport {
    pub checked_at_ms: u64,
    pub stats: AdminHealthStats,
    pub checks: Vec<AdminHealthCheck>,
    pub summary: AdminHealthSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_clock_anomaly: Option<AdminClockAnomaly>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AdminHealthStats {
    pub namespaces: usize,
    pub segments_checked: usize,
    pub records_checked: u64,
    pub index_offsets_checked: u64,
    pub checkpoint_groups_checked: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AdminHealthStatus {
    Pass,
    Warn,
    Fail,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AdminHealthSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AdminHealthRisk {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AdminHealthCheckId {
    WalFrames,
    WalHashes,
    IndexOffsets,
    CheckpointCache,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdminHealthEvidenceCode {
    SegmentHeaderInvalid,
    FrameHeaderInvalid,
    FrameTruncated,
    FrameCrcMismatch,
    RecordDecodeInvalid,
    EventBodyDecodeInvalid,
    RecordHeaderMismatch,
    RecordShaMismatch,
    IndexOffsetInvalid,
    IndexSegmentMissing,
    IndexOpenFailed,
    CheckpointCacheInvalid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminHealthEvidence {
    pub code: AdminHealthEvidenceCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<NamespaceId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<ReplicaId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_id: Option<SegmentId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminHealthCheck {
    pub id: AdminHealthCheckId,
    pub status: AdminHealthStatus,
    pub severity: AdminHealthSeverity,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<AdminHealthEvidence>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suggested_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminHealthSummary {
    pub risk: AdminHealthRisk,
    pub safe_to_accept_writes: bool,
    pub safe_to_prune_wal: bool,
    pub safe_to_rebuild_index: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminFingerprintOutput {
    pub mode: AdminFingerprintMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample: Option<AdminFingerprintSample>,
    pub watermarks_applied: Watermarks<Applied>,
    pub watermarks_durable: Watermarks<Durable>,
    pub namespaces: Vec<AdminNamespaceFingerprint>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdminFingerprintMode {
    Full,
    Sample,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminFingerprintSample {
    pub shard_count: u16,
    pub nonce: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminNamespaceFingerprint {
    pub namespace: NamespaceId,
    pub state_sha256: ContentHash,
    pub tombstones_sha256: ContentHash,
    pub deps_sha256: ContentHash,
    pub namespace_root: ContentHash,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shards: Vec<AdminFingerprintShard>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdminFingerprintKind {
    State,
    Tombstones,
    Deps,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminFingerprintShard {
    pub kind: AdminFingerprintKind,
    pub index: u8,
    pub sha256: ContentHash,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminReloadPoliciesOutput {
    pub applied: Vec<AdminPolicyDiff>,
    pub requires_restart: Vec<AdminPolicyDiff>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminPolicyDiff {
    pub namespace: NamespaceId,
    pub changes: Vec<AdminPolicyChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminPolicyChange {
    pub field: String,
    pub before: String,
    pub after: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminRotateReplicaIdOutput {
    pub old_replica_id: ReplicaId,
    pub new_replica_id: ReplicaId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminMaintenanceModeOutput {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminRebuildIndexOutput {
    pub stats: AdminRebuildIndexStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminRebuildIndexStats {
    pub segments_scanned: usize,
    pub records_indexed: usize,
    pub segments_truncated: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tail_truncations: Vec<AdminRebuildIndexTruncation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminRebuildIndexTruncation {
    pub namespace: NamespaceId,
    pub segment_id: SegmentId,
    pub truncated_from_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminMetricSample {
    pub name: String,
    pub value: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<AdminMetricLabel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminMetricHistogram {
    pub name: String,
    pub count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p95: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<AdminMetricLabel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminMetricLabel {
    pub key: String,
    pub value: String,
}

// =============================================================================
// Status / Stats
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SyncWarning {
    Fetch {
        message: String,
        at_wall_ms: u64,
    },
    Diverged {
        local_oid: String,
        remote_oid: String,
        at_wall_ms: u64,
    },
    ForcePush {
        previous_remote_oid: String,
        remote_oid: String,
        at_wall_ms: u64,
    },
    ClockSkew {
        delta_ms: i64,
        at_wall_ms: u64,
    },
    WalTailTruncated {
        namespace: NamespaceId,
        #[serde(skip_serializing_if = "Option::is_none")]
        segment_id: Option<SegmentId>,
        truncated_from_offset: u64,
        at_wall_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncStatus {
    pub dirty: bool,
    pub sync_in_progress: bool,
    pub last_sync_wall_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_wall_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_in_ms: Option<u64>,
    pub consecutive_failures: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<SyncWarning>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusSummary {
    pub total_issues: usize,
    pub open_issues: usize,
    pub in_progress_issues: usize,
    pub blocked_issues: usize,
    pub closed_issues: usize,
    pub ready_issues: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tombstone_issues: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub epics_eligible_for_closure: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusOutput {
    pub summary: StatusSummary,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync: Option<SyncStatus>,
}

// =============================================================================
// Blocked / Stale
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockedIssue {
    #[serde(flatten)]
    pub issue: IssueSummary,

    pub blocked_by_count: usize,
    pub blocked_by: Vec<String>,
}

// =============================================================================
// Ready
// =============================================================================

/// Ready result with summary counts for context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyResult {
    pub issues: Vec<IssueSummary>,
    pub blocked_count: usize,
    pub closed_count: usize,
}

// =============================================================================
// Epic
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpicStatus {
    pub epic: IssueSummary,
    pub total_children: usize,
    pub closed_children: usize,
    pub eligible_for_close: bool,
}

// =============================================================================
// Count
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountGroup {
    pub group: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CountResult {
    Simple {
        count: usize,
    },
    Grouped {
        total: usize,
        groups: Vec<CountGroup>,
    },
}

// =============================================================================
// Deleted
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletedLookup {
    pub found: bool,
    pub id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<Tombstone>,
}

// =============================================================================
// Notes
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Note {
    pub id: String,
    pub content: String,
    pub author: String,
    pub at: WriteStamp,
}

impl From<&crate::core::Note> for Note {
    fn from(n: &crate::core::Note) -> Self {
        Self {
            id: n.id.as_str().to_string(),
            content: n.content.clone(),
            author: n.author.as_str().to_string(),
            at: n.at.clone(),
        }
    }
}

// =============================================================================
// Realtime subscriptions
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventHlcMax {
    pub actor_id: ActorId,
    pub physical_ms: u64,
    pub logical: u32,
}

impl From<&HlcMax> for EventHlcMax {
    fn from(max: &HlcMax) -> Self {
        Self {
            actor_id: max.actor_id.clone(),
            physical_ms: max.physical_ms,
            logical: max.logical,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBody {
    pub envelope_v: u32,
    pub store: StoreIdentity,
    pub namespace: NamespaceId,
    pub origin_replica_id: ReplicaId,
    pub origin_seq: Seq1,
    pub event_time_ms: u64,
    pub txn_id: TxnId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_request_id: Option<ClientRequestId>,
    pub kind: String,
    pub delta: TxnDeltaV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hlc_max: Option<EventHlcMax>,
}

impl From<&crate::core::EventBody> for EventBody {
    fn from(body: &crate::core::EventBody) -> Self {
        Self {
            envelope_v: body.envelope_v,
            store: body.store,
            namespace: body.namespace.clone(),
            origin_replica_id: body.origin_replica_id,
            origin_seq: body.origin_seq,
            event_time_ms: body.event_time_ms,
            txn_id: body.txn_id,
            client_request_id: body.client_request_id,
            kind: body.kind.as_str().to_string(),
            delta: body.delta.clone(),
            hlc_max: body.hlc_max.as_ref().map(EventHlcMax::from),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvent {
    pub event_id: EventId,
    pub sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_sha256: Option<String>,
    pub body: EventBody,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_bytes_hex: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeInfo {
    pub namespace: NamespaceId,
    pub watermarks_applied: Watermarks<Applied>,
}

// =============================================================================
// Dependencies
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepEdge {
    pub from: String,
    pub to: String,
    pub kind: String,
    pub created_at: WriteStamp,
    pub created_by: String,
    pub deleted_at: Option<WriteStamp>,
    pub deleted_by: Option<String>,
}

impl From<(&CoreDepKey, &CoreDepEdge)> for DepEdge {
    fn from((key, edge): (&CoreDepKey, &CoreDepEdge)) -> Self {
        Self {
            from: key.from().as_str().to_string(),
            to: key.to().as_str().to_string(),
            kind: key.kind().as_str().to_string(),
            created_at: edge.created.at.clone(),
            created_by: edge.created.by.as_str().to_string(),
            deleted_at: edge.deleted_stamp().map(|s| s.at.clone()),
            deleted_by: edge.deleted_stamp().map(|s| s.by.as_str().to_string()),
        }
    }
}

// =============================================================================
// Tombstones
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tombstone {
    pub id: String,
    pub deleted_at: WriteStamp,
    pub deleted_by: String,
    pub reason: Option<String>,
}

impl From<&CoreTombstone> for Tombstone {
    fn from(t: &CoreTombstone) -> Self {
        Self {
            id: t.id.as_str().to_string(),
            deleted_at: t.deleted.at.clone(),
            deleted_by: t.deleted.by.as_str().to_string(),
            reason: t.reason.clone(),
        }
    }
}

// =============================================================================
// Issues
// =============================================================================

/// Full issue representation (includes notes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Issue {
    pub id: String,
    pub title: String,
    pub description: String,
    pub design: Option<String>,
    pub acceptance_criteria: Option<String>,
    pub status: String,
    pub priority: u8,
    #[serde(rename = "type")]
    pub issue_type: String,
    pub labels: Vec<String>,

    pub assignee: Option<String>,
    pub assignee_at: Option<WriteStamp>,
    pub assignee_expires: Option<WallClock>,

    pub created_at: WriteStamp,
    pub created_by: String,
    pub created_on_branch: Option<String>,

    pub updated_at: WriteStamp,
    pub updated_by: String,

    pub closed_at: Option<WriteStamp>,
    pub closed_by: Option<String>,
    pub closed_reason: Option<String>,
    pub closed_on_branch: Option<String>,

    pub external_ref: Option<String>,
    pub source_repo: Option<String>,

    /// Optional time estimate in minutes (beads-go parity).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_minutes: Option<u32>,

    pub content_hash: String,

    pub notes: Vec<Note>,

    /// Incoming dependencies (edges where this issue is the target).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deps_incoming: Vec<DepEdge>,

    /// Outgoing dependencies (edges where this issue is the source).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deps_outgoing: Vec<DepEdge>,
}

/// Summary issue representation (no note bodies).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssueSummary {
    pub id: String,
    pub title: String,
    pub description: String,
    pub design: Option<String>,
    pub acceptance_criteria: Option<String>,
    pub status: String,
    pub priority: u8,
    #[serde(rename = "type")]
    pub issue_type: String,
    pub labels: Vec<String>,

    pub assignee: Option<String>,
    pub assignee_expires: Option<WallClock>,

    pub created_at: WriteStamp,
    pub created_by: String,

    pub updated_at: WriteStamp,
    pub updated_by: String,

    /// Optional time estimate in minutes (beads-go parity).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_minutes: Option<u32>,

    pub content_hash: String,

    pub note_count: usize,
}

impl Issue {
    pub fn from_bead(bead: &Bead) -> Self {
        let updated = bead.updated_stamp();

        let (assignee, assignee_at, assignee_expires) = match &bead.fields.claim.value {
            Claim::Claimed { assignee, expires } => (
                Some(assignee.as_str().to_string()),
                Some(bead.fields.claim.stamp.at.clone()),
                *expires,
            ),
            Claim::Unclaimed => (None, None, None),
        };

        let (closed_at, closed_by, closed_reason, closed_on_branch) =
            match &bead.fields.workflow.value {
                Workflow::Closed(c) => (
                    Some(bead.fields.workflow.stamp.at.clone()),
                    Some(bead.fields.workflow.stamp.by.as_str().to_string()),
                    c.reason.clone(),
                    c.on_branch.clone(),
                ),
                _ => (None, None, None, None),
            };

        let notes = bead.notes.sorted().into_iter().map(Note::from).collect();

        Self {
            id: bead.core.id.as_str().to_string(),
            title: bead.fields.title.value.clone(),
            description: bead.fields.description.value.clone(),
            design: bead.fields.design.value.clone(),
            acceptance_criteria: bead.fields.acceptance_criteria.value.clone(),
            status: bead.fields.workflow.value.status().to_string(),
            priority: bead.fields.priority.value.value(),
            issue_type: bead.fields.bead_type.value.as_str().to_string(),
            labels: bead
                .fields
                .labels
                .value
                .iter()
                .map(|l| l.as_str().to_string())
                .collect(),
            assignee,
            assignee_at,
            assignee_expires,
            created_at: bead.core.created().at.clone(),
            created_by: bead.core.created().by.as_str().to_string(),
            created_on_branch: bead.core.created_on_branch().map(|s| s.to_string()),
            updated_at: updated.at.clone(),
            updated_by: updated.by.as_str().to_string(),
            closed_at,
            closed_by,
            closed_reason,
            closed_on_branch,
            external_ref: bead.fields.external_ref.value.clone(),
            source_repo: bead.fields.source_repo.value.clone(),
            estimated_minutes: bead.fields.estimated_minutes.value,
            content_hash: bead.content_hash().to_hex(),
            notes,
            deps_incoming: Vec::new(),
            deps_outgoing: Vec::new(),
        }
    }
}

impl IssueSummary {
    pub fn from_bead(bead: &Bead) -> Self {
        let updated = bead.updated_stamp();
        Self {
            id: bead.core.id.as_str().to_string(),
            title: bead.fields.title.value.clone(),
            description: bead.fields.description.value.clone(),
            design: bead.fields.design.value.clone(),
            acceptance_criteria: bead.fields.acceptance_criteria.value.clone(),
            status: bead.fields.workflow.value.status().to_string(),
            priority: bead.fields.priority.value.value(),
            issue_type: bead.fields.bead_type.value.as_str().to_string(),
            labels: bead
                .fields
                .labels
                .value
                .iter()
                .map(|l| l.as_str().to_string())
                .collect(),
            assignee: bead
                .fields
                .claim
                .value
                .assignee()
                .map(|a| a.as_str().to_string()),
            assignee_expires: bead.fields.claim.value.expires(),
            created_at: bead.core.created().at.clone(),
            created_by: bead.core.created().by.as_str().to_string(),
            updated_at: updated.at.clone(),
            updated_by: updated.by.as_str().to_string(),
            estimated_minutes: bead.fields.estimated_minutes.value,
            content_hash: bead.content_hash().to_hex(),
            note_count: bead.notes.len(),
        }
    }

    pub fn from_issue(issue: &Issue) -> Self {
        Self {
            id: issue.id.clone(),
            title: issue.title.clone(),
            description: issue.description.clone(),
            design: issue.design.clone(),
            acceptance_criteria: issue.acceptance_criteria.clone(),
            status: issue.status.clone(),
            priority: issue.priority,
            issue_type: issue.issue_type.clone(),
            labels: issue.labels.clone(),
            assignee: issue.assignee.clone(),
            assignee_expires: issue.assignee_expires,
            created_at: issue.created_at.clone(),
            created_by: issue.created_by.clone(),
            updated_at: issue.updated_at.clone(),
            updated_by: issue.updated_by.clone(),
            estimated_minutes: issue.estimated_minutes,
            content_hash: issue.content_hash.clone(),
            note_count: issue.notes.len(),
        }
    }
}
