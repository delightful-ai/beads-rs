//! Admin and daemon status schemas.

use serde::{Deserialize, Serialize};

use crate::core::{
    Applied, ContentHash, Durable, NamespaceId, ReplicaId, ReplicaRole, SegmentId, StoreId,
    Watermarks,
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
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub wal_warnings: Vec<AdminWalWarning>,
    pub replication: Vec<AdminReplicationPeer>,
    pub replica_liveness: Vec<AdminReplicaLiveness>,
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
    pub growth: AdminWalGrowth,
    pub segments: Vec<AdminWalSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminWalGrowth {
    pub window_ms: u64,
    pub segments: u64,
    pub bytes: u64,
    pub segments_per_sec: u64,
    pub bytes_per_sec: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminWalWarning {
    pub namespace: NamespaceId,
    pub kind: AdminWalWarningKind,
    pub observed: u64,
    pub limit: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdminWalWarningKind {
    TotalBytesExceeded,
    SegmentCountExceeded,
    GrowthBytesExceeded,
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
pub struct AdminReplicaLiveness {
    pub replica_id: ReplicaId,
    pub last_seen_ms: u64,
    pub last_handshake_ms: u64,
    pub role: ReplicaRole,
    pub durability_eligible: bool,
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
pub struct AdminFlushOutput {
    pub namespace: NamespaceId,
    pub flushed_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment: Option<AdminFlushSegment>,
    pub checkpoint_now: bool,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub checkpoint_groups: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminFlushSegment {
    pub segment_id: SegmentId,
    pub created_at_ms: u64,
    pub path: String,
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
pub struct AdminReloadReplicationOutput {
    pub store_id: StoreId,
    pub roster_present: bool,
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
