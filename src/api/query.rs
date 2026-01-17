//! Query result schemas.

use serde::{Deserialize, Serialize};

use crate::api::admin::{
    AdminDoctorOutput, AdminFingerprintOutput, AdminFlushOutput, AdminMaintenanceModeOutput,
    AdminMetricsOutput, AdminRebuildIndexOutput, AdminReloadPoliciesOutput,
    AdminRotateReplicaIdOutput, AdminScrubOutput, AdminStatusOutput, DaemonInfo,
};
use crate::api::deps::{DepCycles, DepEdge};
use crate::api::issues::{
    BlockedIssue, CountResult, DeletedLookup, EpicStatus, Issue, IssueSummary, Note, ReadyResult,
    StatusOutput, Tombstone,
};
use crate::core::BeadId;

/// Result of a query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "result", content = "data", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum QueryResult {
    /// Single issue result.
    Issue(Issue),

    /// List of issues (summaries).
    Issues(Vec<IssueSummary>),

    /// Dependency tree.
    DepTree { root: BeadId, edges: Vec<DepEdge> },

    /// Dependencies for a bead.
    Deps {
        incoming: Vec<DepEdge>,
        outgoing: Vec<DepEdge>,
    },

    /// Dependency cycles.
    DepCycles(DepCycles),

    /// Notes for a bead.
    Notes(Vec<Note>),

    /// Issue database status (counts, etc).
    Status(StatusOutput),

    /// Blocked issues.
    Blocked(Vec<BlockedIssue>),

    /// Ready issues with summary counts.
    Ready(ReadyResult),

    /// Stale issues.
    Stale(Vec<IssueSummary>),

    /// Count results.
    Count(CountResult),

    /// Deleted issues (tombstones) list.
    Deleted(Vec<Tombstone>),

    /// Deleted issue lookup by id.
    DeletedLookup(DeletedLookup),

    /// Epic completion status.
    EpicStatus(Vec<EpicStatus>),

    /// Validation result.
    Validation { warnings: Vec<String> },

    /// Daemon info (handshake).
    DaemonInfo(DaemonInfo),

    /// Admin status snapshot.
    AdminStatus(AdminStatusOutput),

    /// Admin metrics snapshot.
    AdminMetrics(AdminMetricsOutput),

    /// Admin doctor report.
    AdminDoctor(AdminDoctorOutput),

    /// Admin scrub report.
    AdminScrub(AdminScrubOutput),

    /// Admin flush report.
    AdminFlush(AdminFlushOutput),

    /// Admin fingerprint report.
    AdminFingerprint(AdminFingerprintOutput),

    /// Admin reload policies report.
    AdminReloadPolicies(AdminReloadPoliciesOutput),

    /// Admin rotate replica id report.
    AdminRotateReplicaId(AdminRotateReplicaIdOutput),

    /// Maintenance mode toggle.
    AdminMaintenanceMode(AdminMaintenanceModeOutput),

    /// Rebuild index outcome.
    AdminRebuildIndex(AdminRebuildIndexOutput),
}
