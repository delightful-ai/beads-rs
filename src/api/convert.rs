use crate::api::{
    BlockedIssue, CountGroup, CountResult, DaemonInfo, DeletedLookup, DepCycles, DepEdge,
    EpicStatus, Issue, IssueSummary, Note, QueryResult, ReadyResult, StatusOutput, StatusSummary,
    SyncStatus, SyncWarning, Tombstone,
};
use crate::daemon::query as internal_query;
use crate::daemon::query_model as model;

impl From<model::DaemonInfo> for DaemonInfo {
    fn from(value: model::DaemonInfo) -> Self {
        Self {
            version: value.version,
            protocol_version: value.protocol_version,
            pid: value.pid,
        }
    }
}

impl From<model::SyncWarning> for SyncWarning {
    fn from(value: model::SyncWarning) -> Self {
        match value {
            model::SyncWarning::Fetch {
                message,
                at_wall_ms,
            } => SyncWarning::Fetch {
                message,
                at_wall_ms,
            },
            model::SyncWarning::Diverged {
                local_oid,
                remote_oid,
                at_wall_ms,
            } => SyncWarning::Diverged {
                local_oid,
                remote_oid,
                at_wall_ms,
            },
            model::SyncWarning::ForcePush {
                previous_remote_oid,
                remote_oid,
                at_wall_ms,
            } => SyncWarning::ForcePush {
                previous_remote_oid,
                remote_oid,
                at_wall_ms,
            },
            model::SyncWarning::ClockSkew {
                delta_ms,
                at_wall_ms,
            } => SyncWarning::ClockSkew {
                delta_ms,
                at_wall_ms,
            },
            model::SyncWarning::WalTailTruncated {
                namespace,
                segment_id,
                truncated_from_offset,
                at_wall_ms,
            } => SyncWarning::WalTailTruncated {
                namespace,
                segment_id,
                truncated_from_offset,
                at_wall_ms,
            },
        }
    }
}

impl From<model::SyncStatus> for SyncStatus {
    fn from(value: model::SyncStatus) -> Self {
        Self {
            dirty: value.dirty,
            sync_in_progress: value.sync_in_progress,
            last_sync_wall_ms: value.last_sync_wall_ms,
            next_retry_wall_ms: value.next_retry_wall_ms,
            next_retry_in_ms: value.next_retry_in_ms,
            consecutive_failures: value.consecutive_failures,
            warnings: value.warnings.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<model::StatusSummary> for StatusSummary {
    fn from(value: model::StatusSummary) -> Self {
        Self {
            total_issues: value.total_issues,
            open_issues: value.open_issues,
            in_progress_issues: value.in_progress_issues,
            blocked_issues: value.blocked_issues,
            closed_issues: value.closed_issues,
            ready_issues: value.ready_issues,
            tombstone_issues: value.tombstone_issues,
            epics_eligible_for_closure: value.epics_eligible_for_closure,
        }
    }
}

impl From<model::StatusOutput> for StatusOutput {
    fn from(value: model::StatusOutput) -> Self {
        Self {
            summary: value.summary.into(),
            sync: value.sync.map(Into::into),
        }
    }
}

impl From<model::BlockedIssue> for BlockedIssue {
    fn from(value: model::BlockedIssue) -> Self {
        Self {
            issue: value.issue.into(),
            blocked_by_count: value.blocked_by_count,
            blocked_by: value.blocked_by,
        }
    }
}

impl From<model::ReadyResult> for ReadyResult {
    fn from(value: model::ReadyResult) -> Self {
        Self {
            issues: value.issues.into_iter().map(Into::into).collect(),
            blocked_count: value.blocked_count,
            closed_count: value.closed_count,
        }
    }
}

impl From<model::EpicStatus> for EpicStatus {
    fn from(value: model::EpicStatus) -> Self {
        Self {
            epic: value.epic.into(),
            total_children: value.total_children,
            closed_children: value.closed_children,
            eligible_for_close: value.eligible_for_close,
        }
    }
}

impl From<model::CountGroup> for CountGroup {
    fn from(value: model::CountGroup) -> Self {
        Self {
            group: value.group,
            count: value.count,
        }
    }
}

impl From<model::CountResult> for CountResult {
    fn from(value: model::CountResult) -> Self {
        match value {
            model::CountResult::Simple { count } => CountResult::Simple { count },
            model::CountResult::Grouped { total, groups } => CountResult::Grouped {
                total,
                groups: groups.into_iter().map(Into::into).collect(),
            },
        }
    }
}

impl From<model::DeletedLookup> for DeletedLookup {
    fn from(value: model::DeletedLookup) -> Self {
        Self {
            found: value.found,
            id: value.id,
            record: value.record.map(Into::into),
        }
    }
}

impl From<model::Note> for Note {
    fn from(value: model::Note) -> Self {
        Self {
            id: value.id,
            content: value.content,
            author: value.author,
            at: value.at,
        }
    }
}

impl From<model::DepEdge> for DepEdge {
    fn from(value: model::DepEdge) -> Self {
        Self {
            from: value.from,
            to: value.to,
            kind: value.kind,
        }
    }
}

impl From<model::DepCycles> for DepCycles {
    fn from(value: model::DepCycles) -> Self {
        Self {
            cycles: value.cycles,
        }
    }
}

impl From<model::Tombstone> for Tombstone {
    fn from(value: model::Tombstone) -> Self {
        Self {
            id: value.id,
            deleted_at: value.deleted_at,
            deleted_by: value.deleted_by,
            reason: value.reason,
        }
    }
}

impl From<model::Issue> for Issue {
    fn from(value: model::Issue) -> Self {
        Self {
            id: value.id,
            namespace: value.namespace,
            title: value.title,
            description: value.description,
            design: value.design,
            acceptance_criteria: value.acceptance_criteria,
            status: value.status,
            priority: value.priority,
            issue_type: value.issue_type,
            labels: value.labels,
            assignee: value.assignee,
            assignee_at: value.assignee_at,
            assignee_expires: value.assignee_expires,
            created_at: value.created_at,
            created_by: value.created_by,
            created_on_branch: value.created_on_branch,
            updated_at: value.updated_at,
            updated_by: value.updated_by,
            closed_at: value.closed_at,
            closed_by: value.closed_by,
            closed_reason: value.closed_reason,
            closed_on_branch: value.closed_on_branch,
            external_ref: value.external_ref,
            source_repo: value.source_repo,
            estimated_minutes: value.estimated_minutes,
            content_hash: value.content_hash,
            notes: value.notes.into_iter().map(Into::into).collect(),
            deps_incoming: value.deps_incoming.into_iter().map(Into::into).collect(),
            deps_outgoing: value.deps_outgoing.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<model::IssueSummary> for IssueSummary {
    fn from(value: model::IssueSummary) -> Self {
        Self {
            id: value.id,
            namespace: value.namespace,
            title: value.title,
            description: value.description,
            design: value.design,
            acceptance_criteria: value.acceptance_criteria,
            status: value.status,
            priority: value.priority,
            issue_type: value.issue_type,
            labels: value.labels,
            assignee: value.assignee,
            assignee_expires: value.assignee_expires,
            created_at: value.created_at,
            created_by: value.created_by,
            updated_at: value.updated_at,
            updated_by: value.updated_by,
            estimated_minutes: value.estimated_minutes,
            content_hash: value.content_hash,
            note_count: value.note_count,
        }
    }
}

impl From<internal_query::QueryResult> for QueryResult {
    fn from(value: internal_query::QueryResult) -> Self {
        match value {
            internal_query::QueryResult::Issue(issue) => QueryResult::Issue(issue.into()),
            internal_query::QueryResult::Issues(issues) => {
                QueryResult::Issues(issues.into_iter().map(Into::into).collect())
            }
            internal_query::QueryResult::DepTree { root, edges } => QueryResult::DepTree {
                root,
                edges: edges.into_iter().map(Into::into).collect(),
            },
            internal_query::QueryResult::Deps { incoming, outgoing } => QueryResult::Deps {
                incoming: incoming.into_iter().map(Into::into).collect(),
                outgoing: outgoing.into_iter().map(Into::into).collect(),
            },
            internal_query::QueryResult::DepCycles(cycles) => QueryResult::DepCycles(cycles.into()),
            internal_query::QueryResult::Notes(notes) => {
                QueryResult::Notes(notes.into_iter().map(Into::into).collect())
            }
            internal_query::QueryResult::Status(status) => QueryResult::Status(status.into()),
            internal_query::QueryResult::Blocked(blocked) => {
                QueryResult::Blocked(blocked.into_iter().map(Into::into).collect())
            }
            internal_query::QueryResult::Ready(ready) => QueryResult::Ready(ready.into()),
            internal_query::QueryResult::Stale(stale) => {
                QueryResult::Stale(stale.into_iter().map(Into::into).collect())
            }
            internal_query::QueryResult::Count(count) => QueryResult::Count(count.into()),
            internal_query::QueryResult::Deleted(deleted) => {
                QueryResult::Deleted(deleted.into_iter().map(Into::into).collect())
            }
            internal_query::QueryResult::DeletedLookup(out) => {
                QueryResult::DeletedLookup(out.into())
            }
            internal_query::QueryResult::EpicStatus(statuses) => {
                QueryResult::EpicStatus(statuses.into_iter().map(Into::into).collect())
            }
            internal_query::QueryResult::Validation { warnings } => {
                QueryResult::Validation { warnings }
            }
            internal_query::QueryResult::DaemonInfo(info) => QueryResult::DaemonInfo(info.into()),
            internal_query::QueryResult::AdminStatus(output) => QueryResult::AdminStatus(output),
            internal_query::QueryResult::AdminMetrics(output) => QueryResult::AdminMetrics(output),
            internal_query::QueryResult::AdminDoctor(output) => QueryResult::AdminDoctor(output),
            internal_query::QueryResult::AdminScrub(output) => QueryResult::AdminScrub(output),
            internal_query::QueryResult::AdminFlush(output) => QueryResult::AdminFlush(output),
            internal_query::QueryResult::AdminCheckpoint(output) => {
                QueryResult::AdminCheckpoint(output)
            }
            internal_query::QueryResult::AdminFingerprint(output) => {
                QueryResult::AdminFingerprint(output)
            }
            internal_query::QueryResult::AdminReloadPolicies(output) => {
                QueryResult::AdminReloadPolicies(output)
            }
            internal_query::QueryResult::AdminReloadReplication(output) => {
                QueryResult::AdminReloadReplication(output)
            }
            internal_query::QueryResult::AdminRotateReplicaId(output) => {
                QueryResult::AdminRotateReplicaId(output)
            }
            internal_query::QueryResult::AdminMaintenanceMode(output) => {
                QueryResult::AdminMaintenanceMode(output)
            }
            internal_query::QueryResult::AdminRebuildIndex(output) => {
                QueryResult::AdminRebuildIndex(output)
            }
        }
    }
}
