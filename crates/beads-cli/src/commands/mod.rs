use beads_api::{
    AdminCheckpointOutput, AdminDoctorOutput, AdminFingerprintOutput, AdminFlushOutput,
    AdminFsckOutput, AdminMaintenanceModeOutput, AdminMetricsOutput, AdminRebuildIndexOutput,
    AdminReloadLimitsOutput, AdminReloadPoliciesOutput, AdminReloadReplicationOutput,
    AdminRotateReplicaIdOutput, AdminScrubOutput, AdminStatusOutput, AdminStoreLockInfoOutput,
    AdminStoreUnlockOutput, BlockedIssue, CountResult, DaemonInfo, DeletedLookup, DepCycles,
    DepEdge, EpicStatus, Issue, IssueSummary, Note, ReadyResult, StatusOutput, Tombstone,
};
use beads_core::CoreError;
use beads_surface::ipc::{IpcError, ResponsePayload};

pub mod admin;
pub mod blocked;
pub mod claim;
pub mod close;
pub mod comments;
pub mod common;
pub mod count;
pub mod create;
pub mod daemon;
pub mod delete;
pub mod deleted;
pub mod dep;
pub mod epic;
pub mod init;
pub mod label;
pub mod list;
pub mod migrate;
pub mod onboard;
pub mod prime;
pub mod ready;
pub mod reopen;
pub mod search;
pub mod setup;
pub mod show;
pub mod stale;
pub mod status;
pub mod store;
pub mod subscribe;
pub mod sync;
pub mod unclaim;
pub mod update;
pub mod upgrade;

#[derive(Debug, Clone, Copy, Default)]
struct CliCommandRenderer;

impl crate::render::HumanRenderer for CliCommandRenderer {
    fn render_created(&self, id: &str) -> String {
        create::render_created(id)
    }

    fn render_updated(&self, id: &str) -> String {
        update::render_updated(id)
    }

    fn render_closed(&self, id: &str) -> String {
        close::render_closed(id)
    }

    fn render_reopened(&self, id: &str) -> String {
        reopen::render_reopened(id)
    }

    fn render_deleted_op(&self, id: &str) -> String {
        delete::render_deleted_op(id)
    }

    fn render_dep_added(&self, from: &str, to: &str) -> String {
        dep::render_dep_added(from, to)
    }

    fn render_dep_removed(&self, from: &str, to: &str) -> String {
        dep::render_dep_removed(from, to)
    }

    fn render_note_added(&self, bead_id: &str) -> String {
        comments::render_comment_added(bead_id)
    }

    fn render_claimed(&self, id: &str, expires: u64) -> String {
        claim::render_claimed(id, expires)
    }

    fn render_unclaimed(&self, id: &str) -> String {
        unclaim::render_unclaimed(id)
    }

    fn render_claim_extended(&self, id: &str, expires: u64) -> String {
        claim::render_claim_extended(id, expires)
    }

    fn render_issue(&self, issue: &Issue) -> String {
        show::render_issue_detail(issue)
    }

    fn render_issues(&self, issues: &[IssueSummary]) -> String {
        list::render_issue_list_opts(issues, false)
    }

    fn render_dep_tree(&self, root: &str, edges: &[DepEdge]) -> String {
        dep::render_dep_tree(root, edges)
    }

    fn render_deps(&self, incoming: &[DepEdge], outgoing: &[DepEdge]) -> String {
        dep::render_deps(incoming, outgoing)
    }

    fn render_dep_cycles(&self, out: &DepCycles) -> String {
        dep::render_dep_cycles(out)
    }

    fn render_notes(&self, notes: &[Note]) -> String {
        comments::render_notes(notes)
    }

    fn render_status(&self, out: &StatusOutput) -> String {
        status::render_status(out)
    }

    fn render_blocked(&self, blocked: &[BlockedIssue]) -> String {
        crate::commands::blocked::render_blocked(blocked)
    }

    fn render_ready(&self, result: &ReadyResult) -> String {
        ready::render_ready(&result.issues, result.blocked_count, result.closed_count)
    }

    fn render_count(&self, result: &CountResult) -> String {
        count::render_count(result)
    }

    fn render_deleted_list(&self, tombs: &[Tombstone]) -> String {
        deleted::render_deleted(tombs)
    }

    fn render_deleted_lookup(&self, out: &DeletedLookup) -> String {
        deleted::render_deleted_lookup(out)
    }

    fn render_epic_status(&self, statuses: &[EpicStatus]) -> String {
        epic::render_epic_statuses(statuses)
    }

    fn render_daemon_info(&self, info: &DaemonInfo) -> String {
        daemon::render_daemon_info(info)
    }

    fn render_admin_status(&self, status: &AdminStatusOutput) -> String {
        admin::render_admin_status(status)
    }

    fn render_admin_metrics(&self, metrics: &AdminMetricsOutput) -> String {
        admin::render_admin_metrics(metrics)
    }

    fn render_admin_doctor(&self, out: &AdminDoctorOutput) -> String {
        admin::render_admin_doctor(out)
    }

    fn render_admin_scrub(&self, out: &AdminScrubOutput) -> String {
        admin::render_admin_scrub(out)
    }

    fn render_admin_flush(&self, out: &AdminFlushOutput) -> String {
        admin::render_admin_flush(out)
    }

    fn render_admin_checkpoint(&self, out: &AdminCheckpointOutput) -> String {
        admin::render_admin_checkpoint(out)
    }

    fn render_admin_fingerprint(&self, out: &AdminFingerprintOutput) -> String {
        admin::render_admin_fingerprint(out)
    }

    fn render_admin_reload_policies(&self, out: &AdminReloadPoliciesOutput) -> String {
        admin::render_admin_reload_policies(out)
    }

    fn render_admin_reload_replication(&self, out: &AdminReloadReplicationOutput) -> String {
        admin::render_admin_reload_replication(out)
    }

    fn render_admin_reload_limits(&self, out: &AdminReloadLimitsOutput) -> String {
        admin::render_admin_reload_limits(out)
    }

    fn render_admin_rotate_replica_id(&self, out: &AdminRotateReplicaIdOutput) -> String {
        admin::render_admin_rotate_replica_id(out)
    }

    fn render_admin_maintenance_mode(&self, out: &AdminMaintenanceModeOutput) -> String {
        admin::render_admin_maintenance(out)
    }

    fn render_admin_rebuild_index(&self, out: &AdminRebuildIndexOutput) -> String {
        admin::render_admin_rebuild_index(out)
    }

    fn render_admin_fsck(&self, out: &AdminFsckOutput) -> String {
        store::render_admin_fsck(out)
    }

    fn render_admin_store_unlock(&self, out: &AdminStoreUnlockOutput) -> String {
        store::render_admin_store_unlock(out)
    }

    fn render_admin_store_lock_info(&self, out: &AdminStoreLockInfoOutput) -> String {
        store::render_admin_store_lock_info(out)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error(transparent)]
    Core(#[from] CoreError),

    #[error(transparent)]
    Ipc(#[from] IpcError),

    #[error(transparent)]
    Runtime(#[from] crate::runtime::RuntimeError),

    #[error(transparent)]
    Validation(#[from] crate::validation::ValidationError),

    #[error(transparent)]
    Filter(#[from] crate::filters::FilterError),
}

pub type CommandResult<T> = std::result::Result<T, CommandError>;

fn render_ok_human(payload: &ResponsePayload) -> String {
    crate::render::render_human(payload, &CliCommandRenderer)
}

pub(crate) fn print_ok(payload: &ResponsePayload, json: bool) -> CommandResult<()> {
    if json {
        crate::render::print_json(payload)?;
        return Ok(());
    }
    crate::render::print_line(&render_ok_human(payload))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use beads_api::QueryResult;
    use beads_core::{BeadId, DurabilityReceipt, StoreEpoch, StoreId, StoreIdentity, TxnId};
    use beads_surface::ipc::OpResponse;
    use beads_surface::ops::OpResult;
    use uuid::Uuid;

    fn created_op_payload() -> ResponsePayload {
        let receipt = DurabilityReceipt::local_fsync_defaults(
            StoreIdentity::new(StoreId::new(Uuid::from_bytes([1u8; 16])), StoreEpoch::ZERO),
            TxnId::new(Uuid::from_bytes([2u8; 16])),
            Vec::new(),
            0,
        );
        ResponsePayload::Op(OpResponse::new(
            OpResult::Created {
                id: BeadId::parse("bd-123").expect("valid bead id"),
            },
            receipt,
        ))
    }

    #[test]
    fn render_ok_human_renders_basic_status_payloads() {
        assert_eq!(render_ok_human(&ResponsePayload::synced()), "synced");
        assert_eq!(render_ok_human(&ResponsePayload::refreshed()), "refreshed");
        assert_eq!(
            render_ok_human(&ResponsePayload::initialized()),
            "initialized"
        );
        assert_eq!(
            render_ok_human(&ResponsePayload::shutting_down()),
            "shutting down"
        );
    }

    #[test]
    fn render_ok_human_renders_validation_query() {
        assert_eq!(
            render_ok_human(&ResponsePayload::Query(QueryResult::Validation {
                warnings: Vec::new()
            })),
            "ok"
        );
        assert_eq!(
            render_ok_human(&ResponsePayload::Query(QueryResult::Validation {
                warnings: vec!["a".to_string(), "b".to_string()]
            })),
            "a\nb"
        );
    }

    #[test]
    fn render_ok_human_renders_op_and_query_variants() {
        let op_payload = created_op_payload();
        assert_eq!(
            render_ok_human(&op_payload),
            create::render_created("bd-123")
        );

        let issue_summaries: Vec<IssueSummary> = Vec::new();
        assert_eq!(
            render_ok_human(&ResponsePayload::Query(QueryResult::Issues(
                issue_summaries.clone()
            ))),
            list::render_issue_list_opts(&issue_summaries, false)
        );
    }
}
