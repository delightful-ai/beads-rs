use beads_api::QueryResult;
use beads_core::CoreError;
use beads_surface::ipc::{IpcError, ResponsePayload};
use beads_surface::ops::OpResult;

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

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error(transparent)]
    Core(#[from] CoreError),

    #[error(transparent)]
    Ipc(#[from] IpcError),

    #[error(transparent)]
    Validation(#[from] crate::validation::ValidationError),

    #[error(transparent)]
    Filter(#[from] crate::filters::FilterError),
}

pub type CommandResult<T> = std::result::Result<T, CommandError>;

pub(crate) fn print_ok(payload: &ResponsePayload, json: bool) -> CommandResult<()> {
    if json {
        crate::render::print_json(payload)?;
        return Ok(());
    }

    match payload {
        ResponsePayload::Synced(_) => crate::render::print_line("synced")?,
        ResponsePayload::Refreshed(_) => crate::render::print_line("refreshed")?,
        ResponsePayload::Initialized(_) => crate::render::print_line("initialized")?,
        ResponsePayload::ShuttingDown(_) => crate::render::print_line("shutting down")?,
        ResponsePayload::Subscribed(sub) => crate::render::print_line(&format!(
            "subscribed to {}",
            sub.subscribed.namespace.as_str()
        ))?,
        ResponsePayload::Event(ev) => {
            crate::render::print_line(&format!("event {}", ev.event.event_id.origin_seq.get()))?
        }
        ResponsePayload::Op(op) => match &op.result {
            OpResult::Created { id } => {
                crate::render::print_line(&create::render_created(id.as_str()))?
            }
            OpResult::Updated { id } => {
                crate::render::print_line(&update::render_updated(id.as_str()))?
            }
            OpResult::Closed { id } => {
                crate::render::print_line(&close::render_closed(id.as_str()))?
            }
            OpResult::Reopened { id } => {
                crate::render::print_line(&reopen::render_reopened(id.as_str()))?
            }
            OpResult::Deleted { id } => {
                crate::render::print_line(&delete::render_deleted_op(id.as_str()))?
            }
            OpResult::DepAdded { from, to } => {
                crate::render::print_line(&dep::render_dep_added(from.as_str(), to.as_str()))?
            }
            OpResult::DepRemoved { from, to } => {
                crate::render::print_line(&dep::render_dep_removed(from.as_str(), to.as_str()))?
            }
            OpResult::NoteAdded { bead_id, .. } => {
                crate::render::print_line(&comments::render_comment_added(bead_id.as_str()))?
            }
            OpResult::Claimed { id, expires } => {
                crate::render::print_line(&claim::render_claimed(id.as_str(), expires.0))?
            }
            OpResult::Unclaimed { id } => {
                crate::render::print_line(&unclaim::render_unclaimed(id.as_str()))?
            }
            OpResult::ClaimExtended { id, expires } => {
                crate::render::print_line(&claim::render_claim_extended(id.as_str(), expires.0))?
            }
        },
        ResponsePayload::Query(query) => match query {
            QueryResult::Issue(issue) => {
                crate::render::print_line(&show::render_issue_detail(issue))?
            }
            QueryResult::Issues(views) => {
                crate::render::print_line(&list::render_issue_list_opts(views, false))?
            }
            QueryResult::DepTree { root, edges } => {
                crate::render::print_line(&dep::render_dep_tree(root.as_str(), edges))?
            }
            QueryResult::Deps { incoming, outgoing } => {
                crate::render::print_line(&dep::render_deps(incoming, outgoing))?
            }
            QueryResult::DepCycles(out) => crate::render::print_line(&dep::render_dep_cycles(out))?,
            QueryResult::Notes(notes) => crate::render::print_line(&comments::render_notes(notes))?,
            QueryResult::Status(out) => crate::render::print_line(&status::render_status(out))?,
            QueryResult::Blocked(blocked) => {
                crate::render::print_line(&blocked::render_blocked(blocked))?
            }
            QueryResult::Ready(result) => crate::render::print_line(&ready::render_ready(
                &result.issues,
                result.blocked_count,
                result.closed_count,
            ))?,
            QueryResult::Stale(issues) => {
                crate::render::print_line(&list::render_issue_list_opts(issues, false))?
            }
            QueryResult::Count(result) => crate::render::print_line(&count::render_count(result))?,
            QueryResult::Deleted(tombs) => {
                crate::render::print_line(&deleted::render_deleted(tombs))?
            }
            QueryResult::DeletedLookup(out) => {
                crate::render::print_line(&deleted::render_deleted_lookup(out))?
            }
            QueryResult::EpicStatus(statuses) => {
                crate::render::print_line(&epic::render_epic_statuses(statuses))?
            }
            QueryResult::Validation { warnings } => {
                if warnings.is_empty() {
                    crate::render::print_line("ok")?
                } else {
                    crate::render::print_line(&warnings.join("\n"))?
                }
            }
            QueryResult::DaemonInfo(_) => crate::render::print_json(payload)?,
            QueryResult::AdminStatus(status) => {
                crate::render::print_line(&admin::render_admin_status(status))?
            }
            QueryResult::AdminMetrics(metrics) => {
                crate::render::print_line(&admin::render_admin_metrics(metrics))?
            }
            QueryResult::AdminDoctor(out) => {
                crate::render::print_line(&admin::render_admin_doctor(out))?
            }
            QueryResult::AdminScrub(out) => {
                crate::render::print_line(&admin::render_admin_scrub(out))?
            }
            QueryResult::AdminFlush(out) => {
                crate::render::print_line(&admin::render_admin_flush(out))?
            }
            QueryResult::AdminCheckpoint(out) => {
                crate::render::print_line(&admin::render_admin_checkpoint(out))?
            }
            QueryResult::AdminFingerprint(out) => {
                crate::render::print_line(&admin::render_admin_fingerprint(out))?
            }
            QueryResult::AdminReloadPolicies(out) => {
                crate::render::print_line(&admin::render_admin_reload_policies(out))?
            }
            QueryResult::AdminRotateReplicaId(out) => {
                crate::render::print_line(&admin::render_admin_rotate_replica_id(out))?
            }
            QueryResult::AdminReloadReplication(out) => {
                crate::render::print_line(&admin::render_admin_reload_replication(out))?
            }
            QueryResult::AdminReloadLimits(out) => {
                crate::render::print_line(&admin::render_admin_reload_limits(out))?
            }
            QueryResult::AdminMaintenanceMode(out) => {
                crate::render::print_line(&admin::render_admin_maintenance(out))?
            }
            QueryResult::AdminRebuildIndex(out) => {
                crate::render::print_line(&admin::render_admin_rebuild_index(out))?
            }
            QueryResult::AdminFsck(out) => {
                crate::render::print_line(&store::render_admin_fsck(out))?
            }
            QueryResult::AdminStoreUnlock(out) => {
                crate::render::print_line(&store::render_admin_store_unlock(out))?
            }
            QueryResult::AdminStoreLockInfo(out) => {
                crate::render::print_line(&store::render_admin_store_lock_info(out))?
            }
        },
    }

    Ok(())
}
