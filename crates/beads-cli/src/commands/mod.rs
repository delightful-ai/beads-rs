use beads_api::QueryResult;
use beads_core::CoreError;
use beads_surface::ipc::{IpcError, ResponsePayload};
use beads_surface::ops::OpResult;

pub mod blocked;
pub mod claim;
pub mod close;
pub mod common;
pub mod count;
pub mod create;
pub mod daemon;
pub mod delete;
pub mod init;
pub mod list;
pub mod onboard;
pub mod prime;
pub mod ready;
pub mod reopen;
pub mod search;
pub mod setup;
pub mod show;
pub mod stale;
pub mod status;
pub mod sync;
pub mod unclaim;
pub mod update;

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
        ResponsePayload::Query(QueryResult::Issue(_)) => crate::render::print_json(payload)?,
        ResponsePayload::Query(QueryResult::Issues(views)) => {
            crate::render::print_line(&list::render_issue_list_opts(views, false))?
        }
        ResponsePayload::Query(QueryResult::DepTree { .. }) => crate::render::print_json(payload)?,
        ResponsePayload::Query(QueryResult::Deps { .. }) => crate::render::print_json(payload)?,
        ResponsePayload::Query(QueryResult::DepCycles(_)) => crate::render::print_json(payload)?,
        ResponsePayload::Query(QueryResult::Status(_)) => crate::render::print_json(payload)?,
        ResponsePayload::Query(QueryResult::Blocked(blocked)) => {
            crate::render::print_line(&blocked::render_blocked(blocked))?
        }
        ResponsePayload::Query(QueryResult::Ready(result)) => crate::render::print_line(
            &ready::render_ready(&result.issues, result.blocked_count, result.closed_count),
        )?,
        ResponsePayload::Query(QueryResult::Stale(issues)) => {
            crate::render::print_line(&list::render_issue_list_opts(issues, false))?
        }
        ResponsePayload::Query(QueryResult::Count(result)) => {
            crate::render::print_line(&count::render_count(result))?
        }
        ResponsePayload::Query(QueryResult::Deleted(_)) => crate::render::print_json(payload)?,
        ResponsePayload::Query(QueryResult::DeletedLookup(_)) => {
            crate::render::print_json(payload)?
        }
        ResponsePayload::Query(QueryResult::Validation { warnings }) => {
            if warnings.is_empty() {
                crate::render::print_line("ok")?
            } else {
                crate::render::print_line(&warnings.join("\n"))?
            }
        }
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
            OpResult::Created { .. }
            | OpResult::Updated { .. }
            | OpResult::Closed { .. }
            | OpResult::Reopened { .. }
            | OpResult::Deleted { .. }
            | OpResult::DepAdded { .. }
            | OpResult::DepRemoved { .. }
            | OpResult::NoteAdded { .. }
            | OpResult::Claimed { .. }
            | OpResult::Unclaimed { .. }
            | OpResult::ClaimExtended { .. } => crate::render::print_json(payload)?,
        },
        ResponsePayload::Query(
            QueryResult::Notes(_)
            | QueryResult::EpicStatus(_)
            | QueryResult::DaemonInfo(_)
            | QueryResult::AdminStatus(_)
            | QueryResult::AdminMetrics(_)
            | QueryResult::AdminDoctor(_)
            | QueryResult::AdminScrub(_)
            | QueryResult::AdminFlush(_)
            | QueryResult::AdminCheckpoint(_)
            | QueryResult::AdminFingerprint(_)
            | QueryResult::AdminReloadPolicies(_)
            | QueryResult::AdminRotateReplicaId(_)
            | QueryResult::AdminReloadReplication(_)
            | QueryResult::AdminReloadLimits(_)
            | QueryResult::AdminMaintenanceMode(_)
            | QueryResult::AdminRebuildIndex(_)
            | QueryResult::AdminFsck(_)
            | QueryResult::AdminStoreUnlock(_)
            | QueryResult::AdminStoreLockInfo(_),
        ) => crate::render::print_json(payload)?,
    }

    Ok(())
}
