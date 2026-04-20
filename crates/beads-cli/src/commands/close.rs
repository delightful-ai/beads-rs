use beads_api::QueryResult;
use beads_core::IssueStatus;
use beads_surface::ipc::{AddNotePayload, ClosePayload, Request, ResponsePayload};
use clap::Args;

use super::common::{fetch_issue, normalize_close_reason};
use super::{CommandResult, print_ok};
use crate::render::print_line;
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::normalize_bead_id;

#[derive(Args, Debug)]
pub struct CloseArgs {
    pub id: String,

    #[arg(long)]
    pub reason: Option<String>,

    /// Add a human note alongside the close operation.
    #[arg(long = "note", alias = "notes", allow_hyphen_values = true)]
    pub note: Option<String>,
}

pub fn handle(ctx: &CliRuntimeCtx, args: CloseArgs) -> CommandResult<()> {
    let id = normalize_bead_id(&args.id)?;
    let reason = normalize_close_reason(args.reason)?;
    let status =
        IssueStatus::from_close_reason(reason.as_deref()).expect("normalized close reason");
    let req = Request::Close {
        ctx: ctx.mutation_ctx(),
        payload: ClosePayload {
            id: id.clone(),
            reason,
            on_branch: None,
        },
    };
    let _ = send(&req)?;
    if let Some(content) = args.note {
        let note = Request::AddNote {
            ctx: ctx.mutation_ctx(),
            payload: AddNotePayload {
                id: id.clone(),
                content,
            },
        };
        let _ = send(&note)?;
    }
    if ctx.json {
        let issue = fetch_issue(ctx, &id)?;
        return print_ok(&ResponsePayload::Query(QueryResult::Issue(issue)), true);
    }
    print_line(&render_closed_with_reason(id.as_str(), status.as_str()))?;
    Ok(())
}

pub fn render_closed(id: &str) -> String {
    format!("✓ Closed {id}")
}

fn render_closed_with_reason(id: &str, reason: &str) -> String {
    format!("✓ Closed {id}: {reason}")
}
