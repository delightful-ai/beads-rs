use beads_surface::ipc::{DeletePayload, Request};
use clap::Args;
use serde::Serialize;

use super::CommandResult;
use crate::render::{print_json, print_line};
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::normalize_bead_ids;

#[derive(Debug, Clone, Serialize)]
struct DeleteResult {
    status: &'static str,
    issue_id: String,
}

#[derive(Args, Debug)]
pub struct DeleteArgs {
    /// One or more issue IDs to delete.
    #[arg(required = true, num_args = 1..)]
    pub ids: Vec<String>,

    #[arg(long)]
    pub reason: Option<String>,
}

pub fn handle(ctx: &CliRuntimeCtx, args: DeleteArgs) -> CommandResult<()> {
    let mut results: Vec<DeleteResult> = Vec::new();
    let ids = normalize_bead_ids(args.ids)?;
    for id in ids {
        let issue_id = id.as_str().to_string();
        let req = Request::Delete {
            ctx: ctx.mutation_ctx(),
            payload: DeletePayload {
                id,
                reason: args.reason.clone(),
            },
        };
        let _ = send(&req)?;
        results.push(DeleteResult {
            status: "deleted",
            issue_id,
        });
    }

    if ctx.json {
        print_json(&results)?;
        return Ok(());
    }

    for result in results {
        print_line(&render_deleted_op(&result.issue_id))?;
    }
    Ok(())
}

pub fn render_deleted_op(id: &str) -> String {
    format!("✓ Deleted {id}")
}
