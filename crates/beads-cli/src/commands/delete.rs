use beads_surface::ipc::{DeletePayload, Request};
use clap::Args;
use serde::Serialize;

use super::CommandResult;
use crate::render::{print_json, print_line};
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::normalize_bead_ref_for;

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
    let refs = args
        .ids
        .into_iter()
        .map(|raw| normalize_bead_ref_for("id", &raw, &ctx.active_namespace()))
        .collect::<Result<Vec<_>, _>>()?;
    for bead_ref in refs {
        let command_ctx = ctx.with_namespace(bead_ref.namespace().clone());
        let id = bead_ref.id().clone();
        let issue_id = id.as_str().to_string();
        let req = Request::Delete {
            ctx: command_ctx.mutation_ctx(),
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
