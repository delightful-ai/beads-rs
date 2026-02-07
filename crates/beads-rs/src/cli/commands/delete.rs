use clap::Args;
use serde::Serialize;

use super::super::{Ctx, print_json, print_line, send};
use crate::Result;
use beads_cli::validation::normalize_bead_ids;
use beads_surface::ipc::{DeletePayload, Request};

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

pub(crate) fn handle(ctx: &Ctx, args: DeleteArgs) -> Result<()> {
    let mut results: Vec<DeleteResult> = Vec::new();

    let ids = normalize_bead_ids(args.ids)?;
    for id in ids {
        let id_str = id.as_str().to_string();
        let req = Request::Delete {
            ctx: ctx.mutation_ctx(),
            payload: DeletePayload {
                id: id.clone(),
                reason: args.reason.clone(),
            },
        };
        let _ = send(&req)?;

        results.push(DeleteResult {
            status: "deleted",
            issue_id: id_str,
        });
    }

    if ctx.json {
        print_json(&results)?;
        return Ok(());
    }

    for r in results {
        print_line(&render_deleted_op(&r.issue_id))?;
    }
    Ok(())
}

pub(crate) fn render_deleted_op(id: &str) -> String {
    format!("✓ Deleted {id}")
}
