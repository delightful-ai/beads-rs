use serde::Serialize;

use clap::Args;

use super::super::{Ctx, normalize_bead_ids, print_json, send};
use crate::Result;
use crate::daemon::ipc::Request;

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
            repo: ctx.repo.clone(),
            id: id_str.clone(),
            reason: args.reason.clone(),
            meta: ctx.mutation_meta(),
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
        println!("✓ Deleted {}", r.issue_id);
    }
    Ok(())
}
