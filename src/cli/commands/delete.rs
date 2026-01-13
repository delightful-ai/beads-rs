use serde::Serialize;

use super::super::{Ctx, DeleteArgs, normalize_bead_ids, send};
use crate::Result;
use crate::daemon::ipc::Request;

#[derive(Debug, Clone, Serialize)]
struct DeleteResult {
    status: &'static str,
    issue_id: String,
}

pub(crate) fn handle(ctx: &Ctx, args: DeleteArgs) -> Result<()> {
    let mut results: Vec<DeleteResult> = Vec::new();

    let ids = normalize_bead_ids(args.ids)?;
    for id in ids {
        let req = Request::Delete {
            repo: ctx.repo.clone(),
            id: id.clone(),
            reason: args.reason.clone(),
            meta: ctx.mutation_meta(),
        };
        let _ = send(&req)?;

        results.push(DeleteResult {
            status: "deleted",
            issue_id: id,
        });
    }

    if ctx.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&results).map_err(crate::daemon::IpcError::from)?
        );
        return Ok(());
    }

    for r in results {
        println!("✓ Deleted {}", r.issue_id);
    }
    Ok(())
}
