use super::super::{CloseArgs, Ctx, normalize_bead_id, print_ok, send};
use crate::Result;
use crate::daemon::ipc::Request;

pub(crate) fn handle(ctx: &Ctx, args: CloseArgs) -> Result<()> {
    let reason_str = args.reason.clone().unwrap_or_else(|| "Closed".to_string());
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Close {
        repo: ctx.repo.clone(),
        id: id.clone(),
        reason: args.reason.clone(),
        on_branch: None,
        meta: ctx.mutation_meta(),
    };
    let ok = send(&req)?;
    if ctx.json {
        return print_ok(&ok, true);
    }
    println!("✓ Closed {}: {}", id, reason_str);
    Ok(())
}
