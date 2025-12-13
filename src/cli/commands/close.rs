use super::super::{CloseArgs, Ctx, print_ok, send};
use crate::Result;
use crate::daemon::ipc::Request;

pub(crate) fn handle(ctx: &Ctx, args: CloseArgs) -> Result<()> {
    let reason_str = args.reason.clone().unwrap_or_else(|| "Closed".to_string());
    let req = Request::Close {
        repo: ctx.repo.clone(),
        id: args.id.clone(),
        reason: args.reason.clone(),
        on_branch: None,
    };
    let ok = send(&req)?;
    if ctx.json {
        return print_ok(&ok, true);
    }
    println!("✓ Closed {}: {}", args.id, reason_str);
    Ok(())
}
