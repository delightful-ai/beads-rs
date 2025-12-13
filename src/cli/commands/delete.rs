use super::super::{Ctx, DeleteArgs, print_ok, send};
use crate::Result;
use crate::daemon::ipc::Request;

pub(crate) fn handle(ctx: &Ctx, args: DeleteArgs) -> Result<()> {
    let req = Request::Delete {
        repo: ctx.repo.clone(),
        id: args.id,
        reason: args.reason,
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
