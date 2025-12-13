use super::super::{ClaimArgs, Ctx, print_ok, send};
use crate::Result;
use crate::daemon::ipc::Request;

pub(crate) fn handle(ctx: &Ctx, args: ClaimArgs) -> Result<()> {
    let req = Request::Claim {
        repo: ctx.repo.clone(),
        id: args.id,
        lease_secs: args.lease_secs,
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
