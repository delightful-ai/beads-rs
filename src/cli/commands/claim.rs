use crate::Result;

use crate::daemon::ipc::Request;

use super::super::{print_ok, send, Ctx, ClaimArgs};

pub(crate) fn handle(ctx: &Ctx, args: ClaimArgs) -> Result<()> {
    let req = Request::Claim {
        repo: ctx.repo.clone(),
        id: args.id,
        lease_secs: args.lease_secs,
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
