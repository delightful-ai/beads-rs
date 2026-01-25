use super::super::{Ctx, print_ok, send};
use crate::Result;
use crate::daemon::ipc::Request;

pub(crate) fn handle(ctx: &Ctx) -> Result<()> {
    let req = Request::Init {
        repo: ctx.repo.clone(),
    };
    let resp = send(&req)?;
    print_ok(&resp, ctx.json)?;
    Ok(())
}
