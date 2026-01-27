use super::super::{Ctx, print_ok, send};
use crate::Result;
use crate::daemon::ipc::{EmptyPayload, Request};

pub(crate) fn handle(ctx: &Ctx) -> Result<()> {
    let req = Request::Init {
        ctx: ctx.repo_ctx(),
        payload: EmptyPayload {},
    };
    let resp = send(&req)?;
    print_ok(&resp, ctx.json)?;
    Ok(())
}
