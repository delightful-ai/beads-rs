use super::super::{Ctx, print_ok, send};
use crate::Result;
use beads_surface::ipc::{EmptyPayload, Request};

pub(crate) fn handle(ctx: &Ctx) -> Result<()> {
    let req = Request::Init {
        ctx: ctx.repo_ctx(),
        payload: EmptyPayload {},
    };
    let resp = send(&req)?;
    print_ok(&resp, ctx.json)?;
    Ok(())
}
