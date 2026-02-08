use beads_surface::ipc::{EmptyPayload, Request};

use super::{CommandResult, print_ok};
use crate::runtime::{CliRuntimeCtx, send};

pub fn handle(ctx: &CliRuntimeCtx) -> CommandResult<()> {
    let req = Request::Init {
        ctx: ctx.repo_ctx(),
        payload: EmptyPayload {},
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
