use beads_surface::OpResult;
use beads_surface::ipc::{IdPayload, Request, ResponsePayload};
use clap::Args;

use super::{CommandResult, print_ok};
use crate::render::print_line;
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::normalize_bead_id;

#[derive(Args, Debug)]
pub struct ReopenArgs {
    pub id: String,
}

pub fn handle(ctx: &CliRuntimeCtx, args: ReopenArgs) -> CommandResult<()> {
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Reopen {
        ctx: ctx.mutation_ctx(),
        payload: IdPayload { id },
    };
    let ok = send(&req)?;
    if ctx.json {
        return print_ok(&ok, true);
    }
    if let ResponsePayload::Op(op) = &ok
        && let OpResult::Reopened { id } = &op.result
    {
        print_line(&render_reopened(id.as_str()))?;
        return Ok(());
    }
    print_ok(&ok, false)
}

pub fn render_reopened(id: &str) -> String {
    format!("↻ Reopened {id}")
}
