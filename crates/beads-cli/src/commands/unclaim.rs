use beads_surface::OpResult;
use beads_surface::ipc::{IdPayload, Request, ResponsePayload};
use clap::Args;

use super::{CommandResult, print_ok};
use crate::render::print_line;
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::normalize_bead_id;

#[derive(Args, Debug)]
pub struct UnclaimArgs {
    pub id: String,
}

pub fn handle(ctx: &CliRuntimeCtx, args: UnclaimArgs) -> CommandResult<()> {
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Unclaim {
        ctx: ctx.mutation_ctx(),
        payload: IdPayload { id },
    };
    let ok = send(&req)?;
    if ctx.json {
        return print_ok(&ok, true);
    }
    if let ResponsePayload::Op(op) = &ok
        && let OpResult::Unclaimed { id } = &op.result
    {
        print_line(&render_unclaimed(id.as_str()))?;
        return Ok(());
    }
    print_ok(&ok, false)
}

pub fn render_unclaimed(id: &str) -> String {
    format!("✓ Unclaimed {id}")
}
