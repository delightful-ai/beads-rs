use beads_surface::ipc::{ClosePayload, Request};
use clap::Args;

use super::{CommandResult, print_ok};
use crate::render::print_line;
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::normalize_bead_id;

#[derive(Args, Debug)]
pub struct CloseArgs {
    pub id: String,

    #[arg(long)]
    pub reason: Option<String>,
}

pub fn handle(ctx: &CliRuntimeCtx, args: CloseArgs) -> CommandResult<()> {
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Close {
        ctx: ctx.mutation_ctx(),
        payload: ClosePayload {
            id: id.clone(),
            reason: args.reason.clone(),
            on_branch: None,
        },
    };
    let ok = send(&req)?;
    if ctx.json {
        return print_ok(&ok, true);
    }
    let reason = args.reason.as_deref().unwrap_or("Closed");
    print_line(&render_closed_with_reason(id.as_str(), reason))?;
    Ok(())
}

pub fn render_closed(id: &str) -> String {
    format!("✓ Closed {id}")
}

fn render_closed_with_reason(id: &str, reason: &str) -> String {
    format!("✓ Closed {id}: {reason}")
}
