use clap::Args;

use super::super::validation::normalize_bead_id;
use super::super::{Ctx, print_line, print_ok, send};
use crate::Result;
use crate::daemon::ipc::{ClosePayload, Request};

#[derive(Args, Debug)]
pub struct CloseArgs {
    pub id: String,

    #[arg(long)]
    pub reason: Option<String>,
}

pub(crate) fn handle(ctx: &Ctx, args: CloseArgs) -> Result<()> {
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
    print_line(&render_closed_with_reason(id.as_str(), reason))
}

pub(crate) fn render_closed(id: &str) -> String {
    format!("✓ Closed {id}")
}

fn render_closed_with_reason(id: &str, reason: &str) -> String {
    format!("✓ Closed {id}: {reason}")
}
