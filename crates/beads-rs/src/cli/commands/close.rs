use clap::Args;

use super::super::{Ctx, normalize_bead_id, print_ok, send};
use crate::Result;
use crate::daemon::ipc::{ClosePayload, Request};

#[derive(Args, Debug)]
pub struct CloseArgs {
    pub id: String,

    #[arg(long)]
    pub reason: Option<String>,
}

pub(crate) fn handle(ctx: &Ctx, args: CloseArgs) -> Result<()> {
    let reason_str = args.reason.clone().unwrap_or_else(|| "Closed".to_string());
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
    println!("✓ Closed {}: {}", id.as_str(), reason_str);
    Ok(())
}

pub(crate) fn render_closed(id: &str) -> String {
    format!("✓ Closed {id}")
}
