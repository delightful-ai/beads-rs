use clap::Args;

use super::super::{Ctx, print_ok, send};
use super::super::validation::normalize_bead_id;
use crate::Result;
use crate::daemon::ipc::{IdPayload, Request};

#[derive(Args, Debug)]
pub struct ReopenArgs {
    pub id: String,
}

pub(crate) fn handle(ctx: &Ctx, args: ReopenArgs) -> Result<()> {
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Reopen {
        ctx: ctx.mutation_ctx(),
        payload: IdPayload { id },
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}

pub(crate) fn render_reopened(id: &str) -> String {
    format!("↻ Reopened {id}")
}
