use clap::Args;

use super::super::{Ctx, print_ok, send};
use super::super::validation::normalize_bead_id;
use crate::Result;
use crate::daemon::ipc::{IdPayload, Request};

#[derive(Args, Debug)]
pub struct UnclaimArgs {
    pub id: String,
}

pub(crate) fn handle(ctx: &Ctx, args: UnclaimArgs) -> Result<()> {
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Unclaim {
        ctx: ctx.mutation_ctx(),
        payload: IdPayload { id },
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}

pub(crate) fn render_unclaimed(id: &str) -> String {
    format!("✓ Unclaimed {id}")
}
