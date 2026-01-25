use clap::Args;

use super::super::{Ctx, normalize_bead_id, print_ok, send};
use crate::Result;
use crate::daemon::ipc::Request;

#[derive(Args, Debug)]
pub struct UnclaimArgs {
    pub id: String,
}

pub(crate) fn handle(ctx: &Ctx, args: UnclaimArgs) -> Result<()> {
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Unclaim {
        repo: ctx.repo.clone(),
        id: id.as_str().to_string(),
        meta: ctx.mutation_meta(),
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
