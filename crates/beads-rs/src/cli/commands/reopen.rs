use clap::Args;

use super::super::{Ctx, normalize_bead_id, print_ok, send};
use crate::Result;
use crate::daemon::ipc::Request;

#[derive(Args, Debug)]
pub struct ReopenArgs {
    pub id: String,
}

pub(crate) fn handle(ctx: &Ctx, args: ReopenArgs) -> Result<()> {
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Reopen {
        repo: ctx.repo.clone(),
        id: id.as_str().to_string(),
        meta: ctx.mutation_meta(),
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}

pub(crate) fn render_reopened(id: &str) -> String {
    format!("↻ Reopened {id}")
}
