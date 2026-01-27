use clap::Args;

use super::super::{Ctx, print_ok, send};
use crate::Result;
use crate::daemon::ipc::{ListPayload, Request};
use crate::daemon::query::Filters;

#[derive(Args, Debug)]
pub struct SearchArgs {
    /// Search query (multiple words allowed).
    #[arg(num_args = 1..)]
    pub query: Vec<String>,

    /// Limit results.
    #[arg(short = 'n', long)]
    pub limit: Option<usize>,
}

pub(crate) fn handle(ctx: &Ctx, args: SearchArgs) -> Result<()> {
    let filters = Filters {
        search: Some(args.query.join(" ")),
        limit: args.limit,
        ..Default::default()
    };
    let req = Request::List {
        ctx: ctx.read_ctx(),
        payload: ListPayload { filters },
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
