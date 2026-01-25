use clap::Args;

use super::super::render;
use super::super::{Ctx, print_ok, send};
use crate::Result;
use crate::api::QueryResult;
use crate::daemon::ipc::{Request, ResponsePayload};

#[derive(Args, Debug)]
pub struct StaleArgs {
    /// Issues not updated in this many days.
    #[arg(short = 'd', long, default_value_t = 30)]
    pub days: u32,

    /// Filter by status (open|in_progress|blocked).
    #[arg(short = 's', long)]
    pub status: Option<String>,

    /// Maximum issues to show.
    #[arg(short = 'n', long, default_value_t = 50)]
    pub limit: usize,
}

pub(crate) fn handle(ctx: &Ctx, args: StaleArgs) -> Result<()> {
    let req = Request::Stale {
        repo: ctx.repo.clone(),
        days: args.days,
        status: args.status.clone(),
        limit: Some(args.limit),
        read: ctx.read_consistency(),
    };
    let ok = send(&req)?;
    if ctx.json {
        return print_ok(&ok, true);
    }

    match ok {
        ResponsePayload::Query(QueryResult::Stale(issues)) => {
            println!("{}", render::render_stale(&issues, args.days));
            Ok(())
        }
        other => print_ok(&other, false),
    }
}
