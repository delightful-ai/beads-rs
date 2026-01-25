use clap::Args;

use super::super::render;
use super::super::{Ctx, print_ok, send};
use crate::Result;
use crate::api::QueryResult;
use crate::daemon::ipc::{Request, ResponsePayload};

#[derive(Args, Debug)]
pub struct ReadyArgs {
    /// Limit results.
    #[arg(short = 'n', long)]
    pub limit: Option<usize>,
}

pub(crate) fn handle(ctx: &Ctx, args: ReadyArgs) -> Result<()> {
    let req = Request::Ready {
        repo: ctx.repo.clone(),
        limit: args.limit,
        read: ctx.read_consistency(),
    };
    let ok = send(&req)?;
    if ctx.json {
        return print_ok(&ok, true);
    }
    match ok {
        ResponsePayload::Query(QueryResult::Ready(result)) => {
            println!(
                "{}",
                render::render_ready(&result.issues, result.blocked_count, result.closed_count)
            );
            Ok(())
        }
        other => print_ok(&other, false),
    }
}
