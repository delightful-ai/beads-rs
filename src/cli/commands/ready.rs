use super::super::render;
use super::super::{Ctx, ReadyArgs, print_ok, send};
use crate::Result;
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::query::QueryResult;

pub(crate) fn handle(ctx: &Ctx, args: ReadyArgs) -> Result<()> {
    let req = Request::Ready {
        repo: ctx.repo.clone(),
        limit: args.limit,
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
