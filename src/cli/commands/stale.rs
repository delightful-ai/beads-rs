use crate::Result;

use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::query::QueryResult;

use super::super::{print_ok, send, Ctx, StaleArgs};
use super::super::render;

pub(crate) fn handle(ctx: &Ctx, args: StaleArgs) -> Result<()> {
    let req = Request::Stale {
        repo: ctx.repo.clone(),
        days: args.days,
        status: args.status.clone(),
        limit: Some(args.limit),
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
