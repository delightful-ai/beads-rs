use super::super::render;
use super::super::{Ctx, StaleArgs, print_ok, send};
use crate::Result;
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::api::QueryResult;

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
