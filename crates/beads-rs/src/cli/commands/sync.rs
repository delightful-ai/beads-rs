use super::super::{Ctx, print_ok, send};
use crate::Result;
use crate::daemon::ipc::{EmptyPayload, Request};

pub(crate) fn handle(ctx: &Ctx) -> Result<()> {
    let req = Request::SyncWait {
        ctx: ctx.repo_ctx(),
        payload: EmptyPayload {},
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
