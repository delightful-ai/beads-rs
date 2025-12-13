use crate::Result;

use crate::daemon::ipc::Request;

use super::super::{print_ok, send, Ctx};

pub(crate) fn handle(ctx: &Ctx) -> Result<()> {
    let req = Request::SyncWait { repo: ctx.repo.clone() };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
