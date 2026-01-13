use super::super::{Ctx, print_ok, send};
use crate::Result;
use crate::daemon::ipc::Request;

pub(crate) fn handle(ctx: &Ctx) -> Result<()> {
    let req = Request::Blocked {
        repo: ctx.repo.clone(),
        read: ctx.read_consistency(),
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
