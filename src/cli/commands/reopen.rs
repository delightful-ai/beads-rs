use super::super::{Ctx, print_ok, send};
use crate::Result;
use crate::daemon::ipc::Request;

pub(crate) fn handle(ctx: &Ctx, id: String) -> Result<()> {
    let req = Request::Reopen {
        repo: ctx.repo.clone(),
        id,
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
