use crate::Result;

use crate::daemon::ipc::Request;

use super::super::{print_ok, send, Ctx};

pub(crate) fn handle(ctx: &Ctx, id: String) -> Result<()> {
    let req = Request::Unclaim {
        repo: ctx.repo.clone(),
        id,
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
