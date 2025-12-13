use crate::Result;

use crate::daemon::ipc::{Request, ResponsePayload};

use super::super::{print_ok, send, Ctx};

pub(crate) fn handle(ctx: &Ctx) -> Result<()> {
    let req = Request::Init { repo: ctx.repo.clone() };
    let _ = send(&req)?;
    print_ok(&ResponsePayload::Initialized, ctx.json)?;
    Ok(())
}
