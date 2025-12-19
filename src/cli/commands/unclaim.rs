use super::super::{Ctx, normalize_bead_id, print_ok, send};
use crate::Result;
use crate::daemon::ipc::Request;

pub(crate) fn handle(ctx: &Ctx, id: String) -> Result<()> {
    let id = normalize_bead_id(&id)?;
    let req = Request::Unclaim {
        repo: ctx.repo.clone(),
        id,
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
