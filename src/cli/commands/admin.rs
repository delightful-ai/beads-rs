use super::super::{AdminCmd, Ctx, print_ok, send};
use crate::Result;
use crate::daemon::ipc::Request;

pub(crate) fn handle(ctx: &Ctx, cmd: AdminCmd) -> Result<()> {
    match cmd {
        AdminCmd::Status => {
            let req = Request::AdminStatus {
                repo: ctx.repo.clone(),
                read: ctx.read_consistency(),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::Metrics => {
            let req = Request::AdminMetrics {
                repo: ctx.repo.clone(),
                read: ctx.read_consistency(),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
    }
}
