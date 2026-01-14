use super::super::{AdminCmd, AdminMaintenanceCmd, Ctx, print_ok, send};
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
        AdminCmd::Doctor(args) => {
            let req = Request::AdminDoctor {
                repo: ctx.repo.clone(),
                read: ctx.read_consistency(),
                max_records_per_namespace: Some(args.max_records),
                verify_checkpoint_cache: true,
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::Scrub(args) => {
            let req = Request::AdminScrub {
                repo: ctx.repo.clone(),
                read: ctx.read_consistency(),
                max_records_per_namespace: Some(args.max_records),
                verify_checkpoint_cache: args.verify_checkpoint_cache,
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::Maintenance { cmd } => {
            let enabled = matches!(cmd, AdminMaintenanceCmd::On);
            let req = Request::AdminMaintenanceMode {
                repo: ctx.repo.clone(),
                enabled,
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::RebuildIndex => {
            let req = Request::AdminRebuildIndex {
                repo: ctx.repo.clone(),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
    }
}
