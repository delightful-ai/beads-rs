use super::super::Ctx;
use crate::Result;

pub type AdminCmd = beads_cli::commands::admin::AdminCmd;
pub type AdminMaintenanceCmd = beads_cli::commands::admin::AdminMaintenanceCmd;

pub(crate) fn handle(ctx: &Ctx, cmd: AdminCmd) -> Result<()> {
    beads_cli::commands::admin::handle(ctx, cmd).map_err(Into::into)
}
