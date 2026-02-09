use super::super::Ctx;
use crate::Result;

pub type MigrateCmd = beads_cli::commands::migrate::MigrateCmd;

pub(crate) fn handle(ctx: &Ctx, cmd: MigrateCmd) -> Result<()> {
    beads_cli::commands::migrate::handle(ctx, cmd, ctx.backend())
}
