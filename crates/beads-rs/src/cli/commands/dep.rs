use super::super::Ctx;
use crate::Result;

pub type DepCmd = beads_cli::commands::dep::DepCmd;

pub(crate) fn handle(ctx: &Ctx, cmd: DepCmd) -> Result<()> {
    beads_cli::commands::dep::handle(ctx, cmd).map_err(Into::into)
}
