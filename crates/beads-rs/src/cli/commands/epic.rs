use super::super::Ctx;
use crate::Result;

pub type EpicCmd = beads_cli::commands::epic::EpicCmd;

pub(crate) fn handle(ctx: &Ctx, cmd: EpicCmd) -> Result<()> {
    beads_cli::commands::epic::handle(ctx, cmd).map_err(Into::into)
}
