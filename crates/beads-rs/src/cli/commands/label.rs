use super::super::Ctx;
use crate::Result;

pub type LabelCmd = beads_cli::commands::label::LabelCmd;

pub(crate) fn handle(ctx: &Ctx, cmd: LabelCmd) -> Result<()> {
    beads_cli::commands::label::handle(ctx, cmd).map_err(Into::into)
}
