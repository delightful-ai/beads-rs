use super::super::Ctx;
use crate::Result;
pub type UnclaimArgs = beads_cli::commands::unclaim::UnclaimArgs;

pub(crate) fn handle(ctx: &Ctx, args: UnclaimArgs) -> Result<()> {
    beads_cli::commands::unclaim::handle(ctx, args).map_err(Into::into)
}
