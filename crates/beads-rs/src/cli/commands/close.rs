use super::super::Ctx;
use crate::Result;
pub type CloseArgs = beads_cli::commands::close::CloseArgs;

pub(crate) fn handle(ctx: &Ctx, args: CloseArgs) -> Result<()> {
    beads_cli::commands::close::handle(ctx, args).map_err(Into::into)
}
