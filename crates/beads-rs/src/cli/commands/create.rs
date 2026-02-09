use super::super::Ctx;
use crate::Result;
pub type CreateArgs = beads_cli::commands::create::CreateArgs;

pub(crate) fn handle(ctx: &Ctx, args: CreateArgs) -> Result<()> {
    beads_cli::commands::create::handle(ctx, args).map_err(Into::into)
}
