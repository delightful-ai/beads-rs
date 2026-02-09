use super::super::Ctx;
use crate::Result;
pub type ClaimArgs = beads_cli::commands::claim::ClaimArgs;

pub(crate) fn handle(ctx: &Ctx, args: ClaimArgs) -> Result<()> {
    beads_cli::commands::claim::handle(ctx, args).map_err(Into::into)
}
