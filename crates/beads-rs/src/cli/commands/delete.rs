use super::super::Ctx;
use crate::Result;
pub type DeleteArgs = beads_cli::commands::delete::DeleteArgs;

pub(crate) fn handle(ctx: &Ctx, args: DeleteArgs) -> Result<()> {
    beads_cli::commands::delete::handle(ctx, args).map_err(Into::into)
}
