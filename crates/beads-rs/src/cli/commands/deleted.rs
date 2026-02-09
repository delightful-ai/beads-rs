use super::super::Ctx;
use crate::Result;

pub type DeletedArgs = beads_cli::commands::deleted::DeletedArgs;

pub(crate) fn handle(ctx: &Ctx, args: DeletedArgs) -> Result<()> {
    beads_cli::commands::deleted::handle(ctx, args).map_err(Into::into)
}
