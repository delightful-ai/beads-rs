use super::super::Ctx;
use crate::Result;
pub type UpdateArgs = beads_cli::commands::update::UpdateArgs;

pub(crate) fn handle(ctx: &Ctx, args: UpdateArgs) -> Result<()> {
    beads_cli::commands::update::handle(ctx, args).map_err(Into::into)
}
