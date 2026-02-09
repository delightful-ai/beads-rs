use super::super::Ctx;
use crate::Result;
pub type ShowArgs = beads_cli::commands::show::ShowArgs;

pub(crate) fn handle(ctx: &Ctx, args: ShowArgs) -> Result<()> {
    beads_cli::commands::show::handle(ctx, args).map_err(Into::into)
}
