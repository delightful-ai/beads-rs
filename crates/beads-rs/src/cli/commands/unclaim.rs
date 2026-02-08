use super::super::Ctx;
use crate::Result;
pub use beads_cli::commands::unclaim::{UnclaimArgs, render_unclaimed};

pub(crate) fn handle(ctx: &Ctx, args: UnclaimArgs) -> Result<()> {
    beads_cli::commands::unclaim::handle(ctx, args).map_err(Into::into)
}
