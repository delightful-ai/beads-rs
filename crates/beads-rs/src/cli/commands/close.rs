use super::super::Ctx;
use crate::Result;
pub use beads_cli::commands::close::{CloseArgs, render_closed};

pub(crate) fn handle(ctx: &Ctx, args: CloseArgs) -> Result<()> {
    beads_cli::commands::close::handle(ctx, args).map_err(Into::into)
}
