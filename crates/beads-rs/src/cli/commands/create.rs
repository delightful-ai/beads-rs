use super::super::Ctx;
use crate::Result;
pub use beads_cli::commands::create::{CreateArgs, render_created};

pub(crate) fn handle(ctx: &Ctx, args: CreateArgs) -> Result<()> {
    beads_cli::commands::create::handle(ctx, args).map_err(Into::into)
}
