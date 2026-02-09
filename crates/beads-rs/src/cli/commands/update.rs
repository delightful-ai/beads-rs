use super::super::Ctx;
use crate::Result;
pub use beads_cli::commands::update::{UpdateArgs, render_updated};

pub(crate) fn handle(ctx: &Ctx, args: UpdateArgs) -> Result<()> {
    beads_cli::commands::update::handle(ctx, args).map_err(Into::into)
}
