use super::super::Ctx;
use crate::Result;
pub use beads_cli::commands::status::render_status;

pub(crate) fn handle(ctx: &Ctx) -> Result<()> {
    beads_cli::commands::status::handle(ctx).map_err(Into::into)
}
