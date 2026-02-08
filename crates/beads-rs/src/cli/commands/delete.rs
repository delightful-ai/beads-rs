use super::super::Ctx;
use crate::Result;
pub use beads_cli::commands::delete::{DeleteArgs, render_deleted_op};

pub(crate) fn handle(ctx: &Ctx, args: DeleteArgs) -> Result<()> {
    beads_cli::commands::delete::handle(ctx, args).map_err(Into::into)
}
