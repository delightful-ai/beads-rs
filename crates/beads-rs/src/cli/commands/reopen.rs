use super::super::Ctx;
use crate::Result;
pub use beads_cli::commands::reopen::{ReopenArgs, render_reopened};

pub(crate) fn handle(ctx: &Ctx, args: ReopenArgs) -> Result<()> {
    beads_cli::commands::reopen::handle(ctx, args).map_err(Into::into)
}
