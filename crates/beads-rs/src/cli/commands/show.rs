use super::super::Ctx;
use crate::Result;
pub use beads_cli::commands::show::{ShowArgs, render_issue_detail};

pub(crate) fn handle(ctx: &Ctx, args: ShowArgs) -> Result<()> {
    beads_cli::commands::show::handle(ctx, args).map_err(Into::into)
}
