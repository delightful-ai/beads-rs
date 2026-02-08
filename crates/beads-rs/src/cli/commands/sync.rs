use super::super::Ctx;
use crate::Result;

pub(crate) fn handle(ctx: &Ctx) -> Result<()> {
    beads_cli::commands::sync::handle(ctx).map_err(Into::into)
}
