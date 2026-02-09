use super::super::Ctx;
use crate::Result;
pub use beads_cli::commands::subscribe::SubscribeArgs;

pub(crate) fn handle(ctx: &Ctx, args: SubscribeArgs) -> Result<()> {
    beads_cli::commands::subscribe::handle(ctx, args).map_err(Into::into)
}
