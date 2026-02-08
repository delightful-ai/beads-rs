use super::super::Ctx;
use crate::Result;
pub use beads_cli::commands::claim::{ClaimArgs, render_claim_extended, render_claimed};

pub(crate) fn handle(ctx: &Ctx, args: ClaimArgs) -> Result<()> {
    beads_cli::commands::claim::handle(ctx, args).map_err(Into::into)
}
