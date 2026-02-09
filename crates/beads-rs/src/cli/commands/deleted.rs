use super::super::Ctx;
use crate::Result;

pub type DeletedArgs = beads_cli::commands::deleted::DeletedArgs;

pub(crate) fn handle(ctx: &Ctx, args: DeletedArgs) -> Result<()> {
    beads_cli::commands::deleted::handle(ctx, args).map_err(Into::into)
}

pub(crate) fn render_deleted(tombs: &[crate::api::Tombstone]) -> String {
    beads_cli::commands::deleted::render_deleted(tombs)
}

pub(crate) fn render_deleted_lookup(out: &crate::api::DeletedLookup) -> String {
    beads_cli::commands::deleted::render_deleted_lookup(out)
}
