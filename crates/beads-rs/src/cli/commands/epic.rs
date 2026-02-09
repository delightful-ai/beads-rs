use super::super::Ctx;
use crate::Result;

pub type EpicCmd = beads_cli::commands::epic::EpicCmd;

pub(crate) fn handle(ctx: &Ctx, cmd: EpicCmd) -> Result<()> {
    beads_cli::commands::epic::handle(ctx, cmd).map_err(Into::into)
}

pub(crate) fn render_epic_statuses(statuses: &[crate::api::EpicStatus]) -> String {
    beads_cli::commands::epic::render_epic_statuses(statuses)
}
