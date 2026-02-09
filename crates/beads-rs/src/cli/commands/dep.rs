use super::super::Ctx;
use crate::Result;

pub type DepCmd = beads_cli::commands::dep::DepCmd;

pub(crate) fn handle(ctx: &Ctx, cmd: DepCmd) -> Result<()> {
    beads_cli::commands::dep::handle(ctx, cmd).map_err(Into::into)
}

pub(crate) fn render_dep_tree(root: &str, edges: &[crate::api::DepEdge]) -> String {
    beads_cli::commands::dep::render_dep_tree(root, edges)
}

pub(crate) fn render_deps(
    incoming: &[crate::api::DepEdge],
    outgoing: &[crate::api::DepEdge],
) -> String {
    beads_cli::commands::dep::render_deps(incoming, outgoing)
}

pub(crate) fn render_dep_cycles(out: &crate::api::DepCycles) -> String {
    beads_cli::commands::dep::render_dep_cycles(out)
}

pub(crate) fn render_dep_added(from: &str, to: &str) -> String {
    beads_cli::commands::dep::render_dep_added(from, to)
}

pub(crate) fn render_dep_removed(from: &str, to: &str) -> String {
    beads_cli::commands::dep::render_dep_removed(from, to)
}
