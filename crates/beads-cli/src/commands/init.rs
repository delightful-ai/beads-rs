use beads_surface::ipc::{InitPayload, Request};

use super::{CommandResult, print_ok};
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::normalize_bead_slug_for;

#[derive(clap::Args, Debug, Clone, Default)]
pub struct InitArgs {
    /// Compatibility flag accepted from Go beads/Gas City; server startup is handled by bd daemon.
    #[arg(long, default_value_t = false)]
    pub server: bool,

    /// Override the root bead id prefix stored in the initial beads metadata.
    #[arg(short = 'p', long = "prefix", value_name = "SLUG")]
    pub prefix: Option<String>,

    /// Compatibility flag accepted from Go beads/Gas City; Rust beads does not install hooks here.
    #[arg(long = "skip-hooks", default_value_t = false)]
    pub skip_hooks: bool,

    /// Compatibility flag accepted from Gas City; Rust beads uses Unix-socket IPC.
    #[arg(long = "server-host", value_name = "HOST")]
    pub server_host: Option<String>,

    /// Compatibility flag accepted from Gas City; Rust beads uses Unix-socket IPC.
    #[arg(long = "server-port", value_name = "PORT")]
    pub server_port: Option<String>,
}

pub fn handle(ctx: &CliRuntimeCtx, args: InitArgs) -> CommandResult<()> {
    let InitArgs {
        server: _,
        prefix,
        skip_hooks: _,
        server_host: _,
        server_port: _,
    } = args;
    let root_slug = prefix
        .as_deref()
        .map(|slug| normalize_bead_slug_for("prefix", slug))
        .transpose()?;
    let req = Request::Init {
        ctx: ctx.repo_ctx(),
        payload: InitPayload { root_slug },
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
