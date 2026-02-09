use beads_cli::backend::CliHostBackend;

use crate::{Error, Result};

pub type UpgradeArgs = beads_cli::commands::upgrade::UpgradeArgs;

pub(crate) fn handle<B>(json: bool, background: bool, backend: &B) -> Result<()>
where
    B: CliHostBackend<Error = Error>,
{
    beads_cli::commands::upgrade::handle(json, background, backend)
}
