use beads_cli::backend::CliHostBackend;

use crate::{Error, Result};

pub type StoreCmd = beads_cli::commands::store::StoreCmd;

pub(crate) fn handle<B>(json: bool, cmd: StoreCmd, backend: &B) -> Result<()>
where
    B: CliHostBackend<Error = Error>,
{
    beads_cli::commands::store::handle(json, cmd, backend)
}
