use clap::Subcommand;

use crate::Result;

#[derive(Subcommand, Debug)]
pub enum DaemonCmd {
    /// Run the daemon in the foreground (internal).
    Run,
}

pub(crate) fn handle(cmd: DaemonCmd) -> Result<()> {
    match cmd {
        DaemonCmd::Run => crate::daemon::run_daemon(),
    }
}
