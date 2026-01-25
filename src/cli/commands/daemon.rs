use clap::Subcommand;

use crate::Result;

#[derive(Subcommand, Debug)]
pub enum DaemonCmd {
    /// Run the daemon in the foreground (internal).
    Run,
}

pub(super) fn cmd_name(cmd: &DaemonCmd) -> &'static str {
    match cmd {
        DaemonCmd::Run => "run",
    }
}

pub(crate) fn handle(cmd: DaemonCmd) -> Result<()> {
    match cmd {
        DaemonCmd::Run => crate::daemon::run_daemon(),
    }
}
