use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum DaemonCmd {
    /// Run the daemon in the foreground (internal).
    Run,
}
