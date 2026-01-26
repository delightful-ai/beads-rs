use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum DaemonCmd {
    /// Run the daemon in the foreground (internal).
    Run,
}

pub(crate) fn render_daemon_info(info: &crate::api::DaemonInfo) -> String {
    format!(
        "daemon {} (protocol {}, pid {})",
        info.version, info.protocol_version, info.pid
    )
}
