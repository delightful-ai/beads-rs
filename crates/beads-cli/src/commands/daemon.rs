use beads_api::DaemonInfo;
use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum DaemonCmd {
    /// Run the daemon in the foreground (internal).
    Run,
}

pub fn render_daemon_info(info: &DaemonInfo) -> String {
    format!(
        "daemon {} (protocol {}, pid {})",
        info.version, info.protocol_version, info.pid
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_daemon_info_matches_human_format() {
        let info = DaemonInfo {
            version: "0.1.26".to_string(),
            protocol_version: 3,
            pid: 4242,
        };
        assert_eq!(
            render_daemon_info(&info),
            "daemon 0.1.26 (protocol 3, pid 4242)"
        );
    }
}
