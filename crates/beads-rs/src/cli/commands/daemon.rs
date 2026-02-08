pub(crate) fn render_daemon_info(info: &crate::api::DaemonInfo) -> String {
    format!(
        "daemon {} (protocol {}, pid {})",
        info.version, info.protocol_version, info.pid
    )
}
