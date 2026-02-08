#[test]
fn cli_daemon_boundary_ui() {
    dylint_testing::ui_test(env!("CARGO_PKG_NAME"), "ui");
}
