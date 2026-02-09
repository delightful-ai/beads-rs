#![allow(dead_code)]

// normalize-stderr-test: "\$DIR/daemon_non_use_path_violation.rs:[0-9]+:[0-9]+" -> "$$DIR/daemon_non_use_path_violation.rs:LL:CC"
// normalize-stderr-test: "(?m)^   = note: .*\n" -> ""
// normalize-stderr-test: "(?m)^warning: 1 warning emitted\n\n" -> ""

mod daemon {
    pub mod ipc {
        pub struct Request;
    }
}

fn main() {
    let _ = core::mem::size_of::<crate::daemon::ipc::Request>();
}
