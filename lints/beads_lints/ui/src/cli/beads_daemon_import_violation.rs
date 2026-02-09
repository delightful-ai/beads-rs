#![allow(dead_code)]

// normalize-stderr-test: "\$DIR/beads_daemon_import_violation.rs:[0-9]+:[0-9]+" -> "$$DIR/beads_daemon_import_violation.rs:LL:CC"
// normalize-stderr-test: "(?m)^   = note: .*\n" -> ""
// normalize-stderr-test: "(?m)^warning: 1 warning emitted\n\n" -> ""

mod beads_daemon {
    pub mod remote {
        pub struct RemoteUrl;
    }
}

use crate::beads_daemon::remote::RemoteUrl;

fn main() {
    let _ = core::mem::size_of::<RemoteUrl>();
}
