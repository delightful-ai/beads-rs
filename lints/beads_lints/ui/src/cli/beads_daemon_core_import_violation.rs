#![allow(dead_code)]

// normalize-stderr-test: "\$DIR/beads_daemon_core_import_violation.rs:[0-9]+:[0-9]+" -> "$$DIR/beads_daemon_core_import_violation.rs:LL:CC"
// normalize-stderr-test: "(?m)^   = note: .*\n" -> ""
// normalize-stderr-test: "(?m)^warning: 1 warning emitted\n\n" -> ""

mod beads_daemon_core {
    pub mod repl {
        pub mod proto {
            pub struct Welcome;
        }
    }
}

use crate::beads_daemon_core::repl::proto::Welcome;

fn main() {
    let _ = core::mem::size_of::<Welcome>();
}
