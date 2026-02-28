#![allow(dead_code)]
#![allow(private_interfaces)]

// normalize-stderr-test: "\$DIR/private_interface_allow_crate_level_violation.rs:[0-9]+:[0-9]+" -> "$$DIR/private_interface_allow_crate_level_violation.rs:LL:CC"
// normalize-stderr-test: "(?m)^   = note: .*\n" -> ""
// normalize-stderr-test: "(?m)^warning: [0-9]+ warnings? emitted\n\n" -> ""

mod hidden {
    pub(crate) struct Token;
}

pub enum Api {
    V(hidden::Token),
}

fn main() {}