#![allow(dead_code)]

// normalize-stderr-test: "\$DIR/private_interface_allow_mixed_violation.rs:[0-9]+:[0-9]+" -> "$$DIR/private_interface_allow_mixed_violation.rs:LL:CC"
// normalize-stderr-test: "(?m)^   = note: .*\n" -> ""
// normalize-stderr-test: "(?m)^warning: [0-9]+ warnings? emitted\n\n" -> ""

mod hidden {
    pub(crate) struct Token;
}

#[allow(dead_code, private_interfaces)]
pub enum Api {
    V(hidden::Token),
}

fn main() {}