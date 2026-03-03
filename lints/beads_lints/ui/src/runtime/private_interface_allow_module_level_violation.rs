#![allow(dead_code)]

// normalize-stderr-test: "\$DIR/private_interface_allow_module_level_violation.rs:[0-9]+:[0-9]+" -> "$$DIR/private_interface_allow_module_level_violation.rs:LL:CC"
// normalize-stderr-test: "(?m)^   = note: .*\n" -> ""
// normalize-stderr-test: "(?m)^warning: [0-9]+ warnings? emitted\n\n" -> ""

mod hidden {
    pub(crate) struct Token;
}

#[allow(private_interfaces)]
mod api {
    use super::hidden;

    pub enum Api {
        V(hidden::Token),
    }
}

fn main() {
    let _ = core::mem::size_of::<api::Api>();
}