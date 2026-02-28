#![allow(dead_code)]

mod hidden {
    pub(crate) struct Token;
}

pub(crate) enum Api {
    V(hidden::Token),
}

fn main() {}