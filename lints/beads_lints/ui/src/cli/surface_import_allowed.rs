#![allow(dead_code)]

mod beads_surface {
    pub mod ipc {
        pub struct Request;
    }
}

use crate::beads_surface::ipc::Request;

fn main() {
    let _ = core::mem::size_of::<Request>();
}
