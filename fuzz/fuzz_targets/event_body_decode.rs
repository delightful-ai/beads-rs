#![no_main]

use libfuzzer_sys::fuzz_target;

use beads_rs::{Limits, decode_event_body};

fuzz_target!(|data: &[u8]| {
    let limits = Limits::default();
    let _ = decode_event_body(data, &limits);
});
