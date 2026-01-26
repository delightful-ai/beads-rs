#![no_main]

use beads_rs::{Limits, decode_event_body};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_event_body(data, &Limits::default());
});
