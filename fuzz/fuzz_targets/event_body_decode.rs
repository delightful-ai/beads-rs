#![no_main]

use beads_rs::{decode_event_body, Limits};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_event_body(data, &Limits::default());
});
