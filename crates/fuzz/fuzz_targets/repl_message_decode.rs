#![no_main]

use beads_rs::Limits;
use beads_rs::daemon::repl::decode_envelope;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_envelope(data, &Limits::default());
});
