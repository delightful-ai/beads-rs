#![no_main]

use libfuzzer_sys::fuzz_target;

use beads_rs::Limits;
use beads_rs::daemon::repl::decode_envelope;

fuzz_target!(|data: &[u8]| {
    let limits = Limits::default();
    let _ = decode_envelope(data, &limits);
});
