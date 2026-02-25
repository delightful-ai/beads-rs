#![no_main]

use beads_core::Limits;
use beads_daemon_core::repl::proto::decode_envelope;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_envelope(data, &Limits::default());
});
