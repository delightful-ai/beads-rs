#![no_main]

use beads_core::Limits;
use beads_daemon_core::wal::FrameReader;
use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    let max_record_bytes = Limits::default().max_wal_record_bytes;
    let mut reader = FrameReader::new(Cursor::new(data), max_record_bytes);
    let _ = reader.read_next();
});
