#![no_main]

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;

use beads_rs::Limits;
use beads_rs::daemon::wal::frame::FrameReader;

fuzz_target!(|data: &[u8]| {
    let limits = Limits::default();
    let max_record_bytes = limits.max_wal_record_bytes.min(limits.max_frame_bytes);
    let mut reader = FrameReader::new(Cursor::new(data), max_record_bytes);
    loop {
        match reader.read_next() {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => break,
        }
    }
});
