#![allow(dead_code)]

// normalize-stderr-test: "\$DIR/raw_last_indexed_offset_violation.rs:[0-9]+:[0-9]+" -> "$$DIR/raw_last_indexed_offset_violation.rs:LL:CC"
// normalize-stderr-test: "(?m)^   = note: .*\n" -> ""
// normalize-stderr-test: "(?m)^warning: 1 warning emitted\n\n" -> ""

struct SegmentRow {
    last_indexed_offset: u64,
}

fn main() {
    let _ = SegmentRow {
        last_indexed_offset: 7,
    };
}
