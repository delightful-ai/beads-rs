#![allow(dead_code)]

#[derive(Clone, Copy)]
struct WalCursorOffset(u64);

struct WatermarkPair;

struct SegmentRow {
    last_indexed_offset: WalCursorOffset,
}

impl SegmentRow {
    fn last_indexed_offset(&self) -> WalCursorOffset {
        self.last_indexed_offset
    }
}

trait WalIndexTxn {
    fn update_watermark(&mut self, watermarks: WatermarkPair);
}

fn main() {}
