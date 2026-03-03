#![allow(dead_code)]

// normalize-stderr-test: "\$DIR/watermark_split_params_violation.rs:[0-9]+:[0-9]+" -> "$$DIR/watermark_split_params_violation.rs:LL:CC"
// normalize-stderr-test: "(?m)^   = note: .*\n" -> ""
// normalize-stderr-test: "(?m)^warning: 1 warning emitted\n\n" -> ""

trait WalIndexTxn {
    fn update_watermark(&mut self, applied: u64, durable: u64);
}

fn main() {}
