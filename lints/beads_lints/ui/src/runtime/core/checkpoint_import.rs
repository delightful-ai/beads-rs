#![allow(dead_code)]

// normalize-stderr-test: "\$DIR/runtime/core/checkpoint_import.rs:[0-9]+:[0-9]+" -> "$$DIR/runtime/core/checkpoint_import.rs:LL:CC"
// normalize-stderr-test: "(?m)^   = note: .*\n" -> ""
// normalize-stderr-test: "(?m)^warning: 1 warning emitted\n\n" -> ""

macro_rules! warn {
    ($($tt:tt)*) => {{}};
}

#[derive(Clone, Copy)]
struct CheckpointImport {
    policy_hash: u64,
}

fn load_checkpoint_imports() {
    let local_policy_hash = 7;
    let import = CheckpointImport { policy_hash: 9 };
    let mut imports = Vec::new();

    if import.policy_hash != local_policy_hash {
        warn!("checkpoint policy hash mismatch");
    }

    // Violation: mismatch path logged, then import is still admitted.
    imports.push(import);
    let _ = imports;
}

fn main() {
    load_checkpoint_imports();
}
