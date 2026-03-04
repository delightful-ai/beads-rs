#![allow(dead_code)]

// normalize-stderr-test: "\$DIR/checkpoint_import.rs:[0-9]+:[0-9]+" -> "$$DIR/checkpoint_import.rs:LL:CC"
// normalize-stderr-test: "(?m)^   = note: .*\n" -> ""
// normalize-stderr-test: "(?m)^warning: [0-9]+ warnings? emitted\n\n" -> ""

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

fn load_checkpoint_imports_inverted() {
    let local_policy_hash = 7;
    let import = CheckpointImport { policy_hash: 9 };
    let mut imports = Vec::new();

    if import.policy_hash == local_policy_hash {
        imports.push(import);
    } else {
        warn!("checkpoint policy hash mismatch");
    }

    // Violation: mismatch path logged, then import is still admitted.
    imports.push(import);
    let _ = imports;
}

fn load_checkpoint_imports_negated_equality() {
    let local_policy_hash = 7;
    let import = CheckpointImport { policy_hash: 9 };
    let mut imports = Vec::new();

    if !(import.policy_hash == local_policy_hash) {
        warn!("checkpoint policy hash mismatch");
    }

    // Violation: negated equality still gates mismatch branch.
    imports.push(import);
    let _ = imports;
}

fn load_checkpoint_imports_mixed_operators() {
    let local_policy_hash = 7;
    let import = CheckpointImport { policy_hash: 9 };
    let status = 0;
    let mut imports = Vec::new();

    if import.policy_hash == local_policy_hash && status != 1 {
        imports.push(import);
    } else {
        warn!("checkpoint policy hash mismatch");
    }

    // Violation: mismatch branch is the else branch selected by hash equality.
    imports.push(import);
    let _ = imports;
}

fn main() {
    load_checkpoint_imports();
    load_checkpoint_imports_inverted();
    load_checkpoint_imports_negated_equality();
    load_checkpoint_imports_mixed_operators();
}
