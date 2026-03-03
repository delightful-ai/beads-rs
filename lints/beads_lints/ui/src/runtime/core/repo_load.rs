#![allow(dead_code)]

macro_rules! warn {
    ($($tt:tt)*) => {{}};
}

#[derive(Clone, Copy)]
struct CheckpointImport {
    roster_hash: u64,
}

fn ensure_checkpoint_compatible(
    import: CheckpointImport,
    local_roster_hash: u64,
) -> Result<(), ()> {
    if import.roster_hash != local_roster_hash {
        warn!("checkpoint roster hash mismatch");
        return Err(());
    }
    Ok(())
}

fn apply_loaded_repo_state() -> Result<(), ()> {
    let import = CheckpointImport { roster_hash: 2 };
    ensure_checkpoint_compatible(import, 1)?;
    Ok(())
}

fn main() {
    let _ = apply_loaded_repo_state();
}
