#![allow(dead_code)]

use std::path::Path;

use beads_rs::core::StoreId;
use beads_rs::daemon::store_lock::{StoreLockError, remove_lock_file};
use beads_rs::paths::override_data_dir_for_tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnlockOutcome {
    Removed,
    Missing,
}

pub fn unlock_store(data_dir: &Path, store_id: StoreId) -> Result<UnlockOutcome, StoreLockError> {
    let _override = override_data_dir_for_tests(Some(data_dir.to_path_buf()));
    let removed = remove_lock_file(store_id)?;
    Ok(if removed {
        UnlockOutcome::Removed
    } else {
        UnlockOutcome::Missing
    })
}
