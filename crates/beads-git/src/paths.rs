//! Minimal path helpers needed by checkpoint cache.

use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use beads_bootstrap::paths as moved;
use beads_core::StoreId;

#[cfg(test)]
use std::cell::RefCell;

static DATA_DIR_OVERRIDE: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();

pub fn init_data_dir_override(path: Option<PathBuf>) {
    let lock = DATA_DIR_OVERRIDE.get_or_init(|| Mutex::new(path.clone()));
    let mut guard = lock.lock().expect("beads-git paths config lock poisoned");
    *guard = path;
}

#[cfg(test)]
#[doc(hidden)]
pub struct DataDirOverride {
    prev: Option<PathBuf>,
}

#[cfg(test)]
impl DataDirOverride {
    pub fn new(path: Option<PathBuf>) -> Self {
        let prev = TEST_DATA_DIR_OVERRIDE.with(|cell| cell.replace(path));
        Self { prev }
    }
}

#[cfg(test)]
impl Drop for DataDirOverride {
    fn drop(&mut self) {
        let prev = self.prev.take();
        TEST_DATA_DIR_OVERRIDE.with(|cell| {
            cell.replace(prev);
        });
    }
}

#[cfg(test)]
#[doc(hidden)]
pub fn override_data_dir_for_tests(path: Option<PathBuf>) -> DataDirOverride {
    DataDirOverride::new(path)
}

pub fn checkpoint_cache_dir(store_id: StoreId) -> PathBuf {
    moved::checkpoint_cache_dir(&data_dir(), store_id)
}

fn data_dir() -> PathBuf {
    #[cfg(test)]
    if let Some(dir) = thread_local_data_dir_override() {
        return dir;
    }

    if let Some(dir) = std::env::var("BD_DATA_DIR")
        .ok()
        .map(|raw| raw.trim().to_owned())
        .filter(|raw| !raw.is_empty())
    {
        return PathBuf::from(dir);
    }

    if let Some(dir) = config_data_dir_override() {
        return dir;
    }

    moved::resolve_data_dir(None, None)
}

fn config_data_dir_override() -> Option<PathBuf> {
    DATA_DIR_OVERRIDE
        .get()
        .and_then(|lock| lock.lock().ok().and_then(|path| path.clone()))
}

#[cfg(test)]
fn thread_local_data_dir_override() -> Option<PathBuf> {
    TEST_DATA_DIR_OVERRIDE.with(|cell| cell.borrow().clone())
}

#[cfg(test)]
thread_local! {
    static TEST_DATA_DIR_OVERRIDE: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
}
