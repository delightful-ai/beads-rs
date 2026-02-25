//! Minimal path helpers needed by checkpoint cache.

use std::path::{Path, PathBuf};

use beads_core::StoreId;

#[cfg(test)]
use std::cell::RefCell;

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
    store_dir(&data_dir(), store_id).join("checkpoint_cache")
}

fn stores_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("stores")
}

fn store_dir(data_dir: &Path, store_id: StoreId) -> PathBuf {
    stores_dir(data_dir).join(store_id.to_string())
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

    std::env::var("XDG_DATA_HOME")
        .ok()
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join(".local")
                .join("share")
        })
        .join("beads-rs")
}

#[cfg(test)]
fn thread_local_data_dir_override() -> Option<PathBuf> {
    TEST_DATA_DIR_OVERRIDE.with(|cell| cell.borrow().clone())
}

#[cfg(test)]
thread_local! {
    static TEST_DATA_DIR_OVERRIDE: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
}
