use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tempfile::TempDir;

static ENV_LOCK: Mutex<()> = Mutex::new(());

pub struct TempStoreDir {
    _lock: std::sync::MutexGuard<'static, ()>,
    _temp: TempDir,
    data_dir: PathBuf,
    prev_data_dir: Option<OsString>,
}

impl TempStoreDir {
    pub fn new() -> std::io::Result<Self> {
        let lock = ENV_LOCK.lock().expect("BD_DATA_DIR env lock poisoned");
        let temp = TempDir::new()?;
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir)?;
        let prev_data_dir = std::env::var_os("BD_DATA_DIR");
        std::env::set_var("BD_DATA_DIR", &data_dir);

        Ok(Self {
            _lock: lock,
            _temp: temp,
            data_dir,
            prev_data_dir,
        })
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn make_dir(&self, rel: impl AsRef<Path>) -> std::io::Result<PathBuf> {
        let path = self.data_dir.join(rel);
        std::fs::create_dir_all(&path)?;
        Ok(path)
    }

    pub fn write_lock(&self, rel: impl AsRef<Path>, contents: &str) -> std::io::Result<PathBuf> {
        let path = self.data_dir.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, contents)?;
        Ok(path)
    }

    pub fn read_lock(&self, rel: impl AsRef<Path>) -> std::io::Result<String> {
        std::fs::read_to_string(self.data_dir.join(rel))
    }

    pub fn force_release_lock(&self, rel: impl AsRef<Path>) -> std::io::Result<()> {
        let path = self.data_dir.join(rel);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }
}

impl Drop for TempStoreDir {
    fn drop(&mut self) {
        if let Some(prev) = self.prev_data_dir.take() {
            std::env::set_var("BD_DATA_DIR", prev);
        } else {
            std::env::remove_var("BD_DATA_DIR");
        }
    }
}
