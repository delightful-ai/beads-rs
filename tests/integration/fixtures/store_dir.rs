#![allow(dead_code)]

use std::path::{Path, PathBuf};
use tempfile::TempDir;

use beads_rs::paths::{DataDirOverride, override_data_dir_for_tests};

pub struct TempStoreDir {
    _temp: TempDir,
    data_dir: PathBuf,
    _override: DataDirOverride,
}

impl TempStoreDir {
    pub fn new() -> std::io::Result<Self> {
        let temp = TempDir::new()?;
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir)?;
        let override_guard = override_data_dir_for_tests(Some(data_dir.clone()));

        Ok(Self {
            _temp: temp,
            data_dir,
            _override: override_guard,
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
