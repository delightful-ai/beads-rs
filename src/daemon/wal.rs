//! Write-Ahead Log for mutation durability.
//!
//! Provides crash-safe persistence without flooding git history with commits.
//! WAL entries are state snapshots (not operation replay) written atomically
//! via rename. Cleared after successful remote sync.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::remote::RemoteUrl;
use crate::core::CanonicalState;

/// WAL format version.
const WAL_VERSION: u32 = 1;

/// A WAL entry containing a full state snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Format version for future compatibility.
    pub version: u32,
    /// Wall clock time when written (for debugging).
    pub written_at_ms: u64,
    /// Full state snapshot.
    pub state: CanonicalState,
    /// Root slug for bead IDs.
    pub root_slug: Option<String>,
    /// Monotonic sequence number.
    pub sequence: u64,
}

impl WalEntry {
    /// Create a new WAL entry.
    pub fn new(
        state: CanonicalState,
        root_slug: Option<String>,
        sequence: u64,
        wall_ms: u64,
    ) -> Self {
        WalEntry {
            version: WAL_VERSION,
            written_at_ms: wall_ms,
            state,
            root_slug,
            sequence,
        }
    }
}

/// WAL errors.
#[derive(Debug, Error)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("WAL version mismatch: expected {expected}, got {got}")]
    VersionMismatch { expected: u32, got: u32 },
}

/// Write-Ahead Log manager.
///
/// Stores per-remote WAL files in a subdirectory of the socket dir.
pub struct Wal {
    dir: PathBuf,
}

impl Wal {
    /// Create a new WAL manager.
    ///
    /// Creates the WAL directory if it doesn't exist.
    pub fn new(socket_dir: &Path) -> Result<Self, WalError> {
        let dir = socket_dir.join("wal");
        fs::create_dir_all(&dir)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o700));
        }

        Ok(Wal { dir })
    }

    /// Get the WAL file path for a remote.
    fn wal_path(&self, remote: &RemoteUrl) -> PathBuf {
        // Hash the remote URL to get a stable filename
        let mut hasher = Sha256::new();
        hasher.update(remote.0.as_bytes());
        let hash = hasher.finalize();
        let hash_hex = hex::encode(&hash[..8]); // First 16 hex chars
        self.dir.join(format!("{}.wal", hash_hex))
    }

    /// Get the temporary file path for atomic writes.
    fn tmp_path(&self, remote: &RemoteUrl) -> PathBuf {
        let wal_path = self.wal_path(remote);
        wal_path.with_extension("wal.tmp")
    }

    /// Write state to WAL atomically.
    ///
    /// Uses write-to-temp + fsync + rename for crash safety.
    pub fn write(&self, remote: &RemoteUrl, entry: &WalEntry) -> Result<(), WalError> {
        let tmp_path = self.tmp_path(remote);
        let wal_path = self.wal_path(remote);

        // Serialize to JSON
        let data = serde_json::to_vec(entry)?;

        // Write to temp file
        let mut file = File::create(&tmp_path)?;
        file.write_all(&data)?;
        file.sync_all()?; // fsync for durability

        // Atomic rename
        fs::rename(&tmp_path, &wal_path)?;

        // fsync the directory to ensure rename is durable
        #[cfg(unix)]
        {
            if let Ok(dir) = File::open(&self.dir) {
                let _ = dir.sync_all();
            }
        }

        Ok(())
    }

    /// Read state from WAL if it exists.
    ///
    /// Returns None if no WAL file exists.
    /// Returns error if file exists but is corrupted.
    pub fn read(&self, remote: &RemoteUrl) -> Result<Option<WalEntry>, WalError> {
        let wal_path = self.wal_path(remote);

        if !wal_path.exists() {
            return Ok(None);
        }

        let data = fs::read(&wal_path)?;
        let entry: WalEntry = serde_json::from_slice(&data)?;

        // Version check
        if entry.version != WAL_VERSION {
            return Err(WalError::VersionMismatch {
                expected: WAL_VERSION,
                got: entry.version,
            });
        }

        Ok(Some(entry))
    }

    /// Delete WAL for a remote.
    ///
    /// Called after successful remote sync.
    pub fn delete(&self, remote: &RemoteUrl) -> Result<(), WalError> {
        let wal_path = self.wal_path(remote);
        let tmp_path = self.tmp_path(remote);

        // Remove both WAL and any stale temp file
        let _ = fs::remove_file(&wal_path);
        let _ = fs::remove_file(&tmp_path);

        Ok(())
    }

    /// Check if a WAL exists for a remote.
    pub fn exists(&self, remote: &RemoteUrl) -> bool {
        self.wal_path(remote).exists()
    }

    /// Clean up any stale temp files (from crashes during write).
    ///
    /// Called on startup.
    pub fn cleanup_stale(&self) -> Result<(), WalError> {
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "tmp") {
                    let _ = fs::remove_file(&path);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_remote() -> RemoteUrl {
        RemoteUrl("git@github.com:test/repo.git".into())
    }

    #[test]
    fn write_read_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::new(tmp.path()).unwrap();
        let remote = test_remote();

        let entry = WalEntry::new(
            CanonicalState::new(),
            Some("test-slug".into()),
            42,
            1234567890,
        );

        wal.write(&remote, &entry).unwrap();

        let loaded = wal.read(&remote).unwrap().unwrap();
        assert_eq!(loaded.version, WAL_VERSION);
        assert_eq!(loaded.root_slug, Some("test-slug".into()));
        assert_eq!(loaded.sequence, 42);
        assert_eq!(loaded.written_at_ms, 1234567890);
    }

    #[test]
    fn read_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::new(tmp.path()).unwrap();
        let remote = test_remote();

        assert!(wal.read(&remote).unwrap().is_none());
    }

    #[test]
    fn delete_removes_file() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::new(tmp.path()).unwrap();
        let remote = test_remote();

        let entry = WalEntry::new(CanonicalState::new(), None, 1, 0);
        wal.write(&remote, &entry).unwrap();
        assert!(wal.exists(&remote));

        wal.delete(&remote).unwrap();
        assert!(!wal.exists(&remote));
    }

    #[test]
    fn cleanup_stale_removes_tmp() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::new(tmp.path()).unwrap();

        // Create a stale .tmp file
        let stale = wal.dir.join("stale.wal.tmp");
        fs::write(&stale, b"garbage").unwrap();
        assert!(stale.exists());

        wal.cleanup_stale().unwrap();
        assert!(!stale.exists());
    }

    #[test]
    fn different_remotes_different_files() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::new(tmp.path()).unwrap();

        let remote1 = RemoteUrl("git@github.com:user/repo1.git".into());
        let remote2 = RemoteUrl("git@github.com:user/repo2.git".into());

        let entry1 = WalEntry::new(CanonicalState::new(), Some("slug1".into()), 1, 0);
        let entry2 = WalEntry::new(CanonicalState::new(), Some("slug2".into()), 2, 0);

        wal.write(&remote1, &entry1).unwrap();
        wal.write(&remote2, &entry2).unwrap();

        let loaded1 = wal.read(&remote1).unwrap().unwrap();
        let loaded2 = wal.read(&remote2).unwrap().unwrap();

        assert_eq!(loaded1.root_slug, Some("slug1".into()));
        assert_eq!(loaded2.root_slug, Some("slug2".into()));
    }
}
