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
use crate::core::{Bead, BeadId, CanonicalState, DepEdge, DepKey, Tombstone, TombstoneKey};

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
    #[serde(with = "wal_state")]
    pub state: CanonicalState,
    /// Root slug for bead IDs.
    pub root_slug: Option<String>,
    /// Monotonic sequence number.
    pub sequence: u64,
}

mod wal_state {
    use super::*;
    use serde::de::Error as DeError;
    use serde::{Deserializer, Serializer};
    use std::collections::BTreeMap;

    #[derive(Serialize, Deserialize)]
    struct WalStateVec {
        live: Vec<Bead>,
        tombstones: Vec<Tombstone>,
        deps: Vec<DepEdge>,
    }

    #[derive(Deserialize)]
    struct WalStateMap {
        live: BTreeMap<BeadId, Bead>,
        tombstones: BTreeMap<TombstoneKey, Tombstone>,
        deps: BTreeMap<DepKey, DepEdge>,
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum WalStateRepr {
        Vecs(WalStateVec),
        Maps(WalStateMap),
    }

    pub fn serialize<S>(state: &CanonicalState, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let snapshot = WalStateVec {
            live: state.iter_live().map(|(_, bead)| bead.clone()).collect(),
            tombstones: state
                .iter_tombstones()
                .map(|(_, tomb)| tomb.clone())
                .collect(),
            deps: state.iter_deps().map(|(_, dep)| dep.clone()).collect(),
        };
        snapshot.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<CanonicalState, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (live, tombstones, deps) = match WalStateRepr::deserialize(deserializer)? {
            WalStateRepr::Vecs(snapshot) => (snapshot.live, snapshot.tombstones, snapshot.deps),
            WalStateRepr::Maps(snapshot) => (
                snapshot.live.into_values().collect(),
                snapshot.tombstones.into_values().collect(),
                snapshot.deps.into_values().collect(),
            ),
        };
        let mut state = CanonicalState::new();
        for bead in live {
            state.insert(bead).map_err(DeError::custom)?;
        }
        for tombstone in tombstones {
            state.insert_tombstone(tombstone);
        }
        for dep in deps {
            state.insert_dep(dep);
        }
        Ok(state)
    }
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
/// Stores per-remote WAL files in a subdirectory of a persistent base dir.
pub struct Wal {
    dir: PathBuf,
}

impl Wal {
    /// Create a new WAL manager.
    ///
    /// Creates the WAL directory if it doesn't exist.
    pub fn new(base_dir: &Path) -> Result<Self, WalError> {
        let dir = base_dir.join("wal");
        fs::create_dir_all(&dir)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o700));
        }

        Ok(Wal { dir })
    }

    /// Best-effort migration from a legacy runtime WAL directory.
    ///
    /// The legacy path is `<runtime_dir>/wal`. Any WAL files found there are
    /// copied into the persistent WAL dir. If both exist, the newer entry wins.
    pub fn migrate_from_runtime_dir(&self, runtime_dir: &Path) {
        let legacy_dir = runtime_dir.join("wal");
        if legacy_dir == self.dir || !legacy_dir.exists() {
            return;
        }

        let entries = match fs::read_dir(&legacy_dir) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("wal migration: failed to read {:?}: {}", legacy_dir, e);
                return;
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_none_or(|e| e != "wal") {
                continue;
            }
            let file_name = match path.file_name() {
                Some(name) => name.to_os_string(),
                None => continue,
            };
            let dest = self.dir.join(&file_name);
            if dest == path {
                continue;
            }

            if !dest.exists() {
                if let Err(e) = copy_then_remove(&path, &dest) {
                    tracing::warn!("wal migration: failed to move {:?}: {}", path, e);
                }
                continue;
            }

            let src_entry = read_entry_at(&path);
            let dest_entry = read_entry_at(&dest);

            match (src_entry, dest_entry) {
                (Ok(src), Ok(dest_entry)) => {
                    if is_newer(&src, &dest_entry) {
                        if let Err(e) = copy_then_remove(&path, &dest) {
                            tracing::warn!("wal migration: failed to update {:?}: {}", dest, e);
                        }
                    } else if let Err(e) = fs::remove_file(&path) {
                        tracing::warn!("wal migration: failed to remove {:?}: {}", path, e);
                    }
                }
                (Ok(_), Err(_)) => {
                    if let Err(e) = copy_then_remove(&path, &dest) {
                        tracing::warn!("wal migration: failed to update {:?}: {}", dest, e);
                    }
                }
                (Err(e), Ok(_)) => {
                    tracing::warn!(
                        "wal migration: keeping legacy WAL {:?} (unreadable): {}",
                        path,
                        e
                    );
                }
                (Err(e1), Err(e2)) => {
                    tracing::warn!(
                        "wal migration: keeping legacy WAL {:?} (unreadable): {}, {}",
                        path,
                        e1,
                        e2
                    );
                }
            }
        }
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

/// Default base directory for WAL storage.
///
/// Uses `BD_WAL_DIR` if set, otherwise `$XDG_DATA_HOME/beads-rs` or
/// `~/.local/share/beads-rs`.
pub fn default_wal_base_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("BD_WAL_DIR")
        && !dir.trim().is_empty()
    {
        return PathBuf::from(dir);
    }

    data_home().join("beads-rs")
}

fn data_home() -> PathBuf {
    std::env::var("XDG_DATA_HOME")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join(".local")
                .join("share")
        })
}

fn read_entry_at(path: &Path) -> Result<WalEntry, WalError> {
    let data = fs::read(path)?;
    let entry: WalEntry = serde_json::from_slice(&data)?;
    if entry.version != WAL_VERSION {
        return Err(WalError::VersionMismatch {
            expected: WAL_VERSION,
            got: entry.version,
        });
    }
    Ok(entry)
}

fn is_newer(a: &WalEntry, b: &WalEntry) -> bool {
    a.sequence > b.sequence || (a.sequence == b.sequence && a.written_at_ms > b.written_at_ms)
}

fn copy_then_remove(src: &Path, dest: &Path) -> Result<(), WalError> {
    fs::copy(src, dest)?;
    fs::remove_file(src)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        ActorId, BeadCore, BeadFields, BeadId, BeadType, Claim, DepKey, DepKind, Lww, Priority,
        Stamp, Workflow, WriteStamp,
    };
    use serde::Serialize;
    use std::collections::BTreeMap;
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

    fn make_bead(id: &str, stamp: &Stamp) -> Bead {
        let core = BeadCore::new(BeadId::parse(id).unwrap(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("test".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::new(2).unwrap(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            labels: Lww::new(Default::default(), stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::Open, stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        Bead::new(core, fields)
    }

    #[test]
    fn write_read_roundtrip_with_tombstones_and_deps() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::new(tmp.path()).unwrap();
        let remote = test_remote();

        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(1234, 0), actor);

        let mut state = CanonicalState::new();
        state.insert(make_bead("bd-abc", &stamp)).unwrap();
        state.insert_tombstone(Tombstone::new(
            BeadId::parse("bd-del").unwrap(),
            stamp.clone(),
            None,
        ));
        let dep_key = DepKey::new(
            BeadId::parse("bd-abc").unwrap(),
            BeadId::parse("bd-def").unwrap(),
            DepKind::Blocks,
        )
        .unwrap();
        state.insert_dep(DepEdge::new(dep_key, stamp.clone()));

        let entry = WalEntry::new(state, None, 7, 42);
        wal.write(&remote, &entry).unwrap();

        let loaded = wal.read(&remote).unwrap().unwrap();
        assert_eq!(loaded.state.live_count(), 1);
        assert_eq!(loaded.state.tombstone_count(), 1);
        assert_eq!(loaded.state.dep_count(), 1);
    }

    #[test]
    fn read_legacy_map_state_format() {
        #[derive(Serialize)]
        struct LegacyWalState {
            live: BTreeMap<BeadId, Bead>,
            tombstones: BTreeMap<TombstoneKey, Tombstone>,
            deps: BTreeMap<DepKey, DepEdge>,
        }

        #[derive(Serialize)]
        struct LegacyWalEntry {
            version: u32,
            written_at_ms: u64,
            state: LegacyWalState,
            root_slug: Option<String>,
            sequence: u64,
        }

        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(1, 0), actor);
        let bead = make_bead("bd-abc", &stamp);

        let mut live = BTreeMap::new();
        live.insert(bead.core.id.clone(), bead);

        let legacy = LegacyWalEntry {
            version: WAL_VERSION,
            written_at_ms: 1,
            state: LegacyWalState {
                live,
                tombstones: BTreeMap::new(),
                deps: BTreeMap::new(),
            },
            root_slug: None,
            sequence: 1,
        };

        let data = serde_json::to_vec(&legacy).unwrap();
        let loaded: WalEntry = serde_json::from_slice(&data).unwrap();
        assert_eq!(loaded.state.live_count(), 1);
        assert_eq!(loaded.state.tombstone_count(), 0);
        assert_eq!(loaded.state.dep_count(), 0);
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

    #[test]
    fn migrate_from_runtime_dir_moves_wal() {
        let legacy_base = TempDir::new().unwrap();
        let new_base = TempDir::new().unwrap();
        let legacy = Wal::new(legacy_base.path()).unwrap();
        let current = Wal::new(new_base.path()).unwrap();
        let remote = test_remote();

        let entry = WalEntry::new(CanonicalState::new(), None, 1, 123);
        legacy.write(&remote, &entry).unwrap();
        assert!(legacy.exists(&remote));

        current.migrate_from_runtime_dir(legacy_base.path());
        assert!(current.exists(&remote));
        assert!(!legacy.exists(&remote));
    }

    #[test]
    fn migrate_prefers_newer_sequence() {
        let legacy_base = TempDir::new().unwrap();
        let new_base = TempDir::new().unwrap();
        let legacy = Wal::new(legacy_base.path()).unwrap();
        let current = Wal::new(new_base.path()).unwrap();
        let remote = test_remote();

        let older = WalEntry::new(CanonicalState::new(), None, 1, 100);
        let newer = WalEntry::new(CanonicalState::new(), None, 2, 200);

        current.write(&remote, &older).unwrap();
        legacy.write(&remote, &newer).unwrap();

        current.migrate_from_runtime_dir(legacy_base.path());
        let loaded = current.read(&remote).unwrap().unwrap();
        assert_eq!(loaded.sequence, 2);
    }
}
