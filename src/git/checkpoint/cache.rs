//! Local checkpoint cache helpers.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use thiserror::Error;

use super::json_canon::CanonJsonError;
use super::layout::{MANIFEST_FILE, META_FILE};
use super::{CheckpointExport, CheckpointManifest, CheckpointMeta};
use crate::core::{ContentHash, StoreId};
use crate::paths;

pub const DEFAULT_CHECKPOINT_CACHE_KEEP: usize = 3;
const CURRENT_FILE: &str = "CURRENT";
const TMP_DIR: &str = ".tmp";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointCacheEntry {
    pub checkpoint_id: ContentHash,
    pub dir: PathBuf,
    pub meta: CheckpointMeta,
    pub manifest: CheckpointManifest,
}

#[derive(Clone, Debug)]
pub struct CheckpointCache {
    store_id: StoreId,
    checkpoint_group: String,
    keep_last: usize,
}

impl CheckpointCache {
    pub fn new(store_id: StoreId, checkpoint_group: impl Into<String>) -> Self {
        Self {
            store_id,
            checkpoint_group: checkpoint_group.into(),
            keep_last: DEFAULT_CHECKPOINT_CACHE_KEEP,
        }
    }

    pub fn with_keep_last(mut self, keep_last: usize) -> Self {
        self.keep_last = keep_last.max(1);
        self
    }

    pub fn publish(
        &self,
        export: &CheckpointExport,
    ) -> Result<CheckpointCacheEntry, CheckpointCacheError> {
        self.publish_with_throttle(export, None)
    }

    pub fn publish_with_throttle(
        &self,
        export: &CheckpointExport,
        mut throttle: Option<&mut dyn FnMut(u64)>,
    ) -> Result<CheckpointCacheEntry, CheckpointCacheError> {
        if export.meta.checkpoint_group != self.checkpoint_group {
            return Err(CheckpointCacheError::GroupMismatch {
                expected: self.checkpoint_group.clone(),
                got: export.meta.checkpoint_group.clone(),
            });
        }

        let group_dir = self.group_dir();
        fs::create_dir_all(&group_dir).map_err(|source| io_err(&group_dir, source))?;

        let checkpoint_id = export.meta.content_hash;
        let checkpoint_hex = checkpoint_id.to_hex();
        let final_dir = group_dir.join(&checkpoint_hex);

        if final_dir.exists() {
            if !final_dir.is_dir() {
                return Err(CheckpointCacheError::InvalidEntry {
                    path: final_dir.clone(),
                    reason: "expected checkpoint directory".to_string(),
                });
            }
        } else {
            let tmp_root = group_dir.join(TMP_DIR);
            fs::create_dir_all(&tmp_root).map_err(|source| io_err(&tmp_root, source))?;
            let tmp_dir = tmp_root.join(&checkpoint_hex);
            if tmp_dir.exists() {
                fs::remove_dir_all(&tmp_dir).map_err(|source| io_err(&tmp_dir, source))?;
            }
            fs::create_dir_all(&tmp_dir).map_err(|source| io_err(&tmp_dir, source))?;
            write_checkpoint_tree(&tmp_dir, export, throttle.as_deref_mut())?;
            fs::rename(&tmp_dir, &final_dir).map_err(|source| io_err(&final_dir, source))?;
            fsync_dir(&group_dir)?;
        }

        write_current(&group_dir, &checkpoint_hex)?;
        prune_old_entries(&group_dir, self.keep_last, &checkpoint_hex)?;

        Ok(CheckpointCacheEntry {
            checkpoint_id,
            dir: final_dir,
            meta: export.meta.clone(),
            manifest: export.manifest.clone(),
        })
    }

    pub fn load_current(&self) -> Result<Option<CheckpointCacheEntry>, CheckpointCacheError> {
        let group_dir = self.group_dir();
        let current_path = group_dir.join(CURRENT_FILE);
        if !current_path.exists() {
            return Ok(None);
        }

        let current_raw =
            fs::read_to_string(&current_path).map_err(|source| io_err(&current_path, source))?;
        let current_id = current_raw.trim();
        if current_id.is_empty() {
            return Err(CheckpointCacheError::InvalidEntry {
                path: current_path,
                reason: "CURRENT is empty".to_string(),
            });
        }
        let checkpoint_id = ContentHash::from_hex(current_id).map_err(|err| {
            CheckpointCacheError::InvalidEntry {
                path: current_path,
                reason: format!("invalid checkpoint id: {err}"),
            }
        })?;

        let checkpoint_dir = group_dir.join(current_id);
        if !checkpoint_dir.is_dir() {
            return Err(CheckpointCacheError::InvalidEntry {
                path: checkpoint_dir,
                reason: "checkpoint directory missing".to_string(),
            });
        }

        let meta_path = checkpoint_dir.join(META_FILE);
        let manifest_path = checkpoint_dir.join(MANIFEST_FILE);
        let meta: CheckpointMeta = read_json(&meta_path)?;
        let manifest: CheckpointManifest = read_json(&manifest_path)?;

        let computed_meta = meta.compute_content_hash()?;
        if computed_meta != meta.content_hash {
            return Err(CheckpointCacheError::InvalidEntry {
                path: meta_path,
                reason: "meta content hash mismatch".to_string(),
            });
        }
        if meta.content_hash != checkpoint_id {
            return Err(CheckpointCacheError::InvalidEntry {
                path: meta_path,
                reason: "CURRENT does not match meta content hash".to_string(),
            });
        }
        let manifest_hash = manifest.manifest_hash()?;
        if manifest_hash != meta.manifest_hash {
            return Err(CheckpointCacheError::InvalidEntry {
                path: manifest_path,
                reason: "manifest hash mismatch".to_string(),
            });
        }

        Ok(Some(CheckpointCacheEntry {
            checkpoint_id,
            dir: checkpoint_dir,
            meta,
            manifest,
        }))
    }

    fn group_dir(&self) -> PathBuf {
        paths::checkpoint_cache_dir(self.store_id).join(&self.checkpoint_group)
    }
}

#[derive(Debug, Error)]
pub enum CheckpointCacheError {
    #[error("checkpoint cache group mismatch (expected {expected}, got {got})")]
    GroupMismatch { expected: String, got: String },
    #[error("checkpoint cache io error at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("checkpoint cache json error at {path:?}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error(transparent)]
    CanonJson(#[from] CanonJsonError),
    #[error("checkpoint cache entry invalid at {path:?}: {reason}")]
    InvalidEntry { path: PathBuf, reason: String },
}

fn write_checkpoint_tree(
    dir: &Path,
    export: &CheckpointExport,
    mut throttle: Option<&mut dyn FnMut(u64)>,
) -> Result<(), CheckpointCacheError> {
    let meta_bytes = export.meta.canon_bytes()?;
    write_bytes(&dir.join(META_FILE), &meta_bytes, throttle.as_deref_mut())?;
    let manifest_bytes = export.manifest.canon_bytes()?;
    write_bytes(
        &dir.join(MANIFEST_FILE),
        &manifest_bytes,
        throttle.as_deref_mut(),
    )?;

    for (path, payload) in &export.files {
        let file_path = dir.join(path);
        write_bytes(&file_path, payload.bytes.as_ref(), throttle.as_deref_mut())?;
    }

    Ok(())
}

fn write_bytes(
    path: &Path,
    bytes: &[u8],
    mut throttle: Option<&mut dyn FnMut(u64)>,
) -> Result<(), CheckpointCacheError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| io_err(parent, source))?;
    }

    if let Some(throttle) = throttle.as_mut() {
        throttle(bytes.len() as u64);
    }

    let mut file = File::create(path).map_err(|source| io_err(path, source))?;
    file.write_all(bytes)
        .map_err(|source| io_err(path, source))?;
    file.sync_all().map_err(|source| io_err(path, source))?;

    Ok(())
}

fn write_current(group_dir: &Path, checkpoint_id: &str) -> Result<(), CheckpointCacheError> {
    let tmp_path = group_dir.join(format!("{CURRENT_FILE}.tmp"));
    let final_path = group_dir.join(CURRENT_FILE);

    let mut file = File::create(&tmp_path).map_err(|source| io_err(&tmp_path, source))?;
    file.write_all(checkpoint_id.as_bytes())
        .map_err(|source| io_err(&tmp_path, source))?;
    file.write_all(b"\n")
        .map_err(|source| io_err(&tmp_path, source))?;
    file.sync_all()
        .map_err(|source| io_err(&tmp_path, source))?;

    fs::rename(&tmp_path, &final_path).map_err(|source| io_err(&final_path, source))?;
    fsync_dir(group_dir)?;

    Ok(())
}

fn prune_old_entries(
    group_dir: &Path,
    keep_last: usize,
    keep_checkpoint_id: &str,
) -> Result<(), CheckpointCacheError> {
    let keep_last = keep_last.max(1);
    let mut entries: Vec<CacheEntry> = Vec::new();

    for entry in fs::read_dir(group_dir).map_err(|source| io_err(group_dir, source))? {
        let entry = entry.map_err(|source| io_err(group_dir, source))?;
        let file_type = entry
            .file_type()
            .map_err(|source| io_err(&entry.path(), source))?;
        if !file_type.is_dir() {
            continue;
        }

        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == CURRENT_FILE || name == TMP_DIR {
            continue;
        }

        let path = entry.path();
        let created_at_ms = read_meta_created_at(&path).unwrap_or(0);
        entries.push(CacheEntry {
            id: name.to_string(),
            path,
            created_at_ms,
        });
    }

    entries.sort_by(|a, b| b.created_at_ms.cmp(&a.created_at_ms));

    let mut keep: HashSet<String> = HashSet::new();
    keep.insert(keep_checkpoint_id.to_string());
    for entry in &entries {
        if keep.len() >= keep_last {
            break;
        }
        keep.insert(entry.id.clone());
    }

    for entry in entries {
        if keep.contains(&entry.id) {
            continue;
        }
        fs::remove_dir_all(&entry.path).map_err(|source| io_err(&entry.path, source))?;
        let archive_path = group_dir.join(format!("{}.tar.zst", entry.id));
        let _ = fs::remove_file(&archive_path);
    }

    Ok(())
}

fn read_meta_created_at(dir: &Path) -> Result<u64, CheckpointCacheError> {
    let meta_path = dir.join(META_FILE);
    let meta: CheckpointMeta = read_json(&meta_path)?;
    Ok(meta.created_at_ms)
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, CheckpointCacheError> {
    let bytes = fs::read(path).map_err(|source| io_err(path, source))?;
    serde_json::from_slice(&bytes).map_err(|source| CheckpointCacheError::Json {
        path: path.to_path_buf(),
        source,
    })
}

fn io_err(path: &Path, source: std::io::Error) -> CheckpointCacheError {
    CheckpointCacheError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(unix)]
fn fsync_dir(path: &Path) -> Result<(), CheckpointCacheError> {
    let dir = File::open(path).map_err(|source| io_err(path, source))?;
    dir.sync_all().map_err(|source| io_err(path, source))?;
    Ok(())
}

#[cfg(not(unix))]
fn fsync_dir(_path: &Path) -> Result<(), CheckpointCacheError> {
    Ok(())
}

struct CacheEntry {
    id: String,
    path: PathBuf,
    created_at_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::core::bead::{BeadCore, BeadFields};
    use crate::core::collections::Labels;
    use crate::core::composite::{Claim, Workflow};
    use crate::core::crdt::Lww;
    use crate::core::domain::{BeadType, Priority};
    use crate::core::identity::BeadId;
    use crate::core::time::{Stamp, WriteStamp};
    use crate::core::{
        ActorId, CanonicalState, ContentHash, Durable, HeadStatus, NamespaceId, ReplicaId, Seq0,
        StoreEpoch, StoreId, StoreState, Watermarks,
    };
    use crate::git::checkpoint::{
        CheckpointExportInput, CheckpointSnapshotInput, build_snapshot, export_checkpoint,
    };
    use crate::paths;

    fn make_stamp(wall_ms: u64, counter: u32, actor: &str) -> Stamp {
        Stamp::new(
            WriteStamp::new(wall_ms, counter),
            ActorId::new(actor).expect("actor id"),
        )
    }

    fn make_bead(id: &BeadId, stamp: &Stamp) -> crate::core::Bead {
        let core = BeadCore::new(id.clone(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("title".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            labels: Lww::new(Labels::new(), stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        crate::core::Bead::new(core, fields)
    }

    fn build_export(store_id: StoreId, created_at_ms: u64) -> CheckpointExport {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
        let stamp = make_stamp(1, 0, "author");
        let bead_id = BeadId::parse("beads-rs-0001").unwrap();

        let bead = make_bead(&bead_id, &stamp);
        let mut core_state = CanonicalState::new();
        core_state.insert(bead).unwrap();
        let mut state = StoreState::new();
        state.set_namespace_state(namespace.clone(), core_state);

        let mut watermarks = Watermarks::<Durable>::new();
        watermarks
            .observe_at_least(
                &namespace,
                &origin,
                Seq0::new(1),
                HeadStatus::Known([1u8; 32]),
            )
            .unwrap();

        let snapshot = build_snapshot(CheckpointSnapshotInput {
            checkpoint_group: "core".to_string(),
            namespaces: vec![namespace.clone()],
            store_id,
            store_epoch: StoreEpoch::new(0),
            created_at_ms,
            created_by_replica_id: origin,
            policy_hash: ContentHash::from_bytes([9u8; 32]),
            roster_hash: None,
            state: &state,
            watermarks_durable: &watermarks,
        })
        .expect("snapshot");

        export_checkpoint(CheckpointExportInput {
            snapshot: &snapshot,
            previous: None,
        })
        .expect("export")
    }

    fn list_checkpoint_dirs(group_dir: &Path) -> Vec<String> {
        let mut out = Vec::new();
        if let Ok(entries) = fs::read_dir(group_dir) {
            for entry in entries.flatten() {
                if let Ok(file_type) = entry.file_type() {
                    if !file_type.is_dir() {
                        continue;
                    }
                }
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name == CURRENT_FILE || name == TMP_DIR {
                    continue;
                }
                out.push(name.to_string());
            }
        }
        out
    }

    #[test]
    fn cache_publish_updates_current_and_loads() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([9u8; 16]));
        let export = build_export(store_id, 1_700_000_000_000);

        let cache = CheckpointCache::new(store_id, export.meta.checkpoint_group.clone());
        let entry = cache.publish(&export).expect("publish");

        let current_path = cache.group_dir().join(CURRENT_FILE);
        let current = fs::read_to_string(&current_path).expect("read current");
        assert_eq!(current.trim(), entry.checkpoint_id.to_hex());
        assert!(entry.dir.is_dir());

        let tmp_path = cache
            .group_dir()
            .join(TMP_DIR)
            .join(entry.checkpoint_id.to_hex());
        assert!(!tmp_path.exists());

        let loaded = cache
            .load_current()
            .expect("load current")
            .expect("current entry");
        assert_eq!(loaded.checkpoint_id, entry.checkpoint_id);
        assert_eq!(loaded.meta, entry.meta);
        assert_eq!(loaded.manifest, entry.manifest);
    }

    #[test]
    fn cache_prunes_old_entries() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
        let cache = CheckpointCache::new(store_id, "core").with_keep_last(2);

        let first = cache.publish(&build_export(store_id, 10)).unwrap();
        let second = cache.publish(&build_export(store_id, 20)).unwrap();
        let third = cache.publish(&build_export(store_id, 30)).unwrap();

        let dirs = list_checkpoint_dirs(&cache.group_dir());
        assert_eq!(dirs.len(), 2);
        assert!(!dirs.contains(&first.checkpoint_id.to_hex()));
        assert!(dirs.contains(&second.checkpoint_id.to_hex()));
        assert!(dirs.contains(&third.checkpoint_id.to_hex()));
    }

    #[test]
    fn load_current_errors_on_missing_entry() {
        let temp = TempDir::new().expect("temp dir");
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));

        let store_id = StoreId::new(Uuid::from_bytes([5u8; 16]));
        let cache = CheckpointCache::new(store_id, "core");
        let group_dir = cache.group_dir();
        fs::create_dir_all(&group_dir).expect("create group dir");
        let bogus_id = ContentHash::from_bytes([8u8; 32]).to_hex();
        fs::write(group_dir.join(CURRENT_FILE), format!("{}\n", bogus_id)).expect("write current");

        let err = cache.load_current().unwrap_err();
        assert!(matches!(err, CheckpointCacheError::InvalidEntry { .. }));
    }
}
