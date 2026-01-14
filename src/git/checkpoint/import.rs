//! Checkpoint import + verification.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256 as Sha2};
use thiserror::Error;

use super::json_canon::CanonJsonError;
use super::layout::{CheckpointFileKind, MANIFEST_FILE, META_FILE, parse_shard_path};
use super::{CheckpointManifest, CheckpointMeta, IncludedHeads, IncludedWatermarks};
use crate::core::error::CoreError;
use crate::core::wire_bead::{WireBeadFull, WireDepV1, WireStamp, WireTombstoneV1};
use crate::core::{
    CanonicalState, ContentHash, DepEdge, DepKey, Limits, NamespaceId, StoreState, Tombstone,
    WriteStamp, sha256_bytes,
};

#[derive(Debug, Error)]
pub enum CheckpointImportError {
    #[error("checkpoint io error at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("checkpoint json parse error at {path:?}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error(transparent)]
    CanonJson(#[from] CanonJsonError),
    #[error("checkpoint manifest hash mismatch (expected {expected}, got {got})")]
    ManifestHashMismatch {
        expected: ContentHash,
        got: ContentHash,
    },
    #[error("checkpoint content hash mismatch (expected {expected}, got {got})")]
    ContentHashMismatch {
        expected: ContentHash,
        got: ContentHash,
    },
    #[error("checkpoint store id mismatch (meta {meta}, manifest {manifest})")]
    StoreIdMismatch {
        meta: crate::core::StoreId,
        manifest: crate::core::StoreId,
    },
    #[error("checkpoint store epoch mismatch (meta {meta}, manifest {manifest})")]
    StoreEpochMismatch {
        meta: crate::core::StoreEpoch,
        manifest: crate::core::StoreEpoch,
    },
    #[error("checkpoint group mismatch (meta {meta}, manifest {manifest})")]
    GroupMismatch { meta: String, manifest: String },
    #[error("checkpoint namespaces mismatch (meta {meta:?}, manifest {manifest:?})")]
    NamespacesMismatch {
        meta: Vec<NamespaceId>,
        manifest: Vec<NamespaceId>,
    },
    #[error("checkpoint file missing at {path:?}")]
    MissingFile { path: PathBuf },
    #[error("checkpoint file size mismatch for {path:?}: expected {expected}, got {got}")]
    FileSizeMismatch {
        path: PathBuf,
        expected: u64,
        got: u64,
    },
    #[error("checkpoint file hash mismatch for {path:?}: expected {expected}, got {got}")]
    FileHashMismatch {
        path: PathBuf,
        expected: ContentHash,
        got: ContentHash,
    },
    #[error("checkpoint jsonl shard too large at {path:?}: max {max_bytes}, got {got_bytes}")]
    ShardTooLarge {
        path: PathBuf,
        max_bytes: u64,
        got_bytes: u64,
    },
    #[error(
        "checkpoint jsonl line too large at {path:?} line {line}: max {max_bytes}, got {got_bytes}"
    )]
    LineTooLong {
        path: PathBuf,
        line: u64,
        max_bytes: u64,
        got_bytes: u64,
    },
    #[error("checkpoint jsonl parse error at {path:?} line {line}: {source}")]
    JsonLine {
        path: PathBuf,
        line: u64,
        #[source]
        source: serde_json::Error,
    },
    #[error("checkpoint contains unexpected file {path}")]
    UnexpectedFile { path: String },
    #[error("invalid tombstone lineage at {path:?} line {line}")]
    InvalidLineage { path: PathBuf, line: u64 },
    #[error("invalid dep at {path:?} line {line}: {reason}")]
    InvalidDep {
        path: PathBuf,
        line: u64,
        reason: String,
    },
    #[error("checkpoint merge failed: {0:?}")]
    Merge(Vec<CoreError>),
}

#[derive(Debug, Clone)]
pub struct CheckpointImport {
    pub state: StoreState,
    pub included: IncludedWatermarks,
    pub included_heads: Option<IncludedHeads>,
}

pub fn import_checkpoint(
    dir: &Path,
    limits: &Limits,
) -> Result<CheckpointImport, CheckpointImportError> {
    let meta_path = dir.join(META_FILE);
    let manifest_path = dir.join(MANIFEST_FILE);

    let meta_bytes = read_file_bytes(&meta_path)?;
    let manifest_bytes = read_file_bytes(&manifest_path)?;

    let meta: CheckpointMeta =
        serde_json::from_slice(&meta_bytes).map_err(|source| CheckpointImportError::Json {
            path: meta_path.clone(),
            source,
        })?;
    let manifest: CheckpointManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|source| CheckpointImportError::Json {
            path: manifest_path.clone(),
            source,
        })?;

    if meta.store_id != manifest.store_id {
        return Err(CheckpointImportError::StoreIdMismatch {
            meta: meta.store_id,
            manifest: manifest.store_id,
        });
    }
    if meta.store_epoch != manifest.store_epoch {
        return Err(CheckpointImportError::StoreEpochMismatch {
            meta: meta.store_epoch,
            manifest: manifest.store_epoch,
        });
    }
    if meta.checkpoint_group != manifest.checkpoint_group {
        return Err(CheckpointImportError::GroupMismatch {
            meta: meta.checkpoint_group.clone(),
            manifest: manifest.checkpoint_group.clone(),
        });
    }

    let mut meta_namespaces = meta.namespaces.clone();
    meta_namespaces.sort();
    meta_namespaces.dedup();
    let mut manifest_namespaces = manifest.namespaces.clone();
    manifest_namespaces.sort();
    manifest_namespaces.dedup();
    if meta_namespaces != manifest_namespaces {
        return Err(CheckpointImportError::NamespacesMismatch {
            meta: meta_namespaces,
            manifest: manifest_namespaces,
        });
    }

    let manifest_hash = manifest.manifest_hash()?;
    if manifest_hash != meta.manifest_hash {
        return Err(CheckpointImportError::ManifestHashMismatch {
            expected: meta.manifest_hash,
            got: manifest_hash,
        });
    }

    let content_hash = meta.compute_content_hash()?;
    if content_hash != meta.content_hash {
        return Err(CheckpointImportError::ContentHashMismatch {
            expected: meta.content_hash,
            got: content_hash,
        });
    }

    let mut state = StoreState::new();
    let allowed_namespaces: BTreeSet<NamespaceId> = manifest_namespaces.iter().cloned().collect();

    for (rel_path, entry) in &manifest.files {
        let full_path = dir.join(rel_path);
        if !full_path.exists() {
            return Err(CheckpointImportError::MissingFile {
                path: full_path.clone(),
            });
        }

        if rel_path == META_FILE {
            verify_bytes(&meta_bytes, entry, &full_path)?;
            continue;
        }
        if rel_path == MANIFEST_FILE {
            verify_bytes(&manifest_bytes, entry, &full_path)?;
            continue;
        }

        let shard =
            parse_shard_path(rel_path).ok_or_else(|| CheckpointImportError::UnexpectedFile {
                path: rel_path.clone(),
            })?;
        if !allowed_namespaces.contains(&shard.namespace) {
            return Err(CheckpointImportError::UnexpectedFile {
                path: rel_path.clone(),
            });
        }

        if entry.bytes > limits.max_jsonl_shard_bytes as u64 {
            return Err(CheckpointImportError::ShardTooLarge {
                path: full_path.clone(),
                max_bytes: limits.max_jsonl_shard_bytes as u64,
                got_bytes: entry.bytes,
            });
        }

        let stats = match shard.kind {
            CheckpointFileKind::State => parse_jsonl_file::<WireBeadFull, _>(
                &full_path,
                limits,
                |line, wire| {
                    let bead = crate::core::Bead::from(wire);
                    let ns = line.namespace.clone();
                    state.ensure_namespace(ns).insert_live(bead);
                    Ok(())
                },
            )?,
            CheckpointFileKind::Tombstones => parse_jsonl_file::<WireTombstoneV1, _>(
                &full_path,
                limits,
                |line, wire| {
                    let tomb = tombstone_from_wire(&wire, &full_path, line)?;
                    let ns = line.namespace.clone();
                    state.ensure_namespace(ns).insert_tombstone(tomb);
                    Ok(())
                },
            )?,
            CheckpointFileKind::Deps => parse_jsonl_file::<WireDepV1, _>(
                &full_path,
                limits,
                |line, wire| {
                    let (key, edge) = dep_from_wire(&wire, &full_path, line)?;
                    let ns = line.namespace.clone();
                    state.ensure_namespace(ns).insert_dep(key, edge);
                    Ok(())
                },
            )?,
        };

        verify_stats(&stats, entry, &full_path)?;
    }

    Ok(CheckpointImport {
        state,
        included: meta.included.clone(),
        included_heads: meta.included_heads.clone(),
    })
}

pub fn merge_store_states(
    a: &StoreState,
    b: &StoreState,
) -> Result<StoreState, CheckpointImportError> {
    let mut merged = StoreState::new();
    let mut errors = Vec::new();

    let mut namespaces: BTreeSet<NamespaceId> = BTreeSet::new();
    namespaces.extend(a.namespaces().map(|(ns, _)| ns.clone()));
    namespaces.extend(b.namespaces().map(|(ns, _)| ns.clone()));

    for namespace in namespaces {
        let left = a.get(&namespace);
        let right = b.get(&namespace);
        let out = match (left, right) {
            (Some(a_state), Some(b_state)) => match CanonicalState::join(a_state, b_state) {
                Ok(state) => state,
                Err(errs) => {
                    errors.extend(errs);
                    a_state.clone()
                }
            },
            (Some(state), None) | (None, Some(state)) => state.clone(),
            (None, None) => CanonicalState::default(),
        };
        merged.ensure_namespace(namespace).clone_from(&out);
    }

    if errors.is_empty() {
        Ok(merged)
    } else {
        Err(CheckpointImportError::Merge(errors))
    }
}

struct JsonlStats {
    sha256: ContentHash,
    bytes: u64,
}

struct JsonlLineContext<'a> {
    path: &'a Path,
    line_no: u64,
    namespace: NamespaceId,
}

fn parse_jsonl_file<T, F>(
    path: &Path,
    limits: &Limits,
    mut on_item: F,
) -> Result<JsonlStats, CheckpointImportError>
where
    T: DeserializeOwned,
    F: FnMut(JsonlLineContext<'_>, T) -> Result<(), CheckpointImportError>,
{
    let file = File::open(path).map_err(|source| CheckpointImportError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let mut buf = Vec::new();
    let mut hasher = Sha2::new();
    let mut total_bytes = 0u64;
    let mut line_no = 0u64;

    loop {
        buf.clear();
        let read =
            reader
                .read_until(b'\n', &mut buf)
                .map_err(|source| CheckpointImportError::Io {
                    path: path.to_path_buf(),
                    source,
                })?;
        if read == 0 {
            break;
        }
        total_bytes = total_bytes.saturating_add(read as u64);
        if total_bytes > limits.max_jsonl_shard_bytes as u64 {
            return Err(CheckpointImportError::ShardTooLarge {
                path: path.to_path_buf(),
                max_bytes: limits.max_jsonl_shard_bytes as u64,
                got_bytes: total_bytes,
            });
        }
        hasher.update(&buf);

        if buf.len() > limits.max_jsonl_line_bytes {
            return Err(CheckpointImportError::LineTooLong {
                path: path.to_path_buf(),
                line: line_no + 1,
                max_bytes: limits.max_jsonl_line_bytes as u64,
                got_bytes: buf.len() as u64,
            });
        }

        if buf.iter().all(|b| b.is_ascii_whitespace()) {
            line_no += 1;
            continue;
        }

        let line = if buf.ends_with(b"\n") {
            &buf[..buf.len() - 1]
        } else {
            &buf[..]
        };
        line_no += 1;
        let value: T = serde_json::from_slice(line).map_err(|source| {
            CheckpointImportError::JsonLine {
                path: path.to_path_buf(),
                line: line_no,
                source,
            }
        })?;

        let namespace = namespace_from_path(path)?;
        let ctx = JsonlLineContext {
            path,
            line_no,
            namespace,
        };
        on_item(ctx, value)?;
    }

    let digest = hasher.finalize();
    let mut buf = [0u8; 32];
    buf.copy_from_slice(&digest);
    Ok(JsonlStats {
        sha256: ContentHash::from_bytes(buf),
        bytes: total_bytes,
    })
}

fn namespace_from_path(path: &Path) -> Result<NamespaceId, CheckpointImportError> {
    let mut parts = path
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .collect::<Vec<_>>();
    parts.reverse();
    let mut iter = parts.into_iter();
    let _file = iter.next();
    let _kind = iter.next();
    let namespace = iter.next().ok_or_else(|| CheckpointImportError::UnexpectedFile {
        path: path.display().to_string(),
    })?;
    NamespaceId::parse(namespace.to_string()).map_err(|_| CheckpointImportError::UnexpectedFile {
        path: path.display().to_string(),
    })
}

fn verify_bytes(
    bytes: &[u8],
    entry: &super::manifest::ManifestFile,
    path: &Path,
) -> Result<(), CheckpointImportError> {
    let actual_bytes = bytes.len() as u64;
    if actual_bytes != entry.bytes {
        return Err(CheckpointImportError::FileSizeMismatch {
            path: path.to_path_buf(),
            expected: entry.bytes,
            got: actual_bytes,
        });
    }
    let hash = ContentHash::from_bytes(sha256_bytes(bytes).0);
    if hash != entry.sha256 {
        return Err(CheckpointImportError::FileHashMismatch {
            path: path.to_path_buf(),
            expected: entry.sha256,
            got: hash,
        });
    }
    Ok(())
}

fn verify_stats(
    stats: &JsonlStats,
    entry: &super::manifest::ManifestFile,
    path: &Path,
) -> Result<(), CheckpointImportError> {
    if stats.bytes != entry.bytes {
        return Err(CheckpointImportError::FileSizeMismatch {
            path: path.to_path_buf(),
            expected: entry.bytes,
            got: stats.bytes,
        });
    }
    if stats.sha256 != entry.sha256 {
        return Err(CheckpointImportError::FileHashMismatch {
            path: path.to_path_buf(),
            expected: entry.sha256,
            got: stats.sha256,
        });
    }
    Ok(())
}

fn read_file_bytes(path: &Path) -> Result<Vec<u8>, CheckpointImportError> {
    let mut file = File::open(path).map_err(|source| CheckpointImportError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .map_err(|source| CheckpointImportError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(buf)
}

fn tombstone_from_wire(
    wire: &WireTombstoneV1,
    path: &Path,
    line: JsonlLineContext<'_>,
) -> Result<Tombstone, CheckpointImportError> {
    let deleted = StampFromWire::stamp(wire.deleted_at, &wire.deleted_by);
    let lineage = match (wire.lineage_created_at, wire.lineage_created_by.clone()) {
        (None, None) => None,
        (Some(at), Some(by)) => Some(StampFromWire::stamp(at, &by)),
        _ => {
            return Err(CheckpointImportError::InvalidLineage {
                path: path.to_path_buf(),
                line: line.line_no,
            });
        }
    };

    Ok(match lineage {
        Some(stamp) => {
            Tombstone::new_collision(wire.id.clone(), deleted, stamp, wire.reason.clone())
        }
        None => Tombstone::new(wire.id.clone(), deleted, wire.reason.clone()),
    })
}

fn dep_from_wire(
    wire: &WireDepV1,
    path: &Path,
    line: JsonlLineContext<'_>,
) -> Result<(DepKey, DepEdge), CheckpointImportError> {
    let created = StampFromWire::stamp(wire.created_at, &wire.created_by);
    let mut edge = DepEdge::new(created.clone());
    match (wire.deleted_at, wire.deleted_by.clone()) {
        (None, None) => {}
        (Some(at), Some(by)) => {
            let deleted = StampFromWire::stamp(at, &by);
            edge.delete(deleted);
        }
        _ => {
            return Err(CheckpointImportError::InvalidDep {
                path: path.to_path_buf(),
                line: line.line_no,
                reason: "deleted_at and deleted_by must be set together".into(),
            });
        }
    }

    let key = DepKey::new(wire.from.clone(), wire.to.clone(), wire.kind).map_err(|err| {
        CheckpointImportError::InvalidDep {
            path: path.to_path_buf(),
            line: line.line_no,
            reason: err.to_string(),
        }
    })?;

    Ok((key, edge))
}

struct StampFromWire;

impl StampFromWire {
    fn stamp(stamp: WireStamp, actor: &crate::core::ActorId) -> crate::core::Stamp {
        crate::core::Stamp::new(WriteStamp::from(stamp), actor.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::core::{NamespaceId, ReplicaId, StoreEpoch, StoreId};

    fn write_file(path: &Path, bytes: &[u8]) {
        std::fs::create_dir_all(path.parent().expect("parent")).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    fn minimal_manifest_and_meta(dir: &Path) -> (CheckpointManifest, CheckpointMeta) {
        let store_id = StoreId::new(Uuid::from_u128(1));
        let store_epoch = StoreEpoch::new(0);
        let manifest = CheckpointManifest {
            checkpoint_group: "core".to_string(),
            store_id,
            store_epoch,
            namespaces: vec![NamespaceId::core()],
            files: Default::default(),
        };
        let manifest_hash = manifest.manifest_hash().unwrap();
        let meta = CheckpointMeta {
            checkpoint_format_version: 1,
            store_id,
            store_epoch,
            checkpoint_group: "core".to_string(),
            namespaces: vec![NamespaceId::core()],
            created_at_ms: 1,
            created_by_replica_id: ReplicaId::new(Uuid::from_u128(2)),
            policy_hash: ContentHash::from_bytes([3u8; 32]),
            roster_hash: None,
            included: IncludedWatermarks::new(),
            included_heads: None,
            content_hash: ContentHash::from_bytes([0u8; 32]),
            manifest_hash,
        };
        (manifest, meta)
    }

    #[test]
    fn import_rejects_manifest_hash_mismatch() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let (manifest, mut meta) = minimal_manifest_and_meta(dir);
        meta.manifest_hash = ContentHash::from_bytes([9u8; 32]);
        meta.content_hash = meta.compute_content_hash().unwrap();

        write_file(
            &dir.join(META_FILE),
            serde_json::to_vec(&meta).unwrap().as_slice(),
        );
        write_file(
            &dir.join(MANIFEST_FILE),
            serde_json::to_vec(&manifest).unwrap().as_slice(),
        );

        let err = import_checkpoint(dir, &Limits::default()).unwrap_err();
        assert!(matches!(err, CheckpointImportError::ManifestHashMismatch { .. }));
    }

    #[test]
    fn import_rejects_oversize_line() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let (mut manifest, mut meta) = minimal_manifest_and_meta(dir);
        let shard_path = "namespaces/core/state/00.jsonl";
        let line = br#"{"id":"bd-abc","created_at":[1,0],"created_by":"me","title":"t","description":"d","priority":"p2","type":"task","labels":[],"status":"open","_at":[1,0],"_by":"me"}"#;
        write_file(&dir.join(shard_path), line);

        let file_bytes = line.len() as u64;
        manifest.files.insert(
            shard_path.to_string(),
            super::manifest::ManifestFile {
                sha256: ContentHash::from_bytes(sha256_bytes(line).0),
                bytes: file_bytes,
            },
        );
        meta.manifest_hash = manifest.manifest_hash().unwrap();
        meta.content_hash = meta.compute_content_hash().unwrap();

        write_file(
            &dir.join(META_FILE),
            serde_json::to_vec(&meta).unwrap().as_slice(),
        );
        write_file(
            &dir.join(MANIFEST_FILE),
            serde_json::to_vec(&manifest).unwrap().as_slice(),
        );

        let limits = Limits {
            max_jsonl_line_bytes: 8,
            ..Limits::default()
        };
        let err = import_checkpoint(dir, &limits).unwrap_err();
        assert!(matches!(err, CheckpointImportError::LineTooLong { .. }));
    }

    #[test]
    fn merge_store_states_is_commutative() {
        let mut left = StoreState::new();
        let mut right = StoreState::new();
        let mut state = CanonicalState::default();
        let bead = crate::core::Bead::from(WireBeadFull {
            id: crate::core::BeadId::parse("bd-abc").unwrap(),
            created_at: WireStamp(1, 0),
            created_by: crate::core::ActorId::new("me").unwrap(),
            created_on_branch: None,
            title: "t".to_string(),
            description: "d".to_string(),
            design: None,
            acceptance_criteria: None,
            priority: crate::core::Priority::P2,
            bead_type: crate::core::BeadType::Task,
            labels: crate::core::Labels::new(),
            external_ref: None,
            source_repo: None,
            estimated_minutes: None,
            status: crate::core::WorkflowStatus::Open,
            closed_at: None,
            closed_by: None,
            closed_reason: None,
            closed_on_branch: None,
            assignee: None,
            assignee_at: None,
            assignee_expires: None,
            notes: Vec::new(),
            at: WireStamp(1, 0),
            by: crate::core::ActorId::new("me").unwrap(),
            v: None,
        });
        state.insert_live(bead);
        left.ensure_namespace(NamespaceId::core())
            .clone_from(&state);
        right
            .ensure_namespace(NamespaceId::core())
            .clone_from(&state);

        let merged_a = merge_store_states(&left, &right).unwrap();
        let merged_b = merge_store_states(&right, &left).unwrap();
        assert_eq!(
            merged_a.get(&NamespaceId::core()),
            merged_b.get(&NamespaceId::core())
        );
    }
}
