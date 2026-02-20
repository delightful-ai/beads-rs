//! Checkpoint import + verification.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde_json::Value;
use sha2::{Digest, Sha256 as Sha2};
use thiserror::Error;

use super::export::CheckpointExport;
use super::json_canon::CanonJsonError;
use super::layout::{CheckpointFileKind, CheckpointShardPath, MANIFEST_FILE, META_FILE};
use super::types::CheckpointShardPayload;
use super::{
    CheckpointFormatVersion, CheckpointManifest, CheckpointMeta, IncludedHeads, IncludedWatermarks,
    ParsedCheckpointManifest, SupportedCheckpointMeta,
};
use crate::core::error::CoreError;
use crate::core::limits::LimitViolation;
use crate::core::state::LabelState;
use crate::core::wire_bead::{WireDepStoreV1, WireLabelStateV1, WireStamp, WireTombstoneV1};
use crate::core::{
    BeadId, BeadSnapshotWireV1, CanonicalState, CheckpointContentSha256, ContentHash, DepKey,
    DepStore, Dot, LabelStore, Limits, NamespaceId, NamespaceSet, OrSet, SnapshotCodec,
    SnapshotSection, Stamp, StoreState, Tombstone, TombstoneKey, WriteStamp, sha256_bytes,
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
        expected: CheckpointContentSha256,
        got: CheckpointContentSha256,
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
        meta: NamespaceSet,
        manifest: NamespaceSet,
    },
    #[error("checkpoint namespaces not normalized for {which}: {namespaces:?}")]
    NamespacesNotNormalized {
        which: &'static str,
        namespaces: NamespaceSet,
    },
    #[error("checkpoint format version unsupported: {got}")]
    UnsupportedFormatVersion { got: u32 },
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
    #[error(
        "checkpoint json depth exceeded at {path:?} line {line}: max {max_depth}, got {got_depth}"
    )]
    JsonDepthExceeded {
        path: PathBuf,
        line: u64,
        max_depth: usize,
        got_depth: usize,
    },
    #[error("checkpoint jsonl parse error at {path:?} line {line}: {source}")]
    JsonLine {
        path: PathBuf,
        line: u64,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "checkpoint jsonl shard entry limit exceeded at {path:?}: max {max_entries}, got {got_entries}"
    )]
    ShardEntryLimit {
        path: PathBuf,
        max_entries: usize,
        got_entries: usize,
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
    #[error("checkpoint entries out of order at {path:?} line {line}: {reason}")]
    OutOfOrder {
        path: PathBuf,
        line: u64,
        reason: String,
    },
    #[error("checkpoint merge failed: {0:?}")]
    Merge(Vec<CoreError>),
}

#[derive(Debug, Clone)]
pub struct CheckpointImport {
    pub checkpoint_group: String,
    pub policy_hash: ContentHash,
    pub roster_hash: Option<ContentHash>,
    pub state: StoreState,
    pub included: IncludedWatermarks,
    pub included_heads: Option<IncludedHeads>,
}

fn state_for_namespace<'a>(
    state: &'a mut StoreState,
    namespace: &NamespaceId,
) -> &'a mut CanonicalState {
    state.ensure_namespace(namespace.clone())
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

    let (meta, manifest) = validate_meta_and_manifest(meta, manifest)?;

    let mut state = StoreState::new();
    let mut label_stores: BTreeMap<NamespaceId, LabelStore> = BTreeMap::new();
    let mut dep_stores: BTreeMap<NamespaceId, DepStore> = BTreeMap::new();
    let allowed_namespaces: BTreeSet<NamespaceId> =
        manifest.manifest().namespaces.iter().cloned().collect();

    for (rel_path, entry) in &manifest.manifest().files {
        let full_path = dir.join(rel_path.to_path());
        if !full_path.exists() {
            return Err(CheckpointImportError::MissingFile {
                path: full_path.clone(),
            });
        }

        if !allowed_namespaces.contains(&rel_path.namespace) {
            return Err(CheckpointImportError::UnexpectedFile {
                path: rel_path.to_path(),
            });
        }

        limits
            .policy()
            .jsonl_shard_bytes(entry.bytes)
            .map_err(|err| map_jsonl_shard_violation(err, &full_path, entry.bytes))?;

        let mut prev_bead: Option<BeadId> = None;
        let mut prev_tombstone: Option<TombstoneKey> = None;
        let stats = match rel_path.kind {
            CheckpointFileKind::State => parse_jsonl_file::<BeadSnapshotWireV1, _>(
                &full_path,
                &rel_path.namespace,
                limits,
                |line, wire| {
                    ensure_strictly_increasing(
                        &mut prev_bead,
                        wire.id.clone(),
                        &full_path,
                        line.line_no,
                        SnapshotSection::Beads,
                    )?;
                    SnapshotCodec::validate_bead_notes(&wire.id, &wire.notes).map_err(|err| {
                        CheckpointImportError::OutOfOrder {
                            path: full_path.clone(),
                            line: line.line_no,
                            reason: err.to_string(),
                        }
                    })?;
                    let ns = line.namespace.clone();
                    let bead_id = wire.id.clone();
                    let label_stamp = wire.label_stamp();
                    let label_state = label_state_from_wire(
                        wire.labels.clone(),
                        label_stamp,
                        &full_path,
                        line,
                        &bead_id,
                    );
                    let lineage =
                        Stamp::new(WriteStamp::from(wire.created_at), wire.created_by.clone());
                    label_stores.entry(ns.clone()).or_default().insert_state(
                        bead_id.clone(),
                        lineage.clone(),
                        label_state,
                    );

                    let notes = wire.notes.clone();
                    let bead = crate::core::Bead::from(wire);
                    let state = state_for_namespace(&mut state, &ns);
                    state.insert_live(bead);

                    for note in notes {
                        let note = crate::core::Note::from(note);
                        state.insert_note(bead_id.clone(), lineage.clone(), note);
                    }

                    Ok(())
                },
            )?,
            CheckpointFileKind::Tombstones => parse_jsonl_file::<WireTombstoneV1, _>(
                &full_path,
                &rel_path.namespace,
                limits,
                |line, wire| {
                    let key = tombstone_key_from_wire(&wire, &full_path, line.line_no)?;
                    ensure_strictly_increasing(
                        &mut prev_tombstone,
                        key,
                        &full_path,
                        line.line_no,
                        SnapshotSection::Tombstones,
                    )?;
                    let ns = line.namespace.clone();
                    let tomb = tombstone_from_wire(&wire, &full_path, line)?;
                    state_for_namespace(&mut state, &ns).insert_tombstone(tomb);
                    Ok(())
                },
            )?,
            CheckpointFileKind::Deps => parse_jsonl_file::<WireDepStoreV1, _>(
                &full_path,
                &rel_path.namespace,
                limits,
                |line, wire| {
                    ensure_dep_entry_order(&wire, &full_path, line.line_no)?;
                    let ns = line.namespace.clone();
                    let dep_store = dep_store_from_wire(&wire, &full_path, line)?;
                    let entry = dep_stores.entry(ns.clone()).or_default();
                    *entry = DepStore::join(entry, &dep_store);
                    Ok(())
                },
            )?,
        };

        verify_stats(&stats, entry, &full_path)?;
    }

    for (ns, labels) in label_stores {
        state_for_namespace(&mut state, &ns).set_label_store(labels);
    }
    for (ns, deps) in dep_stores {
        state_for_namespace(&mut state, &ns).set_dep_store(deps);
    }

    let meta = meta.meta();
    Ok(CheckpointImport {
        checkpoint_group: meta.checkpoint_group.clone(),
        policy_hash: meta.policy_hash,
        roster_hash: meta.roster_hash,
        state,
        included: meta.included.clone(),
        included_heads: meta.included_heads.clone(),
    })
}

/// Import a checkpoint from an in-memory export (as produced by `export_checkpoint` or read from git).
///
/// This is the same verification + parsing as `import_checkpoint(dir, limits)`, but without filesystem I/O.
pub fn import_checkpoint_export(
    export: &CheckpointExport,
    limits: &Limits,
) -> Result<CheckpointImport, CheckpointImportError> {
    let parsed = parse_checkpoint_export(export)?;
    import_checkpoint_export_parsed(&parsed, limits)
}

fn import_checkpoint_export_parsed(
    export: &ParsedCheckpointExport,
    limits: &Limits,
) -> Result<CheckpointImport, CheckpointImportError> {
    let meta = export.meta.meta();
    let manifest = export.manifest.manifest();

    let mut state = StoreState::new();
    let mut label_stores: BTreeMap<NamespaceId, LabelStore> = BTreeMap::new();
    let mut dep_stores: BTreeMap<NamespaceId, DepStore> = BTreeMap::new();
    let allowed_namespaces: BTreeSet<NamespaceId> = manifest.namespaces.iter().cloned().collect();

    for (rel_path, entry) in &manifest.files {
        if !allowed_namespaces.contains(&rel_path.namespace) {
            return Err(CheckpointImportError::UnexpectedFile {
                path: rel_path.to_path(),
            });
        }

        let payload =
            export
                .files
                .get(rel_path)
                .ok_or_else(|| CheckpointImportError::MissingFile {
                    path: PathBuf::from(rel_path.to_path()),
                })?;
        let bytes = payload.bytes.as_ref();

        let got_bytes = bytes.len() as u64;
        if got_bytes != entry.bytes {
            return Err(CheckpointImportError::FileSizeMismatch {
                path: PathBuf::from(rel_path.to_path()),
                expected: entry.bytes,
                got: got_bytes,
            });
        }
        let got_hash = ContentHash::from_bytes(sha256_bytes(bytes).0);
        if got_hash != entry.sha256 {
            return Err(CheckpointImportError::FileHashMismatch {
                path: PathBuf::from(rel_path.to_path()),
                expected: entry.sha256,
                got: got_hash,
            });
        }

        let path = PathBuf::from(rel_path.to_path());
        limits
            .policy()
            .jsonl_shard_bytes(entry.bytes)
            .map_err(|err| map_jsonl_shard_violation(err, &path, entry.bytes))?;
        let mut prev_bead: Option<BeadId> = None;
        let mut prev_tombstone: Option<TombstoneKey> = None;
        match rel_path.kind {
            CheckpointFileKind::State => parse_jsonl_bytes::<BeadSnapshotWireV1, _>(
                bytes,
                &path,
                &rel_path.namespace,
                limits,
                |line, wire| {
                    ensure_strictly_increasing(
                        &mut prev_bead,
                        wire.id.clone(),
                        &path,
                        line.line_no,
                        SnapshotSection::Beads,
                    )?;
                    SnapshotCodec::validate_bead_notes(&wire.id, &wire.notes).map_err(|err| {
                        CheckpointImportError::OutOfOrder {
                            path: path.clone(),
                            line: line.line_no,
                            reason: err.to_string(),
                        }
                    })?;
                    let ns = line.namespace.clone();
                    let bead_id = wire.id.clone();
                    let label_stamp = wire.label_stamp();
                    let label_state = label_state_from_wire(
                        wire.labels.clone(),
                        label_stamp,
                        &path,
                        line,
                        &bead_id,
                    );
                    let lineage =
                        Stamp::new(WriteStamp::from(wire.created_at), wire.created_by.clone());
                    label_stores.entry(ns.clone()).or_default().insert_state(
                        bead_id.clone(),
                        lineage.clone(),
                        label_state,
                    );

                    let notes = wire.notes.clone();
                    let bead = crate::core::Bead::from(wire);
                    let state = state_for_namespace(&mut state, &ns);
                    state.insert_live(bead);

                    for note in notes {
                        let note = crate::core::Note::from(note);
                        state.insert_note(bead_id.clone(), lineage.clone(), note);
                    }

                    Ok(())
                },
            )?,
            CheckpointFileKind::Tombstones => parse_jsonl_bytes::<WireTombstoneV1, _>(
                bytes,
                &path,
                &rel_path.namespace,
                limits,
                |line, wire| {
                    let key = tombstone_key_from_wire(&wire, &path, line.line_no)?;
                    ensure_strictly_increasing(
                        &mut prev_tombstone,
                        key,
                        &path,
                        line.line_no,
                        SnapshotSection::Tombstones,
                    )?;
                    let ns = line.namespace.clone();
                    let tomb = tombstone_from_wire(&wire, &path, line)?;
                    state_for_namespace(&mut state, &ns).insert_tombstone(tomb);
                    Ok(())
                },
            )?,
            CheckpointFileKind::Deps => parse_jsonl_bytes::<WireDepStoreV1, _>(
                bytes,
                &path,
                &rel_path.namespace,
                limits,
                |line, wire| {
                    ensure_dep_entry_order(&wire, &path, line.line_no)?;
                    let ns = line.namespace.clone();
                    let dep_store = dep_store_from_wire(&wire, &path, line)?;
                    let entry = dep_stores.entry(ns.clone()).or_default();
                    *entry = DepStore::join(entry, &dep_store);
                    Ok(())
                },
            )?,
        };
    }

    for (ns, labels) in label_stores {
        state_for_namespace(&mut state, &ns).set_label_store(labels);
    }
    for (ns, deps) in dep_stores {
        state_for_namespace(&mut state, &ns).set_dep_store(deps);
    }

    Ok(CheckpointImport {
        checkpoint_group: meta.checkpoint_group.clone(),
        policy_hash: meta.policy_hash,
        roster_hash: meta.roster_hash,
        state,
        included: meta.included.clone(),
        included_heads: meta.included_heads.clone(),
    })
}

#[derive(Clone, Debug)]
pub struct ParsedCheckpointExport {
    pub meta: SupportedCheckpointMeta,
    pub manifest: ParsedCheckpointManifest,
    pub files: BTreeMap<CheckpointShardPath, CheckpointShardPayload>,
}

pub fn parse_checkpoint_export(
    export: &CheckpointExport,
) -> Result<ParsedCheckpointExport, CheckpointImportError> {
    let (meta, manifest) =
        validate_meta_and_manifest(export.meta.clone(), export.manifest.clone())?;
    Ok(ParsedCheckpointExport {
        meta,
        manifest,
        files: export.files.clone(),
    })
}

fn validate_meta_and_manifest(
    meta: CheckpointMeta,
    manifest: CheckpointManifest,
) -> Result<(SupportedCheckpointMeta, ParsedCheckpointManifest), CheckpointImportError> {
    let version = CheckpointFormatVersion::parse(meta.checkpoint_format_version).ok_or(
        CheckpointImportError::UnsupportedFormatVersion {
            got: meta.checkpoint_format_version,
        },
    )?;

    let meta_namespaces = meta.namespaces_normalized();
    if meta_namespaces != meta.namespaces {
        return Err(CheckpointImportError::NamespacesNotNormalized {
            which: "meta",
            namespaces: meta.namespaces.clone(),
        });
    }

    let manifest_namespaces = manifest.namespaces_normalized();
    if manifest_namespaces != manifest.namespaces {
        return Err(CheckpointImportError::NamespacesNotNormalized {
            which: "manifest",
            namespaces: manifest.namespaces.clone(),
        });
    }

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
    if meta.namespaces != manifest.namespaces {
        return Err(CheckpointImportError::NamespacesMismatch {
            meta: meta.namespaces.clone(),
            manifest: manifest.namespaces.clone(),
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

    Ok((
        SupportedCheckpointMeta::new(meta, version),
        ParsedCheckpointManifest::new(manifest),
    ))
}

pub fn merge_store_states(
    a: &StoreState,
    b: &StoreState,
) -> Result<StoreState, CheckpointImportError> {
    let mut merged = StoreState::new();

    let mut namespaces: BTreeSet<NamespaceId> = BTreeSet::new();
    namespaces.extend(a.namespaces().map(|(ns, _)| ns));
    namespaces.extend(b.namespaces().map(|(ns, _)| ns));

    for namespace in namespaces {
        let left = a.get(&namespace);
        let right = b.get(&namespace);
        let out = match (left, right) {
            (Some(a_state), Some(b_state)) => CanonicalState::join(a_state, b_state),
            (Some(state), None) | (None, Some(state)) => state.clone(),
            (None, None) => CanonicalState::default(),
        };
        state_for_namespace(&mut merged, &namespace).clone_from(&out);
    }

    Ok(merged)
}

/// Lift a legacy, non-namespaced state into the core namespace.
pub fn store_state_from_legacy(state: CanonicalState) -> StoreState {
    let mut store = StoreState::new();
    store.set_core_state(state);
    store
}

struct JsonlStats {
    sha256: ContentHash,
    bytes: u64,
}

struct JsonlLineContext {
    line_no: u64,
    namespace: NamespaceId,
}

fn parse_jsonl_bytes<T, F>(
    bytes: &[u8],
    path: &Path,
    namespace: &NamespaceId,
    limits: &Limits,
    mut on_item: F,
) -> Result<(), CheckpointImportError>
where
    T: DeserializeOwned,
    F: FnMut(JsonlLineContext, T) -> Result<(), CheckpointImportError>,
{
    let total_bytes = bytes.len() as u64;
    limits
        .policy()
        .jsonl_shard_bytes(total_bytes)
        .map_err(|err| map_jsonl_shard_violation(err, path, total_bytes))?;

    let mut line_no = 0u64;
    let mut entries = 0usize;

    for chunk in bytes.split_inclusive(|b| *b == b'\n') {
        let line_bytes = chunk.len();
        limits
            .policy()
            .jsonl_line_bytes(line_bytes)
            .map_err(|err| map_jsonl_line_violation(err, path, line_no + 1, line_bytes))?;

        let line = if chunk.ends_with(b"\n") {
            &chunk[..chunk.len() - 1]
        } else {
            chunk
        };

        line_no += 1;
        let value = parse_json_line::<T>(line, limits, path, line_no)?;
        entries += 1;
        limits
            .policy()
            .snapshot_entries(entries)
            .map_err(|err| map_snapshot_entries_violation(err, path, entries))?;

        let ctx = JsonlLineContext {
            line_no,
            namespace: namespace.clone(),
        };
        on_item(ctx, value)?;
    }

    Ok(())
}

fn parse_jsonl_file<T, F>(
    path: &Path,
    namespace: &NamespaceId,
    limits: &Limits,
    mut on_item: F,
) -> Result<JsonlStats, CheckpointImportError>
where
    T: DeserializeOwned,
    F: FnMut(JsonlLineContext, T) -> Result<(), CheckpointImportError>,
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
    let mut entries = 0usize;

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
        limits
            .policy()
            .jsonl_shard_bytes(total_bytes)
            .map_err(|err| map_jsonl_shard_violation(err, path, total_bytes))?;
        hasher.update(&buf);

        let line_bytes = buf.len();
        limits
            .policy()
            .jsonl_line_bytes(line_bytes)
            .map_err(|err| map_jsonl_line_violation(err, path, line_no + 1, line_bytes))?;

        let line = if buf.ends_with(b"\n") {
            &buf[..buf.len() - 1]
        } else {
            &buf[..]
        };
        line_no += 1;
        let value = parse_json_line::<T>(line, limits, path, line_no)?;
        entries += 1;
        limits
            .policy()
            .snapshot_entries(entries)
            .map_err(|err| map_snapshot_entries_violation(err, path, entries))?;

        let namespace = namespace.clone();
        let ctx = JsonlLineContext { line_no, namespace };
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

fn parse_json_line<T: DeserializeOwned>(
    line: &[u8],
    limits: &Limits,
    path: &Path,
    line_no: u64,
) -> Result<T, CheckpointImportError> {
    let value: Value =
        serde_json::from_slice(line).map_err(|source| CheckpointImportError::JsonLine {
            path: path.to_path_buf(),
            line: line_no,
            source,
        })?;
    let depth = json_depth(&value);
    limits
        .policy()
        .json_depth(depth)
        .map_err(|err| map_json_depth_violation(err, path, line_no, depth))?;
    serde_json::from_value(value).map_err(|source| CheckpointImportError::JsonLine {
        path: path.to_path_buf(),
        line: line_no,
        source,
    })
}

fn json_depth(value: &Value) -> usize {
    match value {
        Value::Array(values) => 1 + values.iter().map(json_depth).max().unwrap_or(0),
        Value::Object(map) => 1 + map.values().map(json_depth).max().unwrap_or(0),
        _ => 1,
    }
}

fn map_jsonl_shard_violation(
    err: LimitViolation,
    path: &Path,
    got_bytes: u64,
) -> CheckpointImportError {
    match err {
        LimitViolation::JsonlShardTooLarge { max_bytes, .. } => {
            CheckpointImportError::ShardTooLarge {
                path: path.to_path_buf(),
                max_bytes,
                got_bytes,
            }
        }
        other => unreachable!("unexpected shard limit violation: {other}"),
    }
}

fn map_jsonl_line_violation(
    err: LimitViolation,
    path: &Path,
    line: u64,
    got_bytes: usize,
) -> CheckpointImportError {
    match err {
        LimitViolation::JsonlLineTooLarge { max_bytes, .. } => CheckpointImportError::LineTooLong {
            path: path.to_path_buf(),
            line,
            max_bytes,
            got_bytes: got_bytes as u64,
        },
        other => unreachable!("unexpected jsonl line limit violation: {other}"),
    }
}

fn map_snapshot_entries_violation(
    err: LimitViolation,
    path: &Path,
    entries: usize,
) -> CheckpointImportError {
    match err {
        LimitViolation::SnapshotEntriesTooMany { max_entries, .. } => {
            CheckpointImportError::ShardEntryLimit {
                path: path.to_path_buf(),
                max_entries,
                got_entries: entries,
            }
        }
        other => unreachable!("unexpected snapshot entry limit violation: {other}"),
    }
}

fn map_json_depth_violation(
    err: LimitViolation,
    path: &Path,
    line: u64,
    depth: usize,
) -> CheckpointImportError {
    match err {
        LimitViolation::JsonDepthExceeded { max_depth, .. } => {
            CheckpointImportError::JsonDepthExceeded {
                path: path.to_path_buf(),
                line,
                max_depth,
                got_depth: depth,
            }
        }
        other => unreachable!("unexpected json depth violation: {other}"),
    }
}

fn ensure_strictly_increasing<T: Ord + std::fmt::Debug>(
    prev: &mut Option<T>,
    next: T,
    path: &Path,
    line: u64,
    section: SnapshotSection,
) -> Result<(), CheckpointImportError> {
    SnapshotCodec::ensure_strictly_increasing(prev, next, section, line as usize).map_err(|err| {
        CheckpointImportError::OutOfOrder {
            path: path.to_path_buf(),
            line,
            reason: err.to_string(),
        }
    })
}

fn tombstone_key_from_wire(
    wire: &WireTombstoneV1,
    _path: &Path,
    _line: u64,
) -> Result<TombstoneKey, CheckpointImportError> {
    let lineage = wire.lineage_stamp();
    Ok(match lineage {
        Some(stamp) => TombstoneKey::lineage(wire.id.clone(), stamp),
        None => TombstoneKey::global(wire.id.clone()),
    })
}

fn ensure_dep_entry_order(
    wire: &WireDepStoreV1,
    path: &Path,
    line: u64,
) -> Result<(), CheckpointImportError> {
    let mut prev: Option<DepKey> = None;
    for entry in &wire.entries {
        ensure_strictly_increasing(
            &mut prev,
            entry.key.clone(),
            path,
            line,
            SnapshotSection::Deps,
        )?;
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
    _path: &Path,
    _line: JsonlLineContext,
) -> Result<Tombstone, CheckpointImportError> {
    let deleted = StampFromWire::stamp(wire.deleted_at, &wire.deleted_by);
    let lineage = wire
        .lineage
        .as_ref()
        .map(|lineage| StampFromWire::stamp(lineage.at, &lineage.by));

    Ok(match lineage {
        Some(stamp) => {
            Tombstone::new_collision(wire.id.clone(), deleted, stamp, wire.reason.clone())
        }
        None => Tombstone::new(wire.id.clone(), deleted, wire.reason.clone()),
    })
}

fn label_state_from_wire(
    wire: WireLabelStateV1,
    stamp: Stamp,
    path: &Path,
    line: JsonlLineContext,
    bead_id: &BeadId,
) -> LabelState {
    let (set, normalization) = OrSet::normalize_for_import(wire.entries, wire.cc);
    if normalization.changed() {
        tracing::debug!(
            path = %path.display(),
            line = line.line_no,
            namespace = %line.namespace,
            bead_id = %bead_id.as_str(),
            normalized_cc = normalization.normalized_cc,
            pruned_dots = normalization.pruned_dots,
            removed_empty_entries = normalization.removed_empty_entries,
            resolved_collisions = normalization.resolved_collisions,
            "normalized label OR-Set during checkpoint import"
        );
    }
    LabelState::from_parts(set, Some(stamp))
}

fn dep_store_from_wire(
    wire: &WireDepStoreV1,
    path: &Path,
    line: JsonlLineContext,
) -> Result<DepStore, CheckpointImportError> {
    let mut entries: BTreeMap<DepKey, BTreeSet<Dot>> = BTreeMap::new();
    for entry in &wire.entries {
        let dots: BTreeSet<Dot> = entry.dots.iter().copied().collect();
        if entries.insert(entry.key.clone(), dots).is_some() {
            return Err(CheckpointImportError::InvalidDep {
                path: path.to_path_buf(),
                line: line.line_no,
                reason: "duplicate dep key in shard".into(),
            });
        }
    }
    let (set, normalization) = OrSet::normalize_for_import(entries, wire.cc.clone());
    if normalization.changed() {
        tracing::debug!(
            path = %path.display(),
            line = line.line_no,
            namespace = %line.namespace,
            normalized_cc = normalization.normalized_cc,
            pruned_dots = normalization.pruned_dots,
            removed_empty_entries = normalization.removed_empty_entries,
            resolved_collisions = normalization.resolved_collisions,
            "normalized dep OR-Set during checkpoint import"
        );
    }
    let stamp = wire
        .stamp
        .as_ref()
        .map(|(at, by)| StampFromWire::stamp(*at, by));
    Ok(DepStore::from_parts(set, stamp))
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

    use crate::core::wire_bead::{WireClaimSnapshot, WireWorkflowSnapshot};
    use crate::core::{
        ActorId, BeadId, BeadType, CanonicalState, CheckpointContentSha256, Dvv, NamespaceId,
        Priority, ReplicaId, StoreEpoch, StoreId,
    };
    use crate::git::checkpoint::{
        CheckpointFileKind, CheckpointShardPath, ManifestFile, shard_name,
    };
    use crate::git::wire::{serialize_deps, serialize_state, serialize_tombstones};

    fn write_file(path: &Path, bytes: &[u8]) {
        std::fs::create_dir_all(path.parent().expect("parent")).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    fn minimal_manifest_and_meta(_dir: &Path) -> (CheckpointManifest, CheckpointMeta) {
        let store_id = StoreId::new(Uuid::from_u128(1));
        let store_epoch = StoreEpoch::new(0);
        let manifest = CheckpointManifest {
            checkpoint_group: "core".to_string(),
            store_id,
            store_epoch,
            namespaces: vec![NamespaceId::core()].into(),
            files: Default::default(),
        };
        let manifest_hash = manifest.manifest_hash().unwrap();
        let meta = CheckpointMeta {
            checkpoint_format_version: 1,
            store_id,
            store_epoch,
            checkpoint_group: "core".to_string(),
            namespaces: vec![NamespaceId::core()].into(),
            created_at_ms: 1,
            created_by_replica_id: ReplicaId::new(Uuid::from_u128(2)),
            policy_hash: ContentHash::from_bytes([3u8; 32]),
            roster_hash: None,
            included: IncludedWatermarks::new(),
            included_heads: None,
            content_hash: CheckpointContentSha256::from_checkpoint_preimage_bytes(&[0u8; 32]),
            manifest_hash,
        };
        (manifest, meta)
    }

    fn sample_wire_bead_full(id: &str) -> BeadSnapshotWireV1 {
        BeadSnapshotWireV1 {
            id: BeadId::parse(id).unwrap(),
            created_at: WireStamp(1, 0),
            created_by: ActorId::new("me").unwrap(),
            created_on_branch: None,
            title: "t".to_string(),
            description: "d".to_string(),
            design: None,
            acceptance_criteria: None,
            priority: Priority::MEDIUM,
            bead_type: BeadType::Task,
            labels: WireLabelStateV1 {
                entries: Default::default(),
                cc: Dvv::default(),
            },
            external_ref: None,
            source_repo: None,
            estimated_minutes: None,
            workflow: WireWorkflowSnapshot::Open,
            claim: WireClaimSnapshot::unclaimed(),
            notes: Vec::new(),
            at: WireStamp(1, 0),
            by: ActorId::new("me").unwrap(),
            v: None,
        }
    }

    fn state_fingerprint(state: &CanonicalState) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let state_bytes = serialize_state(state).expect("serialize state");
        let tomb_bytes = serialize_tombstones(state).expect("serialize tombstones");
        let deps_bytes = serialize_deps(state).expect("serialize deps");
        (state_bytes, tomb_bytes, deps_bytes)
    }

    #[test]
    fn import_rejects_unsupported_version() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let (manifest, mut meta) = minimal_manifest_and_meta(dir);
        meta.checkpoint_format_version = 99;
        meta.manifest_hash = manifest.manifest_hash().unwrap();
        meta.content_hash = meta.compute_content_hash().unwrap();

        write_file(&dir.join(META_FILE), meta.canon_bytes().unwrap().as_slice());
        write_file(
            &dir.join(MANIFEST_FILE),
            manifest.canon_bytes().unwrap().as_slice(),
        );

        let err = import_checkpoint(dir, &Limits::default()).unwrap_err();
        assert!(matches!(
            err,
            CheckpointImportError::UnsupportedFormatVersion { .. }
        ));
    }

    #[test]
    fn parse_export_rejects_unsupported_version() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let (manifest, mut meta) = minimal_manifest_and_meta(dir);
        meta.checkpoint_format_version = 99;
        meta.manifest_hash = manifest.manifest_hash().unwrap();
        meta.content_hash = meta.compute_content_hash().unwrap();

        let export = CheckpointExport {
            manifest,
            meta,
            files: BTreeMap::new(),
        };

        let err = parse_checkpoint_export(&export).unwrap_err();
        assert!(matches!(
            err,
            CheckpointImportError::UnsupportedFormatVersion { .. }
        ));
    }

    #[test]
    fn parse_export_rejects_namespace_mismatch() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let (mut manifest, mut meta) = minimal_manifest_and_meta(dir);
        let extra = NamespaceId::parse("extra").unwrap();
        manifest.namespaces = vec![NamespaceId::core(), extra].into();
        meta.manifest_hash = manifest.manifest_hash().unwrap();
        meta.content_hash = meta.compute_content_hash().unwrap();

        let export = CheckpointExport {
            manifest,
            meta,
            files: BTreeMap::new(),
        };

        let err = parse_checkpoint_export(&export).unwrap_err();
        assert!(matches!(
            err,
            CheckpointImportError::NamespacesMismatch { .. }
        ));
    }

    #[test]
    fn import_export_accepts_v1() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let (manifest, mut meta) = minimal_manifest_and_meta(dir);
        meta.manifest_hash = manifest.manifest_hash().unwrap();
        meta.content_hash = meta.compute_content_hash().unwrap();

        let export = CheckpointExport {
            manifest,
            meta,
            files: BTreeMap::new(),
        };

        let _parsed = parse_checkpoint_export(&export).unwrap();
        let imported = import_checkpoint_export(&export, &Limits::default()).unwrap();
        assert_eq!(imported.checkpoint_group, "core");
    }

    #[test]
    fn import_export_rejects_store_id_mismatch() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let (manifest, mut meta) = minimal_manifest_and_meta(dir);
        meta.store_id = StoreId::new(Uuid::from_bytes([9u8; 16]));
        meta.manifest_hash = manifest.manifest_hash().unwrap();
        meta.content_hash = meta.compute_content_hash().unwrap();

        let export = CheckpointExport {
            manifest,
            meta,
            files: BTreeMap::new(),
        };

        let err = import_checkpoint_export(&export, &Limits::default()).unwrap_err();
        assert!(matches!(err, CheckpointImportError::StoreIdMismatch { .. }));
    }

    #[test]
    fn import_rejects_manifest_hash_mismatch() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let (manifest, mut meta) = minimal_manifest_and_meta(dir);
        meta.manifest_hash = ContentHash::from_bytes([9u8; 32]);
        meta.content_hash = meta.compute_content_hash().unwrap();

        write_file(&dir.join(META_FILE), meta.canon_bytes().unwrap().as_slice());
        write_file(
            &dir.join(MANIFEST_FILE),
            manifest.canon_bytes().unwrap().as_slice(),
        );

        let err = import_checkpoint(dir, &Limits::default()).unwrap_err();
        assert!(matches!(
            err,
            CheckpointImportError::ManifestHashMismatch { .. }
        ));
    }

    #[test]
    fn import_rejects_oversize_line() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let (mut manifest, mut meta) = minimal_manifest_and_meta(dir);
        let shard_path = CheckpointShardPath::new(
            NamespaceId::core(),
            CheckpointFileKind::State,
            shard_name(0),
        );
        let shard_rel = shard_path.to_path();
        let line = serde_json::to_vec(&sample_wire_bead_full("bd-abc")).unwrap();
        write_file(&dir.join(&shard_rel), &line);

        let file_bytes = line.len() as u64;
        manifest.files.insert(
            shard_path,
            ManifestFile {
                sha256: ContentHash::from_bytes(sha256_bytes(&line).0),
                bytes: file_bytes,
            },
        );
        meta.manifest_hash = manifest.manifest_hash().unwrap();
        meta.content_hash = meta.compute_content_hash().unwrap();

        write_file(&dir.join(META_FILE), meta.canon_bytes().unwrap().as_slice());
        write_file(
            &dir.join(MANIFEST_FILE),
            manifest.canon_bytes().unwrap().as_slice(),
        );

        let limits = Limits {
            max_jsonl_line_bytes: 8,
            ..Limits::default()
        };
        let err = import_checkpoint(dir, &limits).unwrap_err();
        assert!(matches!(err, CheckpointImportError::LineTooLong { .. }));
    }

    #[test]
    fn import_rejects_json_depth_exceeded() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let (mut manifest, mut meta) = minimal_manifest_and_meta(dir);
        let shard_path = CheckpointShardPath::new(
            NamespaceId::core(),
            CheckpointFileKind::State,
            shard_name(0),
        );
        let shard_rel = shard_path.to_path();
        let line = b"[[[]]]\n";
        write_file(&dir.join(&shard_rel), line);

        manifest.files.insert(
            shard_path,
            ManifestFile {
                sha256: ContentHash::from_bytes(sha256_bytes(line).0),
                bytes: line.len() as u64,
            },
        );
        meta.manifest_hash = manifest.manifest_hash().unwrap();
        meta.content_hash = meta.compute_content_hash().unwrap();

        write_file(&dir.join(META_FILE), meta.canon_bytes().unwrap().as_slice());
        write_file(
            &dir.join(MANIFEST_FILE),
            manifest.canon_bytes().unwrap().as_slice(),
        );

        let limits = Limits {
            max_cbor_depth: 2,
            ..Limits::default()
        };
        let err = import_checkpoint(dir, &limits).unwrap_err();
        assert!(matches!(
            err,
            CheckpointImportError::JsonDepthExceeded { .. }
        ));
    }

    #[test]
    fn import_rejects_shard_entry_limit() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let (mut manifest, mut meta) = minimal_manifest_and_meta(dir);
        let shard_path = CheckpointShardPath::new(
            NamespaceId::core(),
            CheckpointFileKind::State,
            shard_name(0),
        );
        let shard_rel = shard_path.to_path();
        let line1 = serde_json::to_vec(&sample_wire_bead_full("bd-abc")).unwrap();
        let line2 = serde_json::to_vec(&sample_wire_bead_full("bd-def")).unwrap();
        let mut shard_bytes = Vec::new();
        shard_bytes.extend_from_slice(&line1);
        shard_bytes.push(b'\n');
        shard_bytes.extend_from_slice(&line2);
        shard_bytes.push(b'\n');
        write_file(&dir.join(&shard_rel), &shard_bytes);

        manifest.files.insert(
            shard_path,
            ManifestFile {
                sha256: ContentHash::from_bytes(sha256_bytes(&shard_bytes).0),
                bytes: shard_bytes.len() as u64,
            },
        );
        meta.manifest_hash = manifest.manifest_hash().unwrap();
        meta.content_hash = meta.compute_content_hash().unwrap();

        write_file(&dir.join(META_FILE), meta.canon_bytes().unwrap().as_slice());
        write_file(
            &dir.join(MANIFEST_FILE),
            manifest.canon_bytes().unwrap().as_slice(),
        );

        let limits = Limits {
            max_snapshot_entries: 1,
            ..Limits::default()
        };
        let err = import_checkpoint(dir, &limits).unwrap_err();
        assert!(matches!(err, CheckpointImportError::ShardEntryLimit { .. }));
    }

    #[test]
    fn import_rejects_out_of_order_state_entries() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let (mut manifest, mut meta) = minimal_manifest_and_meta(dir);
        let shard_path = CheckpointShardPath::new(
            NamespaceId::core(),
            CheckpointFileKind::State,
            shard_name(0),
        );
        let shard_rel = shard_path.to_path();
        let line1 = serde_json::to_vec(&sample_wire_bead_full("bd-zzz")).unwrap();
        let line2 = serde_json::to_vec(&sample_wire_bead_full("bd-aaa")).unwrap();
        let mut shard_bytes = Vec::new();
        shard_bytes.extend_from_slice(&line1);
        shard_bytes.push(b'\n');
        shard_bytes.extend_from_slice(&line2);
        shard_bytes.push(b'\n');
        write_file(&dir.join(&shard_rel), &shard_bytes);

        manifest.files.insert(
            shard_path,
            ManifestFile {
                sha256: ContentHash::from_bytes(sha256_bytes(&shard_bytes).0),
                bytes: shard_bytes.len() as u64,
            },
        );
        meta.manifest_hash = manifest.manifest_hash().unwrap();
        meta.content_hash = meta.compute_content_hash().unwrap();

        write_file(&dir.join(META_FILE), meta.canon_bytes().unwrap().as_slice());
        write_file(
            &dir.join(MANIFEST_FILE),
            manifest.canon_bytes().unwrap().as_slice(),
        );

        let err = import_checkpoint(dir, &Limits::default()).unwrap_err();
        assert!(matches!(err, CheckpointImportError::OutOfOrder { .. }));
    }

    #[test]
    fn merge_store_states_is_commutative() {
        let mut left = StoreState::new();
        let mut right = StoreState::new();
        let mut state = CanonicalState::default();
        let bead = crate::core::Bead::from(sample_wire_bead_full("bd-abc"));
        state.insert_live(bead);
        left.core_mut().clone_from(&state);
        right.core_mut().clone_from(&state);

        let merged_a = merge_store_states(&left, &right).unwrap();
        let merged_b = merge_store_states(&right, &left).unwrap();
        let merged_a_state = merged_a.core();
        let merged_b_state = merged_b.core();
        assert_eq!(
            state_fingerprint(merged_a_state),
            state_fingerprint(merged_b_state)
        );
    }

    #[test]
    fn store_state_from_legacy_places_state_in_core_namespace() {
        let mut state = CanonicalState::default();
        let bead = crate::core::Bead::from(sample_wire_bead_full("bd-legacy"));
        state.insert_live(bead);
        let expected = state_fingerprint(&state);

        let store = store_state_from_legacy(state);
        let core_state = store.get(&NamespaceId::core()).expect("core state");
        assert_eq!(state_fingerprint(core_state), expected);
    }
}
