//! Git publish helpers for checkpoints.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::Path;

use git2::{ErrorCode, ObjectType, Oid, Repository, Signature};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::export::{
    CheckpointExport, CheckpointExportError, CheckpointExportInput,
    CheckpointSnapshotFromStateInput, build_snapshot_from_state, export_checkpoint,
};
use super::import::{
    CheckpointImportError, import_checkpoint_export, merge_store_states, parse_checkpoint_export,
};
use super::json_canon::{CanonJsonError, to_canon_json_bytes};
use super::layout::{MANIFEST_FILE, META_FILE};
use crate::core::{ContentHash, Limits, StoreEpoch, StoreId};
use crate::git::error::SyncError;

pub const STORE_META_REF: &str = "refs/beads/meta";
const STORE_META_FILE: &str = "store_meta.json";
const DEFAULT_PUBLISH_MAX_RETRIES: usize = 3;
const ENV_PUBLISH_MAX_RETRIES: &str = "BD_CHECKPOINT_PUBLISH_MAX_RETRIES";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointStoreMeta {
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
    pub checkpoint_format_version: u32,
    pub checkpoint_groups: BTreeMap<String, String>,
}

impl CheckpointStoreMeta {
    pub fn new(
        store_id: StoreId,
        store_epoch: StoreEpoch,
        checkpoint_format_version: u32,
        checkpoint_groups: BTreeMap<String, String>,
    ) -> Self {
        Self {
            store_id,
            store_epoch,
            checkpoint_format_version,
            checkpoint_groups,
        }
    }

    pub fn canon_bytes(&self) -> Result<Vec<u8>, CanonJsonError> {
        to_canon_json_bytes(self)
    }
}

#[derive(Clone, Debug)]
pub struct CheckpointPublishOutcome {
    pub checkpoint_id: ContentHash,
    pub checkpoint_commit: Oid,
    pub store_meta_commit: Oid,
}

#[derive(Debug, Error)]
pub enum CheckpointPublishError {
    #[error(transparent)]
    Sync(#[from] SyncError),
    #[error(transparent)]
    Export(#[from] CheckpointExportError),
    #[error(transparent)]
    Snapshot(#[from] super::export::CheckpointSnapshotError),
    #[error(transparent)]
    CanonJson(#[from] CanonJsonError),
    #[error(transparent)]
    Import(#[from] CheckpointImportError),
    #[error("checkpoint json parse error at {path}: {source}")]
    Json {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("checkpoint publish retry limit exceeded (retries {retries})")]
    TooManyRetries { retries: usize },
    #[error("remote checkpoint incompatible: {reason}")]
    IncompatibleRemote { reason: String },
    #[error("checkpoint tree path conflict at {path}")]
    PathConflict { path: String },
    #[error("checkpoint push rejected (non-fast-forward)")]
    NonFastForward,
    #[error("checkpoint push rejected: {message}")]
    PushRejected { message: String },
    #[error("checkpoint git error: {0}")]
    Git(#[from] git2::Error),
}

pub fn publish_checkpoint(
    repo: &Repository,
    export: &CheckpointExport,
    checkpoint_ref: &str,
    store_meta: &CheckpointStoreMeta,
) -> Result<CheckpointPublishOutcome, CheckpointPublishError> {
    let max_retries = std::env::var(ENV_PUBLISH_MAX_RETRIES)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_PUBLISH_MAX_RETRIES);
    publish_checkpoint_with_retry(repo, export, checkpoint_ref, store_meta, max_retries)
}

pub fn publish_checkpoint_with_retry(
    repo: &Repository,
    export: &CheckpointExport,
    checkpoint_ref: &str,
    store_meta: &CheckpointStoreMeta,
    max_retries: usize,
) -> Result<CheckpointPublishOutcome, CheckpointPublishError> {
    let limits = Limits::default();

    // Candidate state starts as the caller-provided export.
    let mut candidate_export = export.clone();
    let mut candidate_state =
        import_checkpoint_export(&parse_checkpoint_export(&candidate_export)?, &limits)?.state;
    let mut remote_parent: Option<Oid> = None;

    let mut retries = 0usize;
    let checkpoint_commit: Oid;

    loop {
        let parent = match remote_parent {
            Some(oid) => Some(oid),
            None => refname_to_id_optional(repo, checkpoint_ref)?,
        };

        let commit_oid = write_checkpoint_commit(repo, &candidate_export, checkpoint_ref, parent)?;
        match push_refs(repo, &[format!("{checkpoint_ref}:{checkpoint_ref}")]) {
            Ok(()) => {
                checkpoint_commit = commit_oid;
                break;
            }
            Err(CheckpointPublishError::NonFastForward) => {
                retries += 1;
                tracing::debug!(
                    retries,
                    checkpoint_ref,
                    "checkpoint publish retry: non-fast-forward"
                );
                if retries > max_retries {
                    tracing::warn!(
                        retries,
                        checkpoint_ref,
                        "checkpoint publish retry limit exceeded"
                    );
                    return Err(CheckpointPublishError::TooManyRetries { retries });
                }

                let Some(remote_oid) = fetch_remote_ref(repo, checkpoint_ref)? else {
                    // Retryable push error but no remote ref to merge (e.g. transient lock).
                    continue;
                };

                // Verify the commit exists in local object database (fetch should have brought it).
                let _commit = repo.find_commit(remote_oid)?;

                let remote_export = read_checkpoint_export_at_oid(repo, remote_oid)?;
                ensure_checkpoint_compatible(&candidate_export, &remote_export)?;
                let remote_parsed = parse_checkpoint_export(&remote_export)?;
                let remote_state = import_checkpoint_export(&remote_parsed, &limits)?.state;

                let merged_state = merge_store_states(&candidate_state, &remote_state)?;
                let merged_included = merge_included_watermarks(
                    &candidate_export.meta.included,
                    &remote_export.meta.included,
                );
                let merged_heads = merge_included_heads(
                    candidate_export.meta.included_heads.as_ref(),
                    remote_export.meta.included_heads.as_ref(),
                );
                let created_at_ms = std::cmp::max(
                    candidate_export.meta.created_at_ms,
                    remote_export.meta.created_at_ms,
                );

                // Re-export merged state into a new candidate checkpoint.
                let snapshot = build_snapshot_from_state(CheckpointSnapshotFromStateInput {
                    checkpoint_group: candidate_export.meta.checkpoint_group.clone(),
                    namespaces: candidate_export.meta.namespaces.clone(),
                    store_id: candidate_export.meta.store_id,
                    store_epoch: candidate_export.meta.store_epoch,
                    created_at_ms,
                    created_by_replica_id: candidate_export.meta.created_by_replica_id,
                    policy_hash: candidate_export.meta.policy_hash,
                    roster_hash: candidate_export.meta.roster_hash,
                    included: merged_included,
                    included_heads: merged_heads,
                    dirty_shards: None,
                    state: &merged_state,
                })?;

                candidate_export = export_checkpoint(CheckpointExportInput {
                    snapshot: &snapshot,
                    previous: None,
                })?;
                candidate_state = merged_state;
                remote_parent = Some(remote_oid);
                continue;
            }
            Err(e) => return Err(e),
        }
    }

    // Now publish store meta, with its own retry/merge loop.
    let group = candidate_export.meta.checkpoint_group.clone();
    let checkpoint_id_hex = candidate_export.meta.content_hash.to_hex();
    let store_meta_commit =
        publish_store_meta_with_retry(repo, store_meta, &group, &checkpoint_id_hex, max_retries)?;

    Ok(CheckpointPublishOutcome {
        checkpoint_id: candidate_export.meta.content_hash,
        checkpoint_commit,
        store_meta_commit,
    })
}

fn write_checkpoint_commit(
    repo: &Repository,
    export: &CheckpointExport,
    checkpoint_ref: &str,
    parent: Option<Oid>,
) -> Result<Oid, CheckpointPublishError> {
    let tree_oid = build_checkpoint_tree(repo, export)?;
    let tree = repo.find_tree(tree_oid)?;
    let sig = Signature::now("beads", "beads@localhost")?;
    let message = format!(
        "beads checkpoint {} {}",
        export.meta.checkpoint_group,
        export.meta.content_hash.to_hex()
    );

    let parents = match parent {
        Some(parent_oid) => vec![repo.find_commit(parent_oid)?],
        None => Vec::new(),
    };
    let parent_refs: Vec<_> = parents.iter().collect();
    let commit_oid = repo.commit(None, &sig, &sig, &message, &tree, &parent_refs)?;
    update_ref(
        repo,
        checkpoint_ref,
        commit_oid,
        "beads checkpoint: update ref",
    )?;
    Ok(commit_oid)
}

fn write_store_meta_commit(
    repo: &Repository,
    store_meta: &CheckpointStoreMeta,
    parent: Option<Oid>,
) -> Result<Oid, CheckpointPublishError> {
    let meta_bytes = store_meta.canon_bytes()?;
    let meta_oid = repo.blob(&meta_bytes)?;

    let mut builder = repo.treebuilder(None)?;
    builder.insert(STORE_META_FILE, meta_oid, 0o100644)?;
    let tree_oid = builder.write()?;
    let tree = repo.find_tree(tree_oid)?;

    let sig = Signature::now("beads", "beads@localhost")?;
    let message = "beads checkpoint: update store meta";

    let parents = match parent {
        Some(parent_oid) => vec![repo.find_commit(parent_oid)?],
        None => Vec::new(),
    };
    let parent_refs: Vec<_> = parents.iter().collect();
    let commit_oid = repo.commit(None, &sig, &sig, message, &tree, &parent_refs)?;
    update_ref(
        repo,
        STORE_META_REF,
        commit_oid,
        "beads checkpoint: update store meta",
    )?;
    Ok(commit_oid)
}

fn build_checkpoint_tree(
    repo: &Repository,
    export: &CheckpointExport,
) -> Result<Oid, CheckpointPublishError> {
    let meta_bytes = export.meta.canon_bytes()?;
    let manifest_bytes = export.manifest.canon_bytes()?;
    let meta_oid = repo.blob(&meta_bytes)?;
    let manifest_oid = repo.blob(&manifest_bytes)?;

    let mut root = BTreeMap::<String, TreeNode>::new();
    insert_path(&mut root, META_FILE, meta_oid)?;
    insert_path(&mut root, MANIFEST_FILE, manifest_oid)?;
    for (path, payload) in &export.files {
        let oid = repo.blob(payload.bytes.as_ref())?;
        let full_path = path.to_path();
        insert_path(&mut root, &full_path, oid)?;
    }

    build_tree(repo, &root)
}

#[derive(Debug)]
enum TreeNode {
    File(Oid),
    Dir(BTreeMap<String, TreeNode>),
}

fn insert_path(
    root: &mut BTreeMap<String, TreeNode>,
    path: &str,
    oid: Oid,
) -> Result<(), CheckpointPublishError> {
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() {
        return Err(CheckpointPublishError::PathConflict {
            path: path.to_string(),
        });
    }
    insert_path_parts(root, &parts, 0, path, oid)
}

fn insert_path_parts(
    root: &mut BTreeMap<String, TreeNode>,
    parts: &[&str],
    index: usize,
    full_path: &str,
    oid: Oid,
) -> Result<(), CheckpointPublishError> {
    let name = parts[index];
    let entry = root.entry(name.to_string());
    match entry {
        std::collections::btree_map::Entry::Vacant(vacant) => {
            if index + 1 == parts.len() {
                vacant.insert(TreeNode::File(oid));
                return Ok(());
            }
            let mut dir = BTreeMap::new();
            insert_path_parts(&mut dir, parts, index + 1, full_path, oid)?;
            vacant.insert(TreeNode::Dir(dir));
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(mut occupied) => match occupied.get_mut() {
            TreeNode::File(_) => Err(CheckpointPublishError::PathConflict {
                path: full_path.to_string(),
            }),
            TreeNode::Dir(children) => {
                if index + 1 == parts.len() {
                    return Err(CheckpointPublishError::PathConflict {
                        path: full_path.to_string(),
                    });
                }
                insert_path_parts(children, parts, index + 1, full_path, oid)
            }
        },
    }
}

fn build_tree(
    repo: &Repository,
    entries: &BTreeMap<String, TreeNode>,
) -> Result<Oid, CheckpointPublishError> {
    let mut builder = repo.treebuilder(None)?;
    for (name, node) in entries {
        match node {
            TreeNode::File(oid) => {
                builder.insert(name, *oid, 0o100644)?;
            }
            TreeNode::Dir(children) => {
                let child_oid = build_tree(repo, children)?;
                builder.insert(name, child_oid, 0o040000)?;
            }
        }
    }
    Ok(builder.write()?)
}

fn refname_to_id_optional(
    repo: &Repository,
    name: &str,
) -> Result<Option<Oid>, CheckpointPublishError> {
    match repo.refname_to_id(name) {
        Ok(oid) => Ok(Some(oid)),
        Err(e) if e.code() == ErrorCode::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn update_ref(
    repo: &Repository,
    name: &str,
    oid: Oid,
    message: &str,
) -> Result<(), CheckpointPublishError> {
    match repo.find_reference(name) {
        Ok(mut reference) => {
            reference.set_target(oid, message)?;
            Ok(())
        }
        Err(e) if e.code() == ErrorCode::NotFound => {
            repo.reference(name, oid, true, message)?;
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

fn push_refs(repo: &Repository, refspecs: &[String]) -> Result<(), CheckpointPublishError> {
    let mut remote = match repo.find_remote("origin") {
        Ok(remote) => remote,
        Err(_) => return Ok(()),
    };

    let push_error: RefCell<Option<String>> = RefCell::new(None);
    {
        let cfg = repo.config().ok();
        let mut callbacks = git2::RemoteCallbacks::new();
        callbacks.credentials(move |url, username_from_url, allowed| {
            if allowed.is_ssh_key()
                && let Some(user) = username_from_url
            {
                return git2::Cred::ssh_key_from_agent(user);
            }
            if allowed.is_user_pass_plaintext()
                && let Some(ref cfg) = cfg
                && let Ok(cred) = git2::Cred::credential_helper(cfg, url, username_from_url)
            {
                return Ok(cred);
            }
            git2::Cred::default()
        });
        callbacks.push_update_reference(|_ref_name, status| {
            if let Some(msg) = status {
                *push_error.borrow_mut() = Some(msg.to_string());
            }
            Ok(())
        });

        let mut push_options = git2::PushOptions::new();
        push_options.remote_callbacks(callbacks);

        if let Err(e) = remote.push(refspecs, Some(&mut push_options)) {
            let msg = e.to_string();
            if is_non_fast_forward(&msg) {
                return Err(CheckpointPublishError::NonFastForward);
            }
            return Err(CheckpointPublishError::Git(e));
        }
    }

    if let Some(err) = push_error.into_inner() {
        if is_non_fast_forward(&err) {
            return Err(CheckpointPublishError::NonFastForward);
        }
        return Err(CheckpointPublishError::PushRejected { message: err });
    }

    Ok(())
}

fn is_non_fast_forward(message: &str) -> bool {
    let msg = message.to_lowercase();
    msg.contains("non-fast-forward")
        || msg.contains("non-fastforwardable")
        || msg.contains("fetch first")
        || msg.contains("cannot lock ref")
        || msg.contains("failed to update ref")
        || msg.contains("failed to lock file")
        || msg.contains("commits that are not present locally")
}

fn publish_store_meta_with_retry(
    repo: &Repository,
    base: &CheckpointStoreMeta,
    updated_group: &str,
    updated_checkpoint_id_hex: &str,
    max_retries: usize,
) -> Result<Oid, CheckpointPublishError> {
    let mut retries = 0usize;
    let mut candidate = base.clone();
    candidate.checkpoint_groups.insert(
        updated_group.to_string(),
        updated_checkpoint_id_hex.to_string(),
    );

    let mut remote_parent: Option<Oid> = None;

    loop {
        let parent = match remote_parent {
            Some(oid) => Some(oid),
            None => refname_to_id_optional(repo, STORE_META_REF)?,
        };

        let commit_oid = write_store_meta_commit(repo, &candidate, parent)?;
        match push_refs(repo, &[format!("{STORE_META_REF}:{STORE_META_REF}")]) {
            Ok(()) => return Ok(commit_oid),
            Err(CheckpointPublishError::NonFastForward) => {
                retries += 1;
                tracing::debug!(
                    retries,
                    ref_name = STORE_META_REF,
                    "store meta publish retry: non-fast-forward"
                );
                if retries > max_retries {
                    tracing::warn!(
                        retries,
                        ref_name = STORE_META_REF,
                        "store meta publish retry limit exceeded"
                    );
                    return Err(CheckpointPublishError::TooManyRetries { retries });
                }

                let Some(remote_oid) = fetch_remote_ref(repo, STORE_META_REF)? else {
                    // Retryable push error, but no remote store meta to merge.
                    continue;
                };

                let remote_meta = read_store_meta_at_oid(repo, remote_oid)?;
                ensure_store_meta_compatible(base, &remote_meta)?;

                // Merge: keep remote as base, only add missing groups from local base,
                // and always set the one group we're updating.
                let mut merged = remote_meta;
                for (k, v) in &base.checkpoint_groups {
                    merged
                        .checkpoint_groups
                        .entry(k.clone())
                        .or_insert(v.clone());
                }
                merged.checkpoint_groups.insert(
                    updated_group.to_string(),
                    updated_checkpoint_id_hex.to_string(),
                );

                candidate = merged;
                remote_parent = Some(remote_oid);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

fn ensure_checkpoint_compatible(
    local: &CheckpointExport,
    remote: &CheckpointExport,
) -> Result<(), CheckpointPublishError> {
    let l = &local.meta;
    let r = &remote.meta;

    if l.checkpoint_format_version != r.checkpoint_format_version {
        return Err(CheckpointPublishError::IncompatibleRemote {
            reason: "checkpoint_format_version mismatch".to_string(),
        });
    }
    if l.store_id != r.store_id {
        return Err(CheckpointPublishError::IncompatibleRemote {
            reason: "store_id mismatch".to_string(),
        });
    }
    if l.store_epoch != r.store_epoch {
        return Err(CheckpointPublishError::IncompatibleRemote {
            reason: "store_epoch mismatch".to_string(),
        });
    }
    if l.checkpoint_group != r.checkpoint_group {
        return Err(CheckpointPublishError::IncompatibleRemote {
            reason: "checkpoint_group mismatch".to_string(),
        });
    }
    if l.policy_hash != r.policy_hash {
        return Err(CheckpointPublishError::IncompatibleRemote {
            reason: "policy_hash mismatch".to_string(),
        });
    }
    if l.roster_hash != r.roster_hash {
        return Err(CheckpointPublishError::IncompatibleRemote {
            reason: "roster_hash mismatch".to_string(),
        });
    }

    let mut ln = l.namespaces.clone();
    ln.sort();
    ln.dedup();
    let mut rn = r.namespaces.clone();
    rn.sort();
    rn.dedup();
    if ln != rn {
        return Err(CheckpointPublishError::IncompatibleRemote {
            reason: "namespaces mismatch".to_string(),
        });
    }

    Ok(())
}

fn ensure_store_meta_compatible(
    local: &CheckpointStoreMeta,
    remote: &CheckpointStoreMeta,
) -> Result<(), CheckpointPublishError> {
    if local.store_id != remote.store_id {
        return Err(CheckpointPublishError::IncompatibleRemote {
            reason: "store_meta store_id mismatch".to_string(),
        });
    }
    if local.store_epoch != remote.store_epoch {
        return Err(CheckpointPublishError::IncompatibleRemote {
            reason: "store_meta store_epoch mismatch".to_string(),
        });
    }
    if local.checkpoint_format_version != remote.checkpoint_format_version {
        return Err(CheckpointPublishError::IncompatibleRemote {
            reason: "store_meta checkpoint_format_version mismatch".to_string(),
        });
    }
    Ok(())
}

/// Merges two `IncludedWatermarks` maps using CRDT max semantics.
///
/// For each (namespace, replica) pair, takes the maximum sequence number.
/// This is commutative and associative, ensuring convergence regardless of merge order.
fn merge_included_watermarks(
    a: &super::meta::IncludedWatermarks,
    b: &super::meta::IncludedWatermarks,
) -> super::meta::IncludedWatermarks {
    let mut out = a.clone();
    for (ns, origins) in b {
        let entry = out.entry(ns.clone()).or_default();
        for (replica, seq) in origins {
            let slot = entry.entry(*replica).or_insert(*seq);
            *slot = (*slot).max(*seq);
        }
    }
    out
}

/// Merges two `IncludedHeads` maps with deterministic conflict resolution.
///
/// `IncludedHeads` is a diagnostic field tracking the content hash of each replica's
/// last observed event. Unlike watermarks (which are monotonic sequence numbers),
/// content hashes have no natural ordering, so we use lexicographic comparison of
/// the raw bytes for deterministic tie-breaking.
///
/// This ensures all replicas converge to the same merged state regardless of merge
/// order, satisfying the CRDT commutativity requirement. The "winning" hash may not
/// be semantically meaningful, but consistency across replicas is what matters for
/// this diagnostic field.
fn merge_included_heads(
    a: Option<&super::meta::IncludedHeads>,
    b: Option<&super::meta::IncludedHeads>,
) -> Option<super::meta::IncludedHeads> {
    let mut out = super::meta::IncludedHeads::new();

    let mut any = false;
    if let Some(a) = a {
        for (ns, origins) in a {
            any = true;
            let dst = out.entry(ns.clone()).or_default();
            for (replica, head) in origins {
                dst.insert(*replica, *head);
            }
        }
    }
    if let Some(b) = b {
        for (ns, origins) in b {
            any = true;
            let dst = out.entry(ns.clone()).or_default();
            for (replica, head) in origins {
                // Deterministic conflict resolution: compare raw bytes for consistent tie-breaking.
                match dst.get(replica) {
                    Some(existing) => {
                        if head.as_bytes() > existing.as_bytes() {
                            dst.insert(*replica, *head);
                        }
                    }
                    None => {
                        dst.insert(*replica, *head);
                    }
                }
            }
        }
    }

    if any { Some(out) } else { None }
}

fn tracking_ref_for(remote_ref: &str) -> String {
    let suffix = remote_ref.strip_prefix("refs/").unwrap_or(remote_ref);
    format!("refs/remotes/origin/{}", suffix)
}

fn fetch_remote_ref(
    repo: &Repository,
    remote_ref: &str,
) -> Result<Option<Oid>, CheckpointPublishError> {
    let mut remote = match repo.find_remote("origin") {
        Ok(remote) => remote,
        Err(_) => return Ok(None),
    };

    let tracking_ref = tracking_ref_for(remote_ref);
    // Use + prefix to force update tracking ref even if not fast-forward.
    let refspec = format!("+{remote_ref}:{tracking_ref}");

    let cfg = repo.config().ok();
    let mut callbacks = git2::RemoteCallbacks::new();
    callbacks.credentials(move |url, username_from_url, allowed| {
        if allowed.is_ssh_key()
            && let Some(user) = username_from_url
        {
            return git2::Cred::ssh_key_from_agent(user);
        }
        if allowed.is_user_pass_plaintext()
            && let Some(ref cfg) = cfg
            && let Ok(cred) = git2::Cred::credential_helper(cfg, url, username_from_url)
        {
            return Ok(cred);
        }
        git2::Cred::default()
    });

    let mut fo = git2::FetchOptions::new();
    fo.remote_callbacks(callbacks);

    match remote.fetch(&[refspec.as_str()], Some(&mut fo), None) {
        Ok(()) => {}
        Err(e) => {
            let msg = e.to_string();
            // Missing remote ref is normal during bootstrap or if only some refs exist.
            if msg.contains("couldn't find remote ref")
                || msg.contains("not found")
                || e.code() == ErrorCode::NotFound
            {
                // Best-effort cleanup of stale tracking ref.
                let _ = repo.find_reference(&tracking_ref).map(|mut r| r.delete());
                return Ok(None);
            }
            return Err(CheckpointPublishError::Git(e));
        }
    }

    match repo.refname_to_id(&tracking_ref) {
        Ok(oid) => Ok(Some(oid)),
        Err(e) if e.code() == ErrorCode::NotFound => Ok(None),
        Err(e) => Err(CheckpointPublishError::Git(e)),
    }
}

fn read_checkpoint_export_at_oid(
    repo: &Repository,
    oid: Oid,
) -> Result<CheckpointExport, CheckpointPublishError> {
    let commit = repo.find_commit(oid)?;
    let tree = commit.tree()?;

    let meta_bytes = read_blob_bytes(repo, &tree, META_FILE)?;
    let meta: super::meta::CheckpointMeta =
        serde_json::from_slice(&meta_bytes).map_err(|source| CheckpointPublishError::Json {
            path: META_FILE.to_string(),
            source,
        })?;

    let manifest_bytes = read_blob_bytes(repo, &tree, MANIFEST_FILE)?;
    let manifest: super::manifest::CheckpointManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|source| CheckpointPublishError::Json {
            path: MANIFEST_FILE.to_string(),
            source,
        })?;

    let mut files = BTreeMap::new();
    for path in manifest.files.keys() {
        let full_path = path.to_path();
        let bytes = read_blob_bytes(repo, &tree, &full_path)?;
        files.insert(
            path.clone(),
            super::types::CheckpointShardPayload {
                path: path.clone(),
                bytes: bytes::Bytes::copy_from_slice(&bytes),
            },
        );
    }

    Ok(CheckpointExport {
        manifest,
        meta,
        files,
    })
}

fn read_store_meta_at_oid(
    repo: &Repository,
    oid: Oid,
) -> Result<CheckpointStoreMeta, CheckpointPublishError> {
    let commit = repo.find_commit(oid)?;
    let tree = commit.tree()?;
    let bytes = read_blob_bytes(repo, &tree, STORE_META_FILE)?;
    serde_json::from_slice(&bytes).map_err(|source| CheckpointPublishError::Json {
        path: STORE_META_FILE.to_string(),
        source,
    })
}

fn read_blob_bytes(
    repo: &Repository,
    tree: &git2::Tree,
    path: &str,
) -> Result<Vec<u8>, CheckpointPublishError> {
    let entry = tree.get_path(Path::new(path))?;
    let obj = repo.find_object(entry.id(), Some(ObjectType::Blob))?;
    let blob = obj.peel_to_blob()?;
    Ok(blob.content().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::core::bead::{BeadCore, BeadFields};
    use crate::core::composite::{Claim, Workflow};
    use crate::core::crdt::Lww;
    use crate::core::domain::{BeadType, Priority};
    use crate::core::identity::BeadId;
    use crate::core::time::{Stamp, WriteStamp};
    use crate::core::{
        ActorId, CanonicalState, ContentHash, Durable, HeadStatus, Limits, NamespaceId, ReplicaId,
        Seq0, StoreEpoch, StoreId, StoreState, Watermarks,
    };
    use crate::git::checkpoint::{
        CheckpointExportInput, CheckpointSnapshotInput, build_snapshot, export_checkpoint,
    };

    const CHECKPOINT_REF: &str = "refs/beads/checkpoints/core";

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
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        crate::core::Bead::new(core, fields)
    }

    fn build_export(
        store_id: StoreId,
        created_by: ReplicaId,
        bead_id: &str,
        created_at_ms: u64,
    ) -> CheckpointExport {
        let namespace = NamespaceId::core();
        let stamp = make_stamp(created_at_ms, 0, "author");
        let bead_id = BeadId::parse(bead_id).unwrap();
        let bead = make_bead(&bead_id, &stamp);

        let mut core_state = CanonicalState::new();
        core_state.insert(bead).unwrap();
        let mut state = StoreState::new();
        state.set_core_state(core_state);

        let mut watermarks = Watermarks::<Durable>::new();
        watermarks
            .observe_at_least(
                &namespace,
                &created_by,
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
            created_by_replica_id: created_by,
            policy_hash: ContentHash::from_bytes([9u8; 32]),
            roster_hash: None,
            dirty_shards: None,
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

    #[test]
    fn publish_checkpoint_merges_on_non_fast_forward_and_converges() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let a_dir = tmp.path().join("writer-a");
        let b_dir = tmp.path().join("writer-b");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let repo_a = Repository::init(&a_dir).unwrap();
        let repo_b = Repository::init(&b_dir).unwrap();

        repo_a
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();
        repo_b
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
        let replica_a = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
        let replica_b = ReplicaId::new(Uuid::from_bytes([2u8; 16]));

        let export_a = build_export(store_id, replica_a, "beads-rs-aaa1", 1_700_000_000_000);
        let mut groups_a = BTreeMap::new();
        groups_a.insert("core".to_string(), export_a.meta.content_hash.to_hex());
        let store_meta_a = CheckpointStoreMeta::new(
            store_id,
            export_a.meta.store_epoch,
            export_a.meta.checkpoint_format_version,
            groups_a,
        );

        let out_a =
            publish_checkpoint_with_retry(&repo_a, &export_a, CHECKPOINT_REF, &store_meta_a, 3)
                .expect("publish a");

        let export_b = build_export(store_id, replica_b, "beads-rs-bbb1", 1_700_000_000_100);
        let mut groups_b = BTreeMap::new();
        groups_b.insert("core".to_string(), export_b.meta.content_hash.to_hex());
        let store_meta_b = CheckpointStoreMeta::new(
            store_id,
            export_b.meta.store_epoch,
            export_b.meta.checkpoint_format_version,
            groups_b,
        );

        let out_b =
            publish_checkpoint_with_retry(&repo_b, &export_b, CHECKPOINT_REF, &store_meta_b, 3)
                .expect("publish b");

        // Remote head should now be writer B's merged commit, with parent = writer A's commit.
        let remote_head = remote_repo.refname_to_id(CHECKPOINT_REF).unwrap();
        assert_eq!(remote_head, out_b.checkpoint_commit);
        let commit_b = remote_repo.find_commit(out_b.checkpoint_commit).unwrap();
        assert_eq!(commit_b.parent_id(0).unwrap(), out_a.checkpoint_commit);

        // Import remote checkpoint and verify both beads exist.
        let remote_export = read_checkpoint_export_at_oid(&remote_repo, remote_head).unwrap();
        let parsed = parse_checkpoint_export(&remote_export).unwrap();
        let imported = import_checkpoint_export(&parsed, &Limits::default()).unwrap();
        let core_state = imported.state.core();

        let a_id = BeadId::parse("beads-rs-aaa1").unwrap();
        let b_id = BeadId::parse("beads-rs-bbb1").unwrap();
        assert!(core_state.get_live(&a_id).is_some());
        assert!(core_state.get_live(&b_id).is_some());

        // Store meta should point core → merged checkpoint id.
        let meta_oid = remote_repo.refname_to_id(STORE_META_REF).unwrap();
        let meta = read_store_meta_at_oid(&remote_repo, meta_oid).unwrap();
        assert_eq!(
            meta.checkpoint_groups.get("core").unwrap(),
            &out_b.checkpoint_id.to_hex()
        );
    }

    #[test]
    fn publish_checkpoint_returns_too_many_retries_when_limit_exceeded() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let a_dir = tmp.path().join("writer-a");
        let b_dir = tmp.path().join("writer-b");

        Repository::init_bare(&remote_dir).unwrap();
        let repo_a = Repository::init(&a_dir).unwrap();
        let repo_b = Repository::init(&b_dir).unwrap();

        repo_a
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();
        repo_b
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
        let replica_a = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
        let replica_b = ReplicaId::new(Uuid::from_bytes([2u8; 16]));

        // Writer A publishes first, establishing the remote state.
        let export_a = build_export(store_id, replica_a, "beads-rs-aaa1", 1_700_000_000_000);
        let mut groups_a = BTreeMap::new();
        groups_a.insert("core".to_string(), export_a.meta.content_hash.to_hex());
        let store_meta_a = CheckpointStoreMeta::new(
            store_id,
            export_a.meta.store_epoch,
            export_a.meta.checkpoint_format_version,
            groups_a,
        );
        publish_checkpoint_with_retry(&repo_a, &export_a, CHECKPOINT_REF, &store_meta_a, 3)
            .expect("publish a");

        // Writer B tries to publish with max_retries=0, so the first non-fast-forward fails.
        let export_b = build_export(store_id, replica_b, "beads-rs-bbb1", 1_700_000_000_100);
        let mut groups_b = BTreeMap::new();
        groups_b.insert("core".to_string(), export_b.meta.content_hash.to_hex());
        let store_meta_b = CheckpointStoreMeta::new(
            store_id,
            export_b.meta.store_epoch,
            export_b.meta.checkpoint_format_version,
            groups_b,
        );

        let result =
            publish_checkpoint_with_retry(&repo_b, &export_b, CHECKPOINT_REF, &store_meta_b, 0);

        match result {
            Err(CheckpointPublishError::TooManyRetries { retries }) => {
                assert_eq!(retries, 1, "should fail after exactly 1 retry attempt");
            }
            Ok(_) => panic!("expected TooManyRetries error, got success"),
            Err(e) => panic!("expected TooManyRetries error, got: {e:?}"),
        }
    }
}
