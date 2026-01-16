//! Git publish helpers for checkpoints.

use std::cell::RefCell;
use std::collections::BTreeMap;

use git2::{ErrorCode, Oid, Repository, Signature};
use serde::Serialize;
use thiserror::Error;

use super::export::{CheckpointExport, CheckpointExportError};
use super::json_canon::{CanonJsonError, to_canon_json_bytes};
use super::layout::{MANIFEST_FILE, META_FILE};
use crate::core::{ContentHash, StoreEpoch, StoreId};
use crate::git::error::SyncError;

pub const STORE_META_REF: &str = "refs/beads/meta";

#[derive(Clone, Debug, Serialize)]
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
    CanonJson(#[from] CanonJsonError),
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
    let checkpoint_commit = write_checkpoint_commit(repo, export, checkpoint_ref)?;
    let store_meta_commit = write_store_meta_commit(repo, store_meta)?;
    push_refs(
        repo,
        &[
            format!("{checkpoint_ref}:{checkpoint_ref}"),
            format!("{STORE_META_REF}:{STORE_META_REF}"),
        ],
    )?;

    Ok(CheckpointPublishOutcome {
        checkpoint_id: export.meta.content_hash,
        checkpoint_commit,
        store_meta_commit,
    })
}

fn write_checkpoint_commit(
    repo: &Repository,
    export: &CheckpointExport,
    checkpoint_ref: &str,
) -> Result<Oid, CheckpointPublishError> {
    let tree_oid = build_checkpoint_tree(repo, export)?;
    let tree = repo.find_tree(tree_oid)?;
    let sig = Signature::now("beads", "beads@localhost")?;
    let message = format!(
        "beads checkpoint {} {}",
        export.meta.checkpoint_group,
        export.meta.content_hash.to_hex()
    );

    let parents = match refname_to_id_optional(repo, checkpoint_ref)? {
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
) -> Result<Oid, CheckpointPublishError> {
    let meta_bytes = store_meta.canon_bytes()?;
    let meta_oid = repo.blob(&meta_bytes)?;

    let mut builder = repo.treebuilder(None)?;
    builder.insert("store_meta.json", meta_oid, 0o100644)?;
    let tree_oid = builder.write()?;
    let tree = repo.find_tree(tree_oid)?;

    let sig = Signature::now("beads", "beads@localhost")?;
    let message = "beads checkpoint: update store meta";

    let parents = match refname_to_id_optional(repo, STORE_META_REF)? {
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
        insert_path(&mut root, path, oid)?;
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
    message.contains("non-fast-forward")
        || message.contains("fetch first")
        || message.contains("cannot lock ref")
        || message.contains("failed to update ref")
}
