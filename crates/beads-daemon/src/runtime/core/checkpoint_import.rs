use std::path::Path;

use git2::Repository;
use thiserror::Error;

use super::{
    CheckpointCache, CheckpointImport, ContentHash, Daemon, StoreId, checkpoint_ref_oid,
    import_checkpoint, load_replica_roster, policy_hash, roster_hash, write_checkpoint_tree,
};
use crate::git::checkpoint::CheckpointImportError;
use crate::runtime::checkpoint_scheduler::CheckpointGroupSnapshot;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum CheckpointCompatibilityError {
    #[error(
        "checkpoint policy hash mismatch for group {checkpoint_group}: local={local_policy_hash:?} checkpoint={checkpoint_policy_hash}"
    )]
    PolicyHashMismatch {
        checkpoint_group: String,
        local_policy_hash: Option<ContentHash>,
        checkpoint_policy_hash: ContentHash,
    },
    #[error(
        "checkpoint roster hash mismatch for group {checkpoint_group}: local={local_roster_hash:?} checkpoint={checkpoint_roster_hash}"
    )]
    RosterHashMismatch {
        checkpoint_group: String,
        local_roster_hash: Option<ContentHash>,
        checkpoint_roster_hash: ContentHash,
    },
}

fn ensure_checkpoint_compatible(
    import: &CheckpointImport,
    local_policy_hash: Option<ContentHash>,
    local_roster_hash: Option<ContentHash>,
) -> Result<(), CheckpointCompatibilityError> {
    if let Some(local_policy_hash) = local_policy_hash
        && import.policy_hash != local_policy_hash
    {
        return Err(CheckpointCompatibilityError::PolicyHashMismatch {
            checkpoint_group: import.checkpoint_group.clone(),
            local_policy_hash: Some(local_policy_hash),
            checkpoint_policy_hash: import.policy_hash,
        });
    }

    if let Some(checkpoint_roster_hash) = import.roster_hash
        && Some(checkpoint_roster_hash) != local_roster_hash
    {
        return Err(CheckpointCompatibilityError::RosterHashMismatch {
            checkpoint_group: import.checkpoint_group.clone(),
            local_roster_hash,
            checkpoint_roster_hash,
        });
    }

    Ok(())
}

impl Daemon {
    pub(super) fn load_checkpoint_imports(
        &self,
        store_id: StoreId,
        repo: &Path,
    ) -> Result<Vec<CheckpointImport>, CheckpointCompatibilityError> {
        let groups = self.checkpoint_group_snapshots(store_id);
        if groups.is_empty() {
            return Ok(Vec::new());
        }

        let local_policy_hash = self.local_policy_hash(store_id);
        let local_roster_hash = self.local_roster_hash(store_id);

        let repo_handle = match Repository::open(repo) {
            Ok(repo) => Some(repo),
            Err(err) => {
                tracing::warn!(
                    store_id = %store_id,
                    path = %repo.display(),
                    error = ?err,
                    "checkpoint import skipped: repo open failed"
                );
                None
            }
        };

        let mut imports = Vec::new();
        for group in groups {
            if let Some(import) = self.import_checkpoint_from_cache(store_id, &group) {
                ensure_checkpoint_compatible(&import, local_policy_hash, local_roster_hash)?;
                imports.push(import);
                continue;
            }
            if let Some(repo) = repo_handle.as_ref()
                && let Some(import) = self.import_checkpoint_from_git(repo, &group)
            {
                ensure_checkpoint_compatible(&import, local_policy_hash, local_roster_hash)?;
                imports.push(import);
            }
        }

        Ok(imports)
    }

    fn local_policy_hash(&self, store_id: StoreId) -> Option<ContentHash> {
        let store = self.store_sessions.get(&store_id)?.runtime();
        match policy_hash(&store.policies) {
            Ok(hash) => Some(hash),
            Err(err) => {
                tracing::warn!(
                    store_id = %store_id,
                    error = ?err,
                    "policy hash computation failed"
                );
                None
            }
        }
    }

    fn local_roster_hash(&self, store_id: StoreId) -> Option<ContentHash> {
        let roster = match load_replica_roster(self.layout(), store_id) {
            Ok(Some(roster)) => roster,
            Ok(None) => return None,
            Err(err) => {
                tracing::warn!(
                    store_id = %store_id,
                    error = ?err,
                    "replica roster load failed for checkpoint hash"
                );
                return None;
            }
        };

        match roster_hash(&roster) {
            Ok(hash) => Some(hash),
            Err(err) => {
                tracing::warn!(
                    store_id = %store_id,
                    error = ?err,
                    "replica roster hash computation failed"
                );
                None
            }
        }
    }

    fn import_checkpoint_from_cache(
        &self,
        store_id: StoreId,
        group: &CheckpointGroupSnapshot,
    ) -> Option<CheckpointImport> {
        let cache = CheckpointCache::new(store_id, group.group.clone());
        match cache.load_current() {
            Ok(Some(entry)) => match import_checkpoint(&entry.dir, self.limits()) {
                Ok(import) => {
                    tracing::info!(
                        store_id = %store_id,
                        checkpoint_group = %group.group,
                        checkpoint_id = %entry.checkpoint_id,
                        "checkpoint cache import succeeded"
                    );
                    Some(import)
                }
                Err(CheckpointImportError::IncompatibleDepsFormat { .. }) => {
                    tracing::warn!(
                        store_id = %store_id,
                        checkpoint_group = %group.group,
                        "checkpoint cache import incompatible deps format; ignoring and rebuilding from canonical store"
                    );
                    None
                }
                Err(err) => {
                    tracing::warn!(
                        store_id = %store_id,
                        checkpoint_group = %group.group,
                        error = ?err,
                        "checkpoint cache import failed"
                    );
                    None
                }
            },
            Ok(None) => None,
            Err(err) => {
                tracing::warn!(
                    store_id = %store_id,
                    checkpoint_group = %group.group,
                    error = ?err,
                    "checkpoint cache load failed"
                );
                None
            }
        }
    }

    fn import_checkpoint_from_git(
        &self,
        repo: &Repository,
        group: &CheckpointGroupSnapshot,
    ) -> Option<CheckpointImport> {
        let oid = match checkpoint_ref_oid(repo, &group.git_ref) {
            Ok(Some(oid)) => oid,
            Ok(None) => return None,
            Err(err) => {
                tracing::warn!(
                    checkpoint_group = %group.group,
                    git_ref = %group.git_ref,
                    error = ?err,
                    "checkpoint ref lookup failed"
                );
                return None;
            }
        };
        let commit = match repo.find_commit(oid) {
            Ok(commit) => commit,
            Err(err) => {
                tracing::warn!(
                    checkpoint_group = %group.group,
                    git_ref = %group.git_ref,
                    error = ?err,
                    "checkpoint ref is not a commit"
                );
                return None;
            }
        };

        let tree = match commit.tree() {
            Ok(tree) => tree,
            Err(err) => {
                tracing::warn!(
                    checkpoint_group = %group.group,
                    git_ref = %group.git_ref,
                    error = ?err,
                    "checkpoint tree read failed"
                );
                return None;
            }
        };

        let temp = match tempfile::tempdir() {
            Ok(temp) => temp,
            Err(err) => {
                tracing::warn!(
                    checkpoint_group = %group.group,
                    git_ref = %group.git_ref,
                    error = ?err,
                    "checkpoint tempdir creation failed"
                );
                return None;
            }
        };

        if let Err(err) = write_checkpoint_tree(repo, &tree, temp.path()) {
            tracing::warn!(
                checkpoint_group = %group.group,
                git_ref = %group.git_ref,
                error = ?err,
                "checkpoint tree export failed"
            );
            return None;
        }

        match import_checkpoint(temp.path(), self.limits()) {
            Ok(import) => {
                tracing::info!(
                    checkpoint_group = %group.group,
                    git_ref = %group.git_ref,
                    "checkpoint git import succeeded"
                );
                Some(import)
            }
            Err(CheckpointImportError::IncompatibleDepsFormat { .. }) => {
                tracing::warn!(
                    checkpoint_group = %group.group,
                    git_ref = %group.git_ref,
                    "checkpoint git import incompatible deps format; skipping legacy checkpoint"
                );
                None
            }
            Err(err) => {
                tracing::warn!(
                    checkpoint_group = %group.group,
                    git_ref = %group.git_ref,
                    error = ?err,
                    "checkpoint git import failed"
                );
                None
            }
        }
    }
}
