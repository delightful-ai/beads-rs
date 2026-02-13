use std::path::Path;

use git2::Repository;

use super::{
    CheckpointCache, CheckpointImport, ContentHash, Daemon, StoreId, checkpoint_ref_oid,
    import_checkpoint, load_replica_roster, policy_hash, roster_hash, write_checkpoint_tree,
};
use crate::daemon::checkpoint_scheduler::CheckpointGroupSnapshot;

impl Daemon {
    pub(super) fn load_checkpoint_imports(
        &self,
        store_id: StoreId,
        repo: &Path,
    ) -> Vec<CheckpointImport> {
        let groups = self.checkpoint_group_snapshots(store_id);
        if groups.is_empty() {
            return Vec::new();
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
                self.warn_on_checkpoint_hash_mismatch(
                    store_id,
                    &import,
                    local_policy_hash,
                    local_roster_hash,
                );
                imports.push(import);
                continue;
            }
            if let Some(repo) = repo_handle.as_ref()
                && let Some(import) = self.import_checkpoint_from_git(repo, &group)
            {
                self.warn_on_checkpoint_hash_mismatch(
                    store_id,
                    &import,
                    local_policy_hash,
                    local_roster_hash,
                );
                imports.push(import);
            }
        }

        imports
    }

    fn local_policy_hash(&self, store_id: StoreId) -> Option<ContentHash> {
        let store = self.stores.get(&store_id)?;
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
        let roster = match load_replica_roster(store_id) {
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

    pub(super) fn warn_on_checkpoint_hash_mismatch(
        &self,
        store_id: StoreId,
        import: &CheckpointImport,
        local_policy_hash: Option<ContentHash>,
        local_roster_hash: Option<ContentHash>,
    ) {
        if let Some(local_policy_hash) = local_policy_hash
            && import.policy_hash != local_policy_hash
        {
            let checkpoint_policy_hash = import.policy_hash.to_hex();
            let local_policy_hash = local_policy_hash.to_hex();
            tracing::warn!(
                store_id = %store_id,
                checkpoint_group = %import.checkpoint_group,
                local_policy_hash = %local_policy_hash,
                checkpoint_policy_hash = %checkpoint_policy_hash,
                "checkpoint policy hash mismatch"
            );
        }

        if import.roster_hash.is_some() && import.roster_hash != local_roster_hash {
            let checkpoint_roster_hash = import.roster_hash.map(|hash| hash.to_hex());
            let local_roster_hash = local_roster_hash.map(|hash| hash.to_hex());
            tracing::warn!(
                store_id = %store_id,
                checkpoint_group = %import.checkpoint_group,
                local_roster_hash = ?local_roster_hash,
                checkpoint_roster_hash = ?checkpoint_roster_hash,
                "checkpoint roster hash mismatch"
            );
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
