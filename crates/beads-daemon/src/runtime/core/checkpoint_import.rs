use std::path::Path;

use git2::Repository;

use super::{
    CheckpointCache, CheckpointImport, ContentHash, Daemon, StoreId, checkpoint_ref_oid,
    import_checkpoint, load_replica_roster, policy_hash, roster_hash, write_checkpoint_tree,
};
use crate::git::checkpoint::CheckpointImportError;
use crate::runtime::checkpoint_scheduler::CheckpointGroupSnapshot;

pub(super) struct LoadedCheckpointImports {
    pub(super) imports: Vec<CheckpointImport>,
    pub(super) incompatible_groups: Vec<CheckpointGroupSnapshot>,
}

enum CheckpointImportOutcome {
    Imported(CheckpointImport),
    Incompatible,
    Absent,
}

enum CheckpointHashMismatch {
    Policy,
    Roster,
}

impl Daemon {
    pub(super) fn load_checkpoint_imports(
        &self,
        store_id: StoreId,
        repo: &Path,
    ) -> LoadedCheckpointImports {
        let groups = self.checkpoint_group_snapshots(store_id);
        if groups.is_empty() {
            return LoadedCheckpointImports {
                imports: Vec::new(),
                incompatible_groups: Vec::new(),
            };
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
        let mut incompatible_groups = Vec::new();
        for group in groups {
            let mut incompatible = false;
            let mut selected_import = None;

            match self.import_checkpoint_from_cache(store_id, &group) {
                CheckpointImportOutcome::Imported(import) => {
                    if self
                        .ensure_checkpoint_hashes_compatible(
                            store_id,
                            &import,
                            local_policy_hash,
                            local_roster_hash,
                        )
                        .is_ok()
                    {
                        selected_import = Some(import);
                    }
                }
                CheckpointImportOutcome::Incompatible => incompatible = true,
                CheckpointImportOutcome::Absent => {}
            }

            if let Some(repo) = repo_handle.as_ref() {
                match self.import_checkpoint_from_git(repo, &group) {
                    CheckpointImportOutcome::Imported(import) => {
                        if selected_import.is_none()
                            && self
                                .ensure_checkpoint_hashes_compatible(
                                    store_id,
                                    &import,
                                    local_policy_hash,
                                    local_roster_hash,
                                )
                                .is_ok()
                        {
                            selected_import = Some(import);
                        }
                    }
                    CheckpointImportOutcome::Incompatible => incompatible = true,
                    CheckpointImportOutcome::Absent => {}
                }
            }

            if let Some(import) = selected_import {
                imports.push(import);
            }
            // Any legacy-incompatible source for the group should be rebuilt so
            // future loads do not keep tripping over stale checkpoint artifacts,
            // even if another source was usable for this load.
            if incompatible {
                incompatible_groups.push(group);
            }
        }

        LoadedCheckpointImports {
            imports,
            incompatible_groups,
        }
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

    fn ensure_checkpoint_hashes_compatible(
        &self,
        store_id: StoreId,
        import: &CheckpointImport,
        local_policy_hash: Option<ContentHash>,
        local_roster_hash: Option<ContentHash>,
    ) -> Result<(), CheckpointHashMismatch> {
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
                "checkpoint policy hash mismatch; skipping checkpoint import"
            );
            return Err(CheckpointHashMismatch::Policy);
        }

        if import.roster_hash.is_some() && import.roster_hash != local_roster_hash {
            let checkpoint_roster_hash = import.roster_hash.map(|hash| hash.to_hex());
            let local_roster_hash = local_roster_hash.map(|hash| hash.to_hex());
            tracing::warn!(
                store_id = %store_id,
                checkpoint_group = %import.checkpoint_group,
                local_roster_hash = ?local_roster_hash,
                checkpoint_roster_hash = ?checkpoint_roster_hash,
                "checkpoint roster hash mismatch; skipping checkpoint import"
            );
            return Err(CheckpointHashMismatch::Roster);
        }
        Ok(())
    }

    fn import_checkpoint_from_cache(
        &self,
        store_id: StoreId,
        group: &CheckpointGroupSnapshot,
    ) -> CheckpointImportOutcome {
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
                    CheckpointImportOutcome::Imported(import)
                }
                Err(CheckpointImportError::IncompatibleDepsFormat { .. }) => {
                    tracing::warn!(
                        store_id = %store_id,
                        checkpoint_group = %group.group,
                        "checkpoint cache import incompatible deps format; ignoring and rebuilding from canonical store"
                    );
                    CheckpointImportOutcome::Incompatible
                }
                Err(err) => {
                    tracing::warn!(
                        store_id = %store_id,
                        checkpoint_group = %group.group,
                        error = ?err,
                        "checkpoint cache import failed"
                    );
                    CheckpointImportOutcome::Absent
                }
            },
            Ok(None) => CheckpointImportOutcome::Absent,
            Err(err) => {
                tracing::warn!(
                    store_id = %store_id,
                    checkpoint_group = %group.group,
                    error = ?err,
                    "checkpoint cache load failed"
                );
                CheckpointImportOutcome::Absent
            }
        }
    }

    fn import_checkpoint_from_git(
        &self,
        repo: &Repository,
        group: &CheckpointGroupSnapshot,
    ) -> CheckpointImportOutcome {
        let oid = match checkpoint_ref_oid(repo, &group.git_ref) {
            Ok(Some(oid)) => oid,
            Ok(None) => return CheckpointImportOutcome::Absent,
            Err(err) => {
                tracing::warn!(
                    checkpoint_group = %group.group,
                    git_ref = %group.git_ref,
                    error = ?err,
                    "checkpoint ref lookup failed"
                );
                return CheckpointImportOutcome::Absent;
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
                return CheckpointImportOutcome::Absent;
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
                return CheckpointImportOutcome::Absent;
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
                return CheckpointImportOutcome::Absent;
            }
        };

        if let Err(err) = write_checkpoint_tree(repo, &tree, temp.path()) {
            tracing::warn!(
                checkpoint_group = %group.group,
                git_ref = %group.git_ref,
                error = ?err,
                "checkpoint tree export failed"
            );
            return CheckpointImportOutcome::Absent;
        }

        match import_checkpoint(temp.path(), self.limits()) {
            Ok(import) => {
                tracing::info!(
                    checkpoint_group = %group.group,
                    git_ref = %group.git_ref,
                    "checkpoint git import succeeded"
                );
                CheckpointImportOutcome::Imported(import)
            }
            Err(CheckpointImportError::IncompatibleDepsFormat { .. }) => {
                tracing::warn!(
                    checkpoint_group = %group.group,
                    git_ref = %group.git_ref,
                    "checkpoint git import incompatible deps format; skipping legacy checkpoint"
                );
                CheckpointImportOutcome::Incompatible
            }
            Err(err) => {
                tracing::warn!(
                    checkpoint_group = %group.group,
                    git_ref = %group.git_ref,
                    error = ?err,
                    "checkpoint git import failed"
                );
                CheckpointImportOutcome::Absent
            }
        }
    }
}
