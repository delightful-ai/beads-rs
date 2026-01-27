//! Migration helpers for legacy data sources.

use git2::{ErrorCode, Oid, Repository};
use thiserror::Error;

use crate::core::{StoreState, WriteStamp};
use crate::daemon::remote::RemoteUrl;
use crate::daemon::wal_legacy_snapshot::{Wal, WalEntry, WalError};
use crate::git::checkpoint::store_state_from_legacy;
use crate::git::error::SyncError;
use crate::git::sync::read_state_at_oid;

const LEGACY_REF: &str = "refs/heads/beads/store";

#[derive(Debug, Clone)]
pub struct LegacyGitImport {
    pub state: StoreState,
    pub root_slug: Option<String>,
    pub last_write_stamp: Option<WriteStamp>,
}

#[derive(Debug, Clone)]
pub struct LegacyWalImport {
    pub state: StoreState,
    pub root_slug: Option<String>,
    pub sequence: u64,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error(transparent)]
    WalSnapshot(#[from] WalError),
    #[error(transparent)]
    Sync(#[from] SyncError),
}

/// Read legacy beads/store state into the core namespace without mutating the ref.
pub fn import_legacy_git_ref(repo: &Repository) -> Result<Option<LegacyGitImport>, MigrationError> {
    let Some(oid) = legacy_ref_oid(repo)? else {
        return Ok(None);
    };
    let loaded = read_state_at_oid(repo, oid)?;
    Ok(Some(LegacyGitImport {
        state: store_state_from_legacy(loaded.state),
        root_slug: loaded.root_slug,
        last_write_stamp: loaded.last_write_stamp,
    }))
}

/// Read legacy snapshot WAL (if present) into the core namespace.
pub fn import_legacy_snapshot_wal(
    wal: &Wal,
    remote: &RemoteUrl,
) -> Result<Option<LegacyWalImport>, MigrationError> {
    let Some(entry) = wal.read(remote)? else {
        return Ok(None);
    };
    let WalEntry {
        state,
        root_slug,
        sequence,
        ..
    } = entry;
    Ok(Some(LegacyWalImport {
        state: store_state_from_legacy(state),
        root_slug,
        sequence,
    }))
}

fn legacy_ref_oid(repo: &Repository) -> Result<Option<Oid>, SyncError> {
    match repo.refname_to_id(LEGACY_REF) {
        Ok(oid) => Ok(Some(oid)),
        Err(err) if err.code() == ErrorCode::NotFound => Ok(None),
        Err(err) => Err(SyncError::Git(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use git2::Signature;
    use tempfile::TempDir;

    use crate::core::identity::BeadId;
    use crate::core::time::WriteStamp;
    use crate::core::{
        ActorId, Bead, BeadCore, BeadFields, BeadType, Claim, Lww, Priority, Stamp, Workflow,
    };
    use crate::git::wire;

    fn make_bead(id: &str) -> Bead {
        let write_stamp = WriteStamp::new(1, 0);
        let actor = ActorId::new("author").unwrap();
        let stamp = Stamp::new(write_stamp.clone(), actor.clone());
        let core = BeadCore::new(BeadId::parse(id).unwrap(), stamp.clone(), None);
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
        Bead::new(core, fields)
    }

    fn write_legacy_commit(repo: &Repository, root_slug: &str, stamp: WriteStamp) -> Oid {
        let mut state = crate::core::CanonicalState::new();
        state.insert(make_bead("bd-legacy")).unwrap();

        let state_bytes = wire::serialize_state(&state).unwrap();
        let tombs_bytes = wire::serialize_tombstones(&state).unwrap();
        let deps_bytes = wire::serialize_deps(&state).unwrap();
        let notes_bytes = wire::serialize_notes(&state).unwrap();
        let checksums = wire::StoreChecksums::from_bytes(
            &state_bytes,
            &tombs_bytes,
            &deps_bytes,
            Some(&notes_bytes),
        );
        let meta_bytes = wire::serialize_meta(Some(root_slug), Some(&stamp), &checksums).unwrap();

        let state_oid = repo.blob(&state_bytes).unwrap();
        let tombs_oid = repo.blob(&tombs_bytes).unwrap();
        let deps_oid = repo.blob(&deps_bytes).unwrap();
        let notes_oid = repo.blob(&notes_bytes).unwrap();
        let meta_oid = repo.blob(&meta_bytes).unwrap();

        let mut builder = repo.treebuilder(None).unwrap();
        builder.insert("state.jsonl", state_oid, 0o100644).unwrap();
        builder
            .insert("tombstones.jsonl", tombs_oid, 0o100644)
            .unwrap();
        builder.insert("deps.jsonl", deps_oid, 0o100644).unwrap();
        builder.insert("notes.jsonl", notes_oid, 0o100644).unwrap();
        builder.insert("meta.json", meta_oid, 0o100644).unwrap();
        let tree_oid = builder.write().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();

        let sig = Signature::now("test", "test@example.com").unwrap();
        let commit_oid = repo.commit(None, &sig, &sig, "legacy", &tree, &[]).unwrap();
        repo.reference(LEGACY_REF, commit_oid, true, "legacy ref")
            .unwrap();
        commit_oid
    }

    fn test_remote() -> RemoteUrl {
        RemoteUrl("git@github.com:test/repo.git".into())
    }

    #[test]
    fn import_legacy_git_ref_reads_core_state() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let root_slug = "legacy-root";
        let stamp = WriteStamp::new(42, 7);
        write_legacy_commit(&repo, root_slug, stamp.clone());

        let import = import_legacy_git_ref(&repo)
            .unwrap()
            .expect("legacy import");
        let core_state = import.state.core();
        assert_eq!(core_state.live_count(), 1);
        assert_eq!(import.root_slug.as_deref(), Some(root_slug));
        assert_eq!(import.last_write_stamp, Some(stamp));
    }

    #[test]
    fn import_legacy_git_ref_none_when_missing() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        assert!(import_legacy_git_ref(&repo).unwrap().is_none());
    }

    #[test]
    fn import_legacy_snapshot_wal_reads_core_state() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::new(tmp.path()).unwrap();
        let remote = test_remote();

        let mut state = crate::core::CanonicalState::new();
        state.insert(make_bead("bd-legacy-wal")).unwrap();
        let entry = WalEntry::new(state, Some("wal-root".to_string()), 7, 123);
        wal.write(&remote, &entry).unwrap();

        let import = import_legacy_snapshot_wal(&wal, &remote)
            .unwrap()
            .expect("legacy wal import");
        let core_state = import.state.core();
        assert_eq!(core_state.live_count(), 1);
        assert_eq!(import.root_slug.as_deref(), Some("wal-root"));
        assert_eq!(import.sequence, 7);
    }

    #[test]
    fn import_legacy_snapshot_wal_none_when_missing() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::new(tmp.path()).unwrap();
        let remote = test_remote();
        assert!(import_legacy_snapshot_wal(&wal, &remote).unwrap().is_none());
    }
}
