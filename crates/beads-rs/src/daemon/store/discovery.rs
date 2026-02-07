use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use git2::{ErrorCode as GitErrorCode, Repository};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::core::StoreId;
use crate::daemon::ops::OpError;
use beads_daemon::remote::RemoteUrl;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreIdVerification {
    Verified,
    Unverified,
}

crate::enum_str! {
    impl StoreIdVerification {
        fn as_str(&self) -> &'static str;
        fn parse_str(raw: &str) -> Option<Self>;
        variants {
            Verified => ["verified"],
            Unverified => ["unverified"],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreIdResolution {
    pub store_id: StoreId,
    pub source: StoreIdSource,
    pub verified: StoreIdVerification,
}

impl StoreIdResolution {
    pub fn verified(store_id: StoreId, source: StoreIdSource) -> Self {
        Self {
            store_id,
            source,
            verified: StoreIdVerification::Verified,
        }
    }

    pub fn unverified(store_id: StoreId, source: StoreIdSource) -> Self {
        Self {
            store_id,
            source,
            verified: StoreIdVerification::Unverified,
        }
    }

    pub fn is_verified(&self) -> bool {
        matches!(self.verified, StoreIdVerification::Verified)
    }

    pub fn should_persist(&self) -> bool {
        self.is_verified()
    }
}

#[derive(Clone, Debug, Default)]
pub struct StoreCaches {
    pub path_to_store: HashMap<PathBuf, StoreIdResolution>,
    pub remote_to_store: HashMap<RemoteUrl, StoreIdResolution>,
    pub path_to_remote: HashMap<PathBuf, RemoteUrl>,
}

impl StoreCaches {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn resolve_store(&mut self, repo_path: &Path) -> Result<ResolvedStore, OpError> {
        let remote = self.resolve_remote(repo_path)?;
        let resolution = self.resolve_store_id(repo_path, &remote)?;

        self.path_to_store.insert(repo_path.to_owned(), resolution);
        self.remote_to_store.insert(remote.clone(), resolution);

        if resolution.should_persist()
            && let Err(err) = persist_store_id_mapping(repo_path, resolution.store_id)
        {
            tracing::warn!(
                "failed to persist store id mapping for {:?}: {}",
                repo_path,
                err
            );
        }

        tracing::info!(
            store_id = %resolution.store_id,
            source = %resolution.source.as_str(),
            verified = %resolution.verified.as_str(),
            repo = %repo_path.display(),
            remote = %remote,
            "store identity resolved"
        );

        Ok(ResolvedStore { resolution, remote })
    }

    pub fn resolve_remote(&mut self, repo_path: &Path) -> Result<RemoteUrl, OpError> {
        if let Ok(url) = std::env::var("BD_REMOTE_URL")
            && !url.trim().is_empty()
        {
            return Ok(RemoteUrl::new(&url));
        }

        if let Some(remote) = self.path_to_remote.get(repo_path) {
            return Ok(remote.clone());
        }

        let repo =
            Repository::open(repo_path).map_err(|_| OpError::NotAGitRepo(repo_path.to_owned()))?;
        let remote = repo
            .find_remote("origin")
            .map_err(|_| OpError::NoRemote(repo_path.to_owned()))?;
        let url = remote
            .url()
            .ok_or_else(|| OpError::NoRemote(repo_path.to_owned()))?;

        let remote = RemoteUrl::new(url);
        self.path_to_remote
            .insert(repo_path.to_owned(), remote.clone());
        Ok(remote)
    }

    fn resolve_store_id(
        &mut self,
        repo_path: &Path,
        remote: &RemoteUrl,
    ) -> Result<StoreIdResolution, OpError> {
        if let Ok(raw) = std::env::var("BD_STORE_ID")
            && !raw.trim().is_empty()
        {
            let store_id = StoreId::parse_str(raw.trim()).map_err(|e| OpError::InvalidRequest {
                field: Some("store_id".into()),
                reason: e.to_string(),
            })?;
            return Ok(StoreIdResolution::verified(
                store_id,
                StoreIdSource::EnvOverride,
            ));
        }

        let cached = self.path_to_store.get(repo_path).copied();
        let mapped = load_store_id_for_path(repo_path)
            .map(|store_id| StoreIdResolution::unverified(store_id, StoreIdSource::PathMap));

        if let Some(resolution) = cached.or(mapped) {
            if let Ok(repo) = Repository::open(repo_path)
                && let Some(resolution) = resolve_verified_store_id(&repo)?
            {
                return Ok(resolution);
            }
            return Ok(resolution);
        }

        let repo =
            Repository::open(repo_path).map_err(|_| OpError::NotAGitRepo(repo_path.to_owned()))?;

        if let Some(resolution) = resolve_verified_store_id(&repo)? {
            return Ok(resolution);
        }

        Ok(StoreIdResolution::unverified(
            store_id_from_remote(remote),
            StoreIdSource::RemoteFallback,
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreIdSource {
    EnvOverride,
    PathMap,
    GitMeta,
    GitRefs,
    RemoteFallback,
}

crate::enum_str! {
    impl StoreIdSource {
        fn as_str(&self) -> &'static str;
        fn parse_str(raw: &str) -> Option<Self>;
        variants {
            EnvOverride => ["env_override"],
            PathMap => ["path_map"],
            GitMeta => ["git_meta"],
            GitRefs => ["git_refs"],
            RemoteFallback => ["remote_fallback"],
        }
    }
}

#[derive(Clone, Debug)]
pub struct ResolvedStore {
    pub resolution: StoreIdResolution,
    pub remote: RemoteUrl,
}

impl ResolvedStore {
    pub fn store_id(&self) -> StoreId {
        self.resolution.store_id
    }
}

#[derive(Debug, Error)]
enum StoreDiscoveryError {
    #[error("failed to read meta ref: {source}")]
    MetaRef { source: git2::Error },
    #[error("store_meta.json missing from meta ref")]
    MetaMissing,
    #[error("failed to parse store_meta.json: {source}")]
    MetaParse {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to list refs: {source}")]
    RefList { source: git2::Error },
    #[error("multiple store ids found in refs: {ids:?}")]
    MultipleStoreIds { ids: Vec<StoreId> },
}

#[derive(Debug, Serialize, Deserialize)]
struct StoreMetaRef {
    store_id: StoreId,
    #[allow(dead_code)]
    store_epoch: crate::core::StoreEpoch,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StorePathMap {
    entries: BTreeMap<String, StoreId>,
}

fn store_path_map_path() -> PathBuf {
    crate::paths::data_dir().join("store_paths.json")
}

fn load_store_id_for_path(repo_path: &Path) -> Option<StoreId> {
    let path = store_path_map_path();
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) => {
            if err.kind() != io::ErrorKind::NotFound {
                tracing::warn!("store path map read failed {:?}: {}", path, err);
            }
            return None;
        }
    };

    let map: StorePathMap = match serde_json::from_slice(&bytes) {
        Ok(map) => map,
        Err(err) => {
            tracing::warn!("store path map parse failed {:?}: {}", path, err);
            return None;
        }
    };

    let key = repo_path.display().to_string();
    map.entries.get(&key).copied()
}

fn persist_store_id_mapping(repo_path: &Path, store_id: StoreId) -> io::Result<()> {
    let path = store_path_map_path();
    let mut map = read_store_path_map();
    let key = repo_path.display().to_string();
    map.entries.insert(key, store_id);
    write_store_path_map(&path, &map)
}

fn read_store_path_map() -> StorePathMap {
    let path = store_path_map_path();
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return StorePathMap::default(),
        Err(err) => {
            tracing::warn!("store path map read failed {:?}: {}", path, err);
            return StorePathMap::default();
        }
    };

    match serde_json::from_slice(&bytes) {
        Ok(map) => map,
        Err(err) => {
            tracing::warn!("store path map parse failed {:?}: {}", path, err);
            StorePathMap::default()
        }
    }
}

fn write_store_path_map(path: &Path, map: &StorePathMap) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let bytes =
        serde_json::to_vec(map).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    fs::write(path, bytes)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }

    Ok(())
}

fn read_store_id_from_git_meta(repo: &Repository) -> Result<Option<StoreId>, StoreDiscoveryError> {
    let reference = match repo.find_reference("refs/beads/meta") {
        Ok(reference) => reference,
        Err(err) if err.code() == GitErrorCode::NotFound => return Ok(None),
        Err(err) => return Err(StoreDiscoveryError::MetaRef { source: err }),
    };

    let commit = reference
        .peel_to_commit()
        .map_err(|err| StoreDiscoveryError::MetaRef { source: err })?;
    let tree = commit
        .tree()
        .map_err(|err| StoreDiscoveryError::MetaRef { source: err })?;
    let entry = tree
        .get_name("store_meta.json")
        .ok_or(StoreDiscoveryError::MetaMissing)?;
    let blob = repo
        .find_blob(entry.id())
        .map_err(|err| StoreDiscoveryError::MetaRef { source: err })?;
    let meta: StoreMetaRef = serde_json::from_slice(blob.content())
        .map_err(|source| StoreDiscoveryError::MetaParse { source })?;
    Ok(Some(meta.store_id))
}

fn discover_store_id_from_refs(repo: &Repository) -> Result<Option<StoreId>, StoreDiscoveryError> {
    let mut ids = BTreeSet::new();
    let refs = repo
        .references_glob("refs/beads/*")
        .map_err(|source| StoreDiscoveryError::RefList { source })?;

    for reference in refs {
        let reference = reference.map_err(|source| StoreDiscoveryError::RefList { source })?;
        let Some(name) = reference.name() else {
            continue;
        };
        if name == "refs/beads/meta" {
            continue;
        }
        let Some(rest) = name.strip_prefix("refs/beads/") else {
            continue;
        };
        let store_id_raw = rest.split('/').next().unwrap_or_default();
        if store_id_raw.is_empty() {
            continue;
        }
        match StoreId::parse_str(store_id_raw) {
            Ok(id) => {
                ids.insert(id);
            }
            Err(_) => {
                tracing::warn!("ignoring invalid store id ref {}", name);
            }
        }
    }

    if ids.is_empty() {
        return Ok(None);
    }
    if ids.len() > 1 {
        return Err(StoreDiscoveryError::MultipleStoreIds {
            ids: ids.into_iter().collect(),
        });
    }
    Ok(ids.into_iter().next())
}

fn resolve_verified_store_id(repo: &Repository) -> Result<Option<StoreIdResolution>, OpError> {
    let meta = read_store_id_from_git_meta(repo).map_err(|err| OpError::InvalidRequest {
        field: Some("store_id".into()),
        reason: err.to_string(),
    })?;
    let refs = discover_store_id_from_refs(repo).map_err(|err| OpError::InvalidRequest {
        field: Some("store_id".into()),
        reason: err.to_string(),
    })?;

    if let (Some(meta_id), Some(refs_id)) = (meta, refs) {
        if meta_id != refs_id {
            return Err(OpError::StoreIdMismatch {
                meta: meta_id,
                refs: refs_id,
            });
        }
        return Ok(Some(StoreIdResolution::verified(
            meta_id,
            StoreIdSource::GitMeta,
        )));
    }

    if let Some(meta_id) = meta {
        return Ok(Some(StoreIdResolution::verified(
            meta_id,
            StoreIdSource::GitMeta,
        )));
    }

    if let Some(refs_id) = refs {
        return Ok(Some(StoreIdResolution::verified(
            refs_id,
            StoreIdSource::GitRefs,
        )));
    }

    Ok(None)
}

pub(crate) fn store_id_from_remote(remote: &RemoteUrl) -> StoreId {
    let store_uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, remote.as_str().as_bytes());
    StoreId::new(store_uuid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::StoreEpoch;
    use git2::{Repository, Signature};
    use tempfile::TempDir;

    fn init_repo() -> (TempDir, Repository) {
        let dir = TempDir::new().unwrap();
        let repo = Repository::init(dir.path()).unwrap();
        (dir, repo)
    }

    fn write_store_meta(repo: &Repository, store_id: StoreId) {
        let meta = StoreMetaRef {
            store_id,
            store_epoch: StoreEpoch::ZERO,
        };
        let bytes = serde_json::to_vec(&meta).unwrap();
        let blob_id = repo.blob(&bytes).unwrap();
        let mut builder = repo.treebuilder(None).unwrap();
        builder
            .insert("store_meta.json", blob_id, 0o100644)
            .unwrap();
        let tree_id = builder.write().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let sig = Signature::now("test", "test@example.com").unwrap();
        let commit_id = repo
            .commit(Some("HEAD"), &sig, &sig, "meta", &tree, &[])
            .unwrap();
        repo.reference("refs/beads/meta", commit_id, true, "meta")
            .unwrap();
    }

    fn write_store_ref(repo: &Repository, store_id: StoreId) {
        let head = repo.head().unwrap().target().unwrap();
        let name = format!("refs/beads/{store_id}/head");
        repo.reference(&name, head, true, "store ref").unwrap();
    }

    #[test]
    fn resolve_store_id_reports_mismatched_meta_and_refs() {
        assert!(std::env::var("BD_STORE_ID").is_err());
        assert!(std::env::var("BD_REMOTE_URL").is_err());
        let (dir, repo) = init_repo();

        let meta_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let refs_id = StoreId::new(Uuid::from_bytes([2u8; 16]));
        write_store_meta(&repo, meta_id);
        write_store_ref(&repo, refs_id);

        let remote = RemoteUrl::new("https://example.com/repo.git");
        let mut caches = StoreCaches::new();
        let err = caches.resolve_store_id(dir.path(), &remote).unwrap_err();

        assert!(matches!(
            err,
            OpError::StoreIdMismatch { meta, refs }
                if meta == meta_id && refs == refs_id
        ));
    }

    #[test]
    fn remote_fallback_is_unverified_and_not_persisted() {
        assert!(std::env::var("BD_STORE_ID").is_err());
        assert!(std::env::var("BD_REMOTE_URL").is_err());
        let (dir, repo) = init_repo();
        repo.remote("origin", "https://example.com/repo.git")
            .unwrap();

        let data_dir = TempDir::new().unwrap();
        let _override = crate::paths::override_data_dir_for_tests(Some(data_dir.path().into()));

        let mut caches = StoreCaches::new();
        let resolved = caches.resolve_store(dir.path()).unwrap();

        assert_eq!(resolved.resolution.source, StoreIdSource::RemoteFallback);
        assert_eq!(
            resolved.resolution.verified,
            StoreIdVerification::Unverified
        );
        assert_eq!(
            resolved.resolution.store_id,
            store_id_from_remote(&resolved.remote)
        );
        assert!(!store_path_map_path().exists());
    }
}
