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
use crate::daemon::remote::{RemoteUrl, normalize_url};

#[derive(Clone, Debug, Default)]
pub struct StoreCaches {
    pub path_to_store_id: HashMap<PathBuf, StoreId>,
    pub remote_to_store_id: HashMap<RemoteUrl, StoreId>,
    pub path_to_remote: HashMap<PathBuf, RemoteUrl>,
}

impl StoreCaches {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn resolve_store(&mut self, repo_path: &Path) -> Result<ResolvedStore, OpError> {
        let remote = self.resolve_remote(repo_path)?;
        let (store_id, source) = self.resolve_store_id(repo_path, &remote)?;

        self.path_to_store_id.insert(repo_path.to_owned(), store_id);
        self.remote_to_store_id.insert(remote.clone(), store_id);

        if let Err(err) = persist_store_id_mapping(repo_path, store_id) {
            tracing::warn!(
                "failed to persist store id mapping for {:?}: {}",
                repo_path,
                err
            );
        }

        tracing::info!(
            store_id = %store_id,
            source = %source.as_str(),
            repo = %repo_path.display(),
            remote = %remote,
            "store identity resolved"
        );

        Ok(ResolvedStore { store_id, remote })
    }

    pub fn resolve_remote(&mut self, repo_path: &Path) -> Result<RemoteUrl, OpError> {
        if let Ok(url) = std::env::var("BD_REMOTE_URL")
            && !url.trim().is_empty()
        {
            return Ok(RemoteUrl(normalize_url(&url)));
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

        let remote = RemoteUrl(normalize_url(url));
        self.path_to_remote
            .insert(repo_path.to_owned(), remote.clone());
        Ok(remote)
    }

    fn resolve_store_id(
        &mut self,
        repo_path: &Path,
        remote: &RemoteUrl,
    ) -> Result<(StoreId, StoreIdSource), OpError> {
        if let Ok(raw) = std::env::var("BD_STORE_ID")
            && !raw.trim().is_empty()
        {
            let store_id = StoreId::parse_str(raw.trim()).map_err(|e| OpError::InvalidRequest {
                field: Some("store_id".into()),
                reason: e.to_string(),
            })?;
            return Ok((store_id, StoreIdSource::EnvOverride));
        }

        if let Some(store_id) = self.path_to_store_id.get(repo_path).copied() {
            return Ok((store_id, StoreIdSource::PathCache));
        }

        if let Some(store_id) = load_store_id_for_path(repo_path) {
            return Ok((store_id, StoreIdSource::PathMap));
        }

        let repo =
            Repository::open(repo_path).map_err(|_| OpError::NotAGitRepo(repo_path.to_owned()))?;

        if let Some(store_id) =
            read_store_id_from_git_meta(&repo).map_err(|err| OpError::InvalidRequest {
                field: Some("store_id".into()),
                reason: err.to_string(),
            })?
        {
            return Ok((store_id, StoreIdSource::GitMeta));
        }

        if let Some(store_id) =
            discover_store_id_from_refs(&repo).map_err(|err| OpError::InvalidRequest {
                field: Some("store_id".into()),
                reason: err.to_string(),
            })?
        {
            return Ok((store_id, StoreIdSource::GitRefs));
        }

        Ok((store_id_from_remote(remote), StoreIdSource::RemoteFallback))
    }
}

#[derive(Clone, Copy, Debug)]
enum StoreIdSource {
    EnvOverride,
    PathCache,
    PathMap,
    GitMeta,
    GitRefs,
    RemoteFallback,
}

impl StoreIdSource {
    fn as_str(self) -> &'static str {
        match self {
            StoreIdSource::EnvOverride => "env_override",
            StoreIdSource::PathCache => "path_cache",
            StoreIdSource::PathMap => "path_map",
            StoreIdSource::GitMeta => "git_meta",
            StoreIdSource::GitRefs => "git_refs",
            StoreIdSource::RemoteFallback => "remote_fallback",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ResolvedStore {
    pub store_id: StoreId,
    pub remote: RemoteUrl,
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

#[derive(Debug, Deserialize)]
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

pub(crate) fn store_id_from_remote(remote: &RemoteUrl) -> StoreId {
    let store_uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, remote.as_str().as_bytes());
    StoreId::new(store_uuid)
}
