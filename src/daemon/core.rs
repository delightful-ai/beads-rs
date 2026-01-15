//! Daemon core - the central coordinator.
//!
//! Owns all per-repo state, the HLC clock, actor identity, and sync scheduler.
//! The serialization point for all mutations - runs on a single thread.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{self, Seek, SeekFrom};
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crossbeam::channel::Sender;
use git2::{ErrorCode as GitErrorCode, Repository};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use super::Clock;
use super::broadcast::BroadcastEvent;
use super::checkpoint_scheduler::{
    CheckpointGroupConfig, CheckpointGroupKey, CheckpointGroupSnapshot, CheckpointScheduler,
};
use super::executor::DurabilityWait;
use super::git_worker::{GitOp, LoadResult};
use super::ipc::{
    ErrorPayload, IpcError, MutationMeta, ReadConsistency, Request, Response, ResponsePayload,
};
use super::metrics;
use super::migration::MigrationError;
use super::ops::OpError;
use super::query::QueryResult;
use super::remote::{RemoteUrl, normalize_url};
use super::repl::{
    BackoffPolicy, IngestOutcome, ReplIngestRequest, ReplSessionStore, ReplicationManager,
    ReplicationManagerConfig, ReplicationManagerHandle, ReplicationServer, ReplicationServerConfig,
    ReplicationServerHandle, SharedSessionStore, WalRangeReader,
};
use super::repo::{
    ClockSkewRecord, DivergenceRecord, FetchErrorRecord, ForcePushRecord, RepoState,
};
use super::scheduler::SyncScheduler;
use super::store_runtime::{StoreRuntime, StoreRuntimeError};
use super::wal::{
    EventWalError, FrameReader, HlcRow, Record, RecordHeader, SegmentRow, Wal, WalEntry, WalIndex,
    WalIndexError, WalReplayError,
};

use crate::compat::{ExportContext, ensure_symlinks, export_jsonl};
use crate::core::error::details as error_details;
use crate::paths;

/// Proof that a repo is loaded. Only created by `Daemon::ensure_repo_loaded`,
/// `Daemon::ensure_repo_loaded_strict`, or `Daemon::ensure_repo_fresh`.
///
/// This type signals that a repo should exist in the daemon's state; accessors
/// still return Internal if the invariant is violated.
#[derive(Debug, Clone)]
pub struct LoadedStore {
    store_id: StoreId,
    remote: RemoteUrl,
}

impl LoadedStore {
    /// Get the store identity.
    pub fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Get the legacy remote URL.
    pub fn remote(&self) -> &RemoteUrl {
        &self.remote
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone)]
struct ResolvedStore {
    store_id: StoreId,
    remote: RemoteUrl,
}

#[derive(Debug, Error)]
enum StoreDiscoveryError {
    #[error("store_meta.json missing in refs/beads/meta")]
    MetaMissing,
    #[error("store_meta.json parse failed: {source}")]
    MetaParse {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to read refs/beads/meta: {source}")]
    MetaRef {
        #[source]
        source: git2::Error,
    },
    #[error("failed to list refs/beads/*: {source}")]
    RefList {
        #[source]
        source: git2::Error,
    },
    #[error("multiple store ids discovered: {ids:?}")]
    MultipleStoreIds { ids: Vec<StoreId> },
}

#[derive(Debug, Deserialize)]
struct StoreMetaRef {
    store_id: StoreId,
    #[allow(dead_code)]
    store_epoch: StoreEpoch,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StorePathMap {
    entries: BTreeMap<String, StoreId>,
}
use crate::api::DaemonInfo as ApiDaemonInfo;
use crate::core::{
    ActorId, Applied, ApplyError, BeadId, Canonical, CanonicalState, ClientRequestId, CoreError,
    DurabilityClass, ErrorCode, EventBody, EventBytes, EventId, Limits, NamespaceId, PrevVerified,
    ReplicaId, ReplicaRoster, ReplicateMode, SegmentId, Seq1, Sha256, Stamp, StoreEpoch, StoreId,
    StoreIdentity, StoreState, VerifiedEvent, WallClock, Watermark, WatermarkError, Watermarks,
    WriteStamp, apply_event, decode_event_body,
};
use crate::git::SyncError;
use crate::git::checkpoint::{
    CheckpointPublishError, CheckpointPublishOutcome, merge_store_states, store_state_from_legacy,
};
use crate::git::collision::{detect_collisions, resolve_collisions};
use crate::git::sync::SyncOutcome;

const REFRESH_TTL: Duration = Duration::from_millis(1000);
const LOAD_TIMEOUT_SECS: u64 = 30;
const DEFAULT_REPL_MAX_CONNECTIONS: usize = 32;

#[derive(Clone, Debug)]
pub(crate) struct NormalizedMutationMeta {
    pub(crate) namespace: NamespaceId,
    pub(crate) durability: DurabilityClass,
    pub(crate) client_request_id: Option<ClientRequestId>,
    pub(crate) actor_id: ActorId,
}

#[derive(Clone, Debug)]
pub(crate) struct NormalizedReadConsistency {
    namespace: NamespaceId,
    require_min_seen: Option<Watermarks<Applied>>,
    wait_timeout_ms: u64,
}

impl NormalizedReadConsistency {
    pub(crate) fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    pub(crate) fn require_min_seen(&self) -> Option<&Watermarks<Applied>> {
        self.require_min_seen.as_ref()
    }

    pub(crate) fn wait_timeout_ms(&self) -> u64 {
        self.wait_timeout_ms
    }
}

#[derive(Clone, Debug)]
pub(crate) enum ReadGateStatus {
    Satisfied,
    Unsatisfied {
        required: Watermarks<Applied>,
        current_applied: Watermarks<Applied>,
    },
}

#[derive(Debug)]
pub(crate) enum HandleOutcome {
    Response(Response),
    DurabilityWait(DurabilityWait),
}

impl From<Response> for HandleOutcome {
    fn from(response: Response) -> Self {
        HandleOutcome::Response(response)
    }
}

/// The daemon coordinator.
///
/// Owns all state and coordinates between IPC, state mutations, and git sync.
pub struct Daemon {
    /// Per-store runtime, keyed by StoreId.
    stores: BTreeMap<StoreId, StoreRuntime>,

    /// Cache of repo path → store id.
    path_to_store_id: HashMap<PathBuf, StoreId>,

    /// Cache of remote URL → store id.
    remote_to_store_id: HashMap<RemoteUrl, StoreId>,

    /// Cache of repo path → remote URL (legacy).
    path_to_remote: HashMap<PathBuf, RemoteUrl>,

    /// HLC clock for generating timestamps.
    clock: Clock,

    /// Actor identity (username@hostname).
    actor: ActorId,

    /// Sync scheduler for debouncing.
    scheduler: SyncScheduler,
    /// Checkpoint scheduler for debounce/max interval.
    checkpoint_scheduler: CheckpointScheduler,

    /// Write-ahead log for mutation durability.
    wal: Arc<Wal>,

    /// Go-compatibility export context.
    export_ctx: Option<ExportContext>,

    /// Realtime safety limits.
    limits: Limits,

    /// Replication ingest channel (set by run_state_loop).
    repl_ingest_tx: Option<Sender<ReplIngestRequest>>,

    /// Replication runtime handles per store.
    repl_handles: BTreeMap<StoreId, ReplicationHandles>,

    /// Shutdown gate to stop accepting new mutations.
    shutting_down: bool,
}

struct ReplicationHandles {
    manager: Option<ReplicationManagerHandle>,
    server: Option<ReplicationServerHandle>,
}

impl ReplicationHandles {
    fn shutdown(self) {
        if let Some(handle) = self.manager {
            handle.shutdown();
        }
        if let Some(handle) = self.server {
            handle.shutdown();
        }
    }
}

impl Daemon {
    /// Create a new daemon.
    pub fn new(actor: ActorId, wal: Wal) -> Self {
        Self::new_with_limits(actor, wal, Limits::default())
    }

    /// Create a new daemon with custom limits.
    pub fn new_with_limits(actor: ActorId, wal: Wal, limits: Limits) -> Self {
        // Initialize Go-compat export context (best effort - don't fail daemon startup)
        let export_ctx = match ExportContext::new() {
            Ok(ctx) => Some(ctx),
            Err(e) => {
                tracing::warn!("Failed to initialize Go-compat export: {}", e);
                None
            }
        };

        Daemon {
            stores: BTreeMap::new(),
            path_to_store_id: HashMap::new(),
            remote_to_store_id: HashMap::new(),
            path_to_remote: HashMap::new(),
            clock: Clock::new_with_max_forward_drift(limits.hlc_max_forward_drift_ms),
            actor,
            scheduler: SyncScheduler::new(),
            checkpoint_scheduler: CheckpointScheduler::new(),
            wal: Arc::new(wal),
            export_ctx,
            limits,
            repl_ingest_tx: None,
            repl_handles: BTreeMap::new(),
            shutting_down: false,
        }
    }

    /// Get a reference to the WAL.
    pub fn wal(&self) -> &Wal {
        &self.wal
    }

    /// Get the actor identity.
    pub fn actor(&self) -> &ActorId {
        &self.actor
    }

    /// Get the clock (for creating stamps).
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Get the safety limits.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    pub fn begin_shutdown(&mut self) {
        self.shutting_down = true;
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down
    }

    pub fn drain_ipc_inflight(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.ipc_inflight_total() == 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        tracing::warn!("shutdown timed out waiting for inflight mutations");
    }

    pub(crate) fn set_repl_ingest_tx(&mut self, tx: Sender<ReplIngestRequest>) {
        self.repl_ingest_tx = Some(tx);
    }

    /// Get mutable clock.
    pub fn clock_mut(&mut self) -> &mut Clock {
        &mut self.clock
    }

    fn ipc_inflight_total(&self) -> usize {
        self.stores
            .values()
            .map(|store| store.admission.ipc_inflight())
            .sum()
    }

    fn emit_checkpoint_queue_depth(&self) {
        metrics::set_checkpoint_queue_depth(self.checkpoint_scheduler.queue_depth());
    }

    /// Get the next scheduled sync deadline for a remote, if any.
    pub(crate) fn next_sync_deadline_for(&self, remote: &RemoteUrl) -> Option<Instant> {
        self.scheduler.deadline_for(remote)
    }

    pub(crate) fn schedule_sync(&mut self, remote: RemoteUrl) {
        self.scheduler.schedule(remote);
    }

    pub(crate) fn mark_checkpoint_dirty(
        &mut self,
        store_id: StoreId,
        namespace: &NamespaceId,
        events: u64,
    ) {
        self.checkpoint_scheduler
            .mark_dirty_for_namespace(store_id, namespace, events);
        self.emit_checkpoint_queue_depth();
    }

    fn register_default_checkpoint_groups(&mut self, store_id: StoreId) -> Result<(), OpError> {
        let store = self
            .stores
            .get(&store_id)
            .ok_or(OpError::Internal("loaded store missing from state"))?;
        let config = CheckpointGroupConfig::core_default(store_id, store.meta.replica_id);
        self.checkpoint_scheduler.register_group(config);
        self.emit_checkpoint_queue_depth();
        Ok(())
    }

    /// Get repo state. Returns Internal if invariant is violated.
    pub(crate) fn repo_state(&self, proof: &LoadedStore) -> Result<&RepoState, OpError> {
        self.stores
            .get(&proof.store_id)
            .map(|store| &store.repo_state)
            .ok_or(OpError::Internal("loaded store missing from state"))
    }

    /// Get mutable repo state. Returns Internal if invariant is violated.
    pub(crate) fn repo_state_mut(
        &mut self,
        proof: &LoadedStore,
    ) -> Result<&mut RepoState, OpError> {
        self.stores
            .get_mut(&proof.store_id)
            .map(|store| &mut store.repo_state)
            .ok_or(OpError::Internal("loaded store missing from state"))
    }

    pub(crate) fn store_runtime(&self, proof: &LoadedStore) -> Result<&StoreRuntime, OpError> {
        self.stores
            .get(&proof.store_id)
            .ok_or(OpError::Internal("loaded store missing from state"))
    }

    pub(crate) fn store_runtime_mut(
        &mut self,
        proof: &LoadedStore,
    ) -> Result<&mut StoreRuntime, OpError> {
        self.stores
            .get_mut(&proof.store_id)
            .ok_or(OpError::Internal("loaded store missing from state"))
    }

    pub(crate) fn checkpoint_group_snapshots(
        &self,
        store_id: StoreId,
    ) -> Vec<CheckpointGroupSnapshot> {
        self.checkpoint_scheduler.snapshot_for_store(store_id)
    }

    /// Get repo state by raw remote URL (for internal sync waiters, etc.).
    /// Returns None if not loaded.
    pub(crate) fn repo_state_by_url(&self, remote: &RemoteUrl) -> Option<&RepoState> {
        self.remote_to_store_id
            .get(remote)
            .and_then(|store_id| self.stores.get(store_id))
            .map(|store| &store.repo_state)
    }

    pub(crate) fn store_identity(&self, proof: &LoadedStore) -> Result<StoreIdentity, OpError> {
        self.stores
            .get(&proof.store_id)
            .map(|store| store.meta.identity)
            .ok_or(OpError::Internal("loaded store missing from state"))
    }

    pub(crate) fn primary_remote_for_store(&self, store_id: &StoreId) -> Option<&RemoteUrl> {
        self.stores.get(store_id).map(|store| &store.primary_remote)
    }

    /// Resolve a repo path to a normalized remote URL.
    fn resolve_remote(&mut self, repo_path: &Path) -> Result<RemoteUrl, OpError> {
        // 1. Env override (highest priority).
        if let Ok(url) = std::env::var("BD_REMOTE_URL")
            && !url.trim().is_empty()
        {
            return Ok(RemoteUrl(normalize_url(&url)));
        }

        // 2. Cache.
        if let Some(remote) = self.path_to_remote.get(repo_path) {
            return Ok(remote.clone());
        }

        // 3. Git config.
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

    fn resolve_store(&mut self, repo_path: &Path) -> Result<ResolvedStore, OpError> {
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

    /// Ensure repo is loaded using cached refs, without blocking on network fetch.
    ///
    /// If no local refs exist, this will attempt a one-time fetch with a bounded timeout.
    pub fn ensure_repo_loaded(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedStore, OpError> {
        let resolved = self.resolve_store(repo)?;
        let store_id = resolved.store_id;
        let remote = resolved.remote;
        self.path_to_remote.insert(repo.to_owned(), remote.clone());

        if !self.stores.contains_key(&store_id) {
            let open = StoreRuntime::open(
                store_id,
                remote.clone(),
                Arc::clone(&self.wal),
                WallClock::now().0,
                env!("CARGO_PKG_VERSION"),
                self.limits(),
            )?;
            let runtime = open.runtime;
            if let Some(hlc_state) = runtime.hlc_state_for_actor(&self.actor)? {
                self.clock.receive(&hlc_state);
            }
            self.stores.insert(store_id, runtime);
            self.register_default_checkpoint_groups(store_id)?;
            self.register_default_checkpoint_groups(store_id)?;

            let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
            git_tx
                .send(GitOp::LoadLocal {
                    repo: repo.to_owned(),
                    respond: respond_tx,
                })
                .map_err(|_| OpError::Internal("git thread not responding"))?;

            match respond_rx.recv() {
                Ok(Ok(loaded)) => {
                    self.apply_loaded_repo_state(store_id, &remote, repo, loaded)?;
                }
                Ok(Err(SyncError::NoLocalRef(_))) => {
                    // No cached refs; attempt a bounded fetch to discover remote state.
                    let timeout = load_timeout();
                    let (fetch_tx, fetch_rx) = crossbeam::channel::bounded(1);
                    git_tx
                        .send(GitOp::Load {
                            repo: repo.to_owned(),
                            respond: fetch_tx,
                        })
                        .map_err(|_| OpError::Internal("git thread not responding"))?;

                    match fetch_rx.recv_timeout(timeout) {
                        Ok(Ok(loaded)) => {
                            self.apply_loaded_repo_state(store_id, &remote, repo, loaded)?;
                        }
                        Ok(Err(SyncError::NoLocalRef(_))) => {
                            return Err(OpError::RepoNotInitialized(repo.to_owned()));
                        }
                        Ok(Err(e)) => return Err(OpError::from(e)),
                        Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                            return Err(OpError::LoadTimeout {
                                repo: repo.to_owned(),
                                timeout_secs: timeout.as_secs(),
                                remote: remote.as_str().to_string(),
                            });
                        }
                        Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                            return Err(OpError::Internal("git thread died"));
                        }
                    }
                }
                Ok(Err(e)) => return Err(OpError::from(e)),
                Err(_) => return Err(OpError::Internal("git thread died")),
            }
        } else if let Some(store) = self.stores.get_mut(&store_id) {
            store.repo_state.register_path(repo.to_owned());
            if store.primary_remote != remote {
                store.primary_remote = remote.clone();
            }
            self.export_go_compat(store_id, &remote);
        }

        Ok(LoadedStore { store_id, remote })
    }

    /// Ensure repo is loaded, fetching from git if needed.
    ///
    /// This is a blocking operation - sends Load to git thread and waits with a bounded
    /// timeout for the initial fetch. Returns a `LoadedStore` proof for state access.
    pub fn ensure_repo_loaded_strict(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedStore, OpError> {
        let resolved = self.resolve_store(repo)?;
        let store_id = resolved.store_id;
        let remote = resolved.remote;
        self.path_to_remote.insert(repo.to_owned(), remote.clone());

        if !self.stores.contains_key(&store_id) {
            let open = StoreRuntime::open(
                store_id,
                remote.clone(),
                Arc::clone(&self.wal),
                WallClock::now().0,
                env!("CARGO_PKG_VERSION"),
                self.limits(),
            )?;
            let runtime = open.runtime;
            if let Some(hlc_state) = runtime.hlc_state_for_actor(&self.actor)? {
                self.clock.receive(&hlc_state);
            }
            self.stores.insert(store_id, runtime);

            // Blocking load from git (fetches remote first in GitWorker).
            let timeout = load_timeout();
            let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
            git_tx
                .send(GitOp::Load {
                    repo: repo.to_owned(),
                    respond: respond_tx,
                })
                .map_err(|_| OpError::Internal("git thread not responding"))?;

            match respond_rx.recv_timeout(timeout) {
                Ok(Ok(loaded)) => {
                    self.apply_loaded_repo_state(store_id, &remote, repo, loaded)?;
                }
                Ok(Err(SyncError::NoLocalRef(_))) => {
                    return Err(OpError::RepoNotInitialized(repo.to_owned()));
                }
                Ok(Err(e)) => {
                    return Err(OpError::from(e));
                }
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                    return Err(OpError::LoadTimeout {
                        repo: repo.to_owned(),
                        timeout_secs: timeout.as_secs(),
                        remote: remote.as_str().to_string(),
                    });
                }
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                    return Err(OpError::Internal("git thread died"));
                }
            }
        } else if let Some(store) = self.stores.get_mut(&store_id) {
            store.repo_state.register_path(repo.to_owned());
            if store.primary_remote != remote {
                store.primary_remote = remote.clone();
            }

            // Update symlinks for newly registered clone path
            self.export_go_compat(store_id, &remote);
        }

        Ok(LoadedStore { store_id, remote })
    }

    fn apply_loaded_repo_state(
        &mut self,
        store_id: StoreId,
        remote: &RemoteUrl,
        repo: &Path,
        loaded: LoadResult,
    ) -> Result<(), OpError> {
        let mut last_seen_stamp = loaded.last_seen_stamp;
        if let Some(max_stamp) = last_seen_stamp.as_ref() {
            self.clock.receive(max_stamp);
        }
        let mut needs_sync = loaded.needs_sync;
        let mut state = store_state_from_legacy(loaded.state);
        let mut root_slug = loaded.root_slug;

        // WAL recovery: merge any state from legacy snapshot WAL
        match crate::daemon::migration::import_legacy_snapshot_wal(&self.wal, remote) {
            Ok(Some(wal_import)) => {
                tracing::info!(
                    "recovering legacy WAL for {:?} (sequence {})",
                    remote,
                    wal_import.sequence
                );

                // Advance clock for WAL timestamps
                if let Some(max_stamp) = wal_import.state.max_write_stamp() {
                    self.clock.receive(&max_stamp);
                    last_seen_stamp = max_write_stamp(last_seen_stamp, Some(max_stamp));
                }

                // CRDT merge WAL state with git state
                match merge_store_states(&state, &wal_import.state) {
                    Ok(merged) => {
                        state = merged;
                        // WAL slug takes precedence if set
                        if wal_import.root_slug.is_some() {
                            root_slug = wal_import.root_slug;
                        }
                        // WAL had data - need to sync it to remote
                        needs_sync = true;
                    }
                    Err(crate::git::checkpoint::CheckpointImportError::Merge(errors)) => {
                        return Err(OpError::WalMerge {
                            errors: Box::new(errors),
                        });
                    }
                    Err(_) => {
                        return Err(OpError::Internal("store state merge failed"));
                    }
                }
            }
            Ok(None) => {}
            Err(MigrationError::WalSnapshot(err)) => return Err(OpError::from(err)),
            Err(MigrationError::Sync(err)) => return Err(OpError::from(err)),
        }

        let replayed_event_wal = {
            let store = self
                .stores
                .get(&store_id)
                .ok_or(OpError::Internal("loaded store missing from state"))?;
            replay_event_wal(
                store_id,
                store.wal_index.as_ref(),
                &mut state,
                self.limits(),
            )?
        };
        if replayed_event_wal {
            needs_sync = true;
        }

        last_seen_stamp = max_write_stamp(last_seen_stamp, state.max_write_stamp());

        let now_wall_ms = WallClock::now().0;
        let clock_skew = last_seen_stamp
            .as_ref()
            .and_then(|stamp| detect_clock_skew(now_wall_ms, stamp.wall_ms));

        let mut repo_state = RepoState::with_state_and_path(state, root_slug, repo.to_owned());
        repo_state.last_seen_stamp = last_seen_stamp;
        repo_state.last_clock_skew = clock_skew;
        repo_state.last_fetch_error = loaded.fetch_error.map(|message| FetchErrorRecord {
            message,
            wall_ms: now_wall_ms,
        });
        repo_state.last_divergence = loaded.divergence.map(|divergence| DivergenceRecord {
            local_oid: divergence.local_oid.to_string(),
            remote_oid: divergence.remote_oid.to_string(),
            wall_ms: now_wall_ms,
        });
        repo_state.last_force_push = loaded.force_push.map(|force_push| ForcePushRecord {
            previous_remote_oid: force_push.previous_remote_oid.to_string(),
            remote_oid: force_push.remote_oid.to_string(),
            wall_ms: now_wall_ms,
        });

        // If local/WAL has changes that remote doesn't (crash recovery),
        // mark dirty so sync will push those changes.
        if needs_sync {
            repo_state.mark_dirty();
            self.scheduler.schedule(remote.clone());
        }

        let store = self
            .stores
            .get_mut(&store_id)
            .ok_or(OpError::Internal("loaded store missing from state"))?;
        store.repo_state = repo_state;
        if store.primary_remote != *remote {
            store.primary_remote = remote.clone();
        }

        // Initial Go-compat export for newly loaded repo
        self.export_go_compat(store_id, remote);

        if let Err(err) = self.ensure_replication_runtime(store_id) {
            tracing::warn!("replication runtime init failed for {store_id}: {err}");
        }
        Ok(())
    }

    fn ensure_replication_runtime(&mut self, store_id: StoreId) -> Result<(), OpError> {
        if self.repl_handles.contains_key(&store_id) {
            return Ok(());
        }

        let Some(ingest_tx) = self.repl_ingest_tx.clone() else {
            tracing::warn!("replication ingest channel not initialized");
            return Ok(());
        };

        let store = self
            .stores
            .get(&store_id)
            .ok_or(OpError::Internal("loaded store missing from state"))?;

        let wal_index: Arc<dyn WalIndex> = store.wal_index.clone();
        let wal_reader = Some(WalRangeReader::new(
            store_id,
            wal_index.clone(),
            self.limits.clone(),
        ));
        let session_store =
            SharedSessionStore::new(ReplSessionStore::new(store_id, wal_index, ingest_tx));

        let roster = load_replica_roster(store_id)?;
        let peers = Vec::new();

        let manager_config = ReplicationManagerConfig {
            local_store: store.meta.identity,
            local_replica_id: store.meta.replica_id,
            admission: store.admission.clone(),
            broadcaster: store.broadcaster.clone(),
            peer_acks: store.peer_acks.clone(),
            policies: store.policies.clone(),
            roster: roster.clone(),
            peers,
            wal_reader: wal_reader.clone(),
            limits: self.limits.clone(),
            backoff: BackoffPolicy {
                base: Duration::from_millis(250),
                max: Duration::from_secs(5),
            },
        };
        let manager_handle = ReplicationManager::new(session_store.clone(), manager_config).start();

        let max_connections = if roster.is_some() {
            None
        } else {
            replication_max_connections()
        };
        let server_config = ReplicationServerConfig {
            listen_addr: replication_listen_addr(),
            local_store: store.meta.identity,
            local_replica_id: store.meta.replica_id,
            admission: store.admission.clone(),
            broadcaster: store.broadcaster.clone(),
            peer_acks: store.peer_acks.clone(),
            policies: store.policies.clone(),
            roster,
            wal_reader,
            limits: self.limits.clone(),
            max_connections,
        };

        let server_handle = match ReplicationServer::new(session_store, server_config).start() {
            Ok(handle) => Some(handle),
            Err(err) => {
                tracing::warn!("replication server failed to start: {err}");
                None
            }
        };

        self.repl_handles.insert(
            store_id,
            ReplicationHandles {
                manager: Some(manager_handle),
                server: server_handle,
            },
        );

        Ok(())
    }

    pub(crate) fn handle_repl_ingest(&mut self, request: ReplIngestRequest) {
        if self.shutting_down {
            let payload = ErrorPayload::new(ErrorCode::MaintenanceMode, "shutting down", true);
            let _ = request.respond.send(Err(Box::new(payload)));
            return;
        }
        if let Some(payload) = self.stores.get(&request.store_id).and_then(|store| {
            if store.maintenance_mode {
                Some(
                    ErrorPayload::new(ErrorCode::MaintenanceMode, "maintenance mode enabled", true)
                        .with_details(error_details::MaintenanceModeDetails {
                            reason: Some("maintenance mode enabled".into()),
                            until_ms: None,
                        }),
                )
            } else if let Some(policy) = store.policies.get(&request.namespace) {
                if policy.replicate_mode == ReplicateMode::None {
                    Some(
                        ErrorPayload::new(
                            ErrorCode::NamespacePolicyViolation,
                            "namespace replication disabled by policy",
                            false,
                        )
                        .with_details(
                            error_details::NamespacePolicyViolationDetails {
                                namespace: request.namespace.clone(),
                                rule: "replicate_mode".to_string(),
                                reason: Some("replicate_mode=none".to_string()),
                            },
                        ),
                    )
                } else {
                    None
                }
            } else {
                Some(
                    ErrorPayload::new(
                        ErrorCode::NamespaceUnknown,
                        "namespace not configured",
                        false,
                    )
                    .with_details(error_details::NamespaceUnknownDetails {
                        namespace: request.namespace.clone(),
                    }),
                )
            }
        }) {
            let _ = request.respond.send(Err(Box::new(payload)));
            return;
        }
        let outcome = self.ingest_remote_batch(
            request.store_id,
            request.namespace,
            request.origin,
            request.batch,
            request.now_ms,
        );
        let _ = request.respond.send(outcome);
    }

    pub(crate) fn shutdown_replication(&mut self) {
        let handles = std::mem::take(&mut self.repl_handles);
        for (_, handle) in handles {
            handle.shutdown();
        }
    }

    fn ingest_remote_batch(
        &mut self,
        store_id: StoreId,
        namespace: NamespaceId,
        origin: ReplicaId,
        batch: Vec<VerifiedEvent<PrevVerified>>,
        now_ms: u64,
    ) -> Result<IngestOutcome, Box<ErrorPayload>> {
        let store = self.stores.get_mut(&store_id).ok_or_else(|| {
            Box::new(ErrorPayload::new(
                ErrorCode::Internal,
                "store not loaded",
                true,
            ))
        })?;

        if batch.is_empty() {
            let durable = store
                .watermarks_durable
                .get(&namespace, &origin)
                .copied()
                .unwrap_or_else(Watermark::genesis);
            let applied = store
                .watermarks_applied
                .get(&namespace, &origin)
                .copied()
                .unwrap_or_else(Watermark::genesis);
            return Ok(IngestOutcome { durable, applied });
        }

        let mut preview_state = store.repo_state.state.get_or_default(&namespace);
        for event in &batch {
            if event.body.namespace != namespace || event.body.origin_replica_id != origin {
                return Err(Box::new(ErrorPayload::new(
                    ErrorCode::Internal,
                    "replication batch has mismatched origin",
                    false,
                )));
            }
            if let Err(err) = apply_event(&mut preview_state, &event.body) {
                return Err(Box::new(apply_event_error_payload(
                    &namespace, &origin, err,
                )));
            }
        }

        let store_dir = paths::store_dir(store_id);

        let wal_index = Arc::clone(&store.wal_index);
        let mut txn = wal_index
            .writer()
            .begin_txn()
            .map_err(|err| Box::new(wal_index_error_payload(&err)))?;

        for event in &batch {
            let record = Record {
                header: RecordHeader {
                    origin_replica_id: origin,
                    origin_seq: event.body.origin_seq.get(),
                    event_time_ms: event.body.event_time_ms,
                    txn_id: event.body.txn_id,
                    client_request_id: event.body.client_request_id,
                    request_sha256: None,
                    sha256: event.sha256.0,
                    prev_sha256: event.prev.prev.map(|sha| sha.0),
                },
                payload: Bytes::copy_from_slice(event.bytes.as_ref()),
            };

            let append_start = Instant::now();
            let append = match store.event_wal.append(&namespace, &record, now_ms) {
                Ok(append) => {
                    let elapsed = append_start.elapsed();
                    metrics::wal_append_ok(elapsed);
                    metrics::wal_fsync_ok(elapsed);
                    append
                }
                Err(err) => {
                    let elapsed = append_start.elapsed();
                    metrics::wal_append_err(elapsed);
                    metrics::wal_fsync_err(elapsed);
                    return Err(Box::new(event_wal_error_payload(
                        &namespace, None, None, err,
                    )));
                }
            };
            let segment_snapshot = store
                .event_wal
                .segment_snapshot(&namespace)
                .ok_or_else(|| {
                    Box::new(ErrorPayload::new(
                        ErrorCode::Internal,
                        "missing active wal segment",
                        false,
                    ))
                })?;
            let last_indexed_offset = append.offset + append.len as u64;
            let segment_row = SegmentRow {
                namespace: namespace.clone(),
                segment_id: append.segment_id,
                segment_path: segment_rel_path(&store_dir, &segment_snapshot.path),
                created_at_ms: segment_snapshot.created_at_ms,
                last_indexed_offset,
                sealed: false,
                final_len: None,
            };

            txn.upsert_segment(&segment_row)
                .map_err(|err| Box::new(wal_index_error_payload(&err)))?;
            if let Some(sealed) = append.sealed.as_ref() {
                let sealed_row = SegmentRow {
                    namespace: namespace.clone(),
                    segment_id: sealed.segment_id,
                    segment_path: segment_rel_path(&store_dir, &sealed.path),
                    created_at_ms: sealed.created_at_ms,
                    last_indexed_offset: sealed.final_len,
                    sealed: true,
                    final_len: Some(sealed.final_len),
                };
                txn.upsert_segment(&sealed_row)
                    .map_err(|err| Box::new(wal_index_error_payload(&err)))?;
            }
            txn.record_event(
                &namespace,
                &event_id_for(origin, namespace.clone(), event.body.origin_seq),
                event.sha256.0,
                event.prev.prev.map(|sha| sha.0),
                append.segment_id,
                append.offset,
                append.len,
                event.body.event_time_ms,
                event.body.txn_id,
                event.body.client_request_id,
                None,
            )
            .map_err(|err| Box::new(wal_index_error_payload(&err)))?;

            if let Some(hlc_max) = &event.body.hlc_max {
                txn.update_hlc(&HlcRow {
                    actor_id: hlc_max.actor_id.clone(),
                    last_physical_ms: hlc_max.physical_ms,
                    last_logical: hlc_max.logical,
                })
                .map_err(|err| Box::new(wal_index_error_payload(&err)))?;
            }
        }

        txn.commit()
            .map_err(|err| Box::new(wal_index_error_payload(&err)))?;

        let (remote, max_stamp, durable, applied, applied_head, durable_head) = {
            let mut max_stamp = store.repo_state.last_seen_stamp.clone();
            for event in &batch {
                let apply_start = Instant::now();
                let apply_result = {
                    let state = store.repo_state.state.ensure_namespace(namespace.clone());
                    apply_event(state, &event.body)
                };
                if let Err(err) = apply_result {
                    metrics::apply_err(apply_start.elapsed());
                    return Err(Box::new(apply_event_error_payload(
                        &namespace, &origin, err,
                    )));
                }
                metrics::apply_ok(apply_start.elapsed());

                if let Some(hlc_max) = &event.body.hlc_max {
                    let stamp = WriteStamp::new(hlc_max.physical_ms, hlc_max.logical);
                    max_stamp = max_write_stamp(max_stamp, Some(stamp));
                }

                let event_id = event_id_for(origin, namespace.clone(), event.body.origin_seq);
                let prev_sha = event.prev.prev.map(|sha| Sha256(sha.0));
                let broadcast = BroadcastEvent::new(
                    event_id,
                    event.sha256,
                    prev_sha,
                    EventBytes::<Canonical>::new(Bytes::copy_from_slice(event.bytes.as_ref())),
                );
                if let Err(err) = store.broadcaster.publish(broadcast) {
                    tracing::warn!("event broadcast failed: {err}");
                }

                store
                    .watermarks_applied
                    .advance_contiguous(&namespace, &origin, event.body.origin_seq, event.sha256.0)
                    .map_err(|err| Box::new(watermark_error_payload(&namespace, &origin, err)))?;
                store
                    .watermarks_durable
                    .advance_contiguous(&namespace, &origin, event.body.origin_seq, event.sha256.0)
                    .map_err(|err| Box::new(watermark_error_payload(&namespace, &origin, err)))?;
            }

            if let Some(stamp) = max_stamp.clone() {
                let now_wall_ms = WallClock::now().0;
                store.repo_state.last_seen_stamp = Some(stamp.clone());
                store.repo_state.last_clock_skew = detect_clock_skew(now_wall_ms, stamp.wall_ms);
            }
            store.repo_state.mark_dirty();

            let durable = store
                .watermarks_durable
                .get(&namespace, &origin)
                .copied()
                .unwrap_or_else(Watermark::genesis);
            let applied = store
                .watermarks_applied
                .get(&namespace, &origin)
                .copied()
                .unwrap_or_else(Watermark::genesis);
            let applied_head = store.applied_head_sha(&namespace, &origin);
            let durable_head = store.durable_head_sha(&namespace, &origin);
            let remote = store.primary_remote.clone();

            (
                remote,
                max_stamp,
                durable,
                applied,
                applied_head,
                durable_head,
            )
        };

        if let Some(stamp) = max_stamp.clone() {
            self.clock.receive(&stamp);
        }
        self.mark_checkpoint_dirty(store_id, &namespace, batch.len() as u64);
        self.schedule_sync(remote);

        let applied_seq = applied.seq().get();
        let durable_seq = durable.seq().get();

        let mut watermark_txn = wal_index
            .writer()
            .begin_txn()
            .map_err(|err| Box::new(wal_index_error_payload(&err)))?;
        watermark_txn
            .update_watermark(
                &namespace,
                &origin,
                applied_seq,
                durable_seq,
                applied_head,
                durable_head,
            )
            .map_err(|err| Box::new(wal_index_error_payload(&err)))?;
        watermark_txn
            .commit()
            .map_err(|err| Box::new(wal_index_error_payload(&err)))?;

        Ok(IngestOutcome { durable, applied })
    }

    /// Ensure repo is loaded and reasonably fresh from remote.
    ///
    /// For clean (non-dirty) repos, we periodically kick off a background refresh
    /// so read-only commands observe updates pushed by other machines or daemons.
    /// This is non-blocking - it returns cached state immediately and applies
    /// fresh state when the background load completes.
    ///
    /// Returns a `LoadedStore` proof that can be used for infallible state access.
    pub fn ensure_repo_fresh(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedStore, OpError> {
        let loaded = self.ensure_repo_loaded(repo, git_tx)?;

        let repo_state = self.repo_state(&loaded)?;
        let needs_refresh = !repo_state.dirty
            && !repo_state.sync_in_progress
            && !repo_state.refresh_in_progress
            && repo_state
                .last_refresh
                .map(|t| t.elapsed() >= REFRESH_TTL)
                .unwrap_or(true);

        if needs_refresh {
            // Get a valid path for the refresh operation
            let path = repo_state.any_valid_path().cloned();

            if let Some(refresh_path) = path {
                // Mark refresh in progress before sending to avoid races
                let repo_state = self.repo_state_mut(&loaded)?;
                repo_state.refresh_in_progress = true;

                // Kick off background refresh - don't wait for result
                let _ = git_tx.send(GitOp::Refresh {
                    repo: refresh_path,
                    remote: loaded.remote().clone(),
                });
            }
        }

        // Return immediately with cached state
        Ok(loaded)
    }

    /// Complete a background refresh operation.
    ///
    /// Called when git thread reports refresh result. Applies fresh state
    /// if refresh succeeded, otherwise just clears the in-progress flag.
    pub fn complete_refresh(
        &mut self,
        remote: &RemoteUrl,
        result: Result<super::git_worker::LoadResult, SyncError>,
    ) {
        let store_id = match self.remote_to_store_id.get(remote).copied() {
            Some(id) => id,
            None => return,
        };
        let repo_state = match self.stores.get_mut(&store_id) {
            Some(store) => &mut store.repo_state,
            None => return,
        };

        repo_state.refresh_in_progress = false;

        match result {
            Ok(fresh) => {
                // Advance clock to account for remote stamps
                if let Some(max_stamp) = fresh.last_seen_stamp.as_ref() {
                    self.clock.receive(max_stamp);
                }

                // Only apply refresh if repo is still clean (no mutations happened
                // during the refresh). If dirty, we'll sync soon anyway.
                if !repo_state.dirty {
                    repo_state
                        .state
                        .set_namespace_state(NamespaceId::core(), fresh.state);
                    // Update root_slug if remote has one and we don't
                    if fresh.root_slug.is_some() {
                        repo_state.root_slug = fresh.root_slug;
                    }
                }
                repo_state.last_refresh = Some(Instant::now());

                repo_state.last_seen_stamp =
                    max_write_stamp(repo_state.last_seen_stamp.clone(), fresh.last_seen_stamp);

                let now_wall_ms = WallClock::now().0;
                repo_state.last_clock_skew = repo_state
                    .last_seen_stamp
                    .as_ref()
                    .and_then(|stamp| detect_clock_skew(now_wall_ms, stamp.wall_ms));
                repo_state.last_fetch_error = fresh.fetch_error.map(|message| FetchErrorRecord {
                    message,
                    wall_ms: now_wall_ms,
                });
                repo_state.last_divergence = fresh.divergence.map(|divergence| DivergenceRecord {
                    local_oid: divergence.local_oid.to_string(),
                    remote_oid: divergence.remote_oid.to_string(),
                    wall_ms: now_wall_ms,
                });
                repo_state.last_force_push = fresh.force_push.map(|force_push| ForcePushRecord {
                    previous_remote_oid: force_push.previous_remote_oid.to_string(),
                    remote_oid: force_push.remote_oid.to_string(),
                    wall_ms: now_wall_ms,
                });

                // If refresh showed we have local changes that need syncing
                if fresh.needs_sync && !repo_state.dirty {
                    repo_state.mark_dirty();
                    self.scheduler.schedule(remote.clone());
                }
            }
            Err(e) => {
                // Refresh failed - log and continue with cached state.
                // Next TTL hit will retry.
                tracing::debug!("background refresh failed for {:?}: {:?}", remote, e);
            }
        }
    }

    /// Force reload state from git, invalidating any cached state.
    ///
    /// Use this after external changes to refs/heads/beads/store (e.g., migration).
    /// This is a blocking operation that fetches fresh state from git.
    pub fn force_reload(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedStore, OpError> {
        let resolved = self.resolve_store(repo)?;

        // Remove cached state so ensure_repo_loaded will do a fresh load
        self.stores.remove(&resolved.store_id);

        // Now load fresh from git
        self.ensure_repo_loaded_strict(repo, git_tx)
    }

    /// Maybe start a background sync for a repo.
    ///
    /// Only starts if:
    /// - Repo is dirty
    /// - Not already syncing
    pub fn maybe_start_sync(&mut self, remote: &RemoteUrl, git_tx: &Sender<GitOp>) {
        let store_id = match self.remote_to_store_id.get(remote).copied() {
            Some(id) => id,
            None => return,
        };
        let repo_state = match self.stores.get_mut(&store_id) {
            Some(store) => &mut store.repo_state,
            None => return,
        };

        if !repo_state.dirty || repo_state.sync_in_progress {
            return;
        }

        let path = match repo_state.any_valid_path() {
            Some(p) => p.clone(),
            None => return,
        };

        repo_state.start_sync();

        let _ = git_tx.send(GitOp::Sync {
            repo: path,
            remote: remote.clone(),
            state: repo_state.state.get_or_default(&NamespaceId::core()),
            actor: self.actor.clone(),
        });
    }

    pub(crate) fn ensure_loaded_and_maybe_start_sync(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedStore, OpError> {
        let loaded = self.ensure_repo_loaded(repo, git_tx)?;
        self.maybe_start_sync(loaded.remote(), git_tx);
        Ok(loaded)
    }

    /// Complete a sync operation.
    ///
    /// Called when git thread reports sync result.
    pub fn complete_sync(&mut self, remote: &RemoteUrl, result: Result<SyncOutcome, SyncError>) {
        let mut backoff_ms = None;
        let mut sync_succeeded = false;
        let store_id = match self.remote_to_store_id.get(remote).copied() {
            Some(id) => id,
            None => return,
        };

        match result {
            Ok(outcome) => {
                let synced_state = outcome.state;
                // Advance clock to account for remote stamps
                if let Some(max_stamp) = outcome.last_seen_stamp.as_ref() {
                    self.clock.receive(max_stamp);
                }
                let resolution_stamp = {
                    let write_stamp = self.clock_mut().tick();
                    Stamp {
                        at: write_stamp,
                        by: self.actor.clone(),
                    }
                };

                let repo_state = match self.stores.get_mut(&store_id) {
                    Some(store) => &mut store.repo_state,
                    None => return,
                };

                repo_state.last_seen_stamp =
                    max_write_stamp(repo_state.last_seen_stamp.clone(), outcome.last_seen_stamp);
                let now_wall_ms = WallClock::now().0;
                repo_state.last_clock_skew = repo_state
                    .last_seen_stamp
                    .as_ref()
                    .and_then(|stamp| detect_clock_skew(now_wall_ms, stamp.wall_ms));
                repo_state.last_divergence =
                    outcome.divergence.map(|divergence| DivergenceRecord {
                        local_oid: divergence.local_oid.to_string(),
                        remote_oid: divergence.remote_oid.to_string(),
                        wall_ms: now_wall_ms,
                    });
                repo_state.last_force_push = outcome.force_push.map(|force_push| ForcePushRecord {
                    previous_remote_oid: force_push.previous_remote_oid.to_string(),
                    remote_oid: force_push.remote_oid.to_string(),
                    wall_ms: now_wall_ms,
                });

                // If mutations happened during sync, merge them
                if repo_state.dirty {
                    let local_state = repo_state.state.get_or_default(&NamespaceId::core());
                    let merged = match CanonicalState::join(&synced_state, &local_state) {
                        Ok(merged) => Ok(merged),
                        Err(mut errs) => {
                            let collisions = detect_collisions(&local_state, &synced_state);
                            if collisions.is_empty() {
                                Err(errs)
                            } else {
                                match resolve_collisions(
                                    &local_state,
                                    &synced_state,
                                    &collisions,
                                    resolution_stamp,
                                ) {
                                    Ok((local_resolved, remote_resolved)) => {
                                        CanonicalState::join(&remote_resolved, &local_resolved)
                                    }
                                    Err(err) => {
                                        errs.push(err);
                                        Err(errs)
                                    }
                                }
                            }
                        }
                    };

                    match merged {
                        Ok(merged) => {
                            let mut next_state = repo_state.state.clone();
                            next_state.set_namespace_state(NamespaceId::core(), merged);
                            repo_state.complete_sync(next_state, self.clock.wall_ms());
                            // Still dirty from mutations during sync - reschedule
                            repo_state.dirty = true;
                            self.scheduler.schedule(remote.clone());
                            sync_succeeded = true;
                        }
                        Err(errs) => {
                            tracing::error!(
                                "merge after sync failed for {:?}: {:?}; preserving local state",
                                remote,
                                errs
                            );
                            repo_state.fail_sync();
                            backoff_ms = Some(repo_state.backoff_ms());
                        }
                    }
                } else {
                    // No mutations during sync - just take synced state
                    let mut next_state = repo_state.state.clone();
                    next_state.set_namespace_state(NamespaceId::core(), synced_state);
                    repo_state.complete_sync(next_state, self.clock.wall_ms());

                    // State is now durable in remote - delete WAL
                    if let Err(e) = self.wal.delete(remote) {
                        tracing::warn!("failed to delete WAL after sync for {:?}: {}", remote, e);
                    }
                    sync_succeeded = true;
                }
            }
            Err(e) => {
                tracing::error!("sync failed for {:?}: {:?}", remote, e);
                let repo_state = match self.stores.get_mut(&store_id) {
                    Some(store) => &mut store.repo_state,
                    None => return,
                };
                repo_state.fail_sync();
                backoff_ms = Some(repo_state.backoff_ms());
            }
        }

        if let Some(backoff) = backoff_ms {
            let backoff = Duration::from_millis(backoff);
            self.scheduler.schedule_after(remote.clone(), backoff);
        }

        // Export Go-compatible JSONL after successful sync
        if sync_succeeded {
            self.export_go_compat(store_id, remote);
        }
    }

    pub fn complete_checkpoint(
        &mut self,
        store_id: StoreId,
        checkpoint_group: &str,
        result: Result<CheckpointPublishOutcome, CheckpointPublishError>,
    ) {
        let key = CheckpointGroupKey {
            store_id,
            group: checkpoint_group.to_string(),
        };
        match &result {
            Ok(outcome) => {
                tracing::info!(
                    store_id = %store_id,
                    checkpoint_group = checkpoint_group,
                    checkpoint_id = %outcome.checkpoint_id,
                    "checkpoint publish succeeded"
                );
                self.checkpoint_scheduler.complete_success(
                    &key,
                    Instant::now(),
                    self.clock.wall_ms(),
                );
            }
            Err(err) => {
                tracing::warn!(
                    store_id = %store_id,
                    checkpoint_group = checkpoint_group,
                    error = ?err,
                    "checkpoint publish failed"
                );
                self.checkpoint_scheduler
                    .complete_failure(&key, Instant::now());
            }
        }
        self.emit_checkpoint_queue_depth();
    }

    /// Export state to Go-compatible JSONL format.
    ///
    /// Called after successful sync to keep .beads/issues.jsonl in sync.
    fn export_go_compat(&self, store_id: StoreId, remote: &RemoteUrl) {
        let Some(ref ctx) = self.export_ctx else {
            return;
        };

        let Some(store) = self.stores.get(&store_id) else {
            return;
        };
        let repo_state = &store.repo_state;

        let empty_state = CanonicalState::new();
        let export_state = repo_state
            .state
            .get(&NamespaceId::core())
            .unwrap_or(&empty_state);

        // Export to canonical location
        let export_path = match export_jsonl(export_state, ctx, remote.as_str()) {
            Ok(path) => path,
            Err(e) => {
                tracing::warn!("Go-compat export failed for {:?}: {}", remote, e);
                return;
            }
        };

        // Create/update symlinks in each clone
        if let Err(e) = ensure_symlinks(&export_path, &repo_state.known_paths) {
            tracing::warn!("Go-compat symlink update failed for {:?}: {}", remote, e);
        }
    }

    /// Apply a mutation with WAL durability.
    ///
    /// The mutation runs against a cloned state. If the WAL write fails,
    /// the in-memory state is left unchanged and the error is returned.
    pub(crate) fn apply_wal_mutation_with_actor<R>(
        &mut self,
        proof: &LoadedStore,
        actor: &ActorId,
        f: impl FnOnce(&mut CanonicalState, Stamp) -> Result<R, OpError>,
    ) -> Result<R, OpError> {
        let (mut next_state, root_slug, sequence, last_seen_stamp) = {
            let repo_state = self.repo_state(proof)?;
            (
                repo_state.state.get_or_default(&NamespaceId::core()),
                repo_state.root_slug.clone(),
                repo_state.wal_sequence,
                repo_state.last_seen_stamp.clone(),
            )
        };

        if let Some(floor) = last_seen_stamp.as_ref() {
            self.clock.receive(floor);
        }
        let now_wall_ms = WallClock::now().0;
        let clock_skew = last_seen_stamp
            .as_ref()
            .and_then(|stamp| detect_clock_skew(now_wall_ms, stamp.wall_ms));

        let write_stamp = self.clock_mut().tick();
        let stamp = Stamp {
            at: write_stamp.clone(),
            by: actor.clone(),
        };

        let result = f(&mut next_state, stamp)?;

        let entry = WalEntry::new(
            next_state.clone(),
            root_slug,
            sequence,
            self.clock.wall_ms(),
        );
        self.wal
            .write_with_limits(proof.remote(), &entry, self.limits())?;

        let repo_state = self.repo_state_mut(proof)?;
        repo_state
            .state
            .set_namespace_state(NamespaceId::core(), next_state);
        repo_state.wal_sequence = sequence + 1;
        repo_state.last_seen_stamp = Some(write_stamp);
        repo_state.last_clock_skew = clock_skew;
        repo_state.mark_dirty();
        self.scheduler.schedule(proof.remote().clone());

        Ok(result)
    }

    /// Apply a mutation with WAL durability using the daemon's default actor.
    #[allow(dead_code)]
    pub(crate) fn apply_wal_mutation<R>(
        &mut self,
        proof: &LoadedStore,
        f: impl FnOnce(&mut CanonicalState, Stamp) -> Result<R, OpError>,
    ) -> Result<R, OpError> {
        let actor = self.actor.clone();
        self.apply_wal_mutation_with_actor(proof, &actor, f)
    }

    pub fn next_sync_deadline(&mut self) -> Option<Instant> {
        self.scheduler.next_deadline()
    }

    pub fn next_checkpoint_deadline(&mut self) -> Option<Instant> {
        self.checkpoint_scheduler.next_deadline()
    }

    pub fn fire_due_syncs(&mut self, git_tx: &Sender<GitOp>) {
        let due = self.scheduler.drain_due(Instant::now());
        for remote in due {
            self.maybe_start_sync(&remote, git_tx);
        }
    }

    pub fn fire_due_checkpoints(&mut self, git_tx: &Sender<GitOp>) {
        let now = Instant::now();
        let due = self.checkpoint_scheduler.drain_due(now);
        for key in due {
            self.start_checkpoint_job(&key, git_tx, now);
        }
    }

    fn start_checkpoint_job(
        &mut self,
        key: &CheckpointGroupKey,
        git_tx: &Sender<GitOp>,
        now: Instant,
    ) {
        let config = match self.checkpoint_scheduler.group_config(key).cloned() {
            Some(config) => config,
            None => return,
        };
        if !config.auto_push() {
            return;
        }

        let checkpoint_groups = self
            .checkpoint_scheduler
            .checkpoint_groups_for_store(key.store_id);

        let (snapshot, repo_path) = {
            let store = match self.stores.get(&key.store_id) {
                Some(store) => store,
                None => {
                    tracing::warn!(
                        store_id = %key.store_id,
                        checkpoint_group = %config.group,
                        "checkpoint store missing"
                    );
                    self.checkpoint_scheduler.complete_failure(key, now);
                    self.emit_checkpoint_queue_depth();
                    return;
                }
            };
            let Some(path) = store.repo_state.any_valid_path().cloned() else {
                tracing::warn!(
                    store_id = %key.store_id,
                    checkpoint_group = %config.group,
                    "checkpoint repo path missing"
                );
                self.checkpoint_scheduler.complete_failure(key, now);
                self.emit_checkpoint_queue_depth();
                return;
            };
            let created_at_ms = WallClock::now().0;
            let snapshot =
                match store.checkpoint_snapshot(&config.group, &config.namespaces, created_at_ms) {
                    Ok(snapshot) => snapshot,
                    Err(err) => {
                        tracing::warn!(
                            store_id = %key.store_id,
                            checkpoint_group = %config.group,
                            error = ?err,
                            "checkpoint snapshot failed"
                        );
                        self.checkpoint_scheduler.complete_failure(key, now);
                        self.emit_checkpoint_queue_depth();
                        return;
                    }
                };
            (snapshot, path)
        };

        if git_tx
            .send(GitOp::Checkpoint {
                repo: repo_path,
                store_id: key.store_id,
                checkpoint_group: config.group.clone(),
                git_ref: config.git_ref.clone(),
                snapshot,
                checkpoint_groups,
            })
            .is_ok()
        {
            self.checkpoint_scheduler.start_in_flight(key, now);
            self.emit_checkpoint_queue_depth();
        } else {
            tracing::warn!(
                store_id = %key.store_id,
                checkpoint_group = %config.group,
                "checkpoint git worker not responding"
            );
            self.checkpoint_scheduler.complete_failure(key, now);
            self.emit_checkpoint_queue_depth();
        }
    }

    /// Get iterator over all stores.
    pub fn repos(&self) -> impl Iterator<Item = (&StoreId, &RepoState)> {
        self.stores
            .iter()
            .map(|(id, store)| (id, &store.repo_state))
    }

    /// Get mutable iterator over all stores.
    pub fn repos_mut(&mut self) -> impl Iterator<Item = (&StoreId, &mut RepoState)> {
        self.stores
            .iter_mut()
            .map(|(id, store)| (id, &mut store.repo_state))
    }

    pub(crate) fn normalize_mutation_meta(
        &self,
        proof: &LoadedStore,
        meta: MutationMeta,
    ) -> Result<NormalizedMutationMeta, OpError> {
        let namespace = self.normalize_namespace(proof, meta.namespace)?;
        let durability = parse_durability(meta.durability)?;
        let client_request_id =
            match trim_non_empty(meta.client_request_id) {
                None => None,
                Some(raw) => Some(ClientRequestId::parse_str(&raw).map_err(|err| {
                    OpError::InvalidRequest {
                        field: Some("client_request_id".into()),
                        reason: err.to_string(),
                    }
                })?),
            };
        let actor_id = match trim_non_empty(meta.actor_id) {
            None => self.actor.clone(),
            Some(raw) => ActorId::new(raw).map_err(|err| OpError::InvalidRequest {
                field: Some("actor_id".into()),
                reason: err.to_string(),
            })?,
        };

        Ok(NormalizedMutationMeta {
            namespace,
            durability,
            client_request_id,
            actor_id,
        })
    }

    pub(crate) fn normalize_read_consistency(
        &self,
        proof: &LoadedStore,
        read: ReadConsistency,
    ) -> Result<NormalizedReadConsistency, OpError> {
        let namespace = self.normalize_namespace(proof, read.namespace)?;
        Ok(NormalizedReadConsistency {
            namespace,
            require_min_seen: read.require_min_seen,
            wait_timeout_ms: read.wait_timeout_ms.unwrap_or(0),
        })
    }

    pub(crate) fn check_read_gate(
        &self,
        proof: &LoadedStore,
        read: &NormalizedReadConsistency,
    ) -> Result<(), OpError> {
        match self.read_gate_status(proof, read)? {
            ReadGateStatus::Satisfied => Ok(()),
            ReadGateStatus::Unsatisfied {
                required,
                current_applied,
            } => {
                if read.wait_timeout_ms() > 0 {
                    return Err(OpError::RequireMinSeenTimeout {
                        waited_ms: read.wait_timeout_ms(),
                        required: Box::new(required),
                        current_applied: Box::new(current_applied),
                    });
                }
                Err(OpError::RequireMinSeenUnsatisfied {
                    required: Box::new(required),
                    current_applied: Box::new(current_applied),
                })
            }
        }
    }

    pub(crate) fn read_gate_status(
        &self,
        proof: &LoadedStore,
        read: &NormalizedReadConsistency,
    ) -> Result<ReadGateStatus, OpError> {
        let Some(required) = read.require_min_seen() else {
            return Ok(ReadGateStatus::Satisfied);
        };
        let current_applied = self.store_runtime(proof)?.watermarks_applied.clone();
        if current_applied.satisfies_at_least(required) {
            return Ok(ReadGateStatus::Satisfied);
        }
        Ok(ReadGateStatus::Unsatisfied {
            required: required.clone(),
            current_applied,
        })
    }

    fn normalize_namespace(
        &self,
        proof: &LoadedStore,
        raw: Option<String>,
    ) -> Result<NamespaceId, OpError> {
        let namespace = match trim_non_empty(raw) {
            None => NamespaceId::core(),
            Some(value) => {
                NamespaceId::parse(value.clone()).map_err(|err| OpError::NamespaceInvalid {
                    namespace: value,
                    reason: err.to_string(),
                })?
            }
        };
        let store = self.store_runtime(proof)?;
        if store.policies.contains_key(&namespace) {
            Ok(namespace)
        } else {
            Err(OpError::NamespaceUnknown { namespace })
        }
    }

    /// Handle a request from IPC.
    ///
    /// Dispatches to appropriate handler based on request type.
    pub(crate) fn handle_request(
        &mut self,
        req: Request,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        match req {
            // Mutations - delegate to executor module
            Request::Create {
                repo,
                id,
                parent,
                title,
                bead_type,
                priority,
                description,
                design,
                acceptance_criteria,
                assignee,
                external_ref,
                estimated_minutes,
                labels,
                dependencies,
                meta,
            } => {
                self.apply_create(
                    &repo,
                    meta,
                    id,
                    parent,
                    title,
                    bead_type,
                    priority,
                    description,
                    design,
                    acceptance_criteria,
                    assignee,
                    external_ref,
                    estimated_minutes,
                    labels,
                    dependencies,
                    git_tx,
                )
            }

            Request::Update {
                repo,
                id,
                patch,
                cas,
                meta,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.apply_update(&repo, meta, &id, patch, cas, git_tx)
            }

            Request::AddLabels {
                repo,
                id,
                labels,
                meta,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.apply_add_labels(&repo, meta, &id, labels, git_tx)
            }

            Request::RemoveLabels {
                repo,
                id,
                labels,
                meta,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.apply_remove_labels(&repo, meta, &id, labels, git_tx)
            }

            Request::SetParent {
                repo,
                id,
                parent,
                meta,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                let parent = match parent {
                    Some(raw) => match BeadId::parse(&raw) {
                        Ok(parent) => Some(parent),
                        Err(e) => {
                            return Response::err(invalid_id_payload(e)).into();
                        }
                    },
                    None => None,
                };
                self.apply_set_parent(&repo, meta, &id, parent, git_tx)
            }

            Request::Close {
                repo,
                id,
                reason,
                on_branch,
                meta,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.apply_close(&repo, meta, &id, reason, on_branch, git_tx)
            }

            Request::Reopen { repo, id, meta } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.apply_reopen(&repo, meta, &id, git_tx)
            }

            Request::Delete {
                repo,
                id,
                reason,
                meta,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.apply_delete(&repo, meta, &id, reason, git_tx)
            }

            Request::AddDep {
                repo,
                from,
                to,
                kind,
                meta,
            } => {
                let from = match BeadId::parse(&from) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                let to = match BeadId::parse(&to) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.apply_add_dep(&repo, meta, &from, &to, kind, git_tx)
            }

            Request::RemoveDep {
                repo,
                from,
                to,
                kind,
                meta,
            } => {
                let from = match BeadId::parse(&from) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                let to = match BeadId::parse(&to) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.apply_remove_dep(&repo, meta, &from, &to, kind, git_tx)
            }

            Request::AddNote {
                repo,
                id,
                content,
                meta,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.apply_add_note(&repo, meta, &id, content, git_tx)
            }

            Request::Claim {
                repo,
                id,
                lease_secs,
                meta,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.apply_claim(&repo, meta, &id, lease_secs, git_tx)
            }

            Request::Unclaim { repo, id, meta } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.apply_unclaim(&repo, meta, &id, git_tx)
            }

            Request::ExtendClaim {
                repo,
                id,
                lease_secs,
                meta,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.apply_extend_claim(&repo, meta, &id, lease_secs, git_tx)
            }

            // Queries - delegate to query_executor module
            Request::Show { repo, id, read } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.query_show(&repo, &id, read, git_tx).into()
            }

            Request::ShowMultiple { repo, ids, read } => {
                self.query_show_multiple(&repo, &ids, read, git_tx).into()
            }

            Request::List {
                repo,
                filters,
                read,
            } => self.query_list(&repo, &filters, read, git_tx).into(),

            Request::Ready { repo, limit, read } => {
                self.query_ready(&repo, limit, read, git_tx).into()
            }

            Request::DepTree { repo, id, read } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.query_dep_tree(&repo, &id, read, git_tx).into()
            }

            Request::Deps { repo, id, read } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.query_deps(&repo, &id, read, git_tx).into()
            }

            Request::Notes { repo, id, read } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(invalid_id_payload(e)).into(),
                };
                self.query_notes(&repo, &id, read, git_tx).into()
            }

            Request::Blocked { repo, read } => self.query_blocked(&repo, read, git_tx).into(),

            Request::Stale {
                repo,
                days,
                status,
                limit,
                read,
            } => self
                .query_stale(&repo, days, status.as_deref(), limit, read, git_tx)
                .into(),

            Request::Count {
                repo,
                filters,
                group_by,
                read,
            } => self
                .query_count(&repo, &filters, group_by.as_deref(), read, git_tx)
                .into(),

            Request::Deleted {
                repo,
                since_ms,
                id,
                read,
            } => {
                let id = match id {
                    Some(s) => Some(match BeadId::parse(&s) {
                        Ok(id) => id,
                        Err(e) => {
                            return Response::err(invalid_id_payload(e)).into();
                        }
                    }),
                    None => None,
                };
                self.query_deleted(&repo, since_ms, id.as_ref(), read, git_tx)
                    .into()
            }

            Request::EpicStatus {
                repo,
                eligible_only,
                read,
            } => self
                .query_epic_status(&repo, eligible_only, read, git_tx)
                .into(),

            Request::Status { repo, read } => self.query_status(&repo, read, git_tx).into(),

            Request::AdminStatus { repo, read } => self.admin_status(&repo, read, git_tx).into(),

            Request::AdminMetrics { repo, read } => self.admin_metrics(&repo, read, git_tx).into(),

            Request::AdminDoctor {
                repo,
                read,
                max_records_per_namespace,
                verify_checkpoint_cache,
            } => self
                .admin_doctor(
                    &repo,
                    read,
                    max_records_per_namespace,
                    verify_checkpoint_cache,
                    git_tx,
                )
                .into(),

            Request::AdminScrub {
                repo,
                read,
                max_records_per_namespace,
                verify_checkpoint_cache,
            } => self
                .admin_scrub_now(
                    &repo,
                    read,
                    max_records_per_namespace,
                    verify_checkpoint_cache,
                    git_tx,
                )
                .into(),

            Request::AdminFingerprint {
                repo,
                read,
                mode,
                sample,
            } => self
                .admin_fingerprint(&repo, read, mode, sample, git_tx)
                .into(),

            Request::AdminReloadPolicies { repo } => {
                self.admin_reload_policies(&repo, git_tx).into()
            }

            Request::AdminRotateReplicaId { repo } => {
                self.admin_rotate_replica_id(&repo, git_tx).into()
            }

            Request::AdminMaintenanceMode { repo, enabled } => {
                self.admin_maintenance_mode(&repo, enabled, git_tx).into()
            }

            Request::AdminRebuildIndex { repo } => self.admin_rebuild_index(&repo, git_tx).into(),

            Request::Validate { repo, read } => self.query_validate(&repo, read, git_tx).into(),

            Request::Subscribe { .. } => Response::err(error_payload(
                ErrorCode::InvalidRequest,
                "subscribe must be handled by the streaming IPC path",
                false,
            ))
            .into(),

            // Control
            Request::Refresh { repo } => {
                // Force reload from git (invalidates cached state).
                // Used after external changes like migration.
                match self.force_reload(&repo, git_tx) {
                    Ok(_) => Response::ok(ResponsePayload::refreshed()),
                    Err(e) => Response::err(e),
                }
                .into()
            }

            Request::Sync { repo } => {
                // Force immediate sync (used for graceful shutdown)
                match self.ensure_loaded_and_maybe_start_sync(&repo, git_tx) {
                    Ok(_) => Response::ok(ResponsePayload::synced()),
                    Err(e) => Response::err(e),
                }
                .into()
            }

            Request::SyncWait { .. } => {
                unreachable!("SyncWait is handled by the daemon state loop")
            }

            Request::Init { repo } => {
                let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
                if git_tx
                    .send(GitOp::Init {
                        repo: repo.clone(),
                        respond: respond_tx,
                    })
                    .is_err()
                {
                    return Response::err(error_payload(
                        ErrorCode::Internal,
                        "git thread not responding",
                        false,
                    ))
                    .into();
                }

                match respond_rx.recv() {
                    Ok(Ok(())) => match self.ensure_repo_loaded(&repo, git_tx) {
                        Ok(_) => Response::ok(ResponsePayload::initialized()),
                        Err(e) => Response::err(e),
                    },
                    Ok(Err(e)) => {
                        Response::err(error_payload(ErrorCode::InitFailed, &e.to_string(), false))
                    }
                    Err(_) => {
                        Response::err(error_payload(ErrorCode::Internal, "git thread died", false))
                    }
                }
                .into()
            }

            Request::Ping => Response::ok(ResponsePayload::Query(QueryResult::DaemonInfo(
                ApiDaemonInfo {
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    protocol_version: super::ipc::IPC_PROTOCOL_VERSION,
                    pid: std::process::id(),
                },
            )))
            .into(),

            Request::Shutdown => {
                self.begin_shutdown();
                Response::ok(ResponsePayload::shutting_down()).into()
            }
        }
    }
}

fn error_payload(code: ErrorCode, message: &str, retryable: bool) -> ErrorPayload {
    ErrorPayload::new(code, message, retryable)
}

fn invalid_id_payload(err: CoreError) -> ErrorPayload {
    match err {
        CoreError::InvalidId(id) => IpcError::from(id).into(),
        other => ErrorPayload::new(ErrorCode::InternalError, other.to_string(), false),
    }
}

fn load_timeout() -> Duration {
    let override_secs = std::env::var("BD_LOAD_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|v| *v > 0);
    Duration::from_secs(override_secs.unwrap_or(LOAD_TIMEOUT_SECS))
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
    let mut ids = std::collections::BTreeSet::new();
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

fn store_id_from_remote(remote: &RemoteUrl) -> StoreId {
    let store_uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, remote.as_str().as_bytes());
    StoreId::new(store_uuid)
}

const CLOCK_SKEW_WARN_MS: u64 = 5 * 60 * 1000;

pub(crate) fn detect_clock_skew(now_ms: u64, reference_ms: u64) -> Option<ClockSkewRecord> {
    let delta_ms = now_ms as i64 - reference_ms as i64;
    if delta_ms.unsigned_abs() >= CLOCK_SKEW_WARN_MS {
        Some(ClockSkewRecord {
            delta_ms,
            wall_ms: now_ms,
        })
    } else {
        None
    }
}

fn max_write_stamp(a: Option<WriteStamp>, b: Option<WriteStamp>) -> Option<WriteStamp> {
    match (a, b) {
        (Some(a), Some(b)) => Some(std::cmp::max(a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn replay_event_wal(
    store_id: StoreId,
    wal_index: &dyn WalIndex,
    state: &mut StoreState,
    limits: &Limits,
) -> Result<bool, StoreRuntimeError> {
    let store_dir = crate::paths::store_dir(store_id);
    let rows = wal_index.reader().load_watermarks()?;
    if rows.is_empty() {
        return Ok(false);
    }

    let mut segment_cache: HashMap<NamespaceId, HashMap<SegmentId, PathBuf>> = HashMap::new();
    let mut applied_any = false;

    for row in rows {
        if row.applied_seq == 0 {
            continue;
        }
        let namespace = row.namespace.clone();
        if !segment_cache.contains_key(&namespace) {
            let segments = segment_paths_for_namespace(&store_dir, wal_index, &namespace)?;
            segment_cache.insert(namespace.clone(), segments);
        }
        let segments = segment_cache.get(&namespace).ok_or_else(|| {
            StoreRuntimeError::WalIndex(WalIndexError::SegmentRowDecode(
                "segment cache missing".to_string(),
            ))
        })?;

        let state_for_namespace = state.ensure_namespace(namespace.clone());
        let mut from_seq_excl = 0u64;
        while from_seq_excl < row.applied_seq {
            let items = wal_index.reader().iter_from(
                &namespace,
                &row.origin,
                from_seq_excl,
                limits.max_event_batch_bytes,
            )?;
            if items.is_empty() {
                return Err(StoreRuntimeError::WalReplay(Box::new(
                    WalReplayError::NonContiguousSeq {
                        namespace: row.namespace.as_str().to_string(),
                        origin: row.origin,
                        expected: from_seq_excl + 1,
                        got: 0,
                    },
                )));
            }
            for item in items {
                let seq = item.event_id.origin_seq.get();
                if seq > row.applied_seq {
                    from_seq_excl = row.applied_seq;
                    break;
                }
                let segment_path = segments.get(&item.segment_id).ok_or_else(|| {
                    StoreRuntimeError::WalIndex(WalIndexError::SegmentRowDecode(format!(
                        "missing segment {} for {}",
                        item.segment_id,
                        namespace.as_str(),
                    )))
                })?;
                let event_body = load_event_body_at(segment_path, item.offset, limits)?;
                apply_event(state_for_namespace, &event_body).map_err(|err| {
                    StoreRuntimeError::WalReplay(Box::new(WalReplayError::RecordDecode {
                        path: segment_path.clone(),
                        source: EventWalError::RecordHeaderInvalid {
                            reason: format!("apply_event failed: {err}"),
                        },
                    }))
                })?;
                from_seq_excl = seq;
                applied_any = true;
            }
        }
    }

    Ok(applied_any)
}

fn segment_paths_for_namespace(
    store_dir: &Path,
    wal_index: &dyn WalIndex,
    namespace: &NamespaceId,
) -> Result<HashMap<SegmentId, PathBuf>, StoreRuntimeError> {
    let segments = wal_index.reader().list_segments(namespace)?;
    let mut map = HashMap::new();
    for segment in segments {
        let path = if segment.segment_path.is_absolute() {
            segment.segment_path
        } else {
            store_dir.join(&segment.segment_path)
        };
        map.insert(segment.segment_id, path);
    }
    Ok(map)
}

fn load_event_body_at(
    path: &Path,
    offset: u64,
    limits: &Limits,
) -> Result<EventBody, StoreRuntimeError> {
    let mut file = File::open(path).map_err(|source| {
        StoreRuntimeError::WalReplay(Box::new(WalReplayError::Io {
            path: path.to_path_buf(),
            source,
        }))
    })?;
    file.seek(SeekFrom::Start(offset)).map_err(|source| {
        StoreRuntimeError::WalReplay(Box::new(WalReplayError::Io {
            path: path.to_path_buf(),
            source,
        }))
    })?;

    let mut reader = FrameReader::new(file, limits.max_wal_record_bytes);
    let record = reader
        .read_next()
        .map_err(|source| {
            StoreRuntimeError::WalReplay(Box::new(WalReplayError::RecordDecode {
                path: path.to_path_buf(),
                source,
            }))
        })?
        .ok_or_else(|| {
            StoreRuntimeError::WalReplay(Box::new(WalReplayError::RecordDecode {
                path: path.to_path_buf(),
                source: EventWalError::FrameLengthInvalid {
                    reason: "unexpected eof while reading record".to_string(),
                },
            }))
        })?;

    let (_, event_body) = decode_event_body(record.payload.as_ref(), limits).map_err(|source| {
        StoreRuntimeError::WalReplay(Box::new(WalReplayError::EventBodyDecode {
            path: path.to_path_buf(),
            offset,
            source,
        }))
    })?;
    Ok(event_body)
}

fn trim_non_empty(raw: Option<String>) -> Option<String> {
    raw.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn parse_durability(raw: Option<String>) -> Result<DurabilityClass, OpError> {
    let Some(raw) = trim_non_empty(raw) else {
        return Ok(DurabilityClass::LocalFsync);
    };
    let value = raw.trim().to_lowercase();
    if value == "local_fsync" || value == "local-fsync" {
        return Ok(DurabilityClass::LocalFsync);
    }
    if let Some(rest) = value.strip_prefix("replicated_fsync") {
        let rest = rest.trim();
        let rest = rest
            .strip_prefix('(')
            .and_then(|s| s.strip_suffix(')'))
            .or_else(|| rest.strip_prefix(':'))
            .or_else(|| rest.strip_prefix('='))
            .map(str::trim);
        if let Some(k_raw) = rest {
            let k = k_raw.parse::<u32>().ok().and_then(NonZeroU32::new);
            if let Some(k) = k {
                return Ok(DurabilityClass::ReplicatedFsync { k });
            }
        }
    }

    Err(OpError::InvalidRequest {
        field: Some("durability".into()),
        reason: format!("unsupported durability class: {raw}"),
    })
}

fn replication_listen_addr() -> String {
    std::env::var("BD_REPL_LISTEN_ADDR")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "127.0.0.1:0".to_string())
}

fn replication_max_connections() -> Option<NonZeroUsize> {
    if let Ok(raw) = std::env::var("BD_REPL_MAX_CONNECTIONS") {
        match raw.trim().parse::<usize>() {
            Ok(parsed) => return NonZeroUsize::new(parsed),
            Err(err) => {
                tracing::warn!("invalid BD_REPL_MAX_CONNECTIONS: {err}");
            }
        }
    }
    NonZeroUsize::new(DEFAULT_REPL_MAX_CONNECTIONS)
}

pub(crate) fn load_replica_roster(store_id: StoreId) -> Result<Option<ReplicaRoster>, OpError> {
    let path = paths::replicas_path(store_id);
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(OpError::ValidationFailed {
                field: "replicas".into(),
                reason: format!("failed to read {}: {err}", path.display()),
            });
        }
    };

    ReplicaRoster::from_toml_str(&raw)
        .map(Some)
        .map_err(|err| OpError::ValidationFailed {
            field: "replicas".into(),
            reason: format!("replica roster parse failed: {err}"),
        })
}

fn event_id_for(origin: ReplicaId, namespace: NamespaceId, origin_seq: Seq1) -> EventId {
    EventId::new(origin, namespace, origin_seq)
}

fn segment_rel_path(store_dir: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(store_dir).unwrap_or(path).to_path_buf()
}

fn event_wal_error_payload(
    namespace: &NamespaceId,
    segment_id: Option<SegmentId>,
    offset: Option<u64>,
    err: EventWalError,
) -> ErrorPayload {
    ErrorPayload::new(ErrorCode::WalCorrupt, "wal error", true).with_details(
        error_details::WalCorruptDetails {
            namespace: namespace.clone(),
            segment_id,
            offset,
            reason: err.to_string(),
        },
    )
}

fn apply_event_error_payload(
    namespace: &NamespaceId,
    origin: &ReplicaId,
    err: ApplyError,
) -> ErrorPayload {
    ErrorPayload::new(
        ErrorCode::Corruption,
        format!("apply_event rejected for {namespace}/{origin}: {err}"),
        false,
    )
}

fn wal_index_error_payload(err: &WalIndexError) -> ErrorPayload {
    match err {
        WalIndexError::Equivocation {
            namespace,
            origin,
            seq,
            existing_sha256,
            new_sha256,
        } => ErrorPayload::new(ErrorCode::Equivocation, "equivocation", false).with_details(
            error_details::EquivocationDetails {
                eid: error_details::EventIdDetails {
                    namespace: namespace.clone(),
                    origin_replica_id: *origin,
                    origin_seq: *seq,
                },
                existing_sha256: hex::encode(existing_sha256),
                new_sha256: hex::encode(new_sha256),
            },
        ),
        WalIndexError::ClientRequestIdReuseMismatch {
            namespace,
            client_request_id,
            expected_request_sha256,
            got_request_sha256,
            ..
        } => ErrorPayload::new(
            ErrorCode::ClientRequestIdReuseMismatch,
            "client_request_id reuse mismatch",
            false,
        )
        .with_details(error_details::ClientRequestIdReuseMismatchDetails {
            namespace: namespace.clone(),
            client_request_id: *client_request_id,
            expected_request_sha256: hex::encode(expected_request_sha256),
            got_request_sha256: hex::encode(got_request_sha256),
        }),
        _ => ErrorPayload::new(ErrorCode::IndexCorrupt, "index error", true).with_details(
            error_details::IndexCorruptDetails {
                reason: err.to_string(),
            },
        ),
    }
}

fn watermark_error_payload(
    namespace: &NamespaceId,
    origin: &ReplicaId,
    err: WatermarkError,
) -> ErrorPayload {
    match err {
        WatermarkError::NonContiguous { expected, got } => {
            let durable_seen = expected.prev_seq0().get();
            ErrorPayload::new(ErrorCode::GapDetected, "gap detected", false).with_details(
                error_details::GapDetectedDetails {
                    namespace: namespace.clone(),
                    origin_replica_id: *origin,
                    durable_seen,
                    got_seq: got.get(),
                },
            )
        }
        other => ErrorPayload::new(ErrorCode::Internal, other.to_string(), false),
    }
}

#[cfg(test)]
pub(crate) fn insert_store_for_tests(
    daemon: &mut Daemon,
    store_id: StoreId,
    remote: RemoteUrl,
    repo_path: &Path,
) -> Result<(), OpError> {
    let open = StoreRuntime::open(
        store_id,
        remote.clone(),
        Arc::clone(&daemon.wal),
        WallClock::now().0,
        env!("CARGO_PKG_VERSION"),
        daemon.limits(),
    )
    .map_err(|err| OpError::StoreRuntime(Box::new(err)))?;
    daemon.stores.insert(store_id, open.runtime);
    daemon.remote_to_store_id.insert(remote.clone(), store_id);
    daemon
        .path_to_store_id
        .insert(repo_path.to_owned(), store_id);
    daemon.path_to_remote.insert(repo_path.to_owned(), remote);
    daemon.register_default_checkpoint_groups(store_id)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use bytes::Bytes;
    use uuid::Uuid;

    use crate::core::{
        ActorId, Applied, Bead, BeadCore, BeadFields, BeadId, BeadType, CanonicalState, Claim,
        EventBody, EventKindV1, HeadStatus, HlcMax, Labels, Limits, Lww, NamespaceId,
        NamespacePolicy, NoteAppendV1, NoteId, PrevVerified, Priority, ReplicaId, SegmentId, Seq0,
        Seq1, Sha256, Stamp, StoreEpoch, StoreId, StoreIdentity, StoreMeta, StoreMetaVersions,
        TxnDeltaV1, TxnId, TxnOpV1, VerifiedEvent, WallClock, Watermarks, WireBeadPatch, WireNoteV1,
        WireStamp, Workflow, WriteStamp, encode_event_body_canonical, hash_event_body,
    };
    use crate::daemon::ops::OpResult;
    use crate::daemon::git_worker::LoadResult;
    use crate::daemon::store_runtime::StoreRuntime;
    use crate::daemon::wal::frame::encode_frame;
    use crate::daemon::wal::{Record, RecordHeader, SegmentHeader, Wal};
    use tempfile::TempDir;

    fn test_actor() -> ActorId {
        ActorId::new("test@host".to_string()).unwrap()
    }

    fn test_remote() -> RemoteUrl {
        RemoteUrl("example.com/test/repo".into())
    }

    struct TempStoreDir {
        _temp: TempDir,
        data_dir: PathBuf,
        _override: crate::paths::DataDirOverride,
    }

    impl TempStoreDir {
        fn new() -> Self {
            let temp = TempDir::new().unwrap();
            let data_dir = temp.path().join("data");
            std::fs::create_dir_all(&data_dir).unwrap();
            let override_guard = crate::paths::override_data_dir_for_tests(Some(data_dir.clone()));

            Self {
                _temp: temp,
                data_dir,
                _override: override_guard,
            }
        }

        fn data_dir(&self) -> &Path {
            &self.data_dir
        }
    }

    fn test_wal() -> (TempStoreDir, Wal) {
        let tmp = TempStoreDir::new();
        let wal = Wal::new(tmp.data_dir()).unwrap();
        (tmp, wal)
    }

    fn insert_store(daemon: &mut Daemon, remote: &RemoteUrl) -> StoreId {
        let store_id = store_id_from_remote(remote);
        let runtime = StoreRuntime::open(
            store_id,
            remote.clone(),
            Arc::clone(&daemon.wal),
            WallClock::now().0,
            env!("CARGO_PKG_VERSION"),
            daemon.limits(),
        )
        .unwrap()
        .runtime;
        daemon.remote_to_store_id.insert(remote.clone(), store_id);
        daemon.stores.insert(store_id, runtime);
        store_id
    }

    fn verified_event_with_delta(
        store: StoreIdentity,
        namespace: &NamespaceId,
        origin: ReplicaId,
        seq: u64,
        prev: Option<Sha256>,
        delta: TxnDeltaV1,
    ) -> VerifiedEvent<PrevVerified> {
        let event_time_ms = 1_700_000_000_000 + seq;
        let body = EventBody {
            envelope_v: 1,
            store,
            namespace: namespace.clone(),
            origin_replica_id: origin,
            origin_seq: Seq1::from_u64(seq).unwrap(),
            event_time_ms,
            txn_id: TxnId::new(Uuid::from_bytes([seq as u8; 16])),
            client_request_id: None,
            kind: EventKindV1::TxnV1,
            delta,
            hlc_max: Some(HlcMax {
                actor_id: ActorId::new("alice").unwrap(),
                physical_ms: event_time_ms,
                logical: 0,
            }),
        };
        let bytes = encode_event_body_canonical(&body).unwrap();
        let sha256 = hash_event_body(&bytes);
        VerifiedEvent {
            body,
            bytes: bytes.into(),
            sha256,
            prev: PrevVerified { prev },
        }
    }

    fn verified_event_for_seq(
        store: StoreIdentity,
        namespace: &NamespaceId,
        origin: ReplicaId,
        seq: u64,
        prev: Option<Sha256>,
    ) -> VerifiedEvent<PrevVerified> {
        verified_event_with_delta(
            store,
            namespace,
            origin,
            seq,
            prev,
            TxnDeltaV1::new(),
        )
    }

    fn record_for_event(event: &VerifiedEvent<PrevVerified>) -> Record {
        Record {
            header: RecordHeader {
                origin_replica_id: event.body.origin_replica_id,
                origin_seq: event.body.origin_seq.get(),
                event_time_ms: event.body.event_time_ms,
                txn_id: event.body.txn_id,
                client_request_id: event.body.client_request_id,
                request_sha256: None,
                sha256: event.sha256.0,
                prev_sha256: event.prev.prev.map(|sha| sha.0),
            },
            payload: Bytes::copy_from_slice(event.bytes.as_ref()),
        }
    }

    fn make_stamp(wall_ms: u64, actor: &str) -> Stamp {
        Stamp::new(
            WriteStamp::new(wall_ms, 0),
            ActorId::new(actor.to_string()).unwrap(),
        )
    }

    fn make_bead(id: &str, wall_ms: u64, actor: &str) -> Bead {
        let stamp = make_stamp(wall_ms, actor);
        let core = BeadCore::new(BeadId::parse(id).unwrap(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("test".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            labels: Lww::new(Labels::new(), stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::Open, stamp.clone()),
            claim: Lww::new(Claim::default(), stamp),
        };
        Bead::new(core, fields)
    }

    #[test]
    fn complete_refresh_clears_in_progress_flag() {
        let (_tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);

        // Manually insert a repo in refresh_in_progress state
        let mut repo_state = RepoState::new();
        repo_state.refresh_in_progress = true;
        daemon.stores.get_mut(&store_id).unwrap().repo_state = repo_state;

        // Complete refresh with success
        let result = Ok(LoadResult {
            state: CanonicalState::new(),
            root_slug: None,
            needs_sync: false,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(&remote, result);

        let repo_state = &daemon.stores.get(&store_id).unwrap().repo_state;
        assert!(!repo_state.refresh_in_progress);
        assert!(repo_state.last_refresh.is_some());
    }

    #[test]
    fn read_gate_requires_min_seen() {
        let (_tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);
        let proof = LoadedStore {
            store_id,
            remote: remote.clone(),
        };

        let namespace = NamespaceId::core();
        let origin = daemon
            .stores
            .get(&store_id)
            .expect("store runtime")
            .meta
            .replica_id;
        let mut required = Watermarks::<Applied>::new();
        required
            .observe_at_least(&namespace, &origin, Seq0::new(1), HeadStatus::Unknown)
            .expect("watermark");

        let read = NormalizedReadConsistency {
            namespace: namespace.clone(),
            require_min_seen: Some(required.clone()),
            wait_timeout_ms: 0,
        };
        let err = daemon.check_read_gate(&proof, &read).unwrap_err();
        match err {
            OpError::RequireMinSeenUnsatisfied {
                required: got_required,
                current_applied,
            } => {
                assert_eq!(got_required.as_ref(), &required);
                let current_seq = current_applied
                    .as_ref()
                    .get(&namespace, &origin)
                    .map(|watermark| watermark.seq().get())
                    .unwrap_or(0);
                assert_eq!(current_seq, 0);
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let read = NormalizedReadConsistency {
            namespace: namespace.clone(),
            require_min_seen: Some(required.clone()),
            wait_timeout_ms: 50,
        };
        let err = daemon.check_read_gate(&proof, &read).unwrap_err();
        match err {
            OpError::RequireMinSeenTimeout {
                waited_ms,
                required: got_required,
                ..
            } => {
                assert_eq!(waited_ms, 50);
                assert_eq!(got_required.as_ref(), &required);
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let runtime = daemon.stores.get_mut(&store_id).expect("store runtime");
        runtime
            .watermarks_applied
            .observe_at_least(&namespace, &origin, Seq0::new(1), HeadStatus::Unknown)
            .expect("watermark");
        let read = NormalizedReadConsistency {
            namespace,
            require_min_seen: Some(required),
            wait_timeout_ms: 0,
        };
        assert!(daemon.check_read_gate(&proof, &read).is_ok());
    }

    #[test]
    fn complete_refresh_clears_flag_on_error() {
        let (_tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);

        let mut repo_state = RepoState::new();
        repo_state.refresh_in_progress = true;
        daemon.stores.get_mut(&store_id).unwrap().repo_state = repo_state;

        // Complete refresh with error
        let result = Err(SyncError::NoLocalRef("/test".to_string()));
        daemon.complete_refresh(&remote, result);

        let repo_state = &daemon.stores.get(&store_id).unwrap().repo_state;
        assert!(!repo_state.refresh_in_progress);
        // last_refresh should NOT be updated on error
        assert!(repo_state.last_refresh.is_none());
    }

    #[test]
    fn complete_refresh_applies_state_when_clean() {
        let (_tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);

        let mut repo_state = RepoState::new();
        repo_state.refresh_in_progress = true;
        repo_state.dirty = false; // Clean state
        daemon.stores.get_mut(&store_id).unwrap().repo_state = repo_state;

        // Create fresh state with some content
        let fresh_state = CanonicalState::new();
        let result = Ok(LoadResult {
            state: fresh_state.clone(),
            root_slug: Some("fresh-slug".to_string()),
            needs_sync: false,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(&remote, result);

        let repo_state = &daemon.stores.get(&store_id).unwrap().repo_state;
        assert_eq!(repo_state.root_slug, Some("fresh-slug".to_string()));
    }

    #[test]
    fn complete_refresh_skips_state_when_dirty() {
        let (_tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);

        let mut repo_state = RepoState::new();
        repo_state.refresh_in_progress = true;
        repo_state.dirty = true; // Dirty - mutations happened during refresh
        repo_state.root_slug = Some("original-slug".to_string());
        daemon.stores.get_mut(&store_id).unwrap().repo_state = repo_state;

        // Try to apply refresh
        let result = Ok(LoadResult {
            state: CanonicalState::new(),
            root_slug: Some("new-slug".to_string()),
            needs_sync: false,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(&remote, result);

        let repo_state = &daemon.stores.get(&store_id).unwrap().repo_state;
        // Should keep original slug since dirty
        assert_eq!(repo_state.root_slug, Some("original-slug".to_string()));
        // But last_refresh should still be updated
        assert!(repo_state.last_refresh.is_some());
    }

    #[test]
    fn complete_refresh_schedules_sync_when_needs_sync() {
        let (_tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);

        let mut repo_state = RepoState::new();
        repo_state.refresh_in_progress = true;
        repo_state.dirty = false;
        daemon.stores.get_mut(&store_id).unwrap().repo_state = repo_state;

        // Refresh shows local has changes remote doesn't
        let result = Ok(LoadResult {
            state: CanonicalState::new(),
            root_slug: None,
            needs_sync: true,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(&remote, result);

        let repo_state = &daemon.stores.get(&store_id).unwrap().repo_state;
        // Should mark dirty to trigger sync
        assert!(repo_state.dirty);
        // And scheduler should have it pending
        assert!(daemon.scheduler.is_pending(&remote));
    }

    #[test]
    fn complete_refresh_unknown_remote_is_noop() {
        let (_tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let unknown = RemoteUrl("unknown.com/repo".into());

        // Should not panic on unknown remote
        let result = Ok(LoadResult {
            state: CanonicalState::new(),
            root_slug: None,
            needs_sync: false,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(&unknown, result);
        // Just verify no panic and daemon is still valid
        assert!(daemon.stores.is_empty());
    }

    #[test]
    fn complete_sync_success_clean_deletes_wal() {
        use crate::daemon::wal::WalEntry;

        let (tmp, wal) = test_wal();
        let remote = test_remote();

        // Write a WAL entry
        let entry = WalEntry::new(CanonicalState::new(), None, 1, 0);
        wal.write(&remote, &entry).unwrap();
        assert!(wal.exists(&remote));

        // Create daemon with WAL, recreating from same dir
        let wal = Wal::new(tmp.data_dir()).unwrap();
        let mut daemon = Daemon::new(test_actor(), wal);
        let store_id = insert_store(&mut daemon, &remote);

        // Insert a clean repo state
        let mut repo_state = RepoState::new();
        repo_state.dirty = false;
        daemon.stores.get_mut(&store_id).unwrap().repo_state = repo_state;

        // Complete sync with success
        let outcome = SyncOutcome {
            state: CanonicalState::new(),
            divergence: None,
            force_push: None,
            last_seen_stamp: None,
        };
        daemon.complete_sync(&remote, Ok(outcome));

        // WAL should be deleted after successful sync on clean state
        assert!(!daemon.wal.exists(&remote));
    }

    #[test]
    fn complete_sync_success_dirty_keeps_wal() {
        use crate::daemon::wal::WalEntry;

        let (tmp, wal) = test_wal();
        let remote = test_remote();

        // Write a WAL entry
        let entry = WalEntry::new(CanonicalState::new(), None, 1, 0);
        wal.write(&remote, &entry).unwrap();
        assert!(wal.exists(&remote));

        // Create daemon with WAL
        let wal = Wal::new(tmp.data_dir()).unwrap();
        let mut daemon = Daemon::new(test_actor(), wal);
        let store_id = insert_store(&mut daemon, &remote);

        // Insert a DIRTY repo state (mutations happened during sync)
        let mut repo_state = RepoState::new();
        repo_state.dirty = true;
        daemon.stores.get_mut(&store_id).unwrap().repo_state = repo_state;

        // Complete sync with success
        let outcome = SyncOutcome {
            state: CanonicalState::new(),
            divergence: None,
            force_push: None,
            last_seen_stamp: None,
        };
        daemon.complete_sync(&remote, Ok(outcome));

        // WAL should NOT be deleted - dirty state needs another sync
        assert!(daemon.wal.exists(&remote));
    }

    #[test]
    fn complete_sync_failure_keeps_wal() {
        use crate::daemon::wal::WalEntry;

        let (tmp, wal) = test_wal();
        let remote = test_remote();

        // Write a WAL entry
        let entry = WalEntry::new(CanonicalState::new(), None, 1, 0);
        wal.write(&remote, &entry).unwrap();
        assert!(wal.exists(&remote));

        // Create daemon with WAL
        let wal = Wal::new(tmp.data_dir()).unwrap();
        let mut daemon = Daemon::new(test_actor(), wal);
        let store_id = insert_store(&mut daemon, &remote);

        // Insert repo state
        let repo_state = RepoState::new();
        daemon.stores.get_mut(&store_id).unwrap().repo_state = repo_state;

        // Complete sync with failure
        daemon.complete_sync(&remote, Err(SyncError::NonFastForward));

        // WAL should NOT be deleted on failure
        assert!(daemon.wal.exists(&remote));
    }

    #[test]
    fn wal_write_failure_aborts_mutation() {
        let (tmp, wal) = test_wal();
        let remote = test_remote();

        // Remove WAL directory to force write failure.
        let wal_dir = tmp.data_dir().join("wal");
        std::fs::remove_dir_all(&wal_dir).unwrap();

        let mut daemon = Daemon::new(test_actor(), wal);
        let store_id = insert_store(&mut daemon, &remote);

        let proof = LoadedStore {
            store_id,
            remote: remote.clone(),
        };
        let result = daemon.apply_wal_mutation(&proof, |state, stamp| {
            let bead = make_bead("bd-abc", stamp.at.wall_ms, stamp.by.as_str());
            state.insert_live(bead);
            Ok(())
        });

        assert!(matches!(result, Err(OpError::Wal(_))));
        let repo_state = &daemon.stores.get(&store_id).unwrap().repo_state;
        let live_count = repo_state
            .state
            .get(&NamespaceId::core())
            .map(|state| state.live_count())
            .unwrap_or(0);
        assert_eq!(live_count, 0);
        assert_eq!(repo_state.wal_sequence, 0);
    }

    #[test]
    fn apply_wal_mutation_clamps_to_last_seen_stamp() {
        let (_tmp, wal) = test_wal();
        let remote = test_remote();
        let mut daemon = Daemon::new(test_actor(), wal);
        let store_id = insert_store(&mut daemon, &remote);

        let future_stamp = WriteStamp::new(WallClock::now().0 + 60_000, 0);
        let mut repo_state = RepoState::new();
        repo_state.last_seen_stamp = Some(future_stamp.clone());
        daemon.stores.get_mut(&store_id).unwrap().repo_state = repo_state;

        let proof = LoadedStore {
            store_id,
            remote: remote.clone(),
        };
        let result = daemon.apply_wal_mutation(&proof, |state, stamp| {
            let bead = make_bead("bd-abc", stamp.at.wall_ms, stamp.by.as_str());
            state.insert_live(bead);
            Ok(stamp.at.clone())
        });

        let applied_stamp = result.unwrap();
        assert!(applied_stamp >= future_stamp);
    }

    #[test]
    fn non_core_mutation_and_query_use_namespace_state() {
        let (tmp, wal) = test_wal();
        let mut daemon = Daemon::new(test_actor(), wal);
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        let store_id = store_id_from_remote(&remote);
        insert_store_for_tests(&mut daemon, store_id, remote.clone(), &repo_path).unwrap();

        let tmp_ns = NamespaceId::parse("tmp").unwrap();
        {
            let runtime = daemon.stores.get_mut(&store_id).unwrap();
            runtime
                .policies
                .insert(tmp_ns.clone(), NamespacePolicy::tmp_default());
        }

        let (git_tx, _git_rx) = crossbeam::channel::unbounded();
        let response = daemon.handle_request(
            Request::Create {
                repo: repo_path.clone(),
                id: None,
                parent: None,
                title: "non-core".to_string(),
                bead_type: BeadType::Task,
                priority: Priority::MEDIUM,
                description: None,
                design: None,
                acceptance_criteria: None,
                assignee: None,
                external_ref: None,
                estimated_minutes: None,
                labels: Vec::new(),
                dependencies: Vec::new(),
                meta: MutationMeta {
                    namespace: Some(tmp_ns.as_str().to_string()),
                    ..MutationMeta::default()
                },
            },
            &git_tx,
        );

        let created_id = match response {
            HandleOutcome::Response(Response::Ok {
                ok: ResponsePayload::Op(op),
            }) => match op.result {
                OpResult::Created { id } => id,
                other => panic!("unexpected op result: {other:?}"),
            },
            other => panic!("unexpected outcome: {other:?}"),
        };

        let read = ReadConsistency {
            namespace: Some(tmp_ns.as_str().to_string()),
            ..ReadConsistency::default()
        };
        let response = daemon.query_show(&repo_path, &created_id, read, &git_tx);
        match response {
            Response::Ok {
                ok: ResponsePayload::Query(QueryResult::Issue(issue)),
            } => {
                assert_eq!(issue.id, created_id.as_str());
            }
            other => panic!("unexpected query response: {other:?}"),
        }

        let repo_state = &daemon.stores.get(&store_id).unwrap().repo_state;
        let core_count = repo_state
            .state
            .get(&NamespaceId::core())
            .map(|state| state.live_count())
            .unwrap_or(0);
        let tmp_count = repo_state
            .state
            .get(&tmp_ns)
            .map(|state| state.live_count())
            .unwrap_or(0);
        assert_eq!(core_count, 0);
        assert_eq!(tmp_count, 1);
    }

    #[test]
    fn repl_ingest_accepts_non_core_namespace() {
        let tmp = TempStoreDir::new();
        let namespace = NamespaceId::parse("tmp").unwrap();
        let origin = ReplicaId::new(Uuid::from_bytes([10u8; 16]));
        let store_id = StoreId::new(Uuid::from_bytes([11u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::ZERO);
        let now_ms = 1_700_000_000_000u64;

        let bead_id = BeadId::parse("bd-repl").unwrap();
        let mut patch = WireBeadPatch::new(bead_id.clone());
        patch.title = Some("replicated".to_string());
        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
            .unwrap();
        let event = verified_event_with_delta(store, &namespace, origin, 1, None, delta);

        let wal = Wal::new(tmp.data_dir()).unwrap();
        let mut daemon = Daemon::new_with_limits(test_actor(), wal, Limits::default());
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).unwrap();

        {
            let runtime = daemon.stores.get_mut(&store_id).unwrap();
            runtime
                .policies
                .insert(namespace.clone(), NamespacePolicy::core_default());
        }

        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        daemon.handle_repl_ingest(ReplIngestRequest {
            store_id,
            namespace: namespace.clone(),
            origin,
            batch: vec![event],
            now_ms,
            respond: respond_tx,
        });

        let outcome = respond_rx.recv().expect("ingest response");
        assert!(outcome.is_ok());

        let repo_state = &daemon.stores.get(&store_id).unwrap().repo_state;
        let state = repo_state
            .state
            .get(&namespace)
            .expect("namespace state");
        assert!(state.get_live(&bead_id).is_some());
    }

    #[test]
    fn complete_sync_resolves_collisions_for_dirty_state() {
        let (_tmp, wal) = test_wal();
        let remote = test_remote();
        let mut daemon = Daemon::new(test_actor(), wal);
        let store_id = insert_store(&mut daemon, &remote);

        let winner = make_bead("bd-abc", 1000, "alice");
        let loser = make_bead("bd-abc", 2000, "bob");

        let mut synced_state = CanonicalState::new();
        synced_state.insert_live(winner.clone());

        let mut local_state = CanonicalState::new();
        local_state.insert_live(loser);

        let mut repo_state = RepoState::new();
        repo_state
            .state
            .set_namespace_state(NamespaceId::core(), local_state);
        repo_state.dirty = true;
        repo_state.sync_in_progress = true;
        daemon.stores.get_mut(&store_id).unwrap().repo_state = repo_state;

        let outcome = SyncOutcome {
            last_seen_stamp: synced_state.max_write_stamp(),
            state: synced_state,
            divergence: None,
            force_push: None,
        };
        daemon.complete_sync(&remote, Ok(outcome));

        let repo_state = &daemon.stores.get(&store_id).unwrap().repo_state;
        let core_state = repo_state
            .state
            .get(&NamespaceId::core())
            .expect("core state");
        assert_eq!(core_state.live_count(), 2);

        let id = BeadId::parse("bd-abc").unwrap();
        let merged = core_state.get_live(&id).unwrap();
        assert_eq!(merged.core.created(), winner.core.created());
    }

    #[test]
    fn ingest_rotation_seals_previous_segment() {
        let tmp = TempStoreDir::new();
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
        let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::ZERO);
        let now_ms = 1_700_000_000_000u64;

        let event1 = verified_event_for_seq(store, &namespace, origin, 1, None);
        let record1 = record_for_event(&event1);

        let versions = StoreMetaVersions::new(1, 2, 3, 4, 5);
        let meta = StoreMeta::new(
            store,
            ReplicaId::new(Uuid::from_bytes([8u8; 16])),
            versions,
            now_ms,
        );
        let header_len = SegmentHeader::new(
            &meta,
            namespace.clone(),
            now_ms,
            SegmentId::new(Uuid::nil()),
        )
        .encode()
        .unwrap()
        .len() as u64;

        let mut limits = Limits::default();
        let frame_len = encode_frame(&record1, limits.max_wal_record_bytes)
            .unwrap()
            .len() as u64;
        limits.wal_segment_max_bytes = (header_len + frame_len + 1) as usize;

        let wal = Wal::new(tmp.data_dir()).unwrap();
        let mut daemon = Daemon::new_with_limits(test_actor(), wal, limits.clone());
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).unwrap();

        let event2 = verified_event_for_seq(store, &namespace, origin, 2, Some(event1.sha256));
        daemon
            .ingest_remote_batch(store_id, namespace.clone(), origin, vec![event1, event2], now_ms)
            .expect("ingest");

        let store_runtime = daemon.stores.get(&store_id).expect("store runtime");
        let segments = store_runtime
            .wal_index
            .reader()
            .list_segments(&namespace)
            .expect("segments");
        assert_eq!(segments.len(), 2);

        let sealed = segments.iter().find(|row| row.sealed).expect("sealed");
        assert!(segments.iter().any(|row| !row.sealed));
        let sealed_path = paths::store_dir(store_id).join(&sealed.segment_path);
        let sealed_len = std::fs::metadata(&sealed_path)
            .expect("sealed segment metadata")
            .len();
        assert_eq!(sealed.final_len, Some(sealed_len));
    }

    #[test]
    fn ingest_rejects_apply_failure_before_append() {
        let tmp = TempStoreDir::new();
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([10u8; 16]));
        let store_id = StoreId::new(Uuid::from_bytes([11u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::ZERO);
        let now_ms = 1_700_000_000_000u64;

        let mut delta = TxnDeltaV1::new();
        let note_id = NoteId::new("note-1").unwrap();
        let note = WireNoteV1 {
            id: note_id,
            content: "hi".to_string(),
            author: ActorId::new("alice").unwrap(),
            at: WireStamp(now_ms, 0),
        };
        delta
            .insert(TxnOpV1::NoteAppend(NoteAppendV1 {
                bead_id: BeadId::parse("bd-missing").unwrap(),
                note,
            }))
            .unwrap();

        let event = verified_event_with_delta(store, &namespace, origin, 1, None, delta);

        let wal = Wal::new(tmp.data_dir()).unwrap();
        let mut daemon = Daemon::new_with_limits(test_actor(), wal, Limits::default());
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).unwrap();

        let err = daemon
            .ingest_remote_batch(store_id, namespace.clone(), origin, vec![event], now_ms)
            .expect_err("apply failure should reject batch");
        assert_eq!(err.code, ErrorCode::Corruption);

        let store_runtime = daemon.stores.get(&store_id).expect("store runtime");
        let segments = store_runtime
            .wal_index
            .reader()
            .list_segments(&namespace)
            .expect("segments");
        assert!(segments.is_empty());
    }

    #[test]
    fn detect_clock_skew_flags_large_delta() {
        let now = 1_700_000_000_000u64;
        let reference = now - (CLOCK_SKEW_WARN_MS + 1);
        let skew = detect_clock_skew(now, reference).unwrap();
        assert!(skew.delta_ms > 0);
        assert_eq!(skew.wall_ms, now);
    }

    #[test]
    fn detect_clock_skew_ignores_small_delta() {
        let now = 1_700_000_000_000u64;
        let reference = now - (CLOCK_SKEW_WARN_MS - 1);
        assert!(detect_clock_skew(now, reference).is_none());
    }
}
