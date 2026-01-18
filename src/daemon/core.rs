//! Daemon core - the central coordinator.
//!
//! Owns all per-repo state, the HLC clock, actor identity, and sync scheduler.
//! The serialization point for all mutations - runs on a single thread.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, Seek, SeekFrom};
use std::num::NonZeroUsize;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crossbeam::channel::Sender;
use git2::{ErrorCode as GitErrorCode, ObjectType, Oid, Repository, TreeWalkMode, TreeWalkResult};
use thiserror::Error;

use super::Clock;
use super::broadcast::BroadcastEvent;
use super::checkpoint_scheduler::{
    CheckpointGroupConfig, CheckpointGroupKey, CheckpointGroupSnapshot, CheckpointScheduler,
};
use super::executor::DurabilityWait;
use super::git_lane::{
    ClockSkewRecord, DivergenceRecord, FetchErrorRecord, ForcePushRecord, GitLaneState,
};
use super::git_worker::{GitOp, LoadResult};
use super::ipc::Response;
use super::metrics;
use super::ops::OpError;
use super::remote::RemoteUrl;
use super::repl::{
    BackoffPolicy, IngestOutcome, PeerConfig, ReplError, ReplErrorDetails, ReplIngestRequest,
    ReplSessionStore, ReplicationManager, ReplicationManagerConfig, ReplicationManagerHandle,
    ReplicationServer, ReplicationServerConfig, ReplicationServerHandle, SharedSessionStore,
    WalRangeReader,
};
use super::scheduler::SyncScheduler;
use super::store::StoreCaches;
use super::store::discovery::ResolvedStore;
use super::store_runtime::{StoreRuntime, StoreRuntimeError, load_replica_roster};
use super::wal::{
    EventWalError, FrameReader, HlcRow, RecordHeader, SegmentRow, VerifiedRecord, WalIndex,
    WalIndexError, WalReplayError, open_segment_reader,
};

use crate::compat::{ExportContext, ensure_symlinks, export_jsonl};
use crate::config::CheckpointGroupConfig as ConfigCheckpointGroup;
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

#[derive(Debug, Error)]
enum CheckpointTreeError {
    #[error("checkpoint tree git error: {0}")]
    Git(#[from] git2::Error),
    #[error("checkpoint tree io error at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("checkpoint tree invalid path {path}")]
    InvalidPath { path: String },
}

use crate::core::{
    ActorId, Applied, ApplyError, CanonicalState, CliErrorCode, ClientRequestId, ContentHash,
    DurabilityClass, EventBody, EventId, EventKindV1, HeadStatus, Limits, NamespaceId,
    NamespacePolicy, PrevVerified, ProtocolErrorCode, ReplicaId, ReplicateMode, SegmentId, Seq0,
    Seq1, Sha256, StoreId, StoreIdentity, StoreState, VerifiedEvent, WallClock, Watermark,
    WatermarkError, Watermarks, WriteStamp, apply_event, decode_event_body,
};
use crate::git::SyncError;
use crate::git::checkpoint::{
    CheckpointCache, CheckpointImport, CheckpointImportError, CheckpointPublishError,
    CheckpointPublishOutcome, IncludedHeads, import_checkpoint, merge_store_states, policy_hash,
    roster_hash, store_state_from_legacy,
};

const STORE_LOCK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const LOAD_TIMEOUT_SECS: u64 = 30;
const DEFAULT_REPL_MAX_CONNECTIONS: usize = 32;

#[derive(Clone, Debug)]
pub(crate) struct ParsedMutationMeta {
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
    pub(crate) fn new(
        namespace: NamespaceId,
        require_min_seen: Option<Watermarks<Applied>>,
        wait_timeout_ms: u64,
    ) -> Self {
        Self {
            namespace,
            require_min_seen,
            wait_timeout_ms,
        }
    }

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
    /// Per-store git/checkpoint lane state, keyed by StoreId.
    git_lanes: BTreeMap<StoreId, GitLaneState>,
    /// Store discovery caches.
    store_caches: StoreCaches,

    /// HLC clock for generating timestamps.
    clock: Clock,
    /// Per-actor clocks for non-daemon actors.
    actor_clocks: BTreeMap<ActorId, Clock>,

    /// Actor identity (username@hostname).
    actor: ActorId,

    /// Sync scheduler for debouncing.
    scheduler: SyncScheduler,
    /// Checkpoint scheduler for debounce/max interval.
    checkpoint_scheduler: CheckpointScheduler,

    /// Go-compatibility export context.
    export_ctx: Option<ExportContext>,

    /// Realtime safety limits.
    limits: Limits,
    /// Replication settings loaded from config (env overrides applied during load).
    replication: crate::config::ReplicationConfig,
    /// Default checkpoint group specs from config.
    checkpoint_groups: BTreeMap<String, ConfigCheckpointGroup>,
    /// Default namespace policies when namespaces.toml is missing.
    namespace_defaults: BTreeMap<NamespaceId, NamespacePolicy>,

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
    pub fn new(actor: ActorId) -> Self {
        Self::new_with_limits(actor, Limits::default())
    }

    /// Create a new daemon with custom limits.
    pub fn new_with_limits(actor: ActorId, limits: Limits) -> Self {
        let config = crate::config::Config {
            limits,
            ..Default::default()
        };
        Self::new_with_config(actor, config)
    }

    /// Create a new daemon with config settings.
    pub fn new_with_config(actor: ActorId, config: crate::config::Config) -> Self {
        let limits = config.limits.clone();
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
            git_lanes: BTreeMap::new(),
            store_caches: StoreCaches::new(),
            clock: Clock::new_with_max_forward_drift(limits.hlc_max_forward_drift_ms),
            actor_clocks: BTreeMap::new(),
            actor,
            scheduler: SyncScheduler::new(),
            checkpoint_scheduler: CheckpointScheduler::new_with_queue_limit(
                limits.max_checkpoint_job_queue,
            ),
            export_ctx,
            limits,
            replication: config.replication.clone(),
            checkpoint_groups: config.checkpoint_groups.clone(),
            namespace_defaults: config.namespace_defaults.namespaces.clone(),
            repl_ingest_tx: None,
            repl_handles: BTreeMap::new(),
            shutting_down: false,
        }
    }

    /// Get a reference to the WAL.
    /// Get the actor identity.
    pub fn actor(&self) -> &ActorId {
        &self.actor
    }

    /// Get the clock (for creating stamps) for the daemon actor.
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

    /// Get mutable clock for the daemon actor.
    pub fn clock_mut(&mut self) -> &mut Clock {
        &mut self.clock
    }

    pub(crate) fn clock_for_actor_mut(&mut self, actor: &ActorId) -> &mut Clock {
        if *actor == self.actor {
            return &mut self.clock;
        }
        let max_drift_ms = self.limits.hlc_max_forward_drift_ms;
        self.actor_clocks
            .entry(actor.clone())
            .or_insert_with(|| Clock::new_with_max_forward_drift(max_drift_ms))
    }

    fn seed_actor_clocks(&mut self, runtime: &StoreRuntime) -> Result<(), OpError> {
        let rows = runtime.hlc_rows()?;
        for row in rows {
            let stamp = WriteStamp::new(row.last_physical_ms, row.last_logical);
            self.clock_for_actor_mut(&row.actor_id).receive(&stamp);
        }
        Ok(())
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

    fn replication_peers(&self) -> Vec<PeerConfig> {
        self.replication
            .peers
            .iter()
            .map(|peer| PeerConfig {
                replica_id: peer.replica_id,
                addr: peer.addr.clone(),
                role: peer.role,
                allowed_namespaces: peer.allowed_namespaces.clone(),
            })
            .collect()
    }

    fn register_default_checkpoint_groups(&mut self, store_id: StoreId) -> Result<(), OpError> {
        let store = self
            .stores
            .get(&store_id)
            .ok_or(OpError::Internal("loaded store missing from state"))?;
        let configs = self.checkpoint_group_configs(store_id, store.meta.replica_id);
        for config in configs {
            self.checkpoint_scheduler.register_group(config);
        }
        self.emit_checkpoint_queue_depth();
        Ok(())
    }

    fn checkpoint_group_configs(
        &self,
        store_id: StoreId,
        local_replica_id: ReplicaId,
    ) -> Vec<CheckpointGroupConfig> {
        if self.checkpoint_groups.is_empty() {
            return vec![CheckpointGroupConfig::core_default(
                store_id,
                local_replica_id,
            )];
        }

        let mut configs = Vec::new();
        for (group, spec) in &self.checkpoint_groups {
            let namespaces = if spec.namespaces.is_empty() {
                if group == "core" {
                    vec![NamespaceId::core()]
                } else {
                    tracing::warn!(
                        checkpoint_group = %group,
                        "checkpoint group has no namespaces; skipping"
                    );
                    continue;
                }
            } else {
                spec.namespaces.clone()
            };

            let mut config = CheckpointGroupConfig::core_default(store_id, local_replica_id);
            config.group = group.clone();
            config.namespaces = namespaces;
            config.git_ref = resolve_checkpoint_git_ref(store_id, group, spec.git_ref.as_deref());
            config.checkpoint_writers = if spec.checkpoint_writers.is_empty() {
                vec![local_replica_id]
            } else {
                spec.checkpoint_writers.clone()
            };
            config.primary_writer = spec.primary_writer.or(Some(local_replica_id));
            if let Some(primary_writer) = config.primary_writer
                && !config.checkpoint_writers.contains(&primary_writer)
            {
                config.checkpoint_writers.push(primary_writer);
            }
            if let Some(debounce_ms) = spec.debounce_ms {
                config.debounce = Duration::from_millis(debounce_ms);
            }
            if let Some(max_interval_ms) = spec.max_interval_ms {
                config.max_interval = Duration::from_millis(max_interval_ms);
            }
            if let Some(max_events) = spec.max_events {
                config.max_events = max_events;
            }
            config.durable_copy_via_git = spec.durable_copy_via_git;

            configs.push(config);
        }

        if configs.is_empty() {
            configs.push(CheckpointGroupConfig::core_default(
                store_id,
                local_replica_id,
            ));
        }

        configs
    }

    /// Get git lane state. Returns Internal if invariant is violated.
    pub(crate) fn git_lane_state(&self, proof: &LoadedStore) -> Result<&GitLaneState, OpError> {
        self.git_lanes
            .get(&proof.store_id)
            .ok_or(OpError::Internal("loaded store missing from state"))
    }

    /// Get mutable git lane state. Returns Internal if invariant is violated.
    pub(crate) fn git_lane_state_mut(
        &mut self,
        proof: &LoadedStore,
    ) -> Result<&mut GitLaneState, OpError> {
        self.git_lanes
            .get_mut(&proof.store_id)
            .ok_or(OpError::Internal("loaded store missing from state"))
    }

    pub(crate) fn store_runtime(&self, proof: &LoadedStore) -> Result<&StoreRuntime, OpError> {
        self.stores
            .get(&proof.store_id)
            .ok_or(OpError::Internal("loaded store missing from state"))
    }

    #[cfg(feature = "test-harness")]
    pub(crate) fn store_runtime_by_id(&self, store_id: StoreId) -> Option<&StoreRuntime> {
        self.stores.get(&store_id)
    }

    #[cfg(feature = "test-harness")]
    pub(crate) fn store_runtime_by_id_mut(
        &mut self,
        store_id: StoreId,
    ) -> Option<&mut StoreRuntime> {
        self.stores.get_mut(&store_id)
    }

    pub(crate) fn store_runtime_mut(
        &mut self,
        proof: &LoadedStore,
    ) -> Result<&mut StoreRuntime, OpError> {
        self.stores
            .get_mut(&proof.store_id)
            .ok_or(OpError::Internal("loaded store missing from state"))
    }

    pub(crate) fn store_and_lane_mut(
        &mut self,
        proof: &LoadedStore,
    ) -> Result<(&mut StoreRuntime, &mut GitLaneState), OpError> {
        let store_id = proof.store_id();
        let store = self
            .stores
            .get_mut(&store_id)
            .ok_or(OpError::Internal("loaded store missing from state"))?;
        let lane = self
            .git_lanes
            .get_mut(&store_id)
            .ok_or(OpError::Internal("loaded store missing from state"))?;
        Ok((store, lane))
    }

    pub(crate) fn store_id_for_remote(&self, remote: &RemoteUrl) -> Option<StoreId> {
        self.store_caches.remote_to_store_id.get(remote).copied()
    }

    pub(crate) fn store_and_lane_by_id_mut(
        &mut self,
        store_id: StoreId,
    ) -> Option<(&mut StoreRuntime, &mut GitLaneState)> {
        let store = self.stores.get_mut(&store_id)?;
        let lane = self.git_lanes.get_mut(&store_id)?;
        Some((store, lane))
    }

    pub(crate) fn drop_store_state(&mut self, store_id: StoreId) {
        self.stores.remove(&store_id);
        self.git_lanes.remove(&store_id);
    }

    pub(crate) fn resolve_store(&mut self, repo: &Path) -> Result<ResolvedStore, OpError> {
        self.store_caches.resolve_store(repo)
    }

    pub(crate) fn scheduler(&self) -> &SyncScheduler {
        &self.scheduler
    }

    pub(crate) fn scheduler_mut(&mut self) -> &mut SyncScheduler {
        &mut self.scheduler
    }

    pub(crate) fn checkpoint_scheduler_mut(&mut self) -> &mut CheckpointScheduler {
        &mut self.checkpoint_scheduler
    }

    pub(crate) fn checkpoint_group_snapshots(
        &self,
        store_id: StoreId,
    ) -> Vec<CheckpointGroupSnapshot> {
        self.checkpoint_scheduler.snapshot_for_store(store_id)
    }

    pub(crate) fn force_checkpoint_for_namespace(
        &mut self,
        store_id: StoreId,
        namespace: &NamespaceId,
    ) -> Vec<String> {
        let groups = self
            .checkpoint_scheduler
            .force_checkpoint_for_namespace(store_id, namespace);
        self.emit_checkpoint_queue_depth();
        groups
    }

    /// Get git lane state by raw remote URL (for internal sync waiters, etc.).
    /// Returns None if not loaded.
    pub(crate) fn git_lane_state_by_url(&self, remote: &RemoteUrl) -> Option<&GitLaneState> {
        self.store_caches
            .remote_to_store_id
            .get(remote)
            .and_then(|store_id| self.git_lanes.get(store_id))
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

    /// Ensure repo is loaded using cached refs, without blocking on network fetch.
    ///
    /// If no local refs exist, this will attempt a one-time fetch with a bounded timeout.
    pub fn ensure_repo_loaded(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<LoadedStore, OpError> {
        let resolved = self.store_caches.resolve_store(repo)?;
        let store_id = resolved.store_id;
        let remote = resolved.remote;
        self.store_caches
            .path_to_remote
            .insert(repo.to_owned(), remote.clone());

        if !self.stores.contains_key(&store_id) {
            let open = StoreRuntime::open(
                store_id,
                remote.clone(),
                WallClock::now().0,
                env!("CARGO_PKG_VERSION"),
                self.limits(),
                &self.namespace_defaults,
            )?;
            let runtime = open.runtime;
            self.seed_actor_clocks(&runtime)?;
            self.stores.insert(store_id, runtime);
            self.git_lanes.insert(store_id, GitLaneState::new());
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
            if let Some(repo_state) = self.git_lanes.get_mut(&store_id) {
                repo_state.register_path(repo.to_owned());
            } else {
                let repo_state = GitLaneState::with_path(None, repo.to_owned());
                self.git_lanes.insert(store_id, repo_state);
            }
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
        let resolved = self.store_caches.resolve_store(repo)?;
        let store_id = resolved.store_id;
        let remote = resolved.remote;
        self.store_caches
            .path_to_remote
            .insert(repo.to_owned(), remote.clone());

        if !self.stores.contains_key(&store_id) {
            let open = StoreRuntime::open(
                store_id,
                remote.clone(),
                WallClock::now().0,
                env!("CARGO_PKG_VERSION"),
                self.limits(),
                &self.namespace_defaults,
            )?;
            let runtime = open.runtime;
            self.seed_actor_clocks(&runtime)?;
            self.stores.insert(store_id, runtime);
            self.git_lanes.insert(store_id, GitLaneState::new());
            self.register_default_checkpoint_groups(store_id)?;

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
            if let Some(repo_state) = self.git_lanes.get_mut(&store_id) {
                repo_state.register_path(repo.to_owned());
            } else {
                let repo_state = GitLaneState::with_path(None, repo.to_owned());
                self.git_lanes.insert(store_id, repo_state);
            }
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
        let root_slug = loaded.root_slug;
        let checkpoint_imports = self.load_checkpoint_imports(store_id, repo);
        for import in &checkpoint_imports {
            match merge_store_states(&state, &import.state) {
                Ok(merged) => state = merged,
                Err(CheckpointImportError::Merge(errors)) => {
                    tracing::warn!(
                        store_id = %store_id,
                        errors = ?errors,
                        "checkpoint merge failed"
                    );
                    return Err(OpError::Internal("checkpoint merge failed"));
                }
                Err(err) => {
                    tracing::warn!(store_id = %store_id, error = ?err, "checkpoint merge failed");
                    return Err(OpError::Internal("checkpoint merge failed"));
                }
            }
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

        if !checkpoint_imports.is_empty() {
            let store = self
                .stores
                .get_mut(&store_id)
                .ok_or(OpError::Internal("loaded store missing from state"))?;
            apply_checkpoint_watermarks(store, &checkpoint_imports)?;
        }

        last_seen_stamp = max_write_stamp(last_seen_stamp, state.max_write_stamp());
        if let Some(max_stamp) = last_seen_stamp.as_ref() {
            self.clock.receive(max_stamp);
        }

        let now_wall_ms = WallClock::now().0;
        let clock_skew = last_seen_stamp
            .as_ref()
            .and_then(|stamp| detect_clock_skew(now_wall_ms, stamp.wall_ms));

        let mut repo_state = GitLaneState::with_path(root_slug, repo.to_owned());
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
        store.state = state;
        self.git_lanes.insert(store_id, repo_state);
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

    fn load_checkpoint_imports(&self, store_id: StoreId, repo: &Path) -> Vec<CheckpointImport> {
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

    fn warn_on_checkpoint_hash_mismatch(
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

        let roster =
            load_replica_roster(store_id).map_err(|err| OpError::StoreRuntime(Box::new(err)))?;
        let peers = self.replication_peers();

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
            backoff: replication_backoff(&self.replication),
        };
        let manager_handle = ReplicationManager::new(session_store.clone(), manager_config).start();

        let max_connections = if roster.is_some() {
            None
        } else {
            replication_max_connections(&self.replication)
        };
        let server_config = ReplicationServerConfig {
            listen_addr: replication_listen_addr(&self.replication),
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
        let (store_epoch, local_replica_id) = self
            .stores
            .get(&request.store_id)
            .map(|store| {
                (
                    Some(store.meta.identity.store_epoch),
                    Some(store.meta.replica_id),
                )
            })
            .unwrap_or((None, None));
        let origin_seq = request
            .batch
            .first()
            .map(|event| event.body.origin_seq.get());
        let txn_id = request.batch.first().map(|event| event.body.txn_id);
        let client_request_id = request
            .batch
            .first()
            .and_then(|event| event.body.client_request_id);
        let span = tracing::info_span!(
            "repl_ingest_request",
            store_id = %request.store_id,
            store_epoch = ?store_epoch.map(|epoch| epoch.get()),
            replica_id = ?local_replica_id,
            namespace = %request.namespace,
            origin_replica_id = %request.origin,
            origin_seq = ?origin_seq,
            txn_id = ?txn_id,
            client_request_id = ?client_request_id,
            batch_len = request.batch.len()
        );
        let _guard = span.enter();

        if self.shutting_down {
            let error = ReplError::new(
                ProtocolErrorCode::MaintenanceMode.into(),
                "shutting down",
                true,
            );
            let _ = request.respond.send(Err(error));
            return;
        }
        if let Some(error) = self.stores.get(&request.store_id).and_then(|store| {
            if store.maintenance_mode {
                Some(
                    ReplError::new(
                        ProtocolErrorCode::MaintenanceMode.into(),
                        "maintenance mode enabled",
                        true,
                    )
                    .with_details(ReplErrorDetails::MaintenanceMode(
                        error_details::MaintenanceModeDetails {
                            reason: Some("maintenance mode enabled".into()),
                            until_ms: None,
                        },
                    )),
                )
            } else if let Some(policy) = store.policies.get(&request.namespace) {
                if policy.replicate_mode == ReplicateMode::None {
                    Some(
                        ReplError::new(
                            ProtocolErrorCode::NamespacePolicyViolation.into(),
                            "namespace replication disabled by policy",
                            false,
                        )
                        .with_details(
                            ReplErrorDetails::NamespacePolicyViolation(
                                error_details::NamespacePolicyViolationDetails {
                                    namespace: request.namespace.clone(),
                                    rule: "replicate_mode".to_string(),
                                    reason: Some("replicate_mode=none".to_string()),
                                },
                            ),
                        ),
                    )
                } else {
                    None
                }
            } else {
                Some(
                    ReplError::new(
                        ProtocolErrorCode::NamespaceUnknown.into(),
                        "namespace not configured",
                        false,
                    )
                    .with_details(ReplErrorDetails::NamespaceUnknown(
                        error_details::NamespaceUnknownDetails {
                            namespace: request.namespace.clone(),
                        },
                    )),
                )
            }
        }) {
            let _ = request.respond.send(Err(error));
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

    pub(crate) fn reload_replication_runtime(&mut self, store_id: StoreId) -> Result<(), OpError> {
        if let Some(handles) = self.repl_handles.remove(&store_id) {
            handles.shutdown();
        }
        self.ensure_replication_runtime(store_id)
    }

    fn ingest_remote_batch(
        &mut self,
        store_id: StoreId,
        namespace: NamespaceId,
        origin: ReplicaId,
        batch: Vec<VerifiedEvent<PrevVerified>>,
        now_ms: u64,
    ) -> Result<IngestOutcome, ReplError> {
        let actor_stamps: Vec<(ActorId, WriteStamp)> = batch
            .iter()
            .map(|event| {
                let EventKindV1::TxnV1(txn) = &event.body.kind;
                (
                    txn.hlc_max.actor_id.clone(),
                    WriteStamp::new(txn.hlc_max.physical_ms, txn.hlc_max.logical),
                )
            })
            .collect();
        let (stores, git_lanes) = (&mut self.stores, &mut self.git_lanes);
        let store = stores.get_mut(&store_id).ok_or_else(|| {
            ReplError::new(CliErrorCode::Internal.into(), "store not loaded", true)
        })?;
        let git_lane = git_lanes.get_mut(&store_id).ok_or_else(|| {
            ReplError::new(CliErrorCode::Internal.into(), "store not loaded", true)
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

        let store_identity = store.meta.identity;
        let (origin_seq_first, origin_seq_last) =
            batch
                .iter()
                .fold((None::<u64>, None::<u64>), |(min_seq, max_seq), event| {
                    let seq = event.body.origin_seq.get();
                    let min_seq = Some(min_seq.map_or(seq, |min| min.min(seq)));
                    let max_seq = Some(max_seq.map_or(seq, |max| max.max(seq)));
                    (min_seq, max_seq)
                });
        let span = tracing::info_span!(
            "repl_ingest",
            store_id = %store_identity.store_id,
            store_epoch = store_identity.store_epoch.get(),
            namespace = %namespace,
            origin_replica_id = %origin,
            origin_seq_first = ?origin_seq_first,
            origin_seq_last = ?origin_seq_last,
            batch_len = batch.len()
        );
        let _guard = span.enter();

        for event in &batch {
            if event.body.namespace != namespace || event.body.origin_replica_id != origin {
                return Err(ReplError::new(
                    CliErrorCode::Internal.into(),
                    "replication batch has mismatched origin",
                    false,
                ));
            }
        }

        let store_dir = paths::store_dir(store_id);

        let wal_index = Arc::clone(&store.wal_index);
        let mut txn = wal_index
            .writer()
            .begin_txn()
            .map_err(|err| wal_index_error_payload(&err))?;

        for event in &batch {
            let record = VerifiedRecord::new(
                RecordHeader {
                    origin_replica_id: origin,
                    origin_seq: event.body.origin_seq,
                    event_time_ms: event.body.event_time_ms,
                    txn_id: event.body.txn_id,
                    client_request_id: event.body.client_request_id,
                    request_sha256: None,
                    sha256: event.sha256.0,
                    prev_sha256: event.prev.prev.map(|sha| sha.0),
                },
                Bytes::copy_from_slice(event.bytes.as_ref()),
                &event.body,
            )
            .map_err(|err| {
                tracing::error!(error = ?err, "record verification failed");
                ReplError::new(
                    CliErrorCode::Internal.into(),
                    "record verification failed",
                    false,
                )
            })?;

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
                    return Err(event_wal_error_payload(&namespace, None, None, err));
                }
            };
            let segment_snapshot =
                store
                    .event_wal
                    .segment_snapshot(&namespace)
                    .ok_or_else(|| {
                        ReplError::new(
                            CliErrorCode::Internal.into(),
                            "missing active wal segment",
                            false,
                        )
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
                .map_err(|err| wal_index_error_payload(&err))?;
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
                    .map_err(|err| wal_index_error_payload(&err))?;
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
            )
            .map_err(|err| wal_index_error_payload(&err))?;

            let EventKindV1::TxnV1(txn_body) = &event.body.kind;
            txn.update_hlc(&HlcRow {
                actor_id: txn_body.hlc_max.actor_id.clone(),
                last_physical_ms: txn_body.hlc_max.physical_ms,
                last_logical: txn_body.hlc_max.logical,
            })
            .map_err(|err| wal_index_error_payload(&err))?;
        }

        txn.commit().map_err(|err| wal_index_error_payload(&err))?;

        let (remote, max_stamp, durable, applied, applied_head, durable_head) = {
            let mut max_stamp = git_lane.last_seen_stamp.clone();
            for event in &batch {
                let apply_start = Instant::now();
                let apply_result = {
                    let state = store.state.ensure_namespace(namespace.clone());
                    apply_event(state, &event.body)
                };
                let outcome = match apply_result {
                    Ok(outcome) => {
                        metrics::apply_ok(apply_start.elapsed());
                        outcome
                    }
                    Err(err) => {
                        metrics::apply_err(apply_start.elapsed());
                        return Err(apply_event_error_payload(&namespace, &origin, err));
                    }
                };
                store.record_checkpoint_dirty_shards(&namespace, &outcome);

                let EventKindV1::TxnV1(txn_body) = &event.body.kind;
                let stamp = WriteStamp::new(txn_body.hlc_max.physical_ms, txn_body.hlc_max.logical);
                max_stamp = max_write_stamp(max_stamp, Some(stamp));

                let event_id = event_id_for(origin, namespace.clone(), event.body.origin_seq);
                let prev_sha = event.prev.prev.map(|sha| Sha256(sha.0));
                let broadcast =
                    BroadcastEvent::new(event_id, event.sha256, prev_sha, event.bytes.clone());
                if let Err(err) = store.broadcaster.publish(broadcast) {
                    tracing::warn!("event broadcast failed: {err}");
                }

                store
                    .watermarks_applied
                    .advance_contiguous(&namespace, &origin, event.body.origin_seq, event.sha256.0)
                    .map_err(|err| watermark_error_payload(&namespace, &origin, err))?;
                store
                    .watermarks_durable
                    .advance_contiguous(&namespace, &origin, event.body.origin_seq, event.sha256.0)
                    .map_err(|err| watermark_error_payload(&namespace, &origin, err))?;
            }

            if let Some(stamp) = max_stamp.clone() {
                let now_wall_ms = WallClock::now().0;
                git_lane.last_seen_stamp = Some(stamp.clone());
                git_lane.last_clock_skew = detect_clock_skew(now_wall_ms, stamp.wall_ms);
            }
            git_lane.mark_dirty();

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

        for (actor_id, stamp) in actor_stamps {
            self.clock_for_actor_mut(&actor_id).receive(&stamp);
        }

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
            .map_err(|err| wal_index_error_payload(&err))?;
        watermark_txn
            .update_watermark(
                &namespace,
                &origin,
                applied_seq,
                durable_seq,
                applied_head,
                durable_head,
            )
            .map_err(|err| wal_index_error_payload(&err))?;
        watermark_txn
            .commit()
            .map_err(|err| wal_index_error_payload(&err))?;

        Ok(IngestOutcome { durable, applied })
    }

    #[cfg(feature = "test-harness")]
    pub(crate) fn ingest_remote_batch_for_tests(
        &mut self,
        store_id: StoreId,
        namespace: NamespaceId,
        origin: ReplicaId,
        batch: Vec<VerifiedEvent<PrevVerified>>,
        now_ms: u64,
    ) -> Result<IngestOutcome, ReplError> {
        self.ingest_remote_batch(store_id, namespace, origin, batch, now_ms)
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
                if let Some(store) = self.stores.get_mut(&store_id) {
                    store.commit_checkpoint_dirty_shards(checkpoint_group);
                }
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
                if let Some(store) = self.stores.get_mut(&store_id) {
                    store.rollback_checkpoint_dirty_shards(checkpoint_group);
                }
                self.checkpoint_scheduler
                    .complete_failure(&key, Instant::now());
            }
        }
        self.emit_checkpoint_queue_depth();
    }

    /// Export state to Go-compatible JSONL format.
    ///
    /// Called after successful sync to keep .beads/issues.jsonl in sync.
    pub(crate) fn export_go_compat(&self, store_id: StoreId, remote: &RemoteUrl) {
        let Some(ref ctx) = self.export_ctx else {
            return;
        };

        let Some(store) = self.stores.get(&store_id) else {
            return;
        };
        let empty_state = CanonicalState::new();
        let export_state = store
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
        if let Some(repo_state) = self.git_lanes.get(&store_id)
            && let Err(e) = ensure_symlinks(&export_path, &repo_state.known_paths)
        {
            tracing::warn!("Go-compat symlink update failed for {:?}: {}", remote, e);
        }
    }

    pub fn next_wal_checkpoint_deadline(&mut self) -> Option<Instant> {
        self.next_wal_checkpoint_deadline_at(Instant::now())
    }

    fn next_wal_checkpoint_deadline_at(&self, now: Instant) -> Option<Instant> {
        let interval = self.wal_checkpoint_interval()?;
        self.stores
            .values()
            .filter_map(|store| store.wal_checkpoint_deadline(now, interval))
            .min()
    }

    pub fn next_lock_heartbeat_deadline(&mut self) -> Option<Instant> {
        self.next_lock_heartbeat_deadline_at(Instant::now())
    }

    fn next_lock_heartbeat_deadline_at(&self, now: Instant) -> Option<Instant> {
        self.stores
            .values()
            .filter_map(|store| store.lock_heartbeat_deadline(now, STORE_LOCK_HEARTBEAT_INTERVAL))
            .min()
    }

    pub fn fire_due_wal_checkpoints(&mut self) {
        self.fire_due_wal_checkpoints_at(Instant::now());
    }

    fn fire_due_wal_checkpoints_at(&mut self, now: Instant) {
        let Some(interval) = self.wal_checkpoint_interval() else {
            return;
        };
        for (store_id, store) in self.stores.iter_mut() {
            if !store.wal_checkpoint_due(now, interval) {
                continue;
            }
            let start = Instant::now();
            match store.wal_index.checkpoint_truncate() {
                Ok(()) => {
                    metrics::wal_index_checkpoint_ok(start.elapsed());
                    store.mark_wal_checkpoint(now);
                }
                Err(err) => {
                    metrics::wal_index_checkpoint_err(start.elapsed());
                    tracing::warn!(
                        store_id = %store_id,
                        error = ?err,
                        "wal sqlite checkpoint failed"
                    );
                    store.mark_wal_checkpoint(now);
                }
            }
        }
    }

    pub fn fire_due_lock_heartbeats(&mut self) {
        self.fire_due_lock_heartbeats_at(
            Instant::now(),
            STORE_LOCK_HEARTBEAT_INTERVAL,
            WallClock::now().0,
        );
    }

    fn fire_due_lock_heartbeats_at(&mut self, now: Instant, interval: Duration, now_ms: u64) {
        if interval == Duration::ZERO {
            return;
        }
        for (store_id, store) in self.stores.iter_mut() {
            if !store.lock_heartbeat_due(now, interval) {
                continue;
            }
            match store.update_lock_heartbeat(now_ms) {
                Ok(()) => {
                    store.mark_lock_heartbeat(now);
                }
                Err(err) => {
                    tracing::warn!(
                        store_id = %store_id,
                        error = ?err,
                        "store lock heartbeat update failed"
                    );
                    store.mark_lock_heartbeat(now);
                }
            }
        }
    }

    fn wal_checkpoint_interval(&self) -> Option<Duration> {
        let interval_ms = self.limits.wal_sqlite_checkpoint_interval_ms;
        if interval_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(interval_ms))
        }
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
            let Some(path) = self
                .git_lanes
                .get(&key.store_id)
                .and_then(|lane| lane.any_valid_path().cloned())
            else {
                tracing::warn!(
                    store_id = %key.store_id,
                    checkpoint_group = %config.group,
                    "checkpoint repo path missing"
                );
                self.checkpoint_scheduler.complete_failure(key, now);
                self.emit_checkpoint_queue_depth();
                return;
            };
            let store = match self.stores.get_mut(&key.store_id) {
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
            if let Some(store) = self.stores.get_mut(&key.store_id) {
                store.rollback_checkpoint_dirty_shards(&config.group);
            }
            self.checkpoint_scheduler.complete_failure(key, now);
            self.emit_checkpoint_queue_depth();
        }
    }

    /// Get iterator over all stores.
    pub fn repos(&self) -> impl Iterator<Item = (&StoreId, &GitLaneState)> {
        self.git_lanes.iter()
    }

    /// Get mutable iterator over all stores.
    pub fn repos_mut(&mut self) -> impl Iterator<Item = (&StoreId, &mut GitLaneState)> {
        self.git_lanes.iter_mut()
    }
}

fn load_timeout() -> Duration {
    let override_secs = std::env::var("BD_LOAD_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|v| *v > 0);
    Duration::from_secs(override_secs.unwrap_or(LOAD_TIMEOUT_SECS))
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

pub(crate) fn max_write_stamp(a: Option<WriteStamp>, b: Option<WriteStamp>) -> Option<WriteStamp> {
    match (a, b) {
        (Some(a), Some(b)) => Some(std::cmp::max(a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn apply_checkpoint_watermarks(
    store: &mut StoreRuntime,
    imports: &[CheckpointImport],
) -> Result<(), StoreRuntimeError> {
    if imports.is_empty() {
        return Ok(());
    }

    let mut origins: BTreeMap<NamespaceId, BTreeMap<ReplicaId, u64>> = BTreeMap::new();
    for import in imports {
        for (namespace, origin_map) in &import.included {
            for (origin, seq) in origin_map {
                let head =
                    checkpoint_head_status(import.included_heads.as_ref(), namespace, origin, *seq)
                        .map_err(|source| StoreRuntimeError::WatermarkInvalid {
                            kind: "applied",
                            namespace: namespace.clone(),
                            origin: *origin,
                            source,
                        })?;
                store
                    .watermarks_applied
                    .observe_at_least(namespace, origin, Seq0::new(*seq), head)
                    .map_err(|source| StoreRuntimeError::WatermarkInvalid {
                        kind: "applied",
                        namespace: namespace.clone(),
                        origin: *origin,
                        source,
                    })?;
                store
                    .watermarks_durable
                    .observe_at_least(namespace, origin, Seq0::new(*seq), head)
                    .map_err(|source| StoreRuntimeError::WatermarkInvalid {
                        kind: "durable",
                        namespace: namespace.clone(),
                        origin: *origin,
                        source,
                    })?;
                origins
                    .entry(namespace.clone())
                    .or_default()
                    .insert(*origin, *seq);
            }
        }
    }

    if origins.is_empty() {
        return Ok(());
    }

    let wal_index = store.wal_index.clone();
    let mut max_event_seq: BTreeMap<(NamespaceId, ReplicaId), Seq0> = BTreeMap::new();
    for (namespace, origin_map) in &origins {
        for origin in origin_map.keys() {
            let max_seq = wal_index.reader().max_origin_seq(namespace, origin)?;
            max_event_seq.insert((namespace.clone(), *origin), max_seq);
        }
    }

    let mut txn = wal_index.writer().begin_txn()?;
    for (namespace, origin_map) in origins {
        for (origin, _) in origin_map {
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

            let max_seq = max_event_seq
                .get(&(namespace.clone(), origin))
                .copied()
                .unwrap_or(Seq0::ZERO);
            let next_base = durable.seq().get().max(max_seq.get());
            let next_seq_raw =
                next_base
                    .checked_add(1)
                    .ok_or_else(|| WalIndexError::OriginSeqOverflow {
                        namespace: namespace.to_string(),
                        origin,
                    })?;
            let next_seq = Seq1::from_u64(next_seq_raw).ok_or_else(|| {
                WalIndexError::EventIdDecode("origin_seq must be >= 1".to_string())
            })?;
            txn.set_next_origin_seq(&namespace, &origin, next_seq)?;

            if durable.seq().get() == 0 {
                txn.update_watermark(&namespace, &origin, 0, 0, None, None)?;
                continue;
            }

            let applied_head = head_sha(applied.head());
            let durable_head = head_sha(durable.head());
            if let (Some(applied_head), Some(durable_head)) = (applied_head, durable_head) {
                txn.update_watermark(
                    &namespace,
                    &origin,
                    applied.seq().get(),
                    durable.seq().get(),
                    Some(applied_head),
                    Some(durable_head),
                )?;
            }
        }
    }
    txn.commit()?;
    Ok(())
}

fn checkpoint_head_status(
    included_heads: Option<&IncludedHeads>,
    namespace: &NamespaceId,
    origin: &ReplicaId,
    seq: u64,
) -> Result<HeadStatus, WatermarkError> {
    if seq == 0 {
        return Ok(HeadStatus::Genesis);
    }
    if let Some(heads) = included_heads
        && let Some(origins) = heads.get(namespace)
        && let Some(head) = origins.get(origin)
    {
        return Ok(HeadStatus::Known(*head.as_bytes()));
    }
    Err(WatermarkError::MissingHead {
        seq: Seq0::new(seq),
    })
}

fn head_sha(head: HeadStatus) -> Option<[u8; 32]> {
    match head {
        HeadStatus::Known(sha) => Some(sha),
        _ => None,
    }
}

fn checkpoint_ref_oid(repo: &Repository, git_ref: &str) -> Result<Option<Oid>, git2::Error> {
    if let Some(oid) = refname_to_id_optional(repo, git_ref)? {
        return Ok(Some(oid));
    }
    let Some(remote_ref) = checkpoint_remote_tracking_ref(git_ref) else {
        return Ok(None);
    };
    refname_to_id_optional(repo, &remote_ref)
}

fn checkpoint_remote_tracking_ref(git_ref: &str) -> Option<String> {
    let suffix = git_ref.strip_prefix("refs/")?;
    Some(format!("refs/remotes/origin/{suffix}"))
}

fn refname_to_id_optional(repo: &Repository, name: &str) -> Result<Option<Oid>, git2::Error> {
    match repo.refname_to_id(name) {
        Ok(oid) => Ok(Some(oid)),
        Err(err) if err.code() == GitErrorCode::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn write_checkpoint_tree(
    repo: &Repository,
    tree: &git2::Tree,
    dir: &Path,
) -> Result<(), CheckpointTreeError> {
    let mut outcome: Result<(), CheckpointTreeError> = Ok(());
    tree.walk(TreeWalkMode::PreOrder, |root, entry| {
        if outcome.is_err() {
            return TreeWalkResult::Abort;
        }
        if entry.kind() != Some(ObjectType::Blob) {
            return TreeWalkResult::Ok;
        }
        let name = match entry.name() {
            Some(name) => name,
            None => {
                outcome = Err(CheckpointTreeError::InvalidPath {
                    path: root.to_string(),
                });
                return TreeWalkResult::Abort;
            }
        };
        let rel_path = Path::new(root).join(name);
        if rel_path.components().any(|component| {
            matches!(
                component,
                Component::Prefix(_) | Component::RootDir | Component::ParentDir
            )
        }) {
            outcome = Err(CheckpointTreeError::InvalidPath {
                path: rel_path.display().to_string(),
            });
            return TreeWalkResult::Abort;
        }
        let full_path = dir.join(&rel_path);
        if let Some(parent) = full_path.parent()
            && let Err(err) = fs::create_dir_all(parent)
        {
            outcome = Err(CheckpointTreeError::Io {
                path: parent.to_path_buf(),
                source: err,
            });
            return TreeWalkResult::Abort;
        }
        let blob = match repo.find_blob(entry.id()) {
            Ok(blob) => blob,
            Err(err) => {
                outcome = Err(CheckpointTreeError::Git(err));
                return TreeWalkResult::Abort;
            }
        };
        if let Err(err) = fs::write(&full_path, blob.content()) {
            outcome = Err(CheckpointTreeError::Io {
                path: full_path,
                source: err,
            });
            return TreeWalkResult::Abort;
        }
        TreeWalkResult::Ok
    })?;

    outcome
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
        let mut from_seq_excl = Seq0::ZERO;
        while from_seq_excl.get() < row.applied_seq {
            let items = wal_index.reader().iter_from(
                &namespace,
                &row.origin,
                from_seq_excl,
                limits.max_event_batch_bytes,
            )?;
            if items.is_empty() {
                return Err(StoreRuntimeError::WalReplay(Box::new(
                    WalReplayError::NonContiguousSeq {
                        namespace: row.namespace.clone(),
                        origin: row.origin,
                        expected: from_seq_excl.next(),
                        got: Seq0::ZERO,
                    },
                )));
            }
            for item in items {
                let seq = item.event_id.origin_seq.get();
                if seq > row.applied_seq {
                    from_seq_excl = Seq0::new(row.applied_seq);
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
                from_seq_excl = Seq0::new(seq);
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
    let mut reader = open_segment_reader(path).map_err(|source| {
        StoreRuntimeError::WalReplay(Box::new(match source {
            EventWalError::Io { source, .. } => WalReplayError::Io {
                path: path.to_path_buf(),
                source,
            },
            other => WalReplayError::RecordDecode {
                path: path.to_path_buf(),
                source: other,
            },
        }))
    })?;
    reader.seek(SeekFrom::Start(offset)).map_err(|source| {
        StoreRuntimeError::WalReplay(Box::new(WalReplayError::Io {
            path: path.to_path_buf(),
            source,
        }))
    })?;

    let mut reader = FrameReader::new(reader, limits.max_wal_record_bytes);
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

    let (_, event_body) = decode_event_body(record.payload_bytes(), limits).map_err(|source| {
        StoreRuntimeError::WalReplay(Box::new(WalReplayError::EventBodyDecode {
            path: path.to_path_buf(),
            offset,
            source,
        }))
    })?;
    Ok(event_body)
}

fn replication_listen_addr(config: &crate::config::ReplicationConfig) -> String {
    let trimmed = config.listen_addr.trim();
    if trimmed.is_empty() {
        "127.0.0.1:0".to_string()
    } else {
        trimmed.to_string()
    }
}

fn replication_max_connections(config: &crate::config::ReplicationConfig) -> Option<NonZeroUsize> {
    match config.max_connections {
        Some(0) => None,
        Some(value) => NonZeroUsize::new(value),
        None => NonZeroUsize::new(DEFAULT_REPL_MAX_CONNECTIONS),
    }
}

fn replication_backoff(config: &crate::config::ReplicationConfig) -> BackoffPolicy {
    let base = Duration::from_millis(config.backoff_base_ms);
    let mut max = Duration::from_millis(config.backoff_max_ms);
    if max < base {
        max = base;
    }
    BackoffPolicy { base, max }
}

fn resolve_checkpoint_git_ref(store_id: StoreId, group: &str, git_ref: Option<&str>) -> String {
    let raw = git_ref.unwrap_or("").trim();
    if raw.is_empty() {
        return format!("refs/beads/{store_id}/{group}");
    }
    raw.replace("{store_id}", &store_id.to_string())
        .replace("{group}", group)
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
) -> ReplError {
    ReplError::new(ProtocolErrorCode::WalCorrupt.into(), "wal error", true).with_details(
        ReplErrorDetails::WalCorrupt(error_details::WalCorruptDetails {
            namespace: namespace.clone(),
            segment_id,
            offset,
            reason: err.to_string(),
        }),
    )
}

fn apply_event_error_payload(
    namespace: &NamespaceId,
    origin: &ReplicaId,
    err: ApplyError,
) -> ReplError {
    let reason = format!("apply_event rejected for {namespace}/{origin}: {err}");
    ReplError::new(
        ProtocolErrorCode::Corruption.into(),
        "apply_event rejected",
        false,
    )
    .with_details(ReplErrorDetails::Corruption(
        error_details::CorruptionDetails { reason },
    ))
}

fn wal_index_error_payload(err: &WalIndexError) -> ReplError {
    match err {
        WalIndexError::Equivocation {
            namespace,
            origin,
            seq,
            existing_sha256,
            new_sha256,
        } => ReplError::new(
            ProtocolErrorCode::Equivocation.into(),
            "equivocation",
            false,
        )
        .with_details(ReplErrorDetails::Equivocation(
            error_details::EquivocationDetails {
                eid: error_details::EventIdDetails {
                    namespace: namespace.clone(),
                    origin_replica_id: *origin,
                    origin_seq: *seq,
                },
                existing_sha256: hex::encode(existing_sha256),
                new_sha256: hex::encode(new_sha256),
            },
        )),
        WalIndexError::ClientRequestIdReuseMismatch {
            namespace,
            client_request_id,
            expected_request_sha256,
            got_request_sha256,
            ..
        } => ReplError::new(
            ProtocolErrorCode::ClientRequestIdReuseMismatch.into(),
            "client_request_id reuse mismatch",
            false,
        )
        .with_details(ReplErrorDetails::ClientRequestIdReuseMismatch(
            error_details::ClientRequestIdReuseMismatchDetails {
                namespace: namespace.clone(),
                client_request_id: *client_request_id,
                expected_request_sha256: hex::encode(expected_request_sha256),
                got_request_sha256: hex::encode(got_request_sha256),
            },
        )),
        _ => ReplError::new(ProtocolErrorCode::IndexCorrupt.into(), "index error", true)
            .with_details(ReplErrorDetails::IndexCorrupt(
                error_details::IndexCorruptDetails {
                    reason: err.to_string(),
                },
            )),
    }
}

fn watermark_error_payload(
    namespace: &NamespaceId,
    origin: &ReplicaId,
    err: WatermarkError,
) -> ReplError {
    match err {
        WatermarkError::NonContiguous { expected, got } => {
            let durable_seen = expected.prev_seq0().get();
            ReplError::new(ProtocolErrorCode::GapDetected.into(), "gap detected", false)
                .with_details(ReplErrorDetails::GapDetected(
                    error_details::GapDetectedDetails {
                        namespace: namespace.clone(),
                        origin_replica_id: *origin,
                        durable_seen,
                        got_seq: got.get(),
                    },
                ))
        }
        other => ReplError::new(CliErrorCode::Internal.into(), other.to_string(), false),
    }
}

#[cfg(any(test, feature = "test-harness"))]
pub(crate) fn insert_store_for_tests(
    daemon: &mut Daemon,
    store_id: StoreId,
    remote: RemoteUrl,
    repo_path: &Path,
) -> Result<(), OpError> {
    let open = StoreRuntime::open(
        store_id,
        remote.clone(),
        WallClock::now().0,
        env!("CARGO_PKG_VERSION"),
        daemon.limits(),
        &daemon.namespace_defaults,
    )
    .map_err(|err| OpError::StoreRuntime(Box::new(err)))?;
    daemon.seed_actor_clocks(&open.runtime)?;
    daemon.stores.insert(store_id, open.runtime);
    daemon.git_lanes.insert(
        store_id,
        GitLaneState::with_path(None, repo_path.to_owned()),
    );
    daemon
        .store_caches
        .remote_to_store_id
        .insert(remote.clone(), store_id);
    daemon
        .store_caches
        .path_to_store_id
        .insert(repo_path.to_owned(), store_id);
    daemon
        .store_caches
        .path_to_remote
        .insert(repo_path.to_owned(), remote);
    daemon.register_default_checkpoint_groups(store_id)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::QueryResult;
    use crate::daemon::ipc::{MutationMeta, ReadConsistency, Request, ResponsePayload};
    use crate::daemon::store::discovery::store_id_from_remote;
    use crate::git::sync::SyncOutcome;
    use std::collections::BTreeMap;
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use git2::Repository;
    use tracing::{Dispatch, Level};
    use tracing_subscriber::fmt::MakeWriter;
    use uuid::Uuid;

    use crate::core::{
        ActorId, Applied, Bead, BeadCore, BeadFields, BeadId, BeadType, CanonicalState, Claim,
        ContentHash, Durable, ErrorCode, EventBody, EventKindV1, HeadStatus, HlcMax, Labels,
        Limits, Lww, NamespaceId, NamespacePolicy, NoteAppendV1, NoteId, PrevVerified, Priority,
        ReplicaEntry, ReplicaId, ReplicaRole, ReplicaRoster, SegmentId, Seq0, Seq1, Sha256, Stamp,
        StoreEpoch, StoreId, StoreIdentity, StoreMeta, StoreMetaVersions, TxnDeltaV1, TxnId,
        TxnOpV1, TxnV1, VerifiedEvent, WallClock, Watermarks, WireBeadPatch, WireNoteV1, WireStamp,
        Workflow, WriteStamp, encode_event_body_canonical, hash_event_body,
    };
    use crate::daemon::git_worker::LoadResult;
    use crate::daemon::ops::OpResult;
    use crate::daemon::store_lock::read_lock_meta;
    use crate::daemon::store_runtime::StoreRuntime;
    use crate::daemon::wal::frame::encode_frame;
    use crate::daemon::wal::{HlcRow, RecordHeader, SegmentHeader, VerifiedRecord};
    use crate::git::checkpoint::{
        CHECKPOINT_FORMAT_VERSION, CheckpointExportInput, CheckpointImport,
        CheckpointSnapshotInput, CheckpointStoreMeta, IncludedWatermarks, build_snapshot,
        export_checkpoint, policy_hash, publish_checkpoint, store_state_from_legacy,
    };
    use tempfile::TempDir;

    fn test_actor() -> ActorId {
        ActorId::new("test@host".to_string()).unwrap()
    }

    fn assert_err_code(outcome: HandleOutcome, expected: ErrorCode) {
        match outcome {
            HandleOutcome::Response(Response::Err { err }) => {
                assert_eq!(err.code, expected);
            }
            other => panic!("expected error response, got {other:?}"),
        }
    }

    fn test_remote() -> RemoteUrl {
        RemoteUrl("example.com/test/repo".into())
    }

    #[derive(Clone)]
    struct TestWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    struct TestWriterGuard {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl<'a> MakeWriter<'a> for TestWriter {
        type Writer = TestWriterGuard;

        fn make_writer(&'a self) -> Self::Writer {
            TestWriterGuard {
                buffer: self.buffer.clone(),
            }
        }
    }

    impl Write for TestWriterGuard {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.buffer
                .lock()
                .expect("log buffer")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
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

    fn test_store_dir() -> TempStoreDir {
        TempStoreDir::new()
    }

    fn insert_store(daemon: &mut Daemon, remote: &RemoteUrl) -> StoreId {
        let store_id = store_id_from_remote(remote);
        let runtime = StoreRuntime::open(
            store_id,
            remote.clone(),
            WallClock::now().0,
            env!("CARGO_PKG_VERSION"),
            daemon.limits(),
            &daemon.namespace_defaults,
        )
        .unwrap()
        .runtime;
        daemon.seed_actor_clocks(&runtime).unwrap();
        daemon
            .store_caches
            .remote_to_store_id
            .insert(remote.clone(), store_id);
        daemon.stores.insert(store_id, runtime);
        daemon.git_lanes.insert(store_id, GitLaneState::new());
        store_id
    }

    #[test]
    fn wal_checkpoint_deadline_tracks_interval() {
        let _tmp = test_store_dir();
        let limits = Limits {
            wal_sqlite_checkpoint_interval_ms: 10,
            ..Default::default()
        };
        let mut daemon = Daemon::new_with_limits(test_actor(), limits);
        let remote = test_remote();
        insert_store(&mut daemon, &remote);

        let now = Instant::now();
        let deadline = daemon
            .next_wal_checkpoint_deadline_at(now)
            .expect("deadline");
        assert_eq!(deadline, now);

        daemon.fire_due_wal_checkpoints_at(now);
        let next = daemon.next_wal_checkpoint_deadline_at(now).expect("next");
        assert_eq!(next, now + Duration::from_millis(10));
    }

    #[test]
    fn store_lock_heartbeat_updates_metadata() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new_with_limits(test_actor(), Limits::default());
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);
        let before = read_lock_meta(store_id)
            .expect("read lock meta")
            .expect("lock meta");
        let previous = before.last_heartbeat_ms.unwrap_or(0);
        let now = Instant::now() + Duration::from_millis(5);
        let now_ms = previous.saturating_add(1);

        daemon.fire_due_lock_heartbeats_at(now, Duration::from_millis(1), now_ms);

        let after = read_lock_meta(store_id)
            .expect("read lock meta")
            .expect("lock meta");
        assert_eq!(after.last_heartbeat_ms, Some(now_ms));
    }

    #[test]
    fn checkpoint_roster_hash_mismatch_warns() {
        let _tmp = test_store_dir();
        let daemon = Daemon::new_with_limits(test_actor(), Limits::default());
        let store_id = StoreId::new(Uuid::from_bytes([99u8; 16]));
        let import = CheckpointImport {
            checkpoint_group: "core".to_string(),
            policy_hash: ContentHash::from_bytes([1u8; 32]),
            roster_hash: Some(ContentHash::from_bytes([2u8; 32])),
            state: StoreState::new(),
            included: IncludedWatermarks::new(),
            included_heads: None,
        };
        let local_policy_hash = Some(ContentHash::from_bytes([1u8; 32]));
        let local_roster_hash = Some(ContentHash::from_bytes([3u8; 32]));

        let buffer = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(TestWriter {
                buffer: buffer.clone(),
            })
            .with_max_level(Level::WARN)
            .with_ansi(false)
            .finish();

        let dispatch = Dispatch::new(subscriber);
        tracing::dispatcher::with_default(&dispatch, || {
            daemon.warn_on_checkpoint_hash_mismatch(
                store_id,
                &import,
                local_policy_hash,
                local_roster_hash,
            );
        });

        let logs =
            String::from_utf8(buffer.lock().expect("log buffer").clone()).expect("utf8 logs");
        assert!(logs.contains("checkpoint roster hash mismatch"));
    }

    #[cfg(unix)]
    #[test]
    fn replica_roster_enforces_permissions() {
        let _tmp = TempStoreDir::new();
        let store_id = StoreId::new(Uuid::from_bytes([44u8; 16]));
        let path = paths::replicas_path(store_id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create store dir");
        }

        let roster = ReplicaRoster {
            replicas: vec![ReplicaEntry {
                replica_id: ReplicaId::new(Uuid::from_bytes([45u8; 16])),
                name: "alpha".to_string(),
                role: ReplicaRole::Anchor,
                durability_eligible: true,
                allowed_namespaces: None,
                expire_after_ms: None,
            }],
        };
        let toml = toml::to_string(&roster).expect("encode roster");
        std::fs::write(&path, toml).expect("write replicas.toml");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("chmod replicas.toml");

        let roster = load_replica_roster(store_id).expect("load roster");
        assert!(roster.is_some());

        let mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn replica_roster_rejects_symlink() {
        let tmp = TempStoreDir::new();
        let store_id = StoreId::new(Uuid::from_bytes([46u8; 16]));
        let path = paths::replicas_path(store_id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create store dir");
        }

        let target = tmp.data_dir().join("replicas-target.toml");
        std::fs::write(&target, b"").expect("write target");
        symlink(&target, &path).expect("symlink replicas.toml");

        let err = load_replica_roster(store_id).unwrap_err();
        assert!(matches!(
            err,
            StoreRuntimeError::ReplicaRosterSymlink { .. }
        ));
    }

    #[test]
    fn admin_flush_returns_output() {
        let tmp = test_store_dir();
        let actor = test_actor();
        let mut daemon = Daemon::new(actor);
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).expect("create repo dir");

        let store_id = StoreId::new(Uuid::from_bytes([55u8; 16]));
        let remote = RemoteUrl("example.com/test/repo".into());
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).expect("insert store");

        let (git_tx, _git_rx) = crossbeam::channel::bounded(1);
        let response = daemon.admin_flush(&repo_path, None, true, &git_tx);
        let Response::Ok { ok } = response else {
            panic!("expected ok response");
        };
        let ResponsePayload::Query(QueryResult::AdminFlush(out)) = ok else {
            panic!("expected admin flush payload");
        };
        assert_eq!(out.namespace, NamespaceId::core());
        assert!(out.checkpoint_now);
        assert!(out.checkpoint_groups.contains(&"core".to_string()));
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
            kind: EventKindV1::TxnV1(TxnV1 {
                delta,
                hlc_max: HlcMax {
                    actor_id: ActorId::new("alice").unwrap(),
                    physical_ms: event_time_ms,
                    logical: 0,
                },
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
        verified_event_with_delta(store, namespace, origin, seq, prev, TxnDeltaV1::new())
    }

    fn record_for_event(event: &VerifiedEvent<PrevVerified>) -> VerifiedRecord {
        VerifiedRecord::new(
            RecordHeader {
                origin_replica_id: event.body.origin_replica_id,
                origin_seq: event.body.origin_seq,
                event_time_ms: event.body.event_time_ms,
                txn_id: event.body.txn_id,
                client_request_id: event.body.client_request_id,
                request_sha256: None,
                sha256: event.sha256.0,
                prev_sha256: event.prev.prev.map(|sha| sha.0),
            },
            Bytes::copy_from_slice(event.bytes.as_ref()),
            &event.body,
        )
        .expect("verified record")
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
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);

        // Manually insert a repo in refresh_in_progress state
        let mut repo_state = GitLaneState::new();
        repo_state.refresh_in_progress = true;
        daemon.git_lanes.insert(store_id, repo_state);

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

        let repo_state = daemon.git_lanes.get(&store_id).unwrap();
        assert!(!repo_state.refresh_in_progress);
        assert!(repo_state.last_refresh.is_some());
    }

    #[test]
    fn read_gate_requires_min_seen() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
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
            .observe_at_least(
                &namespace,
                &origin,
                Seq0::new(1),
                HeadStatus::Known([1u8; 32]),
            )
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
            .observe_at_least(
                &namespace,
                &origin,
                Seq0::new(1),
                HeadStatus::Known([1u8; 32]),
            )
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
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);

        let mut repo_state = GitLaneState::new();
        repo_state.refresh_in_progress = true;
        daemon.git_lanes.insert(store_id, repo_state);

        // Complete refresh with error
        let result = Err(SyncError::NoLocalRef("/test".to_string()));
        daemon.complete_refresh(&remote, result);

        let repo_state = daemon.git_lanes.get(&store_id).unwrap();
        assert!(!repo_state.refresh_in_progress);
        // last_refresh should NOT be updated on error
        assert!(repo_state.last_refresh.is_none());
    }

    #[test]
    fn complete_refresh_applies_state_when_clean() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);

        let mut repo_state = GitLaneState::new();
        repo_state.refresh_in_progress = true;
        repo_state.dirty = false; // Clean state
        daemon.git_lanes.insert(store_id, repo_state);

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

        let repo_state = daemon.git_lanes.get(&store_id).unwrap();
        assert_eq!(repo_state.root_slug, Some("fresh-slug".to_string()));
    }

    #[test]
    fn complete_refresh_skips_state_when_dirty() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);

        let mut repo_state = GitLaneState::new();
        repo_state.refresh_in_progress = true;
        repo_state.dirty = true; // Dirty - mutations happened during refresh
        repo_state.root_slug = Some("original-slug".to_string());
        daemon.git_lanes.insert(store_id, repo_state);

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

        let repo_state = daemon.git_lanes.get(&store_id).unwrap();
        // Should keep original slug since dirty
        assert_eq!(repo_state.root_slug, Some("original-slug".to_string()));
        // But last_refresh should still be updated
        assert!(repo_state.last_refresh.is_some());
    }

    #[test]
    fn complete_refresh_schedules_sync_when_needs_sync() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);

        let mut repo_state = GitLaneState::new();
        repo_state.refresh_in_progress = true;
        repo_state.dirty = false;
        daemon.git_lanes.insert(store_id, repo_state);

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

        let repo_state = daemon.git_lanes.get(&store_id).unwrap();
        // Should mark dirty to trigger sync
        assert!(repo_state.dirty);
        // And scheduler should have it pending
        assert!(daemon.scheduler.is_pending(&remote));
    }

    #[test]
    fn complete_refresh_unknown_remote_is_noop() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
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
    fn load_from_checkpoint_ref_sets_watermarks() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();

        let repo_dir = TempDir::new().unwrap();
        let repo = Repository::init(repo_dir.path()).unwrap();

        let store_id = store_id_from_remote(&remote);
        insert_store_for_tests(&mut daemon, store_id, remote.clone(), repo_dir.path())
            .expect("store init");

        let bead = make_bead("bd-checkpoint1", 1_700_000_000_000, "checkpoint@test");
        let bead_id = bead.core.id.clone();
        let mut legacy_state = CanonicalState::new();
        legacy_state.insert_live(bead);
        let store_state = store_state_from_legacy(legacy_state);

        let origin = daemon
            .stores
            .get(&store_id)
            .expect("store runtime")
            .meta
            .replica_id;
        let mut watermarks = Watermarks::<Durable>::new();
        let head = [7u8; 32];
        watermarks
            .observe_at_least(
                &NamespaceId::core(),
                &origin,
                Seq0::new(3),
                HeadStatus::Known(head),
            )
            .expect("watermark");

        let mut policies = BTreeMap::new();
        policies.insert(NamespaceId::core(), NamespacePolicy::core_default());
        let policy_hash = policy_hash(&policies).expect("policy hash");

        let snapshot = build_snapshot(CheckpointSnapshotInput {
            checkpoint_group: "core".to_string(),
            namespaces: vec![NamespaceId::core()],
            store_id,
            store_epoch: StoreEpoch::ZERO,
            created_at_ms: 1_700_000_000_000,
            created_by_replica_id: origin,
            policy_hash,
            roster_hash: None,
            dirty_shards: None,
            state: &store_state,
            watermarks_durable: &watermarks,
        })
        .expect("checkpoint snapshot");

        let export = export_checkpoint(CheckpointExportInput {
            snapshot: &snapshot,
            previous: None,
        })
        .expect("checkpoint export");

        let git_ref = format!("refs/beads/{store_id}/core");
        let mut checkpoint_groups = BTreeMap::new();
        checkpoint_groups.insert("core".to_string(), git_ref.clone());
        let store_meta = CheckpointStoreMeta::new(
            store_id,
            StoreEpoch::ZERO,
            CHECKPOINT_FORMAT_VERSION,
            checkpoint_groups,
        );
        publish_checkpoint(&repo, &export, &git_ref, &store_meta).expect("checkpoint publish");

        let loaded = LoadResult {
            state: CanonicalState::new(),
            root_slug: None,
            needs_sync: false,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        };
        daemon
            .apply_loaded_repo_state(store_id, &remote, repo_dir.path(), loaded)
            .expect("load repo");

        let store = daemon.stores.get(&store_id).expect("store runtime");
        let core = store.state.get(&NamespaceId::core()).expect("core state");
        assert!(core.get_live(&bead_id).is_some());

        let durable = store
            .watermarks_durable
            .get(&NamespaceId::core(), &origin)
            .copied()
            .expect("durable watermark");
        assert_eq!(durable.seq().get(), 3);
        assert_eq!(durable.head(), HeadStatus::Known(head));

        let applied = store
            .watermarks_applied
            .get(&NamespaceId::core(), &origin)
            .copied()
            .expect("applied watermark");
        assert_eq!(applied.seq().get(), 3);
        assert_eq!(applied.head(), HeadStatus::Known(head));

        let mut txn = store.wal_index.writer().begin_txn().expect("wal txn");
        let next_seq = txn
            .next_origin_seq(&NamespaceId::core(), &origin)
            .expect("next seq");
        txn.rollback().expect("rollback");
        assert_eq!(next_seq.get(), 4);
    }

    #[test]
    fn non_core_mutation_and_query_use_namespace_state() {
        let tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
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

        let store_state = &daemon.stores.get(&store_id).unwrap().state;
        let core_count = store_state
            .get(&NamespaceId::core())
            .map(|state| state.live_count())
            .unwrap_or(0);
        let tmp_count = store_state
            .get(&tmp_ns)
            .map(|state| state.live_count())
            .unwrap_or(0);
        assert_eq!(core_count, 0);
        assert_eq!(tmp_count, 1);
    }

    #[test]
    fn mutation_meta_rejects_invalid_fields() {
        let tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        let store_id = store_id_from_remote(&remote);
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).unwrap();
        let (git_tx, _git_rx) = crossbeam::channel::unbounded();

        let base_request = |meta| Request::Create {
            repo: repo_path.clone(),
            id: None,
            parent: None,
            title: "bad".to_string(),
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
            meta,
        };

        assert_err_code(
            daemon.handle_request(
                base_request(MutationMeta {
                    durability: Some("nope".into()),
                    ..MutationMeta::default()
                }),
                &git_tx,
            ),
            ProtocolErrorCode::InvalidRequest.into(),
        );
        assert_err_code(
            daemon.handle_request(
                base_request(MutationMeta {
                    client_request_id: Some("not-a-uuid".into()),
                    ..MutationMeta::default()
                }),
                &git_tx,
            ),
            ProtocolErrorCode::InvalidRequest.into(),
        );
        assert_err_code(
            daemon.handle_request(
                base_request(MutationMeta {
                    actor_id: Some("   ".into()),
                    ..MutationMeta::default()
                }),
                &git_tx,
            ),
            ProtocolErrorCode::InvalidRequest.into(),
        );
        assert_err_code(
            daemon.handle_request(
                base_request(MutationMeta {
                    namespace: Some("BAD".into()),
                    ..MutationMeta::default()
                }),
                &git_tx,
            ),
            ProtocolErrorCode::NamespaceInvalid.into(),
        );
    }

    #[test]
    fn restores_hlc_state_for_non_daemon_actor() {
        let tmp = test_store_dir();
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        let store_id = store_id_from_remote(&remote);
        let actor = ActorId::new("alice").unwrap();
        let future_ms = WallClock::now().0 + 60_000;

        {
            let mut daemon = Daemon::new(test_actor());
            insert_store_for_tests(&mut daemon, store_id, remote.clone(), &repo_path).unwrap();

            let runtime = daemon.stores.get(&store_id).unwrap();
            let mut txn = runtime.wal_index.writer().begin_txn().unwrap();
            txn.update_hlc(&HlcRow {
                actor_id: actor.clone(),
                last_physical_ms: future_ms,
                last_logical: 0,
            })
            .unwrap();
            txn.commit().unwrap();
        }

        let mut daemon = Daemon::new(test_actor());
        insert_store_for_tests(&mut daemon, store_id, remote.clone(), &repo_path).unwrap();

        let (git_tx, _git_rx) = crossbeam::channel::unbounded();
        let response = daemon.handle_request(
            Request::Create {
                repo: repo_path.clone(),
                id: None,
                parent: None,
                title: "hlc".to_string(),
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
                    actor_id: Some(actor.as_str().to_string()),
                    ..MutationMeta::default()
                },
            },
            &git_tx,
        );

        match response {
            HandleOutcome::Response(Response::Ok {
                ok: ResponsePayload::Op(_),
            }) => {}
            other => panic!("unexpected outcome: {other:?}"),
        }

        let runtime = daemon.stores.get(&store_id).unwrap();
        let rows = runtime.wal_index.reader().load_hlc().unwrap();
        let row = rows
            .iter()
            .find(|row| row.actor_id == actor)
            .expect("hlc row for actor");
        let restored = WriteStamp::new(row.last_physical_ms, row.last_logical);
        let persisted = WriteStamp::new(future_ms, 0);
        assert!(restored > persisted);
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
        delta.insert(TxnOpV1::BeadUpsert(Box::new(patch))).unwrap();
        let event = verified_event_with_delta(store, &namespace, origin, 1, None, delta);

        let mut daemon = Daemon::new_with_limits(test_actor(), Limits::default());
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

        let state = daemon
            .stores
            .get(&store_id)
            .unwrap()
            .state
            .get(&namespace)
            .expect("namespace state");
        assert!(state.get_live(&bead_id).is_some());
    }

    #[test]
    fn complete_sync_resolves_collisions_for_dirty_state() {
        let _tmp = test_store_dir();
        let remote = test_remote();
        let mut daemon = Daemon::new(test_actor());
        let store_id = insert_store(&mut daemon, &remote);

        let winner = make_bead("bd-abc", 1000, "alice");
        let loser = make_bead("bd-abc", 2000, "bob");

        let mut synced_state = CanonicalState::new();
        synced_state.insert_live(winner.clone());

        let mut local_state = CanonicalState::new();
        local_state.insert_live(loser);

        {
            let store = daemon.stores.get_mut(&store_id).unwrap();
            store
                .state
                .set_namespace_state(NamespaceId::core(), local_state);
        }
        {
            let repo_state = daemon.git_lanes.get_mut(&store_id).unwrap();
            repo_state.dirty = true;
            repo_state.sync_in_progress = true;
        }

        let outcome = SyncOutcome {
            last_seen_stamp: synced_state.max_write_stamp(),
            state: synced_state,
            divergence: None,
            force_push: None,
        };
        daemon.complete_sync(&remote, Ok(outcome));

        let core_state = daemon
            .stores
            .get(&store_id)
            .unwrap()
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

        let mut daemon = Daemon::new_with_limits(test_actor(), limits.clone());
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).unwrap();

        let event2 = verified_event_for_seq(store, &namespace, origin, 2, Some(event1.sha256));
        daemon
            .ingest_remote_batch(
                store_id,
                namespace.clone(),
                origin,
                vec![event1, event2],
                now_ms,
            )
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
    fn ingest_reports_apply_failure_after_append() {
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

        let mut daemon = Daemon::new_with_limits(test_actor(), Limits::default());
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).unwrap();

        let err = daemon
            .ingest_remote_batch(store_id, namespace.clone(), origin, vec![event], now_ms)
            .expect_err("apply failure should reject batch");
        assert_eq!(err.code, ProtocolErrorCode::Corruption.into());
        let details = err
            .to_payload()
            .details_as::<error_details::CorruptionDetails>()
            .unwrap()
            .expect("corruption details");
        assert!(details.reason.contains("apply_event rejected for core/"));

        let store_runtime = daemon.stores.get(&store_id).expect("store runtime");
        let segments = store_runtime
            .wal_index
            .reader()
            .list_segments(&namespace)
            .expect("segments");
        assert_eq!(segments.len(), 1);
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
