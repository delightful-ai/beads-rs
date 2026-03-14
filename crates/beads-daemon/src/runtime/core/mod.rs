//! Daemon core - the central coordinator.
//!
//! Owns all per-repo state, the HLC clock, actor identity, and sync scheduler.
//! The serialization point for all mutations - runs on a single thread.

mod checkpoint_import;
mod checkpoint_scheduling;
mod helpers;
mod housekeeping;
mod loaded_store;
mod read;
mod repl_ingest;
mod replication;
mod repo_access;
mod repo_load;
mod store_session;

use helpers::*;
use housekeeping::ExportPending;

#[cfg(any(test, feature = "test-harness"))]
#[allow(unused_imports)]
pub use helpers::insert_store_for_tests;
pub use helpers::{PendingReplayApply, ReplayApplyOutcome, replay_event_wal};
pub(crate) use helpers::{detect_clock_skew, max_write_stamp};
pub(crate) use loaded_store::LoadedStore;
pub(crate) use read::{ReadGateStatus, ReadScope};
pub(crate) use store_session::{StoreGeneration, StoreSession, StoreSessionToken};

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs;
use std::io::{self, Seek, SeekFrom};
use std::num::NonZeroUsize;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crossbeam::channel::Sender;
use git2::{ErrorCode as GitErrorCode, ObjectType, Oid, Repository, TreeWalkMode, TreeWalkResult};
use thiserror::Error;

use super::Clock;
use super::checkpoint_scheduler::CheckpointScheduler;
use super::executor::DurabilityWait;
use super::export_worker::ExportWorkerHandle;
use super::git_worker::{GitOp, LoadResult};
use super::ipc::{ReadConsistency, Response};
use super::metrics;
use super::ops::OpError;
use super::repl::{
    BackoffPolicy, ContiguousBatch, IngestOutcome, ReplError, ReplErrorDetails, ReplIngestRequest,
    ReplicationManagerHandle, ReplicationServerHandle,
};
#[cfg(any(test, feature = "test-harness"))]
use super::store::discovery::{StoreIdResolution, StoreIdSource};
use super::store::runtime::{
    ReplicationRuntimeVersion, StoreRuntime, StoreRuntimeError, load_replica_roster,
};
use super::store::{ResolvedStore, StoreCaches};
use super::wal::{
    EventWalError, FrameReader, HlcRow, RecordHeader, RequestProof, SegmentRow, VerifiedRecord,
    WalAppend, WalIndex, WalIndexError, WalReplayError, open_segment_reader,
};
use crate::broadcast::BroadcastEvent;
use crate::config::{
    CheckpointGroupConfig as RuntimeCheckpointGroupConfig, CheckpointPolicy, DaemonRuntimeConfig,
    GitSyncPolicy, ReplicationConfig as RuntimeReplicationConfig,
};
use crate::git_lane::{
    ClockSkewRecord, DivergenceRecord, FetchErrorRecord, ForcePushRecord, GitLaneState,
};
use crate::layout::DaemonLayout;
use crate::remote::RemoteUrl;
use crate::scheduler::SyncScheduler;

use crate::compat::ExportContext;
use crate::core::error::details as error_details;

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
    DurabilityClass, EventId, EventKindV1, HeadStatus, Limits, NamespaceId, NamespacePolicy,
    ProtocolErrorCode, ReplicaId, SegmentId, Seq0, Seq1, Sha256, StoreId, StoreIdentity,
    StoreState, TraceId, ValidatedEventBody, WallClock, Watermark, WatermarkError, Watermarks,
    WriteStamp, apply_event, decode_event_body, encode_event_body_canonical, hash_event_body,
};
use crate::git::SyncError;
use crate::git::checkpoint::{
    CheckpointCache, CheckpointImport, CheckpointShardPath, IncludedHeads, import_checkpoint,
    merge_store_states, policy_hash, roster_hash, store_state_from_legacy,
};

const LOAD_TIMEOUT_SECS: u64 = 30;
const DEFAULT_REPL_MAX_CONNECTIONS: usize = 32;

#[derive(Clone, Debug)]
pub(crate) struct ParsedMutationMeta {
    pub(crate) namespace: NamespaceId,
    pub(crate) durability: DurabilityClass,
    pub(crate) client_request_id: Option<ClientRequestId>,
    pub(crate) trace_id: TraceId,
    pub(crate) actor_id: ActorId,
}

#[derive(Debug)]
pub enum HandleOutcome {
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
    /// Filesystem layout injected by the host crate.
    layout: DaemonLayout,

    /// Generation-scoped per-store session state keyed by StoreId.
    store_sessions: BTreeMap<StoreId, StoreSession>,
    /// Monotonic generation allocator for `StoreSessionToken`.
    next_store_generation: u64,
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

    /// Go-compatibility export worker (best-effort).
    export_worker: Option<ExportWorkerHandle>,
    /// Pending Go-compat exports per store.
    export_pending: BTreeMap<StoreId, ExportPending>,

    /// Realtime safety limits.
    limits: Limits,
    /// Git sync policy (test-only overrides).
    git_sync_policy: GitSyncPolicy,
    /// Checkpoint scheduling policy (test-only overrides).
    checkpoint_policy: CheckpointPolicy,
    /// Replication settings loaded from config (env overrides applied during load).
    replication: RuntimeReplicationConfig,
    /// Default checkpoint group specs from config.
    checkpoint_groups: BTreeMap<String, RuntimeCheckpointGroupConfig>,
    /// Default namespace policies when namespaces.toml is missing.
    namespace_defaults: BTreeMap<NamespaceId, NamespacePolicy>,

    /// Replication ingest channel (set by run_state_loop).
    repl_ingest_tx: Option<Sender<ReplIngestRequest>>,

    /// Shutdown gate to stop accepting new mutations.
    shutting_down: bool,
}

pub(crate) struct ReplicationHandles {
    runtime_version: ReplicationRuntimeVersion,
    manager: Option<ReplicationManagerHandle>,
    server: Option<ReplicationServerHandle>,
}

impl ReplicationHandles {
    fn runtime_version(&self) -> ReplicationRuntimeVersion {
        self.runtime_version
    }

    fn server_local_addr(&self) -> Option<String> {
        self.server
            .as_ref()
            .map(|handle| handle.local_addr().to_string())
    }

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
    pub(crate) fn layout(&self) -> &DaemonLayout {
        &self.layout
    }

    pub(crate) fn replication_server_local_addr(&self, store_id: StoreId) -> Option<String> {
        self.store_sessions
            .get(&store_id)
            .and_then(StoreSession::repl_handles)
            .and_then(ReplicationHandles::server_local_addr)
    }

    /// Create a daemon with default runtime config and current layout wiring.
    pub fn new(actor: ActorId) -> Self {
        Self::new_with_limits(actor, Limits::default())
    }

    /// Create a daemon with custom limits and current layout wiring.
    pub fn new_with_limits(actor: ActorId, limits: Limits) -> Self {
        let runtime = DaemonRuntimeConfig {
            limits,
            git_sync_policy: GitSyncPolicy::from_env(),
            checkpoint_policy: CheckpointPolicy::from_env(),
            ..DaemonRuntimeConfig::default()
        };
        Self::new_with_runtime_config(actor, crate::daemon_layout_from_paths(), runtime)
    }

    /// Create a new daemon with explicit runtime wiring.
    pub fn new_with_runtime_config(
        actor: ActorId,
        layout: DaemonLayout,
        config: DaemonRuntimeConfig,
    ) -> Self {
        let limits = config.limits.clone();
        // Initialize Go-compat export worker (best effort - don't fail daemon startup)
        let export_worker = match ExportContext::new() {
            Ok(ctx) => Some(ExportWorkerHandle::start(ctx)),
            Err(e) => {
                tracing::warn!("Failed to initialize Go-compat export: {}", e);
                None
            }
        };

        Daemon {
            layout,
            store_sessions: BTreeMap::new(),
            next_store_generation: 1,
            store_caches: StoreCaches::new(),
            clock: Clock::new_with_max_forward_drift(limits.hlc_max_forward_drift_ms),
            actor_clocks: BTreeMap::new(),
            actor,
            scheduler: SyncScheduler::new(),
            checkpoint_scheduler: CheckpointScheduler::new_with_queue_limit(
                limits.max_checkpoint_job_queue,
            ),
            export_worker,
            export_pending: BTreeMap::new(),
            limits,
            git_sync_policy: config.git_sync_policy,
            checkpoint_policy: config.checkpoint_policy,
            replication: config.replication,
            checkpoint_groups: config.checkpoint_groups,
            namespace_defaults: config.namespace_defaults,
            repl_ingest_tx: None,
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

    pub(crate) fn git_sync_policy(&self) -> GitSyncPolicy {
        self.git_sync_policy
    }

    fn checkpoint_policy(&self) -> CheckpointPolicy {
        self.checkpoint_policy
    }

    pub fn begin_shutdown(&mut self) {
        self.shutting_down = true;
    }

    pub fn shutdown_export_worker(&mut self) {
        if let Some(worker) = self.export_worker.take() {
            worker.shutdown();
        }
    }

    pub(crate) fn apply_limits(&mut self, limits: Limits) -> Result<bool, OpError> {
        let old_limits = self.limits.clone();
        self.limits = limits.clone();
        self.clock
            .set_max_forward_drift(limits.hlc_max_forward_drift_ms);
        self.checkpoint_scheduler
            .set_max_queue_per_store(limits.max_checkpoint_job_queue);

        for session in self.store_sessions.values_mut() {
            session.runtime_mut().reload_limits(&limits);
        }

        let store_ids: Vec<StoreId> = self.store_sessions.keys().copied().collect();
        for store_id in store_ids {
            self.reload_replication_runtime(store_id)?;
        }

        Ok(old_limits.max_background_io_bytes_per_sec != limits.max_background_io_bytes_per_sec)
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

    fn alloc_store_session_token(&mut self, store_id: StoreId) -> StoreSessionToken {
        let generation = StoreGeneration::new(self.next_store_generation);
        self.next_store_generation = self.next_store_generation.saturating_add(1);
        StoreSessionToken::new(store_id, generation)
    }

    #[allow(dead_code)]
    pub(crate) fn session_token_for_store(&self, store_id: StoreId) -> Option<StoreSessionToken> {
        self.store_sessions.get(&store_id).map(StoreSession::token)
    }

    pub(crate) fn session_matches(&self, token: StoreSessionToken) -> bool {
        self.store_sessions
            .get(&token.store_id())
            .is_some_and(|session| session.token() == token)
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
        self.store_sessions
            .values()
            .map(|session| session.runtime().admission.ipc_inflight())
            .sum()
    }

    pub(in crate::runtime) fn fire_due_syncs(&mut self, git_tx: &Sender<GitOp>) {
        let due = self.scheduler.drain_due(Instant::now());
        for remote in due {
            self.maybe_start_sync(&remote, git_tx);
        }
    }

    /// Get iterator over all stores.
    pub fn repos(&self) -> impl Iterator<Item = (&StoreId, &GitLaneState)> {
        self.store_sessions
            .iter()
            .map(|(store_id, session)| (store_id, session.lane()))
    }

    /// Get mutable iterator over all stores.
    pub fn repos_mut(&mut self) -> impl Iterator<Item = (&StoreId, &mut GitLaneState)> {
        self.store_sessions
            .iter_mut()
            .map(|(store_id, session)| (store_id, session.lane_mut()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::sync::SyncOutcome;
    use crate::paths;
    use crate::runtime::ipc::{
        CreatePayload, MutationCtx, MutationMeta, ReadConsistency, Request, ResponsePayload,
    };
    use crate::runtime::store::discovery::store_id_from_remote;
    use beads_api::QueryResult;
    use std::collections::BTreeMap;
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard};
    use std::time::{Duration, Instant};

    use git2::{Oid, Repository, Signature};
    use uuid::Uuid;

    use crate::core::{
        ActorId, Applied, Bead, BeadCore, BeadFields, BeadId, BeadSlug, BeadType, CanonicalState,
        CheckpointContentSha256, Claim, ContentHash, DepKey, DepKind, Durable, EventBody,
        EventKindV1, HeadStatus, HlcMax, Limits, Lww, NamespaceId, NamespacePolicy, NoteAppendV1,
        NoteId, PrevVerified, Priority, ReplicaDurabilityRole, ReplicaEntry, ReplicaId,
        ReplicaRoster, SegmentId, Seq0, Seq1, Sha256, Stamp, StoreEpoch, StoreId, StoreIdentity,
        StoreMeta, StoreMetaVersions, StoreState, TxnDeltaV1, TxnId, TxnOpV1, TxnV1, VerifiedEvent,
        WallClock, Watermarks, WireBeadPatch, WireDepAddV1, WireDotV1, WireNoteV1, WireStamp,
        Workflow, WriteStamp, encode_event_body_canonical, hash_event_body, sha256_bytes,
    };
    use crate::git::checkpoint::{
        CHECKPOINT_FORMAT_VERSION, CheckpointExport, CheckpointExportInput, CheckpointFileKind,
        CheckpointManifest, CheckpointMeta, CheckpointPublishOutcome, CheckpointShardPath,
        CheckpointShardPayload, CheckpointSnapshotInput, CheckpointStoreMeta, IncludedWatermarks,
        ManifestFile, build_snapshot, export_checkpoint, policy_hash, publish_checkpoint,
        shard_for_bead, shard_name, store_state_from_legacy,
    };
    use crate::runtime::git_worker::LoadResult;
    use crate::runtime::store::lock::read_lock_meta;
    use crate::runtime::store::runtime::StoreRuntime;
    use crate::runtime::wal::frame::encode_frame;
    use crate::runtime::wal::{HlcRow, RecordHeader, RequestProof, SegmentHeader, VerifiedRecord};
    use beads_surface::ops::OpResult;
    use tempfile::TempDir;

    fn test_actor() -> ActorId {
        ActorId::new("test@host".to_string()).unwrap()
    }

    fn test_remote() -> RemoteUrl {
        RemoteUrl::new("example.com/test/repo")
    }

    static TEST_STORE_DIR_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct TempStoreDir {
        _temp: TempDir,
        data_dir: PathBuf,
        _lock: MutexGuard<'static, ()>,
        _override: crate::paths::DataDirOverride,
    }

    impl TempStoreDir {
        fn new() -> Self {
            let lock = TEST_STORE_DIR_LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            let temp = TempDir::new().unwrap();
            let data_dir = temp.path().join("data");
            std::fs::create_dir_all(&data_dir).unwrap();
            let override_guard = crate::paths::override_data_dir_for_tests(Some(data_dir.clone()));
            beads_git::init_data_dir_override(Some(data_dir.clone()));

            Self {
                _temp: temp,
                data_dir,
                _lock: lock,
                _override: override_guard,
            }
        }

        fn data_dir(&self) -> &Path {
            &self.data_dir
        }
    }

    impl Drop for TempStoreDir {
        fn drop(&mut self) {
            beads_git::init_data_dir_override(None);
        }
    }

    fn test_store_dir() -> TempStoreDir {
        TempStoreDir::new()
    }

    fn insert_store(daemon: &mut Daemon, remote: &RemoteUrl) -> StoreId {
        let store_id = store_id_from_remote(remote);
        let runtime = StoreRuntime::open(
            &daemon.layout,
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
        daemon.store_caches.remote_to_store.insert(
            remote.clone(),
            StoreIdResolution::unverified(store_id, StoreIdSource::RemoteFallback),
        );
        let token = daemon.alloc_store_session_token(store_id);
        daemon.store_sessions.insert(
            store_id,
            StoreSession::new(token, runtime, GitLaneState::new()),
        );
        store_id
    }

    fn session_token(daemon: &Daemon, store_id: StoreId) -> StoreSessionToken {
        daemon
            .session_token_for_store(store_id)
            .expect("store session token")
    }

    fn bound_repl_runtime_version(daemon: &Daemon, store_id: StoreId) -> ReplicationRuntimeVersion {
        daemon
            .store_sessions
            .get(&store_id)
            .and_then(StoreSession::repl_handles)
            .map(ReplicationHandles::runtime_version)
            .expect("replication runtime handles")
    }

    #[test]
    fn ensure_repo_loaded_rejects_non_git_repo() {
        let _tmp = test_store_dir();
        let repo_dir = TempDir::new().unwrap();
        let mut daemon = Daemon::new(test_actor());
        let (git_tx, _git_rx) = crossbeam::channel::bounded(1);

        let err = daemon
            .ensure_repo_loaded(repo_dir.path(), &git_tx)
            .unwrap_err();

        assert!(matches!(err, OpError::NotAGitRepo(path) if path == repo_dir.path()));
    }

    #[test]
    fn ensure_repo_loaded_rolls_back_store_state_when_initial_load_fails() {
        let _tmp = test_store_dir();
        let repo_dir = TempDir::new().unwrap();
        let repo_path = repo_dir.path().join("repo");
        std::fs::create_dir_all(&repo_path).expect("create repo path");

        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = store_id_from_remote(&remote);
        let resolution = StoreIdResolution::verified(store_id, StoreIdSource::PathMap);
        daemon
            .store_caches
            .path_to_remote
            .insert(repo_path.clone(), remote.clone());
        daemon
            .store_caches
            .path_to_store
            .insert(repo_path.clone(), resolution);
        daemon
            .store_caches
            .remote_to_store
            .insert(remote.clone(), resolution);

        daemon.scheduler.schedule(remote.clone());

        let (git_tx, git_rx) = crossbeam::channel::bounded(2);
        let worker = std::thread::spawn(move || {
            match git_rx.recv().expect("load-local op") {
                GitOp::LoadLocal { respond, .. } => respond
                    .send(Err(SyncError::NoLocalRef("missing local ref".to_string())))
                    .expect("respond load-local"),
                _ => panic!("expected load-local op"),
            }
            match git_rx.recv().expect("load op") {
                GitOp::Load { respond, .. } => respond
                    .send(Err(SyncError::NoLocalRef("missing remote ref".to_string())))
                    .expect("respond load"),
                _ => panic!("expected load op"),
            }
        });

        let err = daemon
            .ensure_repo_loaded(&repo_path, &git_tx)
            .expect_err("load should fail");
        worker.join().expect("worker join");

        assert!(matches!(err, OpError::RepoNotInitialized(path) if path == repo_path));
        assert!(!daemon.store_sessions.contains_key(&store_id));
        assert!(!daemon.store_sessions.contains_key(&store_id));
        assert!(daemon.export_pending.get(&store_id).is_none());
        assert!(!daemon.scheduler.is_pending(&remote));
        assert!(
            daemon
                .checkpoint_scheduler
                .checkpoint_groups_for_store(store_id)
                .is_empty()
        );
    }

    #[test]
    fn admin_reload_replication_restores_config_when_strict_load_fails() {
        let _tmp = test_store_dir();
        let repo_dir = TempDir::new().unwrap();
        let repo_path = repo_dir.path().join("repo");
        std::fs::create_dir_all(&repo_path).expect("create repo path");

        let mut daemon = Daemon::new(test_actor());
        let original_listen_addr = daemon.replication.listen_addr.clone();
        let requested_listen_addr = "127.0.0.1:40111";
        assert_ne!(original_listen_addr, requested_listen_addr);

        std::fs::write(
            repo_path.join("beads.toml"),
            format!(
                "[replication]\nlisten_addr = \"{requested_listen_addr}\"\nbackoff_base_ms = 50\nbackoff_max_ms = 500\nmax_connections = 8\n"
            ),
        )
        .expect("write beads.toml");

        let remote = test_remote();
        let store_id = store_id_from_remote(&remote);
        let resolution = StoreIdResolution::verified(store_id, StoreIdSource::PathMap);
        daemon
            .store_caches
            .path_to_remote
            .insert(repo_path.clone(), remote.clone());
        daemon
            .store_caches
            .path_to_store
            .insert(repo_path.clone(), resolution);
        daemon
            .store_caches
            .remote_to_store
            .insert(remote.clone(), resolution);

        let (git_tx, git_rx) = crossbeam::channel::bounded(1);
        let worker = std::thread::spawn(move || match git_rx.recv().expect("load op") {
            GitOp::Load { respond, .. } => respond
                .send(Err(SyncError::NoLocalRef("missing remote ref".to_string())))
                .expect("respond load"),
            _ => panic!("expected load op"),
        });

        let response = daemon.admin_reload_replication(&repo_path, &git_tx);
        worker.join().expect("worker join");

        assert!(matches!(response, Response::Err { .. }));
        assert_eq!(daemon.replication.listen_addr, original_listen_addr);
        assert_ne!(daemon.replication.listen_addr, requested_listen_addr);
        assert!(!daemon.store_sessions.contains_key(&store_id));
    }

    #[test]
    fn export_go_compat_requires_loaded_lane() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        if daemon.export_worker.is_none() {
            return;
        }

        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);

        daemon.export_go_compat(store_id, &remote);
        assert!(daemon.export_pending.get(&store_id).is_none());

        daemon
            .store_sessions
            .get_mut(&store_id)
            .expect("store session")
            .lane_mut()
            .mark_loaded_from_git();
        daemon.export_go_compat(store_id, &remote);

        assert!(daemon.export_pending.contains_key(&store_id));
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
                role: ReplicaDurabilityRole::anchor(true),
                allowed_namespaces: None,
                expire_after_ms: None,
            }],
        };
        let toml = toml::to_string(&roster).expect("encode roster");
        std::fs::write(&path, toml).expect("write replicas.toml");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("chmod replicas.toml");

        let layout = crate::daemon_layout_from_paths();
        let roster = load_replica_roster(&layout, store_id).expect("load roster");
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

        let layout = crate::daemon_layout_from_paths();
        let err = load_replica_roster(&layout, store_id).unwrap_err();
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
        let remote = RemoteUrl::new("example.com/test/repo");
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

    #[test]
    fn insert_store_for_tests_marks_git_lane_loaded() {
        let tmp = test_store_dir();
        let actor = test_actor();
        let mut daemon = Daemon::new(actor);
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).expect("create repo dir");

        let store_id = StoreId::new(Uuid::from_bytes([56u8; 16]));
        let remote = RemoteUrl::new("example.com/test/repo");
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).expect("insert store");

        let lane = daemon.store_sessions.get(&store_id).expect("lane").lane();
        assert!(lane.is_loaded_from_git());
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
            trace_id: None,
            kind: EventKindV1::TxnV1(TxnV1 {
                delta,
                hlc_max: HlcMax {
                    actor_id: ActorId::new("alice").unwrap(),
                    physical_ms: event_time_ms,
                    logical: 0,
                },
            }),
        };
        let body = body
            .into_validated(&Limits::default())
            .expect("valid event");
        let bytes = encode_event_body_canonical(&body).unwrap();
        let sha256 = hash_event_body(&bytes);
        VerifiedEvent {
            body,
            bytes,
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
        let payload = encode_event_body_canonical(event.body.as_ref()).expect("payload");
        let sha256 = hash_event_body(&payload).0;
        VerifiedRecord::new(
            RecordHeader {
                origin_replica_id: event.body.origin_replica_id,
                origin_seq: event.body.origin_seq,
                event_time_ms: event.body.event_time_ms,
                txn_id: event.body.txn_id,
                request_proof: event
                    .body
                    .client_request_id
                    .map(|client_request_id| RequestProof::ClientNoHash { client_request_id })
                    .unwrap_or(RequestProof::None),
                sha256: sha256,
                prev_sha256: event.prev.prev.map(|sha| sha.0),
            },
            payload,
            event.body.clone(),
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
        *daemon
            .store_sessions
            .get_mut(&store_id)
            .expect("store session")
            .lane_mut() = repo_state;

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
        daemon.complete_refresh(session_token(&daemon, store_id), &remote, result);

        let repo_state = daemon.store_sessions.get(&store_id).unwrap().lane();
        assert!(!repo_state.refresh_in_progress);
        assert!(repo_state.last_refresh.is_some());
    }

    #[test]
    fn read_scope_defaults_namespace_and_timeout() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);
        let loaded = daemon.loaded_store(store_id, remote);

        let scope = ReadScope::new(ReadConsistency::default(), &loaded.runtime().policies).unwrap();
        assert_eq!(scope.namespace(), &NamespaceId::core());
        assert_eq!(scope.wait_timeout_ms(), 0);
        assert!(scope.require_min_seen().is_none());
    }

    #[test]
    fn read_scope_rejects_unknown_namespace() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);
        let loaded = daemon.loaded_store(store_id, remote);

        let unknown = NamespaceId::parse("unknown").unwrap();
        assert!(!loaded.runtime().policies.contains_key(&unknown));

        let read = ReadConsistency {
            namespace: Some(unknown.clone()),
            ..ReadConsistency::default()
        };
        let err = ReadScope::new(read, &loaded.runtime().policies).unwrap_err();
        assert!(matches!(err, OpError::NamespaceUnknown { namespace } if namespace == unknown));
    }

    #[test]
    fn read_gate_requires_min_seen() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);
        let namespace = NamespaceId::core();
        let origin = daemon
            .store_sessions
            .get(&store_id)
            .expect("store session")
            .runtime()
            .meta
            .replica_id;
        let proof = daemon.loaded_store(store_id, remote.clone());
        let policies = proof.runtime().policies.clone();
        let mut required = Watermarks::<Applied>::new();
        required
            .observe_at_least(
                &namespace,
                &origin,
                Seq0::new(1),
                HeadStatus::Known([1u8; 32]),
            )
            .expect("watermark");

        let read = ReadScope::new(
            ReadConsistency {
                namespace: Some(namespace.clone()),
                require_min_seen: Some(required.clone()),
                wait_timeout_ms: None,
            },
            &policies,
        )
        .unwrap();
        let err = proof.check_read_gate(&read).unwrap_err();
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

        let read = ReadScope::new(
            ReadConsistency {
                namespace: Some(namespace.clone()),
                require_min_seen: Some(required.clone()),
                wait_timeout_ms: Some(50),
            },
            &policies,
        )
        .unwrap();
        let err = proof.check_read_gate(&read).unwrap_err();
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

        drop(proof);
        let runtime = daemon
            .store_sessions
            .get_mut(&store_id)
            .expect("store runtime")
            .runtime_mut();
        runtime
            .watermarks_applied
            .observe_at_least(
                &namespace,
                &origin,
                Seq0::new(1),
                HeadStatus::Known([1u8; 32]),
            )
            .expect("watermark");
        let read = ReadScope::new(
            ReadConsistency {
                namespace: Some(namespace),
                require_min_seen: Some(required),
                wait_timeout_ms: None,
            },
            &policies,
        )
        .unwrap();
        let proof = daemon.loaded_store(store_id, remote);
        assert!(proof.check_read_gate(&read).is_ok());
    }

    #[test]
    fn complete_refresh_clears_flag_on_error() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = insert_store(&mut daemon, &remote);

        let mut repo_state = GitLaneState::new();
        repo_state.refresh_in_progress = true;
        *daemon
            .store_sessions
            .get_mut(&store_id)
            .expect("store session")
            .lane_mut() = repo_state;

        // Complete refresh with error
        let result = Err(SyncError::NoLocalRef("/test".to_string()));
        daemon.complete_refresh(session_token(&daemon, store_id), &remote, result);

        let repo_state = daemon.store_sessions.get(&store_id).unwrap().lane();
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
        *daemon
            .store_sessions
            .get_mut(&store_id)
            .expect("store session")
            .lane_mut() = repo_state;

        // Create fresh state with some content
        let fresh_state = CanonicalState::new();
        let result = Ok(LoadResult {
            state: fresh_state.clone(),
            root_slug: Some(BeadSlug::parse("fresh-slug").unwrap()),
            needs_sync: false,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(session_token(&daemon, store_id), &remote, result);

        let repo_state = daemon.store_sessions.get(&store_id).unwrap().lane();
        assert_eq!(
            repo_state.root_slug,
            Some(BeadSlug::parse("fresh-slug").unwrap())
        );
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
        repo_state.root_slug = Some(BeadSlug::parse("original-slug").unwrap());
        *daemon
            .store_sessions
            .get_mut(&store_id)
            .expect("store session")
            .lane_mut() = repo_state;

        // Try to apply refresh
        let result = Ok(LoadResult {
            state: CanonicalState::new(),
            root_slug: Some(BeadSlug::parse("new-slug").unwrap()),
            needs_sync: false,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(session_token(&daemon, store_id), &remote, result);

        let repo_state = daemon.store_sessions.get(&store_id).unwrap().lane();
        // Should keep original slug since dirty
        assert_eq!(
            repo_state.root_slug,
            Some(BeadSlug::parse("original-slug").unwrap())
        );
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
        *daemon
            .store_sessions
            .get_mut(&store_id)
            .expect("store session")
            .lane_mut() = repo_state;

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
        daemon.complete_refresh(session_token(&daemon, store_id), &remote, result);

        let repo_state = daemon.store_sessions.get(&store_id).unwrap().lane();
        // Should mark dirty to trigger sync
        assert!(repo_state.dirty);
        // And scheduler should have it pending
        assert!(daemon.scheduler.is_pending(&remote));
    }

    #[test]
    fn complete_refresh_unknown_remote_is_noop() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let unknown = RemoteUrl::new("unknown.com/repo");
        let token = StoreSessionToken::new(StoreId::new(Uuid::nil()), StoreGeneration::new(0));

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
        daemon.complete_refresh(token, &unknown, result);
        // Just verify no panic and daemon is still valid
        assert!(daemon.store_sessions.is_empty());
    }

    #[test]
    fn reload_invalidates_generation_atomically_and_rejects_stale_handles() {
        let tmp = TempStoreDir::new();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = store_id_from_remote(&remote);
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).expect("create repo");
        insert_store_for_tests(&mut daemon, store_id, remote.clone(), &repo_path)
            .expect("insert store");

        let token_v1 = session_token(&daemon, store_id);
        daemon.drop_store_state(store_id);
        insert_store_for_tests(&mut daemon, store_id, remote.clone(), &repo_path)
            .expect("reload store");
        let token_v2 = session_token(&daemon, store_id);
        assert_ne!(token_v1, token_v2);
        let (repl_tx, _repl_rx) = crossbeam::channel::bounded(8);
        daemon.set_repl_ingest_tx(repl_tx);
        daemon
            .ensure_replication_runtime(store_id)
            .expect("bind replication runtime");
        let runtime_version = bound_repl_runtime_version(&daemon, store_id);

        {
            let mut loaded = daemon.loaded_store(store_id, remote.clone());
            let lane = loaded.lane_mut();
            lane.refresh_in_progress = true;
            lane.root_slug = Some(BeadSlug::parse("fresh-slug").expect("slug"));
        }
        let lane_before = {
            let loaded = daemon.loaded_store(store_id, remote.clone());
            (
                loaded.lane().refresh_in_progress,
                loaded.lane().root_slug.clone(),
            )
        };

        let stale_refresh = Ok(LoadResult {
            state: CanonicalState::new(),
            root_slug: Some(BeadSlug::parse("stale-slug").expect("slug")),
            needs_sync: true,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(token_v1, &remote, stale_refresh);

        let loaded = daemon.loaded_store(store_id, remote.clone());
        assert_eq!(loaded.lane().refresh_in_progress, lane_before.0);
        assert_eq!(loaded.lane().root_slug, lane_before.1);
        drop(loaded);

        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([90u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::ZERO);
        let bead_id = BeadId::parse("bd-stale-repl").expect("bead id");
        let mut patch = WireBeadPatch::new(bead_id.clone());
        patch.title = Some("replicated".to_string());
        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
            .expect("delta insert");
        let event = verified_event_with_delta(store, &namespace, origin, 1, None, delta);
        let batch = ContiguousBatch::try_new(vec![event]).expect("batch");
        let now_ms = 1_700_000_000_000u64;

        let (stale_tx, stale_rx) = crossbeam::channel::bounded(1);
        daemon.handle_repl_ingest(ReplIngestRequest {
            session: token_v1,
            runtime_version,
            batch: batch.clone(),
            now_ms,
            respond: stale_tx,
        });
        let stale = stale_rx.recv().expect("stale response");
        match stale {
            Ok(_) => panic!("stale ingest unexpectedly succeeded"),
            Err(err) => assert!(err.retryable),
        }

        let (fresh_tx, fresh_rx) = crossbeam::channel::bounded(1);
        daemon.handle_repl_ingest(ReplIngestRequest {
            session: token_v2,
            runtime_version,
            batch,
            now_ms,
            respond: fresh_tx,
        });
        assert!(fresh_rx.recv().expect("fresh response").is_ok());
        let state = daemon
            .store_sessions
            .get(&store_id)
            .expect("store session")
            .runtime()
            .state
            .get(&namespace)
            .expect("namespace state");
        assert!(state.get_live(&bead_id).is_some());
    }

    #[test]
    fn ensure_replication_runtime_rebinds_when_binding_version_changes() {
        let tmp = TempStoreDir::new();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = store_id_from_remote(&remote);
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).expect("create repo");
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).expect("insert store");

        let (repl_tx, _repl_rx) = crossbeam::channel::bounded(8);
        daemon.set_repl_ingest_tx(repl_tx);
        daemon
            .ensure_replication_runtime(store_id)
            .expect("initial bind");
        let runtime_version_v1 = bound_repl_runtime_version(&daemon, store_id);

        let rotation = daemon
            .store_sessions
            .get_mut(&store_id)
            .expect("store session")
            .runtime_mut()
            .rotate_replica_id()
            .expect("rotate replica id");
        assert!(rotation.runtime_version > runtime_version_v1);

        daemon
            .ensure_replication_runtime(store_id)
            .expect("rebind replication runtime");
        let runtime_version_v2 = bound_repl_runtime_version(&daemon, store_id);
        assert_eq!(runtime_version_v2, rotation.runtime_version);
    }

    #[test]
    fn replica_id_rotation_invalidates_old_replication_runtime_and_requires_rebind_before_ingest() {
        let tmp = TempStoreDir::new();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = store_id_from_remote(&remote);
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).expect("create repo");
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).expect("insert store");

        let (repl_tx, _repl_rx) = crossbeam::channel::bounded(8);
        daemon.set_repl_ingest_tx(repl_tx);
        daemon
            .ensure_replication_runtime(store_id)
            .expect("initial bind");
        let runtime_version_v1 = bound_repl_runtime_version(&daemon, store_id);

        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([91u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::ZERO);
        let bead_id = BeadId::parse("bd-rotate-repl").expect("bead id");
        let mut patch_v1 = WireBeadPatch::new(bead_id.clone());
        patch_v1.title = Some("replicated v1".to_string());
        let mut delta_v1 = TxnDeltaV1::new();
        delta_v1
            .insert(TxnOpV1::BeadUpsert(Box::new(patch_v1)))
            .expect("delta insert");
        let event_v1 = verified_event_with_delta(store, &namespace, origin, 1, None, delta_v1);
        let mut patch_v2 = WireBeadPatch::new(bead_id.clone());
        patch_v2.title = Some("replicated v2".to_string());
        let mut delta_v2 = TxnDeltaV1::new();
        delta_v2
            .insert(TxnOpV1::BeadUpsert(Box::new(patch_v2)))
            .expect("delta insert");
        let event_v2 = verified_event_with_delta(
            store,
            &namespace,
            origin,
            2,
            Some(event_v1.sha256.clone()),
            delta_v2,
        );
        let batch_v1 = ContiguousBatch::try_new(vec![event_v1]).expect("batch v1");
        let batch_v2 = ContiguousBatch::try_new(vec![event_v2]).expect("batch v2");
        let now_ms = 1_700_000_000_000u64;
        let token = session_token(&daemon, store_id);

        let (initial_tx, initial_rx) = crossbeam::channel::bounded(1);
        daemon.handle_repl_ingest(ReplIngestRequest {
            session: token,
            runtime_version: runtime_version_v1,
            batch: batch_v1,
            now_ms,
            respond: initial_tx,
        });
        assert!(initial_rx.recv().expect("initial ingest response").is_ok());

        let rotation = daemon
            .store_sessions
            .get_mut(&store_id)
            .expect("store session")
            .runtime_mut()
            .rotate_replica_id()
            .expect("rotate replica id");
        let stale_handles = daemon
            .store_sessions
            .get_mut(&store_id)
            .and_then(StoreSession::take_repl_handles)
            .expect("bound handles");
        stale_handles.shutdown();
        assert!(
            daemon
                .store_sessions
                .get(&store_id)
                .and_then(StoreSession::repl_handles)
                .is_none()
        );

        let (stale_v1_tx, stale_v1_rx) = crossbeam::channel::bounded(1);
        daemon.handle_repl_ingest(ReplIngestRequest {
            session: token,
            runtime_version: runtime_version_v1,
            batch: batch_v2.clone(),
            now_ms,
            respond: stale_v1_tx,
        });
        let stale_v1 = stale_v1_rx
            .recv()
            .expect("stale v1 response")
            .expect_err("stale v1 should fail");

        let (stale_v2_tx, stale_v2_rx) = crossbeam::channel::bounded(1);
        daemon.handle_repl_ingest(ReplIngestRequest {
            session: token,
            runtime_version: rotation.runtime_version,
            batch: batch_v2.clone(),
            now_ms,
            respond: stale_v2_tx,
        });
        let stale_v2 = stale_v2_rx
            .recv()
            .expect("stale v2 response")
            .expect_err("stale v2 should fail");
        assert!(stale_v1.retryable);
        assert!(stale_v2.retryable);
        assert_eq!(stale_v1.code, stale_v2.code);
        assert_eq!(stale_v1.message, stale_v2.message);

        daemon
            .reload_replication_runtime(store_id)
            .expect("explicit rebind");
        assert_eq!(
            bound_repl_runtime_version(&daemon, store_id),
            rotation.runtime_version
        );

        let (rebound_tx, rebound_rx) = crossbeam::channel::bounded(1);
        daemon.handle_repl_ingest(ReplIngestRequest {
            session: token,
            runtime_version: rotation.runtime_version,
            batch: batch_v2,
            now_ms,
            respond: rebound_tx,
        });
        assert!(rebound_rx.recv().expect("rebound response").is_ok());
    }

    #[test]
    fn complete_sync_ignores_stale_session_token() {
        let tmp = TempStoreDir::new();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = store_id_from_remote(&remote);
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).expect("create repo");
        insert_store_for_tests(&mut daemon, store_id, remote.clone(), &repo_path)
            .expect("insert store");
        let token_v1 = session_token(&daemon, store_id);

        daemon.drop_store_state(store_id);
        insert_store_for_tests(&mut daemon, store_id, remote.clone(), &repo_path)
            .expect("reload store");
        let token_v2 = session_token(&daemon, store_id);
        assert_ne!(token_v1, token_v2);

        let live = make_bead("bd-current", 2_000, "alice");
        let mut current = CanonicalState::new();
        current.insert_live(live.clone());
        {
            let mut loaded = daemon.loaded_store(store_id, remote.clone());
            loaded.runtime_mut().state.set_core_state(current);
            loaded.lane_mut().sync_in_progress = true;
        }

        let stale = make_bead("bd-stale", 1_000, "bob");
        let mut stale_state = CanonicalState::new();
        stale_state.insert_live(stale);
        daemon.complete_sync(
            token_v1,
            &remote,
            Ok(SyncOutcome {
                last_seen_stamp: stale_state.max_write_stamp(),
                state: stale_state,
                divergence: None,
                force_push: None,
            }),
        );

        let loaded = daemon.loaded_store(store_id, remote);
        let core_state = loaded
            .runtime()
            .state
            .get(&NamespaceId::core())
            .expect("core state");
        assert!(core_state.get_live(&live.core.id).is_some());
        assert!(loaded.lane().sync_in_progress);
    }

    #[test]
    fn complete_checkpoint_ignores_stale_session_token_after_reload() {
        let tmp = TempStoreDir::new();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let store_id = store_id_from_remote(&remote);
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).expect("create repo");
        insert_store_for_tests(&mut daemon, store_id, remote.clone(), &repo_path)
            .expect("insert store");
        let token_v1 = session_token(&daemon, store_id);

        daemon.drop_store_state(store_id);
        insert_store_for_tests(&mut daemon, store_id, remote.clone(), &repo_path)
            .expect("reload store");
        let token_v2 = session_token(&daemon, store_id);
        assert_ne!(token_v1, token_v2);

        let namespace = NamespaceId::core();
        let bead_id = BeadId::parse("bd-checkpoint-stale").expect("bead id");
        let dirty_shard = CheckpointShardPath::new(
            namespace.clone(),
            CheckpointFileKind::State,
            shard_for_bead(&bead_id),
        );
        {
            let mut loaded = daemon.loaded_store(store_id, remote.clone());
            let mut core_state = CanonicalState::new();
            core_state.insert_live(make_bead("bd-checkpoint-stale", 1_700_000_000_000, "alice"));
            loaded.runtime_mut().state.set_core_state(core_state);
            loaded.runtime_mut().record_checkpoint_dirty_paths(
                &namespace,
                std::iter::once(dirty_shard.clone()).collect(),
            );
            let snapshot = loaded.runtime_mut().checkpoint_snapshot(
                "core",
                std::slice::from_ref(&namespace),
                1_700_000_000_000,
            );
            assert!(
                snapshot
                    .expect("checkpoint snapshot")
                    .dirty_shards
                    .contains(&dirty_shard)
            );
        }

        let key = crate::runtime::checkpoint_scheduler::CheckpointGroupKey {
            store_id,
            group: "core".to_string(),
        };
        daemon
            .checkpoint_scheduler
            .start_in_flight(&key, Instant::now());

        daemon.complete_checkpoint(
            token_v1,
            "core",
            Ok(CheckpointPublishOutcome {
                checkpoint_id: CheckpointContentSha256::from_checkpoint_preimage_bytes(b"stale"),
                checkpoint_commit: Oid::zero(),
                store_meta_commit: Oid::zero(),
            }),
        );

        let snapshots = daemon.checkpoint_group_snapshots(store_id);
        let core_snapshot = snapshots
            .iter()
            .find(|snapshot| snapshot.group == "core")
            .expect("core checkpoint snapshot");
        assert!(core_snapshot.in_flight);
        assert!(core_snapshot.last_checkpoint_wall_ms.is_none());

        let mut loaded = daemon.loaded_store(store_id, remote);
        let snapshot = loaded.runtime_mut().checkpoint_snapshot(
            "core",
            std::slice::from_ref(&namespace),
            1_700_000_000_001,
        );
        assert!(
            snapshot
                .expect("checkpoint snapshot")
                .dirty_shards
                .contains(&dirty_shard)
        );
    }

    fn store_policy_hash(daemon: &Daemon, store_id: StoreId) -> ContentHash {
        let policies = &daemon
            .store_sessions
            .get(&store_id)
            .expect("store session")
            .runtime()
            .policies;
        policy_hash(policies).expect("policy hash")
    }

    fn empty_load_result() -> LoadResult {
        LoadResult {
            state: CanonicalState::new(),
            root_slug: None,
            needs_sync: false,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        }
    }

    fn setup_checkpoint_import_fixture(
        checkpoint_policy_hash: Option<ContentHash>,
        checkpoint_roster_hash: Option<ContentHash>,
        bead_slug: &str,
    ) -> (Daemon, RemoteUrl, TempDir, StoreId, BeadId) {
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();

        let repo_dir = TempDir::new().unwrap();
        let repo = Repository::init(repo_dir.path()).unwrap();

        let store_id = store_id_from_remote(&remote);
        insert_store_for_tests(&mut daemon, store_id, remote.clone(), repo_dir.path())
            .expect("store init");
        let checkpoint_policy_hash =
            checkpoint_policy_hash.unwrap_or_else(|| store_policy_hash(&daemon, store_id));

        let bead = make_bead(bead_slug, 1_700_000_000_000, "checkpoint@test");
        let bead_id = bead.core.id.clone();
        let mut legacy_state = CanonicalState::new();
        legacy_state.insert_live(bead);
        let store_state = store_state_from_legacy(legacy_state);

        let origin = daemon
            .store_sessions
            .get(&store_id)
            .expect("store session")
            .runtime()
            .meta
            .replica_id;
        let watermarks = Watermarks::<Durable>::new();

        let snapshot = build_snapshot(CheckpointSnapshotInput {
            checkpoint_group: "core".to_string(),
            namespaces: vec![NamespaceId::core()].into(),
            store_id,
            store_epoch: StoreEpoch::ZERO,
            created_at_ms: 1_700_000_000_000,
            created_by_replica_id: origin,
            policy_hash: checkpoint_policy_hash,
            roster_hash: checkpoint_roster_hash,
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

        (daemon, remote, repo_dir, store_id, bead_id)
    }

    fn write_checkpoint_git_entry(
        repo: &Repository,
        store_id: StoreId,
        checkpoint_group: &str,
        origin: ReplicaId,
        included_seq: u64,
        deps_bytes: &[u8],
    ) {
        let shard_path =
            CheckpointShardPath::new(NamespaceId::core(), CheckpointFileKind::Deps, shard_name(0));
        let manifest = CheckpointManifest {
            checkpoint_group: checkpoint_group.to_string(),
            store_id,
            store_epoch: StoreEpoch::ZERO,
            namespaces: vec![NamespaceId::core()].into(),
            files: BTreeMap::from([(
                shard_path.clone(),
                ManifestFile {
                    sha256: ContentHash::from_bytes(sha256_bytes(deps_bytes).0),
                    bytes: deps_bytes.len() as u64,
                },
            )]),
        };
        let mut included = IncludedWatermarks::new();
        included
            .entry(NamespaceId::core())
            .or_default()
            .insert(origin, included_seq);
        let mut meta = CheckpointMeta {
            checkpoint_format_version: CHECKPOINT_FORMAT_VERSION,
            store_id,
            store_epoch: StoreEpoch::ZERO,
            checkpoint_group: checkpoint_group.to_string(),
            namespaces: vec![NamespaceId::core()].into(),
            created_at_ms: 1_700_000_000_000,
            created_by_replica_id: origin,
            policy_hash: ContentHash::from_bytes([3u8; 32]),
            roster_hash: None,
            included,
            included_heads: None,
            content_hash: CheckpointContentSha256::from_checkpoint_preimage_bytes(&[0u8; 32]),
            manifest_hash: manifest.manifest_hash().expect("manifest hash"),
        };
        meta.content_hash = meta.compute_content_hash().expect("content hash");
        let export = CheckpointExport {
            manifest,
            meta,
            files: BTreeMap::from([(
                shard_path.clone(),
                CheckpointShardPayload {
                    path: shard_path,
                    bytes: deps_bytes.to_vec().into(),
                },
            )]),
        };

        let git_ref = format!("refs/beads/{store_id}/{checkpoint_group}");
        let mut checkpoint_groups = BTreeMap::new();
        checkpoint_groups.insert(checkpoint_group.to_string(), git_ref.clone());
        let store_meta = CheckpointStoreMeta::new(
            store_id,
            StoreEpoch::ZERO,
            CHECKPOINT_FORMAT_VERSION,
            checkpoint_groups,
        );
        let meta_oid = repo
            .blob(&export.meta.canon_bytes().expect("checkpoint meta bytes"))
            .expect("checkpoint meta blob");
        let manifest_oid = repo
            .blob(
                &export
                    .manifest
                    .canon_bytes()
                    .expect("checkpoint manifest bytes"),
            )
            .expect("checkpoint manifest blob");
        let deps_oid = repo.blob(deps_bytes).expect("checkpoint deps blob");

        let mut deps_builder = repo.treebuilder(None).expect("deps treebuilder");
        deps_builder
            .insert("00.jsonl", deps_oid, 0o100644)
            .expect("insert deps shard");
        let deps_tree_oid = deps_builder.write().expect("write deps tree");

        let mut core_builder = repo.treebuilder(None).expect("core treebuilder");
        core_builder
            .insert("deps", deps_tree_oid, 0o040000)
            .expect("insert deps dir");
        let core_tree_oid = core_builder.write().expect("write core tree");

        let mut namespaces_builder = repo.treebuilder(None).expect("namespaces treebuilder");
        namespaces_builder
            .insert("core", core_tree_oid, 0o040000)
            .expect("insert core dir");
        let namespaces_tree_oid = namespaces_builder.write().expect("write namespaces tree");

        let mut root_builder = repo.treebuilder(None).expect("checkpoint root treebuilder");
        root_builder
            .insert("meta.json", meta_oid, 0o100644)
            .expect("insert checkpoint meta");
        root_builder
            .insert("manifest.json", manifest_oid, 0o100644)
            .expect("insert checkpoint manifest");
        root_builder
            .insert("namespaces", namespaces_tree_oid, 0o040000)
            .expect("insert namespaces dir");
        let root_tree_oid = root_builder.write().expect("write checkpoint root tree");
        let root_tree = repo
            .find_tree(root_tree_oid)
            .expect("find checkpoint root tree");

        let sig = Signature::now("beads", "beads@localhost").expect("checkpoint signature");
        let checkpoint_commit = repo
            .commit(
                Some(&git_ref),
                &sig,
                &sig,
                "test checkpoint ref",
                &root_tree,
                &[],
            )
            .expect("write checkpoint ref commit");

        let store_meta_oid = repo
            .blob(&store_meta.canon_bytes().expect("store meta bytes"))
            .expect("store meta blob");
        let mut store_meta_builder = repo.treebuilder(None).expect("store meta treebuilder");
        store_meta_builder
            .insert("store_meta.json", store_meta_oid, 0o100644)
            .expect("insert store meta file");
        let store_meta_tree_oid = store_meta_builder.write().expect("write store meta tree");
        let store_meta_tree = repo
            .find_tree(store_meta_tree_oid)
            .expect("find store meta tree");
        repo.commit(
            Some("refs/beads/meta"),
            &sig,
            &sig,
            "test checkpoint store meta",
            &store_meta_tree,
            &[],
        )
        .expect("write checkpoint store meta ref");
        assert_eq!(
            repo.refname_to_id(&git_ref).expect("checkpoint ref"),
            checkpoint_commit
        );
    }

    fn build_valid_checkpoint_export(
        store_id: StoreId,
        checkpoint_group: &str,
        origin: ReplicaId,
        included_seq: u64,
        policy_hash: ContentHash,
        bead_id: &str,
    ) -> CheckpointExport {
        let bead = make_bead(bead_id, 1_700_000_000_000, "checkpoint@test");
        let mut legacy_state = CanonicalState::new();
        legacy_state.insert_live(bead);
        let store_state = store_state_from_legacy(legacy_state);

        let mut watermarks = Watermarks::<Durable>::new();
        let head = [7u8; 32];
        watermarks
            .observe_at_least(
                &NamespaceId::core(),
                &origin,
                Seq0::new(included_seq),
                HeadStatus::Known(head),
            )
            .expect("watermark");

        let snapshot = build_snapshot(CheckpointSnapshotInput {
            checkpoint_group: checkpoint_group.to_string(),
            namespaces: vec![NamespaceId::core()].into(),
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

        export_checkpoint(CheckpointExportInput {
            snapshot: &snapshot,
            previous: None,
        })
        .expect("checkpoint export")
    }

    fn write_valid_checkpoint_cache_entry(
        store_id: StoreId,
        checkpoint_group: &str,
        origin: ReplicaId,
        included_seq: u64,
        policy_hash: ContentHash,
        bead_id: &str,
    ) {
        let export = build_valid_checkpoint_export(
            store_id,
            checkpoint_group,
            origin,
            included_seq,
            policy_hash,
            bead_id,
        );
        CheckpointCache::new(store_id, checkpoint_group)
            .publish(&export)
            .expect("publish valid checkpoint cache entry");
    }

    #[test]
    fn load_from_checkpoint_ref_skips_policy_hash_mismatch_checkpoint() {
        let _tmp = test_store_dir();
        let checkpoint_policy_hash = ContentHash::from_bytes([9u8; 32]);

        let (mut daemon, remote, repo_dir, store_id, checkpoint_bead_id) =
            setup_checkpoint_import_fixture(
                Some(checkpoint_policy_hash),
                None,
                "bd-checkpoint-policy-mismatch",
            );
        daemon
            .apply_loaded_repo_state(store_id, &remote, repo_dir.path(), empty_load_result())
            .expect("policy mismatch should warn and continue");

        let store = daemon
            .store_sessions
            .get(&store_id)
            .expect("store runtime")
            .runtime();
        assert!(store.state.core().get_live(&checkpoint_bead_id).is_none());
    }

    #[test]
    fn load_from_checkpoint_ref_skips_roster_hash_mismatch_checkpoint() {
        let _tmp = test_store_dir();
        let (mut daemon, remote, repo_dir, store_id, checkpoint_bead_id) =
            setup_checkpoint_import_fixture(
                None,
                Some(ContentHash::from_bytes([8u8; 32])),
                "bd-checkpoint-roster-mismatch",
            );
        daemon
            .apply_loaded_repo_state(store_id, &remote, repo_dir.path(), empty_load_result())
            .expect("roster mismatch should warn and continue");

        let store = daemon
            .store_sessions
            .get(&store_id)
            .expect("store runtime")
            .runtime();
        assert!(store.state.core().get_live(&checkpoint_bead_id).is_none());
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
            .store_sessions
            .get(&store_id)
            .expect("store session")
            .runtime()
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

        let policy_hash = store_policy_hash(&daemon, store_id);

        let snapshot = build_snapshot(CheckpointSnapshotInput {
            checkpoint_group: "core".to_string(),
            namespaces: vec![NamespaceId::core()].into(),
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

        let loaded = empty_load_result();
        daemon
            .apply_loaded_repo_state(store_id, &remote, repo_dir.path(), loaded)
            .expect("load repo");

        let store = daemon
            .store_sessions
            .get(&store_id)
            .expect("store runtime")
            .runtime();
        let core = store.state.core();
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
    fn incompatible_checkpoint_git_still_schedules_rebuild_when_cache_import_succeeds() {
        let _tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();

        let repo_dir = TempDir::new().unwrap();
        let repo = Repository::init(repo_dir.path()).unwrap();

        let store_id = store_id_from_remote(&remote);
        insert_store_for_tests(&mut daemon, store_id, remote.clone(), repo_dir.path())
            .expect("store init");
        let origin = daemon
            .store_sessions
            .get(&store_id)
            .expect("store session")
            .runtime()
            .meta
            .replica_id;
        let policy_hash = store_policy_hash(&daemon, store_id);
        write_valid_checkpoint_cache_entry(
            store_id,
            "core",
            origin,
            3,
            policy_hash,
            "bd-cache-valid",
        );
        write_checkpoint_git_entry(
            &repo,
            store_id,
            "core",
            origin,
            3,
            br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
"#,
        );

        daemon
            .apply_loaded_repo_state(store_id, &remote, repo_dir.path(), empty_load_result())
            .expect("load repo");

        let store = daemon
            .store_sessions
            .get(&store_id)
            .expect("store session")
            .runtime();
        let core = store.state.core();
        let bead_id = BeadId::parse("bd-cache-valid").expect("checkpoint bead id");
        assert!(
            core.get_live(&bead_id).is_some(),
            "valid cache checkpoint state should still merge even when git import is incompatible"
        );
        let durable = store
            .watermarks_durable
            .get(&NamespaceId::core(), &origin)
            .copied()
            .expect("durable watermark from cache import");
        assert_eq!(durable.seq().get(), 3);

        let snapshots = daemon.checkpoint_group_snapshots(store_id);
        let core = snapshots
            .iter()
            .find(|snapshot| snapshot.group == "core")
            .expect("core snapshot");
        assert!(
            core.dirty,
            "incompatible git checkpoint should still schedule a rebuild even when cache import succeeded"
        );
    }

    #[test]
    fn apply_loaded_repo_state_replay_marks_checkpoint_dirty_shards() {
        let tmp = test_store_dir();
        let mut daemon = Daemon::new(test_actor());
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).expect("create repo");

        let store_id = store_id_from_remote(&remote);
        insert_store_for_tests(&mut daemon, store_id, remote.clone(), &repo_path)
            .expect("insert store");
        let (git_tx, _git_rx) = crossbeam::channel::unbounded();
        let create = daemon.handle_request(
            Request::Create {
                ctx: MutationCtx::new(repo_path.clone(), MutationMeta::default()),
                payload: CreatePayload {
                    id: None,
                    parent: None,
                    title: "replay dirty shard".to_string(),
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
                },
            },
            &git_tx,
        );
        let bead_id = match create {
            HandleOutcome::Response(Response::Ok {
                ok: ResponsePayload::Op(op),
            }) => match op.result {
                OpResult::Created { id } => id,
                other => panic!("unexpected op result: {other:?}"),
            },
            other => panic!("unexpected create outcome: {other:?}"),
        };

        let namespace = NamespaceId::core();
        {
            let store = daemon
                .store_sessions
                .get_mut(&store_id)
                .expect("store runtime")
                .runtime_mut();
            let _ = store
                .checkpoint_snapshot("core", std::slice::from_ref(&namespace), 1_700_000_000_000)
                .expect("initial snapshot");
            store.commit_checkpoint_dirty_shards("core");
            store.state = StoreState::new();
        }

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
            .apply_loaded_repo_state(store_id, &remote, &repo_path, loaded)
            .expect("apply loaded state");

        let lane = daemon
            .store_sessions
            .get(&store_id)
            .expect("git lane")
            .lane();
        assert!(lane.dirty);

        let store = daemon
            .store_sessions
            .get_mut(&store_id)
            .expect("store runtime")
            .runtime_mut();
        assert!(store.state.core().get_live(&bead_id).is_some());
        let snapshot = store
            .checkpoint_snapshot("core", std::slice::from_ref(&namespace), 1_700_000_000_001)
            .expect("checkpoint snapshot");
        let expected_state_path = CheckpointShardPath::new(
            namespace,
            CheckpointFileKind::State,
            shard_for_bead(&bead_id),
        );
        assert!(snapshot.dirty_shards.contains(&expected_state_path));
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
            let runtime = daemon
                .store_sessions
                .get_mut(&store_id)
                .unwrap()
                .runtime_mut();
            runtime
                .policies
                .insert(tmp_ns.clone(), NamespacePolicy::tmp_default());
        }

        let (git_tx, _git_rx) = crossbeam::channel::unbounded();
        let response = daemon.handle_request(
            Request::Create {
                ctx: MutationCtx::new(
                    repo_path.clone(),
                    MutationMeta {
                        namespace: Some(tmp_ns.clone()),
                        ..MutationMeta::default()
                    },
                ),
                payload: CreatePayload {
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
            namespace: Some(tmp_ns.clone()),
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

        let store_state = &daemon
            .store_sessions
            .get(&store_id)
            .unwrap()
            .runtime()
            .state;
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

            let runtime = daemon.store_sessions.get(&store_id).unwrap().runtime();
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
                ctx: MutationCtx::new(
                    repo_path.clone(),
                    MutationMeta {
                        actor_id: Some(actor.clone()),
                        ..MutationMeta::default()
                    },
                ),
                payload: CreatePayload {
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

        let runtime = daemon.store_sessions.get(&store_id).unwrap().runtime();
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
        let (repl_tx, _repl_rx) = crossbeam::channel::bounded(8);
        daemon.set_repl_ingest_tx(repl_tx);
        daemon
            .ensure_replication_runtime(store_id)
            .expect("bind replication runtime");
        let runtime_version = bound_repl_runtime_version(&daemon, store_id);

        {
            let runtime = daemon
                .store_sessions
                .get_mut(&store_id)
                .unwrap()
                .runtime_mut();
            runtime
                .policies
                .insert(namespace.clone(), NamespacePolicy::core_default());
        }

        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        let batch = ContiguousBatch::try_new(vec![event]).expect("contiguous batch");
        daemon.handle_repl_ingest(ReplIngestRequest {
            session: session_token(&daemon, store_id),
            runtime_version,
            batch,
            now_ms,
            respond: respond_tx,
        });

        let outcome = respond_rx.recv().expect("ingest response");
        assert!(outcome.is_ok());

        let state = daemon
            .store_sessions
            .get(&store_id)
            .expect("store session")
            .runtime()
            .state
            .get(&namespace)
            .expect("namespace state");
        assert!(state.get_live(&bead_id).is_some());
    }

    #[test]
    fn complete_sync_preserves_local_state_on_collision() {
        let _tmp = test_store_dir();
        let remote = test_remote();
        let mut daemon = Daemon::new(test_actor());
        let store_id = insert_store(&mut daemon, &remote);

        let winner = make_bead("bd-abc", 1000, "alice");
        let loser = make_bead("bd-abc", 2000, "bob");
        let loser_created = loser.core.created().clone();

        let mut synced_state = CanonicalState::new();
        synced_state.insert_live(winner.clone());

        let mut local_state = CanonicalState::new();
        local_state.insert_live(loser);

        {
            let store = daemon
                .store_sessions
                .get_mut(&store_id)
                .unwrap()
                .runtime_mut();
            store.state.set_core_state(local_state);
        }
        {
            let repo_state = daemon.store_sessions.get_mut(&store_id).unwrap().lane_mut();
            repo_state.dirty = true;
            repo_state.sync_in_progress = true;
        }

        let outcome = SyncOutcome {
            last_seen_stamp: synced_state.max_write_stamp(),
            state: synced_state,
            divergence: None,
            force_push: None,
        };
        daemon.complete_sync(session_token(&daemon, store_id), &remote, Ok(outcome));

        let core_state = daemon
            .store_sessions
            .get(&store_id)
            .expect("store session")
            .runtime()
            .state
            .get(&NamespaceId::core())
            .expect("core state");
        assert_eq!(core_state.live_count(), 1);

        let id = BeadId::parse("bd-abc").unwrap();
        let merged = core_state.get_live(&id).unwrap();
        assert_eq!(merged.core.created(), &loser_created);

        let repo_state = daemon.store_sessions.get(&store_id).unwrap().lane();
        assert!(repo_state.dirty);
        assert!(!repo_state.sync_in_progress);
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
        let frame_len = encode_frame(&record1, limits.policy().max_wal_record_bytes())
            .unwrap()
            .len() as u64;
        limits.wal_segment_max_bytes = (header_len + frame_len + 1) as usize;

        let mut daemon = Daemon::new_with_limits(test_actor(), limits.clone());
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).unwrap();

        let event2 = verified_event_for_seq(store, &namespace, origin, 2, Some(event1.sha256));
        let batch = ContiguousBatch::try_new(vec![event1, event2]).expect("contiguous batch");
        daemon
            .ingest_remote_batch(session_token(&daemon, store_id), batch, now_ms)
            .expect("ingest");

        let store_runtime = daemon
            .store_sessions
            .get(&store_id)
            .expect("store runtime")
            .runtime();
        let segments = store_runtime
            .wal_index
            .reader()
            .list_segments(&namespace)
            .expect("segments");
        assert_eq!(segments.len(), 2);

        let sealed = segments.iter().find(|row| row.is_sealed()).expect("sealed");
        assert!(segments.iter().any(|row| !row.is_sealed()));
        let sealed_path = paths::store_dir(store_id).join(sealed.segment_path());
        let sealed_len = std::fs::metadata(&sealed_path)
            .expect("sealed segment metadata")
            .len();
        assert_eq!(sealed.final_len(), Some(sealed_len));
    }

    #[test]
    fn repl_ingest_atomic_commit_failpoint_rolls_back_event_and_watermark() {
        let tmp = TempStoreDir::new();
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([31u8; 16]));
        let store_id = StoreId::new(Uuid::from_bytes([32u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::ZERO);
        let now_ms = 1_700_000_000_000u64;
        let bead_id = BeadId::parse("bd-repl-failpoint").expect("bead id");
        let mut patch = WireBeadPatch::new(bead_id.clone());
        patch.title = Some("repl failpoint".to_string());
        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
            .expect("bead upsert op");
        let event = verified_event_with_delta(store, &namespace, origin, 1, None, delta);
        let canonical_sha = hash_event_body(&event.bytes).0;

        let mut daemon = Daemon::new_with_limits(test_actor(), Limits::default());
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).expect("create repo path");
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).expect("insert store");

        let failpoint = crate::runtime::test_hooks::set_atomic_commit_fail_stage_for_tests(
            "wal_repl_ingest_before_atomic_commit",
        );
        let failed_batch = ContiguousBatch::try_new(vec![event.clone()]).expect("contiguous batch");
        let err = daemon
            .ingest_remote_batch(session_token(&daemon, store_id), failed_batch, now_ms)
            .expect_err("failpoint should abort ingest");
        drop(failpoint);
        assert!(!err.message.is_empty(), "expected indexed error payload");

        let event_id = event_id_for(origin, namespace.clone(), Seq1::from_u64(1).expect("seq"));
        let store_runtime = daemon
            .store_sessions
            .get(&store_id)
            .expect("store runtime")
            .runtime();
        let reader = store_runtime.wal_index.reader();
        assert_eq!(
            reader
                .lookup_event_sha(&namespace, &event_id)
                .expect("lookup event sha"),
            None
        );
        let watermark = reader
            .load_watermarks()
            .expect("load watermarks")
            .into_iter()
            .find(|row| row.namespace == namespace && row.origin == origin);
        assert!(
            watermark.is_none(),
            "watermark must not advance on rollback"
        );
        assert_eq!(
            reader
                .max_origin_seq(&namespace, &origin)
                .expect("max origin seq")
                .get(),
            0
        );
        let failed_state = store_runtime
            .state
            .get(&namespace)
            .expect("namespace state");
        assert!(
            failed_state.get_live(&bead_id).is_none(),
            "state mutation must not survive failed durability commit"
        );

        let retry_batch = ContiguousBatch::try_new(vec![event]).expect("contiguous batch");
        daemon
            .ingest_remote_batch(session_token(&daemon, store_id), retry_batch, now_ms)
            .expect("retry ingest should commit");

        let store_runtime = daemon
            .store_sessions
            .get(&store_id)
            .expect("store runtime")
            .runtime();
        let reader = store_runtime.wal_index.reader();
        let indexed_sha = reader
            .lookup_event_sha(&namespace, &event_id)
            .expect("lookup event sha")
            .expect("event row");
        assert_eq!(indexed_sha, canonical_sha);
        let watermark = reader
            .load_watermarks()
            .expect("load watermarks")
            .into_iter()
            .find(|row| row.namespace == namespace && row.origin == origin)
            .expect("watermark row");
        assert_eq!(watermark.applied_seq(), 1);
        assert_eq!(watermark.durable_seq(), 1);
        assert_eq!(watermark.applied_head_sha(), Some(canonical_sha));
        assert_eq!(watermark.durable_head_sha(), Some(canonical_sha));
        assert_eq!(
            reader
                .max_origin_seq(&namespace, &origin)
                .expect("max origin seq")
                .get(),
            1
        );
        let committed_state = store_runtime
            .state
            .get(&namespace)
            .expect("namespace state");
        assert!(committed_state.get_live(&bead_id).is_some());
    }

    #[test]
    fn repl_ingest_apply_failure_does_not_commit_event_or_watermark() {
        let tmp = TempStoreDir::new();
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([33u8; 16]));
        let store_id = StoreId::new(Uuid::from_bytes([34u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::ZERO);
        let now_ms = 1_700_000_000_000u64;

        let bead_a = BeadId::parse("bd-cycle-a").expect("bead a");
        let bead_b = BeadId::parse("bd-cycle-b").expect("bead b");

        let mut patch_a = WireBeadPatch::new(bead_a.clone());
        patch_a.title = Some("a".to_string());
        let mut patch_b = WireBeadPatch::new(bead_b.clone());
        patch_b.title = Some("b".to_string());

        let mut seed_delta = TxnDeltaV1::new();
        seed_delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch_a)))
            .expect("seed patch a");
        seed_delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch_b)))
            .expect("seed patch b");
        let seed_event = verified_event_with_delta(store, &namespace, origin, 1, None, seed_delta);

        let mut dep_ab_delta = TxnDeltaV1::new();
        dep_ab_delta
            .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                key: DepKey::new(bead_a.clone(), bead_b.clone(), DepKind::Blocks)
                    .expect("dep key a->b"),
                dot: WireDotV1 {
                    replica: origin,
                    counter: 1,
                },
            }))
            .expect("dep add a->b");
        let dep_ab_event = verified_event_with_delta(
            store,
            &namespace,
            origin,
            2,
            Some(seed_event.sha256),
            dep_ab_delta,
        );

        let mut daemon = Daemon::new_with_limits(test_actor(), Limits::default());
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).expect("create repo path");
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).expect("insert store");

        let seed_batch =
            ContiguousBatch::try_new(vec![seed_event, dep_ab_event.clone()]).expect("seed batch");
        daemon
            .ingest_remote_batch(session_token(&daemon, store_id), seed_batch, now_ms)
            .expect("seed ingest should succeed");

        let mut dep_ba_delta = TxnDeltaV1::new();
        dep_ba_delta
            .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                key: DepKey::new(bead_b.clone(), bead_a.clone(), DepKind::Blocks)
                    .expect("dep key b->a"),
                dot: WireDotV1 {
                    replica: origin,
                    counter: 2,
                },
            }))
            .expect("dep add b->a");
        let failing_event = verified_event_with_delta(
            store,
            &namespace,
            origin,
            3,
            Some(dep_ab_event.sha256),
            dep_ba_delta,
        );
        let failing_batch = ContiguousBatch::try_new(vec![failing_event]).expect("failing batch");
        let err = daemon
            .ingest_remote_batch(session_token(&daemon, store_id), failing_batch, now_ms)
            .expect_err("cycle should fail apply");
        assert!(
            !err.message.is_empty(),
            "apply failure should return structured error payload"
        );

        let store_runtime = daemon
            .store_sessions
            .get(&store_id)
            .expect("store runtime")
            .runtime();
        let reader = store_runtime.wal_index.reader();
        let failed_event_id = event_id_for(
            origin,
            namespace.clone(),
            Seq1::from_u64(3).expect("failed origin seq"),
        );
        assert_eq!(
            reader
                .lookup_event_sha(&namespace, &failed_event_id)
                .expect("lookup failed event"),
            None
        );
        assert_eq!(
            reader
                .max_origin_seq(&namespace, &origin)
                .expect("max origin seq")
                .get(),
            2
        );
        let watermark = reader
            .load_watermarks()
            .expect("load watermarks")
            .into_iter()
            .find(|row| row.namespace == namespace && row.origin == origin)
            .expect("watermark row");
        assert_eq!(watermark.applied_seq(), 2);
        assert_eq!(watermark.durable_seq(), 2);

        let state = store_runtime
            .state
            .get(&namespace)
            .expect("namespace state");
        assert!(state.dep_contains(
            &DepKey::new(bead_a.clone(), bead_b.clone(), DepKind::Blocks).expect("dep key a->b")
        ));
        assert!(!state.dep_contains(
            &DepKey::new(bead_b.clone(), bead_a.clone(), DepKind::Blocks).expect("dep key b->a")
        ));
    }

    #[test]
    fn ingest_uses_canonical_sha_for_index_and_watermarks() {
        let tmp = TempStoreDir::new();
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([21u8; 16]));
        let store_id = StoreId::new(Uuid::from_bytes([22u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::ZERO);
        let now_ms = 1_700_000_000_000u64;

        let mut event = verified_event_for_seq(store, &namespace, origin, 1, None);
        let canonical_sha = hash_event_body(&event.bytes);
        let mut wrong_sha = canonical_sha.0;
        wrong_sha[0] ^= 0xFF;
        event.sha256 = Sha256(wrong_sha);

        let mut daemon = Daemon::new_with_limits(test_actor(), Limits::default());
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).unwrap();

        let batch = ContiguousBatch::try_new(vec![event]).expect("contiguous batch");
        daemon
            .ingest_remote_batch(session_token(&daemon, store_id), batch, now_ms)
            .expect("ingest");

        let store_runtime = daemon
            .store_sessions
            .get(&store_id)
            .expect("store runtime")
            .runtime();
        let event_id = event_id_for(origin, namespace.clone(), Seq1::from_u64(1).unwrap());
        let indexed_sha = store_runtime
            .wal_index
            .reader()
            .lookup_event_sha(&namespace, &event_id)
            .expect("lookup event sha")
            .expect("event sha");
        assert_eq!(indexed_sha, canonical_sha.0);
        assert_eq!(
            store_runtime.applied_head_sha(&namespace, &origin),
            Some(canonical_sha.0)
        );
        assert_eq!(
            store_runtime.durable_head_sha(&namespace, &origin),
            Some(canonical_sha.0)
        );
    }

    #[test]
    fn ingest_accepts_orphan_note_after_append() {
        let tmp = TempStoreDir::new();
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([10u8; 16]));
        let store_id = StoreId::new(Uuid::from_bytes([11u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::ZERO);
        let now_ms = 1_700_000_000_000u64;

        let mut delta = TxnDeltaV1::new();
        let note_id = NoteId::new("note-1").unwrap();
        let note = WireNoteV1 {
            id: note_id.clone(),
            content: "hi".to_string(),
            author: ActorId::new("alice").unwrap(),
            at: WireStamp(now_ms, 0),
        };
        delta
            .insert(TxnOpV1::NoteAppend(NoteAppendV1 {
                bead_id: BeadId::parse("bd-missing").unwrap(),
                note,
                lineage: None,
            }))
            .unwrap();

        let event = verified_event_with_delta(store, &namespace, origin, 1, None, delta);

        let mut daemon = Daemon::new_with_limits(test_actor(), Limits::default());
        let remote = test_remote();
        let repo_path = tmp.data_dir().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).unwrap();

        let batch = ContiguousBatch::try_new(vec![event]).expect("contiguous batch");
        let _outcome = daemon
            .ingest_remote_batch(session_token(&daemon, store_id), batch, now_ms)
            .expect("ingest should accept orphan note");

        let store_runtime = daemon
            .store_sessions
            .get(&store_id)
            .expect("store runtime")
            .runtime();
        let segments = store_runtime
            .wal_index
            .reader()
            .list_segments(&namespace)
            .expect("segments");
        assert_eq!(segments.len(), 1);
        let state = store_runtime
            .state
            .get(&namespace)
            .expect("namespace state");
        assert!(
            state.note_id_exists(&BeadId::parse("bd-missing").unwrap(), &note_id),
            "orphan note stored"
        );
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
