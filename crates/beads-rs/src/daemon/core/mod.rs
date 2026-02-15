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

use helpers::*;
use housekeeping::ExportPending;

#[cfg(any(test, feature = "test-harness"))]
pub(crate) use helpers::insert_store_for_tests;
pub(crate) use helpers::{detect_clock_skew, max_write_stamp, replay_event_wal};
pub use loaded_store::LoadedStore;
pub(crate) use read::{ReadGateStatus, ReadScope};

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
use super::store::runtime::{StoreRuntime, StoreRuntimeError, load_replica_roster};
use super::store::{ResolvedStore, StoreCaches};
use super::wal::{
    EventWalError, FrameReader, HlcRow, RecordHeader, RequestProof, SegmentRow, VerifiedRecord,
    WalAppend, WalIndex, WalIndexError, WalIndexTxnProvider, WalReplayError, open_segment_reader,
};
use beads_daemon::broadcast::BroadcastEvent;
use beads_daemon::git_lane::{
    ClockSkewRecord, DivergenceRecord, FetchErrorRecord, ForcePushRecord, GitLaneState,
};
use beads_daemon::remote::RemoteUrl;
use beads_daemon::scheduler::SyncScheduler;

use crate::compat::ExportContext;
use crate::config::CheckpointGroupConfig as ConfigCheckpointGroup;
use crate::core::error::details as error_details;
use crate::paths;

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
    CheckpointCache, CheckpointImport, CheckpointImportError, IncludedHeads, import_checkpoint,
    merge_store_states, policy_hash, roster_hash, store_state_from_legacy,
};

const LOAD_TIMEOUT_SECS: u64 = 30;
const DEFAULT_REPL_MAX_CONNECTIONS: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GitSyncPolicy {
    Enabled,
    Disabled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointPolicy {
    Enabled,
    Disabled,
}

impl GitSyncPolicy {
    fn from_env() -> Self {
        if env_flag_truthy("BD_TEST_DISABLE_GIT_SYNC") {
            Self::Disabled
        } else {
            Self::Enabled
        }
    }

    pub(crate) fn allows_sync(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

impl CheckpointPolicy {
    fn from_env() -> Self {
        if env_flag_truthy("BD_TEST_DISABLE_CHECKPOINTS") {
            Self::Disabled
        } else {
            Self::Enabled
        }
    }

    pub(crate) fn allows_checkpoints(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

fn env_flag_truthy(name: &str) -> bool {
    let Ok(raw) = std::env::var(name) else {
        return false;
    };
    !matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "no" | "n" | "off"
    )
}

#[derive(Clone, Debug)]
pub(crate) struct ParsedMutationMeta {
    pub(crate) namespace: NamespaceId,
    pub(crate) durability: DurabilityClass,
    pub(crate) client_request_id: Option<ClientRequestId>,
    pub(crate) trace_id: TraceId,
    pub(crate) actor_id: ActorId,
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
        let git_sync_policy = GitSyncPolicy::from_env();
        let checkpoint_policy = CheckpointPolicy::from_env();
        // Initialize Go-compat export worker (best effort - don't fail daemon startup)
        let export_worker = match ExportContext::new() {
            Ok(ctx) => Some(ExportWorkerHandle::start(ctx)),
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
            export_worker,
            export_pending: BTreeMap::new(),
            limits,
            git_sync_policy,
            checkpoint_policy,
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

        for store in self.stores.values_mut() {
            store.reload_limits(&limits);
        }

        let store_ids: Vec<StoreId> = self.stores.keys().copied().collect();
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

    pub(in crate::daemon) fn fire_due_syncs(&mut self, git_tx: &Sender<GitOp>) {
        let due = self.scheduler.drain_due(Instant::now());
        for remote in due {
            self.maybe_start_sync(&remote, git_tx);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::ipc::{
        CreatePayload, MutationCtx, MutationMeta, ReadConsistency, Request, ResponsePayload,
    };
    use crate::daemon::store::discovery::store_id_from_remote;
    use crate::git::sync::SyncOutcome;
    use beads_api::QueryResult;
    use std::collections::BTreeMap;
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use git2::Repository;
    use tracing::{Dispatch, Level};
    use tracing_subscriber::fmt::MakeWriter;
    use uuid::Uuid;

    use crate::core::{
        ActorId, Applied, Bead, BeadCore, BeadFields, BeadId, BeadSlug, BeadType, CanonicalState,
        Claim, ContentHash, Durable, EventBody, EventKindV1, HeadStatus, HlcMax, Limits, Lww,
        NamespaceId, NamespacePolicy, NoteAppendV1, NoteId, PrevVerified, Priority,
        ReplicaDurabilityRole, ReplicaEntry, ReplicaId, ReplicaRoster, SegmentId, Seq0, Seq1,
        Sha256, Stamp, StoreEpoch, StoreId, StoreIdentity, StoreMeta, StoreMetaVersions,
        TxnDeltaV1, TxnId, TxnOpV1, TxnV1, VerifiedEvent, WallClock, Watermarks, WireBeadPatch,
        WireNoteV1, WireStamp, Workflow, WriteStamp, encode_event_body_canonical, hash_event_body,
    };
    use crate::daemon::OpResult;
    use crate::daemon::git_worker::LoadResult;
    use crate::daemon::store::lock::read_lock_meta;
    use crate::daemon::store::runtime::StoreRuntime;
    use crate::daemon::wal::frame::encode_frame;
    use crate::daemon::wal::{HlcRow, RecordHeader, RequestProof, SegmentHeader, VerifiedRecord};
    use crate::git::checkpoint::{
        CHECKPOINT_FORMAT_VERSION, CheckpointExportInput, CheckpointImport,
        CheckpointSnapshotInput, CheckpointStoreMeta, IncludedWatermarks, build_snapshot,
        export_checkpoint, policy_hash, publish_checkpoint, store_state_from_legacy,
    };
    use tempfile::TempDir;

    fn test_actor() -> ActorId {
        ActorId::new("test@host".to_string()).unwrap()
    }

    fn test_remote() -> RemoteUrl {
        RemoteUrl::new("example.com/test/repo")
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
        daemon.store_caches.remote_to_store.insert(
            remote.clone(),
            StoreIdResolution::unverified(store_id, StoreIdSource::RemoteFallback),
        );
        daemon.stores.insert(store_id, runtime);
        daemon.git_lanes.insert(store_id, GitLaneState::new());
        store_id
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
                role: ReplicaDurabilityRole::anchor(true),
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
            .stores
            .get(&store_id)
            .expect("store runtime")
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
            root_slug: Some(BeadSlug::parse("fresh-slug").unwrap()),
            needs_sync: false,
            last_seen_stamp: None,
            fetch_error: None,
            divergence: None,
            force_push: None,
        });
        daemon.complete_refresh(&remote, result);

        let repo_state = daemon.git_lanes.get(&store_id).unwrap();
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
        daemon.git_lanes.insert(store_id, repo_state);

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
        daemon.complete_refresh(&remote, result);

        let repo_state = daemon.git_lanes.get(&store_id).unwrap();
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
        let unknown = RemoteUrl::new("unknown.com/repo");

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
        let batch = ContiguousBatch::try_new(vec![event]).expect("contiguous batch");
        daemon.handle_repl_ingest(ReplIngestRequest {
            store_id,
            batch,
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
            let store = daemon.stores.get_mut(&store_id).unwrap();
            store.state.set_core_state(local_state);
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
        assert_eq!(core_state.live_count(), 1);

        let id = BeadId::parse("bd-abc").unwrap();
        let merged = core_state.get_live(&id).unwrap();
        assert_eq!(merged.core.created(), &loser_created);

        let repo_state = daemon.git_lanes.get(&store_id).unwrap();
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
            .ingest_remote_batch(store_id, batch, now_ms)
            .expect("ingest");

        let store_runtime = daemon.stores.get(&store_id).expect("store runtime");
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
            .ingest_remote_batch(store_id, batch, now_ms)
            .expect("ingest");

        let store_runtime = daemon.stores.get(&store_id).expect("store runtime");
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
            .ingest_remote_batch(store_id, batch, now_ms)
            .expect("ingest should accept orphan note");

        let store_runtime = daemon.stores.get(&store_id).expect("store runtime");
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
