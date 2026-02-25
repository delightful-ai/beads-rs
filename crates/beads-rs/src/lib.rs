#![forbid(unsafe_code)]
#![allow(clippy::result_large_err)]

// Re-export enum_str! macro from beads-macros for internal use and downstream consumers
pub use beads_macros::enum_str;

// Re-export beads-core as core module for backwards compatibility
pub use beads_core as core;

pub use beads_api as api;
// Optional direct access to surface types.
pub use beads_surface as surface;
#[cfg(feature = "cli")]
pub mod cli;
pub mod compat;
pub mod config;
pub mod daemon;
pub mod error;
pub mod git;
#[cfg(feature = "model-testing")]
pub mod model;
pub mod paths;
pub mod repo;
pub(crate) mod store_admin;
pub mod telemetry;
#[cfg(feature = "test-harness")]
pub mod test_harness;
pub mod upgrade;

pub use error::{Effect, Error, OpError, Transience};
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(feature = "cli")]
/// Thin orchestration shim for the `bd` binary.
///
/// Entry-point binaries should stay as minimal wiring while command behavior
/// lives behind crate boundaries.
pub fn run_cli_entrypoint(cli: cli::Cli) -> i32 {
    let is_daemon = matches!(
        cli.command,
        cli::Command::Daemon {
            cmd: cli::DaemonCmd::Run
        }
    );
    let _telemetry_guard = init_cli_tracing(cli.verbose, is_daemon);

    let command = cli::command_name(&cli.command);
    let span = tracing::info_span!(
        "cli_command",
        command = %command,
        repo = ?cli.repo
    );
    let _guard = span.enter();

    if let Err(err) = cli::run(cli) {
        tracing::error!("error: {}", err);
        return 1;
    }

    0
}

#[cfg(feature = "cli")]
fn init_cli_tracing(verbose: u8, is_daemon: bool) -> telemetry::TelemetryGuard {
    let cfg = match config::load() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("config load failed, using defaults: {err}");
            let mut cfg = config::Config::default();
            config::apply_env_overrides(&mut cfg);
            cfg
        }
    };

    // Initialize path overrides from config before any IPC/daemon operations.
    paths::init_from_config(&cfg.paths);

    let mut logging = cfg.logging;
    if is_daemon {
        telemetry::apply_daemon_logging_defaults(&mut logging);
    }
    let telemetry_cfg = telemetry::TelemetryConfig::new(verbose, logging);
    telemetry::init(telemetry_cfg)
}

/// Stable wrapper for daemon-run entrypoint so CLI code doesn't import daemon internals directly.
pub fn run_daemon_command() -> Result<()> {
    let config = config::load_or_init();
    paths::init_from_config(&config.paths);
    let _socket_dir = daemon::ipc::ensure_socket_dir()?;

    let actor = daemon_actor_from_config(&config)?;
    let layout = daemon_layout_from_paths();
    let runtime = daemon_runtime_config_from_config(&config);
    Ok(daemon::run_daemon(actor, layout, runtime)?)
}

pub(crate) fn daemon_layout_from_paths() -> beads_daemon::layout::DaemonLayout {
    beads_daemon::layout::DaemonLayout::new(
        paths::data_dir(),
        daemon::ipc::socket_path(),
        paths::log_dir(),
    )
}

pub(crate) fn daemon_runtime_config_from_config(
    config: &config::Config,
) -> beads_daemon::config::DaemonRuntimeConfig {
    beads_daemon::config::DaemonRuntimeConfig {
        limits: config.limits.clone(),
        namespace_defaults: config.namespace_defaults.namespaces.clone(),
        checkpoint_groups: config
            .checkpoint_groups
            .iter()
            .map(|(name, group)| {
                (
                    name.clone(),
                    beads_daemon::config::CheckpointGroupConfig {
                        namespaces: group.namespaces.clone(),
                        git_ref: group.git_ref.clone(),
                        checkpoint_writers: group.checkpoint_writers.clone(),
                        primary_writer: group.primary_writer,
                        debounce_ms: group.debounce_ms,
                        max_interval_ms: group.max_interval_ms,
                        max_events: group.max_events,
                        durable_copy_via_git: group.durable_copy_via_git,
                    },
                )
            })
            .collect(),
        replication: beads_daemon::config::ReplicationConfig {
            listen_addr: config.replication.listen_addr.clone(),
            max_connections: config.replication.max_connections,
            peers: config
                .replication
                .peers
                .iter()
                .map(|peer| beads_daemon::config::ReplicationPeerConfig {
                    replica_id: peer.replica_id,
                    addr: peer.addr.clone(),
                    role: peer.role,
                    allowed_namespaces: peer.allowed_namespaces.clone(),
                })
                .collect(),
            backoff_base_ms: config.replication.backoff_base_ms,
            backoff_max_ms: config.replication.backoff_max_ms,
        },
        git_sync_policy: beads_daemon::config::GitSyncPolicy::from_env(),
        checkpoint_policy: beads_daemon::config::CheckpointPolicy::from_env(),
    }
}

fn daemon_actor_from_config(config: &config::Config) -> Result<ActorId> {
    match config.defaults.actor.clone() {
        Some(actor) => Ok(actor),
        None => {
            let username = whoami::username();
            let hostname = whoami::fallible::hostname().unwrap_or_else(|_| "unknown".into());
            let default_actor = format!("{username}@{hostname}");
            Ok(ActorId::new(default_actor)?)
        }
    }
}

// Re-export core types at crate root for convenience
pub use crate::core::{
    ActorId, AcyclicDepKey, Applied, ApplyError, ApplyOutcome, Bead, BeadCore, BeadFields, BeadId,
    BeadPatchWireV1, BeadSnapshotWireV1, BeadType, CanonJsonError, Canonical, CanonicalState,
    CheckpointContentSha256, CheckpointGroup, Claim, CliErrorCode, ClientRequestId, Closure,
    DecodeError, DepAddKey, DepKey, DepKind, DurabilityClass, DurabilityOutcome, DurabilityProofV1,
    DurabilityReceipt, Durable, EncodeError, ErrorCode, ErrorPayload, EventBody, EventBytes,
    EventFrameError, EventFrameV1, EventId, EventKindV1, EventShaLookup, EventShaLookupError,
    EventValidationError, FreeDepKey, GcAuthority, HeadStatus, HlcMax, Labels, Limits,
    LocalFsyncProof, Lww, NamespaceId, NamespacePolicies, NamespacePoliciesError, NamespacePolicy,
    NamespaceVisibility, NoCycleProof, Note, NoteId, NoteKey, Opaque, ParentEdge, PrevDeferred,
    PrevVerified, Priority, ProtocolErrorCode, ReceiptMergeError, ReplicaDurabilityRole,
    ReplicaDurabilityRoleError, ReplicaEntry, ReplicaId, ReplicaRole, ReplicaRoster,
    ReplicaRosterError, ReplicateMode, ReplicatedProof, RetentionPolicy, SegmentId, Seq0, Seq1,
    Sha256, Stamp, StateCanonicalJsonSha256, StateDigest, StateJsonlSha256, StoreEpoch, StoreId,
    StoreIdentity, StoreMeta, StoreMetaVersions, StoreState, Tombstone, TraceId, TtlBasis,
    TxnDeltaError, TxnDeltaV1, TxnId, TxnOpKey, TxnOpV1, TxnV1, ValidatedActorId, ValidatedBeadId,
    ValidatedBeadPatch, ValidatedDepAdd, ValidatedDepKind, ValidatedDepRemove, ValidatedEventBody,
    ValidatedEventKindV1, ValidatedNamespaceId, ValidatedParentAdd, ValidatedParentRemove,
    ValidatedTombstone, ValidatedTxnDeltaV1, ValidatedTxnOpV1, ValidatedTxnV1, VerifiedEvent,
    VerifiedEventAny, WallClock, Watermark, WatermarkError, Watermarks, WireBeadFull,
    WireBeadPatch, WireDepAddV1, WireDepRemoveV1, WireDotV1, WireDvvV1, WireFieldStamp,
    WireLabelAddV1, WireLabelRemoveV1, WireLabelStateV1, WireNoteV1, WireParentAddV1,
    WireParentRemoveV1, WirePatch, WireStamp, WireTombstoneV1, Workflow, WorkflowStatus,
    WriteStamp, apply_event, decode_event_body, decode_event_hlc_max, encode_event_body_canonical,
    hash_event_body, sha256_bytes, to_canon_json_bytes, verify_event_frame,
};
