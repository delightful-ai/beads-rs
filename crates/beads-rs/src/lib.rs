#![forbid(unsafe_code)]
#![allow(clippy::result_large_err)]

// Re-export enum_str! macro from beads-macros for internal use and downstream consumers
pub use beads_macros::enum_str;

#[cfg(feature = "cli")]
pub mod cli;
pub mod config;
pub mod error;
#[cfg(feature = "model-testing")]
pub mod model;
pub mod paths;
pub mod repo;
pub(crate) mod store_admin;
pub mod telemetry;
#[cfg(feature = "test-harness")]
pub mod test_harness;
pub mod upgrade;

pub use beads_daemon::compat;
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
    let _socket_dir = beads_surface::ipc::ensure_socket_dir()?;

    let actor = daemon_actor_from_config(&config)?;
    let layout = daemon_layout_from_paths();
    let runtime = daemon_runtime_config_from_config(&config);
    Ok(beads_daemon::run_daemon(actor, layout, runtime)?)
}

pub(crate) fn daemon_layout_from_paths() -> beads_daemon::layout::DaemonLayout {
    beads_daemon::layout::DaemonLayout::new(
        paths::data_dir(),
        beads_surface::ipc::socket_path(),
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

fn daemon_actor_from_config(config: &config::Config) -> Result<beads_core::ActorId> {
    match config.defaults.actor.clone() {
        Some(actor) => Ok(actor),
        None => {
            let username = whoami::username();
            let hostname = whoami::fallible::hostname().unwrap_or_else(|_| "unknown".into());
            let default_actor = format!("{username}@{hostname}");
            Ok(beads_core::ActorId::new(default_actor)?)
        }
    }
}
