use beads_rs::{cli, config, telemetry};

fn main() {
    let cli = cli::parse_from(std::env::args_os());

    // Set actor env var before anything else (unsafe in Rust 2024 due to data races,
    // but CLI is single-threaded at this point)
    if let Some(actor) = &cli.actor {
        // SAFETY: CLI is single-threaded at this point, no concurrent env access
        #[allow(unsafe_code)]
        unsafe {
            std::env::set_var("BD_ACTOR", actor)
        };
    }

    let is_daemon = matches!(
        cli.command,
        cli::Command::Daemon {
            cmd: cli::DaemonCmd::Run
        }
    );
    let _telemetry_guard = init_tracing(cli.verbose, is_daemon);

    let command = cli::command_name(&cli.command);
    let span = tracing::info_span!(
        "cli_command",
        command = %command,
        repo = ?cli.repo
    );
    let _guard = span.enter();

    if let Err(e) = cli::run(cli) {
        tracing::error!("error: {}", e);
        std::process::exit(1);
    }
}

fn init_tracing(verbose: u8, is_daemon: bool) -> telemetry::TelemetryGuard {
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
    beads_rs::paths::init_from_config(&cfg.paths);

    let mut logging = cfg.logging;
    if is_daemon {
        telemetry::apply_daemon_logging_defaults(&mut logging);
    }
    let telemetry_cfg = telemetry::TelemetryConfig::new(verbose, logging);
    telemetry::init(telemetry_cfg)
}
