use beads_rs::{cli, config, telemetry};

fn main() {
    let cli = cli::parse_from(std::env::args_os());

    // Set actor env var before anything else (unsafe in Rust 2024 due to data races,
    // but CLI is single-threaded at this point)
    if let Some(actor) = &cli.actor {
        // SAFETY: CLI is single-threaded at this point, no concurrent env access
        unsafe { std::env::set_var("BD_ACTOR", actor) };
    }

    let is_daemon = matches!(
        cli.command,
        cli::Commands::Daemon {
            cmd: cli::DaemonCmd::Run
        }
    );
    let _telemetry_guard = init_tracing(cli.verbose, is_daemon);

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
    let mut logging = cfg.logging;
    if is_daemon {
        telemetry::apply_daemon_logging_defaults(&mut logging);
    }
    let telemetry_cfg = telemetry::TelemetryConfig::new(verbose, logging);
    telemetry::init(telemetry_cfg)
}
