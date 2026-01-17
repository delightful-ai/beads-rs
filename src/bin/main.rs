use beads_rs::{cli, config, telemetry};

fn main() {
    let cli = cli::parse_from(std::env::args_os());

    // Set actor env var before anything else (unsafe in Rust 2024 due to data races,
    // but CLI is single-threaded at this point)
    if let Some(actor) = &cli.actor {
        // SAFETY: CLI is single-threaded at this point, no concurrent env access
        unsafe { std::env::set_var("BD_ACTOR", actor) };
    }

    let _telemetry_guard = init_tracing(cli.verbose);

    if let Err(e) = cli::run(cli) {
        tracing::error!("error: {}", e);
        std::process::exit(1);
    }
}

fn init_tracing(verbose: u8) -> telemetry::TelemetryGuard {
    let cfg = match config::load() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("config load failed, using defaults: {err}");
            let mut cfg = config::Config::default();
            config::apply_env_overrides(&mut cfg);
            cfg
        }
    };
    let telemetry_cfg = telemetry::TelemetryConfig::new(verbose, cfg.logging);
    telemetry::init(telemetry_cfg)
}
