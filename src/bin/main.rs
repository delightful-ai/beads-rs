use beads_rs::cli;
use tracing::metadata::LevelFilter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Registry};

fn main() {
    let cli = cli::parse_from(std::env::args_os());

    // Set actor env var before anything else (unsafe in Rust 2024 due to data races,
    // but CLI is single-threaded at this point)
    if let Some(actor) = &cli.actor {
        // SAFETY: CLI is single-threaded at this point, no concurrent env access
        unsafe { std::env::set_var("BD_ACTOR", actor) };
    }

    init_tracing(cli.verbose);

    if let Err(e) = cli::run(cli) {
        tracing::error!("error: {}", e);
        std::process::exit(1);
    }
}

fn init_tracing(verbose: u8) {
    let level = match verbose {
        0 => LevelFilter::ERROR,
        1 => LevelFilter::INFO,
        _ => LevelFilter::DEBUG,
    };
    Registry::default()
        .with(tracing_tree::HierarchicalLayer::new(2))
        .with(
            EnvFilter::builder()
                .with_default_directive(level.into())
                .with_env_var("LOG")
                .from_env_lossy(),
        )
        .init();
}
