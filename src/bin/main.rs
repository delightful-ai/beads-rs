use beads_rs::cli;
use tracing::metadata::LevelFilter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Registry};

fn main() {
    let cli = cli::parse_from(std::env::args_os());
    init_tracing(cli.verbose);

    if let Err(e) = cli::run(cli) {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}

fn init_tracing(verbose: u8) {
    let level = match verbose {
        0 => LevelFilter::OFF,
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
