use beads_rs::cli;

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

    let exit_code = beads_rs::run_cli_entrypoint(cli);
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}
