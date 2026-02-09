## Boundary
This directory is now a thin host adapter for the CLI.
Depends on: `beads-cli` + config/repo discovery + daemon/bootstrap hooks.
Depended on by: `src/bin/main.rs` via `beads_rs::cli`.
NEVER: reintroduce command parse/dispatch or per-command handlers here.

## Ownership
- Top-level CLI parse/dispatch and command mapping live in `crates/beads-cli/src/cli.rs`.
- Command handlers/rendering live in `crates/beads-cli/src/commands/**`.
- `crates/beads-rs/src/cli/mod.rs` only supplies host-specific runtime construction and daemon/repo hooks.
- `crates/beads-rs/src/cli/backend.rs` is the typed host backend seam implementation.

## Verification
- `cargo run --bin bd -- --help`
- `cargo test --all-features`
- `just dylint`
