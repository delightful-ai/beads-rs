## Boundary
This directory contains binary entrypoints.
Depends on: `beads_rs` crate APIs and tracing setup.
Depended on by: end users running `bd`.
NEVER: add business logic here; keep it as wiring (parse args, init tracing, call `cli::run`).

## How to work here
- Logging defaults are set in `src/bin/main.rs`; CLI verbosity toggles should remain the source of truth.

## Verification
- `cargo run --bin bd -- --help`
