# CLI Ownership Closeout (`bd-atsg.5`)

Date: 2026-02-09

## Final Ownership Map

- `crates/beads-cli/src/cli.rs`
  - Owns CLI parse surface (`Cli`), command tree (`Command`), command-name mapping, and top-level dispatch.
- `crates/beads-cli/src/commands/**`
  - Owns command handlers and human/json rendering behavior.
- `crates/beads-rs/src/cli/mod.rs`
  - Thin host adapter only: runtime/context construction from repo+config, daemon entry hook delegation, and host policy wiring.
- `crates/beads-rs/src/cli/backend.rs`
  - Typed host backend seam implementation (`CliHostBackend`) for host-coupled operations.
- `crates/beads-rs/src/bin/main.rs` and `crates/beads-rs/src/lib.rs`
  - Entry/telemetry wrappers only.

## Shim Cleanup

- Removed legacy `crates/beads-rs/src/cli/commands/**` shim directory.
- This eliminates stale duplicate command ownership and leaves a single source of truth for CLI parse/dispatch in `beads-cli`.

## Parity Verification

Critical-path integration suite remains green after migration:

- `cargo test --all-features` (includes `crates/beads-rs/tests/integration/cli/critical_path.rs`)
- `just dylint`
- `cargo clippy --all-features -- -D warnings`

Slow-suite note:

- Parallel slow-suite runs intermittently fail on
  `daemon::repl_e2e::repl_daemon_crash_restart_roundtrip` with transient `gap_detected` during `bd status`.
- A stable full run passed with
  `cargo test --all-features --features slow-tests -- --test-threads=1`.
- Follow-up bug tracked as `bd-crxg`.
