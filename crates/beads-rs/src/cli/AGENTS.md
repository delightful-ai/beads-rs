## Boundary
This directory is the thin host adapter over `beads-cli`.
It owns runtime construction from local repo/config/environment and the host-only backend operations that need package/git/IPC ownership.
NEVER: recreate `src/cli/commands/**`, top-level parse/dispatch, or user-facing rendering here.

## Ownership
- Top-level CLI parse/dispatch and command mapping live in `crates/beads-cli/src/cli.rs`.
- Command handlers and rendering live in `crates/beads-cli/src/commands/**`.
- `crates/beads-rs/src/cli/mod.rs` implements `CliHost`: repo discovery, config/default resolution, actor validation, and daemon/upgrade hooks passed into `beads-cli`.
- `crates/beads-rs/src/cli/backend.rs` implements `CliHostBackend`: upgrade, store-admin, migration detect/apply, and other operations that need direct access to package/git/IPC seams.
- Upgrade/package behavior that needs the package seam stays in `beads-rs`, not in `beads-cli`. That surface was moved out of `beads-cli` on purpose.
- Copy host-hook patterns from `mod.rs` and `backend.rs`; do not push repo/config/bootstrap ownership down into `beads-cli`, and do not let canonical bootstrap logic regrow here either.

## Verification
- Start with `cargo check -p beads-rs`.
- Run `cargo run --bin bd -- --help` when you change parse/bootstrap wiring through this seam.
- Append `cargo xtest` when the change affects shipped CLI behavior, IPC calls, or migration/admin flows.
