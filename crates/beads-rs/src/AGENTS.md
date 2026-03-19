## Boundary
This directory is the implementation surface for the `beads-rs` assembly crate.
Current areas include the CLI host adapter in `cli/`, binary entrypoints in `bin/`, config loading in `config/`, assembly-facing model helpers in `model/`, and product glue such as `repo.rs`, `paths.rs`, `store_admin.rs`, `telemetry.rs`, `upgrade.rs`, and `error.rs`.
NEVER: turn `src/` back into the old compatibility umbrella. History already moved canonical path/config/repo logic to `beads-bootstrap`, command language to `beads-cli`, and most daemon internals out of this crate.

## Routing
- `lib.rs` is the package/public seam and the orchestration order matters there: config load, path override init, telemetry init, then daemon/CLI handoff.
- `cli/` is a host seam over `beads-cli`, not a command-language home. Copy `src/cli/{mod.rs,backend.rs}` for host-hook patterns, not for user-facing command behavior.
- `bin/` is for executable wiring only.
- `src/repo.rs`, `src/config/load.rs`, and `src/paths.rs` are thin assembly consumers of bootstrap ownership, not templates for regrowing repo/config/path policy here.
- `lib.rs` still publicly re-exports `beads_daemon::compat`. Treat that as a surviving compatibility seam for downstream callers, not as an invitation to grow `src/compat/` or new assembly-owned compatibility surfaces here.
- `upgrade.rs` and `upgrade/` remain assembly-owned because install/update behavior depends on the shipped package surface.
- `store_admin.rs` and `telemetry.rs` are host seams. Do not recreate private daemon/git shims or compat re-exports under `src/`; call the owning crate through its existing public seam.

## Verification
- Run `cargo check -p beads-rs` for ordinary assembly-glue changes in this subtree.
- If you touch public/package seams or ownership guardrails, append `cargo test -p beads-rs --test public_boundary`.
- If you touch CLI host wiring, package seams, or shipped behavior, append `cargo xtest`.
