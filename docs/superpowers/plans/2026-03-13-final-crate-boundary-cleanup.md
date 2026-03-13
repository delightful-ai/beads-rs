# Final Crate Boundary Cleanup Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Seal crate public boundaries end-to-end, extract shared bootstrap ownership into a dedicated crate, and leave `beads-rs` as a tiny assembly crate.

**Architecture:** First remove all external reach-throughs into `beads_daemon::runtime::*` and `beads_daemon::git::*` so the daemon crate can expose only deliberate top-level APIs. Then extract pure bootstrap concerns (config, paths, repo-root discovery) into a new `beads-bootstrap` crate, move git-owned state-loading helpers into `beads-git`, and finish by shrinking `beads-rs` and re-homing tests to their owner crates.

**Tech Stack:** Rust workspace crates, `jj`, `bd`, `clap`, `git2`, `serde`, custom dylint boundary policy, cargo test/clippy/fmt.

---

**Spec:** [2026-03-13-final-crate-boundary-cleanup-design.md](/Users/darin/src/personal/beads-rs/docs/superpowers/specs/2026-03-13-final-crate-boundary-cleanup-design.md)

## File Structure

### New crate

- Create: `crates/beads-bootstrap/Cargo.toml`
- Create: `crates/beads-bootstrap/src/lib.rs`
- Create: `crates/beads-bootstrap/src/config/mod.rs`
- Create: `crates/beads-bootstrap/src/config/load.rs`
- Create: `crates/beads-bootstrap/src/config/merge.rs`
- Create: `crates/beads-bootstrap/src/config/schema.rs`
- Create: `crates/beads-bootstrap/src/paths.rs`
- Create: `crates/beads-bootstrap/src/repo.rs`

### Daemon boundary files

- Modify: `crates/beads-daemon/src/lib.rs`
- Create or modify: `crates/beads-daemon/src/admin.rs`
- Modify: `crates/beads-daemon/src/runtime/mod.rs`
- Modify: `crates/beads-daemon/src/runtime/admin.rs`
- Modify: `crates/beads-daemon/src/runtime/run.rs`
- Modify: `crates/beads-daemon/src/config.rs`
- Modify: `crates/beads-daemon/src/paths.rs`
- Modify: `crates/beads-daemon/src/repo.rs`
- Modify: `crates/beads-daemon/src/telemetry.rs`
- Modify: `crates/beads-daemon/src/runtime/core/mod.rs`
- Modify: `crates/beads-daemon/src/runtime/core/checkpoint_scheduling.rs`
- Modify: `crates/beads-daemon/src/runtime/core/replication.rs`
- Modify: `crates/beads-daemon/src/runtime/store/runtime.rs`
- Modify: `crates/beads-daemon/src/runtime/admin/offline_store.rs`

### Assembly crate files

- Modify: `crates/beads-rs/src/lib.rs`
- Modify: `crates/beads-rs/src/cli/mod.rs`
- Modify: `crates/beads-rs/src/cli/backend.rs`
- Modify: `crates/beads-rs/src/config/mod.rs`
- Modify: `crates/beads-rs/src/config/load.rs`
- Modify: `crates/beads-rs/src/config/merge.rs`
- Modify: `crates/beads-rs/src/config/schema.rs`
- Modify: `crates/beads-rs/src/paths.rs`
- Modify: `crates/beads-rs/src/repo.rs`
- Modify: `crates/beads-rs/src/store_admin.rs`
- Modify: `crates/beads-rs/src/upgrade.rs`
- Modify: `crates/beads-rs/src/telemetry.rs`
- Modify: `crates/beads-rs/src/test_harness/mod.rs`

### CLI and git owner files

- Modify: `crates/beads-cli/Cargo.toml`
- Modify: `crates/beads-cli/src/lib.rs`
- Modify: `crates/beads-cli/src/paths.rs`
- Modify: `crates/beads-cli/src/upgrade/mod.rs`
- Modify: `crates/beads-cli/src/backend.rs`
- Modify: `crates/beads-git/src/lib.rs`
- Modify: `crates/beads-git/src/paths.rs`
- Modify: `crates/beads-git/src/sync.rs`

### Policy and verification files

- Modify: `Cargo.toml`
- Modify: `docs/CRATE_DAG.md`
- Modify: `lints/beads_lints/tests/crate_dag_policy.rs`
- Modify: `docs/architecture/non-cli-daemon-coupling-inventory.md`
- Modify: `docs/superpowers/specs/2026-03-13-final-crate-boundary-cleanup-design.md` only if design clarifications are needed during execution

### Tests likely to move or be updated

- Modify: `crates/beads-daemon/tests/**`
- Modify: `crates/beads-git/tests/**`
- Modify: `crates/beads-bootstrap/tests/**` or inline `#[cfg(test)]` modules
- Modify: `crates/beads-rs/tests/integration/**`

## Chunk 1: Seal Daemon Public Boundaries

### Task 1: Add failing guardrails for the cutover

**Files:**
- Modify: `docs/CRATE_DAG.md`
- Modify: `lints/beads_lints/tests/crate_dag_policy.rs`
- Optionally create: `crates/beads-daemon/tests/public_boundary.rs`

- [ ] Add `beads-bootstrap` to the target DAG in `docs/CRATE_DAG.md`.
- [ ] Add the future allowed edges for `beads-bootstrap` in `lints/beads_lints/tests/crate_dag_policy.rs`.
- [ ] Add explicit notes that `beads_daemon::runtime::*` and `beads_daemon::git::*` are not public boundary APIs.
- [ ] Capture the boundary grep commands in the plan or docs so they can be run as red/green checks:

```bash
rg -n "beads_daemon::runtime::" crates --glob '!**/target/**'
rg -n "beads_daemon::git::" crates --glob '!**/target/**'
```

- [ ] Run the grep commands once before code changes and record the expected current failures in the implementation notes.

### Task 2: Expose deliberate daemon-owned top-level APIs

**Files:**
- Modify: `crates/beads-daemon/src/lib.rs`
- Create or modify: `crates/beads-daemon/src/admin.rs`
- Modify: `crates/beads-daemon/src/runtime/admin.rs`

- [ ] Create a public top-level daemon admin boundary for offline store operations.
- [ ] Prefer a shape like:

```rust
pub mod admin {
    pub use crate::runtime::admin::{
        offline_store_fsck_output,
        offline_store_unlock_output,
    };
}
```

- [ ] Keep the implementation in `runtime/admin.rs`; do not move daemon logic into `lib.rs`.
- [ ] Ensure the new public module re-exports only the intentional offline admin helpers, not the whole runtime admin module.

### Task 3: Rewrite external daemon-runtime reach-throughs

**Files:**
- Modify: `crates/beads-rs/src/lib.rs`
- Modify: `crates/beads-rs/src/store_admin.rs`
- Modify: `crates/beads-rs/src/upgrade.rs`

- [ ] Replace `beads_daemon::runtime::ipc::*` usage in `beads-rs` with `beads_surface::ipc::*` where the ownership is already in `beads-surface`.
- [ ] Replace offline store admin calls with the new top-level `beads_daemon::admin::*` surface.
- [ ] Keep `run_daemon_command()` calling `beads_daemon::run_daemon()` only; do not let it reach into daemon internals.
- [ ] Remove any remaining direct references to `beads_daemon::runtime::OpError` outside the daemon crate.

- [ ] Verify the targeted files compile:

```bash
cargo check -p beads-rs -p beads-daemon -p beads-surface
```

### Task 4: Remove accidental daemon public exports

**Files:**
- Modify: `crates/beads-daemon/src/lib.rs`
- Modify: `crates/beads-daemon/src/runtime/mod.rs`
- Modify any affected import sites found by `rg`

- [ ] Remove `pub mod runtime` from `crates/beads-daemon/src/lib.rs`.
- [ ] Remove the `pub mod git { pub use beads_git::*; }` re-export from `crates/beads-daemon/src/lib.rs`.
- [ ] Keep `runtime` private and internal to the daemon crate.
- [ ] Rewrite any daemon-internal references to use `crate::runtime::*` or direct internal paths as needed.
- [ ] Rewrite any external callers to import `beads_git::*` directly.

- [ ] Run the guardrails and make them pass:

```bash
rg -n "beads_daemon::runtime::" crates --glob '!**/target/**'
rg -n "beads_daemon::git::" crates --glob '!**/target/**'
cargo check -p beads-daemon -p beads-rs
```

## Chunk 2: Extract `beads-bootstrap`

### Task 5: Scaffold the new bootstrap crate

**Files:**
- Create: `crates/beads-bootstrap/Cargo.toml`
- Create: `crates/beads-bootstrap/src/lib.rs`
- Modify: `Cargo.toml`
- Modify: `docs/CRATE_DAG.md`
- Modify: `lints/beads_lints/tests/crate_dag_policy.rs`

- [ ] Add `crates/beads-bootstrap` to the workspace members and default-members in `Cargo.toml`.
- [ ] Keep `beads-bootstrap` pure: it should depend on `beads-core` and standard library helpers only unless a stronger need appears during implementation.
- [ ] Do not let `beads-bootstrap` depend on `beads-surface`, `beads-git`, `beads-daemon`, or `beads-rs`.
- [ ] Add the crate to the DAG policy and lint test.

### Task 6: Move path resolution ownership out of `beads-cli`

**Files:**
- Create: `crates/beads-bootstrap/src/paths.rs`
- Modify: `crates/beads-cli/src/paths.rs`
- Modify: `crates/beads-rs/src/paths.rs`
- Modify: `crates/beads-daemon/src/paths.rs`
- Modify: `crates/beads-cli/Cargo.toml`

- [ ] Move the pure path helpers from `crates/beads-cli/src/paths.rs` into `crates/beads-bootstrap/src/paths.rs`.
- [ ] Move config-driven path override state and test override support from `crates/beads-rs/src/paths.rs` and `crates/beads-daemon/src/paths.rs` into `beads-bootstrap`.
- [ ] Keep `beads-bootstrap::paths` pure to bootstrap ownership. Do not have it call into `beads_surface` or `beads_git`.
- [ ] Replace `beads_cli::paths` imports in `beads-rs` with `beads_bootstrap::paths`.
- [ ] Rewrite daemon path users to consume `beads_bootstrap::paths` rather than daemon-local duplicate helpers.
- [ ] Delete `crates/beads-cli/src/paths.rs` once all call sites are migrated.

- [ ] Verify the new owner crate:

```bash
cargo test -p beads-bootstrap
cargo check -p beads-cli -p beads-rs -p beads-daemon
```

### Task 7: Move config schema/load/merge into `beads-bootstrap`

**Files:**
- Create: `crates/beads-bootstrap/src/config/mod.rs`
- Create: `crates/beads-bootstrap/src/config/load.rs`
- Create: `crates/beads-bootstrap/src/config/merge.rs`
- Create: `crates/beads-bootstrap/src/config/schema.rs`
- Modify: `crates/beads-rs/src/config/mod.rs`
- Modify: `crates/beads-rs/src/config/load.rs`
- Modify: `crates/beads-rs/src/config/merge.rs`
- Modify: `crates/beads-rs/src/config/schema.rs`
- Modify: `crates/beads-daemon/src/config.rs`
- Modify: `crates/beads-daemon/src/telemetry.rs`
- Modify: `crates/beads-rs/src/telemetry.rs`

- [ ] Move the canonical app config schema from `crates/beads-rs/src/config/**` into `beads-bootstrap`.
- [ ] Keep the following types canonical in `beads-bootstrap`:
  - `Config`
  - `ConfigLayer`
  - `DefaultsConfig`
  - `PathsConfig`
  - logging config types
  - replication/checkpoint config schema types
- [ ] Remove the duplicate top-level `Config` and `PathsConfig` definitions from `crates/beads-daemon/src/config.rs`.
- [ ] Keep runtime-only daemon policy types in `beads-daemon`, including:
  - `DaemonRuntimeConfig`
  - `GitSyncPolicy`
  - `CheckpointPolicy`
- [ ] Add a deliberate conversion boundary, for example:

```rust
pub fn daemon_runtime_config_from_bootstrap(
    config: &beads_bootstrap::config::Config,
) -> DaemonRuntimeConfig
```

- [ ] Update daemon telemetry and assembly telemetry to use the canonical bootstrap logging types.
- [ ] Reduce `crates/beads-rs/src/config/mod.rs` to either a thin re-export of `beads-bootstrap` or delete it if all local callers can import `beads_bootstrap` directly.

### Task 8: Move repo-root discovery into `beads-bootstrap`

**Files:**
- Create: `crates/beads-bootstrap/src/repo.rs`
- Modify: `crates/beads-rs/src/repo.rs`
- Modify: `crates/beads-daemon/src/repo.rs`
- Modify: `crates/beads-rs/src/cli/mod.rs`
- Modify: `crates/beads-daemon/src/config.rs`
- Modify: `crates/beads-rs/src/config/load.rs`

- [ ] Move pure repo-root discovery (`discover_root`, `discover_root_optional`, fast `.git` walk) into `beads-bootstrap`.
- [ ] Keep `beads-rs::repo` only for assembly-level helpers that truly belong to the package crate.
- [ ] Keep `git2::Repository::discover()` wrappers that open a repository only where they are actually needed by assembly or git ownership.
- [ ] Rewrite config-loading code in both `beads-rs` and `beads-daemon` to depend on bootstrap-owned repo discovery.

## Chunk 3: Re-home Git Helpers and Narrow `beads-rs`

### Task 9: Move git-owned state-loading helpers into `beads-git`

**Files:**
- Modify: `crates/beads-git/src/lib.rs`
- Modify: `crates/beads-git/src/paths.rs`
- Modify: `crates/beads-rs/src/repo.rs`
- Modify any callers found by `rg -n "load_state\\(|load_store\\(" crates`

- [ ] Move `load_state()` and `load_store()` out of `crates/beads-rs/src/repo.rs` into a `beads-git` public API.
- [ ] If necessary, add a small `beads-git` module for store-ref loading helpers rather than bloating `sync.rs`.
- [ ] Decide whether `beads-git` should continue to own its own `DATA_DIR_OVERRIDE`; if bootstrap path ownership can fully replace it without a cycle, collapse it during this step.
- [ ] Leave `beads-rs::repo` with only assembly-appropriate repository-opening helpers, or delete it entirely if bootstrap + git cover the use cases.

### Task 10: Finish shrinking `beads-rs`

**Files:**
- Modify: `crates/beads-rs/src/lib.rs`
- Modify: `crates/beads-rs/src/upgrade.rs`
- Modify: `crates/beads-rs/src/cli/mod.rs`
- Modify: `crates/beads-rs/src/cli/backend.rs`
- Modify: `crates/beads-rs/src/paths.rs`
- Modify: `crates/beads-rs/src/config/mod.rs`
- Modify: `crates/beads-rs/src/repo.rs`
- Modify: `crates/beads-rs/src/store_admin.rs`

- [ ] Make the assembly crate depend on `beads-bootstrap` directly for config/path/repo policy.
- [ ] Replace duplicated daemon runtime config mapping in `crates/beads-rs/src/lib.rs` with a deliberate daemon conversion API.
- [ ] Make internal assembly helpers private wherever possible.
- [ ] Keep public surface limited to bin-facing entrypoints and telemetry/bootstrap wiring that same-package binaries need.

### Task 11: Narrow `beads-cli` to command language and presentation

**Files:**
- Modify: `crates/beads-cli/Cargo.toml`
- Modify: `crates/beads-cli/src/lib.rs`
- Modify: `crates/beads-cli/src/upgrade/mod.rs`
- Modify: `crates/beads-rs/src/upgrade.rs`
- Modify: `crates/beads-rs/src/cli/backend.rs`

- [ ] Remove bootstrap path ownership from `beads-cli` by deleting `src/paths.rs` after Task 6.
- [ ] Move upgrade support utilities out of `beads-cli/src/upgrade/mod.rs` into `beads-rs` or another assembly-owned module, because self-upgrade is package policy, not CLI language.
- [ ] Keep `beads-cli` owning:
  - clap grammar
  - CLI-only validation
  - argv-to-request lowering
  - rendering
  - backend/host traits
- [ ] Do not widen scope to extract `migrate/go_export.rs` in this pass unless it blocks the boundary cleanup; if it still feels mis-owned after the main refactor, file a follow-up bead instead of expanding the critical path.

## Chunk 4: Tests, Docs, and Verification

### Task 12: Re-home tests to owner crates

**Files:**
- Modify: `crates/beads-rs/tests/integration/**`
- Modify: `crates/beads-rs/src/test_harness/mod.rs`
- Modify or create: `crates/beads-bootstrap/tests/**`
- Modify or create: `crates/beads-git/tests/**`
- Modify or create: `crates/beads-daemon/tests/**`

- [ ] Move path/config tests to `beads-bootstrap`.
- [ ] Move git state-loading tests to `beads-git`.
- [ ] Keep daemon-internal tests in daemon-owned crates.
- [ ] Leave `beads-rs` tests focused on assembly and e2e behavior only.
- [ ] Fix `core/bd-qicx` while doing this by ensuring package-level `cargo check -p beads-rs --all-features` works without workspace feature unification.

### Task 13: Refresh boundary docs and inventories

**Files:**
- Modify: `docs/CRATE_DAG.md`
- Modify: `docs/architecture/non-cli-daemon-coupling-inventory.md`
- Optionally create: `docs/architecture/bootstrap-extraction-closeout.md`

- [ ] Update the documented DAG with `beads-bootstrap`.
- [ ] Refresh the non-CLI daemon coupling inventory after the cutover.
- [ ] Document any intentional remaining cross-crate seams.

### Task 14: Run the full verification matrix

**Files:**
- No file changes required; this is the mandatory closeout gate.

- [ ] Run:

```bash
cargo fmt --all
just dylint
cargo clippy --all-features -- -D warnings
cargo test
cargo test --features slow-tests
cargo check -p beads-rs --all-features
```

- [ ] Re-run the boundary grep checks:

```bash
rg -n "beads_daemon::runtime::" crates --glob '!**/target/**'
rg -n "beads_daemon::git::" crates --glob '!**/target/**'
```

- [ ] Confirm expected result: no matches outside daemon-internal implementation files and any explicitly documented examples.

## Execution Notes

- Use `bd` to track each major phase as one or more focused beads. Do not implement this entire plan in one bead.
- Use `jj` checkpoint rhythm aggressively; this refactor touches multiple ownership boundaries and should be split into many small commits.
- Prefer package-level checks while extracting ownership, not only workspace-level checks, to avoid hidden feature unification problems.
- If `beads-bootstrap` starts accumulating non-bootstrap concerns, stop and split them back out rather than turning it into a new umbrella crate.

## Suggested Bead Breakdown

- `Seal daemon public boundary and remove runtime/git reach-throughs`
- `Create beads-bootstrap crate and move path helpers out of beads-cli`
- `Move canonical config schema/load/merge into beads-bootstrap`
- `Move repo-root discovery into beads-bootstrap and delete daemon duplication`
- `Move git state-loading helpers into beads-git`
- `Move upgrade support out of beads-cli and shrink beads-rs public surface`
- `Re-home tests to owner crates and close package-level all-features gap`

