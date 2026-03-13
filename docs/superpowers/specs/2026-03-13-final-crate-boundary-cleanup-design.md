# Final Crate Boundary Cleanup Design

## Problem

The workspace decomposition landed, but the final public boundaries are still
too soft:

- `beads-rs` still reaches into `beads_daemon::runtime::*` for daemon startup,
  upgrade, and offline store admin helpers.
- `beads-daemon` still exposes `pub mod runtime` and a broad `git` re-export,
  which preserves shortcut imports instead of forcing callers onto the owning
  crate.
- Config, path resolution, and repo-root discovery are duplicated across
  `beads-rs` and `beads-daemon`, leaving multiple sources of truth for one
  concept.
- `beads-rs` still looks more like a library crate than an assembly crate.

The result is that the codebase is split structurally, but not yet sealed
semantically. A crate-local reader can still get pulled across boundaries to
understand behavior.

## Goals

- Make `beads-rs` a tiny assembly crate, not a subsystem API.
- Seal daemon runtime internals behind deliberate `beads-daemon` top-level APIs.
- Remove daemon-owned convenience re-exports that bypass crate ownership.
- Introduce a single owner for app bootstrap concerns: config, paths, and repo
  discovery.
- Leave each crate locally understandable without opening `beads-rs`.

## Non-Goals

- No CLI behavior changes.
- No store format, wire format, or protocol format changes.
- No compatibility shims for old crate paths beyond the minimum needed during
  the implementation branch.
- No new umbrella crate that re-exports the workspace.

## Chosen Direction

Use a hard cutover with no long-lived compatibility seams.

This repository is already on `0.2.0-alpha`, and the remaining cleanup is
internal Rust API work, not product-surface work. The design therefore
optimizes for strong ownership and simple boundaries rather than migration
softness.

## Target Crate Roles

### `beads-core`

Owns domain truth only:

- CRDTs
- validated IDs and enums
- event/domain/state types
- shared error payload primitives

Must not depend on daemon, git, CLI, or bootstrap concerns.

### `beads-api`

Owns canonical JSON schemas for daemon IPC and `--json` output.

Must remain a pure schema crate.

### `beads-surface`

Owns the public interaction surface:

- IPC request/response types
- IPC codec
- IPC client/autostart/runtime-dir helpers
- store-admin no-autostart helper calls
- patch/query surface types

Must not grow daemon policy, git logic, or app-config loading.

### `beads-cli`

Owns parse/dispatch and the typed host trait used by the app package.

Must not import daemon internals or git internals directly.

### `beads-git`

Owns git sync and checkpoint behavior:

- store ref loading
- sync typestate/process
- checkpoint import/export/publish/cache
- git-specific repo/store helpers

Anything that reads or writes `refs/heads/beads/store` belongs here rather than
`beads-rs`.

### `beads-daemon-core`

Owns shared daemon primitives:

- WAL implementation and frame/index primitives
- repl framing/protocol
- other runtime-agnostic daemon-core contracts

Must not depend on daemon runtime policy.

### `beads-daemon`

Owns daemon runtime behavior:

- lifecycle/run loop
- scheduling
- mutation/query execution
- replication orchestration
- offline/online daemon admin behavior
- daemon testkit and daemon test utilities

Its public API should be deliberate and small:

- `run_daemon`
- `DaemonLayout`
- `DaemonRuntimeConfig`
- explicitly named daemon-owned admin helpers
- `testkit`

It must not publicly expose `runtime::*`, and it must not act as a proxy for
`beads-git`.

### `beads-bootstrap` (new)

Owns app bootstrap concerns shared by the app package and daemon:

- config schema/load/merge/write
- path resolution and test overrides
- repo-root discovery

This crate exists to kill scatter, not to become a new umbrella crate.

### `beads-rs`

Owns assembly only:

- binary entrypoint wiring
- CLI host implementation
- daemon entrypoint wiring
- telemetry initialization and defaults
- self-upgrade/install behavior

`beads-rs` is the package crate for shipping `bd`, not the place where shared
domain or subsystem APIs live.

## Public Boundary Rules

1. A public module exists only if an external crate genuinely needs it.
2. No crate may publicly re-export another subsystem crate just for convenience.
3. `beads-rs` must not be used as a dependency by other workspace crates.
4. `beads-daemon` must not expose `runtime` as public API.
5. Callers needing git behavior must import `beads-git` directly.
6. Config/path/repo bootstrap must have one owning crate.

## Required Structural Changes

### 1. Seal `beads-daemon`

- Remove `pub mod runtime` from the daemon crate root.
- Remove the daemon crate's `git` re-export module.
- Promote only the truly public daemon-owned APIs to top-level modules or
  functions.
- Keep internal organization under a private `runtime` tree.

### 2. Make `beads-rs` a true assembly crate

- Stop reaching into `beads_daemon::runtime::*`.
- Keep only small bin-facing entrypoints and assembly helpers public.
- Treat config/path/repo/store-admin helpers as assembly internals unless a
  local binary in the same package needs access.
- Remove any remaining subsystem-style re-exports.

### 3. Introduce `beads-bootstrap`

Move these concepts out of `beads-rs` and `beads-daemon` duplication:

- config schema
- config loading/merging/writing
- path resolution and config-driven overrides
- repo root discovery

After the move:

- `beads-rs` depends on `beads-bootstrap`
- `beads-daemon` depends on `beads-bootstrap`
- duplicate config/path/repo definitions are deleted from `beads-daemon`
- `beads-rs` becomes either a thin wrapper over `beads-bootstrap` or imports it
  directly

### 4. Re-home git-owned helpers

Store/ref loading helpers that interpret git state should live in `beads-git`,
not `beads-rs`.

This includes helpers like:

- reading current state/store metadata from `refs/heads/beads/store`
- git-oriented repo loading utilities that are not assembly-specific

### 5. Tighten test ownership

- `beads-rs` tests cover assembly and end-to-end behavior only.
- bootstrap tests move with `beads-bootstrap`.
- git helper tests live in `beads-git`.
- daemon internal behavior stays in `beads-daemon` and `beads-daemon-core`.

## Cleanup Sequence

### Phase 1: Replace daemon runtime reach-throughs

- Add or expose the small daemon-owned top-level APIs required by
  `beads-rs`.
- Rewrite `beads-rs` call sites to use:
  - `beads-surface` for IPC/socket/autostart helpers
  - `beads-surface::store_admin` for no-autostart store admin helpers
  - top-level `beads-daemon` functions for daemon-owned offline admin and run
    entrypoints
  - direct `beads-git` imports for git behavior

Exit condition: no `beads_daemon::runtime::*` imports outside `beads-daemon`.

### Phase 2: Remove accidental daemon exports

- Remove public `runtime`.
- Remove daemon `git` re-export.
- Fix downstream imports and tests to use owning crates directly.

Exit condition: no `beads_daemon::git::*` imports anywhere.

### Phase 3: Extract `beads-bootstrap`

- Create the new crate.
- Move config schema/load/merge/write, path resolution, and repo-root discovery
  into it.
- Rewire `beads-rs` and `beads-daemon` to depend on it.
- Delete duplicated config/path/repo ownership from `beads-daemon`.

Exit condition: one source of truth for bootstrap concerns.

### Phase 4: Re-home git-owned state-loading helpers

- Move git store-loading helpers from `beads-rs` into `beads-git`.
- Keep `beads-rs` limited to assembly-level repo resolution when needed for the
  binaries.

Exit condition: `beads-rs` no longer owns git subsystem helpers.

### Phase 5: Re-home tests and tighten visibility

- Move tests to owning crates.
- Make assembly-only modules in `beads-rs` private where possible.
- Re-run package-level and workspace-level checks.

Exit condition: `beads-rs` reads as a small app package when opened cold.

## Verification

At the end of the cleanup, all of the following should pass:

```bash
cargo fmt --all
just dylint
cargo clippy --all-features -- -D warnings
cargo test
cargo test --features slow-tests
cargo check -p beads-rs --all-features
```

In addition, boundary grep checks should hold:

```bash
rg -n "beads_daemon::runtime::" crates --glob '!**/target/**'
rg -n "beads_daemon::git::" crates --glob '!**/target/**'
```

Expected result for both commands: no matches outside allowed daemon-internal
documentation examples, if any remain.

## Done-State Invariants

The cleanup is complete when all of the following are true:

- `beads-rs` is obviously an assembly crate, not a subsystem API.
- No workspace crate imports `beads_daemon::runtime::*`.
- No code imports `beads_daemon::git::*`.
- Config/path/repo bootstrap has a single owner.
- Git store-loading helpers live with `beads-git`.
- `beads-rs` tests are assembly/e2e only.
- A new agent can work inside `beads-git`, `beads-daemon`,
  `beads-daemon-core`, or `beads-bootstrap` without reading `beads-rs` to find
  the real logic.

## Risks

- Moving bootstrap concerns too broadly could accidentally create another
  umbrella crate. `beads-bootstrap` must stay narrow.
- Some tests may currently rely on `beads-rs` paths as a convenience; those
  should be treated as migration work, not justification to keep the wrong
  boundary.
- Package-level checks may expose feature-wiring bugs that workspace feature
  unification currently masks.
