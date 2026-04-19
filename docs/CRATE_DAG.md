# Crate Dependency DAG Policy

This document is the canonical dependency policy for internal crate boundaries.

## Crates in scope

- `beads-core`
- `beads-api`
- `beads-bootstrap`
- `beads-surface`
- `beads-cli`
- `beads-git`
- `beads-daemon`
- `beads-daemon-core`
- `beads-http`
- `beads-rs`

`beads-bootstrap`, `beads-cli`, `beads-git`, `beads-daemon`, `beads-daemon-core`, and `beads-http` are first-class workspace crates.
`beads-rs` is the tiny assembly crate/package entrypoint and depends on the leaf crates it assembles.
It is not a compatibility umbrella or a reusable dependency surface for other internal crates.

## Allowed edges

Only the directed edges below are allowed:

- `beads-api -> beads-core`
- `beads-bootstrap -> beads-core`
- `beads-surface -> beads-core`
- `beads-surface -> beads-api`
- `beads-cli -> beads-surface`
- `beads-cli -> beads-core`
- `beads-cli -> beads-api`
- `beads-git -> beads-core`
- `beads-git -> beads-bootstrap`
- `beads-daemon -> beads-surface`
- `beads-daemon -> beads-api`
- `beads-daemon -> beads-core`
- `beads-daemon -> beads-bootstrap`
- `beads-daemon -> beads-git`
- `beads-daemon -> beads-daemon-core`
- `beads-daemon-core -> beads-core`
- `beads-http -> beads-core`
- `beads-http -> beads-api`
- `beads-http -> beads-surface`
- `beads-rs -> beads-core`
- `beads-rs -> beads-api`
- `beads-rs -> beads-bootstrap`
- `beads-rs -> beads-surface`
- `beads-rs -> beads-cli`
- `beads-rs -> beads-git`
- `beads-rs -> beads-daemon`
- `beads-rs -> beads-daemon-core`

## Forbidden edges

- Any internal dependency edge not listed in **Allowed edges** is forbidden.
- `beads-core` must not depend on `beads-api`, `beads-bootstrap`, `beads-surface`, `beads-cli`, `beads-git`, `beads-daemon`, `beads-daemon-core`, `beads-http`, or `beads-rs`.
- `beads-api` must not depend on `beads-bootstrap`, `beads-surface`, `beads-cli`, `beads-git`, `beads-daemon`, `beads-daemon-core`, `beads-http`, or `beads-rs`.
- `beads-bootstrap` must not depend on `beads-api`, `beads-surface`, `beads-cli`, `beads-git`, `beads-daemon`, `beads-daemon-core`, `beads-http`, or `beads-rs`.
- `beads-surface` must not depend on `beads-bootstrap`, `beads-cli`, `beads-git`, `beads-daemon`, `beads-daemon-core`, `beads-http`, or `beads-rs`.
- `beads-cli` must not depend on `beads-bootstrap`, `beads-git`, `beads-daemon`, `beads-daemon-core`, `beads-http`, or `beads-rs`.
- `beads-git` must not depend on `beads-api`, `beads-surface`, `beads-cli`, `beads-daemon`, `beads-daemon-core`, `beads-http`, or `beads-rs`.
- `beads-daemon-core` must not depend on `beads-api`, `beads-bootstrap`, `beads-surface`, `beads-cli`, `beads-git`, `beads-daemon`, `beads-http`, or `beads-rs`.
- `beads-daemon` must not depend on `beads-cli`, `beads-http`, or `beads-rs`.
- `beads-http` must not depend on `beads-bootstrap`, `beads-cli`, `beads-git`, `beads-daemon`, `beads-daemon-core`, or `beads-rs`.
- `beads-rs` must not be used as a dependency by `beads-core`, `beads-api`, `beads-bootstrap`, `beads-surface`, `beads-cli`, `beads-git`, `beads-daemon-core`, `beads-daemon`, or `beads-http`.

## CLI Boundary Invariant

The CLI tree must not import daemon modules directly.

Invariant command:

```bash
rg -n "crate::daemon::|beads_daemon::|beads_daemon_core::" crates/beads-rs/src/cli crates/beads-cli/src
```

Expected result: no matches.

Enforcement gate:

```bash
just dylint
```

`just dylint` runs the boundary lint and must fail if CLI daemon imports are reintroduced.

## Daemon Public Boundary Invariant

- Non-daemon crates must not import `beads_daemon::runtime::*`.
- No crate may import `beads_daemon::git::*`.
- `beads-daemon` may keep private `runtime` and `git` shims for internal organization, but those are not public APIs.

Invariant commands:

```bash
rg -n "beads_daemon::runtime::" crates --glob '!**/target/**'
rg -n "beads_daemon::git::" crates --glob '!**/target/**'
```

Expected result: no matches.

## CLI Ownership Invariant

- `beads-cli` owns the `bd` command language: flag/subcommand parsing, CLI-only validation,
  request construction, and human/JSON rendering.
- `beads-rs` CLI code is limited to host orchestration hooks (repo/config/runtime resolution,
  telemetry/bootstrap wiring, daemon entrypoint wiring, and package-level upgrade/install flow).
- Do not reintroduce parallel command trees under `crates/beads-rs/src/cli/commands/**`.

## Test Ownership Invariant

- `beads-rs` tests are limited to assembly/product integration and end-to-end coverage.
- Pure daemon WAL/repl/session coverage lives under `crates/beads-daemon/tests/**`.
- Pure git/checkpoint/state-serialization coverage lives under `crates/beads-git/**`.
- Pure core semantics/validation coverage lives under `crates/beads-core/**`.
- Assembly tests may use `beads_daemon::testkit::*` or `beads_daemon::test_utils::*`
  only for product-level integration seams; they must not reach into `beads_daemon::runtime::*`
  or `beads_daemon::git::*`.
