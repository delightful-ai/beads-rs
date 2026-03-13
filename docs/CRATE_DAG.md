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
- `beads-rs`

`beads-bootstrap`, `beads-cli`, `beads-git`, `beads-daemon`, and `beads-daemon-core` are first-class workspace crates.
`beads-rs` remains the orchestration/compat entrypoint and depends on the leaf crates it assembles.

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
- `beads-core` must not depend on `beads-api`, `beads-bootstrap`, `beads-surface`, `beads-cli`, `beads-git`, `beads-daemon`, `beads-daemon-core`, or `beads-rs`.
- `beads-api` must not depend on `beads-bootstrap`, `beads-surface`, `beads-cli`, `beads-git`, `beads-daemon`, `beads-daemon-core`, or `beads-rs`.
- `beads-bootstrap` must not depend on `beads-api`, `beads-surface`, `beads-cli`, `beads-git`, `beads-daemon`, `beads-daemon-core`, or `beads-rs`.
- `beads-surface` must not depend on `beads-bootstrap`, `beads-cli`, `beads-git`, `beads-daemon`, `beads-daemon-core`, or `beads-rs`.
- `beads-cli` must not depend on `beads-bootstrap`, `beads-git`, `beads-daemon`, `beads-daemon-core`, or `beads-rs`.
- `beads-git` must not depend on `beads-api`, `beads-surface`, `beads-cli`, `beads-daemon`, `beads-daemon-core`, or `beads-rs`.
- `beads-daemon-core` must not depend on `beads-api`, `beads-bootstrap`, `beads-surface`, `beads-cli`, `beads-git`, `beads-daemon`, or `beads-rs`.
- `beads-daemon` must not depend on `beads-cli` or `beads-rs`.
- `beads-rs` must not be used as a dependency by `beads-core`, `beads-api`, `beads-bootstrap`, `beads-surface`, `beads-cli`, `beads-git`, `beads-daemon-core`, or `beads-daemon`.

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

- `beads-cli` owns the CLI parse/dispatch surface and command mapping.
- `beads-rs` CLI code is limited to host orchestration hooks (repo/config/runtime resolution and daemon entrypoint wiring).
- Do not reintroduce parallel command trees under `crates/beads-rs/src/cli/commands/**`.
