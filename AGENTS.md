## beads-rs

Distributed work-item database for agent swarms, using git as the sync layer. Rust rewrite of the original Go beads.

Core idea: beads are a CRDT. Most fields are last-writer-wins via `Lww<T>` (timestamp + actor); merges are deterministic. State syncs through `refs/heads/beads/store`, separate from code branches.

A local daemon holds canonical state and schedules git sync (~500ms). CLI talks to it over Unix socket, auto-starts on first use. One daemon serves many clones by keying state on normalized remote URL.

## Worldview

- **Kill scatter**: One source of truth per concept. If data lives in two places, merge or derive.
- **Types tell the truth**: Typed digests, enums not strings, validated IDs. Wrong states should be unrepresentable.

## Architecture

```
crates/beads-core/     CRDT/domain model, Lww<T>, CanonicalState, validated types
crates/beads-api/      IPC + --json schemas
crates/beads-rs/src/
  ├── git/             sync protocol, wire format, checkpoint
  ├── daemon/          ops + queries, Unix socket IPC, sync scheduling
  ├── cli/             clap parsing, command handlers, rendering
  ├── migrate/         import utilities (beads-go JSONL)
  └── bin/             bd entrypoint
```

## Key Files

- `SPEC.md` — data model + invariants
- `CLI_SPEC.md` — CLI surface
- `WORKFLOW.md` — jj + bd workflow details

## Build & Verify

```bash
cargo check                      # typecheck (run compulsively)
cargo fmt --all                  # format
just dylint                      # required boundary gate (CLI/daemon layering)
cargo clippy --all-features -- -D warnings  # lint (CI uses --all-features)
cargo xtest                      # canonical fast tier (nextest fast profile)
cargo nextest run --profile slow --workspace --all-features --features slow-tests
```

Logging: `LOG=debug` or `LOG=beads_rs=trace`, or `-v/-vv` on `bd`.

## Test Infrastructure

- Default to `cargo xtest` for the warm fast tier. It intentionally runs the workspace fast profile plus `beads-rs/e2e-tests`, not `--all-features`, so `slow-tests` stay out of the fast tier. Use `cargo nextest run --profile slow --workspace --all-features --features slow-tests` for the slow tier.
- The shared nextest runner is intentionally capped in `.config/nextest.toml` (`test-threads = 4`). Do not raise it blindly; prove any change with repeated whole-suite burn-in receipts.
- Tailnet fault-injection `daemon::repl_e2e` tests run in the `tailnet-fault-injection` nextest group (`max-threads = 1`). If you add another tailnet/proxy stress test, fence it into the same group instead of letting it silently contend with the rest of the fast tier.
- Profile the suite with `./scripts/profile-tests.sh`. It captures per-test nextest timing plus env-gated fixture timing under `tmp/perf/tests-*`.
- `./scripts/profile-tests.sh` defaults to `PROFILE_TEST_THREADS=2` so profiling artifacts stay reproducible under load. Override only when you are deliberately measuring a different concurrency level.
- Reuse shared helpers under `crates/beads-rs/tests/integration/fixtures/`. If a test needs new repo/runtime/daemon setup behavior, add or extend a shared fixture instead of creating another one-off local `TestRepo`.
- Reuse `fixtures::temp` for integration temp roots. Do not derive test temp dirs from `current_dir()/tmp`; keep Unix socket paths short and deterministic instead. Use `BD_TEST_TMP_ROOT` only when you need an explicit override, and `BD_TEST_KEEP_TMP=1` when you need to retain fixture dirs for debugging.
- Shared fixture timing lives in `crates/beads-rs/tests/integration/fixtures/timing.rs` and is activated with `BD_TEST_TIMING_DIR`.
- Avoid fixed sleeps in tests. Prefer shared condition-based wait helpers and explicit readiness barriers.
- Reuse `fixtures::bd_runtime` and `fixtures::wait` for daemon socket/meta/store discovery and process-exit polling instead of open-coding JSON/socket probes in individual tests.
- If a test can autostart or restart a daemon, allocate its runtime through `BdRuntimeRepo` or another shared fixture with an owned `Drop` cleanup path. Do not spin up git-sync-enabled daemons against ad hoc `TempDir` runtimes.
- If a fixture intends to own a daemon process, spawn `bd daemon run` explicitly from the fixture before `Init` and drive bootstrap/status over `IpcClient::for_runtime_dir(...).with_autostart(false)`. Do not bootstrap owned daemons through helper CLI autostart paths like `bd init` or `bd status`, or you will lose process ownership before teardown.
- If a fixture owns a `std::process::Child`, wait for shutdown by polling `Child::try_wait()` and reap it through the handle. If graceful daemon shutdown misses its deadline, force-kill the owned child and reap it through the same handle instead of hoping PID/lock discovery finds it later.
- For replicated-fsync integration tests, gate writes on `ReplRig::assert_replication_durability_ready()` rather than handshake-only helpers. `dead_ms` is also the daemon-side durability wait budget, so a write started before peers become `durability_eligible` can stall until nextest kills the test.
- For namespace convergence polling, prefer `AdminFingerprint` over full `AdminStatus`. Reserve `AdminStatus` for liveness/WAL assertions that actually need the heavier payload.
- If you add a read-based admin helper, thread its `ctx.read.wait_timeout_ms` through the fixture IPC timeout mapping. Do not let read-heavy helpers like `AdminFingerprint` fall back to the 7-second floor, or convergence loops will spin until nextest kills the test under load.
- For crash/restart replication tests, snapshot readiness first and then require fresh post-restart handshakes with `replication_ready_snapshot()` and `assert_replication_ready_since(...)`. Do not treat persisted pre-crash liveness rows as proof of recovery.
- Do not call `reload_replication()` after ordinary replicated writes when the roster/config has not changed; wait on the existing readiness/convergence helpers instead.
- The default tailnet smoke profile should stay deterministic (latency/jitter only). Put loss, duplication, blackholes, and reorder stress in the explicit pathological tailnet tests instead of the default roundtrip smoke.
- Fault-injection tests should use test-local liveness budgets when the assertion is about eventual recovery under packet loss or resets. Do not inherit production `keepalive_ms` / `dead_ms` into pathological CI tests unless the test is explicitly about those production budgets.
- External daemon + proxy fault-injection tests must declare their isolation boundary in both runners. Fence them into the `tailnet-fault-injection` nextest group, and if they live in the same libtest binary, guard the conflicting cases with a shared single-flight lock so plain `cargo test` does not hang on the same OS-level harness contention. Fast/default profiles can share the runner with non-tailnet tests once the convergence helper stays lightweight; the slow profile still reserves full runner ownership for these tests.

## Coding Style

- Follow `rustfmt` + keep `clippy` clean.
- Explicit error types (`thiserror`), `Result<T, beads_rs::Error>` in library APIs.
- Naming: `snake_case.rs` modules, `CamelCase` types, `kebab-case` CLI flags.
- Regression test for bug fixes; keep tests deterministic and OS-independent.

## Version Control

We use **jj** (jujutsu), not git directly. Use the `/using-jj` skill for VCS operations.
- Resolving conflicts? See the conflicts section in the jj skill.
- Making PRs? Read the github file first.
- Messing with the DAG? Read the surgery file.

### JJ Rhythm

**Commits are checkpoints, not milestones.**

```
jj new                              # start fresh change
# edit: one logical thing
jj describe "bd-xyz: what you did"  # label it
# repeat
```

This loop runs 3-20 times per bead. A bead spanning 1 commit means you batched too much.

- ~50 lines without `jj describe`? Too much. Describe and `jj new`.
- Touched 2+ unrelated things? Should've been 2 commits.
- About to context-switch? Describe first.
- **Every commit message includes the bead ID.**

Fixing mistakes: `jj split` (batched too much), `jj describe` (wrong message), `jj squash`/`jj rebase -r` (reorganize).

## Issue Tracking

We use **bd**. If this is your first time interacting with beads this session, run `bd prime` for a quick, dense tutorial.

### The Core Workflow

```bash
bd ready                    # see what's next
bd show bd-xyz              # understand it
bd claim bd-xyz             # you own it now
jj new                      # start first change

# --- this loop runs MANY times per bead ---
# edit: one coherent thing (add fn, fix bug, write test)
jj describe "bd-xyz: added validation for Foo"
jj new
# edit: next coherent thing
jj describe "bd-xyz: tests for Foo validation"
jj new
# ---

# required verification gate before close
cargo fmt --all
just dylint                       # required boundary gate (crate layering)
cargo clippy --all-features -- -D warnings
cargo test

bd close bd-xyz            # bead done, all acceptance criteria met
bd ready                    # next bead
```

The jj loop is INSIDE the bead loop. Many commits per bead is correct.

### Follow-up Beads

When you notice out-of-scope work while implementing something, **file a bead immediately**—don't just mention it in commit messages.

```bash
bd create "Hardcoded 30s timeout in sync.rs:234" --type=bug --priority=2
bd create "executor.rs:145 check-then-unwrap should use require_live" --type=chore --priority=3
```

### Writing Good Beads

Each bead should be **one self-contained, independently doable thing**. If you're writing a bead that says "and also..." — stop and make two beads.

**Structure for non-trivial beads:**

```
**Problem**
What's wrong or missing. Be specific — file paths, error messages, code snippets.

**Design**
How to fix it. Include implementation approach and code examples.

**Acceptance**
- [ ] Concrete, verifiable checklist items
- [ ] Tests pass
- [ ] Specific behavior works

**Files:** list of affected files
```

**Priority:** P0 (critical) → P1 (high) → P2 (medium) → P3 (low) → P4 (backlog)

**Epics & deps:**
```bash
bd create "Auth overhaul" --type=epic
bd create "Add OAuth support" --parent=bd-xxx
bd dep add A B              # A depends on B
```

## Philosophy

This codebase will outlive you. Every shortcut becomes someone else's burden. Every hack compounds.

Fight entropy. Leave the codebase better than you found it.
