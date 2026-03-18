# Test Suite Performance And Isolation Hardening Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the warm fast-tier test path to 60 seconds or less via `cargo xtest`, while making `slow-tests` runnable and deterministic.

**Architecture:** Use `cargo nextest` as the canonical measurement and execution engine, add reproducible profiling output first, then attack slowness at the harness layer instead of guessing at individual tests. Instrument fixture setup, daemon boot, readiness barriers, and command round-trips so every change has a before/after receipt. Consolidate repeated repo/runtime/daemon setup, replace fixed sleeps with shared condition-based waits, and eliminate process-global leaks before wiring the final `cargo xtest` and CI path.

**Tech Stack:** Rust, cargo nextest, macOS Instruments/Samply, assert_cmd, tempfile, jj, bd

---

## Scope And Tracker

- Epic: `beads-rs-fblt` — Test suite performance and isolation hardening
- New child beads:
  - `beads-rs-fblt.1` — Instrument fast and slow test tiers with reproducible timing baselines
  - `beads-rs-fblt.2` — Deduplicate assembly integration harness setup across CLI and daemon tests
  - `beads-rs-fblt.3` — Replace fixed sleeps with condition-driven wait helpers in slow tests
  - `beads-rs-fblt.4` — Land a canonical cargo xtest plus nextest config for local and CI runs
- Existing beads pulled under the epic:
  - `bd-6ole`, `bd-crxg`, `beads-rs-iymc`, `bd-bfbr`, `bd-fpa7`, `beads-rs-vi7x`

## Chunk 1: Baseline And Harness Consolidation

### Task 1: Capture Reproducible Test Baselines (`beads-rs-fblt.1`)

**Files:**
- Modify: `.cargo/config.toml`
- Create: `.config/nextest.toml`
- Create: `scripts/profile-tests.sh`
- Modify: `docs/PERF_HOTPATHS.md`

- [ ] Add a repo-local `cargo xtest` alias in `.cargo/config.toml` that shells out to `cargo nextest run --workspace --all-features`.
- [ ] Add `.config/nextest.toml` with explicit profiles for `fast` and `slow`, stable thread counts, slow-test timeout thresholds, and junit/status settings that work in CI.
- [ ] Add `scripts/profile-tests.sh` that captures:
  - `cargo nextest list --workspace --all-features`
  - `cargo nextest list --workspace --all-features --features slow-tests`
  - `cargo nextest run --workspace --all-features --no-fail-fast --status-level slow --final-status-level slow --message-format libtest-json-plus`
  - `cargo nextest run --workspace --all-features --features slow-tests --no-fail-fast --status-level slow --final-status-level slow --message-format libtest-json-plus`
- [ ] Add test-only timing instrumentation in the shared integration fixtures so setup cost is broken down into repo/bootstrap, daemon start, replication readiness, and command round-trip phases.
- [ ] Make the script write timestamped artifacts under `tmp/perf/tests-*/`.
- [ ] Run the profiler once on the current tree and record the top slow binaries/tests plus setup-phase hotspots in `docs/PERF_HOTPATHS.md`.
- [ ] Describe and checkpoint the tracker-only work under `beads-rs-fblt.1`.

### Task 2: Extract Shared CLI/Daemon Runtime Fixtures (`beads-rs-fblt.2`)

**Files:**
- Create: `crates/beads-rs/tests/integration/fixtures/bd_runtime.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/mod.rs`
- Modify: `crates/beads-rs/tests/integration/cli/critical_path.rs`
- Modify: `crates/beads-rs/tests/integration/cli/migration.rs`
- Modify: `crates/beads-rs/tests/integration/daemon/admin.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/realtime.rs`
- Test: `cargo test -p beads-rs --test integration cli::critical_path::test_init_creates_beads_branch -- --exact`
- Test: `cargo test -p beads-rs --test integration cli::migration::test_migrate_basic_import -- --exact`

- [ ] Add one shared fixture module in `bd_runtime.rs` that owns:
  - work repo creation
  - optional bare remote creation
  - runtime/data-dir allocation
  - canonical `bd` command construction
  - daemon cleanup on drop
- [ ] Write or move one targeted regression test in `critical_path.rs` that proves the shared fixture still creates isolated runtime/data-dir paths per test instance.
- [ ] Run the targeted `critical_path` test first and confirm it fails if the new fixture is incomplete.
- [ ] Port `critical_path.rs` to the shared fixture with no behavior changes.
- [ ] Port `migration.rs` to the same fixture instead of its local `TestRepo`.
- [ ] Migrate any matching daemon integration setup that still duplicates repo/runtime bootstrapping.
- [ ] Re-run the targeted CLI tests plus `cargo nextest run -p beads-rs --test integration --all-features`.
- [ ] Describe and checkpoint the fixture extraction work under `beads-rs-fblt.2`.

### Task 3: Fix REPL Harness Path Length And Pre-init Config Ordering (`bd-6ole`, `beads-rs-iymc`)

**Files:**
- Modify: `crates/beads-rs/tests/integration/fixtures/repl_rig.rs`
- Modify: `crates/beads-rs/tests/integration/daemon/repl_e2e.rs`
- Test: `cargo test -p beads-rs --test integration --features slow-tests daemon::repl_e2e::repl_daemon_to_daemon_roundtrip -- --exact`

- [ ] Add a focused regression around temp-root selection or a harness assertion that fails when runtime/socket paths exceed the platform limit.
- [ ] Run the targeted REPL e2e test and confirm the current harness still relies on repo-path-derived temp roots or pre-init config timing.
- [ ] Change `repl_rig.rs` to allocate from a short system temp root with an env override for keep-tmp debugging.
- [ ] Ensure WAL-limit and replication config are written before init/daemon startup in the shared REPL seed path.
- [ ] Re-run the targeted REPL test plus `cargo nextest run -p beads-rs --test integration --all-features --features slow-tests daemon::repl_e2e::`.
- [ ] Describe and checkpoint the REPL harness fixes under `bd-6ole` and `beads-rs-iymc`.

## Chunk 2: Wait Semantics And Shared-State Isolation

### Task 4: Replace Fixed Sleeps With Condition-Based Wait Helpers (`beads-rs-fblt.3`, `bd-bfbr`, `bd-crxg`)

**Files:**
- Create: `crates/beads-rs/tests/integration/fixtures/wait.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/mod.rs`
- Modify: `crates/beads-rs/tests/integration/cli/critical_path.rs`
- Modify: `crates/beads-rs/tests/integration/cli/migration.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/tailnet_proxy.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/repl_rig.rs`
- Modify: `crates/beads-rs/tests/integration/daemon/subscribe.rs`
- Modify: `crates/beads-rs/tests/integration/daemon/repl_e2e.rs`
- Test: `cargo test -p beads-rs --test integration --features slow-tests daemon::repl_e2e::repl_daemon_crash_restart_roundtrip -- --exact`
- Test: `cargo test -p beads-rs --test integration --features slow-tests daemon::subscribe::subscribe_multiple_clients_receive_same_events -- --exact`

- [ ] Add `fixtures/wait.rs` with reusable polling helpers for:
  - process exit
  - socket readiness
  - WAL segment appearance
  - replication/admin readiness
- [ ] Write or tighten a failing regression for the crash/restart flake (`bd-crxg`) so it exercises the missing readiness barrier instead of relying on one successful run.
- [ ] Replace raw `thread::sleep` calls in the targeted files with named helpers from `fixtures/wait.rs` unless a fixed pause is semantically required.
- [ ] Add the replication-ready barrier from `bd-bfbr` where tailnet tests currently assume peer readiness.
- [ ] Re-run the two targeted slow tests, then the full REPL/subscribe slow slice through `nextest`.
- [ ] Update the perf notes with before/after timing for the affected test set.
- [ ] Describe and checkpoint this work under `beads-rs-fblt.3`, `bd-bfbr`, and `bd-crxg`.

### Task 5: Eliminate Process-Global Test State Leaks (`bd-fpa7`, `beads-rs-vi7x`)

**Files:**
- Modify: `crates/beads-daemon/tests/support/env.rs`
- Modify: `crates/beads-daemon/tests/config_precedence.rs`
- Modify: `crates/beads-bootstrap/tests/repo_discovery.rs`
- Modify: `crates/beads-daemon/src/metrics.rs`
- Modify: `crates/beads-git/src/paths.rs`
- Modify: `crates/beads-daemon/src/paths.rs`
- Test: `cargo test -p beads-daemon --test config_precedence`
- Test: `cargo test -p beads-bootstrap --test repo_discovery`
- Test: `cargo test -p beads-daemon snapshot_collects_metrics`

- [ ] Add a failing regression that proves env-mutating tests must serialize or restore global config state correctly.
- [ ] Make `crates/beads-daemon/tests/support/env.rs` the single env guard path and use it anywhere config env vars are mutated.
- [ ] Add a test-only metrics reset/scope helper behind `cfg(test)` and migrate exact-delta assertions to it.
- [ ] Tighten the `beads-git` path override API so daemon-owned tests can isolate cache paths without widening the normal production surface.
- [ ] Re-run the targeted daemon/bootstrap tests plus any `beads-git` isolation coverage.
- [ ] Describe and checkpoint the isolation work under `bd-fpa7` and `beads-rs-vi7x`.

## Chunk 3: Canonical Runner, CI, And Final Verification

### Task 6: Wire The Canonical `cargo xtest` Path (`beads-rs-fblt.4`)

**Files:**
- Modify: `.cargo/config.toml`
- Modify: `.config/nextest.toml`
- Modify: `justfile`
- Modify: `.github/workflows/` test workflow files
- Modify: `AGENTS.md`
- Modify: `WORKFLOW.md`

- [ ] Add the final `cargo xtest` alias and any companion `just` recipes so local fast-tier usage is one command.
- [ ] Point CI at the same nextest configuration for the fast tier and keep `slow-tests` as an explicit, runnable tier.
- [ ] Remove or simplify redundant `cargo test --all-features` invocations in docs/workflow where the new `cargo xtest` path should be the default developer loop.
- [ ] Run `scripts/profile-tests.sh` again and compare the new artifacts with the baseline from Task 1.
- [ ] Do not close `beads-rs-fblt.4` until the warm fast-tier path is at or below 60 seconds.
- [ ] Describe and checkpoint the runner/CI work under `beads-rs-fblt.4`.

### Task 7: Whole-Epic Verification And Closeout (`beads-rs-fblt`)

**Files:**
- Modify only if verification reveals gaps

- [ ] Run `cargo fmt --all`.
- [ ] Run `just dylint`.
- [ ] Run `cargo clippy --all-features -- -D warnings`.
- [ ] Run `cargo xtest`.
- [ ] Run `cargo nextest run --workspace --all-features --features slow-tests`.
- [ ] If any slow slice still flakes, write the failing regression before changing production or harness code again.
- [ ] Close child beads as their acceptance criteria are met, then close `beads-rs-fblt` only after the final timing target and slow-test trust target are both satisfied.
