# Test Harness Determinism Follow-Through Closeout Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close `beads-rs-81vj` honestly by landing `beads-rs-fblt.15` first, then removing the remaining fixture nondeterminism, shared-state traps, and stale guidance with explicit review gates between risk boundaries.

**Architecture:** Start by moving the deepest tailnet fault-injection coverage off the external daemon plus `tailnet_proxy` harness and onto the deterministic `beads_daemon::testkit::e2e` seam, leaving only thin product-smoke coverage in `beads-rs` assembly tests. After that foundation is reviewed, tighten fixture cleanup and IPC ownership, harden autostart failure surfacing in `beads-surface`, and finish with migration-fixture canon plus an AGENTS/doc audit once the implementation surface stops moving.

**Tech Stack:** Rust, cargo, cargo-nextest, assert_cmd, tempfile, `bd`, `jj`

---

## Scope And Tracker

- Epic: `beads-rs-81vj` — Test harness determinism follow-through
- Predecessor plan: `docs/superpowers/plans/2026-03-14-test-suite-performance-and-isolation.md`
- Child beads in planned execution order:
  - `beads-rs-fblt.15` — Move external tailnet REPL fault-injection tests onto a deterministic shared harness
  - `beads-rs-81vj.2` — Generalize daemon cleanup for sibling runtime/data fixture layouts
  - `beads-rs-81vj.1` — Remove ambient socket defaults from integration helper clients
  - `beads-rs-81vj.4` — Make IPC autostart failures observable and bounded in test workflows
  - `beads-rs-81vj.3` — Unify migration store fixture data and harness APIs
  - `beads-rs-81vj.5` — Audit and codify canonical test fixture rules in AGENTS.md
- Tracker note: `beads-rs-fblt.15` keeps its original id even though it is parented to `beads-rs-81vj`. Use the exact bead id in `bd` commands, audit notes, and `jj` descriptions.

## Review Gate Policy

- Gate A: after `beads-rs-fblt.15`
- Gate B: after `beads-rs-81vj.2` and `beads-rs-81vj.1`
- Gate C: after `beads-rs-81vj.4`
- Gate D: after `beads-rs-81vj.3` and `beads-rs-81vj.5`, then epic closeout
- At every gate, run the full repo verification stack:
  - `cargo fmt --all --check`
  - `just dylint`
  - `cargo clippy --all-features -- -D warnings`
  - `cargo xtest`
  - `cargo nextest run --profile slow --workspace --all-features --features slow-tests`
- Narrow-first rule: before the full gate, run the owning-crate or exact-test slice named in the task.
- Reviewer slices at every gate:
  - deterministic runtime or IPC semantics
  - assembly coverage and fixture ownership
  - docs or operator workflow, when the batch touches guidance or receipts
- If review finds a real in-scope gap, file a child bead under `beads-rs-81vj` immediately, claim it, fix it in a new `jj` change, and rerun the current gate before closing anything.

## Chunk 1: Deterministic Tailnet Fault Harness

### Task 1: Move deep tailnet fault cases onto deterministic shared harness (`beads-rs-fblt.15`)

**Files:**
- Modify: `crates/beads-daemon/src/testkit/e2e.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/repl_rig.rs`
- Modify: `crates/beads-rs/tests/integration/daemon/repl_e2e.rs`
- Modify: `docs/PERF_HOTPATHS.md`
- Test: `cargo test -p beads-daemon --features test-harness -- --list`
- Test: `cargo check -p beads-daemon --all-features`
- Test: `cargo test -p beads-rs --test integration --features slow-tests repl_daemon_pathological_tailnet_roundtrip -- --exact`
- Test: `cargo test -p beads-rs --test integration --features slow-tests repl_daemon_crash_restart_tailnet_roundtrip -- --exact`
- Test: `cargo test -p beads-rs --test integration --features slow-tests repl_tailnet_proxy_smoke -- --exact`

- [ ] Read `bd show beads-rs-fblt.15`, `crates/beads-daemon/AGENTS.md`, `crates/beads-rs/tests/integration/daemon/AGENTS.md`, and `crates/beads-rs/tests/integration/fixtures/AGENTS.md` before editing.
- [ ] Claim the bead and start a fresh change: `bd claim beads-rs-fblt.15` then `jj new -m "beads-rs-fblt.15: deterministic tailnet harness"`.
- [ ] Add deterministic network controls to `beads_daemon::testkit::e2e::ReplicationRig` and `NetworkController` for the behaviors that currently require external proxies:
  - tailnet latency and jitter
  - one-way loss
  - bounded blackhole windows
  - forced session reset points
  - restart sequencing that can prove fresh post-restart handshakes
- [ ] Keep the new surface under the existing `test-harness` seam in `beads-daemon`; do not route anything through `runtime::*`.
- [ ] Port the deep fault-injection cases in `repl_e2e.rs` onto the deterministic harness first:
  - `repl_daemon_pathological_tailnet_roundtrip`
  - `repl_daemon_crash_restart_tailnet_roundtrip`
- [ ] Leave only thin external product-smoke coverage in `repl_e2e.rs`:
  - keep `repl_tailnet_proxy_smoke`
  - keep at most one daemon-to-daemon external tailnet roundtrip if it still proves a shipped-process seam that the deterministic harness cannot
- [ ] Delete `TAILNET_FAULT_INJECTION_GUARD` if the deep cases no longer need it. If one external smoke test still needs it, narrow the guard to that smoke-only path and document why.
- [ ] Record receipts for the migration:
  - targeted burn-in of the migrated tests
  - a fresh `cargo xtest`
  - a fresh slow-profile run
  - update `docs/PERF_HOTPATHS.md` with the new evidence instead of leaving it in tracker comments
- [ ] Run the narrow proof loop listed above before the full gate.
- [ ] Run Gate A full verification.
- [ ] Run Gate A review with two reviewer scopes:
  - deterministic replication and restart semantics
  - assembly test coverage and remaining external-smoke justification
- [ ] Add a `bd` audit note with the workspace path, relevant `jj` change ids, and Gate A receipts, then close `beads-rs-fblt.15`.

## Chunk 2: Fixture Ownership And Cleanup

### Task 2: Generalize daemon cleanup for sibling runtime/data layouts (`beads-rs-81vj.2`)

**Files:**
- Modify: `crates/beads-rs/tests/integration/fixtures/daemon_runtime.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/repl_rig.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/bd_runtime.rs`
- Test: `cargo test -p beads-rs --test integration shutdown_daemon_removes_stale_store_lock -- --exact`
- Test: `cargo test -p beads-rs --test integration shutdown_daemon_terminates_live_daemon_without_started_at_ms -- --exact`
- Test: `cargo test -p beads-rs --test integration --features slow-tests repl_daemon_crash_restart_roundtrip -- --exact`

- [ ] Read `bd show beads-rs-81vj.2` and the nearest `AGENTS.md` files before editing.
- [ ] Claim the bead and start a fresh change: `bd claim beads-rs-81vj.2` then `jj new -m "beads-rs-81vj.2: generalize daemon cleanup layouts"`.
- [ ] Change `daemon_runtime.rs` cleanup to discover store-lock locations from the owning fixture paths instead of assuming `<runtime>/data/stores`.
- [ ] Remove the `ReplRig`-local compensating unlock path once canonical cleanup can handle sibling `runtime/` and `data/` directories.
- [ ] Add regression coverage for both supported layouts:
  - nested `runtime/data/stores`
  - sibling `runtime/` plus `data/`
- [ ] Reuse existing fixture ownership helpers instead of open-coding another teardown path.
- [ ] Run the narrow proof loop listed above before proceeding to Task 3.

### Task 3: Remove ambient socket defaults from integration helper clients (`beads-rs-81vj.1`)

**Files:**
- Modify: `crates/beads-rs/tests/integration/fixtures/load_gen.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/admin_status.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/ipc_stream.rs`
- Modify: `crates/beads-surface/src/ipc/client.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/AGENTS.md`
- Test: `cargo test -p beads-rs --test integration fixtures_load_gen_reports_failures_when_daemon_missing -- --exact`
- Test: `cargo test -p beads-rs --test integration fixtures_admin_status_monotonic_empty_samples -- --exact`
- Test: `cargo test -p beads-rs --test integration fixtures_ipc_stream_parses_subscribed -- --exact`
- Test: `cargo test -p beads-rs --test integration fixtures_ipc_stream_parses_event -- --exact`
- Test: `cargo test -p beads-surface`

- [ ] Read `bd show beads-rs-81vj.1` and `crates/beads-surface/AGENTS.md` before editing.
- [ ] Claim the bead and start a fresh change: `bd claim beads-rs-81vj.1` then `jj new -m "beads-rs-81vj.1: remove ambient fixture sockets"`.
- [ ] Replace helper constructors that default to `IpcClient::new()` with explicit runtime-bound client or runtime-dir APIs.
- [ ] Make the fixture callsites pass owned runtime information instead of inheriting the per-user socket.
- [ ] Add one regression that proves new helper entry points do not silently fall back to the shared per-user socket when a fixture-owned daemon is absent.
- [ ] Update local fixture guidance so new tests start from `BdRuntimeRepo` or `IpcClient::for_runtime_dir(...).with_autostart(false)` instead of ambient IPC.
- [ ] Run the narrow proof loop listed above.
- [ ] Run Gate B full verification after both Task 2 and Task 3 are green.
- [ ] Run Gate B review with two reviewer scopes:
  - fixture ownership and shared-state isolation
  - cleanup and teardown correctness across runtime layouts
- [ ] Add `bd` audit notes for both beads and close `beads-rs-81vj.2` and `beads-rs-81vj.1`.

## Chunk 3: Autostart Failure Hardening

### Task 4: Make IPC autostart failures observable and bounded (`beads-rs-81vj.4`)

**Files:**
- Modify: `crates/beads-surface/src/ipc/client.rs`
- Modify: `crates/beads-surface/src/ipc/spawn_sanitizer.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/load_gen.rs`
- Modify: `crates/beads-rs/tests/integration/daemon/admin_status.rs`
- Modify: `docs/PERF_HOTPATHS.md`
- Test: `cargo test -p beads-surface spawn_command_closes_non_stdio_fds_in_child -- --exact`
- Test: `cargo test -p beads-surface`
- Test: `cargo test -p beads-rs --test integration fixtures_load_gen_reports_failures_when_daemon_missing -- --exact`

- [ ] Read `bd show beads-rs-81vj.4`, `crates/beads-surface/AGENTS.md`, and any child AGENTS under `src/ipc/` before editing.
- [ ] Claim the bead and start a fresh change: `bd claim beads-rs-81vj.4` then `jj new -m "beads-rs-81vj.4: bound autostart failures"`.
- [ ] Add a targeted regression in `beads-surface` that reproduces an immediate daemon-autostart failure and proves the caller gets an actionable error instead of a long socket wait.
- [ ] Replace the broad inherited-fd close sweep in `spawn_sanitizer.rs` with a bounded platform-appropriate strategy.
- [ ] Keep the fix production-safe: do not add test-only behavior to the normal autostart path unless it is explicitly guarded.
- [ ] Update one assembly-facing test or fixture so the new error surface is exercised from a real test workflow, not only from a unit test.
- [ ] Refresh `docs/PERF_HOTPATHS.md` with any profiling or receipt evidence the bead needs.
- [ ] Run the narrow proof loop listed above.
- [ ] Run Gate C full verification.
- [ ] Run Gate C review with two reviewer scopes:
  - IPC/autostart contract and failure propagation
  - perf and process-sanitization safety
- [ ] Add a `bd` audit note and close `beads-rs-81vj.4`.

## Chunk 4: Migration Fixture Canon And Final Documentation

### Task 5: Unify migration fixture data and harness APIs (`beads-rs-81vj.3`)

**Files:**
- Modify: `crates/beads-rs/tests/integration/cli/migration.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/legacy_store.rs`
- Delete: `crates/beads-rs/tests/integration/fixtures/migration_store.rs` after folding its remaining value into `legacy_store.rs`
- Move: `crates/beads-rs/tests/fixture-data/migration/v0_1_26_minimal/*` into `crates/beads-rs/tests/integration/fixtures/legacy_store_corpus/v0_1_26_minimal/`
- Modify: `crates/beads-rs/tests/integration/cli/AGENTS.md`
- Modify: `crates/beads-rs/tests/integration/fixtures/AGENTS.md`
- Test: `cargo test -p beads-rs --test integration test_migrate_fixture_rich_workflow_rewrites_and_preserves_state -- --exact`
- Test: `cargo test -p beads-rs --test integration test_migrate_fixture_related_divergence_merges_realistic_fixtures -- --exact`
- Test: `cargo test -p beads-rs --test integration test_migrate_fixture_unrelated_divergence_requires_force -- --exact`

- [ ] Read `bd show beads-rs-81vj.3` and `crates/beads-rs/tests/integration/cli/AGENTS.md` before editing.
- [ ] Claim the bead and start a fresh change: `bd claim beads-rs-81vj.3` then `jj new -m "beads-rs-81vj.3: unify migration fixture canon"`.
- [ ] Pick one canonical fixture surface for migration/store-ref tests and route the existing helper flows through it instead of keeping parallel local plumbing in `migration.rs`.
- [ ] Remove `migration_store.rs` after folding its dead duplicate loader role into `legacy_store.rs` and the canonical `legacy_store_corpus/` path.
- [ ] Keep `migration.rs` scenario-focused: fixture install, CLI invocation, and assertions should stay there; git/blob/meta plumbing should move into the shared fixture surface.
- [ ] Update local AGENTS guidance so future migration tests start from the canonical fixture instead of copying ad hoc helpers.
- [ ] Run the narrow proof loop listed above.

### Task 6: Audit and codify canonical test fixture rules (`beads-rs-81vj.5`)

**Files:**
- Modify: `AGENTS.md`
- Modify: `crates/beads-rs/tests/AGENTS.md`
- Modify: `crates/beads-rs/tests/integration/AGENTS.md`
- Modify: `crates/beads-rs/tests/integration/daemon/AGENTS.md`
- Modify: `crates/beads-rs/tests/integration/fixtures/AGENTS.md`
- Modify: `docs/PERF_HOTPATHS.md`
- Test: `cargo test -p beads-rs --test public_boundary`

- [ ] Read `bd show beads-rs-81vj.5` plus the AGENTS files in every touched subtree before editing.
- [ ] Claim the bead and start a fresh change: `bd claim beads-rs-81vj.5` then `jj new -m "beads-rs-81vj.5: codify fixture canon"`.
- [ ] Audit the final post-closeout rules for:
  - runtime ownership
  - explicit no-autostart clients for owned daemons
  - temp-root usage
  - wait-helper usage
  - migration fixture canon
  - tailnet fault-isolation rules and runner expectations
- [ ] Delete or rewrite stale instructions instead of layering new text on top of known-bad guidance.
- [ ] Keep the rule at the highest layer where it is stably true; only push detail downward when it is local to one subtree.
- [ ] Run `cargo test -p beads-rs --test public_boundary` if any ownership marker or boundary note changes.
- [ ] Run Gate D full verification after both Task 5 and Task 6 are green.
- [ ] Run Gate D review with three reviewer scopes:
  - migration fixture ownership and scenario coverage
  - docs and AGENTS truthfulness
  - epic closeout completeness
- [ ] Add `bd` audit notes and close `beads-rs-81vj.3` and `beads-rs-81vj.5`.

### Task 7: Close the epic honestly (`beads-rs-81vj`)

**Files:**
- Modify only if final review exposes a real gap

- [ ] Run `bd show beads-rs-81vj` and `bd list --parent=beads-rs-81vj` to confirm every in-scope child is closed or explicitly spun out.
- [ ] Reopen the epic immediately if any review-driven in-scope gap appears after a premature close.
- [ ] Add the epic closeout note with:
  - the child beads closed in this run
  - the review gates that passed
  - the current receipts used to justify deterministic harness trust
  - any residual out-of-scope follow-up beads
- [ ] Close `beads-rs-81vj` only after Gate D is green and no reviewer has an unresolved in-scope finding.
