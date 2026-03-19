# Hotpath Benchmarking

This repo includes a first-class benchmark harness for CLI + daemon hotpaths:

- script: `scripts/profile-hotpaths.sh`
- compare helper: `scripts/compare-hotpaths.sh`
- just recipes:
  - `just bench-hotpaths`
  - `just bench-compare <baseline_dir> <candidate_dir>`
  - `just bench-guard <baseline_dir> <candidate_dir>`

## Quick Start

```bash
# Build debug bd and run benchmark suite
just bench-hotpaths

# Or run against a specific binary (for example installed bd)
BD_BIN=/Users/darin/.local/bin/bd just bench-hotpaths
```

The run prints an artifact directory path (for example `tmp/perf/hotpaths-YYYYMMDD-HHMMSS`).
It also refreshes `tmp/perf/hotpaths-latest` as a symlink to the newest run.

## What Gets Measured

Read-heavy workflows (from `bd prime`):
- `ready`, `list`, `blocked`, `stale`, `search`, `count`, `show`, `show --json`, `dep tree`, `comments`, `status`, `epic status`

Mutation/lifecycle workflows:
- `create`, `claim/unclaim`, `close/reopen`, `comment`, `label add/remove`, `dep add/rm`, full create->claim->show->comment->close->reopen->delete scenario

Daemon observability:
- request fanout counts
- store identity resolution timing summary
- admin metrics snapshot (`bd admin metrics --json`)
- warning/error extraction from daemon logs

Measured-phase filtering:
- Log-derived summaries (`request-type-counts`, `store-identity-latency-summary`, `warnings-errors`) start from a captured daemon-log line boundary after warm-up, so startup/import noise is excluded.

## Key Artifacts

- `SUMMARY.txt`: human-readable run digest
- `hyperfine-read.json`, `hyperfine-write.json`: canonical benchmark data
- `read-latency-summary.tsv`, `write-latency-summary.tsv`: compact latency tables (`mean`, `median`, `p95`, `p99`, `min`, `max`)
- `request-type-counts.txt`: daemon request mix
- `store-identity-latency-summary.tsv`: per-request identity-resolution overhead
- `ipc-request-latency-summary.tsv`: request-type latency (`p50`, `p95`, `max`) from daemon metrics
- `ipc-read-gate-wait-summary.tsv`: read-gate wait latency (`p50`, `p95`, `max`) from daemon metrics
- `admin-metrics.json`: full admin metrics

Note:
- `admin-metrics.json` reflects metrics accumulated across the daemon lifetime for that run (including startup/warm-up).
- For steady-state comparisons, rely on hyperfine summaries plus measured-phase log-derived summaries.

## Comparing Two Runs

```bash
just bench-compare tmp/perf/hotpaths-<baseline> tmp/perf/hotpaths-<candidate>
```

This prints:
- read latency delta per command
- read tail delta (`p95`, `p99`, `max`)
- write latency delta per workflow
- write tail delta (`p95`, `p99`, `max`)
- store-identity mean latency deltas (when available)
- candidate `ipc_request_duration` histogram breakdowns

## Regression Guardrail

```bash
just bench-guard tmp/perf/hotpaths-<baseline> tmp/perf/hotpaths-<candidate>
```

This enforces threshold checks and exits non-zero on regression:
- Critical read paths (`ready`, `list --status open`, `show`, `show --json`, `status`): max +25% mean latency
- Write workflows (create/claim-close/comment/label/dep/scenario): max +35% mean latency
- Critical read paths: max +50% `p95` latency
- Write workflows: max +65% `p95` latency

Thresholds are configurable:
- `READ_THRESHOLD_PCT` (default `25`)
- `WRITE_THRESHOLD_PCT` (default `35`)
- `READ_P95_THRESHOLD_PCT` (default `50`)
- `WRITE_P95_THRESHOLD_PCT` (default `65`)

## Environment Knobs

- `BD_BIN` (default `./target/debug/bd`)
- `RUNS` (default `15`)
- `WARMUP` (default `3`)
- `HOTPATH_ITERS` (default `20` via `just` recipe)
- `OUT_DIR` (optional explicit artifact path)
- `LOG` (default `error`; also applied to `RUST_LOG` for reproducible log verbosity)

## Recommended Practice

- Always compare against a saved baseline artifact directory.
- Keep benchmark runs pinned to a known binary via `BD_BIN`.
- Treat `hyperfine-*.json` + `admin-metrics.json` as source-of-truth data.

## Test Suite Profiling

The repo now has a first-class test-suite profiling harness:

- script: `scripts/profile-tests.sh`
- fast entrypoint: `cargo xtest` (`nextest` fast profile plus `beads-rs/e2e-tests`, excluding `slow-tests`)
- slow entrypoint: `cargo nextest run --profile slow --workspace --all-features --features slow-tests`

### What It Captures

- `fast-list.txt`, `slow-list.txt`: enumerated test inventory
- `fast-raw.log`, `slow-raw.log`: full nextest output
- `fast-messages.jsonl`, `slow-messages.jsonl`: structured nextest timing events
- `fast-top-tests.tsv`, `slow-top-tests.tsv`: slowest individual tests
- `fast-top-suites.tsv`, `slow-top-suites.tsv`: slowest binaries / suites
- `fast-phase-summary.tsv`, `slow-phase-summary.tsv`: env-gated fixture timing aggregated by phase
- `fast-phases/`, `slow-phases/`: raw per-process timing JSONL emitted by shared test fixtures
- `SUMMARY.txt`: condensed run digest

### Fixture Timing

Shared integration fixtures write timing events when `BD_TEST_TIMING_DIR` is set. Current coverage includes:

- git fixture setup in `crates/beads-rs/tests/integration/fixtures/git.rs`
- realtime fixture setup and daemon bootstrap in `crates/beads-rs/tests/integration/fixtures/realtime.rs`
- tailnet proxy spawn and readiness in `crates/beads-rs/tests/integration/fixtures/tailnet_proxy.rs`
- replication rig construction, bootstrap, proxy startup, and daemon startup in `crates/beads-rs/tests/integration/fixtures/repl_rig.rs`

### Current Baseline

Pre-fix baseline artifact:

- `tmp/perf/tests-20260314-104053`

What that baseline showed:

- fast profile exit code: `100`
- blocking failures: `cli::migration::test_migrate_fixture_related_divergence_merges_realistic_fixtures` and `cli::migration::test_migrate_fixture_unrelated_divergence_requires_force`
- hottest suite: `beads-rs::integration` at about `221.5s`
- next hottest suite: `beads-git::beads_git` at about `65.7s`
- hottest test: `daemon::repl_e2e::repl_checkpoint_bootstrap_under_churn` at about `17.8s`
- hottest instrumented setup phase: `fixture.repl_rig.new` with `24.4s` total across the run

Current passing profiling artifact:

- `tmp/perf/tests-20260314-122418`

Current receipts from that run:

- fast profile exit code: `0`
- slow profile exit code: `0`
- profiling threads: `2` (`PROFILE_TEST_THREADS=2`)
- fast hottest suite: `beads-rs::integration` at about `118.0s`
- slow hottest suite: `beads-rs::integration` at about `147.2s`
- hottest fast test: `daemon::repl_e2e::repl_daemon_replicated_fsync_timeout_receipt` at about `4.4s`
- hottest slow test: `daemon::repl_e2e::repl_daemon_replicated_fsync_receipt` at about `31.1s`
- hottest common setup phase after consolidation: `fixture.repl_rig.new` at about `7.0s` fast / `6.9s` slow total across the run
- the prior flaky restart/tailnet tests now pass inside the profiling run: `repl_daemon_crash_restart_tailnet_roundtrip`, `repl_daemon_pathological_tailnet_roundtrip`, `test_crash_recovery_replays_wal`, and the lifecycle restart tests

March 19 follow-through note:

- `beads-rs-fblt.15` moved the deterministic restart/pathological tailnet proofs out of `crates/beads-rs/tests/integration/daemon/repl_e2e.rs` and into `crates/beads-daemon/tests/repl/e2e.rs` as `replication_rig_tailnet_restart_requires_fresh_handshakes` and `replication_rig_pathological_tailnet_recovers_without_external_proxies`
- the historical March 14 receipts below intentionally keep the old `daemon::repl_e2e::*` names because those were the profiled test IDs at the time; the assembly harness now keeps only thin external-process smoke/proxy coverage
- `beads-rs-81vj.4` made the built-in `bd daemon run` autostart path fail fast in test workflows by surfacing direct spawned-daemon early exits as immediate `DaemonUnavailable` errors; override programs remain launcher-compatible and wait for the socket instead
- the autostart fd sanitizer now closes inherited non-stdio descriptors with child-side primitives on the primary supported dev platforms: macOS uses `posix_spawn(... POSIX_SPAWN_CLOEXEC_DEFAULT ...)`, Linux uses the `close_range` syscall, and unsupported Unix targets still keep the child-side `_SC_OPEN_MAX` fallback sweep
- local proof receipts for that follow-through were `cargo test -p beads-surface ipc:: -- --nocapture` and `cargo test -p beads-rs --test integration cli::critical_path::test_auto_init_on_first_create -- --exact --nocapture`

Warm runner burn-in receipts:

- `tmp/perf/burnin-20260314-121518`
- all six iterations green
- fast tier wall time: `63.865s`, `42.515s`, `41.896s`
- slow tier wall time: `45.980s`, `42.504s`, `42.185s`
- acceptance targets met on warm runs: fast tier `<60s`, slow tier `<120s`

Stability notes behind the current receipts:

- `.config/nextest.toml` now caps the shared runner at `test-threads = 4`
- tailnet fault-injection tests are fenced into the `tailnet-fault-injection` nextest group so proxy-heavy `repl_e2e` cases do not oversubscribe the fast tier
- `scripts/profile-tests.sh` runs with `PROFILE_TEST_THREADS=2` by default for reproducible timing artifacts
- restart-tailnet readiness now requires fresh post-restart handshakes instead of trusting persisted pre-crash liveness rows
- test fixtures now share common wait/runtime helpers instead of repeating ad-hoc socket/meta/store polling
- git-sync crash-recovery coverage now reuses `BdRuntimeRepo` instead of ad hoc runtime tempdirs, so restarted daemons are owned by shared fixture cleanup instead of leaking past process-per-test teardown
- pathological tailnet coverage now uses test-local liveness budgets (`keepalive_ms = 250`, `dead_ms = 1500`) so the test still proves recovery under loss/reset churn without waiting on production-scale failover timers

Latest hostile-env runner receipts:

- `env BD_CONFIG_DIR=/tmp/codex-ambient-config BD_RUNTIME_DIR=/tmp/codex-ambient-runtime XDG_CONFIG_HOME=/tmp/codex-ambient-xdg GIT_DIR=/tmp/codex-ambient-git-dir GIT_WORK_TREE=/tmp/codex-ambient-git-worktree cargo xtest --no-fail-fast --status-level none --final-status-level slow` -> `42.099s`
- `env BD_CONFIG_DIR=/tmp/codex-ambient-config BD_RUNTIME_DIR=/tmp/codex-ambient-runtime XDG_CONFIG_HOME=/tmp/codex-ambient-xdg GIT_DIR=/tmp/codex-ambient-git-dir GIT_WORK_TREE=/tmp/codex-ambient-git-worktree cargo nextest run --profile slow --workspace --all-features --features slow-tests --no-fail-fast --status-level none --final-status-level slow` -> `41.689s`
- the remaining tailnet nextest flake was fixed by combining port-race removal in `ReplRig`/`tailnet_proxy` with the explicit tailnet nextest group

Latest clean-stack runner receipts:

- `tmp/perf/tests-20260314-155243-xtest-after-pathology-fix`
- `cargo xtest --no-fail-fast --status-level none --final-status-level slow` -> `51.123s` nextest / `53.14s` wall-clock, `1209 passed`, no leaked `bd` or `tailnet_proxy` processes after the run
- `tmp/perf/tests-20260314-155359-slow-profile-after-pathology-fix`
- `cargo nextest run --profile slow --workspace --all-features --features slow-tests --no-fail-fast --status-level none --final-status-level slow` -> `50.966s` nextest / `51.47s` wall-clock, `1209 passed`, no leaked `bd` or `tailnet_proxy` processes after the run
- `tmp/perf/tests-20260314-155227-pathological-burnin`
- `daemon::repl_e2e::repl_daemon_pathological_tailnet_roundtrip` burn-in: `5/5` passes, each at about `0.8s-0.9s`, with a clean process table after the sequence

Latest convergence + runner receipts:

- `tmp/perf/tests-20260314-175617-fast-profile-after-fingerprint-convergence`
- fast profile exit code: `0`
- hottest fast suite after the convergence/orchestration fixes: `beads-rs::integration` at about `165.4s`
- hottest fast instrumented phase after the convergence/orchestration fixes: `fixture.repl_rig.assert_converged` at about `3.7s` total across the run
- `tmp/perf/receipts-20260314-175752-post-fingerprint`
- `cargo xtest --no-fail-fast --status-level none --final-status-level slow` -> `50.063s` nextest / `50.61s` wall-clock, `1210 passed`
- `cargo nextest run --profile slow --workspace --all-features --features slow-tests --no-fail-fast --status-level none --final-status-level slow` -> `61.340s` nextest / `61.80s` wall-clock, `1210 passed`

Root-cause notes behind the latest receipts:

- explicit daemon ownership initially regressed the fast tier because teardown still polled PID liveness for owned children; exited-but-unreaped children stayed visible as alive, so `fixture.wait.process_exit` burned the full timeout on almost every REPL daemon shutdown
- that teardown tax was removed by reaping owned daemon children with `Child::try_wait()` instead of PID-only polling
- `ReplRig::assert_converged()` was also too heavy under concurrent REPL tests because it polled full `AdminStatus` payloads, including WAL/reporting work, on every retry; switching convergence polling to `AdminFingerprint` removed that hot-path contention
- after the convergence fix, fast/default profiles no longer need tailnet tests to monopolize the whole runner; the slow profile still reserves full runner ownership for tailnet fault-injection tests because the broader slow suite can otherwise starve `repl_daemon_pathological_tailnet_roundtrip`

Latest March 14 closeout receipts:

- `tmp/perf/tests-20260314-190941-xtest-after-fingerprint-timeout-fix`
- `cargo xtest --no-fail-fast --status-level none --final-status-level slow` -> `50.240s` nextest / `52.12s` wall-clock, `1210 passed`
- `tmp/perf/tests-20260314-191038-slow-after-fingerprint-timeout-fix`
- `cargo nextest run --profile slow --workspace --all-features --features slow-tests --no-fail-fast --status-level none --final-status-level slow` -> `61.245s` nextest / `61.76s` wall-clock, `1210 passed`
- `tmp/perf/repro-20260314-185615-pathological-neighbors` reproduced the fast-tier stall immediately (`1/1` timeout) with `repl_daemon_pathological_tailnet_roundtrip` plus its neighboring REPL tests
- `tmp/perf/repro-20260314-190331-pathological-roster-timeout-fix` proved the targeted roster/pathological pair green for `10/10` runs after the timeout-mapping fix
- `tmp/perf/repro-20260314-190804-pathological-neighbors-debug` kept the original 4-test interaction green for `10/10` runs after the same fix

Root-cause note for the latest receipts:

- `ReplRig::assert_converged()` already passed `ctx.read.wait_timeout_ms` into `AdminFingerprint`, but `ipc_timeout_for_request()` only honored that field for `Show` and `AdminStatus`
- under concurrent REPL churn, `AdminFingerprint` therefore fell back to the 7-second IPC timeout floor, repeatedly timed out inside the convergence loop, and let nextest kill `repl_daemon_pathological_tailnet_roundtrip` at 120 seconds even though the test passed in isolation
- wiring `AdminOp::Fingerprint` into the fixture timeout mapping restored the intended bounded wait semantics and brought the fast and slow runners back under target on the current stack

Tracker linkage:

- profiling/instrumentation bead: `beads-rs-fblt.1`
- newly discovered fast-tier blocker: `beads-rs-fblt.6`

Latest March 14 review-loop receipts:

- `tmp/perf/xtest-review-loop-20260314-235010`
- `cargo xtest --no-fail-fast --status-level none --final-status-level slow` -> `24.508s` nextest / `26.21s` wall-clock, `1102 passed`
- `tmp/perf/slow-review-loop-20260314-235042`
- `cargo nextest run --profile slow --workspace --all-features --features slow-tests --no-fail-fast --status-level none --final-status-level slow` -> `62.550s` nextest / `65.41s` wall-clock, `1225 passed`
- `daemon::repl_e2e::repl_daemon_store_discovery_roundtrip` targeted burn-in: `5/5` passes at about `2.5s` each, with a clean post-run `ps` audit for `bd daemon run` and `tailnet_proxy`

Root-cause note for the review-loop receipts:

- `cargo xtest` now stays on the intended fast surface by enabling `beads-rs/e2e-tests` explicitly instead of `--all-features`, so `slow-tests` do not silently leak back into the fast tier during review fixes
- `ReplRig` teardown still had one leak path after the earlier ownership work: once the socket/meta files disappeared, generic daemon discovery could miss split `runtime/` + sibling `data/` fixtures, leaving the owned child alive long enough for nextest to flag a leaky test
- the shared wait helper now force-kills and reaps owned daemon children through the `Child` handle when graceful shutdown misses its deadline, which closes that teardown hole without relying on late PID/lock-file discovery

Latest March 19 deterministic-harness migration receipts:

- `cargo xtest` -> `28.257s` nextest, `1128 passed`
- `cargo nextest run --profile slow --workspace --all-features --features slow-tests` -> `64.217s` nextest, `1250 passed`
- deterministic pathological-tailnet and crash/restart coverage now lives in `crates/beads-daemon/tests/repl/e2e.rs` as `replication_rig_pathological_tailnet_recovers_without_external_proxies` and `replication_rig_tailnet_restart_requires_fresh_handshakes`
- `crates/beads-rs/tests/integration/daemon/repl_e2e.rs` now keeps only external-process smoke, one thin tailnet crash/restart product proof (`repl_daemon_tailnet_crash_restart_roundtrip`), tailnet-proxy coverage, and other package-owned seams
