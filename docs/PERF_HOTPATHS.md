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
- fast entrypoint: `cargo xtest`
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

Latest hostile-env runner receipts:

- `env BD_CONFIG_DIR=/tmp/codex-ambient-config BD_RUNTIME_DIR=/tmp/codex-ambient-runtime XDG_CONFIG_HOME=/tmp/codex-ambient-xdg GIT_DIR=/tmp/codex-ambient-git-dir GIT_WORK_TREE=/tmp/codex-ambient-git-worktree cargo xtest --no-fail-fast --status-level none --final-status-level slow` -> `42.099s`
- `env BD_CONFIG_DIR=/tmp/codex-ambient-config BD_RUNTIME_DIR=/tmp/codex-ambient-runtime XDG_CONFIG_HOME=/tmp/codex-ambient-xdg GIT_DIR=/tmp/codex-ambient-git-dir GIT_WORK_TREE=/tmp/codex-ambient-git-worktree cargo nextest run --profile slow --workspace --all-features --features slow-tests --no-fail-fast --status-level none --final-status-level slow` -> `41.689s`
- the remaining tailnet nextest flake was fixed by combining port-race removal in `ReplRig`/`tailnet_proxy` with the explicit tailnet nextest group

Tracker linkage:

- profiling/instrumentation bead: `beads-rs-fblt.1`
- newly discovered fast-tier blocker: `beads-rs-fblt.6`
