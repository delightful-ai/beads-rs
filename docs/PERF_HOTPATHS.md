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
