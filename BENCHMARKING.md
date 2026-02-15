# Benchmarking

This repo has a first-class hotpath benchmark harness for CLI + daemon workflows.

## Quick Start

Build and run a benchmark:

```bash
just bench-hotpaths
```

Recommended explicit binary (avoids benchmarking an older installed `bd`):

```bash
cargo build --bin bd
BD_BIN=./target/debug/bd just bench-hotpaths
```

Tune runs:

```bash
RUNS=25 WARMUP=5 HOTPATH_ITERS=40 BD_BIN=./target/debug/bd just bench-hotpaths
```

Artifacts are written under:

```text
tmp/perf/hotpaths-<timestamp>/
tmp/perf/hotpaths-latest -> symlink to latest run
```

## Compare Runs

```bash
just bench-compare tmp/perf/hotpaths-<baseline> tmp/perf/hotpaths-<candidate>
```

Regression guard:

```bash
just bench-guard tmp/perf/hotpaths-<baseline> tmp/perf/hotpaths-<candidate>
```

Threshold overrides:

```bash
READ_THRESHOLD_PCT=20 WRITE_THRESHOLD_PCT=30 just bench-guard <base> <new>
```

## What Gets Measured

- Read hotpaths (`bd ready`, `bd list`, `bd show`, `bd dep tree`, `bd status`, etc.)
- Write/lifecycle hotpaths (`bd create`, `bd claim/unclaim`, `bd close/reopen`, comments, deps, labels)
- Daemon request fanout and warnings
- Admin metric snapshots, including per-request latency histograms

## Key Artifacts

- `SUMMARY.txt`: human-readable run digest
- `hyperfine-read.json`, `hyperfine-write.json`: raw latency samples
- `read-latency-summary.tsv`, `write-latency-summary.tsv`: mean/p95/p99 by command
- `ipc-request-latency-summary.tsv`: daemon-side request latency histograms
- `warnings-errors.log`: warnings/errors seen during measured phase
- `daemon.stdout.log`: daemon log for full trace-level diagnosis

## Workflow Pattern

1. Capture baseline on current `main`.
2. Apply change.
3. Re-run benchmark with same `BD_BIN`, `RUNS`, and `WARMUP`.
4. Run `just bench-compare` and `just bench-guard`.
5. Include `SUMMARY.txt` deltas in the PR/bead notes.
