#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNS="${RUNS:-15}"
WARMUP="${WARMUP:-3}"
HOTPATH_ITERS="${HOTPATH_ITERS:-40}"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-$ROOT/tmp/perf/hotpaths-$STAMP}"
BD_BIN="${BD_BIN:-bd}"

mkdir -p "$OUT_DIR" "$OUT_DIR/runtime" "$OUT_DIR/data" "$OUT_DIR/logs"

export BD_RUNTIME_DIR="$OUT_DIR/runtime"
export BD_DATA_DIR="$OUT_DIR/data"
export BD_LOG_DIR="$OUT_DIR/logs"
export LOG="${LOG:-error}"
export RUST_LOG="$LOG"
export NO_COLOR=1

need_cmd() {
  local cmd="$1"
  if [[ "$cmd" == */* ]]; then
    if [[ ! -x "$cmd" ]]; then
      echo "missing required executable: $cmd" >&2
      exit 1
    fi
  else
    if ! command -v "$cmd" >/dev/null 2>&1; then
      echo "missing required command: $cmd" >&2
      exit 1
    fi
  fi
}

need_cmd "$BD_BIN"
need_cmd jq
need_cmd rg
need_cmd hyperfine
need_cmd git

bd_cmd() {
  "$BD_BIN" --repo "$ROOT" "$@"
}

BD_BIN_Q="$(printf "%q" "$BD_BIN")"
ROOT_Q="$(printf "%q" "$ROOT")"

GIT_BRANCH="$(git -C "$ROOT" symbolic-ref --short HEAD 2>/dev/null || git -C "$ROOT" describe --dirty --always)"
GIT_COMMIT="$(git -C "$ROOT" rev-parse HEAD)"
GIT_DIRTY="0"
if [[ -n "$(git -C "$ROOT" status --porcelain)" ]]; then
  GIT_DIRTY="1"
fi

UNAME_INFO="$(uname -a)"
RUSTC_VERSION="$(rustc --version 2>/dev/null || true)"
CARGO_VERSION="$(cargo --version 2>/dev/null || true)"
HYPERFINE_VERSION="$(hyperfine --version 2>/dev/null || true)"
CPU_INFO="$(
  {
    sysctl -n machdep.cpu.brand_string 2>/dev/null \
      || lscpu 2>/dev/null | awk -F: '/Model name/ {sub(/^[ \t]+/, "", $2); print $2; exit}'
  } || true
)"

GIT_STATUS_FILE="$OUT_DIR/git-status.txt"
git -C "$ROOT" status --short >"$GIT_STATUS_FILE"

cat > "$OUT_DIR/run_meta.txt" <<META
timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
repo=$ROOT
out_dir=$OUT_DIR
runs=$RUNS
warmup=$WARMUP
hotpath_iters=$HOTPATH_ITERS
bd_bin=$BD_BIN
bd_version=$("$BD_BIN" --version)
git_branch=$GIT_BRANCH
git_commit=$GIT_COMMIT
git_dirty=$GIT_DIRTY
rustc_version=${RUSTC_VERSION:-missing}
cargo_version=${CARGO_VERSION:-missing}
hyperfine_version=${HYPERFINE_VERSION:-missing}
cpu_model=${CPU_INFO:-unknown}
os_uname="$UNAME_INFO"
git_status_file=$GIT_STATUS_FILE
META

# Capture prime workflow guidance used for this run.
bd_cmd prime > "$OUT_DIR/bd_prime.md"

# Start dedicated daemon for this run.
DAEMON_STDOUT="$OUT_DIR/daemon.stdout.log"
(
  export LOG="${DAEMON_LOG_FILTER:-beads_rs=debug,metrics=info}"
  bd_cmd daemon run
) >"$DAEMON_STDOUT" 2>&1 &
DAEMON_PID=$!

cleanup() {
  if kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
    kill -INT "$DAEMON_PID" >/dev/null 2>&1 || true
    for _ in $(seq 1 30); do
      if ! kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
        break
      fi
      sleep 0.1
    done
    kill -9 "$DAEMON_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

READY_OK=0
for _ in $(seq 1 120); do
  if bd_cmd ready >/dev/null 2>&1; then
    READY_OK=1
    break
  fi
  sleep 0.1
done
if [[ "$READY_OK" -ne 1 ]]; then
  echo "daemon failed to become ready" >&2
  exit 1
fi

# Select hot-path IDs from real data where possible.
ISSUE_ID="$(bd_cmd ready --json | jq -r '.data.issues[0].id // empty')"
if [[ -z "$ISSUE_ID" ]]; then
  ISSUE_ID="$(bd_cmd list --status open --json | jq -r '.data[0].id // empty')"
fi
if [[ -z "$ISSUE_ID" ]]; then
  ISSUE_ID="$(bd_cmd --namespace tmp create "perf-seed-read" --description "hotpath benchmark seed issue" --type task --priority 2 --json | jq -r '.data.id')"
fi

EPIC_ID="$(bd_cmd list --status open --type epic --json | jq -r '.data[0].id // empty')"

# Seed mutation-path IDs in tmp namespace (isolated from core workflow state).
MUT_A="$(bd_cmd --namespace tmp create "perf-seed-mut-a" --description "hotpath benchmark mutation seed A" --type task --priority 2 --json | jq -r '.data.id')"
MUT_B="$(bd_cmd --namespace tmp create "perf-seed-mut-b" --description "hotpath benchmark mutation seed B" --type task --priority 2 --json | jq -r '.data.id')"

cat > "$OUT_DIR/workflows_under_test.txt" <<WF
from_bd_prime:
- bd ready
- bd list --status=in_progress
- bd blocked
- bd stale
- bd search auth
- bd show <id>
- bd dep tree <id>
- bd status
- bd epic status
- bd claim <id>
- bd close <id>
- bd create ...
- bd dep add A B
- bd label add <id> <label>

selected_ids:
- issue_id=$ISSUE_ID
- epic_id=${EPIC_ID:-<none-open>}
- mutation_a=$MUT_A
- mutation_b=$MUT_B
WF

# Warm-up critical paths.
bd_cmd ready >/dev/null 2>&1
bd_cmd show "$ISSUE_ID" >/dev/null 2>&1
bd_cmd show "$ISSUE_ID" --json >/dev/null 2>&1
bd_cmd dep tree "$ISSUE_ID" >/dev/null 2>&1
bd_cmd status >/dev/null 2>&1
bd_cmd epic status >/dev/null 2>&1

# Snapshot daemon log boundary for measured-phase parsing. We parse only lines
# written after this point to exclude startup/import noise.
MEASURE_LOG_START_LINE=$(wc -l < "$DAEMON_STDOUT" 2>/dev/null || echo 0)

# Read-heavy command latency benchmark.
hyperfine \
  --warmup "$WARMUP" \
  --runs "$RUNS" \
  --export-json "$OUT_DIR/hyperfine-read.json" \
  "${BD_BIN_Q} --repo ${ROOT_Q} ready >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} list --status open >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} blocked >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} stale >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} search auth >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} count --status open >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} show '${ISSUE_ID}' >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} show '${ISSUE_ID}' --json >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} dep tree '${ISSUE_ID}' >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} comments '${ISSUE_ID}' >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} status >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} epic status >/dev/null 2>&1"

# Mutation/lifecycle latency benchmark (tmp namespace).
hyperfine \
  --warmup "$WARMUP" \
  --runs "$RUNS" \
  --export-json "$OUT_DIR/hyperfine-write.json" \
  "${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp create 'perf-create' --description 'hotpath perf create benchmark' --type task --priority 2 >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp claim '${MUT_A}' >/dev/null 2>&1 && ${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp unclaim '${MUT_A}' >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp close '${MUT_A}' >/dev/null 2>&1 && ${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp reopen '${MUT_A}' >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp comment '${MUT_A}' 'perf-note' >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp label add '${MUT_A}' perf >/dev/null 2>&1 && ${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp label remove '${MUT_A}' perf >/dev/null 2>&1" \
  "${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp dep add '${MUT_A}' '${MUT_B}' >/dev/null 2>&1 && ${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp dep rm '${MUT_A}' '${MUT_B}' >/dev/null 2>&1" \
  "id=\$(${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp create 'perf-scenario' --description 'hotpath benchmark lifecycle scenario' --type task --priority 2 --json | jq -r '.data.id'); ${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp claim \"\$id\" >/dev/null 2>&1; ${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp show \"\$id\" >/dev/null 2>&1; ${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp comment \"\$id\" scenario-note >/dev/null 2>&1; ${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp close \"\$id\" >/dev/null 2>&1; ${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp reopen \"\$id\" >/dev/null 2>&1; ${BD_BIN_Q} --repo ${ROOT_Q} --namespace tmp delete \"\$id\" >/dev/null 2>&1"

# Stress representative workflows to enrich logs.
for _ in $(seq 1 "$HOTPATH_ITERS"); do
  bd_cmd ready >/dev/null 2>&1
  bd_cmd list --status open --limit 50 >/dev/null 2>&1
  bd_cmd show "$ISSUE_ID" >/dev/null 2>&1
  bd_cmd show "$ISSUE_ID" --json >/dev/null 2>&1
  bd_cmd dep tree "$ISSUE_ID" >/dev/null 2>&1
  bd_cmd comments "$ISSUE_ID" >/dev/null 2>&1
  bd_cmd status >/dev/null 2>&1
  bd_cmd epic status >/dev/null 2>&1
done

# Capture admin snapshots.
bd_cmd admin status --json > "$OUT_DIR/admin-status.json"
bd_cmd admin metrics --json > "$OUT_DIR/admin-metrics.json"

# Summaries: client-side latency (hyperfine).
jq -r '
  def msv($v): if $v == null then 0 else (($v * 1000) | round) end;
  def pct($arr; $p):
    if ($arr | length) == 0 then null
    else
      ($arr | sort) as $s
      | $s[((((($s | length) - 1) * $p)) | floor)]
    end;
  .results[] as $r
  | ($r.times // []) as $times
  | "\($r.command)\tmean_ms=\(msv($r.mean))\tmedian_ms=\(msv($r.median))\tp95_ms=\(msv(pct($times; 0.95)))\tp99_ms=\(msv(pct($times; 0.99)))\tstddev_ms=\(msv($r.stddev))\tmin_ms=\(msv($r.min))\tmax_ms=\(msv($r.max))"
' "$OUT_DIR/hyperfine-read.json" \
  > "$OUT_DIR/read-latency-summary.tsv"
jq -r '
  def msv($v): if $v == null then 0 else (($v * 1000) | round) end;
  def pct($arr; $p):
    if ($arr | length) == 0 then null
    else
      ($arr | sort) as $s
      | $s[((((($s | length) - 1) * $p)) | floor)]
    end;
  .results[] as $r
  | ($r.times // []) as $times
  | "\($r.command)\tmean_ms=\(msv($r.mean))\tmedian_ms=\(msv($r.median))\tp95_ms=\(msv(pct($times; 0.95)))\tp99_ms=\(msv(pct($times; 0.99)))\tstddev_ms=\(msv($r.stddev))\tmin_ms=\(msv($r.min))\tmax_ms=\(msv($r.max))"
' "$OUT_DIR/hyperfine-write.json" \
  > "$OUT_DIR/write-latency-summary.tsv"

# Summaries: daemon request fanout.
tail -n +"$((MEASURE_LOG_START_LINE + 1))" "$DAEMON_STDOUT" \
  | rg 'ipc_request request_type=' \
  | sed -E 's/.*request_type="([^"]+)".*/\1/' \
  | sort | uniq -c | sort -nr \
  > "$OUT_DIR/request-type-counts.txt"

# Approximate store identity resolution timing by active request span.
tail -n +"$((MEASURE_LOG_START_LINE + 1))" "$DAEMON_STDOUT" \
  | awk '
  /ipc_request request_type="/ {
    if (match($0, /request_type="[^"]+"/)) {
      req = substr($0, RSTART + 14, RLENGTH - 15)
    }
  }
  /store identity resolved/ {
    if (req != "" && $1 ~ /^[0-9]+ms$/) {
      v = $1
      sub(/ms$/, "", v)
      print req "\t" v
    }
  }
' > "$OUT_DIR/store-identity-latency.tsv"

awk -F '\t' '
  {
    k=$1
    v=$2 + 0
    c[k] += 1
    s[k] += v
    if (v > m[k]) m[k] = v
  }
  END {
    for (k in c) {
      printf "%s\tsamples=%d\tmean_ms=%.1f\tmax_ms=%d\n", k, c[k], s[k]/c[k], m[k]
    }
  }
' "$OUT_DIR/store-identity-latency.tsv" | sort > "$OUT_DIR/store-identity-latency-summary.tsv"

# Warning/error extraction.
tail -n +"$((MEASURE_LOG_START_LINE + 1))" "$DAEMON_STDOUT" \
  | rg -n 'WARN|ERROR|checkpoint git import failed|failed to lock file|background refresh failed|store lock already held|shutdown timed out waiting for syncs' \
  > "$OUT_DIR/warnings-errors.log" || true

# Metric summary available from admin metrics snapshot.
jq -r '.data.histograms[]? | ([.labels[]? | "\(.key)=\(.value)"] | sort | join(",")) as $labels | "\(.name)\tlabels=\($labels)\tcount=\(.count)\tp50=\(.p50)\tp95=\(.p95)\tmax=\(.max)"' "$OUT_DIR/admin-metrics.json" \
  | sort > "$OUT_DIR/admin-metrics-histograms.tsv"

jq -r '
  .data.histograms[]?
  | select(.name == "ipc_request_duration")
  | ([.labels[]? | select(.key == "request_type") | .value][0] // "") as $request_type
  | ([.labels[]? | select(.key == "outcome") | .value][0] // "") as $outcome
  | "\($request_type)\toutcome=\($outcome)\tcount=\(.count)\tp50=\(.p50)\tp95=\(.p95)\tmax=\(.max)"
' "$OUT_DIR/admin-metrics.json" \
  | sort > "$OUT_DIR/ipc-request-latency-summary.tsv"

jq -r '
  .data.histograms[]?
  | select(.name == "ipc_read_gate_wait_duration")
  | ([.labels[]? | select(.key == "request_type") | .value][0] // "") as $request_type
  | ([.labels[]? | select(.key == "outcome") | .value][0] // "") as $outcome
  | "\($request_type)\toutcome=\($outcome)\tcount=\(.count)\tp50=\(.p50)\tp95=\(.p95)\tmax=\(.max)"
' "$OUT_DIR/admin-metrics.json" \
  | sort > "$OUT_DIR/ipc-read-gate-wait-summary.tsv"

JSON_LOG="$(ls -t "$OUT_DIR"/logs/beads.log* 2>/dev/null | head -n 1 || true)"
if [[ -n "$JSON_LOG" ]]; then
  jq -r 'select(.span.name == "ipc_request") | .span.request_type // empty' "$JSON_LOG" \
    | sort | uniq -c | sort -nr > "$OUT_DIR/request-type-counts-from-jsonlog.txt"
  jq -r 'select(.fields.message == "store identity resolved") | [.timestamp, (.span.request_type // ""), (.fields.store_id // ""), (.fields.repo // "")] | @tsv' "$JSON_LOG" \
    > "$OUT_DIR/store-identity-events-from-jsonlog.tsv"
fi

# Human-readable run digest.
{
  echo "out_dir=$OUT_DIR"
  echo "issue_id=$ISSUE_ID"
  echo "epic_id=${EPIC_ID:-<none-open>}"
  echo ""
  echo "== Read Latency Summary =="
  cat "$OUT_DIR/read-latency-summary.tsv"
  echo ""
  echo "== Write Latency Summary =="
  cat "$OUT_DIR/write-latency-summary.tsv"
  echo ""
  echo "== Request Type Counts (daemon stdout) =="
  cat "$OUT_DIR/request-type-counts.txt"
  echo ""
  echo "== Store Identity Resolution Summary =="
  cat "$OUT_DIR/store-identity-latency-summary.tsv"
  echo ""
  echo "== IPC Request Latency Summary (admin metrics) =="
  cat "$OUT_DIR/ipc-request-latency-summary.tsv"
  echo ""
  echo "== Read Gate Wait Summary (admin metrics) =="
  cat "$OUT_DIR/ipc-read-gate-wait-summary.tsv"
  echo ""
  echo "== Warning/Error Lines =="
  wc -l "$OUT_DIR/warnings-errors.log" | awk '{print $1 " lines"}'
} > "$OUT_DIR/SUMMARY.txt"

ln -sfn "$OUT_DIR" "$ROOT/tmp/perf/hotpaths-latest"

echo "profiling complete"
echo "artifacts: $OUT_DIR"
