#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-$ROOT/tmp/perf/tests-$STAMP}"
FAST_PROFILE="${FAST_PROFILE:-fast}"
SLOW_PROFILE="${SLOW_PROFILE:-slow}"
RUN_FAST="${RUN_FAST:-1}"
RUN_SLOW="${RUN_SLOW:-1}"
FAIL_ON_RUN_ERROR="${FAIL_ON_RUN_ERROR:-0}"

mkdir -p "$OUT_DIR"
ln -snf "$(basename "$OUT_DIR")" "$ROOT/tmp/perf/tests-latest"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need_cmd cargo
need_cmd jq
need_cmd rg

cat >"$OUT_DIR/run_meta.txt" <<META
timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
repo=$ROOT
out_dir=$OUT_DIR
fast_profile=$FAST_PROFILE
slow_profile=$SLOW_PROFILE
run_fast=$RUN_FAST
run_slow=$RUN_SLOW
META

list_tests() {
  local label="$1"
  shift
  cargo nextest list "$@" >"$OUT_DIR/${label}-list.txt"
}

run_profile() {
  local label="$1"
  shift
  local phase_dir="$OUT_DIR/${label}-phases"
  mkdir -p "$phase_dir"

  local raw_log="$OUT_DIR/${label}-raw.log"
  set +e
  (
    cd "$ROOT"
    NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1 \
      BD_TEST_TIMING_DIR="$phase_dir" \
      "$@"
  ) | tee "$raw_log"
  local run_status="${PIPESTATUS[0]}"
  set -e

  printf '%s\n' "$run_status" >"$OUT_DIR/${label}-exit-code.txt"
  rg '^\{' "$raw_log" >"$OUT_DIR/${label}-messages.jsonl" || true
  return 0
}

render_top_tests() {
  local input="$1"
  local output="$2"
  jq -r -s '
    [ .[]
      | select(.type == "test" and (.event == "ok" or .event == "failed"))
      | {
          name,
          event,
          exec_time_ms: (((.exec_time // 0) * 1000) | round)
        }
    ]
    | sort_by(.exec_time_ms)
    | reverse
    | (["test", "event", "exec_time_ms"] | @tsv),
      (.[] | [.name, .event, (.exec_time_ms | tostring)] | @tsv)
  ' "$input" >"$output"
}

render_top_suites() {
  local input="$1"
  local output="$2"
  jq -r -s '
    [ .[]
      | select(.type == "suite" and (.event == "ok" or .event == "failed"))
      | {
          suite: ((.nextest.crate // "unknown") + "::" + (.nextest.test_binary // "unknown")),
          event,
          exec_time_ms: (((.exec_time // 0) * 1000) | round)
        }
    ]
    | sort_by(.exec_time_ms)
    | reverse
    | (["suite", "event", "exec_time_ms"] | @tsv),
      (.[] | [.suite, .event, (.exec_time_ms | tostring)] | @tsv)
  ' "$input" >"$output"
}

render_phase_summary() {
  local input_dir="$1"
  local output="$2"
  local phase_files=()
  while IFS= read -r file; do
    phase_files+=("$file")
  done < <(find "$input_dir" -type f -name '*.jsonl' | sort)

  if [[ "${#phase_files[@]}" -eq 0 ]]; then
    printf 'phase\tcount\ttotal_ms\tavg_ms\tmax_ms\n' >"$output"
    return
  fi

  jq -r -s '
    group_by(.phase)
    | map({
        phase: .[0].phase,
        count: length,
        total_ms: (map(.elapsed_ms) | add),
        avg_ms: ((map(.elapsed_ms) | add) / length | round),
        max_ms: (map(.elapsed_ms) | max)
      })
    | sort_by(.total_ms)
    | reverse
    | (["phase", "count", "total_ms", "avg_ms", "max_ms"] | @tsv),
      (.[] | [.phase, (.count | tostring), (.total_ms | tostring), (.avg_ms | tostring), (.max_ms | tostring)] | @tsv)
  ' "${phase_files[@]}" >"$output"
}

if [[ "$RUN_FAST" == "1" ]]; then
  list_tests fast --workspace --all-features
  run_profile fast cargo xtest --no-fail-fast --status-level all --final-status-level all --message-format libtest-json-plus
  render_top_tests "$OUT_DIR/fast-messages.jsonl" "$OUT_DIR/fast-top-tests.tsv"
  render_top_suites "$OUT_DIR/fast-messages.jsonl" "$OUT_DIR/fast-top-suites.tsv"
  render_phase_summary "$OUT_DIR/fast-phases" "$OUT_DIR/fast-phase-summary.tsv"
fi

if [[ "$RUN_SLOW" == "1" ]]; then
  list_tests slow --workspace --all-features --features slow-tests
  run_profile slow cargo nextest run --profile "$SLOW_PROFILE" --workspace --all-features --features slow-tests --no-fail-fast --status-level all --final-status-level all --message-format libtest-json-plus
  render_top_tests "$OUT_DIR/slow-messages.jsonl" "$OUT_DIR/slow-top-tests.tsv"
  render_top_suites "$OUT_DIR/slow-messages.jsonl" "$OUT_DIR/slow-top-suites.tsv"
  render_phase_summary "$OUT_DIR/slow-phases" "$OUT_DIR/slow-phase-summary.tsv"
fi

{
  echo "test profiling artifacts: $OUT_DIR"
  if [[ "$RUN_FAST" == "1" ]]; then
    echo "fast_exit_code=$(cat "$OUT_DIR/fast-exit-code.txt")"
  fi
  if [[ "$RUN_SLOW" == "1" ]]; then
    echo "slow_exit_code=$(cat "$OUT_DIR/slow-exit-code.txt")"
  fi
  if [[ "$RUN_FAST" == "1" ]]; then
    echo
    echo "fast top suites:"
    sed -n '1,15p' "$OUT_DIR/fast-top-suites.tsv"
    echo
    echo "fast top phases:"
    sed -n '1,15p' "$OUT_DIR/fast-phase-summary.tsv"
  fi
  if [[ "$RUN_SLOW" == "1" ]]; then
    echo
    echo "slow top suites:"
    sed -n '1,15p' "$OUT_DIR/slow-top-suites.tsv"
    echo
    echo "slow top phases:"
    sed -n '1,15p' "$OUT_DIR/slow-phase-summary.tsv"
  fi
} >"$OUT_DIR/SUMMARY.txt"

cat "$OUT_DIR/SUMMARY.txt"

if [[ "$FAIL_ON_RUN_ERROR" == "1" ]]; then
  if [[ "$RUN_FAST" == "1" ]] && [[ "$(cat "$OUT_DIR/fast-exit-code.txt")" != "0" ]]; then
    exit 1
  fi
  if [[ "$RUN_SLOW" == "1" ]] && [[ "$(cat "$OUT_DIR/slow-exit-code.txt")" != "0" ]]; then
    exit 1
  fi
fi
