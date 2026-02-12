#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <baseline-artifacts-dir> <candidate-artifacts-dir>" >&2
  exit 1
fi

BASE="$1"
NEW="$2"

need_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "missing file: $path" >&2
    exit 1
  fi
}

need_file "$BASE/hyperfine-read.json"
need_file "$NEW/hyperfine-read.json"

base_tmp="$(mktemp)"
new_tmp="$(mktemp)"
base_write_tmp="$(mktemp)"
new_write_tmp="$(mktemp)"
trap 'rm -f "$base_tmp" "$new_tmp" "$base_write_tmp" "$new_write_tmp"' EXIT

jq -r '.results[] | [(.command | gsub(" >/dev/null( 2>&1)?$"; "")), ((.mean * 1000) | round)] | @tsv' "$BASE/hyperfine-read.json" | sort > "$base_tmp"
jq -r '.results[] | [(.command | gsub(" >/dev/null( 2>&1)?$"; "")), ((.mean * 1000) | round)] | @tsv' "$NEW/hyperfine-read.json" | sort > "$new_tmp"

echo "baseline=$BASE"
echo "candidate=$NEW"
echo

echo "== Read Latency Delta (ms) =="
awk -F'\t' '
  NR==FNR { base[$1]=$2; next }
  {
    cmd=$1
    now=$2+0
    old=(cmd in base)?base[cmd]+0:-1
    if (old < 0) next
    delta=now-old
    pct=(old==0)?0:(100.0*delta/old)
    printf "%s\tbase=%d\tnew=%d\tdelta=%+d\tpct=%+.1f%%\n", cmd, old, now, delta, pct
  }
' "$base_tmp" "$new_tmp" | sort

if [[ -f "$BASE/hyperfine-write.json" && -f "$NEW/hyperfine-write.json" ]]; then
  jq -r '.results | to_entries[] | [.key, (.value.command | gsub(" >/dev/null( 2>&1)?$"; "")), ((.value.mean * 1000) | round)] | @tsv' "$BASE/hyperfine-write.json" > "$base_write_tmp"
  jq -r '.results | to_entries[] | [.key, (.value.command | gsub(" >/dev/null( 2>&1)?$"; "")), ((.value.mean * 1000) | round)] | @tsv' "$NEW/hyperfine-write.json" > "$new_write_tmp"

  echo
  echo "== Write Latency Delta (ms) =="
  awk -F'\t' '
    NR==FNR { base[$1]=$3; next }
    {
      idx=$1
      cmd=$2
      now=$3+0
      old=(idx in base)?base[idx]+0:-1
      if (old < 0) next
      delta=now-old
      pct=(old==0)?0:(100.0*delta/old)
      printf "%s\tbase=%d\tnew=%d\tdelta=%+d\tpct=%+.1f%%\n", cmd, old, now, delta, pct
    }
  ' "$base_write_tmp" "$new_write_tmp"
fi

if [[ -f "$BASE/store-identity-latency-summary.tsv" && -f "$NEW/store-identity-latency-summary.tsv" ]]; then
  echo
  echo "== Store Identity Mean Delta (ms) =="
  awk -F'\t' '
    function mean(field,    v) { sub(/^mean_ms=/, "", field); return field + 0 }
    NR==FNR {
      key=$1
      base_mean[key]=mean($3)
      next
    }
    {
      key=$1
      if (!(key in base_mean)) next
      new_mean=mean($3)
      delta=new_mean-base_mean[key]
      pct=(base_mean[key]==0)?0:(100.0*delta/base_mean[key])
      printf "%s\tbase=%.1f\tnew=%.1f\tdelta=%+.1f\tpct=%+.1f%%\n", key, base_mean[key], new_mean, delta, pct
    }
  ' "$BASE/store-identity-latency-summary.tsv" "$NEW/store-identity-latency-summary.tsv" | sort
fi

if [[ -f "$NEW/admin-metrics.json" ]]; then
  echo
  echo "== IPC Request Histograms (candidate) =="
  jq -r '.data.histograms[]
    | select(.name == "ipc_request_duration")
    | ([.labels[]? | select(.key=="request_type") | .value][0]) as $req
    | ([.labels[]? | select(.key=="outcome") | .value][0]) as $out
    | [$req, $out, .count, .p50, .p95, .max] | @tsv' "$NEW/admin-metrics.json" \
    | sort
fi
