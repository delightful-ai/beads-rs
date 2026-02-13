#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <baseline-artifacts-dir> <candidate-artifacts-dir>" >&2
  exit 1
fi

BASE="$1"
NEW="$2"

READ_THRESHOLD_PCT="${READ_THRESHOLD_PCT:-25}"
WRITE_THRESHOLD_PCT="${WRITE_THRESHOLD_PCT:-35}"
READ_P95_THRESHOLD_PCT="${READ_P95_THRESHOLD_PCT:-50}"
WRITE_P95_THRESHOLD_PCT="${WRITE_P95_THRESHOLD_PCT:-65}"

need_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "missing file: $path" >&2
    exit 1
  fi
}

need_file "$BASE/hyperfine-read.json"
need_file "$NEW/hyperfine-read.json"
need_file "$BASE/hyperfine-write.json"
need_file "$NEW/hyperfine-write.json"

base_read="$(mktemp)"
new_read="$(mktemp)"
base_write="$(mktemp)"
new_write="$(mktemp)"
trap 'rm -f "$base_read" "$new_read" "$base_write" "$new_write"' EXIT

jq -r '
    def msv($v): if $v == null then 0 else (($v * 1000) | round) end;
    def pct($arr; $p):
      if ($arr | length) == 0 then null
      else
        ($arr | sort) as $s
        | $s[((((($s | length) - 1) * $p)) | floor)]
      end;
    .results[] as $r
    | [($r.command | gsub(" >/dev/null( 2>&1)?$"; "")), msv($r.mean), msv(pct(($r.times // []); 0.95))] | @tsv
  ' "$BASE/hyperfine-read.json" \
  | awk -F'\t' '
      function key(cmd) {
        if (cmd ~ / ready$/) return "ready";
        if (cmd ~ / list --status open$/) return "list_open";
        if (cmd ~ / epic status$/) return "";
        if (cmd ~ / show .* --json$/) return "show_json";
        if (cmd ~ / show .*$/) return "show";
        if (cmd ~ / status$/) return "status";
        return "";
      }
      {
        k = key($1);
        if (k != "") print k "\t" $2 "\t" $3;
      }
    ' \
  | sort > "$base_read"

jq -r '
    def msv($v): if $v == null then 0 else (($v * 1000) | round) end;
    def pct($arr; $p):
      if ($arr | length) == 0 then null
      else
        ($arr | sort) as $s
        | $s[((((($s | length) - 1) * $p)) | floor)]
      end;
    .results[] as $r
    | [($r.command | gsub(" >/dev/null( 2>&1)?$"; "")), msv($r.mean), msv(pct(($r.times // []); 0.95))] | @tsv
  ' "$NEW/hyperfine-read.json" \
  | awk -F'\t' '
      function key(cmd) {
        if (cmd ~ / ready$/) return "ready";
        if (cmd ~ / list --status open$/) return "list_open";
        if (cmd ~ / epic status$/) return "";
        if (cmd ~ / show .* --json$/) return "show_json";
        if (cmd ~ / show .*$/) return "show";
        if (cmd ~ / status$/) return "status";
        return "";
      }
      {
        k = key($1);
        if (k != "") print k "\t" $2 "\t" $3;
      }
    ' \
  | sort > "$new_read"

jq -r '
  def msv($v): if $v == null then 0 else (($v * 1000) | round) end;
  def pct($arr; $p):
    if ($arr | length) == 0 then null
    else
      ($arr | sort) as $s
      | $s[((((($s | length) - 1) * $p)) | floor)]
    end;
  .results | to_entries[] | [(.key + 1), msv(.value.mean), msv(pct((.value.times // []); 0.95))] | @tsv
' "$BASE/hyperfine-write.json" \
  | sort > "$base_write"
jq -r '
  def msv($v): if $v == null then 0 else (($v * 1000) | round) end;
  def pct($arr; $p):
    if ($arr | length) == 0 then null
    else
      ($arr | sort) as $s
      | $s[((((($s | length) - 1) * $p)) | floor)]
    end;
  .results | to_entries[] | [(.key + 1), msv(.value.mean), msv(pct((.value.times // []); 0.95))] | @tsv
' "$NEW/hyperfine-write.json" \
  | sort > "$new_write"

echo "baseline=$BASE"
echo "candidate=$NEW"
echo "read_threshold_pct=$READ_THRESHOLD_PCT"
echo "write_threshold_pct=$WRITE_THRESHOLD_PCT"
echo "read_p95_threshold_pct=$READ_P95_THRESHOLD_PCT"
echo "write_p95_threshold_pct=$WRITE_P95_THRESHOLD_PCT"
echo

status=0

echo "== Read Guardrail Check =="
if ! awk -F'\t' -v threshold="$READ_THRESHOLD_PCT" '
  NR==FNR { base[$1]=$2+0; next }
  {
    key=$1
    now=$2+0
    if (!(key in base)) next
    old=base[key]
    allowed=old * (1 + threshold / 100.0)
    delta=now-old
    pct=(old==0)?0:(100.0*delta/old)
    if (now > allowed) {
      printf "FAIL %s\tbase=%d\tnew=%d\tdelta=%+d\tpct=%+.1f%%\tlimit=+%.1f%%\n", key, old, now, delta, pct, threshold
      fail=1
    } else {
      printf "OK   %s\tbase=%d\tnew=%d\tdelta=%+d\tpct=%+.1f%%\tlimit=+%.1f%%\n", key, old, now, delta, pct, threshold
    }
  }
  END { exit fail ? 1 : 0 }
' "$base_read" "$new_read"; then
  status=1
fi

echo
echo "== Read P95 Guardrail Check =="
if ! awk -F'\t' -v threshold="$READ_P95_THRESHOLD_PCT" '
  NR==FNR { base[$1]=$3+0; next }
  {
    key=$1
    now=$3+0
    if (!(key in base)) next
    old=base[key]
    allowed=old * (1 + threshold / 100.0)
    delta=now-old
    pct=(old==0)?0:(100.0*delta/old)
    if (now > allowed) {
      printf "FAIL %s\tbase=%d\tnew=%d\tdelta=%+d\tpct=%+.1f%%\tlimit=+%.1f%%\n", key, old, now, delta, pct, threshold
      fail=1
    } else {
      printf "OK   %s\tbase=%d\tnew=%d\tdelta=%+d\tpct=%+.1f%%\tlimit=+%.1f%%\n", key, old, now, delta, pct, threshold
    }
  }
  END { exit fail ? 1 : 0 }
' "$base_read" "$new_read"; then
  status=1
fi

echo
echo "== Write Guardrail Check =="
if ! awk -F'\t' -v threshold="$WRITE_THRESHOLD_PCT" '
  function workflow_name(idx) {
    if (idx == 1) return "create";
    if (idx == 2) return "claim_unclaim";
    if (idx == 3) return "close_reopen";
    if (idx == 4) return "comment";
    if (idx == 5) return "label_add_remove";
    if (idx == 6) return "dep_add_rm";
    if (idx == 7) return "lifecycle_scenario";
    return "workflow_" idx;
  }
  NR==FNR { base[$1]=$2+0; next }
  {
    idx=$1+0
    now=$2+0
    if (!(idx in base)) next
    old=base[idx]
    allowed=old * (1 + threshold / 100.0)
    delta=now-old
    pct=(old==0)?0:(100.0*delta/old)
    name=workflow_name(idx)
    if (now > allowed) {
      printf "FAIL %s\tbase=%d\tnew=%d\tdelta=%+d\tpct=%+.1f%%\tlimit=+%.1f%%\n", name, old, now, delta, pct, threshold
      fail=1
    } else {
      printf "OK   %s\tbase=%d\tnew=%d\tdelta=%+d\tpct=%+.1f%%\tlimit=+%.1f%%\n", name, old, now, delta, pct, threshold
    }
  }
  END { exit fail ? 1 : 0 }
' "$base_write" "$new_write"; then
  status=1
fi

echo
echo "== Write P95 Guardrail Check =="
if ! awk -F'\t' -v threshold="$WRITE_P95_THRESHOLD_PCT" '
  function workflow_name(idx) {
    if (idx == 1) return "create";
    if (idx == 2) return "claim_unclaim";
    if (idx == 3) return "close_reopen";
    if (idx == 4) return "comment";
    if (idx == 5) return "label_add_remove";
    if (idx == 6) return "dep_add_rm";
    if (idx == 7) return "lifecycle_scenario";
    return "workflow_" idx;
  }
  NR==FNR { base[$1]=$3+0; next }
  {
    idx=$1+0
    now=$3+0
    if (!(idx in base)) next
    old=base[idx]
    allowed=old * (1 + threshold / 100.0)
    delta=now-old
    pct=(old==0)?0:(100.0*delta/old)
    name=workflow_name(idx)
    if (now > allowed) {
      printf "FAIL %s\tbase=%d\tnew=%d\tdelta=%+d\tpct=%+.1f%%\tlimit=+%.1f%%\n", name, old, now, delta, pct, threshold
      fail=1
    } else {
      printf "OK   %s\tbase=%d\tnew=%d\tdelta=%+d\tpct=%+.1f%%\tlimit=+%.1f%%\n", name, old, now, delta, pct, threshold
    }
  }
  END { exit fail ? 1 : 0 }
' "$base_write" "$new_write"; then
  status=1
fi

exit "$status"
