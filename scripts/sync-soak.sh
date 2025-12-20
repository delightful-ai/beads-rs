#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BD_BIN="${BD_BIN:-$ROOT/target/debug/bd}"

if [[ ! -x "$BD_BIN" ]]; then
  echo "building bd..." >&2
  (cd "$ROOT" && cargo build --bin bd)
fi

tmp="$(mktemp -d)"
if [[ "${KEEP:-}" != "1" ]]; then
  trap 'rm -rf "$tmp"' EXIT
else
  echo "KEEP=1 set; temp dir: $tmp"
fi

remote="$tmp/remote.git"
git init --bare "$remote" >/dev/null

clone_a="$tmp/a"
clone_b="$tmp/b"
git clone "$remote" "$clone_a" >/dev/null
git clone "$remote" "$clone_b" >/dev/null

xdg_a="$tmp/xdg-a"
xdg_b="$tmp/xdg-b"
mkdir -p "$xdg_a" "$xdg_b"

run_a() { (cd "$clone_a" && XDG_RUNTIME_DIR="$xdg_a" "$BD_BIN" "$@"); }
run_b() { (cd "$clone_b" && XDG_RUNTIME_DIR="$xdg_b" "$BD_BIN" "$@"); }

run_a init >/dev/null
run_b init >/dev/null

run_a create "A1" --type=task >/dev/null
run_a sync >/dev/null
base_oid="$(git --git-dir="$remote" rev-parse refs/heads/beads/store)"

run_b create "B1" --type=task >/dev/null
run_b sync >/dev/null

# Force-push remote back to the earlier commit.
git --git-dir="$remote" update-ref refs/heads/beads/store "$base_oid"

run_b create "B2" --type=task >/dev/null
run_b sync >/dev/null

# Crash recovery: create a bead, kill daemon, then ensure it survives restart.
a_crash_json="$(run_a create "A-crash" --json)"
a_crash_id="$(python3 -c 'import json,sys; data=json.load(sys.stdin); print(data["data"]["id"])' <<<"$a_crash_json")"

meta="$xdg_a/beads/daemon.meta.json"
for _ in {1..20}; do
  [[ -f "$meta" ]] && break
  sleep 0.05
done

pid="$(python3 -c 'import json,sys,pathlib; data=json.loads(pathlib.Path(sys.argv[1]).read_text()); print(data["pid"])' "$meta")"
kill -9 "$pid" >/dev/null 2>&1 || true

run_a sync >/dev/null
run_a show "$a_crash_id" >/dev/null

echo "sync soak complete (temp: $tmp)"
