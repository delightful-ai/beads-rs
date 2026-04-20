#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BD_BIN="${BD_BIN:-$ROOT/target/debug/bd}"
BD_HTTP_BIN="${BD_HTTP_BIN:-$ROOT/target/debug/bd-http}"
BOARD_APP="${BOARD_APP:-$ROOT/scripts/tracker_board.py}"

if [[ "${SKIP_BUILD:-}" != "1" ]]; then
  echo "building fresh bd + bd-http..." >&2
  (cd "$ROOT" && cargo build -p beads-rs --bin bd -p beads-http --bin bd-http >/dev/null)
elif [[ ! -x "$BD_BIN" || ! -x "$BD_HTTP_BIN" ]]; then
  echo "SKIP_BUILD=1 but bd/bd-http binaries are missing" >&2
  exit 1
fi

if [[ ! -x "$BOARD_APP" ]]; then
  chmod +x "$BOARD_APP"
fi

tmp="$(mktemp -d /tmp/beads-tracker-http-e2e.XXXXXX)"
http_pid=""

cleanup() {
  if [[ -n "$http_pid" ]]; then
    kill "$http_pid" >/dev/null 2>&1 || true
    wait "$http_pid" 2>/dev/null || true
  fi
  if [[ "${KEEP:-}" != "1" ]]; then
    rm -rf "$tmp"
  fi
}

trap cleanup EXIT

if [[ "${KEEP:-}" != "1" ]]; then
  :
else
  echo "KEEP=1 set; temp dir: $tmp"
fi

origin="$tmp/origin.git"
repo="$tmp/repo"
runtime="$tmp/runtime"
data="$tmp/data"
config="$tmp/config"
mkdir -p "$repo" "$runtime" "$data" "$config"

git init --bare "$origin" >/dev/null
git init "$repo" >/dev/null
git -C "$repo" config user.name "Codex Tracker E2E"
git -C "$repo" config user.email "codex@example.com"
git -C "$repo" remote add origin "$origin"
printf 'seed\n' > "$repo/README.md"
git -C "$repo" add README.md
git -C "$repo" commit -m "init" >/dev/null
git -C "$repo" push -u origin HEAD >/dev/null

run_env() {
  env \
    BD_RUNTIME_DIR="$runtime" \
    BD_DATA_DIR="$data" \
    BD_CONFIG_DIR="$config" \
    "$@"
}

run_env "$BD_BIN" init --repo "$repo" >/dev/null

port="$(
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
endpoint="http://127.0.0.1:${port}/rpc"

run_env env BD_HTTP_ADDR="127.0.0.1:${port}" "$BD_HTTP_BIN" >/dev/null 2>&1 &
http_pid="$!"

for _ in {1..50}; do
  if curl -fsS "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
curl -fsS "http://127.0.0.1:${port}/healthz" >/dev/null

todo_id="$(python3 "$BOARD_APP" --repo "$repo" --endpoint "$endpoint" create "Board demo issue" --description "seed tracker issue")"
blocker_id="$(python3 "$BOARD_APP" --repo "$repo" --endpoint "$endpoint" create "Backend sync lane" --description "Long-running backend work" --priority 1)"
review_id="$(python3 "$BOARD_APP" --repo "$repo" --endpoint "$endpoint" create "API review pass" --description "Waiting for review")"
blocked_id="$(python3 "$BOARD_APP" --repo "$repo" --endpoint "$endpoint" create "Blocked UI polish" --description "Depends on backend sync")"

python3 "$BOARD_APP" --repo "$repo" --endpoint "$endpoint" depend "$blocked_id" "$blocker_id" >/dev/null
python3 "$BOARD_APP" --repo "$repo" --endpoint "$endpoint" move "$blocker_id" "In Progress" >/dev/null
python3 "$BOARD_APP" --repo "$repo" --endpoint "$endpoint" move "$review_id" "Human Review" >/dev/null
python3 "$BOARD_APP" --repo "$repo" --endpoint "$endpoint" comment "$review_id" "## Codex Workpad"$'\n'"- review pending" >/dev/null
python3 "$BOARD_APP" --repo "$repo" --endpoint "$endpoint" move "$todo_id" "In Progress" >/dev/null

board_json="$tmp/tracker.json"
python3 "$BOARD_APP" --repo "$repo" --endpoint "$endpoint" list --json > "$board_json"

python3 - "$runtime" "$board_json" "$todo_id" "$blocker_id" "$review_id" "$blocked_id" <<'PY'
import json
import pathlib
import sys

runtime, board_json, todo_id, blocker_id, review_id, blocked_id = sys.argv[1:]
issues = json.loads(pathlib.Path(board_json).read_text())
by_id = {issue["id"]: issue for issue in issues}

socket_path = pathlib.Path(runtime) / "beads" / "daemon.sock"
meta_path = pathlib.Path(runtime) / "beads" / "daemon.meta.json"
assert socket_path.exists(), f"expected autostarted daemon socket at {socket_path}"
assert meta_path.exists(), f"expected autostarted daemon metadata at {meta_path}"

assert by_id[todo_id]["status"] == "In Progress", by_id[todo_id]
assert by_id[review_id]["status"] == "Human Review", by_id[review_id]
assert by_id[review_id]["labels"] == [], by_id[review_id]
assert by_id[blocked_id]["blocked_by"][0]["id"] == blocker_id, by_id[blocked_id]
assert by_id[blocked_id]["blocked_by"][0]["status"] == "In Progress", by_id[blocked_id]
PY

python3 "$BOARD_APP" --repo "$repo" --endpoint "$endpoint" list
echo
echo "tracker HTTP e2e complete"
echo "repo: $repo"
echo "endpoint: $endpoint"
