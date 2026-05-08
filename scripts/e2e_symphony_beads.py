#!/usr/bin/env python3
"""Disposable end-to-end proof for Symphony running against a beads backend."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
import urllib.error
import urllib.request


ROOT = pathlib.Path(__file__).resolve().parent.parent
BD_BIN = ROOT / "target" / "debug" / "bd"
BD_HTTP_BIN = ROOT / "target" / "debug" / "bd-http"
BOARD_APP = ROOT / "scripts" / "tracker_board.py"
DEFAULT_SYMPHONY_ROOT = pathlib.Path("/Users/darin/vendor/github.com/openai/symphony/elixir")
DEFAULT_WORKFLOW_PROMPT = "You are an agent for this repository."
LIFECYCLE_PROOF_FILE = "LIFECYCLE_PROOF.txt"


class HarnessError(RuntimeError):
    """Raised when the disposable live proof fails."""


def render_workflow(
    *,
    repo_path: str,
    rpc_endpoint: str,
    workspace_root: str,
    prompt: str = DEFAULT_WORKFLOW_PROMPT,
    active_states: list[str] | None = None,
    terminal_states: list[str] | None = None,
    poll_interval_ms: int = 1000,
    before_remove_hook: str | None = None,
) -> str:
    active_states = active_states or ["Todo", "In Progress"]
    terminal_states = terminal_states or ["Cancelled", "Canceled", "Duplicate", "Done"]
    lines = [
        "---",
        "tracker:",
        '  kind: "beads"',
        f'  rpc_endpoint: "{repo_path_escape(rpc_endpoint)}"',
        f'  repo_path: "{repo_path_escape(repo_path)}"',
        f"  active_states: {yaml_list(active_states)}",
        f"  terminal_states: {yaml_list(terminal_states)}",
        "polling:",
        f"  interval_ms: {poll_interval_ms}",
        "workspace:",
        f'  root: "{repo_path_escape(workspace_root)}"',
        "agent:",
        "  max_concurrent_agents: 1",
        "  max_turns: 2",
        "  max_retry_backoff_ms: 1000",
        "codex:",
        '  command: "codex app-server"',
        '  approval_policy: "never"',
        '  thread_sandbox: "workspace-write"',
        "  turn_sandbox_policy:",
        '    "type": "workspaceWrite"',
        f'    "writableRoots": {yaml_list([workspace_root])}',
        '    "readOnlyAccess": {"type": "fullAccess"}',
        '    "networkAccess": true',
        '    "excludeTmpdirEnvVar": false',
        '    "excludeSlashTmp": false',
    ]
    if before_remove_hook is not None:
        lines.extend(
            [
                "hooks:",
                f'  before_remove: "{repo_path_escape(before_remove_hook)}"',
            ]
        )
    lines.extend(
        [
            "observability:",
            "  dashboard_enabled: false",
            "---",
            prompt,
            "",
        ]
    )
    return "\n".join(lines)


def repo_path_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def yaml_list(values: list[str]) -> str:
    return "[" + ", ".join(f'"{repo_path_escape(value)}"' for value in values) + "]"


def pick_free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Spin a disposable beads repo and prove Symphony tracker reads/writes against it."
    )
    parser.add_argument(
        "--symphony-root",
        default=os.environ.get("SYMPHONY_ROOT") or (
            str(DEFAULT_SYMPHONY_ROOT) if DEFAULT_SYMPHONY_ROOT.exists() else None
        ),
        help="path to the Symphony Elixir repo (or set SYMPHONY_ROOT)",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        default=os.environ.get("SKIP_BUILD") == "1",
        help="reuse existing bd and bd-http binaries",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        default=os.environ.get("KEEP") == "1",
        help="preserve the temporary disposable repo on success/failure",
    )
    return parser


def run(
    cmd: list[str],
    *,
    cwd: pathlib.Path | None = None,
    env: dict[str, str] | None = None,
    capture: bool = False,
) -> str | None:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)

    completed = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=merged_env,
        text=True,
        capture_output=capture,
        check=False,
    )
    if completed.returncode != 0:
        raise HarnessError(
            f"command failed ({completed.returncode}): {' '.join(cmd)}\n"
            f"{completed.stdout}{completed.stderr}"
        )
    if capture:
        return completed.stdout
    return None


def wait_for_http(url: str, *, timeout_seconds: float = 10.0) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                if 200 <= response.status < 300:
                    return
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
            time.sleep(0.1)
    raise HarnessError(f"timed out waiting for {url}: {last_error}")


def post_json(url: str, payload: dict[str, object] | None = None) -> dict:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload or {}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.load(response)


def get_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read().decode("utf-8")


def collect_observed_tool_fragments(
    state_payload: dict, expected_tool_fragments: set[str]
) -> set[str]:
    observed: set[str] = set()
    for running in list(state_payload.get("running", [])) + list(
        state_payload.get("recently_completed", [])
    ):
        haystacks: list[str] = []
        last_message = running.get("last_message")
        if isinstance(last_message, str):
            haystacks.append(last_message)
        for event in running.get("recent_events") or []:
            message = event.get("message")
            if isinstance(message, str):
                haystacks.append(message)
        for fragment in expected_tool_fragments:
            if any(fragment in haystack for haystack in haystacks):
                observed.add(fragment)
    return observed


def tracker_board_json(endpoint: str, repo: pathlib.Path, *, issue_id: str | None = None) -> list[dict]:
    cmd = [
        sys.executable,
        str(BOARD_APP),
        "--repo",
        str(repo),
        "--endpoint",
        endpoint,
        "list",
        "--json",
    ]
    if issue_id is not None:
        cmd.extend(["--id", issue_id])
    output = run(cmd, capture=True)
    assert output is not None
    return json.loads(output)


def write_workflow(
    path: pathlib.Path,
    *,
    repo: pathlib.Path,
    endpoint: str,
    workspace_root: pathlib.Path,
    prompt: str = DEFAULT_WORKFLOW_PROMPT,
    active_states: list[str] | None = None,
    terminal_states: list[str] | None = None,
    poll_interval_ms: int = 1000,
    before_remove_hook: str | None = None,
) -> None:
    path.write_text(
        render_workflow(
            repo_path=str(repo),
            rpc_endpoint=endpoint,
            workspace_root=str(workspace_root),
            prompt=prompt,
            active_states=active_states,
            terminal_states=terminal_states,
            poll_interval_ms=poll_interval_ms,
            before_remove_hook=before_remove_hook,
        ),
        encoding="utf-8",
    )


def make_run_env(runtime: pathlib.Path, data: pathlib.Path, config: pathlib.Path) -> dict[str, str]:
    return {
        "BD_RUNTIME_DIR": str(runtime),
        "BD_DATA_DIR": str(data),
        "BD_CONFIG_DIR": str(config),
    }


def start_bd_http(*, runtime: pathlib.Path, data: pathlib.Path, config: pathlib.Path, port: int) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env.update(make_run_env(runtime, data, config))
    env["BD_HTTP_ADDR"] = f"127.0.0.1:{port}"
    process = subprocess.Popen(
        [str(BD_HTTP_BIN)],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return process


def stop_process(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def ensure_binaries(skip_build: bool) -> None:
    if not skip_build:
        print("building fresh bd + bd-http...", file=sys.stderr)
        run(
            [
                "cargo",
                "build",
                "-p",
                "beads-rs",
                "--bin",
                "bd",
                "-p",
                "beads-http",
                "--bin",
                "bd-http",
            ],
            cwd=ROOT,
        )
        return
    if not BD_BIN.exists() or not BD_HTTP_BIN.exists():
        raise HarnessError("SKIP_BUILD=1 but bd/bd-http binaries are missing")


def ensure_symphony_cli(symphony_root: pathlib.Path) -> None:
    run(["mise", "exec", "--", "mix", "escript.build"], cwd=symphony_root)


def seed_repo(origin: pathlib.Path, repo: pathlib.Path) -> None:
    run(["git", "init", "--bare", str(origin)])
    run(["git", "init", str(repo)])
    run(["git", "-C", str(repo), "config", "user.name", "Codex Symphony E2E"])
    run(["git", "-C", str(repo), "config", "user.email", "codex@example.com"])
    run(["git", "-C", str(repo), "remote", "add", "origin", str(origin)])
    (repo / "README.md").write_text("seed\n", encoding="utf-8")
    run(["git", "-C", str(repo), "add", "README.md"])
    run(["git", "-C", str(repo), "commit", "-m", "init"])
    run(["git", "-C", str(repo), "push", "-u", "origin", "HEAD"])


def seed_tracker(endpoint: str, repo: pathlib.Path) -> tuple[str, str]:
    todo_id = run(
        [
            sys.executable,
            str(BOARD_APP),
            "--repo",
            str(repo),
            "--endpoint",
            endpoint,
            "create",
            "Symphony todo candidate",
            "--description",
            "Created for Symphony live proof",
        ],
        capture=True,
    )
    doing_id = run(
        [
            sys.executable,
            str(BOARD_APP),
            "--repo",
            str(repo),
            "--endpoint",
            endpoint,
            "create",
            "Symphony in-progress lane",
            "--description",
            "Already being worked",
        ],
        capture=True,
    )
    assert todo_id is not None
    assert doing_id is not None
    todo_id = todo_id.strip()
    doing_id = doing_id.strip()
    run(
        [
            sys.executable,
            str(BOARD_APP),
            "--repo",
            str(repo),
            "--endpoint",
            endpoint,
            "move",
            doing_id,
            "In Progress",
        ]
    )
    return todo_id, doing_id


def run_symphony_tracker_proof(
    *,
    symphony_root: pathlib.Path,
    workflow_path: pathlib.Path,
    todo_id: str,
    doing_id: str,
    comment_text: str,
) -> dict:
    code = """
workflow = System.fetch_env!("WORKFLOW_PATH")
todo_id = System.fetch_env!("TODO_ID")
doing_id = System.fetch_env!("DOING_ID")
comment_text = System.fetch_env!("COMMENT_TEXT")

SymphonyElixir.Workflow.set_workflow_file_path(workflow)
Application.ensure_all_started(:symphony_elixir)

{:ok, candidates} = SymphonyElixir.Tracker.fetch_candidate_issues()
candidate_states = Enum.into(candidates, %{}, &{&1.id, &1.state})

unless Map.get(candidate_states, todo_id) == "Todo" do
  raise "todo candidate missing from fetch_candidate_issues: #{inspect(candidate_states)}"
end

unless Map.get(candidate_states, doing_id) == "In Progress" do
  raise "in-progress candidate missing from fetch_candidate_issues: #{inspect(candidate_states)}"
end

:ok = SymphonyElixir.Tracker.update_issue_state(todo_id, "Human Review")
:ok = SymphonyElixir.Tracker.create_comment(todo_id, comment_text)

{:ok, refreshed} = SymphonyElixir.Tracker.fetch_issue_states_by_ids([todo_id])
issue = Enum.find(refreshed, &(&1.id == todo_id)) || raise("refreshed issue missing")

IO.puts(Jason.encode!(%{
  candidate_states: candidate_states,
  updated_state: issue.state,
  updated_id: issue.id
}))
"""
    env = {
        "WORKFLOW_PATH": str(workflow_path),
        "TODO_ID": todo_id,
        "DOING_ID": doing_id,
        "COMMENT_TEXT": comment_text,
    }
    output = run(
        ["mise", "exec", "--", "mix", "run", "--no-start", "-e", code],
        cwd=symphony_root,
        env=env,
        capture=True,
    )
    assert output is not None
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    return json.loads(lines[-1])


def run_dashboard_proof(*, symphony_root: pathlib.Path, workflow_path: pathlib.Path, port: int) -> None:
    process = subprocess.Popen(
        [
            "mise",
            "exec",
            "--",
            "./bin/symphony",
            "--i-understand-that-this-will-be-running-without-the-usual-guardrails",
            "--port",
            str(port),
            str(workflow_path),
        ],
        cwd=str(symphony_root),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        wait_for_http(f"http://127.0.0.1:{port}/api/v1/state")
        wait_for_http(f"http://127.0.0.1:{port}/")
    finally:
        stop_process(process)


def lifecycle_prompt(*, endpoint: str, repo_path: str) -> str:
    return textwrap.dedent(
        f"""
        You are closing out a beads-backed tracker issue.

        Work only inside the current workspace. Do not ask for help. Do not leave the tracker issue active.
        Use Symphony's tracker tools for tracker writes. Do not use Python, curl, or raw HTTP for tracker mutations when the tracker tools are available.

        1. Write `{LIFECYCLE_PROOF_FILE}` in the current working directory with the exact contents:
           `symphony lifecycle ok {{{{ issue.id }}}}`
        2. Use `tracker_create_issue` to create a follow-up issue with:
           - `title`: `Fuse terminal branch metadata into canonical status lifecycle for {{{{ issue.identifier }}}}`
           - `description`: `Problem: terminal branch metadata still sits beside canonical issue status. Design: fuse terminal metadata into the canonical lifecycle model so only terminal issues can carry it. Acceptance: the status model no longer needs a peer closed-on-branch field to stay coherent.`
           - `priority`: `2`
           - `issueType`: `chore`
           - `labels`: `["status-model", "follow-up"]`
        3. Use the returned issue id from step 2 with `tracker_add_blocker` so the new follow-up issue is blocked by the current issue:
           - `blockedIssueId`: `<new issue id from step 2>`
           - `blockerIssueId`: `{{{{ issue.id }}}}`
        4. Use `tracker_create_comment` with:
           - `issueId`: `{{{{ issue.id }}}}`
           - `body`: `Symphony lifecycle worker completed {{{{ issue.identifier }}}}`
        5. Use `tracker_update_issue_state` with:
           - `issueId`: `{{{{ issue.id }}}}`
           - `state`: `Done`
        6. Verify `{LIFECYCLE_PROOF_FILE}` exists in the workspace and then end the turn.
        """
    ).strip()


def before_remove_copy_hook(proof_path: pathlib.Path) -> str:
    quoted_destination = shlex.quote(str(proof_path))
    quoted_source = shlex.quote(LIFECYCLE_PROOF_FILE)
    return f"if [ -f {quoted_source} ]; then cp {quoted_source} {quoted_destination}; fi"


def create_issue(endpoint: str, repo: pathlib.Path, title: str, description: str) -> str:
    issue_id = run(
        [
            sys.executable,
            str(BOARD_APP),
            "--repo",
            str(repo),
            "--endpoint",
            endpoint,
            "create",
            title,
            "--description",
            description,
        ],
        capture=True,
    )
    assert issue_id is not None
    return issue_id.strip()


def get_issue(endpoint: str, repo: pathlib.Path, issue_id: str) -> dict:
    issues = tracker_board_json(endpoint, repo, issue_id=issue_id)
    if len(issues) != 1:
        raise HarnessError(f"expected one issue for {issue_id}, got {issues}")
    return issues[0]


def get_issue_by_title(endpoint: str, repo: pathlib.Path, title: str) -> dict:
    issues = tracker_board_json(endpoint, repo)
    matches = [issue for issue in issues if issue.get("title") == title]
    if len(matches) != 1:
        raise HarnessError(f"expected one issue titled {title!r}, got {matches}")
    return matches[0]


def log_tail(path: pathlib.Path, max_bytes: int = 4096) -> str:
    if not path.exists():
        return "<missing log>"
    data = path.read_text(encoding="utf-8", errors="replace")
    if len(data) <= max_bytes:
        return data
    return data[-max_bytes:]


def wait_for_issue_state(
    endpoint: str,
    repo: pathlib.Path,
    issue_id: str,
    target_status: str,
    *,
    timeout_seconds: float,
) -> dict:
    deadline = time.time() + timeout_seconds
    last_issue: dict | None = None
    while time.time() < deadline:
        issue = get_issue(endpoint, repo, issue_id)
        last_issue = issue
        if issue.get("status") == target_status:
            return issue
        time.sleep(0.25)
    raise HarnessError(f"timed out waiting for {issue_id} to reach {target_status}: last={last_issue}")


def run_orchestrator_lifecycle_proof(
    *,
    symphony_root: pathlib.Path,
    workflow_path: pathlib.Path,
    endpoint: str,
    repo: pathlib.Path,
    run_env: dict[str, str],
    lifecycle_issue_id: str,
    lifecycle_identifier: str,
    workspace_root: pathlib.Path,
    temp_root: pathlib.Path,
) -> dict:
    service_port = pick_free_port()
    log_path = temp_root / "symphony-lifecycle.log"
    process: subprocess.Popen[str] | None = None
    workspace_path = workspace_root / lifecycle_identifier
    workspace_proof_path = workspace_path / LIFECYCLE_PROOF_FILE
    preserved_proof_path = temp_root / f"{lifecycle_identifier}-proof.txt"
    expected_comment = f"Symphony lifecycle worker completed {lifecycle_identifier}"
    expected_proof = f"symphony lifecycle ok {lifecycle_issue_id}"
    followup_title = f"Fuse terminal branch metadata into canonical status lifecycle for {lifecycle_identifier}"
    expected_tool_fragments = {
        "tracker_create_issue",
        "tracker_add_blocker",
        "tracker_create_comment",
        "tracker_update_issue_state",
    }
    observed_tool_fragments: set[str] = set()
    done_seen_at: float | None = None

    try:
        with log_path.open("w", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                [
                    "mise",
                    "exec",
                    "--",
                    "./bin/symphony",
                    "--i-understand-that-this-will-be-running-without-the-usual-guardrails",
                    "--port",
                    str(service_port),
                    str(workflow_path),
                ],
                cwd=str(symphony_root),
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
            )

            wait_for_http(f"http://127.0.0.1:{service_port}/api/v1/state", timeout_seconds=30.0)
            wait_for_http(f"http://127.0.0.1:{service_port}/", timeout_seconds=30.0)
            refresh_url = f"http://127.0.0.1:{service_port}/api/v1/refresh"
            state_url = f"http://127.0.0.1:{service_port}/api/v1/state"
            deadline = time.time() + 120.0
            next_refresh = 0.0
            issue: dict | None = None
            while time.time() < deadline:
                now = time.time()
                if now >= next_refresh:
                    post_json(refresh_url)
                    next_refresh = now + 5.0
                try:
                    state_payload = get_json(state_url)
                except urllib.error.URLError:
                    state_payload = {}
                observed_tool_fragments.update(
                    collect_observed_tool_fragments(state_payload, expected_tool_fragments)
                )
                issue = get_issue(endpoint, repo, lifecycle_issue_id)
                if issue.get("status") == "Done" and done_seen_at is None:
                    done_seen_at = now
                    deadline = min(deadline, done_seen_at + 10.0)

                if issue.get("status") == "Done" and observed_tool_fragments == expected_tool_fragments:
                    break
                time.sleep(0.25)
            else:
                if issue is None or issue.get("status") != "Done":
                    raise HarnessError(
                        f"timed out waiting for {lifecycle_issue_id} to reach Done: last={issue}\n"
                        f"log tail:\n{log_tail(log_path)}"
                    )

        proof_path: pathlib.Path | None = None
        deadline = time.time() + 10.0
        while time.time() < deadline:
            for candidate in [preserved_proof_path, workspace_proof_path]:
                if candidate.exists():
                    proof_path = candidate
                    break
            if proof_path is not None:
                break
            time.sleep(0.25)

        if proof_path is None:
            raise HarnessError(
                "lifecycle proof file was neither preserved on cleanup nor left in the live workspace\n"
                f"workspace={workspace_proof_path}\n"
                f"preserved={preserved_proof_path}\n"
                f"log tail:\n{log_tail(log_path)}"
            )

        proof_contents = proof_path.read_text(encoding="utf-8").strip()
        if proof_contents != expected_proof:
            raise HarnessError(
                f"unexpected lifecycle proof contents at {proof_path}: {proof_contents!r}"
            )

        show_output = run(
            [str(BD_BIN), "show", lifecycle_issue_id, "--repo", str(repo)],
            env=run_env,
            capture=True,
        )
        assert show_output is not None
        if expected_comment not in show_output:
            raise HarnessError(
                f"lifecycle comment missing from bd show output:\n{show_output}\nlog tail:\n{log_tail(log_path)}"
            )
        followup_issue = get_issue_by_title(endpoint, repo, followup_title)

        followup_show_output = run(
            [str(BD_BIN), "show", followup_issue["id"], "--repo", str(repo)],
            env=run_env,
            capture=True,
        )
        assert followup_show_output is not None
        if "Depends on" not in followup_show_output or lifecycle_issue_id not in followup_show_output:
            raise HarnessError(
                "follow-up issue is missing the expected dependency on the lifecycle issue\n"
                f"follow-up show output:\n{followup_show_output}\n"
                f"log tail:\n{log_tail(log_path)}"
            )

        missing_tool_fragments = expected_tool_fragments.difference(observed_tool_fragments)
        if missing_tool_fragments:
            raise HarnessError(
                "expected tracker dynamic tool calls were not all observed via the Symphony status API during the lifecycle proof\n"
                f"missing={sorted(missing_tool_fragments)} observed={sorted(observed_tool_fragments)}\n"
                f"log tail:\n{log_tail(log_path)}"
            )

        issue_payload = get_json(f"http://127.0.0.1:{service_port}/api/v1/{lifecycle_identifier}")
        if issue_payload.get("status") != "completed":
            raise HarnessError(
                "completed issue drill-down endpoint did not report the expected completed status\n"
                f"payload={json.dumps(issue_payload, indent=2)}\n"
                f"log tail:\n{log_tail(log_path)}"
            )

        completed_payload = issue_payload.get("completed") or {}
        if completed_payload.get("state") != "Done":
            raise HarnessError(
                "completed issue drill-down endpoint did not preserve the final status\n"
                f"payload={json.dumps(issue_payload, indent=2)}\n"
                f"log tail:\n{log_tail(log_path)}"
            )

        issue_recent_messages = [
            event.get("message")
            for event in issue_payload.get("recent_events") or []
            if isinstance(event, dict)
        ]
        if not any(
            isinstance(message, str) and "tracker_update_issue_state" in message
            for message in issue_recent_messages
        ):
            raise HarnessError(
                "completed issue drill-down endpoint did not retain the status-update tool event in recent history\n"
                f"payload={json.dumps(issue_payload, indent=2)}\n"
                f"log tail:\n{log_tail(log_path)}"
            )

        dashboard_html = get_text(f"http://127.0.0.1:{service_port}/")
        for expected_fragment in [
            "Recently completed sessions",
            lifecycle_identifier,
            f"/api/v1/{lifecycle_identifier}",
        ]:
            if expected_fragment not in dashboard_html:
                raise HarnessError(
                    "dashboard HTML did not expose the recently completed lifecycle session\n"
                    f"missing={expected_fragment!r}\n"
                    f"log tail:\n{log_tail(log_path)}"
                )

        return {
            "service_port": service_port,
            "workspace_path": str(workspace_path),
            "proof_path": str(proof_path),
            "workspace_cleaned": not workspace_path.exists(),
            "final_status": issue["status"],
            "log_path": str(log_path),
            "observed_tool_fragments": sorted(observed_tool_fragments),
            "followup_issue_id": followup_issue["id"],
            "followup_title": followup_title,
        }
    finally:
        stop_process(process)


def main(argv: list[str]) -> int:
    args = build_parser().parse_args(argv)
    if not args.symphony_root:
        raise HarnessError("pass --symphony-root or set SYMPHONY_ROOT")

    symphony_root = pathlib.Path(args.symphony_root).resolve()
    if not symphony_root.exists():
        raise HarnessError(f"symphony root does not exist: {symphony_root}")

    ensure_binaries(args.skip_build)
    ensure_symphony_cli(symphony_root)
    if not BOARD_APP.exists():
        raise HarnessError(f"tracker board helper missing: {BOARD_APP}")

    temp_root = pathlib.Path(tempfile.mkdtemp(prefix="beads-symphony-e2e."))
    print(f"temp root: {temp_root}")
    http_process: subprocess.Popen[str] | None = None

    try:
        origin = temp_root / "origin.git"
        repo = temp_root / "repo"
        runtime = temp_root / "runtime"
        data = temp_root / "data"
        config = temp_root / "config"
        workspace_root = temp_root / "workspaces"
        workflow_path = temp_root / "WORKFLOW.md"
        for path in [repo, runtime, data, config, workspace_root]:
            path.mkdir(parents=True, exist_ok=True)

        run_env = make_run_env(runtime, data, config)
        seed_repo(origin, repo)
        run([str(BD_BIN), "init", "--repo", str(repo)], env=run_env)

        port = pick_free_port()
        endpoint = f"http://127.0.0.1:{port}/rpc"
        http_process = start_bd_http(runtime=runtime, data=data, config=config, port=port)
        wait_for_http(f"http://127.0.0.1:{port}/healthz")

        todo_id, doing_id = seed_tracker(endpoint, repo)
        write_workflow(workflow_path, repo=repo, endpoint=endpoint, workspace_root=workspace_root)

        comment_text = "Symphony beads e2e comment"
        proof = run_symphony_tracker_proof(
            symphony_root=symphony_root,
            workflow_path=workflow_path,
            todo_id=todo_id,
            doing_id=doing_id,
            comment_text=comment_text,
        )

        if proof.get("updated_state") != "Human Review":
            raise HarnessError(f"unexpected updated status: {proof}")

        refreshed = tracker_board_json(endpoint, repo, issue_id=todo_id)
        if len(refreshed) != 1:
            raise HarnessError(f"expected one refreshed issue for {todo_id}, got {refreshed}")
        issue = refreshed[0]
        if issue.get("status") != "Human Review":
            raise HarnessError(f"tracker board status mismatch: {issue}")
        if issue.get("labels") != []:
            raise HarnessError(f"reserved tracker labels leaked back into board projection: {issue}")

        show_output = run(
            [str(BD_BIN), "show", todo_id, "--repo", str(repo)],
            env=run_env,
            capture=True,
        )
        assert show_output is not None
        if comment_text not in show_output:
            raise HarnessError(f"comment missing from bd show output:\n{show_output}")

        run(
            [
                sys.executable,
                str(BOARD_APP),
                "--repo",
                str(repo),
                "--endpoint",
                endpoint,
                "move",
                doing_id,
                "Done",
            ]
        )

        lifecycle_id = create_issue(
            endpoint,
            repo,
            "Symphony orchestrator lifecycle proof",
            "The workflow prompt contains the exact completion steps.",
        )
        lifecycle_issue = get_issue(endpoint, repo, lifecycle_id)
        lifecycle_identifier = lifecycle_issue.get("identifier") or lifecycle_id
        preserved_proof_path = temp_root / f"{lifecycle_identifier}-proof.txt"

        write_workflow(
            path=workflow_path,
            repo=repo,
            endpoint=endpoint,
            workspace_root=workspace_root,
            prompt=lifecycle_prompt(endpoint=endpoint, repo_path=str(repo)),
            active_states=["Todo", "In Progress"],
            terminal_states=["Cancelled", "Canceled", "Duplicate", "Done"],
            poll_interval_ms=250,
            before_remove_hook=before_remove_copy_hook(preserved_proof_path),
        )

        lifecycle = run_orchestrator_lifecycle_proof(
            symphony_root=symphony_root,
            workflow_path=workflow_path,
            endpoint=endpoint,
            repo=repo,
            run_env=run_env,
            lifecycle_issue_id=lifecycle_id,
            lifecycle_identifier=lifecycle_identifier,
            workspace_root=workspace_root,
            temp_root=temp_root,
        )

        print(json.dumps({
            "repo": str(repo),
            "endpoint": endpoint,
            "workflow": str(workflow_path),
            "todo_id": todo_id,
            "doing_id": doing_id,
            "updated_state": proof["updated_state"],
            "lifecycle_issue_id": lifecycle_id,
            "lifecycle_final_status": lifecycle["final_status"],
            "service_port": lifecycle["service_port"],
            "lifecycle_workspace": lifecycle["workspace_path"],
            "lifecycle_proof": lifecycle["proof_path"],
            "lifecycle_observed_tools": lifecycle["observed_tool_fragments"],
            "lifecycle_followup_issue_id": lifecycle["followup_issue_id"],
            "lifecycle_followup_title": lifecycle["followup_title"],
            "service_log": lifecycle["log_path"],
        }, indent=2))
        return 0
    finally:
        stop_process(http_process)
        if not args.keep:
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except HarnessError as exc:
        raise SystemExit(str(exc)) from exc
