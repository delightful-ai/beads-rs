#!/usr/bin/env python3
"""Small operator-facing board client for the beads HTTP tracker surface."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request


STATUS_ORDER = [
    "Todo",
    "In Progress",
    "Human Review",
    "Rework",
    "Merging",
    "Done",
    "Cancelled",
    "Duplicate",
]


def rpc(endpoint: str, payload: dict) -> dict:
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"HTTP {exc.code}: {body}") from exc


def unwrap_ok(response: dict) -> dict:
    if "ok" in response and isinstance(response["ok"], dict):
        return response["ok"]
    if "err" in response and isinstance(response["err"], dict):
        err = response["err"]
        code = err.get("code", "unknown")
        message = err.get("message", "unknown error")
        raise SystemExit(f"{code}: {message}")
    raise SystemExit(f"unexpected response: {json.dumps(response, indent=2)}")


def cmd_list(args: argparse.Namespace) -> int:
    payload = {"op": "tracker_list", "repo": args.repo}
    if args.status:
        payload["statuses"] = args.status
    if args.id:
        payload["ids"] = args.id
    if args.limit is not None:
        payload["limit"] = args.limit

    ok = unwrap_ok(rpc(args.endpoint, payload))
    issues = ok.get("data", [])

    if args.json:
        print(json.dumps(issues, indent=2))
        return 0

    grouped: dict[str, list[dict]] = {}
    for issue in issues:
        grouped.setdefault(issue.get("status", "Unknown"), []).append(issue)

    ordered_statuses = [status for status in STATUS_ORDER if grouped.get(status)]
    ordered_statuses.extend(
        status for status in sorted(grouped) if status not in ordered_statuses
    )

    if not ordered_statuses:
        print("No tracker issues found.")
        return 0

    for index, status in enumerate(ordered_statuses):
        if index:
            print()
        status_issues = grouped[status]
        print(f"{status} ({len(status_issues)})")
        print("-" * (len(status) + len(str(len(status_issues))) + 3))
        for issue in status_issues:
            print(
                f"{issue['identifier']}  P{issue['priority']}  {issue['title']}"
            )
            if issue.get("blocked_by"):
                blockers = ", ".join(
                    f"{blocker['identifier']} [{blocker['status']}]"
                    for blocker in issue["blocked_by"]
                )
                print(f"  blocked by: {blockers}")
            if issue.get("labels"):
                print(f"  labels: {', '.join(issue['labels'])}")
            if issue.get("assignee"):
                print(f"  assignee: {issue['assignee']}")
    return 0


def cmd_create(args: argparse.Namespace) -> int:
    payload = {
        "op": "create",
        "repo": args.repo,
        "title": args.title,
        "description": args.description,
        "type": args.issue_type,
        "priority": args.priority,
        "labels": args.label or [],
    }
    ok = unwrap_ok(rpc(args.endpoint, payload))
    if args.json:
        print(json.dumps(ok, indent=2))
    else:
        print(ok["id"])
    return 0


def cmd_move(args: argparse.Namespace) -> int:
    ok = unwrap_ok(
        rpc(
            args.endpoint,
            {
                "op": "tracker_transition",
                "repo": args.repo,
                "id": args.id,
                "status": args.status,
            },
        )
    )
    if args.json:
        print(json.dumps(ok, indent=2))
    else:
        print(f"{ok['id']} -> {args.status}")
    return 0


def cmd_comment(args: argparse.Namespace) -> int:
    ok = unwrap_ok(
        rpc(
            args.endpoint,
            {
                "op": "tracker_comment",
                "repo": args.repo,
                "id": args.id,
                "content": args.content,
            },
        )
    )
    if args.json:
        print(json.dumps(ok, indent=2))
    else:
        print(f"{ok['bead_id']} comment added")
    return 0


def cmd_depend(args: argparse.Namespace) -> int:
    ok = unwrap_ok(
        rpc(
            args.endpoint,
            {
                "op": "add_dep",
                "repo": args.repo,
                "from": args.blocked,
                "to": args.blocker,
                "kind": "blocks",
            },
        )
    )
    if args.json:
        print(json.dumps(ok, indent=2))
    else:
        print(f"{args.blocked} depends on {args.blocker}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Simple board client for the beads HTTP tracker surface."
    )
    parser.add_argument(
        "--endpoint",
        default="http://127.0.0.1:7777/rpc",
        help="beads-http /rpc endpoint",
    )
    parser.add_argument("--repo", required=True, help="repo path for the tracker store")

    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="list tracker issues")
    list_parser.add_argument("--status", action="append", help="filter by issue status")
    list_parser.add_argument("--id", action="append", help="filter by bead id")
    list_parser.add_argument("--limit", type=int, help="truncate issue list")
    list_parser.add_argument("--json", action="store_true", help="print raw issue JSON")
    list_parser.set_defaults(func=cmd_list)

    create_parser = subparsers.add_parser("create", help="create a tracker issue")
    create_parser.add_argument("title", help="issue title")
    create_parser.add_argument(
        "--description", default="", help="issue description/body text"
    )
    create_parser.add_argument(
        "--priority", type=int, default=2, help="priority 0-4"
    )
    create_parser.add_argument(
        "--type", dest="issue_type", default="task", help="bead type"
    )
    create_parser.add_argument("--label", action="append", help="initial label")
    create_parser.add_argument("--json", action="store_true", help="print raw JSON")
    create_parser.set_defaults(func=cmd_create)

    move_parser = subparsers.add_parser("move", help="transition issue status")
    move_parser.add_argument("id", help="bead id")
    move_parser.add_argument("status", help="target issue status")
    move_parser.add_argument("--json", action="store_true", help="print raw JSON")
    move_parser.set_defaults(func=cmd_move)

    comment_parser = subparsers.add_parser("comment", help="append a tracker comment")
    comment_parser.add_argument("id", help="bead id")
    comment_parser.add_argument("content", help="comment text")
    comment_parser.add_argument("--json", action="store_true", help="print raw JSON")
    comment_parser.set_defaults(func=cmd_comment)

    depend_parser = subparsers.add_parser(
        "depend", help="mark one issue as blocked by another"
    )
    depend_parser.add_argument("blocked", help="issue that is waiting")
    depend_parser.add_argument("blocker", help="issue that must complete first")
    depend_parser.add_argument("--json", action="store_true", help="print raw JSON")
    depend_parser.set_defaults(func=cmd_depend)

    return parser


def main(argv: list[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
