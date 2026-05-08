#!/usr/bin/env python3
"""Serve a browser-friendly tracker board against the beads HTTP RPC surface."""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import pathlib
import socket
import urllib.error
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


ASSET_DIR = pathlib.Path(__file__).with_name("tracker_board_web")
DEFAULT_STATUSES = [
    "Todo",
    "In Progress",
    "Human Review",
    "Rework",
    "Merging",
    "Done",
    "Cancelled",
    "Duplicate",
]


class TrackerBoardHandler(BaseHTTPRequestHandler):
    server_version = "TrackerBoard/0.1"

    def do_GET(self) -> None:  # noqa: N802
        if self.path in {"/", "/index.html"}:
            self._serve_asset("index.html")
            return
        if self.path == "/favicon.ico":
            self._send_bytes(b"", status=HTTPStatus.NO_CONTENT)
            return
        if self.path == "/app.js":
            self._serve_asset("app.js")
            return
        if self.path == "/app.css":
            self._serve_asset("app.css")
            return
        if self.path == "/api/config":
            self._send_json(
                {
                    "repo": self.server.repo,
                    "rpc_endpoint": self.server.rpc_endpoint,
                    "statuses": DEFAULT_STATUSES,
                }
            )
            return
        if self.path == "/api/healthz":
            upstream = self._upstream_health()
            self._send_json(
                {
                    "ok": upstream["ok"],
                    "service": "tracker-board-web",
                    "upstream": upstream,
                },
                status=HTTPStatus.OK if upstream["ok"] else HTTPStatus.BAD_GATEWAY,
            )
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/api/rpc":
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        length = int(self.headers.get("content-length", "0"))
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError as exc:
            self._send_json(
                {"error": {"code": "bad_json", "message": str(exc)}},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        if self.server.repo and "repo" not in payload:
            payload["repo"] = self.server.repo

        try:
            response = self._proxy_rpc(payload)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            self._send_bytes(
                body.encode("utf-8"),
                status=exc.code,
                content_type=exc.headers.get_content_type() or "application/json",
            )
            return
        except (TimeoutError, socket.timeout, urllib.error.URLError) as exc:
            reason = getattr(exc, "reason", exc)
            self._send_json(
                {
                    "error": {
                        "code": "upstream_unavailable",
                        "message": str(reason),
                    }
                },
                status=HTTPStatus.BAD_GATEWAY,
            )
            return

        self._send_json(response)

    def log_message(self, fmt: str, *args) -> None:
        print(
            f"[tracker-board] {self.address_string()} - {fmt % args}",
            flush=True,
        )

    def _serve_asset(self, name: str) -> None:
        asset = ASSET_DIR / name
        if not asset.exists():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        content_type, _ = mimetypes.guess_type(asset.name)
        self._send_bytes(
            asset.read_bytes(),
            content_type=content_type or "application/octet-stream",
        )

    def _proxy_rpc(self, payload: dict) -> dict:
        request = urllib.request.Request(
            self.server.rpc_endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.server.timeout_seconds) as response:
            return json.load(response)

    def _upstream_health(self) -> dict:
        health_url = self.server.rpc_endpoint.rsplit("/", 1)[0] + "/healthz"
        try:
            with urllib.request.urlopen(
                health_url,
                timeout=self.server.timeout_seconds,
            ) as response:
                body = json.load(response)
        except (TimeoutError, socket.timeout, urllib.error.URLError) as exc:
            reason = getattr(exc, "reason", exc)
            return {"ok": False, "error": str(reason)}
        return {
            "ok": bool(body.get("ok")),
            "service": body.get("service"),
        }

    def _send_json(self, payload: dict, status: int = HTTPStatus.OK) -> None:
        self._send_bytes(
            json.dumps(payload).encode("utf-8"),
            status=status,
            content_type="application/json",
        )

    def _send_bytes(
        self,
        payload: bytes,
        *,
        status: int = HTTPStatus.OK,
        content_type: str = "text/plain; charset=utf-8",
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class TrackerBoardServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        *,
        repo: str,
        rpc_endpoint: str,
        timeout_seconds: float,
    ):
        super().__init__(server_address, TrackerBoardHandler)
        self.repo = repo
        self.rpc_endpoint = rpc_endpoint
        self.timeout_seconds = timeout_seconds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve a browserable tracker board for the beads HTTP RPC."
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("BD_TRACKER_BOARD_HOST", "127.0.0.1"),
        help="bind host",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("BD_TRACKER_BOARD_PORT", "8087")),
        help="bind port",
    )
    parser.add_argument(
        "--repo",
        default=os.environ.get("BD_TRACKER_REPO", ""),
        help="default repo path for tracker requests",
    )
    parser.add_argument(
        "--rpc-endpoint",
        default=os.environ.get("BD_TRACKER_RPC_ENDPOINT", "http://127.0.0.1:7777/rpc"),
        help="beads-http /rpc endpoint",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=float(os.environ.get("BD_TRACKER_TIMEOUT_SECONDS", "5")),
        help="upstream beads-http request timeout",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    server = TrackerBoardServer(
        (args.host, args.port),
        repo=args.repo,
        rpc_endpoint=args.rpc_endpoint,
        timeout_seconds=args.timeout_seconds,
    )
    print(
        f"tracker board listening on http://{args.host}:{args.port}/ "
        f"(repo={args.repo or '<unset>'}, rpc={args.rpc_endpoint})",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
