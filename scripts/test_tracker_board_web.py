from __future__ import annotations

import http.client
import importlib.util
import json
import pathlib
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


MODULE_PATH = pathlib.Path(__file__).with_name("tracker_board_web.py")
MODULE_SPEC = importlib.util.spec_from_file_location("tracker_board_web", MODULE_PATH)
assert MODULE_SPEC is not None
assert MODULE_SPEC.loader is not None
tracker_board_web = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(tracker_board_web)


class StubUpstreamHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/healthz":
            self.send_error(404)
            return

        time.sleep(self.server.healthz_delay)
        payload = json.dumps(self.server.healthz_payload).encode("utf-8")
        self.send_response(self.server.healthz_status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/rpc":
            self.send_error(404)
            return

        length = int(self.headers.get("content-length", "0"))
        raw = self.rfile.read(length)
        self.server.requests.append(json.loads(raw.decode("utf-8") or "{}"))
        time.sleep(self.server.rpc_delay)

        payload = json.dumps(self.server.rpc_payload).encode("utf-8")
        self.send_response(self.server.rpc_status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


class StubUpstreamServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        *,
        healthz_status: int = 200,
        healthz_payload: dict | None = None,
        healthz_delay: float = 0.0,
        rpc_status: int = 200,
        rpc_payload: dict | None = None,
        rpc_delay: float = 0.0,
    ) -> None:
        super().__init__(server_address, StubUpstreamHandler)
        self.healthz_status = healthz_status
        self.healthz_payload = healthz_payload or {"ok": True, "service": "stub-upstream"}
        self.healthz_delay = healthz_delay
        self.rpc_status = rpc_status
        self.rpc_payload = rpc_payload or {"ok": {"result": "tracker_issues", "data": []}}
        self.rpc_delay = rpc_delay
        self.requests: list[dict] = []


class BoardServerTest(unittest.TestCase):
    def start_server(self, server: ThreadingHTTPServer) -> tuple[threading.Thread, str]:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        return thread, f"http://{host}:{port}"

    def request(self, base_url: str, method: str, path: str, body: dict | None = None) -> tuple[int, bytes]:
        parsed = http.client.HTTPConnection(base_url.removeprefix("http://"), timeout=2)
        headers = {}
        payload = None
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        parsed.request(method, path, body=payload, headers=headers)
        response = parsed.getresponse()
        raw = response.read()
        parsed.close()
        return response.status, raw

    def test_favicon_returns_no_content(self) -> None:
        server = tracker_board_web.TrackerBoardServer(
            ("127.0.0.1", 0),
            repo="/tmp/beads-demo",
            rpc_endpoint="http://127.0.0.1:17777/rpc",
            timeout_seconds=0.05,
        )
        thread, base_url = self.start_server(server)
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join, 1)

        status, body = self.request(base_url, "GET", "/favicon.ico")

        self.assertEqual(status, 204)
        self.assertEqual(body, b"")

    def test_rpc_injects_repo_when_missing(self) -> None:
        upstream = StubUpstreamServer(("127.0.0.1", 0))
        upstream_thread, upstream_url = self.start_server(upstream)
        self.addCleanup(upstream.shutdown)
        self.addCleanup(upstream.server_close)
        self.addCleanup(upstream_thread.join, 1)

        board = tracker_board_web.TrackerBoardServer(
            ("127.0.0.1", 0),
            repo="/tmp/beads-demo",
            rpc_endpoint=f"{upstream_url}/rpc",
            timeout_seconds=0.05,
        )
        board_thread, board_url = self.start_server(board)
        self.addCleanup(board.shutdown)
        self.addCleanup(board.server_close)
        self.addCleanup(board_thread.join, 1)

        status, body = self.request(board_url, "POST", "/api/rpc", {"op": "tracker_list"})

        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body.decode("utf-8")), {"ok": {"result": "tracker_issues", "data": []}})
        self.assertEqual(upstream.requests, [{"op": "tracker_list", "repo": "/tmp/beads-demo"}])

    def test_rpc_timeout_returns_bad_gateway_json(self) -> None:
        upstream = StubUpstreamServer(("127.0.0.1", 0), rpc_delay=0.2)
        upstream_thread, upstream_url = self.start_server(upstream)
        self.addCleanup(upstream.shutdown)
        self.addCleanup(upstream.server_close)
        self.addCleanup(upstream_thread.join, 1)

        board = tracker_board_web.TrackerBoardServer(
            ("127.0.0.1", 0),
            repo="/tmp/beads-demo",
            rpc_endpoint=f"{upstream_url}/rpc",
            timeout_seconds=0.05,
        )
        board_thread, board_url = self.start_server(board)
        self.addCleanup(board.shutdown)
        self.addCleanup(board.server_close)
        self.addCleanup(board_thread.join, 1)

        status, body = self.request(board_url, "POST", "/api/rpc", {"op": "tracker_list"})
        payload = json.loads(body.decode("utf-8"))

        self.assertEqual(status, 502)
        self.assertEqual(payload["error"]["code"], "upstream_unavailable")


if __name__ == "__main__":
    unittest.main()
