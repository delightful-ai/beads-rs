## Boundary
This crate owns the HTTP transport surface in front of the beads daemon's
`Request`/`Response` enum contract. It is a thin adapter; the wire body is the
existing `beads_surface::Request`/`Response` JSON encoding, no parallel REST
shape.

NEVER:
- Invent a parallel request/response shape, payload type, or error envelope.
  Extend `beads-surface` instead.
- Import `beads_daemon::*`, `beads_daemon::runtime::*`, or `beads_daemon::git::*`.
  This crate talks to the daemon through `beads_surface::IpcClient` and (later)
  the public `Dispatcher` seam, never daemon internals.
- Add command/CLI semantics here. Render/format belongs in `beads-cli`;
  command language belongs there too.

## Routing
- `src/lib.rs` owns the Axum `Router`, the `POST /rpc` handler, and small
  liveness/health endpoints. Keep handlers thin: deserialize `Request`, call
  the surface client, return `Response`.
- `src/bin/bd-http.rs` is the minimal binary entry point. Configuration is
  environment-driven (`BD_HTTP_ADDR`, `BD_RUNTIME_DIR`, `BD_HTTP_NO_AUTOSTART`).
  Resist growing flag/clap surface here; promote to a config module if it grows.
- Auth (tailnet peer identity, optional bearer) lands in its own module
  (`auth.rs`) when added, layered as a `tower` middleware. Do not inline auth
  checks into handlers.
- `GET /subscribe` is currently a 501 stub. The streaming implementation is
  deferred until the in-process `Dispatcher` seam lands so `StreamEvent`s do
  not have to be re-framed across the Unix socket.

## Wire Contract
- `POST /rpc` body is `beads_surface::Request` JSON; response body is
  `beads_surface::Response` JSON. HTTP status is `200 OK` for any successfully
  framed `Response` (including `Response::Err`). Use 4xx only for malformed
  request bodies and 5xx for unrecoverable transport failures.
- Transport-level errors from `IpcClient` (daemon unavailable, version
  mismatch, frame too large) are mapped onto `Response::Err` via
  `IntoErrorPayload` so callers always deserialize the same enum.

## Verification
- `cargo test -p beads-http` exercises router wiring, healthz, the
  unavailable-daemon error mapping, and malformed-body rejection.
- `cargo check -p beads-http` is the fast typecheck loop.
- `just dylint` must still pass; the crate DAG policy lists the allowed edges
  for `beads-http`.
