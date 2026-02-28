## Boundary
This directory is the daemon: the single serialization point for mutations + queries.
Depends on: `core/` + `git/` + Unix socket IPC.
Depended on by: `cli/` (via IPC) and local agents/tools (via `bd`).
NEVER: add user-facing rendering here; return structured payloads and let `cli/` render them.

## How to work here
- Concurrency model (high level):
  - one state loop applies ops + answers queries
  - background git loop syncs state to `refs/heads/beads/store`
  - socket acceptor spawns per-connection handlers
- IPC is newline-delimited JSON over a Unix socket; see `src/daemon/ipc.rs`.
- Multi-clone sharing keys state by normalized remote URL; see `src/daemon/remote.rs` + `src/daemon/core.rs`.
- When adding a new operation:
  1. extend `Request`/`ResponsePayload` (`src/daemon/ipc.rs`)
  2. implement the op in `src/daemon/ops.rs` / `src/daemon/executor.rs`
  3. add/adjust API schema in `src/api/` if it affects `--json`

## Verification
- `cargo test`
- Manual smoke: `cargo run --bin bd -- status` (should auto-start daemon on Unix).

## Don't copy this
- Don't parse IDs in handlers; accept `ValidatedBeadId` etc from the request layer.
- Don't branch on `namespace.is_core()` for state access; `StoreState` is a uniform map.
- Don't normalize namespace/dep lists manually; use `NamespaceSet` / `DepSpecSet`.
- Don't map errors to payloads in transport; errors implement `IntoErrorPayload` themselves.
- Don't write snapshot serialization; use `SnapshotCodec` from core.

## Gotchas
- IPC currently uses `std::os::unix`; Windows support will require a different transport.
