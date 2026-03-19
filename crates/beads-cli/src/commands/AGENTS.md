## Boundary
This subtree owns per-command CLI behavior, rendering, and request construction.
NEVER: reach into `IpcClient`, repo discovery, or actor/bootstrap plumbing from a handler just because it needs a typed request.

## Routing
- Most handlers follow the IPC path: normalize args, build a typed `Request::*` or `AdminOp::*`, call `runtime::send` or `send_raw`, then render the typed payload or hand it to `print_ok`.
- Backend-driven exceptions go through `../backend.rs`: copy `store.rs` for offline-or-host-backed admin flows, and keep `upgrade.rs`/`migrate.rs` in that family instead of forcing them through IPC.
- `setup.rs` and `onboard.rs` are the local file/stdout exception: copy them for editor/config generation flows that do not talk to the daemon.
- `mod.rs` is the registry and shared render dispatch. If a new payload needs human output wiring, extend that dispatch instead of open-coding a second switch.
- `common.rs` is the first place to extend shared fetch/format helpers before adding another command-local copy.
- Copy `list.rs` for query-plus-custom-human rendering, `admin.rs` for IPC subcommand fanout into typed ops, `create.rs` when a mutation may need fetch-after-create fallback, `store.rs` for backend-driven exceptions, and `setup.rs` for local file/config flows.
- The global parse/build-runtime split lives across `../cli.rs` and `../runtime.rs`; command handlers should assume setup is already done.

## Verification
- `cargo test -p beads-cli commands::` is the quickest proof loop for handler and renderer changes in this subtree.
- Run `just dylint` if a command change tempts you to reach into daemon internals or host-only seams.
