## Boundary
This crate owns command language, handler dispatch, parsing, filtering, and rendering for `bd`.
NEVER: rebuild a parallel command tree in `beads-rs` or import daemon internals directly; parse/dispatch was moved here on purpose.

## Routing
- `src/cli.rs` owns the clap tree and top-level dispatch. New subcommands start there and in `src/commands/`.
- `src/commands/` owns typed request construction and human output. `src/render.rs` owns shared response rendering once a payload comes back.
- `src/runtime.rs` owns actor resolution, read-consistency shaping, and IPC connection reuse. Command code should not open sockets or rediscover actor/env state on its own.
- `src/backend.rs` is the crate-local host seam for non-IPC commands such as store/admin offline flows, migration, and upgrade support that cannot be modeled as ordinary daemon requests.
- The host seam over this crate lives in `crates/beads-rs/src/cli/`.
- Keep upgrade/package behavior that needs the package seam in `beads-rs`, not here; that support was moved back out of `beads-cli`.
- If a change is about product behavior across CLI and daemon/package seams, the proof belongs under `crates/beads-rs/tests/`, not here.
- This crate owns user-facing language and rendering; daemon/runtime crates should return structured payloads instead.

## Verification
- `cargo test -p beads-cli` exercises clap parsing, validation, runtime shaping, and command renderers together.
- `just dylint` is the boundary proof when a CLI change gets close to daemon or package seams.
