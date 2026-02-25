## Boundary
This crate owns the daemon runtime process: IPC serving, mutation/query orchestration, and sync scheduling.
Depends on: `beads-daemon-core` primitives, `beads-git`, `beads-core`, and IPC surface types.
Depended on by: `beads-rs` entrypoint wiring and test harnesses.
NEVER: add CLI presentation logic or reimplement core CRDT/domain rules here.

## How To Work Here
- Keep daemon state ownership centralized in runtime store coordinators.
- Route protocol/WAL primitives through `beads-daemon-core` instead of local copies.
- Keep path/config wiring deterministic and explicit (`paths.rs`, `config.rs`).
- Maintain clear transport boundaries: IPC/REPL payloads in typed structs, not ad-hoc JSON maps.

## Verification
- `cargo check -p beads-daemon`
- `cargo test -p beads-daemon`
- `just dylint` (workspace boundary policy)

## Don't Copy This
- Don't bypass runtime gates for mutation admission/read consistency.
- Don't couple runtime internals back into `beads-rs` facade exports.
- Don't leak daemon-only helpers into generic model code without a `beads-daemon-core` home.
