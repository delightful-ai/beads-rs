## Boundary
This crate provides reusable daemon primitives: durability coordination, repl protocol, and WAL/index contracts.
Depends on: `beads-core` plus low-level serialization/storage helpers.
Depended on by: `beads-daemon`, model adapters, and fuzz/model-checking surfaces.
NEVER: add process management, git sync orchestration, or CLI concerns here.

## How To Work Here
- Keep protocol and WAL contracts deterministic and typed.
- Preserve feature-gated seams (`wal-fs`, `wal-sqlite`, `test-harness`) to keep dependencies explicit.
- Put shared protocol logic here first; runtime crates should compose, not fork behavior.
- Prefer narrow trait contracts over leaking runtime-global state.

## Verification
- `cargo check -p beads-daemon-core --all-features`
- `cargo test -p beads-daemon-core --all-features`

## Don't Copy This
- Don't import `beads-daemon` runtime modules from here.
- Don't use untyped map payloads where core typed IDs/digests already exist.
- Don't split protocol logic between core and runtime without a hard boundary reason.
