## Boundary
This directory is the entire product surface area for `beads-rs` (library + CLI + daemon).
Depends on: Rust stdlib + crates in `Cargo.toml`.
Depended on by: `src/bin/` (CLI entrypoint) and external crates via `beads_rs`.
NEVER: add ad-hoc state outside `CanonicalState` / the git ref; keep “truth” in types.

## How to work here
- Follow the layering: `core` (types) → `git` (sync) → `daemon` (ops/query) → `cli` (UX).
- If you add a new user-visible field, wire it end-to-end: `src/core/` → `src/git/wire.rs` → `src/api/` → `src/cli/`.
- Golden examples: CRDT invariants in `src/core/state.rs`, sync protocol in `src/git/sync.rs`, CLI handler pattern in `src/cli/commands/create.rs`.

## Verification
- `cargo fmt --all`
- `cargo clippy -- -D warnings`
- `cargo test`

## Don’t copy this
- Don’t add direct git writes or state mutation in `cli/` command handlers; go through daemon IPC.
- Don’t introduce lossy “view” structs for JSON; define explicit summaries in `src/api/` if needed.
