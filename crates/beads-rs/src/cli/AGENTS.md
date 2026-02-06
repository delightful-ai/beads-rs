## Boundary
This directory is the CLI surface: argument parsing + command handlers + rendering.
Depends on: `clap` + `api/` + daemon IPC.
Depended on by: `src/bin/main.rs` (the `bd` binary).
NEVER: mutate state by directly calling git sync; always go through daemon requests.

## How to work here
- Keep handlers thin: validate args → build `Request` → send → render.
- Human output lives alongside each command handler in `src/cli/commands/`; JSON output should use canonical `src/api/` types.
- Golden pattern: `src/cli/commands/create.rs` (request/response + JSON vs human rendering).
- Command wiring lives in `src/cli/mod.rs` and `src/cli/commands/mod.rs`.

## Verification
- `cargo run --bin bd -- --help`
- `cargo test`

## Don't copy this
- Don't print from deep helpers; prefer returning `ResponsePayload` and rendering at the edge.
- Don't parse/validate IDs here; use `ValidatedBeadId`, `ValidatedNamespaceId` etc from `beads-core::validated`.
- Don't normalize collections; use `NamespaceSet` / `DepSpecSet` which self-canonicalize.
