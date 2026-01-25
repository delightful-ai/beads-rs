## Boundary
This directory contains one module per CLI command handler.
Depends on: `src/cli/mod.rs` types, `src/daemon/ipc.rs` requests, and `src/cli/render.rs`.
Depended on by: `src/cli/mod.rs` dispatch.
NEVER: introduce shared business logic here; move it into daemon ops/executor or core types.

## How to work here
- Handler shape: `pub(crate) fn handle(ctx: &Ctx, args: ...) -> Result<()>`.
- Validate “LLM-robust” flag combos early and return `Error::Op(ValidationFailed{...})`.
- For JSON mode: print structured payloads; for human mode: call into `render::*`.

## Verification
- `cargo test` (use `cargo test <command_name>` to filter).
