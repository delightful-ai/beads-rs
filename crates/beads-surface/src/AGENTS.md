## Boundary
This directory defines IPC protocol types, schemas, and client helpers (public surface boundary).
Depends on: `beads-core` and `beads-api`.
Depended on by: `beads-rs` daemon integration and external clients.
NEVER: include daemon operational logic or git/WAL/store internals here.

## How to work here
- Keep wire formats stable; prefer additive, backward-compatible changes.
- Treat these types as the public contract; document any behavior shifts.
- Keep client helpers thin and deterministic.

## Verification
- `cargo check -p beads-surface`
- `cargo test -p beads-surface`

## Don't copy this
- Don't duplicate core logic; depend on `beads-core` instead.
