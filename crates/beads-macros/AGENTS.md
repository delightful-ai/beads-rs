## Boundary
This directory defines proc-macro helpers for the beads workspace.
NEVER: add runtime logic or reach into daemon/CLI behavior.

## How to work here
- Keep expansions deterministic and minimal; avoid side effects.
- Prefer clear compile errors over silent defaults.
- When changing generated APIs, update downstream usage and tests.

## Verification
- `cargo check -p beads-macros`
- `cargo test -p beads-macros`

## Don't copy this
- Don't embed business rules in macros; keep it a thin codegen layer.
