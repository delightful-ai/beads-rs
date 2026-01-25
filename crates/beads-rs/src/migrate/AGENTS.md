## Boundary
This directory contains migration/import utilities (compat tooling), not the hot path.
Depends on: `core/` and parsing helpers.
Depended on by: hidden `bd migrate ...` CLI paths.
NEVER: sneak in new “normal operation” logic here; migrations should be explicit and auditable.

## How to work here
- Keep conversions deterministic; prefer a structured report type (see `GoImportReport`).
- If you add a new migration format, isolate it behind a module and keep the surface narrow.

## Verification
- `cargo test`
