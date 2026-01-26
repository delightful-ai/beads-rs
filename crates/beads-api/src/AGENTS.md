## Boundary
This directory defines canonical serialized schemas for daemon IPC and CLI `--json`.
Depends on: `serde` + `core/` (and minimal helpers).
Depended on by: `cli/` rendering and any external tooling that consumes JSON output.
NEVER: create lossy “view” structs that silently drop information (use explicit summary types instead).

## How to work here
- Prefer additive, backward-compatible changes (new optional fields with `skip_serializing_if`).
- Keep naming stable (`kebab` flags live in CLI; JSON fields here should be consistent and documented).
- When you change these types, update the conversions from `core` and the CLI code paths that emit them.

## Verification
- `cargo test`
