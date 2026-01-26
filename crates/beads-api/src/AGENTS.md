## Boundary
This directory defines canonical serialized schemas for daemon IPC and CLI `--json`.
Depends on: `serde` plus `beads-core` types and minimal helpers.
Depended on by: `beads-rs` CLI rendering and external tooling that consumes JSON output.
NEVER: create lossy “view” structs that silently drop information (use explicit summary types instead).

## How to work here
- Prefer additive, backward-compatible changes (new optional fields with `skip_serializing_if`).
- Keep naming stable (`kebab` flags live in CLI; JSON fields here should be consistent and documented).
- When you change these types, update conversions from `beads-core` and any CLI emitters.

## Verification
- `cargo check -p beads-api`
- `cargo test -p beads-api`

## Don't copy this
- Don't bypass these types by building ad-hoc JSON in the CLI or daemon.
