## Boundary
This directory defines the CRDT/domain model (“Layers 0–9”).
Depends on: pure Rust + `serde`/`time`-level utilities.
Depended on by: every other subsystem (`git/`, `daemon/`, `cli/`, `api/`).
NEVER: do IO (git, sockets, filesystem) or embed UX concerns (CLI flags, JSON wire quirks).

## How to work here
- Prefer types that make invalid states unrepresentable (e.g., `Workflow` over freeform status/closure fields).
- Most user fields should be `Lww<T>` (timestamp + actor). If it’s not `Lww`, document why.
- Keep invariants enforced by construction (e.g., `CanonicalState` maintains `live ∩ tombstones = ∅`).
- When adding/changing fields, update:
  - serialization: `src/git/wire.rs`
  - JSON schema: `src/api/mod.rs`
  - CLI rendering: `src/cli/render.rs` (and relevant command handlers)

## Verification
- `cargo test` after touching merge/invariant code.
- Optional narrowing: `cargo test <pattern>` (e.g., a module name) to iterate faster.

## Don’t copy this
- Don’t “fix” merge behavior with ad-hoc conditionals in callers; CRDT rules belong here.
