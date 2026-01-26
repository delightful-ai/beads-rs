## Boundary
This directory defines the CRDT/domain model and core invariants.
Depends on: minimal utility crates plus serialization helpers.
Depended on by: `beads-api`, `beads-surface`, and `beads-rs` daemon logic.
NEVER: introduce IPC, git/WAL/store plumbing, or CLI rendering here.

## How to work here
- Preserve deterministic merges and LWW semantics (`Lww<T>` and friends).
- Keep invariants explicit; prefer types that make invalid states unrepresentable.
- Avoid nondeterminism (iteration order, timestamps) unless explicitly modeled.

## Verification
- `cargo check -p beads-core`
- `cargo test -p beads-core`

## Don't copy this
- Don't leak daemon or IPC concerns into core types.
