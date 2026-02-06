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

## Canonical types (others consume, don't reinvent)
- `ValidatedBeadId`, `ValidatedNamespaceId`, etc. (`validated.rs`) — parse at boundary, trust downstream
- `NamespaceSet`, `DepSpecSet` (`namespace.rs`, `dep.rs`) — self-canonicalizing collections
- `StateJsonlSha256`, `CheckpointContentSha256` (`identity.rs`) — typed digests, not raw `ContentHash`
- `BeadProjection` (`bead.rs`) — derived read view, use instead of re-extracting fields from `BeadView`
- `SnapshotCodec` (`wire_bead.rs`) — single snapshot serialization path for git/checkpoint/WAL

## Don't copy this
- Don't leak daemon or IPC concerns into core types.
- Don't add legacy label/note stores; labels/notes are lineage-only (migrated at import boundary).
- Don't use `ContentHash` for state digests; use the typed wrappers (`StateJsonlSha256` etc).
- Don't add ad-hoc normalization helpers; if a collection needs canonicalization, make a type that enforces it.
