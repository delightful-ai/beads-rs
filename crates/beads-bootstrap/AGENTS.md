## Boundary
This crate owns repo discovery, path derivation, and config loading/merging.
NEVER: reintroduce repo/config/path discovery into `beads-rs` or daemon glue just because a thin wrapper still exists there.

## Routing
- `src/repo.rs` owns git-root discovery, including `GIT_DIR`/`GIT_WORK_TREE` behavior.
- `src/paths.rs` stays pure derivation from typed inputs and env strings; do not add filesystem probes or repo discovery there.
- Config-specific behavior lives under `src/config/`.
- Callers may wrap these helpers, but canonical bootstrap behavior belongs here.

## Verification
- `cargo test -p beads-bootstrap` proves repo discovery with git env overrides plus config/path loading semantics.
