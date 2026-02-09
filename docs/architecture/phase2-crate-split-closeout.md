# Phase 2 Crate Split Closeout (`bd-21eg.28`)

Date: 2026-02-08

Follow-on closeout: see `docs/architecture/cli-ownership-closeout.md` (`bd-atsg.5`, 2026-02-09)
for final parse/dispatch ownership and shim cleanup after the phased migration.

## Final State

- `beads-cli`, `beads-daemon`, and `beads-daemon-core` are workspace crates.
- `beads-rs` remains a thin orchestration/compat layer and no longer keeps wrapper-only ownership
  for migrated read-only CLI handlers (`list`, `search`, `ready`, `blocked`, `stale`, `count`).
- Host-coupled CLI paths are boundary-safe through a typed backend seam:
  - trait in `crates/beads-cli/src/backend.rs`
  - impl in `crates/beads-rs/src/cli/backend.rs`

## Boundary Checks

- No direct `crate::daemon::*` imports remain under CLI trees:
  - `crates/beads-rs/src/cli`
  - `crates/beads-cli/src`
- Boundary lint remains required via `just dylint`.

## Verification Matrix

Commands executed for closeout:

```bash
cargo fmt --all
just dylint
cargo clippy --all-features -- -D warnings
cargo test
cargo test --features slow-tests
```

All commands must pass before closing `bd-21eg.28` and the parent epic.
